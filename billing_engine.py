#!/usr/bin/env python3
"""Build GEP billing output files from usage + billing rule inputs.

This script replicates the core fee logic from the supplied n8n workflow:
1. Detect latest usage CSV in the configured inputs directory beginning with "gepusage".
2. Load gep_billing_rules.xlsx tabs: Pricing, Minimums, Config, Mapping.
3. Calculate ER and IU fees from tier rules with ACH override behavior.
4. Apply partner minimum true-up adjustments when applicable.
5. Write:
   - ./outputs/gep_billing_log/{YYYY.MM}_Master_Calculation.csv
   - ./outputs/gep_netsuite_invoice_import.csv
   - ./outputs/gep_partner_details/{PartnerFolder}/{Partner Name} - YYYY.MM.xlsx
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - import guard for runtime setup
    raise SystemExit(
        "Missing dependency: pandas/openpyxl. Install with: python3 -m pip install pandas openpyxl"
    ) from exc

try:
    import xlsxwriter  # noqa: F401
except ImportError as exc:  # pragma: no cover - import guard for runtime setup
    raise SystemExit(
        "Missing dependency: xlsxwriter. Install with: python3 -m pip install xlsxwriter"
    ) from exc


class BillingEngineError(Exception):
    """Raised when input data or configuration is invalid."""


@dataclass(frozen=True)
class Tier:
    """Normalized pricing tier."""

    metric: str
    start: float
    end: float
    company_fee: float
    user_fee: float
    included: int


DEFAULT_INPUTS_DIR = Path(
    "/Users/sam.martinez/Library/CloudStorage/GoogleDrive-sam.martinez@gusto.com/"
    "Shared drives/Accounting Shared Drive (Public)/8 - Team Perm Files/"
    "Revenue Accounting - Perm Files/Embedded Payroll/Invoice Support/"
    "billing_engine_test/inputs"
)
DEFAULT_OUTPUTS_DIR = Path("outputs")
DEFAULT_CONFIG_FILE = "gep_billing_rules.xlsx"
DEFAULT_USAGE_PREFIX = "gepusage"

SHEET_PRICING = "Pricing"
SHEET_MINIMUMS = "Minimums"
SHEET_CONFIG = "Config"
SHEET_MAPPING = "Mapping"

DATE_COLUMNS_OUTPUT = {
    "FOR_MONTH",
    "FIRST_BILLABLE_ACTIVITY_DATE",
    "SUSPENSION_DATE",
    "DISSOCIATION_DATE",
    "MRB_BILLING_ANNIVERSARY",
}

# Preferred output order mirrors the n8n sheet header shape.
PREFERRED_OUTPUT_COLUMNS = [
    "FOR_MONTH",
    "ER_NAME",
    "ER_ID",
    "COMPANY_UUID",
    "PARTNER_NAME",
    "PARTNER_ID",
    "FIRST_BILLABLE_ACTIVITY_DATE",
    "SUSPENSION_DATE",
    "DISSOCIATION_DATE",
    "ACTIVE_EMPLOYEES",
    "NUMBER_ACTIVE_CONTRACTORS",
    "TOTAL_INDIVIDUAL_USERS",
    "CURRENT_ACH_SPEED",
    "IS_MRB",
    "MRB_BILLING_ANNIVERSARY",
    "partner_tier_metric",
    "tier_start",
    "tier_end",
    "metric_value_used",
    "included_users_in_company_fee",
    "user_fee_units_charged",
    "company_fee_units_charged",
    "unit_price_er",
    "er_fee",
    "unit_price_iu",
    "iu_fee",
    "total_fee",
    "fee_calc_status",
    "row_type",
    "partner_minimum_month_index",
    "partner_minimum_amount",
    "partner_minimum_shortfall",
    "partner_minimum_applied",
    "netsuite_customer_name",
]

PARTNER_DETAIL_CALC_COLUMNS = [
    "tier_start",
    "tier_end",
    "er_fee",
    "unit_price_iu",
    "iu_fee",
    "total_fee",
]

PARTNER_DETAIL_INTEGER_COLUMNS = [
    "ACTIVE_EMPLOYEES",
    "NUMBER_ACTIVE_CONTRACTORS",
    "TOTAL_INDIVIDUAL_USERS",
    "tier_start",
    "tier_end",
]

PARTNER_DETAIL_FINANCIAL_COLUMNS = [
    "er_fee",
    "unit_price_iu",
    "iu_fee",
    "total_fee",
]


def normalize_column_name(column: Any) -> str:
    """Normalize a column name to lower snake_case."""
    raw = str(column).strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", raw)
    return normalized.strip("_")


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with normalized snake_case column names."""
    renamed = {col: normalize_column_name(col) for col in df.columns}
    return df.rename(columns=renamed)


def is_missing(value: Any) -> bool:
    """Null-like check that works across pandas/numpy/native types."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def key(value: Any) -> str:
    """String trim helper used by partner/company keying logic."""
    if is_missing(value):
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if numeric.is_integer():
            return str(int(numeric))
    return str(value).strip()


def lower_key(value: Any) -> str:
    """Lowercased, trimmed key helper."""
    return key(value).lower()


def round2(value: Any) -> float:
    """Round to 2 decimals using workflow-consistent behavior."""
    return float(Decimal(str(num(value))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def num(value: Any) -> float:
    """Coerce common monetary/number text values to float."""
    if is_missing(value):
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    s = str(value).replace("$", "").replace(",", "").rstrip("+").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def clean_id(value: Any) -> str:
    """Remove non-digits from partner IDs for mapping consistency."""
    return re.sub(r"[^0-9]", "", key(value))


def metric_norm(metric: Any) -> str:
    """Normalize tier metric values from the pricing sheet."""
    m = key(metric).upper().replace(" ", "_")
    if "IU_PER_ER" in m:
        return "IU_PER_ER"
    if "ACH" in m:
        return "ACH_SPEED"
    if m not in {"ER", "IU", "FLAT"}:
        return "FLAT"
    return m


def parse_excel_serial(value: float) -> Optional[date]:
    """Convert Excel serial date to date if it looks valid."""
    # Keep a broad but practical range to avoid converting random numbers.
    if value < 1000 or value > 100000:
        return None
    base = datetime(1899, 12, 30)
    try:
        converted = base + timedelta(days=float(value))
    except OverflowError:
        return None
    return converted.date()


def parse_date(value: Any) -> Optional[date]:
    """Parse many date forms (Excel serial, M/D/YY, ISO, pandas Timestamp)."""
    if is_missing(value):
        return None

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return parse_excel_serial(float(value))

    s = str(value).strip()
    if not s:
        return None

    # Numeric strings can be Excel serial values.
    if re.fullmatch(r"\d+(\.\d+)?", s):
        yyyymm = re.fullmatch(r"(\d{4})(\d{2})", s)
        if yyyymm:
            year, month = int(yyyymm.group(1)), int(yyyymm.group(2))
            if 1 <= month <= 12:
                return date(year, month, 1)
        maybe_serial = parse_excel_serial(float(s))
        if maybe_serial is not None:
            return maybe_serial

    # Year-month style (e.g. 2026-01 or 2026_01).
    ym = re.fullmatch(r"(\d{4})[-_/](\d{1,2})", s)
    if ym:
        year, month = int(ym.group(1)), int(ym.group(2))
        if 1 <= month <= 12:
            return date(year, month, 1)

    # Explicit M/D/YY or M/D/YYYY handling.
    mdy = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2}|\d{4})", s)
    if mdy:
        month = int(mdy.group(1))
        day = int(mdy.group(2))
        year_raw = mdy.group(3)
        if len(year_raw) == 2:
            year = 1900 + int(year_raw) if int(year_raw) >= 70 else 2000 + int(year_raw)
        else:
            year = int(year_raw)
        try:
            return date(year, month, day)
        except ValueError:
            return None

    parsed = pd.to_datetime(s, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def format_date_mdy_yy(value: Optional[date]) -> str:
    """Format as M/D/YY."""
    if value is None:
        return ""
    return f"{value.month}/{value.day}/{value.year % 100:02d}"


def month_index_1(start_date: date, current_month: date) -> Optional[int]:
    """1-based month index from start date to current month."""
    s = date(start_date.year, start_date.month, 1)
    c = date(current_month.year, current_month.month, 1)
    idx = (c.year - s.year) * 12 + (c.month - s.month) + 1
    return idx if idx >= 1 else None


def usage_partner_key(row: Dict[str, Any]) -> str:
    """Partner key from usage row: partner_id else lower partner_name."""
    return key(row.get("PARTNER_ID")) or lower_key(row.get("PARTNER_NAME"))


def usage_company_key(row: Dict[str, Any]) -> str:
    """Company key from usage row: er_id else lower er_name."""
    return key(row.get("ER_ID")) or lower_key(row.get("ER_NAME"))


def pricing_partner_key(row: Dict[str, Any]) -> str:
    """Partner key from pricing/config/mapping rows."""
    return key(row.get("partner_id")) or lower_key(row.get("partner_name"))


def value_from_aliases(row: Dict[str, Any], aliases: List[str]) -> Any:
    """Return the first non-missing value found for alias names."""
    for alias in aliases:
        if alias in row and not is_missing(row.get(alias)):
            return row.get(alias)
    return None


def looks_like_ach_override(speed_value: Any) -> bool:
    """True when ACH speed indicates 1-day/same-day behavior."""
    s = lower_key(speed_value)
    return ("1-day" in s) or ("1 day" in s) or ("same day" in s)


def sanitize_partner_name(name: Any) -> str:
    """Build filesystem-safe partner token for detail filenames."""
    s = key(name)
    if not s:
        return "UnknownPartner"
    s = s.replace("/", "").replace("\\", "")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]", "", s)
    return s or "UnknownPartner"


def safe_partner_display_name(name: Any) -> str:
    """Build readable, filesystem-safe partner name for detail filenames."""
    s = key(name)
    if not s:
        return "Unknown Partner"
    s = s.replace("/", " ").replace("\\", " ")
    s = re.sub(r"[^A-Za-z0-9 ._-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or "Unknown Partner"


def canonicalize_usage_rows(usage_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert usage dataframe rows to output-style dicts with uppercase columns."""
    records = usage_df.to_dict(orient="records")
    output: List[Dict[str, Any]] = []
    for record in records:
        out_row: Dict[str, Any] = {str(k).upper(): v for k, v in record.items()}
        if "COMPANY_UUID" not in out_row and "company_uuid" in record:
            out_row["COMPANY_UUID"] = record["company_uuid"]
        output.append(out_row)
    return output


def pick_base_tier(p_key: str, tiers_by_partner: Dict[str, List[Tier]], driver: Dict[str, Dict[str, float]]) -> Optional[Tier]:
    """Fallback tier selection logic used when ACH/IU_PER_ER do not apply."""
    tiers = tiers_by_partner.get(p_key, [])
    if not tiers:
        return None

    flat = next((t for t in tiers if t.metric == "FLAT"), None)
    if flat is not None:
        return flat

    metrics = driver.get(p_key, {"ER": 0.0, "IU": 0.0})
    best: Optional[Tier] = None
    for tier in tiers:
        if tier.metric == "ACH_SPEED":
            continue
        if tier.metric == "ER":
            val = metrics.get("ER", 0.0)
        elif tier.metric == "IU":
            val = metrics.get("IU", 0.0)
        else:
            val = None
        if val is None:
            continue
        if tier.start <= val <= tier.end and (best is None or tier.start >= best.start):
            best = tier

    if best is not None:
        return best
    return next((t for t in tiers if t.metric != "ACH_SPEED"), None)


def normalize_output_dates(row: Dict[str, Any]) -> None:
    """Normalize output date columns in-place to M/D/YY."""
    for column in DATE_COLUMNS_OUTPUT:
        if column in row:
            parsed = parse_date(row.get(column))
            row[column] = format_date_mdy_yy(parsed) if parsed else ""


def detect_usage_file(inputs_dir: Path, usage_prefix: str, logger: logging.Logger) -> Path:
    """Find newest usage CSV file whose name starts with usage_prefix."""
    if not inputs_dir.exists() or not inputs_dir.is_dir():
        raise BillingEngineError(f"Inputs folder not found: {inputs_dir}")

    candidates = [
        path
        for path in inputs_dir.iterdir()
        if path.is_file()
        and path.suffix.lower() == ".csv"
        and path.name.lower().startswith(usage_prefix.lower())
    ]
    if not candidates:
        raise BillingEngineError(
            f"No usage CSV found in {inputs_dir} with prefix '{usage_prefix}'."
        )

    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    logger.info("Selected usage file: %s", newest.name)
    return newest


def load_rules_workbook(config_path: Path) -> Dict[str, pd.DataFrame]:
    """Read required tabs from the workbook and normalize their columns."""
    if not config_path.exists():
        raise BillingEngineError(f"Config workbook not found: {config_path}")

    required_sheets = [SHEET_PRICING, SHEET_MINIMUMS, SHEET_CONFIG, SHEET_MAPPING]
    try:
        sheet_map = pd.read_excel(config_path, sheet_name=required_sheets, dtype=object)
    except ValueError as exc:
        raise BillingEngineError(
            f"Workbook is missing one or more required tabs: {required_sheets}"
        ) from exc

    normalized: Dict[str, pd.DataFrame] = {}
    for sheet_name, sheet_df in sheet_map.items():
        normalized[sheet_name] = normalize_dataframe_columns(sheet_df)
    return normalized


def order_columns(rows: List[Dict[str, Any]]) -> List[str]:
    """Build stable output column order with preferred columns first."""
    present = set()
    for row in rows:
        present.update(row.keys())

    ordered = [col for col in PREFERRED_OUTPUT_COLUMNS if col in present]
    extras = sorted(col for col in present if col not in ordered)
    return ordered + extras


def sanitize_partner_id_for_invoice(value: Any) -> str:
    """Normalize partner id token used in invoice external ids."""
    sanitized = re.sub(r"[^A-Za-z0-9]", "", key(value))
    return sanitized or "UNKNOWN"


def month_end(day: date) -> date:
    """Return last calendar day for the month containing the provided date."""
    if day.month == 12:
        first_next = date(day.year + 1, 1, 1)
    else:
        first_next = date(day.year, day.month + 1, 1)
    return first_next - timedelta(days=1)


def generate_netsuite_import_file(df_usage: pd.DataFrame, output_path: Path) -> None:
    """Create NetSuite invoice import CSV with three billing lines per group."""
    required = {"netsuite_customer_name", "FOR_MONTH", "PARTNER_ID", "er_fee", "iu_fee", "total_fee", "row_type"}
    missing = sorted(col for col in required if col not in df_usage.columns)
    if missing:
        raise BillingEngineError(
            f"Cannot generate NetSuite import: missing required columns {missing}"
        )

    rows: List[Dict[str, Any]] = []
    grouped = df_usage.groupby(["netsuite_customer_name", "FOR_MONTH"], dropna=False)
    for (customer_name, for_month), group in grouped:
        parsed_month = parse_date(for_month)
        if parsed_month is None:
            continue
        billing_month = date(parsed_month.year, parsed_month.month, 1)
        transaction_date = month_end(billing_month)
        yyyymm = f"{billing_month.year:04d}{billing_month.month:02d}"

        partner_values = group["PARTNER_ID"].tolist()
        partner_id_raw = next((v for v in partner_values if key(v)), "")
        partner_token = sanitize_partner_id_for_invoice(partner_id_raw)
        invoice_external_id = f"INV-GEP-{yyyymm}-{partner_token}"

        end_users_amount = round2(pd.to_numeric(group["er_fee"], errors="coerce").fillna(0).sum())
        individual_users_amount = round2(pd.to_numeric(group["iu_fee"], errors="coerce").fillna(0).sum())

        min_mask = group["row_type"].fillna("").astype(str).str.lower() == "min_trueup"
        minimum_trueup_amount = round2(
            pd.to_numeric(group.loc[min_mask, "total_fee"], errors="coerce").fillna(0).sum()
        )

        line_defs = [
            ("Embedded Payroll", "End Users", end_users_amount),
            ("Embedded Payroll", "Individual Users", individual_users_amount),
            ("Embedded Payroll : Monthly Minimum", "Minimum True-up", minimum_trueup_amount),
        ]
        for item, desc, amount in line_defs:
            rows.append(
                {
                    "Invoice External ID": invoice_external_id,
                    "Transaction Date": transaction_date.isoformat(),
                    "Customer": key(customer_name),
                    "Item": item,
                    "Desc": desc,
                    "Amount": amount,
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    netsuite_df = pd.DataFrame(
        rows,
        columns=["Invoice External ID", "Transaction Date", "Customer", "Item", "Desc", "Amount"],
    )
    netsuite_df.to_csv(output_path, index=False)


def run_billing_engine(inputs_dir: Path, outputs_dir: Path, usage_prefix: str, config_filename: str, logger: logging.Logger) -> None:
    """Main workflow execution."""
    usage_file = detect_usage_file(inputs_dir, usage_prefix, logger)
    config_path = inputs_dir / config_filename

    logger.info("Loading usage CSV...")
    usage_df = pd.read_csv(usage_file, dtype=object)
    usage_df = normalize_dataframe_columns(usage_df)
    if usage_df.empty:
        raise BillingEngineError(f"Usage file is empty: {usage_file}")
    usage_source_columns = [str(col).upper() for col in usage_df.columns]

    logger.info("Loading rules workbook...")
    sheets = load_rules_workbook(config_path)
    pricing_df = sheets[SHEET_PRICING]
    minimums_df = sheets[SHEET_MINIMUMS]
    config_df = sheets[SHEET_CONFIG]
    mapping_df = sheets[SHEET_MAPPING]

    usage_rows = canonicalize_usage_rows(usage_df)

    # Build NetSuite name mappings by cleaned partner_id and partner_name.
    ns_map_by_clean_id: Dict[str, str] = {}
    ns_map_by_name: Dict[str, str] = {}
    ns_map_by_partner_key: Dict[str, str] = {}
    for row in mapping_df.to_dict(orient="records"):
        partner_id = key(row.get("partner_id"))
        partner_name = lower_key(row.get("partner_name"))
        customer_name = key(
            value_from_aliases(row, ["ns_customer_name", "netsuite_customer_name", "customer_name"])
        )
        if not customer_name:
            continue
        cleaned = clean_id(partner_id)
        if cleaned:
            ns_map_by_clean_id[cleaned] = customer_name
        if partner_name:
            ns_map_by_name[partner_name] = customer_name
        p_key = pricing_partner_key(row)
        if p_key:
            ns_map_by_partner_key[p_key] = customer_name

    # Build config and minimum schedules.
    cfg_by_partner: Dict[str, Dict[str, Optional[date]]] = {}
    for row in config_df.to_dict(orient="records"):
        p_key = pricing_partner_key(row)
        if not p_key:
            continue
        cfg_by_partner[p_key] = {
            "min_start": parse_date(
                value_from_aliases(
                    row,
                    ["minimums_start_month", "minimum_start_month", "min_start_month", "min_start"],
                )
            ),
            "min_end": parse_date(
                value_from_aliases(
                    row,
                    ["minimums_end_month", "minimum_end_month", "min_end_month", "min_end"],
                )
            ),
        }

    min_sched_by_partner: Dict[str, List[Dict[str, float]]] = {}
    for row in minimums_df.to_dict(orient="records"):
        p_key = pricing_partner_key(row)
        if not p_key:
            continue
        min_sched_by_partner.setdefault(p_key, []).append(
            {
                "start": num(value_from_aliases(row, ["month_start", "start_month", "min_month_start"])),
                "end": num(value_from_aliases(row, ["month_end", "end_month", "min_month_end"])),
                "amount": num(value_from_aliases(row, ["minimum_amount", "amount", "minimum"])),
            }
        )
    for p_key in min_sched_by_partner:
        min_sched_by_partner[p_key].sort(key=lambda r: r["start"])

    # Build tier rules by partner.
    tiers_by_partner: Dict[str, List[Tier]] = {}
    for row in pricing_df.to_dict(orient="records"):
        p_key = pricing_partner_key(row)
        if not p_key:
            continue
        end_value = value_from_aliases(row, ["tierend", "tier_end", "end"])
        if is_missing(end_value) or key(end_value) == "":
            tier_end = float("inf")
        else:
            tier_end = num(end_value)
        tier = Tier(
            metric=metric_norm(value_from_aliases(row, ["tier_metric", "metric"])),
            start=num(value_from_aliases(row, ["tierstart", "tier_start", "start"])),
            end=tier_end,
            company_fee=num(value_from_aliases(row, ["er_fee", "company_fee", "unit_price_er"])),
            user_fee=num(value_from_aliases(row, ["iu_fee", "user_fee", "unit_price_iu"])),
            included=1
            if num(value_from_aliases(row, ["include_iu", "included_users_in_company_fee", "included"])) > 0
            else 0,
        )
        tiers_by_partner.setdefault(p_key, []).append(tier)
    for p_key in tiers_by_partner:
        tiers_by_partner[p_key].sort(key=lambda t: t.start)

    # Aggregate partner drivers from usage (ER count and IU total).
    agg_by_partner: Dict[str, Dict[str, Any]] = {}
    for row in usage_rows:
        p_key = usage_partner_key(row)
        c_key = usage_company_key(row)
        if not p_key or not c_key:
            continue
        users = num(row.get("TOTAL_INDIVIDUAL_USERS"))
        agg_entry = agg_by_partner.setdefault(
            p_key, {"companies": set(), "users_by_company": {}}
        )
        agg_entry["companies"].add(c_key)
        previous = agg_entry["users_by_company"].get(c_key, 0.0)
        agg_entry["users_by_company"][c_key] = max(users, previous)

    driver: Dict[str, Dict[str, float]] = {}
    for p_key, agg_entry in agg_by_partner.items():
        er_count = float(len(agg_entry["companies"]))
        iu_sum = float(sum(num(v) for v in agg_entry["users_by_company"].values()))
        driver[p_key] = {"ER": er_count, "IU": iu_sum}

    # FOR_MONTH is required by the workflow logic.
    first_usage = usage_rows[0]
    for_month_raw = (
        first_usage.get("FOR_MONTH")
        if not is_missing(first_usage.get("FOR_MONTH"))
        else first_usage.get("for_month")
    )
    for_month_parsed = parse_date(for_month_raw)
    if for_month_parsed is None:
        raise BillingEngineError("FOR_MONTH not found or invalid in the usage file.")
    for_month_any = date(for_month_parsed.year, for_month_parsed.month, 1)

    # Pass 1: fee computation.
    seen_partner_company: set[str] = set()
    out_rows: List[Dict[str, Any]] = []
    partner_totals: Dict[str, float] = {}

    def lookup_netsuite_name(partner_id_raw: Any, partner_name_raw: Any, p_key: str) -> str:
        cleaned = clean_id(partner_id_raw)
        by_id = ns_map_by_clean_id.get(cleaned, "") if cleaned else ""
        by_name = ns_map_by_name.get(lower_key(partner_name_raw), "")
        by_key = ns_map_by_partner_key.get(p_key, "")
        return by_id or by_name or by_key or ""

    for row in usage_rows:
        p_key = usage_partner_key(row)
        c_key = usage_company_key(row)
        partner_id_raw = row.get("PARTNER_ID")
        partner_name_raw = row.get("PARTNER_NAME")

        if "company_uuid" in row and "COMPANY_UUID" not in row:
            row["COMPANY_UUID"] = row.pop("company_uuid")

        row["netsuite_customer_name"] = lookup_netsuite_name(
            partner_id_raw, partner_name_raw, p_key
        )

        if not p_key or not c_key:
            row["fee_calc_status"] = "bad_data"
            row["row_type"] = "usage"
            row["company_fee_units_charged"] = 0
            row["user_fee_units_charged"] = 0
            row["unit_price_er"] = 0.0
            row["unit_price_iu"] = 0.0
            row["er_fee"] = 0.0
            row["iu_fee"] = 0.0
            row["total_fee"] = 0.0
            out_rows.append(row)
            continue

        tiers = tiers_by_partner.get(p_key, [])
        selected_tier: Optional[Tier] = None

        if looks_like_ach_override(row.get("CURRENT_ACH_SPEED")):
            selected_tier = next((t for t in tiers if t.metric == "ACH_SPEED"), None)

        if selected_tier is None:
            row_users = num(row.get("TOTAL_INDIVIDUAL_USERS"))
            selected_tier = next(
                (
                    t
                    for t in tiers
                    if t.metric == "IU_PER_ER" and t.start <= row_users <= t.end
                ),
                None,
            )

        if selected_tier is None:
            selected_tier = pick_base_tier(p_key, tiers_by_partner, driver)

        if selected_tier is None:
            row["fee_calc_status"] = "missing_tier"
            row["row_type"] = "usage"
            row["company_fee_units_charged"] = 0
            row["user_fee_units_charged"] = 0
            row["unit_price_er"] = 0.0
            row["unit_price_iu"] = 0.0
            row["er_fee"] = 0.0
            row["iu_fee"] = 0.0
            row["total_fee"] = 0.0
            out_rows.append(row)
            continue

        partner_company_key = f"{p_key}|{c_key}"
        first_for_company = partner_company_key not in seen_partner_company
        if first_for_company:
            seen_partner_company.add(partner_company_key)

        total_users = num(row.get("TOTAL_INDIVIDUAL_USERS"))
        chargeable_users = max(total_users - (1 if selected_tier.included else 0), 0.0)

        row["company_fee_units_charged"] = 1 if first_for_company else 0
        row["user_fee_units_charged"] = chargeable_users if first_for_company else 0.0
        company_fee = selected_tier.company_fee if first_for_company else 0.0
        user_fee = round2(chargeable_users * selected_tier.user_fee) if first_for_company else 0.0

        row["partner_tier_metric"] = selected_tier.metric
        row["tier_start"] = selected_tier.start
        row["tier_end"] = (
            selected_tier.end if math.isfinite(selected_tier.end) else ""
        )
        if selected_tier.metric == "ACH_SPEED":
            row["metric_value_used"] = 1
        elif selected_tier.metric == "IU_PER_ER":
            row["metric_value_used"] = total_users
        else:
            partner_driver = driver.get(p_key, {"ER": 0.0, "IU": 0.0})
            if selected_tier.metric == "ER":
                row["metric_value_used"] = partner_driver.get("ER", 0.0)
            elif selected_tier.metric in {"IU", "FLAT"}:
                row["metric_value_used"] = partner_driver.get("IU", 0.0)
            else:
                row["metric_value_used"] = ""

        row["included_users_in_company_fee"] = selected_tier.included
        row["unit_price_iu"] = selected_tier.user_fee
        row["unit_price_er"] = selected_tier.company_fee
        row["er_fee"] = round2(company_fee)
        row["iu_fee"] = round2(user_fee)
        row["total_fee"] = round2(row["er_fee"] + row["iu_fee"])
        row["fee_calc_status"] = "ok"
        row["row_type"] = "usage"

        if first_for_company:
            partner_totals[p_key] = round2(partner_totals.get(p_key, 0.0) + row["total_fee"])

        out_rows.append(row)

    # Pass 2: partner minimum true-up.
    for p_key, base_total in partner_totals.items():
        cfg = cfg_by_partner.get(p_key)
        if not cfg:
            continue

        min_start = cfg.get("min_start")
        min_end = cfg.get("min_end")
        if min_start is None:
            continue

        now_idx = month_index_1(min_start, for_month_any)
        if now_idx is None:
            continue

        if min_end is not None:
            end_idx = month_index_1(min_start, min_end)
            if end_idx is not None and now_idx > end_idx:
                continue

        schedule_row = next(
            (
                sched
                for sched in min_sched_by_partner.get(p_key, [])
                if sched["start"] <= now_idx <= sched["end"]
            ),
            None,
        )
        if schedule_row is None:
            continue

        minimum_amount = num(schedule_row["amount"])
        shortfall = round2(max(minimum_amount - base_total, 0.0))
        if shortfall <= 0:
            continue

        any_row = next(
            (
                r
                for r in out_rows
                if (key(r.get("PARTNER_ID")) == key(p_key))
                or (lower_key(r.get("PARTNER_NAME")) == lower_key(p_key))
            ),
            None,
        )

        partner_id_raw = any_row.get("PARTNER_ID") if any_row else p_key
        partner_name_raw = any_row.get("PARTNER_NAME") if any_row else ""
        ns_name = lookup_netsuite_name(partner_id_raw, partner_name_raw, p_key)
        for_month_for_row = format_date_mdy_yy(date(for_month_any.year, for_month_any.month, 1))

        out_rows.append(
            {
                "FOR_MONTH": for_month_for_row,
                "PARTNER_ID": partner_id_raw,
                "PARTNER_NAME": partner_name_raw,
                "ER_ID": "MIN_ADJ",
                "ER_NAME": "Monthly Minimum True-up",
                "TOTAL_INDIVIDUAL_USERS": 0,
                "netsuite_customer_name": ns_name,
                "unit_price_iu": 0.0,
                "unit_price_er": 0.0,
                "partner_tier_metric": "MIN",
                "tier_start": now_idx,
                "tier_end": now_idx,
                "metric_value_used": now_idx,
                "included_users_in_company_fee": 0,
                "company_fee_units_charged": 0,
                "user_fee_units_charged": 0,
                "er_fee": 0.0,
                "iu_fee": shortfall,
                "total_fee": shortfall,
                "fee_calc_status": "min_trueup",
                "row_type": "min_trueup",
                "partner_minimum_month_index": now_idx,
                "partner_minimum_amount": minimum_amount,
                "partner_minimum_shortfall": shortfall,
                "partner_minimum_applied": True,
            }
        )

    # Sort output similarly to workflow behavior.
    out_rows.sort(
        key=lambda r: (
            lower_key(r.get("PARTNER_NAME")),
            1 if r.get("row_type") == "min_trueup" else 0,
            parse_date(r.get("FIRST_BILLABLE_ACTIVITY_DATE") or r.get("FOR_MONTH")) or date.min,
        )
    )

    for row in out_rows:
        normalize_output_dates(row)
        row.setdefault("partner_minimum_applied", False)
        row.setdefault("partner_minimum_month_index", "")
        row.setdefault("partner_minimum_amount", "")
        row.setdefault("partner_minimum_shortfall", "")
        row.setdefault("partner_tier_metric", "")
        row.setdefault("tier_start", "")
        row.setdefault("tier_end", "")
        row.setdefault("metric_value_used", "")
        row.setdefault("included_users_in_company_fee", "")
        row.setdefault("company_fee_units_charged", "")
        row.setdefault("user_fee_units_charged", "")
        row.setdefault("unit_price_er", "")
        row.setdefault("unit_price_iu", "")
        row.setdefault("er_fee", "")
        row.setdefault("iu_fee", "")
        row.setdefault("total_fee", "")
        row.setdefault("fee_calc_status", "")
        row.setdefault("row_type", "usage")
        row.setdefault("netsuite_customer_name", "")

    outputs_dir.mkdir(parents=True, exist_ok=True)
    partner_dir = outputs_dir / "gep_partner_details"
    partner_dir.mkdir(parents=True, exist_ok=True)
    history_path = outputs_dir / "gep_billing_log"
    history_path.mkdir(parents=True, exist_ok=True)
    netsuite_template_path = outputs_dir / "gep_netsuite_invoice_import.csv"
    logger.debug("NetSuite upload template path: %s", netsuite_template_path)

    output_columns = order_columns(out_rows)
    master_df = pd.DataFrame(out_rows, columns=output_columns)

    billing_period = f"{for_month_any.year:04d}.{for_month_any.month:02d}"
    history_file_path = history_path / f"{billing_period}_Master_Calculation.csv"
    master_df.to_csv(history_file_path, index=False)
    logger.info("Wrote master calculation history file: %s", history_file_path)

    generate_netsuite_import_file(master_df, netsuite_template_path)
    logger.info("Wrote NetSuite import file: %s", netsuite_template_path)

    # Partner split files.
    if "PARTNER_NAME" not in master_df.columns:
        raise BillingEngineError("PARTNER_NAME column missing from output data.")
    if "FOR_MONTH" not in master_df.columns:
        raise BillingEngineError("FOR_MONTH column missing from output data.")

    detail_columns: List[str] = []
    for col in usage_source_columns + PARTNER_DETAIL_CALC_COLUMNS:
        if col not in detail_columns:
            detail_columns.append(col)
    for col in detail_columns:
        if col not in master_df.columns:
            master_df[col] = ""

    for partner_name, group in master_df.groupby("PARTNER_NAME", dropna=False):
        group_partner_name = key(partner_name)
        if not group_partner_name:
            partner_id = key(group["PARTNER_ID"].iloc[0]) if "PARTNER_ID" in group.columns else ""
            group_partner_name = partner_id or "UnknownPartner"

        for_month_candidates = [v for v in group["FOR_MONTH"].tolist() if key(v)]
        month_source = for_month_candidates[0] if for_month_candidates else ""
        parsed_month = parse_date(month_source) or for_month_any
        year_month = f"{parsed_month.year:04d}.{parsed_month.month:02d}"

        partner_folder = sanitize_partner_name(group_partner_name)
        partner_folder_path = partner_dir / partner_folder
        os.makedirs(partner_folder_path, exist_ok=True)

        partner_display = safe_partner_display_name(group_partner_name)
        file_name = f"{partner_display} - {year_month}.xlsx"
        file_path = partner_folder_path / file_name

        detail_df = group.loc[:, detail_columns].copy()

        # Normalize known numeric output columns before applying Excel formats.
        for col_name in PARTNER_DETAIL_INTEGER_COLUMNS:
            if col_name in detail_df.columns:
                detail_df[col_name] = pd.to_numeric(detail_df[col_name], errors="coerce").fillna(0)
        for col_name in PARTNER_DETAIL_FINANCIAL_COLUMNS:
            if col_name in detail_df.columns:
                detail_df[col_name] = pd.to_numeric(detail_df[col_name], errors="coerce").fillna(0)

        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            sheet_name = "Partner Detail"
            detail_df.to_excel(writer, index=False, sheet_name=sheet_name)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            fmt_int = workbook.add_format({"num_format": "0"})
            fmt_money = workbook.add_format({"num_format": "#,##0.00"})

            for col_idx, col_name in enumerate(detail_df.columns):
                col_fmt = None
                if col_name in PARTNER_DETAIL_INTEGER_COLUMNS:
                    col_fmt = fmt_int
                elif col_name in PARTNER_DETAIL_FINANCIAL_COLUMNS:
                    col_fmt = fmt_money
                worksheet.set_column(col_idx, col_idx, 18, col_fmt)

        logger.info("Wrote partner detail file: %s", file_path)


def build_parser() -> argparse.ArgumentParser:
    """CLI parser."""
    parser = argparse.ArgumentParser(
        description="Replicate GEP n8n billing workflow into staging CSV and partner Excel outputs."
    )
    parser.add_argument(
        "--inputs-dir",
        default=str(DEFAULT_INPUTS_DIR),
        help=(
            "Input directory containing usage CSV and rules workbook. "
            "Defaults to the configured Google Drive path."
        ),
    )
    parser.add_argument(
        "--outputs-dir",
        default=str(DEFAULT_OUTPUTS_DIR),
        help="Output directory for staging CSV and partner detail Excel files.",
    )
    parser.add_argument(
        "--usage-prefix",
        default=DEFAULT_USAGE_PREFIX,
        help="Prefix used to detect usage CSV files in inputs directory.",
    )
    parser.add_argument(
        "--config-file",
        default=DEFAULT_CONFIG_FILE,
        help="Workbook file name inside inputs directory.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    """Program entrypoint."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("billing_engine")

    inputs_dir = Path(args.inputs_dir).resolve()
    outputs_dir = Path(args.outputs_dir).resolve()

    try:
        run_billing_engine(
            inputs_dir=inputs_dir,
            outputs_dir=outputs_dir,
            usage_prefix=args.usage_prefix,
            config_filename=args.config_file,
            logger=logger,
        )
    except BillingEngineError as exc:
        logger.error("%s", exc)
        return 2
    except Exception:
        logger.exception("Unexpected failure while building billing files.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
