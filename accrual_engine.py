#!/usr/bin/env python3
"""GEP revenue accrual engine: estimate using prior-month actuals, output NetSuite JE CSV.

Runs independently of the billing portal. Use for month-end accruals before actual
usage is available. Estimation = prior month raw usage re-priced with current rules.
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from billing_engine import (
    BillingEngineError,
    DEFAULT_CONFIG_FILE,
    month_end,
    normalize_dataframe_columns,
    parse_date,
    round2,
    run_billing_engine,
)


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DEFAULT_V3_USAGE_DIR = Path(
    "/Users/sam.martinez/Library/CloudStorage/GoogleDrive-sam.martinez@gusto.com/"
    "Shared drives/Accounting Shared Drive (Public)/8 - Team Perm Files/"
    "Revenue Accounting - Perm Files/Embedded Payroll/Invoice Support/"
    "! - Query Exports/v3 invoice queries"
)
ACCRUAL_GL_AR = "11140"
ACCRUAL_GL_REVENUE = "40113"

JE_COLUMNS = [
    "Customer",
    "Product",
    "Transaction Date",
    "Account",
    "Debit",
    "Credit",
    "Journal Entry - Line Memo",
    "Journal Entry : Memo",
]


# -----------------------------------------------------------------------------
# Prior-month file detection
# -----------------------------------------------------------------------------
def _parse_date_prefix_from_filename(name: str) -> Optional[Tuple[int, int]]:
    """Parse YYYY.MM from start of filename (e.g. 2025.12_REDASH_...). Returns (year, month) or None."""
    match = re.match(r"^(\d{4})\.(\d{1,2})", name.strip())
    if not match:
        return None
    y, m = int(match.group(1)), int(match.group(2))
    if 1 <= m <= 12:
        return (y, m)
    return None


def find_prior_month_usage_file(
    usage_dir: Path,
    accrual_month: date,
    logger: logging.Logger,
) -> Path:
    """Find the usage CSV in usage_dir whose date prefix is the latest month < accrual_month."""
    if not usage_dir.exists() or not usage_dir.is_dir():
        raise BillingEngineError(f"Usage directory not found: {usage_dir}")

    candidates: List[Tuple[Tuple[int, int], Path]] = []
    for path in usage_dir.iterdir():
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue
        parsed = _parse_date_prefix_from_filename(path.name)
        if parsed is None:
            continue
        y, m = parsed
        file_month = date(y, m, 1)
        if file_month < accrual_month:
            candidates.append((parsed, path))

    if not candidates:
        raise BillingEngineError(
            f"No prior-month usage CSV found in {usage_dir} with date prefix before "
            f"{accrual_month.year}.{accrual_month.month:02d}. "
            "Expected filenames like 2025.12_REDASH_....csv"
        )

    # Latest prior month
    candidates.sort(key=lambda x: x[0], reverse=True)
    chosen = candidates[0][1]
    logger.info("Selected prior-month usage file: %s", chosen.name)
    return chosen


# -----------------------------------------------------------------------------
# Accrual run: load prior month, override FOR_MONTH, run billing engine
# -----------------------------------------------------------------------------
def _load_and_override_for_month(
    usage_path: Path,
    accrual_month: date,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Load usage CSV, normalize columns, set FOR_MONTH to accrual month."""
    logger.info("Loading usage from %s", usage_path.name)
    df = pd.read_csv(usage_path, dtype=object)
    df = normalize_dataframe_columns(df)
    if df.empty:
        raise BillingEngineError(f"Usage file is empty: {usage_path}")

    # Ensure FOR_MONTH column (may be for_month after normalize)
    for_month_col = None
    for c in df.columns:
        if c.upper() == "FOR_MONTH":
            for_month_col = c
            break
    if for_month_col is None:
        raise BillingEngineError("Usage file has no FOR_MONTH column.")

    # Set every row to accrual month (first day)
    accrual_first = accrual_month.replace(day=1)
    value = accrual_first.strftime("%Y-%m-%d")
    df[for_month_col] = value
    logger.info("Overrode FOR_MONTH to %s for accrual month", value)
    return df


def _aggregate_by_customer(master_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Aggregate master billing output by netsuite_customer_name. Returns dict customer -> {usage, next_day, minimums, total}."""
    if "netsuite_customer_name" not in master_df.columns:
        return {}

    # Normalize numeric columns
    for col in ["er_fee", "iu_fee", "next_day_er_fee", "next_day_iu_fee", "total_fee", "row_type"]:
        if col not in master_df.columns:
            master_df[col] = 0.0 if col != "row_type" else ""

    master_df["_er_fee"] = pd.to_numeric(master_df["er_fee"], errors="coerce").fillna(0)
    master_df["_iu_fee"] = pd.to_numeric(master_df["iu_fee"], errors="coerce").fillna(0)
    master_df["_nd_er"] = pd.to_numeric(master_df["next_day_er_fee"], errors="coerce").fillna(0)
    master_df["_nd_iu"] = pd.to_numeric(master_df["next_day_iu_fee"], errors="coerce").fillna(0)
    master_df["_total_fee"] = pd.to_numeric(master_df["total_fee"], errors="coerce").fillna(0)
    master_df["_row_type"] = master_df["row_type"].fillna("").astype(str).str.strip().str.lower()

    usage_mask = master_df["_row_type"] == "usage"
    min_mask = master_df["_row_type"] == "min_trueup"

    aggregated: Dict[str, Dict[str, float]] = {}
    for customer in master_df["netsuite_customer_name"].dropna().unique():
        customer = str(customer).strip()
        if not customer:
            continue
        subset = master_df[master_df["netsuite_customer_name"] == customer]
        usage_sub = subset.loc[usage_mask]
        min_sub = subset.loc[min_mask]

        usage_amt = round2(
            usage_sub["_er_fee"].sum() + usage_sub["_iu_fee"].sum()
        )
        next_day_amt = round2(
            usage_sub["_nd_er"].sum() + usage_sub["_nd_iu"].sum()
        )
        min_amt = round2(min_sub["_total_fee"].sum())
        total = round2(usage_amt + next_day_amt + min_amt)
        if total <= 0:
            continue
        aggregated[customer] = {
            "usage": usage_amt,
            "next_day": next_day_amt,
            "minimums": min_amt,
            "total": total,
        }
    return aggregated


def _memo_base(accrual_month: date) -> str:
    """Base memo text: e.g. January 2026 Embedded Payroll Estimate."""
    return accrual_month.strftime("%B %Y Embedded Payroll Estimate")


def generate_journal_entry_csv(
    aggregated: Dict[str, Dict[str, float]],
    accrual_month: date,
    output_path: Path,
    logger: logging.Logger,
) -> None:
    """Write NetSuite JE import CSV. One AR line and up to three revenue lines per customer."""
    transaction_date = month_end(accrual_month).strftime("%m/%d/%Y")
    memo_base = _memo_base(accrual_month)

    rows: List[Dict[str, Any]] = []
    for customer, amounts in sorted(aggregated.items()):
        total = amounts["total"]
        usage = amounts["usage"]
        next_day = amounts["next_day"]
        minimums = amounts["minimums"]

        # AR line: debit total, no product
        rows.append({
            "Customer": customer,
            "Product": "",
            "Transaction Date": transaction_date,
            "Account": ACCRUAL_GL_AR,
            "Debit": round2(total),
            "Credit": 0,
            "Journal Entry - Line Memo": memo_base,
            "Journal Entry : Memo": memo_base,
        })

        # Revenue lines: credit to 40113, with product and line memo suffix
        if usage > 0:
            rows.append({
                "Customer": customer,
                "Product": "GEP Usage",
                "Transaction Date": transaction_date,
                "Account": ACCRUAL_GL_REVENUE,
                "Debit": 0,
                "Credit": round2(usage),
                "Journal Entry - Line Memo": f"{memo_base} - GEP Usage",
                "Journal Entry : Memo": memo_base,
            })
        if next_day > 0:
            rows.append({
                "Customer": customer,
                "Product": "GEP Next-Day Direct Deposit",
                "Transaction Date": transaction_date,
                "Account": ACCRUAL_GL_REVENUE,
                "Debit": 0,
                "Credit": round2(next_day),
                "Journal Entry - Line Memo": f"{memo_base} - GEP Next-Day Direct Deposit",
                "Journal Entry : Memo": memo_base,
            })
        if minimums > 0:
            rows.append({
                "Customer": customer,
                "Product": "GEP Minimums",
                "Transaction Date": transaction_date,
                "Account": ACCRUAL_GL_REVENUE,
                "Debit": 0,
                "Credit": round2(minimums),
                "Journal Entry - Line Memo": f"{memo_base} - GEP Minimums",
                "Journal Entry : Memo": memo_base,
            })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=JE_COLUMNS)
    df.to_csv(output_path, index=False)
    logger.info("Wrote journal entry CSV: %s (%d lines)", output_path, len(rows))


def run_accrual(
    accrual_month: date,
    usage_dir: Path,
    rules_path: Path,
    output_dir: Path,
    logger: logging.Logger,
    save_billing_detail: bool = False,
) -> Tuple[Path, Path]:
    """
    Run accrual: find prior-month usage, re-price with current rules, output JE CSV and accrual totals.

    If save_billing_detail is True, copies the billing engine's Master Billing Report to output_dir
    so you can inspect how fees were calculated (Audit & Controls, Executive Summary, Source Data).

    Returns (path_to_je_csv, path_to_accrual_totals_csv) for variance reporting.
    """
    usage_dir = usage_dir.resolve()
    rules_path = rules_path.resolve()
    output_dir = output_dir.resolve()

    if not rules_path.exists():
        raise BillingEngineError(f"Rules workbook not found: {rules_path}")

    prior_file = find_prior_month_usage_file(usage_dir, accrual_month, logger)
    df_usage = _load_and_override_for_month(prior_file, accrual_month, logger)

    with tempfile.TemporaryDirectory(prefix="gep_accrual_") as tmp:
        tmp_path = Path(tmp)
        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()
        outputs_dir = tmp_path / "outputs"

        # Write usage CSV with prefix billing engine expects
        usage_csv = inputs_dir / "gepusage.csv"
        # Billing engine normalizes to snake_case on read; keep columns as-is for compatibility
        df_usage.to_csv(usage_csv, index=False)
        config_copy = inputs_dir / rules_path.name
        shutil.copy2(rules_path, config_copy)

        logger.info("Running billing engine with prior-month usage (accrual month=%s)...", accrual_month.strftime("%Y-%m"))
        run_billing_engine(
            inputs_dir=inputs_dir,
            outputs_dir=outputs_dir,
            usage_prefix="gepusage",
            config_filename=rules_path.name,
            logger=logger,
        )

        billing_period = f"{accrual_month.year:04d}.{accrual_month.month:02d}"
        master_path = outputs_dir / "gep_billing_log" / f"{billing_period}_Master_Billing_Report.xlsx"
        if not master_path.exists():
            raise BillingEngineError(f"Expected master report not found: {master_path}")

        master_df = pd.read_excel(master_path, sheet_name="Source Data", dtype=object)
        aggregated = _aggregate_by_customer(master_df)

        if not aggregated:
            raise BillingEngineError(
                "No customer totals after aggregation. Check Mapping sheet for netsuite_customer_name."
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        je_path = output_dir / f"gep_accrual_JE_{accrual_month.year:04d}{accrual_month.month:02d}.csv"
        generate_journal_entry_csv(aggregated, accrual_month, je_path, logger)

        # Accrual totals for variance report (customer, usage, next_day, minimums, total)
        totals_path = output_dir / f"gep_accrual_totals_{accrual_month.year:04d}{accrual_month.month:02d}.csv"
        totals_rows = [
            {
                "Customer": c,
                "Usage": agg["usage"],
                "Next_Day": agg["next_day"],
                "Minimums": agg["minimums"],
                "Total": agg["total"],
            }
            for c, agg in sorted(aggregated.items())
        ]
        pd.DataFrame(totals_rows).to_csv(totals_path, index=False)
        logger.info("Wrote accrual totals for variance: %s", totals_path)

        if save_billing_detail:
            detail_name = f"gep_accrual_billing_detail_{accrual_month.year:04d}{accrual_month.month:02d}.xlsx"
            detail_path = output_dir / detail_name
            shutil.copy2(master_path, detail_path)
            logger.info("Wrote billing detail (fee calculation) for review: %s", detail_path)

    return (je_path, totals_path)


# -----------------------------------------------------------------------------
# Variance report: compare accrual totals vs actual billing output
# -----------------------------------------------------------------------------
def run_variance_report(
    accrual_totals_path: Path,
    actual_master_report_path: Path,
    output_path: Path,
    logger: logging.Logger,
    materiality_pct: float = 5.0,
) -> None:
    """
    Compare accrual estimate to actual billing. actual_master_report_path = path to
    *Master_Billing_Report.xlsx (Source Data sheet used).
    """
    accrual_df = pd.read_csv(accrual_totals_path)
    actual_df = pd.read_excel(actual_master_report_path, sheet_name="Source Data", dtype=object)

    # Aggregate actual by netsuite_customer_name (same as accrual)
    actual_agg = _aggregate_by_customer(actual_df)

    # Build variance rows
    all_customers = sorted(
        set(accrual_df["Customer"].dropna().astype(str)) | set(actual_agg.keys())
    )
    variance_rows: List[Dict[str, Any]] = []
    accrual_by_customer = accrual_df.set_index("Customer")["Total"].to_dict()

    for customer in all_customers:
        est = float(accrual_by_customer.get(customer, 0))
        act = float(actual_agg.get(customer, {}).get("total", 0))
        var = round2(act - est)
        var_pct = (var / est * 100.0) if est else (100.0 if act else 0.0)
        flag = "⚠" if abs(var_pct) > materiality_pct else ""
        variance_rows.append({
            "Customer": customer,
            "Estimated": round2(est),
            "Actual": round2(act),
            "Variance": var,
            "Variance_Pct": round2(var_pct),
            "Flag": flag,
        })

    total_est = round2(accrual_df["Total"].sum())
    total_act = round2(sum(a["total"] for a in actual_agg.values()))
    total_var = round2(total_act - total_est)
    total_var_pct = (total_var / total_est * 100.0) if total_est else 0.0
    variance_rows.append({
        "Customer": "TOTAL",
        "Estimated": total_est,
        "Actual": total_act,
        "Variance": total_var,
        "Variance_Pct": round2(total_var_pct),
        "Flag": "⚠" if abs(total_var_pct) > materiality_pct else "✓",
    })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(variance_rows)
    out_df.to_csv(output_path, index=False)
    logger.info("Wrote variance report: %s (materiality threshold %.1f%%)", output_path, materiality_pct)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _parse_accrual_month(s: str) -> date:
    """Parse YYYY-MM or YYYY.MM to first day of month."""
    s = s.strip().replace(".", "-")
    parts = s.split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected YYYY-MM or YYYY.MM, got: {s}")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError(f"Invalid month: {m}")
    return date(y, m, 1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GEP accrual engine: estimate revenue from prior-month usage, output NetSuite JE CSV."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # accrual
    acc = subparsers.add_parser("accrual", help="Run accrual for a given month")
    acc.add_argument(
        "--accrual-month",
        required=True,
        metavar="YYYY-MM",
        help="Month to accrue (e.g. 2026-02). JE date = last day of this month.",
    )
    acc.add_argument(
        "--usage-dir",
        default=str(DEFAULT_V3_USAGE_DIR),
        help="Directory containing prior-month usage CSVs with YYYY.MM prefix.",
    )
    acc.add_argument(
        "--rules-path",
        required=True,
        metavar="PATH",
        help="Path to gep_billing_rules.xlsx.",
    )
    acc.add_argument(
        "--output-dir",
        default="outputs/accrual",
        help="Output directory for JE CSV and accrual totals CSV.",
    )
    acc.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    acc.add_argument(
        "--save-billing-detail",
        action="store_true",
        help="Save the billing engine Master Billing Report (Audit, Executive Summary, Source Data) to output-dir for fee calculation review.",
    )

    # variance
    var = subparsers.add_parser("variance", help="Compare accrual estimate to actual billing")
    var.add_argument(
        "--accrual-totals",
        required=True,
        metavar="PATH",
        help="Path to gep_accrual_totals_YYYYMM.csv from accrual run.",
    )
    var.add_argument(
        "--actual-master-report",
        required=True,
        metavar="PATH",
        help="Path to YYYY.MM_Master_Billing_Report.xlsx from actual billing run.",
    )
    var.add_argument(
        "--output",
        required=True,
        metavar="PATH",
        help="Output path for variance report CSV.",
    )
    var.add_argument(
        "--materiality-pct",
        type=float,
        default=5.0,
        help="Variance %% threshold for flag (default 5).",
    )
    var.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("accrual_engine")

    try:
        if args.command == "accrual":
            accrual_month = _parse_accrual_month(args.accrual_month)
            usage_dir = Path(args.usage_dir).resolve()
            rules_path = Path(args.rules_path).resolve()
            output_dir = Path(args.output_dir).resolve()
            run_accrual(
                accrual_month=accrual_month,
                usage_dir=usage_dir,
                rules_path=rules_path,
                output_dir=output_dir,
                logger=logger,
                save_billing_detail=getattr(args, "save_billing_detail", False),
            )
        elif args.command == "variance":
            run_variance_report(
                accrual_totals_path=Path(args.accrual_totals).resolve(),
                actual_master_report_path=Path(args.actual_master_report).resolve(),
                output_path=Path(args.output).resolve(),
                logger=logger,
                materiality_pct=args.materiality_pct,
            )
        else:
            parser.error(f"Unknown command: {args.command}")
    except BillingEngineError as exc:
        logger.error("%s", exc)
        return 2
    except ValueError as exc:
        logger.error("%s", exc)
        return 2
    except Exception:
        logger.exception("Unexpected failure.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
