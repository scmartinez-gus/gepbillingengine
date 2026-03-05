"""Generate example partner report mockups for the new two-sheet format.

Each workbook has:
  Sheet 1 – "Fee Summary": header block + adaptive product summary table
  Sheet 2 – "Fee Detail": simplified per-company line items
"""

import xlsxwriter
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _add_formats(wb):
    """Return a dict of reusable formats."""
    return {
        "title": wb.add_format({
            "bold": True, "font_size": 14, "font_color": "#1F2937",
            "bottom": 2, "bottom_color": "#D1D5DB",
        }),
        "header_label": wb.add_format({
            "bold": True, "font_color": "#6B7280", "font_size": 11,
        }),
        "header_value": wb.add_format({
            "font_size": 11, "font_color": "#1F2937",
        }),
        "header_value_money": wb.add_format({
            "font_size": 11, "font_color": "#1F2937", "num_format": "$#,##0.00",
        }),
        "col_header": wb.add_format({
            "bold": True, "font_size": 11, "font_color": "#FFFFFF",
            "bg_color": "#374151", "border": 1, "border_color": "#D1D5DB",
            "text_wrap": True, "valign": "vcenter",
        }),
        "cell": wb.add_format({
            "font_size": 11, "border": 1, "border_color": "#E5E7EB",
            "valign": "vcenter",
        }),
        "cell_money": wb.add_format({
            "font_size": 11, "num_format": "$#,##0.00",
            "border": 1, "border_color": "#E5E7EB", "valign": "vcenter",
        }),
        "cell_int": wb.add_format({
            "font_size": 11, "num_format": "#,##0",
            "border": 1, "border_color": "#E5E7EB", "valign": "vcenter",
        }),
        "total_label": wb.add_format({
            "bold": True, "font_size": 11, "border": 1, "border_color": "#D1D5DB",
            "bg_color": "#F3F4F6", "valign": "vcenter",
        }),
        "total_money": wb.add_format({
            "bold": True, "font_size": 11, "num_format": "$#,##0.00",
            "border": 1, "border_color": "#D1D5DB",
            "bg_color": "#F3F4F6", "valign": "vcenter",
        }),
        "note": wb.add_format({
            "italic": True, "font_size": 10, "font_color": "#9CA3AF",
        }),
    }


def _write_summary_sheet(wb, fmts, header, charges, note=None):
    """Write the Fee Summary sheet."""
    ws = wb.add_worksheet("Fee Summary")
    ws.hide_gridlines(2)
    ws.set_column("A:A", 36)
    ws.set_column("B:B", 24)
    ws.set_column("C:C", 30)
    ws.set_column("D:D", 16)

    disclaimer = (
        "This report reflects calculated fees. "
        "Final invoice amounts may differ due to adjustments, credits, or discounts."
    )

    row = 0
    ws.merge_range(row, 0, row, 3, "Gusto Embedded Fee Summary", fmts["title"])
    row += 2

    for label, value in header:
        ws.write(row, 0, label, fmts["header_label"])
        if isinstance(value, (int, float)) and "$" not in str(label):
            ws.write(row, 1, value, fmts["header_value"])
        elif isinstance(value, (int, float)):
            ws.write(row, 1, value, fmts["header_value_money"])
        else:
            ws.write(row, 1, str(value), fmts["header_value"])
        row += 1

    row += 1
    charge_headers = ["Product Summary", "Rate", "Quantity", "Amount"]
    for ci, ch in enumerate(charge_headers):
        ws.write(row, ci, ch, fmts["col_header"])
    row += 1

    for charge in charges:
        ws.write(row, 0, charge["charge"], fmts["cell"])
        ws.write(row, 1, charge["rate"], fmts["cell"])
        ws.write(row, 2, charge["quantity"], fmts["cell"])
        ws.write(row, 3, charge["amount"], fmts["cell_money"])
        row += 1

    total = sum(c["amount"] for c in charges)
    ws.write(row, 0, "", fmts["total_label"])
    ws.write(row, 1, "", fmts["total_label"])
    ws.write(row, 2, "Total Fees", fmts["total_label"])
    ws.write(row, 3, total, fmts["total_money"])
    row += 2

    if note:
        ws.merge_range(row, 0, row, 3, note, fmts["note"])
        row += 1

    ws.merge_range(row, 0, row, 3, disclaimer, fmts["note"])


def _write_detail_sheet(wb, fmts, columns, rows, has_next_day=False):
    """Write the Fee Detail sheet."""
    ws = wb.add_worksheet("Fee Detail")
    ws.hide_gridlines(2)

    money_cols = {
        "Company Fee", "Individual User Fee", "Total Fee",
        "Next-Day ACH Fee (Company)", "Next-Day ACH Fee (Individual User)",
    }
    int_cols = {"Active Employees", "Active Contractors", "Total Individual Users"}

    col_widths = {
        "Period": 14, "Company Name": 26, "Company ID": 14,
        "Active Employees": 18, "Active Contractors": 18,
        "Total Individual Users": 22, "Company Fee": 16,
        "Individual User Fee": 20, "Total Fee": 14,
        "Next-Day ACH Fee (Company)": 26,
        "Next-Day ACH Fee (Individual User)": 30,
    }
    for ci, col in enumerate(columns):
        ws.set_column(ci, ci, col_widths.get(col, 18))
        ws.write(0, ci, col, fmts["col_header"])

    for ri, data_row in enumerate(rows, start=1):
        for ci, col in enumerate(columns):
            val = data_row.get(col, "")
            if col in money_cols:
                ws.write(ri, ci, val, fmts["cell_money"])
            elif col in int_cols:
                ws.write(ri, ci, val, fmts["cell_int"])
            else:
                ws.write(ri, ci, str(val), fmts["cell"])


# ---------------------------------------------------------------------------
# Example data builders
# ---------------------------------------------------------------------------

DETAIL_COLS_BASE = [
    "Period", "Company Name", "Company ID",
    "Active Employees", "Active Contractors", "Total Individual Users",
    "Company Fee", "Individual User Fee", "Total Fee",
]

DETAIL_COLS_ACH = DETAIL_COLS_BASE + [
    "Next-Day ACH Fee (Company)", "Next-Day ACH Fee (Individual User)",
]

COMPANIES = [
    ("Sunrise Bakery", "ER-1001", 12, 3),
    ("Cascade Plumbing", "ER-1002", 8, 1),
    ("Redwood Landscaping", "ER-1003", 22, 5),
    ("Summit Electric", "ER-1004", 6, 0),
    ("Harbor Marine Services", "ER-1005", 45, 8),
    ("Pine Valley Dental", "ER-1006", 15, 2),
    ("Silver Creek Auto", "ER-1007", 10, 4),
    ("Golden Gate Catering", "ER-1008", 30, 6),
    ("Bayshore Construction", "ER-1009", 18, 3),
    ("Coastal Roofing", "ER-1010", 5, 1),
    ("Metro Staffing Group", "ER-1011", 60, 12),
    ("Pacific Rim Imports", "ER-1012", 9, 2),
    ("Valley Fresh Produce", "ER-1013", 14, 0),
    ("Beacon Hill Consulting", "ER-1014", 7, 1),
    ("Ironworks Fabrication", "ER-1015", 25, 4),
    ("Lakeside Veterinary", "ER-1016", 11, 2),
    ("Prairie Wind Energy", "ER-1017", 35, 7),
    ("Atlas Moving Co.", "ER-1018", 16, 3),
]


def _build_detail_rows(period, companies, er_rate, iu_rate, included_per_co,
                       next_day_er=0, next_day_iu=0, ach_company_ids=None):
    """Build detail rows for a set of companies."""
    rows = []
    for name, cid, emps, contractors in companies:
        total_iu = emps + contractors
        co_fee = er_rate
        billable_iu = max(total_iu - included_per_co, 0)
        iu_fee = round(billable_iu * iu_rate, 2)
        total_fee = round(co_fee + iu_fee, 2)

        row = {
            "Period": period,
            "Company Name": name,
            "Company ID": cid,
            "Active Employees": emps,
            "Active Contractors": contractors,
            "Total Individual Users": total_iu,
            "Company Fee": co_fee,
            "Individual User Fee": iu_fee,
            "Total Fee": total_fee,
        }

        if ach_company_ids is not None:
            nd_er = next_day_er if cid in ach_company_ids else 0
            billable_for_nd = billable_iu if included_per_co else total_iu
            nd_iu = round(next_day_iu * billable_for_nd, 2) if cid in ach_company_ids else 0
            row["Next-Day ACH Fee (Company)"] = nd_er
            row["Next-Day ACH Fee (Individual User)"] = nd_iu
            row["Total Fee"] = round(total_fee + nd_er + nd_iu, 2)

        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Example 1: ALL_IN · ER metric · 1 included user per company
# ---------------------------------------------------------------------------

def example_1():
    path = OUTPUT_DIR / "Example_1_ER_Metric_Included_Users.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    fmts = _add_formats(wb)

    er_rate = 5.00
    iu_rate = 0.50
    included = 1
    period = "March 2026"
    num_cos = len(COMPANIES)
    total_iu = sum(e + c for _, _, e, c in COMPANIES)
    total_included = num_cos * included
    billable_iu = total_iu - total_included

    total_er_fees = round(num_cos * er_rate, 2)
    total_iu_fees = round(billable_iu * iu_rate, 2)

    header = [
        ("Partner", "Acme Payroll Partners"),
        ("Period", period),
        ("Total Companies", num_cos),
        ("Total Individual Users", total_iu),
    ]

    charges = [
        {
            "charge": f"Company Fee (Tier: 1 \u2013 25)",
            "rate": f"${er_rate:.2f} / company",
            "quantity": f"{num_cos} companies",
            "amount": total_er_fees,
        },
        {
            "charge": f"Individual User Fee (1 included per company)",
            "rate": f"${iu_rate:.2f} / user",
            "quantity": f"{billable_iu} billable of {total_iu} total",
            "amount": total_iu_fees,
        },
    ]

    _write_summary_sheet(wb, fmts, header, charges)
    rows = _build_detail_rows(period, COMPANIES, er_rate, iu_rate, included)
    _write_detail_sheet(wb, fmts, DETAIL_COLS_BASE, rows)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# Example 2: ALL_IN · IU metric · no included users
# ---------------------------------------------------------------------------

def example_2():
    path = OUTPUT_DIR / "Example_2_IU_Metric_No_Included.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    fmts = _add_formats(wb)

    er_rate = 3.00
    iu_rate = 0.75
    included = 0
    period = "March 2026"
    num_cos = len(COMPANIES)
    total_iu = sum(e + c for _, _, e, c in COMPANIES)

    total_er_fees = round(num_cos * er_rate, 2)
    total_iu_fees = round(total_iu * iu_rate, 2)

    header = [
        ("Partner", "Summit HR Solutions"),
        ("Period", period),
        ("Total Companies", num_cos),
        ("Total Individual Users", total_iu),
    ]

    charges = [
        {
            "charge": "Company Fee (Tier: 1 \u2013 500)",
            "rate": f"${er_rate:.2f} / company",
            "quantity": f"{num_cos} companies",
            "amount": total_er_fees,
        },
        {
            "charge": "Individual User Fee",
            "rate": f"${iu_rate:.2f} / user",
            "quantity": f"{total_iu} users",
            "amount": total_iu_fees,
        },
    ]

    _write_summary_sheet(wb, fmts, header, charges)
    rows = _build_detail_rows(period, COMPANIES, er_rate, iu_rate, included)
    _write_detail_sheet(wb, fmts, DETAIL_COLS_BASE, rows)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# Example 3: ALL_IN · FLAT metric (company fee only, no IU fee)
# ---------------------------------------------------------------------------

def example_3():
    path = OUTPUT_DIR / "Example_3_FLAT_Metric.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    fmts = _add_formats(wb)

    er_rate = 12.00
    iu_rate = 0.00
    included = 0
    period = "March 2026"
    num_cos = len(COMPANIES)
    total_iu = sum(e + c for _, _, e, c in COMPANIES)

    total_er_fees = round(num_cos * er_rate, 2)

    header = [
        ("Partner", "Flatline Staffing Co."),
        ("Period", period),
        ("Total Companies", num_cos),
        ("Total Individual Users", total_iu),
    ]

    charges = [
        {
            "charge": "Company Fee",
            "rate": f"${er_rate:.2f} / company",
            "quantity": f"{num_cos} companies",
            "amount": total_er_fees,
        },
    ]

    _write_summary_sheet(wb, fmts, header, charges)

    detail_cols = [
        "Period", "Company Name", "Company ID",
        "Active Employees", "Active Contractors", "Total Individual Users",
        "Company Fee", "Total Fee",
    ]
    rows = _build_detail_rows(period, COMPANIES, er_rate, iu_rate, included)
    for r in rows:
        r.pop("Individual User Fee", None)
    _write_detail_sheet(wb, fmts, detail_cols, rows)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# Example 4: SPLIT · ER and IU land in different tier bands
# ---------------------------------------------------------------------------

def example_4():
    path = OUTPUT_DIR / "Example_4_SPLIT_Pricing.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    fmts = _add_formats(wb)

    er_rate = 4.50
    iu_rate = 0.35
    included = 1
    period = "March 2026"
    num_cos = len(COMPANIES)
    total_iu = sum(e + c for _, _, e, c in COMPANIES)
    total_included = num_cos * included
    billable_iu = total_iu - total_included

    total_er_fees = round(num_cos * er_rate, 2)
    total_iu_fees = round(billable_iu * iu_rate, 2)

    header = [
        ("Partner", "Horizon Workforce Group"),
        ("Period", period),
        ("Total Companies", num_cos),
        ("Total Individual Users", total_iu),
    ]

    charges = [
        {
            "charge": "Company Fee (Tier: 1 \u2013 25)",
            "rate": f"${er_rate:.2f} / company",
            "quantity": f"{num_cos} companies",
            "amount": total_er_fees,
        },
        {
            "charge": "Individual User Fee (Tier: 1 \u2013 500, 1 included per company)",
            "rate": f"${iu_rate:.2f} / user",
            "quantity": f"{billable_iu} billable of {total_iu} total",
            "amount": total_iu_fees,
        },
    ]

    _write_summary_sheet(wb, fmts, header, charges)
    rows = _build_detail_rows(period, COMPANIES, er_rate, iu_rate, included)
    _write_detail_sheet(wb, fmts, DETAIL_COLS_BASE, rows)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# Example 5: IU_PER_ER · rate varies by company size
# ---------------------------------------------------------------------------

def example_5():
    path = OUTPUT_DIR / "Example_5_IU_PER_ER_Metric.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    fmts = _add_formats(wb)

    period = "March 2026"
    num_cos = len(COMPANIES)
    total_iu = sum(e + c for _, _, e, c in COMPANIES)

    tiers = [
        (0, 5, 20.00, 0.00),    # <5 IUs: flat $20, no per-user
        (5, 15, 5.00, 0.50),    # 5-15 IUs: $5 + $0.50/user
        (15, float("inf"), 4.00, 0.40),  # 15+ IUs: $4 + $0.40/user
    ]

    def pick_tier(iu_count):
        for start, end, co_fee, usr_fee in tiers:
            if start <= iu_count < end:
                return co_fee, usr_fee, f"{int(start)} \u2013 {int(end)}" if end != float("inf") else f"{int(start)}+"
        return tiers[-1][2], tiers[-1][3], f"{int(tiers[-1][0])}+"

    detail_rows = []
    total_co_fees = 0
    total_iu_fees = 0
    tier_buckets = {}

    for name, cid, emps, contractors in COMPANIES:
        total_users = emps + contractors
        co_fee, usr_fee, tier_label = pick_tier(total_users)
        iu_fee = round(total_users * usr_fee, 2)
        total = round(co_fee + iu_fee, 2)
        total_co_fees += co_fee
        total_iu_fees += iu_fee

        tier_buckets.setdefault(tier_label, {"co": 0, "rate_co": co_fee, "rate_iu": usr_fee, "count": 0})
        tier_buckets[tier_label]["co"] += 1
        tier_buckets[tier_label]["count"] += total_users

        detail_rows.append({
            "Period": period,
            "Company Name": name,
            "Company ID": cid,
            "Active Employees": emps,
            "Active Contractors": contractors,
            "Total Individual Users": total_users,
            "Company Fee": co_fee,
            "Individual User Fee": iu_fee,
            "Total Fee": total,
        })

    header = [
        ("Partner", "Versatile People Inc."),
        ("Period", period),
        ("Total Companies", num_cos),
        ("Total Individual Users", total_iu),
    ]

    charges = []
    for tier_label in sorted(tier_buckets.keys()):
        b = tier_buckets[tier_label]
        charges.append({
            "charge": f"Company Fee (Tier: {tier_label})",
            "rate": f"${b['rate_co']:.2f} / company",
            "quantity": f"{b['co']} companies",
            "amount": round(b["co"] * b["rate_co"], 2),
        })
        if b["rate_iu"] > 0:
            charges.append({
                "charge": f"Individual User Fee (Tier: {tier_label})",
                "rate": f"${b['rate_iu']:.2f} / user",
                "quantity": f"{b['count']} users",
                "amount": round(b["count"] * b["rate_iu"], 2),
            })

    note = "Rates are determined by each company\u2019s individual user count. See Fee Detail for per-company breakdown."
    _write_summary_sheet(wb, fmts, header, charges, note=note)
    _write_detail_sheet(wb, fmts, DETAIL_COLS_BASE, detail_rows)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# Example 6: ALL_IN · ER metric · included users · Next-Day ACH
# ---------------------------------------------------------------------------

def example_6():
    path = OUTPUT_DIR / "Example_6_With_NextDay_ACH.xlsx"
    wb = xlsxwriter.Workbook(str(path))
    fmts = _add_formats(wb)

    er_rate = 5.00
    iu_rate = 0.50
    included = 1
    nd_er_rate = 2.00
    nd_iu_rate = 0.25
    period = "March 2026"
    num_cos = len(COMPANIES)
    total_iu = sum(e + c for _, _, e, c in COMPANIES)
    total_included = num_cos * included
    billable_iu = total_iu - total_included

    ach_ids = {"ER-1003", "ER-1005", "ER-1008", "ER-1011", "ER-1017"}
    ach_cos = [(n, c, e, ct) for n, c, e, ct in COMPANIES if c in ach_ids]
    ach_count = len(ach_cos)
    ach_iu_total = sum(
        max((e + ct) - included, 0) for _, _, e, ct in ach_cos
    )

    total_er_fees = round(num_cos * er_rate, 2)
    total_iu_fees = round(billable_iu * iu_rate, 2)
    total_nd_er = round(ach_count * nd_er_rate, 2)
    total_nd_iu = round(ach_iu_total * nd_iu_rate, 2)

    header = [
        ("Partner", "Acme Payroll Partners"),
        ("Period", period),
        ("Total Companies", num_cos),
        ("Total Individual Users", total_iu),
    ]

    charges = [
        {
            "charge": "Company Fee (Tier: 1 \u2013 25)",
            "rate": f"${er_rate:.2f} / company",
            "quantity": f"{num_cos} companies",
            "amount": total_er_fees,
        },
        {
            "charge": "Individual User Fee (1 included per company)",
            "rate": f"${iu_rate:.2f} / user",
            "quantity": f"{billable_iu} billable of {total_iu} total",
            "amount": total_iu_fees,
        },
        {
            "charge": "Next-Day ACH \u2013 Company",
            "rate": f"${nd_er_rate:.2f} / company",
            "quantity": f"{ach_count} companies",
            "amount": total_nd_er,
        },
        {
            "charge": "Next-Day ACH \u2013 Individual User",
            "rate": f"${nd_iu_rate:.2f} / user",
            "quantity": f"{ach_iu_total} users",
            "amount": total_nd_iu,
        },
    ]

    _write_summary_sheet(wb, fmts, header, charges)
    rows = _build_detail_rows(period, COMPANIES, er_rate, iu_rate, included,
                              nd_er_rate, nd_iu_rate, ach_ids)
    _write_detail_sheet(wb, fmts, DETAIL_COLS_ACH, rows, has_next_day=True)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    builders = [example_1, example_2, example_3, example_4, example_5, example_6]
    for fn in builders:
        p = fn()
        print(f"  Created: {p.name}")
    print(f"\nAll {len(builders)} examples written to {OUTPUT_DIR}")
