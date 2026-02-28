# gepbillingengine

## Billing Engine CLI

Run the engine directly:

```bash
python3 billing_engine.py
```

Override paths when needed:

```bash
python3 billing_engine.py \
  --inputs-dir "/path/to/inputs" \
  --outputs-dir "/path/to/outputs"
```

## Billing Import Portal (MVP)

This project includes a Streamlit-based import portal:

- Upload usage CSV
- Validate input before run
- Execute billing run
- View audit controls and run status
- Download outputs (master report, NetSuite CSV, partner ZIP, run manifest)
- Optional Slack notifications via webhook

Run locally:

```bash
streamlit run billing_portal.py
```

### Portal configuration

- **Rules workbook path** is set in the sidebar.
- Optional Slack webhook can be entered in the sidebar or environment:

```bash
export BILLING_SLACK_WEBHOOK="https://hooks.slack.com/services/..."
```

## Accrual Engine (CLI)

Month-end revenue accrual using **prior-month actual usage** re-priced with current rules. Outputs a NetSuite **journal entry** CSV (not invoices). Run from the command line; not part of the Streamlit portal.

### Accrual run (estimate for a given month)

Uses the latest usage CSV in the v3 folder whose date prefix is before the accrual month (e.g. for February accrual, uses January or December file). Overrides `FOR_MONTH` to the accrual month and runs the billing engine to produce estimated totals, then writes:

- **JE CSV** — NetSuite journal entry import (AR debit 11140, revenue credits 40113; usage, next-day, minimums by product).
- **Accrual totals CSV** — Per-customer totals for variance reporting.

```bash
python3 accrual_engine.py accrual \
  --accrual-month 2026-02 \
  --rules-path "/path/to/gep_billing_rules.xlsx" \
  --output-dir ./outputs/accrual
```

Optional: `--usage-dir` (default: v3 invoice queries folder), `--log-level`.

### Variance report (after actual billing)

Compare accrual estimate to actual billing output. Run after the real billing run for the same period.

```bash
python3 accrual_engine.py variance \
  --accrual-totals ./outputs/accrual/gep_accrual_totals_202601.csv \
  --actual-master-report "/path/to/2026.01_Master_Billing_Report.xlsx" \
  --output ./outputs/accrual/variance_202601.csv
```

Optional: `--materiality-pct` (default 5).