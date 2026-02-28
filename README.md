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

## Billing Watcher (automated runs)

A background service that monitors the inputs folder for new usage files. When a `gepusage*.csv` appears, the watcher confirms the file is fully synced (stable file size across two consecutive polls), then runs the billing engine automatically.

### Start the watcher

```bash
python3 billing_watcher.py
```

This checks the default Google Drive inputs folder every 30 minutes. Outputs go to the default outputs folder. To customize:

```bash
python3 billing_watcher.py \
  --inputs-dir "/path/to/inputs" \
  --outputs-dir "/path/to/outputs" \
  --poll-interval 900 \
  --slack-webhook "https://hooks.slack.com/services/..."
```

### How it works

1. Every N seconds (default 1800 = 30 min), the watcher lists the inputs folder for `gepusage*.csv` files.
2. New files are held for one cycle to confirm the file size is stable (Google Drive sync is finished).
3. Once stable, the billing engine runs and outputs are written (Master Report, NetSuite CSV, partner details).
4. The file is recorded in a local ledger (`outputs/watcher_state/processed_files.json`) so it won't be processed again.
5. If a Slack webhook is configured, a notification is sent with run status and audit outcome.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--poll-interval` | 1800 (30 min) | Seconds between folder checks |
| `--slack-webhook` | `$BILLING_SLACK_WEBHOOK` | Slack webhook URL for notifications |
| `--dry-run` | off | Detect files but don't run billing |
| `--reset-ledger` | off | Clear the processed-files ledger and start fresh |
| `--log-level` | INFO | DEBUG, INFO, WARNING, or ERROR |

### Stop the watcher

Press `Ctrl+C` — it will finish the current cycle and exit cleanly.

## Billing Dashboard (monitoring)

Read-only dashboard for reviewing billing results. No uploads, no manual triggers — it reads directly from the outputs the watcher produces.

```bash
python3 -m streamlit run billing_dashboard.py
```

Three tabs:

- **Latest Run** — status banner (pass/fail), key metrics (total billed, partners, end users, revenue breakdown), Audit & Controls table, Executive Summary by partner, and download buttons.
- **Run History** — table of all past runs with details. Select any run to view its audit and summary.
- **Watcher Status** — is the watcher running, which files have been processed, and recent log output.

## Accrual Engine (CLI)

Month-end revenue accrual using **prior-month actual usage** re-priced with current rules. Outputs a NetSuite **journal entry** CSV (not invoices). Run from the command line; not part of the Streamlit portal.

### Accrual run (estimate for a given month)

Uses the latest usage CSV in the v3 folder whose date prefix is before the accrual month (e.g. for February accrual, uses January or December file). Overrides `FOR_MONTH` to the accrual month and runs the billing engine to produce estimated totals, then writes:

- **JE CSV** — NetSuite journal entry import (AR debit 11140, revenue credits 40113; usage, next-day, minimums by product).
- **Accrual totals CSV** — Per-customer totals for variance reporting.

```bash
python3 accrual_engine.py accrual \
  --accrual-month 2026-02 \
  --rules-path "/path/to/gep_billing_rules.xlsx"
```

Outputs default to the Google Drive `billing_engine_test/outputs/gep_accrual/` folder (same root as billing outputs). Override with `--output-dir` if needed.

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