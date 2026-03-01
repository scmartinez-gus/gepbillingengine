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

A background service that monitors the v3 usage queries folder for new `YYYY.MM_*.csv` files. When a new file appears and is confirmed stable (Google Drive sync finished), the billing engine runs automatically. The same folder is used for accruals — on the 25th of each month, the watcher grabs the prior month's file and generates the accrual JE.

### Start the watcher

```bash
python3 billing_watcher.py
```

This watches the v3 query exports folder every 30 minutes. To customize:

```bash
python3 billing_watcher.py \
  --usage-dir "/path/to/usage/csvs" \
  --outputs-dir "/path/to/outputs" \
  --poll-interval 900 \
  --slack-webhook "https://hooks.slack.com/services/..."
```

### How it works

1. Every N seconds (default 1800 = 30 min), the watcher scans the usage directory for `YYYY.MM_*.csv` files.
2. New files are held for one cycle to confirm the file size is stable (Google Drive sync is finished).
3. Once stable, the file is copied alongside the rules workbook into a temp directory and the billing engine runs. Outputs are written to the outputs folder (Master Report, NetSuite CSV, partner details).
4. The file is recorded in a local ledger (`outputs/watcher_state/processed_files.json`) so it won't be processed again.
5. If a Slack webhook is configured, a notification is sent with run status and audit outcome.
6. On the 25th of each month, the watcher auto-runs the accrual engine using the prior month's file from the same folder, generating the JE support workbook, totals, and billing detail.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--usage-dir` | v3 query exports | Directory to watch for `YYYY.MM_*.csv` usage files |
| `--rules-dir` | Google Drive inputs | Directory containing `gep_billing_rules.xlsx` |
| `--outputs-dir` | Google Drive outputs | Output directory for billing results |
| `--poll-interval` | 1800 (30 min) | Seconds between folder checks |
| `--slack-webhook` | `$BILLING_SLACK_WEBHOOK` | Slack webhook URL for notifications |
| `--dry-run` | off | Detect files but don't run billing |
| `--reset-ledger` | off | Clear the processed-files ledger and start fresh |
| `--accrual-day` | 25 | Day of month to auto-run accruals |
| `--accrual-output-dir` | `outputs/gep_accrual` | Output directory for accrual JE and totals |
| `--disable-accrual` | off | Disable automatic accrual scheduling |
| `--log-level` | INFO | DEBUG, INFO, WARNING, or ERROR |

### Stop the watcher

Press `Ctrl+C` — it will finish the current cycle and exit cleanly.

## Billing Dashboard (monitoring)

Read-only dashboard for reviewing billing results. No uploads, no manual triggers — it reads directly from the outputs the watcher produces.

### Auto-start (launchd service)

The dashboard can run as a persistent macOS service at `http://localhost:8502`:

```bash
cp com.gep.billing-dashboard.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.gep.billing-dashboard.plist
```

To stop: `launchctl unload ~/Library/LaunchAgents/com.gep.billing-dashboard.plist`

### Manual start

```bash
python3 -m streamlit run billing_dashboard.py
```

### Tabs

- **Latest Run** — status banner (pass/fail), key metrics (total billed, partners, end users, revenue breakdown), Audit & Controls table, Executive Summary by partner, and download buttons.
- **Accruals** — accrual totals, journal entry preview, variance report (accrual vs actual), and downloads.
- **Run History** — table of all past runs with details. Select any run to view its audit and summary.
- **Watcher Status** — service health for both watcher and dashboard, processed files, automated accrual history, and recent log output.

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
  --accrual-totals "/path/to/outputs/gep_accrual/gep_accrual_totals_202601.csv" \
  --actual-master-report "/path/to/outputs/gep_billing_log/2026.01_Master_Billing_Report.xlsx" \
  --output "/path/to/outputs/gep_accrual/variance_202601.csv"
```

Optional: `--materiality-pct` (default 5).