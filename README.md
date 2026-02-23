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