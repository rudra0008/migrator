# Email Migration Web App (Flask + imapsync)

This project provides a web UI for email migrations using `imapsync`.

## Features
- **Single migration:** manually enter source + destination credentials.
- **Batch migration:** upload a CSV with exact header:
  - `sourceserver,sourceuser,sourcepassword,destinationserver,destinationuser,destinationpassword`
- Parallel migration execution using a thread pool.
- Job tracking (`queued`, `running`, `success`, `failed`, `canceled`) with error details.
- Cancel an in-progress or queued migration from the UI.
- View per-job migration remaining messages (`msgs left`) in the status table.
- View full console logs for each migration job via the **Logs** action (auto-refreshes while open).

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open http://localhost:5000

## Notes
- `imapsync` must be installed and available on your system PATH.
- Default parallel jobs: 4. Override using `MAX_PARALLEL_MIGRATIONS`.
- `Msgs Left` is parsed only from lines containing both `ETA` and `msgs left` (e.g. `326/327 msgs left`).
