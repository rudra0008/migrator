from __future__ import annotations

import csv
import io
import os
import subprocess
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from flask import Flask, jsonify, render_template, request

CSV_HEADERS = [
    "sourceserver",
    "sourceuser",
    "sourcepassword",
    "destinationserver",
    "destinationuser",
    "destinationpassword",
]


@dataclass
class MigrationJob:
    id: str
    batch_id: str
    source_server: str
    source_user: str
    source_password: str
    destination_server: str
    destination_user: str
    destination_password: str
    status: str = "queued"
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    output: str = ""


class MigrationManager:
    def __init__(self, max_workers: int = 4) -> None:
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.jobs: Dict[str, MigrationJob] = {}
        self.futures: Dict[str, Future] = {}
        self.processes: Dict[str, subprocess.Popen] = {}
        self.lock = threading.Lock()

    def submit_jobs(self, rows: List[dict]) -> str:
        batch_id = str(uuid.uuid4())
        for row in rows:
            self.submit_single(
                source_server=row["sourceserver"],
                source_user=row["sourceuser"],
                source_password=row["sourcepassword"],
                destination_server=row["destinationserver"],
                destination_user=row["destinationuser"],
                destination_password=row["destinationpassword"],
                batch_id=batch_id,
            )
        return batch_id

    def submit_single(
        self,
        source_server: str,
        source_user: str,
        source_password: str,
        destination_server: str,
        destination_user: str,
        destination_password: str,
        batch_id: str | None = None,
    ) -> str:
        job = MigrationJob(
            id=str(uuid.uuid4()),
            batch_id=batch_id or str(uuid.uuid4()),
            source_server=source_server,
            source_user=source_user,
            source_password=source_password,
            destination_server=destination_server,
            destination_user=destination_user,
            destination_password=destination_password,
        )
        with self.lock:
            self.jobs[job.id] = job
            self.futures[job.id] = self.executor.submit(self._run_migration, job.id)
        return job.id

    def cancel_job(self, job_id: str) -> tuple[bool, str]:
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return False, "Job not found."
            if job.status in {"success", "failed", "canceled"}:
                return False, f"Job already finished with status '{job.status}'."

            if job.status == "queued":
                future = self.futures.get(job_id)
                if future and future.cancel():
                    job.status = "canceled"
                    job.finished_at = datetime.utcnow().isoformat()
                    job.error = "Migration canceled before start."
                    return True, "Queued migration canceled."
                return False, "Unable to cancel queued job (it may have started)."

            process = self.processes.get(job_id)
            if process and process.poll() is None:
                process.terminate()
                job.status = "canceled"
                job.error = "Migration canceled by user."
                job.finished_at = datetime.utcnow().isoformat()
                return True, "Running migration cancellation requested."

            return False, "Unable to cancel running job."

    def _run_migration(self, job_id: str) -> None:
        with self.lock:
            job = self.jobs[job_id]
            if job.status == "canceled":
                return
            job.status = "running"
            job.started_at = datetime.utcnow().isoformat()

        command = [
            "imapsync",
            "--host1",
            job.source_server,
            "--user1",
            job.source_user,
            "--password1",
            job.source_password,
            "--host2",
            job.destination_server,
            "--user2",
            job.destination_user,
            "--password2",
            job.destination_password,
            "--automap",
        ]

        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            with self.lock:
                self.processes[job_id] = process

            stdout, stderr = process.communicate()
            output = (stdout or "") + ("\n" + stderr if stderr else "")

            with self.lock:
                job.output = output.strip()
                if job.status == "canceled":
                    pass
                elif process.returncode == 0:
                    job.status = "success"
                elif process.returncode == -15:
                    job.status = "canceled"
                    job.error = job.error or "Migration canceled by user."
                else:
                    job.status = "failed"
                    job.error = f"imapsync exited with code {process.returncode}"
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                if job.status != "canceled":
                    job.status = "failed"
                    job.error = str(exc)
        finally:
            with self.lock:
                job.finished_at = job.finished_at or datetime.utcnow().isoformat()
                self.processes.pop(job_id, None)

    def all_jobs(self) -> List[MigrationJob]:
        with self.lock:
            return sorted(self.jobs.values(), key=lambda j: j.started_at or "", reverse=True)


def parse_csv(file_storage) -> List[dict]:
    content = file_storage.stream.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))

    if reader.fieldnames != CSV_HEADERS:
        raise ValueError("CSV header must be: sourceserver,sourceuser,sourcepassword,destinationserver,destinationuser,destinationpassword")

    rows: List[dict] = []
    for row in reader:
        cleaned = {k: (v.strip() if v else "") for k, v in row.items()}
        if not all(cleaned.get(col) for col in CSV_HEADERS):
            continue
        rows.append(cleaned)

    if not rows:
        raise ValueError("CSV file does not contain valid migration rows.")
    return rows


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024

    manager = MigrationManager(max_workers=int(os.getenv("MAX_PARALLEL_MIGRATIONS", "4")))

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.post("/upload")
    def upload_csv():
        csv_file = request.files.get("csv_file")
        if not csv_file or csv_file.filename == "":
            return jsonify({"error": "Please upload a CSV file."}), 400

        try:
            rows = parse_csv(csv_file)
            batch_id = manager.submit_jobs(rows)
            return jsonify({"message": "Migration jobs queued", "batch_id": batch_id, "jobs": len(rows)})
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    @app.post("/single")
    def single_migration():
        payload = {
            "source_server": request.form.get("source_server", "").strip(),
            "source_user": request.form.get("source_user", "").strip(),
            "source_password": request.form.get("source_password", "").strip(),
            "destination_server": request.form.get("destination_server", "").strip(),
            "destination_user": request.form.get("destination_user", "").strip(),
            "destination_password": request.form.get("destination_password", "").strip(),
        }

        if not all(payload.values()):
            return jsonify({"error": "All single migration fields are required."}), 400

        job_id = manager.submit_single(**payload)
        return jsonify({"message": "Single migration queued", "job_id": job_id})

    @app.post("/jobs/<job_id>/cancel")
    def cancel_job(job_id: str):
        canceled, message = manager.cancel_job(job_id)
        status_code = 200 if canceled else 400
        return jsonify({"message": message}), status_code

    @app.get("/api/jobs")
    def jobs_api():
        return jsonify([job.__dict__ for job in manager.all_jobs()])

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000, debug=True)
