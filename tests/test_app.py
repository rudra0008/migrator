import io
import uuid
from concurrent.futures import Future

from app import CSV_HEADERS, MigrationJob, MigrationManager, parse_csv


class DummyFile:
    def __init__(self, content: str):
        self.stream = io.BytesIO(content.encode("utf-8"))


def test_parse_csv_success():
    file = DummyFile(
        "sourceserver,sourceuser,sourcepassword,destinationserver,destinationuser,destinationpassword\n"
        "source.a,user1@example.com,pw1,dest.a,user1@new.com,pw2\n"
    )
    rows = parse_csv(file)
    assert rows[0]["sourceserver"] == "source.a"


def test_parse_csv_invalid_header():
    file = DummyFile("Incoming Server,E-mailadress,Password\na,b,c\n")
    try:
        parse_csv(file)
    except ValueError as exc:
        assert "CSV header" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_submit_jobs_registers_jobs():
    manager = MigrationManager(max_workers=1)
    rows = [
        {
            "sourceserver": "source.host",
            "sourceuser": "source@example.com",
            "sourcepassword": "pw",
            "destinationserver": "dest.host",
            "destinationuser": "dest@example.com",
            "destinationpassword": "pw2",
        }
    ]
    batch_id = manager.submit_jobs(rows)
    assert batch_id
    assert len(manager.jobs) == 1


def test_submit_single_registers_job():
    manager = MigrationManager(max_workers=1)
    job_id = manager.submit_single(
        source_server="s1",
        source_user="u1",
        source_password="p1",
        destination_server="d1",
        destination_user="u2",
        destination_password="p2",
    )
    assert job_id
    assert len(manager.jobs) == 1


def test_cancel_queued_job_sets_canceled_status():
    manager = MigrationManager(max_workers=1)
    job_id = str(uuid.uuid4())
    manager.jobs[job_id] = MigrationJob(
        id=job_id,
        batch_id=str(uuid.uuid4()),
        source_server="s1",
        source_user="u1",
        source_password="p1",
        destination_server="d1",
        destination_user="u2",
        destination_password="p2",
    )
    manager.futures[job_id] = Future()

    canceled, _ = manager.cancel_job(job_id)

    assert canceled is True
    assert manager.jobs[job_id].status == "canceled"


def test_extract_msgs_left_eta_line():
    assert MigrationManager._extract_msgs_left("... ETA: time ... 326/327 msgs left") == "326/327"
    assert MigrationManager._extract_msgs_left("... ETA: time ... 1,226/3,327 msgs left") == "1226/3327"
    assert MigrationManager._extract_msgs_left("326/327 msgs left") is None
    assert MigrationManager._extract_msgs_left("ETA: soon but no mailbox counters") is None


def test_get_job_logs_not_found():
    manager = MigrationManager(max_workers=1)
    ok, message = manager.get_job_logs("missing")
    assert ok is False
    assert message == "Job not found."


def test_expected_headers_constant():
    assert CSV_HEADERS == [
        "sourceserver",
        "sourceuser",
        "sourcepassword",
        "destinationserver",
        "destinationuser",
        "destinationpassword",
    ]
