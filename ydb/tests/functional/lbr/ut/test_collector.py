import json
import os
from pathlib import Path
import signal
import subprocess
import time
import threading

import pytest

from ydb.tests.functional.lbr.lib.lbr import LbrCollector, parse_numeric_parameter, validate_training_settings


@pytest.fixture
def fake_perf(tmp_path):
    executable = tmp_path / "perf"
    executable.write_text('''#!/bin/sh
output=
while [ "$#" -gt 0 ]; do
    if [ "$1" = -o ]; then shift; output=$1; fi
    if [ "$1" = -- ]; then shift; break; fi
    shift
done
printf 'fake perf data' > "$output"
echo "$$" > "$output.pid"
exec "$@"
''')
    executable.chmod(0o755)
    return str(executable)


def manifest(directory):
    return json.loads((directory / "manifest.json").read_text())


class TestLbrCollector:
    def test_workload_snapshots_are_after_warmup(self, tmp_path, fake_perf, monkeypatch):
        output = tmp_path / "workload"
        collector = LbrCollector(
            [os.getpid()], output, 0.8, 0.03, 3, fake_perf, start_delay_seconds=0.1, phase="workload"
        )
        ready = threading.Event()
        original_record = collector._record

        def record(index, offset):
            original_record(index, offset)
            if index == 3:
                ready.set()

        monkeypatch.setattr(collector, "_record", record)
        with collector:
            assert ready.wait(5)
        data = manifest(output)
        assert data["schedule_seconds"] == pytest.approx([0.3, 0.5, 0.7])
        assert len(data["records"]) == 3
        assert all(record["complete"] and not record["stopped_on_phase_end"] for record in data["records"])
        command = data["records"][0]["command"]
        assert command[command.index("-p") + 1] == str(os.getpid())
        assert command[command.index("-e") + 1] == "cycles:u"
        assert "-b" in command and "-a" not in command

    def test_short_preparation_phase_flushes_partial_snapshot(self, tmp_path, fake_perf):
        output = tmp_path / "compaction"
        with LbrCollector([os.getpid()], output, None, 5, perf=fake_perf, interval_seconds=6, phase="compaction"):
            time.sleep(0.1)
        data = manifest(output)
        assert data["phase"] == "compaction" and data["periodic"]
        assert not data["workload_failed"] and data["error"] is None
        assert len(data["records"]) == 1
        record = data["records"][0]
        assert record["complete"] and record["stopped_on_phase_end"]
        assert record["actual_seconds"] < record["requested_seconds"]
        pid = int((output / "snap1.perf.data.pid").read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

    def test_perf_sigterm_after_phase_end_is_valid(self, tmp_path, fake_perf):
        executable = Path(fake_perf)
        executable.write_text(
            executable.read_text().replace(
                'exec "$@"',
                'trap \'kill -TERM "$child"; wait "$child"; kill -TERM $$\' INT\n"$@" &\nchild=$!\nwait "$child"',
            )
        )
        output = tmp_path / "compaction"
        with LbrCollector([os.getpid()], output, None, 5, perf=fake_perf, interval_seconds=6):
            time.sleep(0.1)
        record = manifest(output)["records"][0]
        assert record["return_code"] == -signal.SIGTERM
        assert record["complete"] and record["stopped_on_phase_end"]

    def test_import_is_sampled_repeatedly(self, tmp_path, fake_perf, monkeypatch):
        output = tmp_path / "import"
        collector = LbrCollector([os.getpid()], output, None, 0.03, perf=fake_perf, interval_seconds=0.2)
        ready = threading.Event()
        original_record = collector._record

        def record(index, offset):
            original_record(index, offset)
            if index >= 3:
                ready.set()

        monkeypatch.setattr(collector, "_record", record)
        with collector:
            assert ready.wait(5)
        records = manifest(output)["records"]
        assert len(records) >= 3
        assert all(record["complete"] for record in records)
        assert all(a["finished_at"] <= b["started_at"] for a, b in zip(records, records[1:]))

    def test_phases_write_separate_records(self, tmp_path, fake_perf):
        for phase in ("import", "compaction"):
            with LbrCollector(
                [os.getpid()], tmp_path / phase, None, 5, perf=fake_perf, interval_seconds=6, phase=phase
            ):
                time.sleep(0.1)
        for phase in ("import", "compaction"):
            data = manifest(tmp_path / phase)
            assert data["phase"] == phase
            assert (tmp_path / phase / "snap1.perf.data").is_file()
            assert len(data["records"]) == 1

    def test_incomplete_workload_is_rejected(self, tmp_path, fake_perf):
        output = tmp_path / "workload"
        with pytest.raises(RuntimeError, match="before all"):
            with LbrCollector([os.getpid()], output, 10, 1, 1, fake_perf):
                pass
        assert not manifest(output)["records"]

    def test_phase_failure_is_preserved(self, tmp_path, fake_perf):
        output = tmp_path / "import"
        with pytest.raises(ValueError, match="import failed"):
            with LbrCollector([os.getpid()], output, None, 5, perf=fake_perf, interval_seconds=6):
                time.sleep(0.1)
                raise ValueError("import failed")
        data = manifest(output)
        assert data["workload_failed"] and not data["records"][0]["complete"]

    def test_perf_failure_fails_successful_phase(self, tmp_path, fake_perf):
        Path(fake_perf).write_text("#!/bin/sh\nexit 7\n")
        with pytest.raises(RuntimeError, match="collection failed"):
            with LbrCollector([os.getpid()], tmp_path / "import", None, 0.1, perf=fake_perf, interval_seconds=0.2):
                time.sleep(0.1)

    def test_missing_perf_is_reported_on_entry(self, tmp_path):
        output = tmp_path / "import"
        with pytest.raises(FileNotFoundError):
            with LbrCollector([os.getpid()], output, None, 0.1, perf=str(tmp_path / "missing"), interval_seconds=0.2):
                pytest.fail("Phase must not start without perf")
        assert manifest(output)["error"]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"window_seconds": 30, "record_seconds": 30, "snapshots": 3},
            {"window_seconds": None, "record_seconds": 30, "interval_seconds": 10},
            {"window_seconds": 30, "start_delay_seconds": -1},
        ],
    )
    def test_invalid_schedule(self, tmp_path, kwargs):
        with pytest.raises(ValueError):
            LbrCollector([os.getpid()], tmp_path / "out", **kwargs)


def test_closed_snapshot_is_queued_before_phase_ends(tmp_path, fake_perf):
    ready = threading.Event()
    calls = []

    class Conversion:
        def submit(self, phase, index, data):
            assert data.read_bytes() == b"fake perf data"
            pid = int(Path(str(data) + ".pid").read_text())
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
            calls.append((phase, index))
            ready.set()

        def finish_phase(self, phase, valid):
            calls.append((phase, valid))

    with LbrCollector(
        [os.getpid()],
        tmp_path / "import",
        None,
        0.03,
        perf=fake_perf,
        interval_seconds=10,
        phase="import",
        conversion=Conversion(),
    ):
        assert ready.wait(5)
        assert calls == [("import", 1)]
    assert calls == [("import", 1), ("import", True)]


def test_incomplete_workload_is_not_valid_for_conversion(tmp_path, fake_perf):
    calls = []

    class Conversion:
        def submit(self, phase, index, data):
            pytest.fail("Unfinished snapshot must not be submitted")

        def finish_phase(self, phase, valid):
            calls.append((phase, valid))

    with pytest.raises(RuntimeError, match="before all"):
        with LbrCollector(
            [os.getpid()], tmp_path / "workload", 10, 1, 1, fake_perf, phase="workload", conversion=Conversion()
        ):
            pass
    assert calls == [("workload", False)]
    assert not manifest(tmp_path / "workload")["valid"]


@pytest.mark.parametrize("executable", [False, True])
def test_windowed_perf_is_checked_before_start(tmp_path, executable):
    perf = tmp_path / "missing-or-not-executable-perf"
    if executable:
        perf.write_text("#!/bin/sh\nexit 0\n")
        perf.chmod(0o644)
    collector = LbrCollector([os.getpid()], tmp_path / "workload", 1800, perf=str(perf), start_delay_seconds=600)
    with pytest.raises(FileNotFoundError, match="perf executable is missing or not executable"):
        with collector:
            pytest.fail("Invalid perf must fail before the workload starts")
    assert collector._thread is None


def test_startup_timeout_uses_bounded_join(tmp_path, fake_perf, monkeypatch):
    joins = []

    class StuckThread:
        def __init__(self, **kwargs):
            assert kwargs["daemon"]

        def start(self):
            pass

        def join(self, timeout=None):
            joins.append(timeout)

        def is_alive(self):
            return False

    collector = LbrCollector([os.getpid()], tmp_path / "import", None, perf=fake_perf)
    monkeypatch.setattr(threading, "Thread", StuckThread)
    monkeypatch.setattr(collector._first_attempt, "wait", lambda timeout: False)
    with pytest.raises(RuntimeError, match="Timed out starting LBR"):
        with collector:
            pytest.fail("Startup timeout must prevent the phase from starting")
    assert joins == [75]
    assert collector._stop.is_set()
    assert "Timed out starting" in manifest(tmp_path / "import")["error"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"snapshots": 0},
        {"record_seconds": 600},
        {"interval_seconds": 10},
        {"window_seconds": float("nan")},
        {"record_seconds": float("inf")},
        {"start_delay_seconds": -1},
        {"mmap_pages": 0},
    ],
)
def test_training_settings_reject_invalid_inputs(monkeypatch, overrides):
    monkeypatch.delenv("YDB_USE_IN_MEMORY_PDISKS", raising=False)
    monkeypatch.delenv("YDB_PDISKS_DIRECTORY", raising=False)
    settings = dict(
        window_seconds=1800, record_seconds=30, snapshots=3, start_delay_seconds=600, interval_seconds=60, mmap_pages=64
    )
    settings.update(overrides)
    with pytest.raises(ValueError):
        validate_training_settings(**settings)


@pytest.mark.parametrize(
    "variable,value,rejected",
    [
        ("YDB_USE_IN_MEMORY_PDISKS", "true", True),
        ("YDB_USE_IN_MEMORY_PDISKS", "false", False),
        ("YDB_PDISKS_DIRECTORY", "/another/disk", True),
        ("YDB_PDISKS_DIRECTORY", "", False),
    ],
)
def test_training_settings_check_storage_environment(monkeypatch, variable, value, rejected):
    monkeypatch.delenv("YDB_USE_IN_MEMORY_PDISKS", raising=False)
    monkeypatch.delenv("YDB_PDISKS_DIRECTORY", raising=False)
    monkeypatch.setenv(variable, value)
    settings = dict(
        window_seconds=1800, record_seconds=30, snapshots=3, start_delay_seconds=600, interval_seconds=60, mmap_pages=64
    )
    if rejected:
        with pytest.raises(ValueError, match=variable):
            validate_training_settings(**settings)
    else:
        validate_training_settings(**settings)


@pytest.mark.parametrize("late_error", [False, True])
def test_late_startup_worker_writes_final_manifest(tmp_path, fake_perf, monkeypatch, late_error):
    entered = threading.Event()
    release = threading.Event()
    real_thread = threading.Thread
    workers = []

    class DelayedThread:
        def __init__(self, **kwargs):
            self.worker = real_thread(**kwargs)
            workers.append(self.worker)

        def start(self):
            self.worker.start()

        def join(self, timeout=None):
            assert timeout == 75
            # Simulate a worker that is still in Popen after the bounded join.

        def is_alive(self):
            return self.worker.is_alive()

    collector = LbrCollector([os.getpid()], tmp_path / "import", None, perf=fake_perf)
    original_record = collector._record

    def delayed_record(index, offset):
        entered.set()
        assert release.wait(5)
        if late_error:
            raise OSError("late perf exec failure")
        original_record(index, offset)

    def startup_timeout(timeout):
        assert entered.wait(5)
        return False

    monkeypatch.setattr(threading, "Thread", DelayedThread)
    monkeypatch.setattr(collector, "_record", delayed_record)
    monkeypatch.setattr(collector._first_attempt, "wait", startup_timeout)
    try:
        with pytest.raises(RuntimeError, match="Timed out starting LBR"):
            collector.__enter__()
        assert not (collector.output_dir / "manifest.json").exists()
    finally:
        release.set()
        for worker in workers:
            worker.join(timeout=5)
    assert all(not worker.is_alive() for worker in workers)
    data = manifest(collector.output_dir)
    assert "Timed out starting LBR" in data["startup_error"]
    if late_error:
        assert data["error"] == "late perf exec failure"
        assert not data["records"]
    else:
        assert "Timed out starting LBR" in data["error"]
        assert len(data["records"]) == 1
        assert "finished_at" in data["records"][0]
        assert not data["records"][0]["complete"]


def test_unkillable_perf_does_not_block_exit(tmp_path, fake_perf, monkeypatch):
    waits = []
    signals = []
    submitted = []

    class StuckProcess:
        pid = 999999
        returncode = None

        def __init__(self, *args, **kwargs):
            pass

        def poll(self):
            return None

        def wait(self, timeout=None):
            assert timeout is not None, "Unbounded wait for perf"
            waits.append(timeout)
            raise subprocess.TimeoutExpired("perf", timeout)

    class Conversion:
        def submit(self, *args):
            pytest.fail("Unreaped snapshot must not be converted")

        def finish_phase(self, phase, valid):
            submitted.append((phase, valid))

    monkeypatch.setattr(subprocess, "Popen", StuckProcess)
    monkeypatch.setattr(os, "killpg", lambda pid, sig: signals.append(sig))
    collector = LbrCollector(
        [os.getpid()],
        tmp_path / "import",
        None,
        perf=fake_perf,
        phase="import",
        conversion=Conversion(),
    )
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="collection failed"):
        with collector:
            pass
    assert time.monotonic() - started < 5
    assert waits == [60, 10]
    assert signals == [signal.SIGINT, signal.SIGKILL]
    assert submitted == [("import", False)]
    record = manifest(collector.output_dir)["records"][0]
    assert record["reap_timed_out"] and not record["complete"]
    assert record["return_code"] is None


def test_exit_does_not_wait_forever_for_collector_thread(tmp_path, monkeypatch):
    joins = []

    class StuckThread:
        def join(self, timeout=None):
            joins.append(timeout)

        def is_alive(self):
            return True

    collector = LbrCollector([os.getpid()], tmp_path / "import", None)
    collector._thread = StuckThread()
    monkeypatch.setattr(collector, "_write_manifest", lambda: pytest.fail("Worker still owns records"))
    with pytest.raises(TimeoutError, match="Timed out stopping LBR collector"):
        collector.__exit__(None, None, None)
    assert joins == [75]
    assert collector._entry_failed.is_set()
    assert collector._phase_failed and not collector.valid


@pytest.mark.parametrize("converter,value,expected", [(int, "42", 42), (float, "1.5", 1.5)])
def test_numeric_parameter_parsing(converter, value, expected):
    assert parse_numeric_parameter("tpc-lbr-test", value, converter) == expected


@pytest.mark.parametrize(
    "converter,value", [(int, "abc"), (int, "1.5"), (float, "abc"), (float, "nan"), (float, "inf"), (float, "-inf")]
)
def test_numeric_parameter_error_names_parameter(converter, value):
    with pytest.raises(ValueError, match="tpc-lbr-test") as error:
        parse_numeric_parameter("tpc-lbr-test", value, converter)
    assert repr(value) in str(error.value)


@pytest.mark.parametrize("exit_code", [0, -signal.SIGINT, 128 + signal.SIGINT])
def test_interrupted_workload_reports_incomplete_schedule(tmp_path, fake_perf, monkeypatch, exit_code):
    class InterruptedProcess:
        pid = 999999
        returncode = None

        def __init__(self, command, **kwargs):
            Path(command[command.index("-o") + 1]).write_bytes(b"partial data")

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.returncode = exit_code
            return exit_code

    monkeypatch.setattr(subprocess, "Popen", InterruptedProcess)
    monkeypatch.setattr(os, "killpg", lambda *args: None)
    collector = LbrCollector([os.getpid()], tmp_path / "workload", 10, record_seconds=1, snapshots=1, perf=fake_perf)
    collector.output_dir.mkdir()
    collector.started = time.monotonic()
    collector.started_at = time.time()
    collector._stop.set()
    with pytest.raises(RuntimeError, match="Workload ended before all LBR snapshots completed"):
        collector._record(1, 1)
    assert not collector.records[0]["complete"]


def test_unstarted_phase_writes_manifest_and_preserves_body_error(tmp_path):
    calls = []

    class Conversion:
        def finish_phase(self, phase, valid):
            calls.append((phase, valid))

    collector = LbrCollector([os.getpid()], tmp_path / "workload", 1800, phase="workload", conversion=Conversion())
    original = ValueError("save_nodes_state failed")
    with pytest.raises(ValueError) as caught:
        with collector.ensure_started():
            raise original
    assert caught.value is original
    data = manifest(collector.output_dir)
    assert data["started_at"] is None
    assert data["error"] == "LBR phase was not started"
    assert data["workload_failed"] and not data["valid"] and not data["records"]
    assert calls == [("workload", False)]


def test_successful_body_without_collection_is_rejected(tmp_path):
    collector = LbrCollector([os.getpid()], tmp_path / "workload", 1800)
    with pytest.raises(RuntimeError, match="LBR phase was not started"):
        with collector.ensure_started():
            pass
    assert not manifest(collector.output_dir)["valid"]


def test_started_phase_guard_does_not_overwrite_manifest(tmp_path, fake_perf):
    collector = LbrCollector([os.getpid()], tmp_path / "import", None, perf=fake_perf)
    with collector.ensure_started():
        with collector:
            time.sleep(0.1)
    assert manifest(collector.output_dir)["valid"]
