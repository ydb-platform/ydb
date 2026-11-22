from concurrent.futures import CancelledError, Future
import json
import subprocess
import threading
import time
from pathlib import Path

import pytest

from ydb.tests.functional.lbr.lib.conversion import LbrConversion


def fake_result(path):
    return {"profile": str(path), "profile_bytes": 1}


def test_every_ready_snapshot_starts_without_waiting(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    entered = threading.Barrier(4)
    release = threading.Event()

    def convert(data, prefix):
        entered.wait(timeout=5)
        assert release.wait(5)
        return fake_result(prefix.with_suffix(".prof"))

    monkeypatch.setattr(conversion, "_convert", convert)
    try:
        for index in range(1, 4):
            conversion.submit("workload", index, tmp_path / str(index))
        entered.wait(timeout=5)
        assert len(conversion.threads) == 3
        assert all(thread.is_alive() for thread in conversion.threads)
    finally:
        release.set()
        conversion.abort()


def test_only_successful_phase_is_merged(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    monkeypatch.setattr(conversion, "_convert", lambda data, prefix: fake_result(prefix.with_suffix(".prof")))
    merged = []

    def merge(command, prefix):
        merged.append(command)
        Path(command[command.index("-o") + 1]).write_bytes(b"merged")

    monkeypatch.setattr(conversion, "_run", merge)
    conversion.submit("import", 1, tmp_path / "first")
    conversion.submit("workload", 1, tmp_path / "second")
    conversion.finish_phase("import", True)
    conversion.finish_phase("workload", False)
    conversion.finish()
    assert len(merged) == 1
    assert (tmp_path / "profiles/import.prof").read_bytes() == b"merged"
    assert not (tmp_path / "profiles/workload.prof").exists()
    report = json.loads((tmp_path / "profiles/conversion.json").read_text())
    assert not next(row for row in report if row["phase"] == "workload")["phase_valid"]


def test_conversion_failure_rejects_valid_phase(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")

    def fail(data, prefix):
        raise RuntimeError("profgen failed")

    monkeypatch.setattr(conversion, "_convert", fail)
    conversion.submit("import", 1, tmp_path / "data")
    conversion.finish_phase("import", True)
    with pytest.raises(RuntimeError, match="LBR conversion failed"):
        conversion.finish()
    assert not (tmp_path / "profiles/import.prof").exists()
    assert "profgen failed" in (tmp_path / "profiles/conversion.json").read_text()


def test_raw_lbr_filter_and_atomic_profile(tmp_path):
    binary = tmp_path / "ydbd"
    binary.touch()
    perf = tmp_path / "perf"
    perf.write_text(
        f"#!/bin/sh\nprintf 'PERF_RECORD_MMAP2 1/1: [0x1000(0x1000) @ 0 00:00 0 0]: r-xp {binary}\\n0x123/0x456\\nunrelated\\n'\n"
    )
    perf.chmod(0o755)
    llvm = tmp_path / "llvm"
    llvm.mkdir()
    profgen = llvm / "llvm-profgen"
    profgen.write_text(
        "#!/bin/sh\nfor arg; do case $arg in --output=*) printf profile > \"${arg#--output=}\";; esac; done\n"
    )
    profgen.chmod(0o755)
    profdata = llvm / "llvm-profdata"
    profdata.write_text('#!/bin/sh\nwhile [ "$1" != -o ]; do shift; done\nshift\nprintf merged > "$1"\n')
    profdata.chmod(0o755)
    conversion = LbrConversion(binary, tmp_path / "profiles", llvm, str(perf))
    conversion.submit("workload", 1, tmp_path / "ready.perf.data")
    conversion.finish_phase("workload", True)
    conversion.finish()
    assert (
        tmp_path / "profiles/workload/snap1.lbr"
    ).read_text() == f"PERF_RECORD_MMAP2 1/1: [0x1000(0x1000) @ 0 00:00 0 0]: r-xp {binary}\n0x123/0x456\n"
    assert (tmp_path / "profiles/workload/snap1.prof").read_bytes() == b"profile"
    assert not list((tmp_path / "profiles").rglob("*.tmp"))
    report = json.loads((tmp_path / "profiles/conversion.json").read_text())[0]
    assert report["samples"] == 1 and report["peak_rss_kib"] is not None


def test_bad_snapshot_does_not_prevent_other_phase_merge(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")

    def convert(data, prefix):
        if data.name == "bad":
            raise RuntimeError("bad snapshot")
        return fake_result(prefix.with_suffix(".prof"))

    def merge(command, prefix):
        Path(command[command.index("-o") + 1]).write_bytes(b"merged")

    monkeypatch.setattr(conversion, "_convert", convert)
    monkeypatch.setattr(conversion, "_run", merge)
    conversion.submit("import", 1, tmp_path / "good")
    conversion.submit("workload", 1, tmp_path / "good")
    conversion.submit("workload", 2, tmp_path / "bad")
    conversion.finish_phase("import", True)
    conversion.finish_phase("workload", True)
    with pytest.raises(RuntimeError, match="LBR conversion failed"):
        conversion.finish()
    assert (tmp_path / "profiles/import.prof").read_bytes() == b"merged"
    assert not (tmp_path / "profiles/workload.prof").exists()


def test_merge_error_does_not_prevent_other_phase_merge(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    monkeypatch.setattr(conversion, "_convert", lambda data, prefix: fake_result(prefix.with_suffix(".prof")))

    def merge(command, prefix):
        if prefix.name == "merge-import":
            raise RuntimeError("merge failed")
        Path(command[command.index("-o") + 1]).write_bytes(b"merged")

    monkeypatch.setattr(conversion, "_run", merge)
    for phase in ("import", "workload"):
        conversion.submit(phase, 1, tmp_path / "data")
        conversion.finish_phase(phase, True)
    with pytest.raises(RuntimeError, match="LBR conversion failed"):
        conversion.finish()
    assert (tmp_path / "profiles/workload.prof").is_file()
    report = json.loads((tmp_path / "profiles/conversion.json").read_text())
    assert next(row for row in report if "merge_error" in row)["index"] is None
    assert "merge failed" in (tmp_path / "profiles/conversion.json").read_text()


@pytest.mark.parametrize("resolved_mapping", [False, True])
def test_symlink_binary_mapping(tmp_path, monkeypatch, resolved_mapping):
    binary = tmp_path / "server"
    binary.touch()
    link = tmp_path / "ydbd"
    link.symlink_to(binary)
    mapped = binary if resolved_mapping else link
    perf = tmp_path / "perf"
    perf.write_text(
        f"#!/bin/sh\nprintf 'PERF_RECORD_MMAP2 1/1: [0x1000(0x1000) @ 0 00:00 0 0]: r-xp {mapped}\\n0x123/0x456\\n'\n"
    )
    perf.chmod(0o755)
    conversion = LbrConversion(link, tmp_path / "profiles", perf=str(perf))

    def profgen(command, prefix):
        output = next(arg.split("=", 1)[1] for arg in command if arg.startswith("--output="))
        Path(output).write_bytes(b"profile")
        return {}

    monkeypatch.setattr(conversion, "_run", profgen)
    result = conversion._convert(tmp_path / "data", tmp_path / "profiles/snap1")
    assert result["samples"] == 1


def test_thread_start_failure_is_reported(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")

    def fail_start(thread):
        raise RuntimeError("cannot start thread")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    conversion.submit("workload", 1, tmp_path / "data")
    conversion.finish_phase("workload", True)
    with pytest.raises(RuntimeError, match="LBR conversion failed"):
        conversion.finish()
    conversion.abort()
    assert "cannot start thread" in (tmp_path / "profiles/conversion.json").read_text()


@pytest.mark.parametrize("stage", ["perf", "profgen"])
def test_hung_process_group_is_killed(tmp_path, stage):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles", timeout=0.2)
    sleeper = tmp_path / "sleeper"
    # The child inherits stdout: killing only the shell would leave perf's pipe open.
    sleeper.write_text("#!/bin/sh\nsleep 60 &\nwait\n")
    sleeper.chmod(0o755)
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="timed out"):
        if stage == "perf":
            conversion.perf = str(sleeper)
            conversion._convert(tmp_path / "data", tmp_path / "profiles/snap1")
        else:
            conversion._run([str(sleeper)], tmp_path / "profiles/snap1")
    assert time.monotonic() - started < 5
    assert not conversion._processes


def test_abort_stops_active_conversion(tmp_path):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles", timeout=60)
    perf = tmp_path / "perf"
    perf.write_text("#!/bin/sh\nsleep 60\n")
    perf.chmod(0o755)
    conversion.perf = str(perf)
    conversion.submit("import", 1, tmp_path / "data")
    deadline = time.monotonic() + 5
    while not conversion._processes and time.monotonic() < deadline:
        time.sleep(0.01)
    assert conversion._processes
    conversion.abort()
    assert all(not thread.is_alive() for thread in conversion.threads)


def test_workload_error_survives_conversion_error(tmp_path, monkeypatch, caplog):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    workload_error = ValueError("TPC-C failed")

    def fail():
        raise RuntimeError("conversion failed")

    monkeypatch.setattr(conversion, "finish", fail)
    with pytest.raises(ValueError) as exc:
        with conversion.finish_on_exit():
            raise workload_error
    assert exc.value is workload_error
    assert "conversion failed" in caplog.text
    assert "preserving the original workload error" in caplog.text


def test_conversion_error_fails_successful_workload(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")

    def fail():
        raise RuntimeError("conversion failed")

    monkeypatch.setattr(conversion, "finish", fail)
    with pytest.raises(RuntimeError, match="conversion failed"):
        with conversion.finish_on_exit():
            pass


def test_invalid_phase_kills_only_its_processes(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles", timeout=60)
    entered = {phase: threading.Event() for phase in ("import", "workload")}
    completed = {phase: threading.Event() for phase in entered}

    def convert(data, prefix):
        phase = prefix.parent.name
        try:
            with conversion._process(["sleep", "60"], stdout=subprocess.DEVNULL) as process:
                entered[phase].set()
                process.wait(timeout=65)
        finally:
            completed[phase].set()
        return fake_result(prefix.with_suffix(".prof"))

    monkeypatch.setattr(conversion, "_convert", convert)
    try:
        for phase in entered:
            conversion.submit(phase, 1, tmp_path / "data")
        assert all(event.wait(5) for event in entered.values())
        conversion.finish_phase("import", False)
        assert completed["import"].wait(5)
        assert not completed["workload"].is_set()
        with conversion._process_lock:
            assert list(conversion._processes.values()) == ["workload"]
        future = next(future for phase, _, future in conversion.jobs if phase == "import")
        with pytest.raises(CancelledError):
            future.result(timeout=5)
    finally:
        conversion.abort()


def test_invalidated_phase_cannot_start_next_process(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    entered = threading.Event()
    resume = threading.Event()

    def convert(data, prefix):
        entered.set()
        assert resume.wait(5)
        with conversion._process(["this-must-not-be-started"]):
            pytest.fail("Cancelled phase started another process")

    monkeypatch.setattr(conversion, "_convert", convert)
    try:
        conversion.submit("import", 1, tmp_path / "data")
        assert entered.wait(5)
        conversion.finish_phase("import", False)
        resume.set()
        conversion.finish()
        report = json.loads((tmp_path / "profiles/conversion.json").read_text())
        assert report[0]["error"] == "cancelled"
    finally:
        resume.set()
        conversion.abort()


def test_pending_cancellation_has_readable_error(tmp_path):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    future = Future()
    conversion.jobs.append(("import", 1, future))
    conversion.finish_phase("import", False)
    conversion.finish()
    report = json.loads((tmp_path / "profiles/conversion.json").read_text())
    assert future.cancelled()
    assert report[0]["error"] == "cancelled"


def test_conversion_without_gnu_time(tmp_path, monkeypatch):
    monkeypatch.setattr("ydb.tests.functional.lbr.lib.conversion.os.access", lambda path, mode: False)
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles")
    assert conversion.time_tool is None
    result = conversion._run(["/bin/sh", "-c", "printf converted"], tmp_path / "profiles/snap1")
    assert result["exit_code"] == 0
    assert result["peak_rss_kib"] is None
    assert (tmp_path / "profiles/snap1.log").read_text() == "converted"


@pytest.mark.parametrize("wrong_prefix", [False, True])
def test_mmap_filename_is_exact_and_can_contain_spaces(tmp_path, monkeypatch, wrong_prefix):
    binary = tmp_path / "directory with spaces" / "ydbd"
    binary.parent.mkdir()
    binary.touch()
    mapped = f"/another{binary}" if wrong_prefix else str(binary)
    perf = tmp_path / "perf"
    perf.write_text(
        f"#!/bin/sh\nprintf 'PERF_RECORD_MMAP2 1/1: [0x1000(0x1000) @ 0 00:00 0 0]: r-xp {mapped}\\n0x123/0x456\\n'\n"
    )
    perf.chmod(0o755)
    conversion = LbrConversion(binary, tmp_path / "profiles", perf=str(perf))

    def profgen(command, prefix):
        output = next(arg.split("=", 1)[1] for arg in command if arg.startswith("--output="))
        Path(output).write_bytes(b"profile")
        return {}

    monkeypatch.setattr(conversion, "_run", profgen)
    if wrong_prefix:
        with pytest.raises(RuntimeError, match="mappings=0"):
            conversion._convert(tmp_path / "data", tmp_path / "profiles/snap1")
    else:
        assert conversion._convert(tmp_path / "data", tmp_path / "profiles/snap1")["samples"] == 1


def test_finish_allows_two_sequential_commands(tmp_path, monkeypatch):
    conversion = LbrConversion(tmp_path / "ydbd", tmp_path / "profiles", timeout=6)
    entered = threading.Event()

    def convert(data, prefix):
        entered.set()
        for _ in range(2):
            with conversion._process(["sleep", "5.7"], stdout=subprocess.DEVNULL) as process:
                process.wait(timeout=11)
        return fake_result(prefix.with_suffix(".prof"))

    def merge(command, prefix):
        Path(command[command.index("-o") + 1]).write_bytes(b"merged")

    monkeypatch.setattr(conversion, "_convert", convert)
    monkeypatch.setattr(conversion, "_run", merge)
    try:
        conversion.submit("workload", 1, tmp_path / "data")
        assert entered.wait(5)
        conversion.finish_phase("workload", True)
        conversion.finish()
        assert not conversion._aborted.is_set()
        assert (tmp_path / "profiles/workload.prof").read_bytes() == b"merged"
    finally:
        conversion.abort()
