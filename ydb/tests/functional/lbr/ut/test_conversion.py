import json
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
    perf.write_text(f"#!/bin/sh\nprintf 'PERF_RECORD_MMAP {binary}\\n0x123/0x456\\nunrelated\\n'\n")
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
    assert (tmp_path / "profiles/workload/snap1.lbr").read_text() == f"PERF_RECORD_MMAP {binary}\n0x123/0x456\n"
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
    assert "merge failed" in (tmp_path / "profiles/conversion.json").read_text()


@pytest.mark.parametrize("resolved_mapping", [False, True])
def test_symlink_binary_mapping(tmp_path, monkeypatch, resolved_mapping):
    binary = tmp_path / "server"
    binary.touch()
    link = tmp_path / "ydbd"
    link.symlink_to(binary)
    mapped = binary if resolved_mapping else link
    perf = tmp_path / "perf"
    perf.write_text(f"#!/bin/sh\nprintf 'PERF_RECORD_MMAP {mapped}\\n0x123/0x456\\n'\n")
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
