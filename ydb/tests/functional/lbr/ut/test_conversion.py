import json
import threading
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
