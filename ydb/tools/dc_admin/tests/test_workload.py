import json
import tempfile
import time
import unittest
from pathlib import Path

from ydb.tools.dc_admin.workload import Workload

FAKE_CLI = r"""#!/bin/sh
set -eu
cd "${0%/*}"
echo "$*" >> calls
if [ "$1" = scheme ]; then
    case "$2" in
        mkdir) mkdir -p "db/$3";;
        ls)
            separator=
            printf '['
            for entry in "db/$5"/*; do
                [ -d "$entry" ] || continue
                printf '%s{"type":"dir","path":"%s"}' "$separator" "${entry##*/}"
                separator=,
            done
            echo ']';;
    esac
else
    case "$5" in
        init) mkdir -p "db/$4";;
        import) echo 'loading' >&2;;
        run) printf 'warming up\rtransactions\n';;
    esac
    while [ -f "hold-$5" ]; do sleep 0.05; done
    if [ -f "fail-$5" ]; then echo "error from $5" >&2; exit 1; fi
fi
"""


class TestWorkload(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.cli = self.directory / "ydb"
        self.cli.write_text(FAKE_CLI)
        self.cli.chmod(0o755)
        self.workload = self.new_workload()

    def new_workload(self):
        workload = Workload([str(self.cli)])
        self.addCleanup(workload.close)
        return workload

    def wait_phase(self, workload, phase):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = workload.status()
            if status["phase"] == phase:
                return status
            if status["phase"] == "failed":
                self.fail(json.dumps(status, ensure_ascii=False))
            time.sleep(0.01)
        self.fail(f"Expected {phase}, got {workload.status()}")

    def calls(self):
        return (self.directory / "calls").read_text().splitlines()

    def test_successful_import_reused_after_restart(self):
        self.assertEqual(self.workload.start()["phase"], "preparing")
        status = self.wait_phase(self.workload, "finished")
        self.assertIn("transactions", status["log"])
        self.assertIn("warming up", status["log"])
        initial = self.calls()
        self.assertIn("workload tpcc --path dc-admin-tpcc-100/data import -w 100 --threads 2 --no-tui", initial)
        self.assertIn("workload tpcc --path dc-admin-tpcc-100/data init -w 100", initial)
        self.assertIn("run -w 100 ", initial[-1])
        self.assertIn("scheme mkdir dc-admin-tpcc-100/ready_v1", initial)
        self.workload.close()
        resumed = self.new_workload()
        resumed.start()
        self.wait_phase(resumed, "finished")
        commands = self.calls()[len(initial) :]
        self.assertEqual(commands[-1], initial[-1])
        self.assertEqual(sum(command.startswith("workload ") for command in commands), 1)

    def test_cancel_running_cli_and_exclude_concurrent_start(self):
        (self.directory / "hold-run").touch()
        self.workload.start()
        self.wait_phase(self.workload, "running")
        with self.assertRaises(RuntimeError):
            self.workload.start()
        self.assertEqual(self.workload.stop()["phase"], "stopping")
        self.wait_phase(self.workload, "stopped")

    def test_retry_interrupted_import_in_same_directory(self):
        (self.directory / "fail-import").touch()
        self.workload.start()
        self.assertIn("error from import", self.wait_phase(self.workload, "failed")["message"])
        self.assertNotIn("scheme mkdir dc-admin-tpcc-100/ready_v1", self.calls())
        (self.directory / "fail-import").unlink()
        self.workload.start()
        self.wait_phase(self.workload, "finished")
        self.assertEqual(self.calls().count("workload tpcc --path dc-admin-tpcc-100/data clean"), 1)

    def test_existing_unmanaged_data_is_preserved(self):
        (self.directory / "db/dc-admin-tpcc-100/data").mkdir(parents=True)
        self.workload.start()
        self.wait_phase(self.workload, "failed")
        self.assertFalse(any(command.startswith("workload ") for command in self.calls()))
