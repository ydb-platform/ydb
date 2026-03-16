# Hooks are installable context managers defined as entry points so that
# arbitrary code can by run right before and after the actual internal
# benchmarking code is run.


import abc
import importlib.metadata
import os.path
import shlex
import subprocess
import sys
import tempfile
import uuid


def get_hooks():
    hook_prefix = "pyperf.hook"
    entry_points = importlib.metadata.entry_points()
    if sys.version_info[:2] < (3, 10):
        group = entry_points[hook_prefix]
    else:
        group = entry_points.select(group=hook_prefix)
    return group


def get_hook_names():
    return (x.name for x in get_hooks())


def get_selected_hooks(hook_names):
    if hook_names is None:
        return

    hook_mapping = {hook.name: hook for hook in get_hooks()}
    for hook_name in hook_names:
        yield hook_mapping[hook_name]


def instantiate_selected_hooks(hook_names):
    hook_managers = {}
    for hook in get_selected_hooks(hook_names):
        try:
            hook_managers[hook.name] = hook.load()()
        except HookError as e:
            print(f"ERROR setting up hook '{hook.name}':", file=sys.stderr)
            print(str(e), file=sys.stderr)
            sys.exit(1)

    return hook_managers


class HookError(Exception):
    pass


class HookBase(abc.ABC):
    def __init__(self):
        """
        Create a new instance of the hook.
        """
        pass

    def teardown(self, _metadata):
        """
        Called when the hook is completed for a process. May add any information
        collected to the passed-in `metadata` dictionary.
        """
        pass

    def __enter__(self):
        """
        Called immediately before running benchmark code.

        May be called multiple times per instance.
        """
        pass

    def __exit__(self, _exc_type, _exc_value, _traceback):
        """
        Called immediately after running benchmark code.
        """
        pass


class _test_hook(HookBase):
    def __init__(self):
        self._count = 0

    def teardown(self, metadata):
        metadata["_test_hook"] = self._count

    def __enter__(self):
        self._count += 1

    def __exit__(self, _exc_type, _exc_value, _traceback):
        pass


class pystats(HookBase):
    def __init__(self):
        if not hasattr(sys, "_stats_on"):
            raise HookError(
                "Can not collect pystats because python was not built with --enable-pystats"
            )
        sys._stats_off()
        sys._stats_clear()

    def teardown(self, metadata):
        metadata["pystats"] = "enabled"

    def __enter__(self):
        sys._stats_on()

    def __exit__(self, _exc_type, _exc_value, _traceback):
        sys._stats_off()


class perf_record(HookBase):
    """Profile the benchmark using perf-record.

    Profile data is written to the current directory directory by default, or
    to the value of the `PYPERF_PERF_RECORD_DATA_DIR` environment variable, if
    it is provided.

    Profile data files have a basename of the form `perf.data.<uuid>`

    The value of the `PYPERF_PERF_RECORD_EXTRA_OPTS` environment variable is
    appended to the command line of perf-record, if provided.
    """

    def __init__(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.ctl_fifo = self.mkfifo(self.tempdir.name, "ctl_fifo")
        self.ack_fifo = self.mkfifo(self.tempdir.name, "ack_fifo")
        perf_data_dir = os.environ.get("PYPERF_PERF_RECORD_DATA_DIR", "")
        perf_data_basename = f"perf.data.{uuid.uuid4()}"
        cmd = ["perf", "record",
               "--pid", str(os.getpid()),
               "--output", os.path.join(perf_data_dir, perf_data_basename),
               "--control", f"fifo:{self.ctl_fifo},{self.ack_fifo}"]
        extra_opts = os.environ.get("PYPERF_PERF_RECORD_EXTRA_OPTS", "")
        cmd += shlex.split(extra_opts)
        self.perf = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.ctl_fd = open(self.ctl_fifo, "w")
        self.ack_fd = open(self.ack_fifo, "r")

    def __enter__(self):
        self.exec_perf_cmd("enable")

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.exec_perf_cmd("disable")

    def teardown(self, metadata):
        try:
            self.exec_perf_cmd("stop")
            self.perf.wait(timeout=120)
        finally:
            self.ctl_fd.close()
            self.ack_fd.close()

    def mkfifo(self, tmpdir, basename):
        path = os.path.join(tmpdir, basename)
        os.mkfifo(path)
        return path

    def exec_perf_cmd(self, cmd):
        self.ctl_fd.write(f"{cmd}\n")
        self.ctl_fd.flush()
        self.ack_fd.readline()
