import functools
import json
import os.path
import subprocess
import sys

from pyperf._utils import shell_quote, popen_communicate
from pyperf._worker import WorkerTask


def parse_subprocess_data(output):
    # Parse the data send from the subprocess.
    # It is three lines containing:
    #    - The runtime (in seconds)
    #    - max_rss (or -1, if not able to compute)
    #    - The metadata to add to the benchmark entry, as a JSON dictionary

    rss = None
    metadata = {}
    try:
        lines = output.splitlines()
        timing = float(lines[0])
        rss = int(lines[1])
        metadata = json.loads(lines[2])
    except ValueError:
        raise ValueError("failed to parse worker output: %r" % output)

    return timing, rss, metadata


def bench_command(command, task, loops):
    path = os.path.dirname(__file__)
    script = os.path.join(path, '_process_time.py')
    run_script = [sys.executable, script]

    args = run_script + [str(loops)] + command
    proc = subprocess.Popen(args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            universal_newlines=True)
    output = popen_communicate(proc)[0]
    if proc.returncode:
        raise Exception("Command failed with exit code %s"
                        % proc.returncode)

    timing, rss, metadata = parse_subprocess_data(output)

    if rss and rss > 0:
        # store the maximum
        max_rss = task.metadata.get('command_max_rss', 0)
        task.metadata['command_max_rss'] = max(max_rss, rss)

    task.metadata.update(metadata)

    return timing


class BenchCommandTask(WorkerTask):
    def __init__(self, runner, name, command):
        command_str = ' '.join(map(shell_quote, command))
        metadata = {'command': command_str}
        task_func = functools.partial(bench_command, command)
        WorkerTask.__init__(self, runner, name, task_func, metadata)

    def compute(self):
        WorkerTask.compute(self)
        if self.args.track_memory:
            value = self.metadata.pop('command_max_rss', None)
            if not value:
                raise RuntimeError("failed to get the process RSS")

            self._set_memory_value(value)
