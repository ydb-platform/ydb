"""Run reviewer commands, cleaning up their POSIX process group when interrupted or timed out."""

import os
import signal
import subprocess


def run_process(argv, cwd, input_text, timeout):
    if os.name != "posix":
        return subprocess.run(argv, input=input_text, cwd=cwd, capture_output=True, text=True, timeout=timeout)

    with subprocess.Popen(argv, cwd=cwd, stdin=subprocess.PIPE if input_text is not None else None,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True) as proc:
        try:
            stdout, stderr = proc.communicate(input_text, timeout=timeout)
        except BaseException:
            # The leader may have exited while its children still hold the output pipes open.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.communicate()
            raise
        return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)
