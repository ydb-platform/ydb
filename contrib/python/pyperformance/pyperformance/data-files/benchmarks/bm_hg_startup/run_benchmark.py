import sys
import subprocess

import pyperf
from pyperformance.venv import get_venv_program


def get_hg_version(hg_bin):
    # Fast-path: use directly the Python module
    try:
        from mercurial.__version__ import version
        if isinstance(version, bytes):
            return version.decode('utf8')
        else:
            return version
    except ImportError:
        pass

    # Slow-path: run the "hg --version" command
    proc = subprocess.Popen([sys.executable, hg_bin, "--version"],
                            stdout=subprocess.PIPE,
                            universal_newlines=True)
    stdout = proc.communicate()[0]
    if proc.returncode:
        print("ERROR: Mercurial command failed!")
        sys.exit(proc.returncode)
    return stdout.splitlines()[0]


if __name__ == "__main__":
    runner = pyperf.Runner(values=25)

    runner.metadata['description'] = "Performance of the Python startup"
    args = runner.parse_args()

    hg_bin = get_venv_program('hg')
    runner.metadata['hg_version'] = get_hg_version(hg_bin)

    command = [sys.executable, hg_bin, "help"]
    runner.bench_command('hg_startup', command)
