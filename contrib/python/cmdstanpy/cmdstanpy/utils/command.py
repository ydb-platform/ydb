"""
Run commands and handle returncodes
"""

import os
import subprocess
import sys
from typing import Callable, Optional, TextIO

from .filesystem import pushd
from .logging import get_logger


def do_command(
    cmd: list[str],
    cwd: Optional[str] = None,
    *,
    fd_out: Optional[TextIO] = sys.stdout,
    pbar: Optional[Callable[[str], None]] = None,
) -> None:
    """
    Run command as subprocess, polls process output pipes and
    either streams outputs to supplied output stream or sends
    each line (stripped) to the supplied progress bar callback hook.

    Raises ``RuntimeError`` on non-zero return code or execption ``OSError``.

    :param cmd: command and args.
    :param cwd: directory in which to run command, if unspecified,
        run command in the current working directory.
    :param fd_out: when supplied, streams to this output stream,
        else writes to sys.stdout.
    :param pbar: optional callback hook to tqdm, which takes
       single ``str`` arguent, see:
       https://github.com/tqdm/tqdm#hooks-and-callbacks.

    """
    get_logger().debug('cmd: %s\ncwd: %s', ' '.join(cmd), cwd)
    try:
        # NB: Using this rather than cwd arg to Popen due to windows behavior
        with pushd(cwd if cwd is not None else '.'):
            # TODO: replace with subprocess.run in later Python versions?
            proc = subprocess.Popen(
                cmd,
                bufsize=1,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # avoid buffer overflow
                env=os.environ,
                universal_newlines=True,
            )
            while proc.poll() is None:
                if proc.stdout is not None:
                    line = proc.stdout.readline()
                    if fd_out is not None:
                        fd_out.write(line)
                    if pbar is not None:
                        pbar(line.strip())

            stdout, _ = proc.communicate()
            if stdout:
                if len(stdout) > 0:
                    if fd_out is not None:
                        fd_out.write(stdout)
                    if pbar is not None:
                        pbar(stdout.strip())

            if proc.returncode != 0:  # throw RuntimeError + msg
                serror = ''
                try:
                    serror = os.strerror(proc.returncode)
                except (ArithmeticError, ValueError):
                    pass
                msg = "Command {}\n\texited with code '{}' {}".format(
                    cmd, proc.returncode, serror
                )
                raise RuntimeError(msg)
    except OSError as e:
        msg = 'Command: {}\nfailed with error {}\n'.format(cmd, str(e))
        raise RuntimeError(msg) from e
