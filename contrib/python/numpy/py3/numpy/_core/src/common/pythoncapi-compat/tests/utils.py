import subprocess
import sys


PYTHON3 = (sys.version_info >= (3,))


def run_command(cmd, **kw):
    if hasattr(subprocess, 'run'):
        proc = subprocess.run(cmd, **kw)
    else:
        kw['shell'] = False
        proc = subprocess.Popen(cmd, **kw)
        try:
            proc.wait()
        except:
            proc.kill()
            proc.wait()
            raise

    exitcode = proc.returncode
    if exitcode:
        sys.exit(exitcode)


def command_stdout(cmd, **kw):
    kw['stdout'] = subprocess.PIPE
    kw['universal_newlines'] = True
    if hasattr(subprocess, 'run'):
        proc = subprocess.run(cmd, **kw)
        return (proc.returncode, proc.stdout)
    else:
        kw['shell'] = False
        proc = subprocess.Popen(cmd, **kw)
        try:
            stdout = proc.communicate()[0]
        except:
            proc.kill()
            proc.wait()
            raise
        return (proc.returncode, stdout)
