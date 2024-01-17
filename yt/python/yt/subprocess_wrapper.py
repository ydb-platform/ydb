from yt.common import to_native_str

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

Popen = subprocess.Popen
PIPE = subprocess.PIPE
STDOUT = subprocess.STDOUT
CalledProcessError = subprocess.CalledProcessError


def check_call(*args, **kwargs):
    return subprocess.check_call(*args, **kwargs)


def check_output(*args, **kwargs):
    return to_native_str(subprocess.check_output(*args, **kwargs))
