import os
import subprocess
import sys
import tempfile

import pytest

from .test_base import JSON


def _do_test_benchmark(method="basic_parse", multiple_values=True, extra_args=None):
    # Use python backend to ensure multiple_values works
    if multiple_values:
        env = dict(os.environ)
        env['IJSON_BACKEND'] = 'python'
    # Ensure printing works on the subprocess in Windows
    # by using utf-8 on its stdout
    if sys.platform == 'win32':
        env = dict(os.environ)
        env['PYTHONIOENCODING'] = 'utf-8'
    cmd = [sys.executable, '-m', 'ijson.benchmark', '-m', method, '-p', '', '-s', '1']
    if multiple_values:
        cmd.append('-M')
    if extra_args:
        cmd += extra_args
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    out, err = proc.communicate()
    status = proc.wait()
    assert 0 == status, "out:\n%s\nerr:%s" % (out.decode('utf-8'), err.decode('utf-8'))

@pytest.mark.parametrize("input_type", ("gen", "coro", "async"))
@pytest.mark.parametrize("method", ("basic_parse", "parse", "kvitems", "items"))
def test_benchmark(method, input_type):
    extra_args = {"gen": [], "coro": ["-c"], "async": ["-a"]}
    _do_test_benchmark(method, extra_args=extra_args[input_type])

def test_list():
    _do_test_benchmark(extra_args=['-l'])

@pytest.mark.parametrize("iterations", (1, 2, 3))
def test_more_iterations(iterations):
    _do_test_benchmark(extra_args=['-I', str(iterations)])

def test_input_files():
    fd, fname = tempfile.mkstemp()
    os.write(fd, JSON)
    os.close(fd)
    try:
        _do_test_benchmark(extra_args=[fname])
        _do_test_benchmark(extra_args=[fname, fname])
    finally:
        os.unlink(fname)
