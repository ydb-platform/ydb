import os
import subprocess
import sys

import pytest

from .test_base import JSON


@pytest.mark.parametrize("multiple_values", (True, False))
@pytest.mark.parametrize("method", ("basic_parse", "parse", "kvitems", "items"))
def test_dump(method, multiple_values):
    # Use python backend to ensure multiple_values works
    env = dict(os.environ)
    env['IJSON_BACKEND'] = 'python'
    # Ensure printing works on the subprocess in Windows
    # by using utf-8 on its stdout
    if sys.platform == 'win32':
        env = dict(os.environ)
        env['PYTHONIOENCODING'] = 'utf-8'
    cmd = [sys.executable, '-m', 'ijson.dump', '-m', method, '-p', '']
    if multiple_values:
        cmd.append('-M')
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    input_data = JSON
    if multiple_values:
        input_data += JSON
    out, err = proc.communicate(input_data)
    status = proc.wait()
    assert 0 == status, "out:\n%s\nerr:%s" % (out.decode('utf-8'), err.decode('utf-8'))