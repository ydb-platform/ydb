import re
import subprocess

import yatest.common

MISMATCH = re.compile(r'Mismatch YQL ABI versions in a single binary: (\d+\.\d+\.\d+) != (\d+\.\d+\.\d+)')


def run_probe(name):
    probe = yatest.common.binary_path('yql/essentials/public/udf/abi_version_check/test/{0}/{0}'.format(name))
    return subprocess.run([probe], stderr=subprocess.PIPE)


def test_single_abi_binary_starts():
    result = run_probe('single_abi')
    assert result.returncode == 0, result.stderr


def test_mixed_abi_binary_aborts():
    result = run_probe('mixed_abi')
    assert result.returncode != 0
    reported = MISMATCH.search(result.stderr.decode())
    assert reported is not None, result.stderr
    assert reported.group(1) != reported.group(2), result.stderr
