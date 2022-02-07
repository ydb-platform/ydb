from __future__ import print_function

import os
import subprocess
import sys

import yatest.common as yc


def test_fuzzing():
    errfile = './errfile'
    outfile = './outfile'
    env = os.environ.copy()

    for number in range(25000):
        with open(errfile, 'w') as fe:
            with open(outfile, 'w') as fo:
                p = subprocess.Popen(
                    [
                        yc.build_path('library/cpp/tvmauth/src/rw/ut_large/gen/gen'),
                    ],
                    env=env,
                    stdout=fo,
                    stderr=fe,
                )
                code = p.wait()

        with open(errfile) as fe:
            all = fe.read()
            if all != '':
                with open(outfile) as fo:
                    print(fo.read(), file=sys.stderr)
                assert all == ''

        assert code == 0
