from __future__ import print_function

import os
import sys
import time
import subprocess


def need_retry(text):
    return 'Stack dump' in text


def retry_inf(cmd):
    while True:
        try:
            yield subprocess.check_output(cmd, stderr=subprocess.STDOUT), None
        except subprocess.CalledProcessError as e:
            yield e.output, e


def retry(cmd):
    for n, (out, err) in enumerate(retry_inf(cmd)):
        if out:
            sys.stderr.write(out)

        if n > 5:
            raise Exception('all retries failed')
        elif need_retry(out):
            time.sleep(1 + n)
        elif err:
            raise err
        else:
            return


if __name__ == '__main__':
    cmd = sys.argv[1:]

    if '-c' in cmd:
        try:
            retry(cmd)
        except subprocess.CalledProcessError as e:
            sys.exit(e.returncode)
    else:
        os.execv(cmd[0], cmd)
