import os
import subprocess
import logging

import svn.config
import svn.exception

_LOGGER = logging.getLogger(__name__)


class CommonBase(object):
    def external_command(self, cmd, success_code=0, do_combine=False,
                         return_binary=False, environment={}, wd=None):
        _LOGGER.debug("RUN: %s" % (cmd,))

        env = os.environ.copy()
        env['LANG'] = svn.config.CONSOLE_ENCODING
        env.update(environment)

        decode_text = return_binary is False

        try:
            stdout = \
                subprocess.check_output(
                    cmd,
                    cwd=wd,
                    env=env,
                    stderr=subprocess.STDOUT,
                    universal_newlines=decode_text)
        except subprocess.CalledProcessError as cpe:
            stdout = cpe.output
            return_code = cpe.returncode
        else:
            return_code = 0

        if return_code != 0:
            raise svn.exception.SvnException(
                "Command failed with ({}): {}\n{}".format(
                return_code, cmd, stdout))

        if return_binary is True or do_combine is True:
            return stdout

        return stdout.strip('\n').split('\n')

    def rows_to_dict(self, rows, lc=True):
        d = {}
        for row in rows:
            row = row.strip()
            if not row:
                continue

            pivot = row.index(': ')

            k = row[:pivot]
            v = row[pivot + 2:]

            if lc is True:
                k = k.lower()

            d[k] = v

        return d
