# -*- coding: utf-8 -*-
"""Test the reading of env vars."""

import os
import sys
from subprocess import PIPE, Popen

from . import BaseTestCase


class TestEnvVarHandling(BaseTestCase):
    """Test reading env vars"""

    def test_reading_wrap_handler(self):
        with Popen([sys.executable], stdin=PIPE, stdout=PIPE) as p:
            stdout, _ = p.communicate(
                b"from pyvisa import ctwrapper;print(ctwrapper.WRAP_HANDLER);exit()"
            )
        assert b"True" == stdout.rstrip()

        env = os.environ.copy()
        env["PYVISA_WRAP_HANDLER"] = "0"
        with Popen([sys.executable], stdin=PIPE, stdout=PIPE, env=env) as p:
            stdout, _ = p.communicate(
                b"from pyvisa import ctwrapper;print(ctwrapper.WRAP_HANDLER);exit()"
            )
        assert b"False" == stdout.rstrip()
