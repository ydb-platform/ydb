"""Tests for the JupyterConsoleApp"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os

import pytest
from jupyter_core.application import JupyterApp

from jupyter_client.consoleapp import JupyterConsoleApp
from jupyter_client.manager import start_new_kernel


class MockConsoleApp(JupyterConsoleApp, JupyterApp):  # type:ignore
    pass


def test_console_app_no_existing():
    app = MockConsoleApp()
    app.initialize([])


def test_console_app_existing(tmp_path):
    km, kc = start_new_kernel()
    cf = kc.connection_file
    app = MockConsoleApp(connection_file=cf, existing=cf)
    app.initialize([])
    kc.stop_channels()
    km.shutdown_kernel()


def test_console_app_ssh(tmp_path):
    km, kc = start_new_kernel()
    cf = kc.connection_file
    os.chdir(tmp_path)
    app = MockConsoleApp(
        connection_file=cf, existing=cf, sshserver="does_not_exist", sshkey="test_console_app"
    )
    with pytest.raises(SystemExit):
        app.initialize([])
    kc.stop_channels()
    km.shutdown_kernel()
