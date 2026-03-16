"""Tests for KernelManager"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os
import tempfile
from unittest import mock

from jupyter_client.kernelspec import KernelSpec
from jupyter_client.manager import KernelManager


def test_connection_file_real_path():
    """Verify realpath is used when formatting connection file"""
    with mock.patch("os.path.realpath") as patched_realpath:
        patched_realpath.return_value = "foobar"
        km = KernelManager(
            connection_file=os.path.join(tempfile.gettempdir(), "kernel-test.json"),
            kernel_name="test_kernel",
        )

        # KernelSpec and launch args have to be mocked as we don't have an actual kernel on disk
        km._kernel_spec = KernelSpec(
            resource_dir="test",
            **{
                "argv": ["python.exe", "-m", "test_kernel", "-f", "{connection_file}"],
                "env": {},
                "display_name": "test_kernel",
                "language": "python",
                "metadata": {},
            },
        )
        km._launch_args = {}
        cmds = km.format_kernel_cmd()
        assert cmds[4] == "foobar"


def test_env_update_launch_args_not_set():
    km = KernelManager()
    km.update_env(env={"A": "A"})


def test_env_update_launch_args_not_dict():
    km = KernelManager()
    km._launch_args = None
    km.update_env(env={"B": "B"})


def test_env_update_launch_args_no_env():
    km = KernelManager()
    km._launch_args = {}
    km.update_env(env={"C": "C"})


def test_env_update_launch_args_env_not_dict():
    km = KernelManager()
    km._launch_args = {"env": None}
    km.update_env(env={"D": "D"})


def test_env_update_launch_args_env_dic():
    km = KernelManager()
    km._launch_args = {"env": {}}
    km.update_env(env={"E": "E"})
    assert km._launch_args["env"]["E"] == "E"
