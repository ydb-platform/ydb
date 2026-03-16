# -*- coding: utf-8 -*-
"""Test the behavior of the command line tools."""

import argparse
from subprocess import PIPE, Popen, run

import pytest

from pyvisa import cmd_line_tools, util

from . import BaseTestCase, require_visa_lib


class TestCmdLineTools(BaseTestCase):
    """Test the cmd line tools functions and scripts."""

    def test_visa_info(self):
        """Test the visa info command line tool."""
        result = run("pyvisa-info", stdout=PIPE, universal_newlines=True)
        details = util.system_details_to_str(util.get_system_details())
        # Path difference can lead to subtle differences in the backends
        # and Python path so compare only the first lines.
        assert result.stdout.strip().split("\n")[:5] == details.strip().split("\n")[:5]

    # TODO test backend selection: this is not easy at all to assert
    @require_visa_lib
    def test_visa_shell(self):
        """Test the visa shell function."""
        with Popen(["pyvisa-shell"], stdin=PIPE, stdout=PIPE) as p:
            stdout, _stderr = p.communicate(b"exit")
        assert stdout.count(b"Welcome to the VISA shell") == 1


@pytest.mark.parametrize(
    "args, want",
    [
        (argparse.Namespace(backend=None), ""),
        (argparse.Namespace(backend="py"), "@py"),
        (argparse.Namespace(backend="foo.yaml@sim"), "foo.yaml@sim"),
        (argparse.Namespace(backend="/foo/bar/baz.yaml@sim"), "/foo/bar/baz.yaml@sim"),
        (argparse.Namespace(backend="@sim"), "@sim"),
    ],
)
def test__create_backend_str(args: argparse.Namespace, want: str) -> None:
    got = cmd_line_tools._create_backend_str(args)
    assert got == want
