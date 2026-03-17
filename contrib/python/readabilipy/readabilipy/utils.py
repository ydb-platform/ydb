# -*- coding: utf-8 -*-

"""Utility functions

"""

import os
import subprocess
import sys

from contextlib import contextmanager


@contextmanager
def chdir(path):
    """Change directory in context and return to original on exit"""
    # From https://stackoverflow.com/a/37996581, couldn't find a built-in
    original_path = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_path)


def have_npm():
    try:
        cp = subprocess.run(
            ["npm", "version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
    except FileNotFoundError:
        return False
    return cp.returncode == 0


def run_npm_install():
    # Run NPM installation
    if not have_npm():
        print(
            "Warning: A working NPM installation was not found. The package will use Python-based article extraction.",
            file=sys.stderr,
        )
        return

    here = os.path.abspath(os.path.dirname(__file__))
    jsdir = os.path.join(here, "javascript")
    pkgjson = os.path.join(jsdir, "package.json")
    if not os.path.exists(pkgjson):
        print(
            "Error: Couldn't find package.json. Package will use Python-based extraction.",
            file=sys.stderr,
        )
        return

    with chdir(jsdir):
        try:
            cp = subprocess.run(["npm", "install"], check=True)
            returncode = cp.returncode
        except FileNotFoundError:
            returncode = 1

    if returncode != 0:
        print(
            "Error: Failed to install dependencies with npm. Package will fall back on Python-based extraction.",
            file=sys.stderr,
        )
