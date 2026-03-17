"""Utility code for tests."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2017-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import pytest


class Sandboxer:
    """
    Given a sandbox template path, uses pytest to create a temporary directory,
    then copies the template's contents there, and changes to the directory.

    At exit, it changes back to the current working directory.
    """

    def __init__(self, tmpdir, path=None):
        self.__tmpdir = tmpdir
        self.__templatepath = path

    def __enter__(self):
        # Save current working directory & python path
        import os, sys

        self.__cwd = os.getcwd()
        self.__syspath = sys.path[:]

        # Create a new temporary directory and copy the template over
        if self.__templatepath:
            tmp = str(self.__tmpdir.join(self.__templatepath))
            import shutil

            shutil.copytree(self.__templatepath, tmp)
        else:
            tmp = self.__tmpdir

        # Then change to the temporary directory & python path
        os.chdir(str(tmp))
        import site

        sys.path.insert(0, tmp)

    def __exit__(self, exc_type, exc_value, traceback):
        # Restore current working directory & python path
        import os, sys

        os.chdir(str(self.__cwd))
        sys.path = self.__syspath

        # Don't suppress exceptions
        return False


def sandbox(*args, **kwargs):
    """Create a sandbox"""
    return Sandboxer(*args, **kwargs)
