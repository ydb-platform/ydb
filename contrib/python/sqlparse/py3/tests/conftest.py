"""Helpers for testing."""

import io
import os

import pytest


@pytest.fixture()
def filepath():
    """Returns full file path for test files."""
    import yatest.common
    DIR_PATH = yatest.common.test_source_path()
    FILES_DIR = os.path.join(DIR_PATH, 'files')

    def make_filepath(filename):
        # https://stackoverflow.com/questions/18011902/py-test-pass-a-parameter-to-a-fixture-function
        # Alternate solution is to use parametrization `indirect=True`
        # https://stackoverflow.com/questions/18011902/py-test-pass-a-parameter-to-a-fixture-function/33879151#33879151
        # Syntax is noisy and requires specific variable names
        return os.path.join(FILES_DIR, filename)

    return make_filepath


@pytest.fixture()
def load_file(filepath):
    """Opens filename with encoding and return its contents."""

    def make_load_file(filename, encoding='utf-8'):
        # https://stackoverflow.com/questions/18011902/py-test-pass-a-parameter-to-a-fixture-function
        # Alternate solution is to use parametrization `indirect=True`
        # https://stackoverflow.com/questions/18011902/py-test-pass-a-parameter-to-a-fixture-function/33879151#33879151
        # Syntax is noisy and requires specific variable names
        # And seems to be limited to only 1 argument.
        with open(filepath(filename), encoding=encoding) as f:
            return f.read().strip()

    return make_load_file


@pytest.fixture()
def get_stream(filepath):
    def make_stream(filename, encoding='utf-8'):
        return open(filepath(filename), encoding=encoding)

    return make_stream
