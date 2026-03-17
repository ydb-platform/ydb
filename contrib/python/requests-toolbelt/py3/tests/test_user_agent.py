# -*- coding: utf-8 -*-
import unittest
import sys

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import pytest

from requests_toolbelt.utils import user_agent as ua


class Object(object):
    """
    A simple mock object that can have attributes added to it.
    """
    pass


class TestUserAgentBuilder(unittest.TestCase):
    def test_only_user_agent_name(self):
        assert 'fake/1.0.0' == ua.UserAgentBuilder('fake', '1.0.0').build()

    def test_includes_extras(self):
        expected = 'fake/1.0.0 another-fake/2.0.1 yet-another-fake/17.1.0'
        actual = ua.UserAgentBuilder('fake', '1.0.0').include_extras([
            ('another-fake', '2.0.1'),
            ('yet-another-fake', '17.1.0'),
        ]).build()
        assert expected == actual

    @patch('platform.python_implementation', return_value='CPython')
    @patch('platform.python_version', return_value='2.7.13')
    def test_include_implementation(self, *_):
        expected = 'fake/1.0.0 CPython/2.7.13'
        actual = ua.UserAgentBuilder('fake', '1.0.0').include_implementation(
            ).build()
        assert expected == actual

    @patch('platform.system', return_value='Linux')
    @patch('platform.release', return_value='4.9.5')
    def test_include_system(self, *_):
        expected = 'fake/1.0.0 Linux/4.9.5'
        actual = ua.UserAgentBuilder('fake', '1.0.0').include_system(
            ).build()
        assert expected == actual


class TestUserAgent(unittest.TestCase):
    def test_user_agent_provides_package_name(self):
        assert "my-package" in ua.user_agent("my-package", "0.0.1")

    def test_user_agent_provides_package_version(self):
        assert "0.0.1" in ua.user_agent("my-package", "0.0.1")

    def test_user_agent_builds_extras_appropriately(self):
        assert "extra/1.0.0" in ua.user_agent(
            "my-package", "0.0.1", extras=[("extra", "1.0.0")]
        )

    def test_user_agent_checks_extras_for_tuples_of_incorrect_length(self):
        with pytest.raises(ValueError):
            ua.user_agent("my-package", "0.0.1", extras=[
                ("extra", "1.0.0", "oops")
            ])

        with pytest.raises(ValueError):
            ua.user_agent("my-package", "0.0.1", extras=[
                ("extra",)
            ])


class TestImplementationString(unittest.TestCase):
    @patch('platform.python_implementation')
    @patch('platform.python_version')
    def test_cpython_implementation(self, mock_version, mock_implementation):
        mock_implementation.return_value = 'CPython'
        mock_version.return_value = '2.7.5'
        assert 'CPython/2.7.5' == ua._implementation_string()

    @patch('platform.python_implementation')
    def test_pypy_implementation_final(self, mock_implementation):
        mock_implementation.return_value = 'PyPy'
        sys.pypy_version_info = Object()
        sys.pypy_version_info.major = 2
        sys.pypy_version_info.minor = 0
        sys.pypy_version_info.micro = 1
        sys.pypy_version_info.releaselevel = 'final'

        assert 'PyPy/2.0.1' == ua._implementation_string()

    @patch('platform.python_implementation')
    def test_pypy_implementation_non_final(self, mock_implementation):
        mock_implementation.return_value = 'PyPy'
        sys.pypy_version_info = Object()
        sys.pypy_version_info.major = 2
        sys.pypy_version_info.minor = 0
        sys.pypy_version_info.micro = 1
        sys.pypy_version_info.releaselevel = 'beta2'

        assert 'PyPy/2.0.1beta2' == ua._implementation_string()

    @patch('platform.python_implementation')
    def test_unknown_implementation(self, mock_implementation):
        mock_implementation.return_value = "Lukasa'sSuperPython"

        assert "Lukasa'sSuperPython/Unknown" == ua._implementation_string()
