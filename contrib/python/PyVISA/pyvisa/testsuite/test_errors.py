# -*- coding: utf-8 -*-
"""Test the handling of errors."""

import pickle

from pyvisa import errors
from pyvisa.testsuite import BaseTestCase


class TestPicleUnpickle(BaseTestCase):
    def _test_pickle_unpickle(self, instance):
        pickled = pickle.dumps(instance)
        unpickled = pickle.loads(pickled)
        assert isinstance(unpickled, type(instance))
        for attr in instance.__dict__:
            assert getattr(instance, attr) == getattr(unpickled, attr)

    def test_VisaIOError(self):
        self._test_pickle_unpickle(errors.VisaIOError(0))

    def test_VisaIOWarning(self):
        self._test_pickle_unpickle(errors.VisaIOWarning(0))

    def test_UnknownHandler(self):
        self._test_pickle_unpickle(errors.UnknownHandler(0, 0, 0))

    def test_OSNotSupported(self):
        self._test_pickle_unpickle(errors.OSNotSupported(""))

    def test_InvalidBinaryFormat(self):
        self._test_pickle_unpickle(errors.InvalidBinaryFormat())
        self._test_pickle_unpickle(errors.InvalidBinaryFormat("test"))

    def test_InvalidSession(self):
        self._test_pickle_unpickle(errors.InvalidSession())


class TestLibraryError(BaseTestCase):
    """Test the creation of Library errors."""

    def test_from_exception_not_found(self):
        """Test handling a missing library file."""
        exc = errors.LibraryError.from_exception(
            ValueError("visa.dll: image not found"), "visa.dll"
        )
        assert "File not found" in str(exc)

    def test_from_exception_wrong_arch(self):
        """Test handling a library that report the wrong bitness."""
        exc = errors.LibraryError.from_exception(
            ValueError("visa.dll: no suitable image found. no matching architecture"),
            "visa.dll",
        )
        assert "No matching architecture" in str(exc)

    def test_from_exception_wrong_filetype(self):
        """Test handling a library file of the wrong type."""
        exc = errors.LibraryError.from_exception(
            ValueError("visa.dll: no suitable image found."), "visa.dll"
        )
        assert "Could not determine filetype" in str(exc)

    def test_from_exception_wrong_ELF(self):
        """Test handling a library file with a wrong ELF."""
        exc = errors.LibraryError.from_exception(
            ValueError("visa.dll: wrong ELF class"), "visa.dll"
        )
        assert "No matching architecture" in str(exc)

    def test_from_exception_random(self):
        """Test handling a library for which the error is not a usual one."""
        exc = errors.LibraryError.from_exception(ValueError("visa.dll"), "visa.dll")
        assert "Error while accessing" in str(exc)

    def test_from_exception_decode_error(self):
        """Test handling an error that decode to string."""

        class DummyExc(Exception):
            def __str__(self):
                raise b"\xff".decode("ascii")

        exc = errors.LibraryError.from_exception(
            DummyExc("visa.dll: wrong ELF class"), "visa.dll"
        )
        assert "Error while accessing visa.dll." == str(exc)

    # from_wrong_arch is exercised through the above tests.
