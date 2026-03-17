#!/usr/bin/env python
"""Tests for tablib._vendor.dbfpy."""

import datetime as dt
import unittest

from tablib._vendor.dbfpy import utils


class UtilsUnzfillTestCase(unittest.TestCase):
    """dbfpy.utils.unzfill test cases."""

    def test_unzfill_with_nul(self):
        # Arrange
        text = b"abc\0xyz"

        # Act
        output = utils.unzfill(text)

        # Assert
        self.assertEqual(output, b"abc")

    def test_unzfill_without_nul(self):
        # Arrange
        text = b"abcxyz"

        # Act
        output = utils.unzfill(text)

        # Assert
        self.assertEqual(output, b"abcxyz")


class UtilsGetDateTestCase(unittest.TestCase):
    """dbfpy.utils.getDate test cases."""

    def test_getDate_none(self):
        # Arrange
        value = None

        # Act
        output = utils.getDate(value)

        # Assert
        self.assertIsInstance(output, dt.date)

    def test_getDate_datetime_date(self):
        # Arrange
        value = dt.date(2019, 10, 19)

        # Act
        output = utils.getDate(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, value)

    def test_getDate_datetime_datetime(self):
        # Arrange
        value = dt.datetime(2019, 10, 19, 12, 00, 00)

        # Act
        output = utils.getDate(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, value)

    def test_getDate_datetime_timestamp(self):
        # Arrange
        value = 1571515306

        # Act
        output = utils.getDate(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, dt.date(2019, 10, 19))

    def test_getDate_datetime_string_yyyy_mm_dd(self):
        # Arrange
        value = "20191019"

        # Act
        output = utils.getDate(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, dt.date(2019, 10, 19))

    def test_getDate_datetime_string_yymmdd(self):
        # Arrange
        value = "191019"

        # Act
        output = utils.getDate(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, dt.date(2019, 10, 19))


class UtilsGetDateTimeTestCase(unittest.TestCase):
    """dbfpy.utils.getDateTime test cases."""

    def test_getDateTime_none(self):
        # Arrange
        value = None

        # Act
        output = utils.getDateTime(value)

        # Assert
        self.assertIsInstance(output, dt.datetime)

    def test_getDateTime_datetime_datetime(self):
        # Arrange
        value = dt.datetime(2019, 10, 19, 12, 00, 00)

        # Act
        output = utils.getDateTime(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, value)

    def test_getDateTime_datetime_date(self):
        # Arrange
        value = dt.date(2019, 10, 19)

        # Act
        output = utils.getDateTime(value)

        # Assert
        self.assertIsInstance(output, dt.date)
        self.assertEqual(output, dt.datetime(2019, 10, 19, 00, 00))

    def test_getDateTime_datetime_timestamp(self):
        # Arrange
        value = 1571515306

        # Act
        output = utils.getDateTime(value)

        # Assert
        self.assertIsInstance(output, dt.datetime)

    def test_getDateTime_datetime_string(self):
        # Arrange
        value = "20191019"

        # Act / Assert
        with self.assertRaises(NotImplementedError):
            utils.getDateTime(value)


class InvalidValueTestCase(unittest.TestCase):
    """dbfpy.utils._InvalidValue test cases."""

    def test_sanity(self):
        # Arrange
        INVALID_VALUE = utils.INVALID_VALUE

        # Act / Assert
        self.assertEqual(INVALID_VALUE, INVALID_VALUE)
        self.assertNotEqual(INVALID_VALUE, 123)
        self.assertEqual(int(INVALID_VALUE), 0)
        self.assertEqual(float(INVALID_VALUE), 0.0)
        self.assertEqual(str(INVALID_VALUE), "")
        self.assertEqual(repr(INVALID_VALUE), "<INVALID>")
