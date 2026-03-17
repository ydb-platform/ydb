import datetime
from decimal import Decimal as D

import isodate
import pytest
import pytz
import six

from zeep.xsd.types import builtins


class TestString:
    def test_xmlvalue(self):
        instance = builtins.String()
        result = instance.xmlvalue("foobar")
        assert result == "foobar"

    def test_pythonvalue(self):
        instance = builtins.String()
        result = instance.pythonvalue("foobar")
        assert result == "foobar"


class TestBoolean:
    def test_xmlvalue(self):
        instance = builtins.Boolean()
        assert instance.xmlvalue(True) == "true"
        assert instance.xmlvalue(False) == "false"
        assert instance.xmlvalue(1) == "true"
        assert instance.xmlvalue(0) == "false"
        assert instance.xmlvalue("false") == "false"
        assert instance.xmlvalue("0") == "false"

    def test_pythonvalue(self):
        instance = builtins.Boolean()
        assert instance.pythonvalue("1") is True
        assert instance.pythonvalue("true") is True
        assert instance.pythonvalue("0") is False
        assert instance.pythonvalue("false") is False


class TestDecimal:
    def test_xmlvalue(self):
        instance = builtins.Decimal()
        assert instance.xmlvalue(D("10.00")) == "10.00"
        assert instance.xmlvalue(D("10.000002")) == "10.000002"
        assert instance.xmlvalue(D("10.000002")) == "10.000002"
        assert instance.xmlvalue(D("10")) == "10"
        assert instance.xmlvalue(D("-10")) == "-10"

    def test_pythonvalue(self):
        instance = builtins.Decimal()
        assert instance.pythonvalue("10") == D("10")
        assert instance.pythonvalue("10.001") == D("10.001")
        assert instance.pythonvalue("+10.001") == D("10.001")
        assert instance.pythonvalue("-10.001") == D("-10.001")


class TestFloat:
    def test_xmlvalue(self):
        instance = builtins.Float()
        assert instance.xmlvalue(float(10)) == "10.0"
        assert instance.xmlvalue(float(3.9999)) == "3.9999"
        assert instance.xmlvalue(float("inf")) == "INF"
        assert instance.xmlvalue(float(12.78e-2)) == "0.1278"
        if six.PY2:
            assert instance.xmlvalue(float("1267.43233E12")) == "1.26743233E+15"
        else:
            assert instance.xmlvalue(float("1267.43233E12")) == "1267432330000000.0"

    def test_pythonvalue(self):
        instance = builtins.Float()
        assert instance.pythonvalue("10") == float("10")
        assert instance.pythonvalue("-1E4") == float("-1E4")
        assert instance.pythonvalue("1267.43233E12") == float("1267.43233E12")
        assert instance.pythonvalue("12.78e-2") == float("0.1278")
        assert instance.pythonvalue("12") == float(12)
        assert instance.pythonvalue("-0") == float(0)
        assert instance.pythonvalue("0") == float(0)
        assert instance.pythonvalue("INF") == float("inf")


class TestDouble:
    def test_xmlvalue(self):
        instance = builtins.Double()
        assert instance.xmlvalue(float(10)) == "10.0"
        assert instance.xmlvalue(float(3.9999)) == "3.9999"
        assert instance.xmlvalue(float(12.78e-2)) == "0.1278"

    def test_pythonvalue(self):
        instance = builtins.Double()
        assert instance.pythonvalue("10") == float("10")
        assert instance.pythonvalue("12") == float(12)
        assert instance.pythonvalue("-0") == float(0)
        assert instance.pythonvalue("0") == float(0)


class TestDuration:
    def test_xmlvalue(self):
        instance = builtins.Duration()
        value = isodate.parse_duration("P0Y1347M0D")
        assert instance.xmlvalue(value) == "P1347M"

    def test_pythonvalue(self):
        instance = builtins.Duration()
        expected = isodate.parse_duration("P0Y1347M0D")
        value = "P0Y1347M0D"
        assert instance.pythonvalue(value) == expected


class TestDateTime:
    def test_xmlvalue(self):
        instance = builtins.DateTime()
        value = datetime.datetime(2016, 3, 4, 21, 14, 42)
        assert instance.xmlvalue(value) == "2016-03-04T21:14:42"

        value = datetime.datetime(2016, 3, 4, 21, 14, 42, tzinfo=pytz.utc)
        assert instance.xmlvalue(value) == "2016-03-04T21:14:42Z"

        value = datetime.datetime(2016, 3, 4, 21, 14, 42, 123456, tzinfo=pytz.utc)
        assert instance.xmlvalue(value) == "2016-03-04T21:14:42.123456Z"

        value = datetime.datetime(2016, 3, 4, 21, 14, 42, tzinfo=pytz.utc)
        value = value.astimezone(pytz.timezone("Europe/Amsterdam"))
        assert instance.xmlvalue(value) == "2016-03-04T22:14:42+01:00"

        assert (
            instance.xmlvalue("2016-03-04T22:14:42+01:00")
            == "2016-03-04T22:14:42+01:00"
        )
        assert instance.xmlvalue("2016-03-04") == "2016-03-04"

    def test_pythonvalue(self):
        instance = builtins.DateTime()
        value = datetime.datetime(2016, 3, 4, 21, 14, 42)
        assert instance.pythonvalue("2016-03-04T21:14:42") == value

        value = datetime.datetime(2016, 3, 4, 21, 14, 42, 123456)
        assert instance.pythonvalue("2016-03-04T21:14:42.123456") == value

        value = datetime.datetime(2016, 3, 4, 0, 0, 0)
        assert instance.pythonvalue("2016-03-04") == value

    def test_pythonvalue_invalid(self):
        instance = builtins.DateTime()
        with pytest.raises(ValueError):
            assert instance.pythonvalue("  :  :  ")


class TestTime:
    def test_xmlvalue(self):
        instance = builtins.Time()
        value = datetime.time(21, 14, 42)
        assert instance.xmlvalue(value) == "21:14:42"
        assert instance.xmlvalue("21:14:42") == "21:14:42"

    def test_pythonvalue(self):
        instance = builtins.Time()
        value = datetime.time(21, 14, 42)
        assert instance.pythonvalue("21:14:42") == value

        value = datetime.time(21, 14, 42, 120000)
        assert instance.pythonvalue("21:14:42.120") == value

        value = isodate.parse_time("21:14:42.120+0200")
        assert instance.pythonvalue("21:14:42.120+0200") == value

    def test_pythonvalue_invalid(self):
        instance = builtins.Time()
        with pytest.raises(ValueError):
            assert instance.pythonvalue(":")


class TestDate:
    def test_xmlvalue(self):
        instance = builtins.Date()
        value = datetime.datetime(2016, 3, 4)
        assert instance.xmlvalue(value) == "2016-03-04"
        assert instance.xmlvalue("2016-03-04") == "2016-03-04"
        assert instance.xmlvalue("2016-04") == "2016-04"

    def test_pythonvalue(self):
        instance = builtins.Date()
        assert instance.pythonvalue("2016-03-04") == datetime.date(2016, 3, 4)
        assert instance.pythonvalue("2001-10-26+02:00") == datetime.date(2001, 10, 26)
        assert instance.pythonvalue("2001-10-26Z") == datetime.date(2001, 10, 26)
        assert instance.pythonvalue("2001-10-26+00:00") == datetime.date(2001, 10, 26)

    def test_pythonvalue_invalid(self):
        instance = builtins.Date()
        # negative dates are not supported for datetime.date objects so lets
        # hope no-one uses it for now..
        with pytest.raises(ValueError):
            assert instance.pythonvalue("-2001-10-26")
        with pytest.raises(ValueError):
            assert instance.pythonvalue("-20000-04-01")


class TestgYearMonth:
    def test_xmlvalue(self):
        instance = builtins.gYearMonth()
        assert instance.xmlvalue((2012, 10, None)) == "2012-10"
        assert instance.xmlvalue((2012, 10, pytz.utc)) == "2012-10Z"

    def test_pythonvalue(self):
        instance = builtins.gYearMonth()
        assert instance.pythonvalue("2001-10") == (2001, 10, None)
        assert instance.pythonvalue("2001-10+02:00") == (
            2001,
            10,
            pytz.FixedOffset(120),
        )
        assert instance.pythonvalue("2001-10Z") == (2001, 10, pytz.utc)
        assert instance.pythonvalue("2001-10+00:00") == (2001, 10, pytz.utc)
        assert instance.pythonvalue("-2001-10") == (-2001, 10, None)
        assert instance.pythonvalue("-20001-10") == (-20001, 10, None)

        with pytest.raises(builtins.ParseError):
            assert instance.pythonvalue("10-10")


class TestgYear:
    def test_xmlvalue(self):
        instance = builtins.gYear()
        instance.xmlvalue((2001, None)) == "2001"
        instance.xmlvalue((2001, pytz.utc)) == "2001Z"

    def test_pythonvalue(self):
        instance = builtins.gYear()
        assert instance.pythonvalue("2001") == (2001, None)
        assert instance.pythonvalue("2001+02:00") == (2001, pytz.FixedOffset(120))
        assert instance.pythonvalue("2001Z") == (2001, pytz.utc)
        assert instance.pythonvalue("2001+00:00") == (2001, pytz.utc)
        assert instance.pythonvalue("-2001") == (-2001, None)
        assert instance.pythonvalue("-20000") == (-20000, None)

        with pytest.raises(builtins.ParseError):
            assert instance.pythonvalue("99")


class TestgMonthDay:
    def test_xmlvalue(self):
        instance = builtins.gMonthDay()
        assert instance.xmlvalue((12, 30, None)) == "--12-30"

    def test_pythonvalue(self):
        instance = builtins.gMonthDay()
        assert instance.pythonvalue("--05-01") == (5, 1, None)
        assert instance.pythonvalue("--11-01Z") == (11, 1, pytz.utc)
        assert instance.pythonvalue("--11-01+02:00") == (11, 1, pytz.FixedOffset(120))
        assert instance.pythonvalue("--11-01-04:00") == (11, 1, pytz.FixedOffset(-240))
        assert instance.pythonvalue("--11-15") == (11, 15, None)
        assert instance.pythonvalue("--02-29") == (2, 29, None)

        with pytest.raises(builtins.ParseError):
            assert instance.pythonvalue("99")


class TestgMonth:
    def test_xmlvalue(self):
        instance = builtins.gMonth()
        assert instance.xmlvalue((12, None)) == "--12"

    def test_pythonvalue(self):
        instance = builtins.gMonth()
        assert instance.pythonvalue("--05") == (5, None)
        assert instance.pythonvalue("--11Z") == (11, pytz.utc)
        assert instance.pythonvalue("--11+02:00") == (11, pytz.FixedOffset(120))
        assert instance.pythonvalue("--11-04:00") == (11, pytz.FixedOffset(-240))
        assert instance.pythonvalue("--11") == (11, None)
        assert instance.pythonvalue("--02") == (2, None)

        with pytest.raises(builtins.ParseError):
            assert instance.pythonvalue("99")


class TestgDay:
    def test_xmlvalue(self):
        instance = builtins.gDay()

        value = (1, None)
        assert instance.xmlvalue(value) == "---01"

        value = (1, pytz.FixedOffset(120))
        assert instance.xmlvalue(value) == "---01+02:00"

        value = (1, pytz.FixedOffset(-240))
        assert instance.xmlvalue(value) == "---01-04:00"

    def test_pythonvalue(self):
        instance = builtins.gDay()
        assert instance.pythonvalue("---01") == (1, None)
        assert instance.pythonvalue("---01Z") == (1, pytz.utc)
        assert instance.pythonvalue("---01+02:00") == (1, pytz.FixedOffset(120))
        assert instance.pythonvalue("---01-04:00") == (1, pytz.FixedOffset(-240))
        assert instance.pythonvalue("---15") == (15, None)
        assert instance.pythonvalue("---31") == (31, None)
        with pytest.raises(builtins.ParseError):
            assert instance.pythonvalue("99")


class TestHexBinary:
    def test_xmlvalue(self):
        instance = builtins.HexBinary()
        assert instance.xmlvalue(b"\xFF") == b"\xFF"

    def test_pythonvalue(self):
        instance = builtins.HexBinary()
        assert instance.pythonvalue(b"\xFF") == b"\xFF"


class TestBase64Binary:
    def test_xmlvalue(self):
        instance = builtins.Base64Binary()
        assert instance.xmlvalue(b"hoi") == b"aG9p"

    def test_pythonvalue(self):
        instance = builtins.Base64Binary()
        assert instance.pythonvalue(b"aG9p") == b"hoi"


class TestAnyURI:
    def test_xmlvalue(self):
        instance = builtins.AnyURI()
        assert (
            instance.xmlvalue("http://test.python-zeep.org")
            == "http://test.python-zeep.org"
        )

    def test_pythonvalue(self):
        instance = builtins.AnyURI()
        assert (
            instance.pythonvalue("http://test.python-zeep.org")
            == "http://test.python-zeep.org"
        )


class TestInteger:
    def test_xmlvalue(self):
        instance = builtins.Integer()
        assert instance.xmlvalue(100) == "100"

    def test_pythonvalue(self):
        instance = builtins.Integer()
        assert instance.pythonvalue("100") == 100


class TestAnyType:
    def test_xmlvalue(self):
        instance = builtins.AnyType()
        assert (
            instance.xmlvalue("http://test.python-zeep.org")
            == "http://test.python-zeep.org"
        )

    def test_pythonvalue(self):
        instance = builtins.AnyType()
        assert (
            instance.pythonvalue("http://test.python-zeep.org")
            == "http://test.python-zeep.org"
        )
