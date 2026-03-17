from __future__ import annotations

import codecs
import random
from io import BytesIO
from typing import Any

from w3lib.encoding import (
    html_body_declared_encoding,
    html_to_unicode,
    http_content_type_encoding,
    read_bom,
    resolve_encoding,
    to_unicode,
)


class TestRequestEncoding:
    utf8_fragments = [
        # Content-Type as meta http-equiv
        b"""<meta http-equiv="content-type" content="text/html;charset=UTF-8" />""",
        b"""\n<meta http-equiv="Content-Type"\ncontent="text/html; charset=utf-8">""",
        b"""<meta http-equiv="Content-Type" content="text/html" charset="utf-8">""",
        b"""<meta http-equiv=Content-Type content="text/html" charset='utf-8'>""",
        b"""<meta http-equiv="Content-Type" content\t=\n"text/html" charset\t="utf-8">""",
        b"""<meta content="text/html; charset=utf-8"\n http-equiv='Content-Type'>""",
        b""" bad html still supported < meta http-equiv='Content-Type'\n content="text/html; charset=utf-8">""",
        # html5 meta charset
        b"""<meta charset="utf-8">""",
        b"""<meta charset =\n"utf-8">""",
        # xml encoding
        b"""<?xml version="1.0" encoding="utf-8"?>""",
    ]

    def test_bom(self):
        # cjk water character in unicode
        water_unicode = "\u6c34"
        # BOM + water character encoded
        utf16be = b"\xfe\xff\x6c\x34"
        utf16le = b"\xff\xfe\x34\x6c"
        utf32be = b"\x00\x00\xfe\xff\x00\x00\x6c\x34"
        utf32le = b"\xff\xfe\x00\x00\x34\x6c\x00\x00"
        for string in (utf16be, utf16le, utf32be, utf32le):
            bom_encoding, bom = read_bom(string)
            assert bom_encoding is not None
            assert bom is not None
            decoded = string[len(bom) :].decode(bom_encoding)
            assert water_unicode == decoded
        # Body without BOM
        enc, bom = read_bom(b"foo")
        assert enc is None
        assert bom is None
        # Empty body
        enc, bom = read_bom(b"")
        assert enc is None
        assert bom is None

    def test_http_encoding_header(self):
        header_value = "Content-Type: text/html; charset=ISO-8859-4"
        extracted = http_content_type_encoding(header_value)
        assert extracted == "iso8859-4"
        assert http_content_type_encoding("something else") is None

    def test_html_body_declared_encoding(self):
        for fragment in self.utf8_fragments:
            encoding = html_body_declared_encoding(fragment)
            assert encoding == "utf-8", fragment
        assert None is html_body_declared_encoding(b"something else")
        assert (
            html_body_declared_encoding(
                b"""
            <head></head><body>
            this isn't searched
            <meta charset="utf-8">
        """
            )
            is None
        )
        assert (
            html_body_declared_encoding(
                b"""<meta http-equiv="Fake-Content-Type-Header" content="text/html; charset=utf-8">"""
            )
            is None
        )

    def test_html_body_declared_encoding_unicode(self):
        # html_body_declared_encoding should work when unicode body is passed
        assert html_body_declared_encoding("something else") is None

        for fragment in self.utf8_fragments:
            encoding = html_body_declared_encoding(fragment.decode("utf8"))
            assert encoding == "utf-8", fragment

        assert (
            html_body_declared_encoding(
                """
            <head></head><body>
            this isn't searched
            <meta charset="utf-8">
        """
            )
            is None
        )
        assert (
            html_body_declared_encoding(
                """<meta http-equiv="Fake-Content-Type-Header" content="text/html; charset=utf-8">"""
            )
            is None
        )


class TestCodecsEncoding:
    def test_resolve_encoding(self):
        assert resolve_encoding("latin1") == "cp1252"
        assert resolve_encoding(" Latin-1") == "cp1252"
        assert resolve_encoding("gb_2312-80") == "gb18030"
        assert resolve_encoding("unknown encoding") is None


class TestUnicodeDecoding:
    def test_utf8(self):
        assert to_unicode(b"\xc2\xa3", "utf-8") == "\xa3"

    def test_invalid_utf8(self):
        assert to_unicode(b"\xc2\xc2\xa3", "utf-8") == "\ufffd\xa3"


def ct(charset: str | None) -> str | None:
    return "Content-Type: text/html; charset=" + charset if charset else None


def norm_encoding(enc: str) -> str:
    return codecs.lookup(enc).name


class TestHtmlConversion:
    def test_unicode_body(self):
        unicode_string = "\u043a\u0438\u0440\u0438\u043b\u043b\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u0442\u0435\u043a\u0441\u0442"
        original_string = unicode_string.encode("cp1251")
        _, body_unicode = html_to_unicode(ct("cp1251"), original_string)
        # check body_as_unicode
        assert isinstance(body_unicode, str)
        assert body_unicode == unicode_string

    def _assert_encoding(
        self,
        content_type: str | None,
        body: bytes,
        expected_encoding: str,
        expected_unicode: str | list[str],
    ) -> None:
        assert not isinstance(body, str)
        encoding, body_unicode = html_to_unicode(ct(content_type), body)
        assert isinstance(body_unicode, str)
        assert norm_encoding(encoding) == norm_encoding(expected_encoding)

        if isinstance(expected_unicode, str):
            assert body_unicode == expected_unicode
        else:
            assert body_unicode in expected_unicode, (
                f"{body_unicode} is not in {expected_unicode}"
            )

    def test_content_type_and_conversion(self):
        """Test content type header is interpreted and text converted as
        expected
        """
        self._assert_encoding("utf-8", b"\xc2\xa3", "utf-8", "\xa3")
        # something like this in the scrapy tests - but that's invalid?
        # self._assert_encoding('', "\xa3", 'utf-8', "\xa3")
        # iso-8859-1 is overridden to cp1252
        self._assert_encoding("iso-8859-1", b"\xa3", "cp1252", "\xa3")
        self._assert_encoding("", b"\xc2\xa3", "utf-8", "\xa3")
        self._assert_encoding("none", b"\xc2\xa3", "utf-8", "\xa3")
        self._assert_encoding("gb2312", b"\xa8D", "gb18030", "\u2015")
        self._assert_encoding("gbk", b"\xa8D", "gb18030", "\u2015")
        self._assert_encoding("big5", b"\xf9\xda", "big5hkscs", "\u6052")

    def test_invalid_utf8_encoded_body_with_valid_utf8_BOM(self):
        # unlike scrapy, the BOM is stripped
        self._assert_encoding(
            "utf-8", b"\xef\xbb\xbfWORD\xe3\xabWORD2", "utf-8", "WORD\ufffdWORD2"
        )
        self._assert_encoding(
            None, b"\xef\xbb\xbfWORD\xe3\xabWORD2", "utf-8", "WORD\ufffdWORD2"
        )

    def test_utf8_unexpected_end_of_data_with_valid_utf8_BOM(self):
        # Python implementations handle unexpected end of UTF8 data
        # differently (see https://bugs.pypy.org/issue1536).
        # It is hard to fix this for PyPy in w3lib, so the test
        # is permissive.

        # unlike scrapy, the BOM is stripped
        self._assert_encoding(
            "utf-8",
            b"\xef\xbb\xbfWORD\xe3\xab",
            "utf-8",
            ["WORD\ufffd\ufffd", "WORD\ufffd"],
        )
        self._assert_encoding(
            None,
            b"\xef\xbb\xbfWORD\xe3\xab",
            "utf-8",
            ["WORD\ufffd\ufffd", "WORD\ufffd"],
        )

    def test_replace_wrong_encoding(self):
        """Test invalid chars are replaced properly"""
        _, body_unicode = html_to_unicode(ct("utf-8"), b"PREFIX\xe3\xabSUFFIX")
        # XXX: Policy for replacing invalid chars may suffer minor variations
        # but it should always contain the unicode replacement char ('\ufffd')
        assert "\ufffd" in body_unicode, repr(body_unicode)
        assert "PREFIX" in body_unicode, repr(body_unicode)
        assert "SUFFIX" in body_unicode, repr(body_unicode)

        # Do not destroy html tags due to encoding bugs
        _, body_unicode = html_to_unicode(ct("utf-8"), b"\xf0<span>value</span>")
        assert "<span>value</span>" in body_unicode, repr(body_unicode)

    def _assert_encoding_detected(
        self,
        content_type: str | None,
        expected_encoding: str,
        body: bytes,
        **kwargs: Any,
    ) -> None:
        assert not isinstance(body, str)
        encoding, body_unicode = html_to_unicode(ct(content_type), body, **kwargs)
        assert isinstance(body_unicode, str)
        assert norm_encoding(encoding) == norm_encoding(expected_encoding)

    def test_BOM(self):
        # utf-16 cases already tested, as is the BOM detection function

        # BOM takes precedence, ahead of the http header
        bom_be_str = codecs.BOM_UTF16_BE + "hi".encode("utf-16-be")
        expected = "hi"
        self._assert_encoding("utf-8", bom_be_str, "utf-16-be", expected)

        # BOM is stripped when present
        bom_utf8_str = codecs.BOM_UTF8 + b"hi"
        self._assert_encoding("utf-8", bom_utf8_str, "utf-8", "hi")
        self._assert_encoding(None, bom_utf8_str, "utf-8", "hi")

    def test_utf16_32(self):
        # tools.ietf.org/html/rfc2781 section 4.3

        # USE BOM and strip it
        bom_be_str = codecs.BOM_UTF16_BE + "hi".encode("utf-16-be")
        self._assert_encoding("utf-16", bom_be_str, "utf-16-be", "hi")
        self._assert_encoding(None, bom_be_str, "utf-16-be", "hi")

        bom_le_str = codecs.BOM_UTF16_LE + "hi".encode("utf-16-le")
        self._assert_encoding("utf-16", bom_le_str, "utf-16-le", "hi")
        self._assert_encoding(None, bom_le_str, "utf-16-le", "hi")

        bom_be_str = codecs.BOM_UTF32_BE + "hi".encode("utf-32-be")
        self._assert_encoding("utf-32", bom_be_str, "utf-32-be", "hi")
        self._assert_encoding(None, bom_be_str, "utf-32-be", "hi")

        bom_le_str = codecs.BOM_UTF32_LE + "hi".encode("utf-32-le")
        self._assert_encoding("utf-32", bom_le_str, "utf-32-le", "hi")
        self._assert_encoding(None, bom_le_str, "utf-32-le", "hi")

        # if there is no BOM,  big endian should be chosen
        self._assert_encoding("utf-16", "hi".encode("utf-16-be"), "utf-16-be", "hi")
        self._assert_encoding("utf-32", "hi".encode("utf-32-be"), "utf-32-be", "hi")

    def test_python_crash(self):
        random.seed(42)
        buf = BytesIO()
        for _ in range(150000):
            buf.write(bytes([random.randint(0, 255)]))  # noqa: S311
        to_unicode(buf.getvalue(), "utf-16-le")
        to_unicode(buf.getvalue(), "utf-16-be")
        to_unicode(buf.getvalue(), "utf-32-le")
        to_unicode(buf.getvalue(), "utf-32-be")

    def test_html_encoding(self):
        # extracting the encoding from raw html is tested elsewhere
        body = b"""blah blah < meta   http-equiv="Content-Type"
            content="text/html; charset=iso-8859-1"> other stuff"""
        self._assert_encoding_detected(None, "cp1252", body)

        # header encoding takes precedence
        self._assert_encoding_detected("utf-8", "utf-8", body)
        # BOM encoding takes precedence
        self._assert_encoding_detected(None, "utf-8", codecs.BOM_UTF8 + body)

    def test_autodetect(self):
        def asciif(x):
            return "ascii"

        body = b"""<meta charset="utf-8">"""
        # body encoding takes precedence
        self._assert_encoding_detected(None, "utf-8", body, auto_detect_fun=asciif)
        # if no other encoding, the auto detect encoding is used.
        self._assert_encoding_detected(
            None, "ascii", b"no encoding info", auto_detect_fun=asciif
        )

    def test_default_encoding(self):
        # if no other method available, the default encoding of utf-8 is used
        self._assert_encoding_detected(None, "utf-8", b"no encoding info")
        # this can be overridden
        self._assert_encoding_detected(
            None, "ascii", b"no encoding info", default_encoding="ascii"
        )

    def test_empty_body(self):
        # if no other method available, the default encoding of utf-8 is used
        self._assert_encoding_detected(None, "utf-8", b"")
