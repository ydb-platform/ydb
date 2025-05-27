"""Tests for TR46 code."""

import os.path
import re
import unittest

import idna

_RE_UNICODE = re.compile("\\\\u([0-9a-fA-F]{4})")
_RE_SURROGATE = re.compile("[\ud800-\udbff][\udc00-\udfff]")
_SKIP_TESTS = [
    # These are strings that are illegal in IDNA 2008. Older versions of the UTS-46 test suite
    # had these denoted with the 'NV8' marker but this has been removed, so we need to manually
    # review exceptions and add them here to skip them as text vectors if they are invalid.
    "\U000102f7\u3002\u200d",
    "\U0001d7f5\u9681\u2bee\uff0e\u180d\u200c",
    "9\u9681\u2bee.\u180d\u200c",
    "\u00df\u200c\uaaf6\u18a5.\u22b6\u2d21\u2d16",
    "ss\u200c\uaaf6\u18a5.\u22b6\u2d21\u2d16",
    "\u00df\u200c\uaaf6\u18a5\uff0e\u22b6\u2d21\u2d16",
    "ss\u200c\uaaf6\u18a5\uff0e\u22b6\u2d21\u2d16",
    "\U00010a57\u200d\u3002\u2d09\u2d15",
    "\U00010a57\u200d\uff61\u2d09\u2d15",
    "\U0001d7cf\U0001da19\u2e16.\u200d",
    "1\U0001da19\u2e16.\u200d",
    "\U0001d7e04\U000e01d7\U0001d23b\uff0e\u200d\U000102f5\u26e7\u200d",
    "84\U000e01d7\U0001d23b.\u200d\U000102f5\u26e7\u200d",
    "\u00a1",
    "xn--7a",
    "\u19da",
    "xn--pkf",
    "\u2615",
    "xn--53h",
    "\U0001e937.\U00010b90\U0001e881\U00010e60\u0624",
    "\U0001e937.\U00010b90\U0001e881\U00010e60\u0648\u0654",
    "\U0001e915.\U00010b90\U0001e881\U00010e60\u0648\u0654",
    "\U0001e915.\U00010b90\U0001e881\U00010e60\u0624",
    "xn--ve6h.xn--jgb1694kz0b2176a",
    "\u00df\u3002\U000102f3\u2d0c\u0fb8",
    "ss\u3002\U000102f3\u2d0c\u0fb8",
    "ss.xn--lgd921mvv0m",
    "ss.\U000102f3\u2d0c\u0fb8",
    "xn--zca.xn--lgd921mvv0m",
    "\u00df.\U000102f3\u2d0c\u0fb8",
    "\u00df\uff61\U000102f3\u2d0c\u0fb8",
    "ss\uff61\U000102f3\u2d0c\u0fb8",
    "\u16ad\uff61\U0001d320\u00df\U00016af1",
    "\u16ad\u3002\U0001d320\u00df\U00016af1",
    "\u16ad\u3002\U0001d320SS\U00016af1",
    "\u16ad\u3002\U0001d320ss\U00016af1",
    "\u16ad\u3002\U0001d320Ss\U00016af1",
    "xn--hwe.xn--ss-ci1ub261a",
    "\u16ad.\U0001d320ss\U00016af1",
    "\u16ad.\U0001d320SS\U00016af1",
    "\u16ad.\U0001d320Ss\U00016af1",
    "xn--hwe.xn--zca4946pblnc",
    "\u16ad.\U0001d320\u00df\U00016af1",
    "\u16ad\uff61\U0001d320SS\U00016af1",
    "\u16ad\uff61\U0001d320ss\U00016af1",
    "\u16ad\uff61\U0001d320Ss\U00016af1",
    "\u2d1a\U000102f8\U000e0104\u30025\ud7f6\u103a",
    "xn--ilj2659d.xn--5-dug9054m",
    "\u2d1a\U000102f8.5\ud7f6\u103a",
    "\u2d1a\U000102f8\U000e0104\u3002\U0001d7dd\ud7f6\u103a",
    "xn--9-mfs8024b.",
    "9\u9681\u2bee.",
    "xn--ss-4epx629f.xn--ifh802b6a",
    "ss\uaaf6\u18a5.\u22b6\u2d21\u2d16",
    "xn--pt9c.xn--0kjya",
    "\U00010a57.\u2d09\u2d15",
    "\ua5f7\U00011180.\u075d\U00010a52",
    "xn--ju8a625r.xn--hpb0073k",
    "\u03c2.\u0641\u0645\u064a\U0001f79b1.",
    "\u03a3.\u0641\u0645\u064a\U0001f79b1.",
    "\u03c3.\u0641\u0645\u064a\U0001f79b1.",
    "xn--4xa.xn--1-gocmu97674d.",
    "xn--3xa.xn--1-gocmu97674d.",
    "xn--1-5bt6845n.",
    "1\U0001da19\u2e16.",
    "xn--84-s850a.xn--59h6326e",
    "84\U0001d23b.\U000102f5\u26e7",
    "xn--r97c.",
    "\U000102f7.",
    # These appear to be errors in the test vectors. All relate to incorrectly applying
    # bidi rules across label boundaries. Appears independently confirmed
    # at http://www.alvestrand.no/pipermail/idna-update/2017-January/007946.html
    "0\u00e0.\u05d0",
    "0a\u0300.\u05d0",
    "0A\u0300.\u05d0",
    "0\u00c0.\u05d0",
    "xn--0-sfa.xn--4db",
    "\u00e0\u02c7.\u05d0",
    "a\u0300\u02c7.\u05d0",
    "A\u0300\u02c7.\u05d0",
    "\u00c0\u02c7.\u05d0",
    "xn--0ca88g.xn--4db",
    "0A.\u05d0",
    "0a.\u05d0",
    "0a.xn--4db",
    "c.xn--0-eha.xn--4db",
    "c.0\u00fc.\u05d0",
    "c.0u\u0308.\u05d0",
    "C.0U\u0308.\u05d0",
    "C.0\u00dc.\u05d0",
    "C.0\u00fc.\u05d0",
    "C.0\u0075\u0308.\u05d0",
    "\u06b6\u06df\u3002\u2087\ua806",
    "\u06b6\u06df\u30027\ua806",
    "xn--pkb6f.xn--7-x93e",
    "\u06b6\u06df.7\ua806",
    "1.\uac7e6.\U00010c41\u06d0",
    "1.\u1100\u1165\u11b56.\U00010c41\u06d0",
    "1.xn--6-945e.xn--glb1794k",
]


def unicode_fixup(string):
    """Replace backslash-u-XXXX with appropriate unicode characters."""
    return _RE_SURROGATE.sub(
        lambda match: chr((ord(match.group(0)[0]) - 0xD800) * 0x400 + ord(match.group(0)[1]) - 0xDC00 + 0x10000),
        _RE_UNICODE.sub(lambda match: chr(int(match.group(1), 16)), string),
    )


def parse_idna_test_table(inputstream):
    """Parse IdnaTestV2.txt and return a list of tuples."""
    for lineno, line in enumerate(inputstream):
        line = line.decode("utf-8").strip()
        if "#" in line:
            line = line.split("#", 1)[0]
        if not line:
            continue
        yield ((lineno + 1, tuple(field.strip() for field in line.split(";"))))


class TestIdnaTest(unittest.TestCase):
    """Run one of the IdnaTestV2.txt test lines."""

    def __init__(self, lineno=None, fields=None):
        super().__init__()
        self.lineno = lineno
        self.fields = fields

    def id(self):
        return "{}.{}".format(super().id(), self.lineno)

    def shortDescription(self):
        if not self.fields:
            return ""
        return "IdnaTestV2.txt line {}: {}".format(self.lineno, "; ".join(self.fields))

    def runTest(self):
        if not self.fields:
            return
        (
            source,
            to_unicode,
            to_unicode_status,
            to_ascii,
            to_ascii_status,
            to_ascii_t,
            to_ascii_t_status,
        ) = self.fields
        if source in _SKIP_TESTS:
            return
        if not to_unicode:
            to_unicode = source
        if not to_unicode_status:
            to_unicode_status = "[]"
        if not to_ascii:
            to_ascii = to_unicode
        if not to_ascii_status:
            to_ascii_status = to_unicode_status
        if not to_ascii_t:
            to_ascii_t = to_ascii
        if not to_ascii_t_status:
            to_ascii_t_status = to_ascii_status

        try:
            output = idna.decode(source, uts46=True, strict=True)
            if to_unicode_status != "[]":
                self.fail("decode() did not emit required error {} for {}".format(to_unicode, repr(source)))
            self.assertEqual(output, to_unicode, "unexpected decode() output")
        except (idna.IDNAError, UnicodeError, ValueError) as exc:
            if str(exc).startswith("Unknown"):
                raise unittest.SkipTest("Test requires support for a newer" " version of Unicode than this Python supports")
            if to_unicode_status == "[]":
                raise

        try:
            output = idna.encode(source, uts46=True, strict=True).decode("ascii")
            if to_ascii_status != "[]":
                self.fail("encode() did not emit required error {} for {}".format(to_ascii_status, repr(source)))
            self.assertEqual(output, to_ascii, "unexpected encode() output")
        except (idna.IDNAError, UnicodeError, ValueError) as exc:
            if str(exc).startswith("Unknown"):
                raise unittest.SkipTest("Test requires support for a newer" " version of Unicode than this Python supports")
            if to_ascii_status == "[]":
                raise

        try:
            output = idna.encode(source, uts46=True, strict=True, transitional=True).decode("ascii")
            if to_ascii_t_status != "[]":
                self.fail(
                    "encode(transitional=True) did not emit required error {} for {}".format(to_ascii_t_status, repr(source))
                )
            self.assertEqual(output, to_ascii_t, "unexpected encode() output")
        except (idna.IDNAError, UnicodeError, ValueError) as exc:
            if str(exc).startswith("Unknown"):
                raise unittest.SkipTest("Test requires support for a newer" " version of Unicode than this Python supports")
            if to_ascii_t_status == "[]":
                raise


def load_tests(loader, tests, pattern):
    """Create a suite of all the individual tests."""
    suite = unittest.TestSuite()
    with open(os.path.join(os.path.dirname(__file__), "IdnaTestV2.txt"), "rb") as tests_file:
        suite.addTests(TestIdnaTest(lineno, fields) for lineno, fields in parse_idna_test_table(tests_file))
    return suite
