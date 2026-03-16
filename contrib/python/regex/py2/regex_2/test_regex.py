from __future__ import with_statement
# from test.test_support import run_unittest
from weakref import proxy
import copy
import pickle
import re
import regex
import string
import unittest

# _AssertRaisesContext is defined here because the class doesn't exist before
# Python 2.7.
class _AssertRaisesContext(object):
    """A context manager used to implement TestCase.assertRaises* methods."""

    def __init__(self, expected, test_case, expected_regexp=None):
        self.expected = expected
        self.failureException = test_case.failureException
        self.expected_regexp = expected_regexp

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            try:
                exc_name = self.expected.__name__
            except AttributeError:
                exc_name = str(self.expected)
            raise self.failureException(
                "%s not raised" % exc_name)
        if not issubclass(exc_type, self.expected):
            # let unexpected exceptions pass through
            return False
        self.exception = exc_value # store for later retrieval
        if self.expected_regexp is None:
            return True

        expected_regexp = self.expected_regexp
        if isinstance(expected_regexp, basestring):
            expected_regexp = re.compile(expected_regexp)
        if not expected_regexp.search(str(exc_value)):
            raise self.failureException('"%s" does not match "%s"' %
                     (expected_regexp.pattern, str(exc_value)))
        return True

class RegexTests(unittest.TestCase):
    PATTERN_CLASS = "<type '_regex.Pattern'>"
    FLAGS_WITH_COMPILED_PAT = "cannot process flags argument with a compiled pattern"
    INVALID_GROUP_REF = "invalid group reference"
    MISSING_GT = "missing >"
    BAD_GROUP_NAME = "bad character in group name"
    MISSING_GROUP_NAME = "missing group name"
    MISSING_LT = "missing <"
    UNKNOWN_GROUP_I = "unknown group"
    UNKNOWN_GROUP = "unknown group"
    BAD_ESCAPE = r"bad escape \(end of pattern\)"
    BAD_OCTAL_ESCAPE = r"bad escape \\"
    BAD_SET = "unterminated character set"
    STR_PAT_ON_BYTES = "cannot use a string pattern on a bytes-like object"
    BYTES_PAT_ON_STR = "cannot use a bytes pattern on a string-like object"
    STR_PAT_BYTES_TEMPL = "expected str instance, bytes found"
    BYTES_PAT_STR_TEMPL = "expected a bytes-like object, str found"
    BYTES_PAT_UNI_FLAG = "cannot use UNICODE flag with a bytes pattern"
    MIXED_FLAGS = "ASCII, LOCALE and UNICODE flags are mutually incompatible"
    MISSING_RPAREN = "missing \\)"
    TRAILING_CHARS = "unbalanced parenthesis"
    BAD_CHAR_RANGE = "bad character range"
    NOTHING_TO_REPEAT = "nothing to repeat"
    MULTIPLE_REPEAT = "multiple repeat"
    OPEN_GROUP = "cannot refer to an open group"
    DUPLICATE_GROUP = "duplicate group"
    CANT_TURN_OFF = "bad inline flags: cannot turn flags off"
    UNDEF_CHAR_NAME = "undefined character name"

    # assertRaisesRegex is defined here because the method isn't in the
    # superclass before Python 2.7.
    def assertRaisesRegex(self, expected_exception, expected_regexp,
                           callable_obj=None, *args, **kwargs):
        """Asserts that the message in a raised exception matches a regexp.

        Args:
            expected_exception: Exception class expected to be raised.
            expected_regexp: Regexp (re pattern object or string) expected
                    to be found in error message.
            callable_obj: Function to be called.
            args: Extra args.
            kwargs: Extra kwargs.
        """
        context = _AssertRaisesContext(expected_exception, self, expected_regexp)
        if callable_obj is None:
            return context
        with context:
            callable_obj(*args, **kwargs)

    def assertTypedEqual(self, actual, expect, msg=None):
        self.assertEqual(actual, expect, msg)

        def recurse(actual, expect):
            if isinstance(expect, (tuple, list)):
                for x, y in zip(actual, expect):
                    recurse(x, y)
            else:
                self.assertIs(type(actual), type(expect), msg)

        recurse(actual, expect)

    def test_weakref(self):
        s = 'QabbbcR'
        x = regex.compile('ab+c')
        y = proxy(x)
        if x.findall('QabbbcR') != y.findall('QabbbcR'):
            self.fail()

    def test_search_star_plus(self):
        self.assertEqual(regex.search('a*', 'xxx').span(0), (0, 0))
        self.assertEqual(regex.search('x*', 'axx').span(), (0, 0))
        self.assertEqual(regex.search('x+', 'axx').span(0), (1, 3))
        self.assertEqual(regex.search('x+', 'axx').span(), (1, 3))
        self.assertEqual(regex.search('x', 'aaa'), None)
        self.assertEqual(regex.match('a*', 'xxx').span(0), (0, 0))
        self.assertEqual(regex.match('a*', 'xxx').span(), (0, 0))
        self.assertEqual(regex.match('x*', 'xxxa').span(0), (0, 3))
        self.assertEqual(regex.match('x*', 'xxxa').span(), (0, 3))
        self.assertEqual(regex.match('a+', 'xxx'), None)

    def bump_num(self, matchobj):
        int_value = int(matchobj[0])
        return str(int_value + 1)

    def test_basic_regex_sub(self):
        self.assertEqual(regex.sub("(?i)b+", "x", "bbbb BBBB"), 'x x')
        self.assertEqual(regex.sub(r'\d+', self.bump_num, '08.2 -2 23x99y'),
          '9.3 -3 24x100y')
        self.assertEqual(regex.sub(r'\d+', self.bump_num, '08.2 -2 23x99y', 3),
          '9.3 -3 23x99y')

        self.assertEqual(regex.sub('.', lambda m: r"\n", 'x'), "\\n")
        self.assertEqual(regex.sub('.', r"\n", 'x'), "\n")

        self.assertEqual(regex.sub('(?P<a>x)', r'\g<a>\g<a>', 'xx'), 'xxxx')
        self.assertEqual(regex.sub('(?P<a>x)', r'\g<a>\g<1>', 'xx'), 'xxxx')
        self.assertEqual(regex.sub('(?P<unk>x)', r'\g<unk>\g<unk>', 'xx'),
          'xxxx')
        self.assertEqual(regex.sub('(?P<unk>x)', r'\g<1>\g<1>', 'xx'), 'xxxx')

        self.assertEqual(regex.sub('a', r'\t\n\v\r\f\a\b\B\Z\a\A\w\W\s\S\d\D',
          'a'), "\t\n\v\r\f\a\b\\B\\Z\a\\A\\w\\W\\s\\S\\d\\D")
        self.assertEqual(regex.sub('a', '\t\n\v\r\f\a', 'a'), "\t\n\v\r\f\a")
        self.assertEqual(regex.sub('a', '\t\n\v\r\f\a', 'a'), chr(9) + chr(10)
          + chr(11) + chr(13) + chr(12) + chr(7))

        self.assertEqual(regex.sub(r'^\s*', 'X', 'test'), 'Xtest')

        self.assertEqual(regex.sub(ur"x", ur"\x0A", u"x"), u"\n")
        self.assertEqual(regex.sub(ur"x", ur"\u000A", u"x"), u"\n")
        self.assertEqual(regex.sub(ur"x", ur"\U0000000A", u"x"), u"\n")
        self.assertEqual(regex.sub(ur"x", ur"\N{LATIN CAPITAL LETTER A}",
          u"x"), u"A")

        self.assertEqual(regex.sub(r"x", r"\x0A", "x"), "\n")
        self.assertEqual(regex.sub(r"x", r"\u000A", "x"), "\\u000A")
        self.assertEqual(regex.sub(r"x", r"\U0000000A", "x"),
          "\\U0000000A")
        self.assertEqual(regex.sub(r"x", r"\N{LATIN CAPITAL LETTER A}",
          "x"), "\\N{LATIN CAPITAL LETTER A}")

    def test_bug_449964(self):
        # Fails for group followed by other escape.
        self.assertEqual(regex.sub(r'(?P<unk>x)', r'\g<1>\g<1>\b', 'xx'),
          "xx\bxx\b")

    def test_bug_449000(self):
        # Test for sub() on escaped characters.
        self.assertEqual(regex.sub(r'\r\n', r'\n', 'abc\r\ndef\r\n'),
          "abc\ndef\n")
        self.assertEqual(regex.sub('\r\n', r'\n', 'abc\r\ndef\r\n'),
          "abc\ndef\n")
        self.assertEqual(regex.sub(r'\r\n', '\n', 'abc\r\ndef\r\n'),
          "abc\ndef\n")
        self.assertEqual(regex.sub('\r\n', '\n', 'abc\r\ndef\r\n'),
          "abc\ndef\n")

    def test_bug_1140(self):
        # regex.sub(x, y, u'') should return u'', not '', and
        # regex.sub(x, y, '') should return '', not u''.
        # Also:
        # regex.sub(x, y, unicode(x)) should return unicode(y), and
        # regex.sub(x, y, str(x)) should return
        #     str(y) if isinstance(y, str) else unicode(y).
        for x in 'x', u'x':
            for y in 'y', u'y':
                z = regex.sub(x, y, u'')
                self.assertEqual((type(z), z), (unicode, u''))
                z = regex.sub(x, y, '')
                self.assertEqual((type(z), z), (str, ''))
                z = regex.sub(x, y, unicode(x))
                self.assertEqual((type(z), z), (unicode, unicode(y)))
                z = regex.sub(x, y, str(x))
                self.assertEqual((type(z), z), (type(y), y))

    def test_bug_1661(self):
        # Verify that flags do not get silently ignored with compiled patterns
        pattern = regex.compile('.')
        self.assertRaisesRegex(ValueError, self.FLAGS_WITH_COMPILED_PAT,
          lambda: regex.match(pattern, 'A', regex.I))
        self.assertRaisesRegex(ValueError, self.FLAGS_WITH_COMPILED_PAT,
          lambda: regex.search(pattern, 'A', regex.I))
        self.assertRaisesRegex(ValueError, self.FLAGS_WITH_COMPILED_PAT,
          lambda: regex.findall(pattern, 'A', regex.I))
        self.assertRaisesRegex(ValueError, self.FLAGS_WITH_COMPILED_PAT,
          lambda: regex.compile(pattern, regex.I))

    def test_bug_3629(self):
        # A regex that triggered a bug in the sre-code validator
        self.assertEqual(repr(type(regex.compile("(?P<quote>)(?(quote))"))),
          self.PATTERN_CLASS)

    def test_sub_template_numeric_escape(self):
        # Bug 776311 and friends.
        self.assertEqual(regex.sub('x', r'\0', 'x'), "\0")
        self.assertEqual(regex.sub('x', r'\000', 'x'), "\000")
        self.assertEqual(regex.sub('x', r'\001', 'x'), "\001")
        self.assertEqual(regex.sub('x', r'\008', 'x'), "\0" + "8")
        self.assertEqual(regex.sub('x', r'\009', 'x'), "\0" + "9")
        self.assertEqual(regex.sub('x', r'\111', 'x'), "\111")
        self.assertEqual(regex.sub('x', r'\117', 'x'), "\117")

        self.assertEqual(regex.sub('x', r'\1111', 'x'), "\1111")
        self.assertEqual(regex.sub('x', r'\1111', 'x'), "\111" + "1")

        self.assertEqual(regex.sub('x', r'\00', 'x'), '\x00')
        self.assertEqual(regex.sub('x', r'\07', 'x'), '\x07')
        self.assertEqual(regex.sub('x', r'\08', 'x'), "\0" + "8")
        self.assertEqual(regex.sub('x', r'\09', 'x'), "\0" + "9")
        self.assertEqual(regex.sub('x', r'\0a', 'x'), "\0" + "a")

        self.assertEqual(regex.sub(u'x', ur'\400', u'x'), u"\u0100")
        self.assertEqual(regex.sub(u'x', ur'\777', u'x'), u"\u01FF")
        self.assertEqual(regex.sub('x', r'\400', 'x'), "\x00")
        self.assertEqual(regex.sub('x', r'\777', 'x'), "\xFF")

        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\1', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\8', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\9', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\11', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\18', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\1a', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\90', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\99', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\118', 'x')) # r'\11' + '8'
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\11a', 'x'))
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\181', 'x')) # r'\18' + '1'
        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.sub('x', r'\800', 'x')) # r'\80' + '0'

        # In Python 2.3 (etc), these loop endlessly in sre_parser.py.
        self.assertEqual(regex.sub('(((((((((((x)))))))))))', r'\11', 'x'),
          'x')
        self.assertEqual(regex.sub('((((((((((y))))))))))(.)', r'\118', 'xyz'),
          'xz8')
        self.assertEqual(regex.sub('((((((((((y))))))))))(.)', r'\11a', 'xyz'),
          'xza')

    def test_qualified_re_sub(self):
        self.assertEqual(regex.sub('a', 'b', 'aaaaa'), 'bbbbb')
        self.assertEqual(regex.sub('a', 'b', 'aaaaa', 1), 'baaaa')

    def test_bug_114660(self):
        self.assertEqual(regex.sub(r'(\S)\s+(\S)', r'\1 \2', 'hello  there'),
          'hello there')

    def test_bug_462270(self):
        # Test for empty sub() behaviour, see SF bug #462270
        self.assertEqual(regex.sub('(?V1)x*', '-', 'abxd'), '-a-b--d-')
        self.assertEqual(regex.sub('x+', '-', 'abxd'), 'ab-d')

    def test_bug_14462(self):
        # chr(255) is not a valid identifier in Python 2.
        group_name = u'\xFF'
        self.assertRaisesRegex(regex.error, self.BAD_GROUP_NAME, lambda:
          regex.search(ur'(?P<' + group_name + '>a)', u'a'))

    def test_symbolic_refs(self):
        self.assertRaisesRegex(regex.error, self.MISSING_GT, lambda:
          regex.sub('(?P<a>x)', r'\g<a', 'xx'))
        self.assertRaisesRegex(regex.error, self.MISSING_GROUP_NAME, lambda:
          regex.sub('(?P<a>x)', r'\g<', 'xx'))
        self.assertRaisesRegex(regex.error, self.MISSING_LT, lambda:
          regex.sub('(?P<a>x)', r'\g', 'xx'))
        self.assertRaisesRegex(regex.error, self.BAD_GROUP_NAME, lambda:
          regex.sub('(?P<a>x)', r'\g<a a>', 'xx'))
        self.assertRaisesRegex(regex.error, self.BAD_GROUP_NAME, lambda:
          regex.sub('(?P<a>x)', r'\g<1a1>', 'xx'))
        self.assertRaisesRegex(IndexError, self.UNKNOWN_GROUP_I, lambda:
          regex.sub('(?P<a>x)', r'\g<ab>', 'xx'))

        # The new behaviour of unmatched but valid groups is to treat them like
        # empty matches in the replacement template, like in Perl.
        self.assertEqual(regex.sub('(?P<a>x)|(?P<b>y)', r'\g<b>', 'xx'), '')
        self.assertEqual(regex.sub('(?P<a>x)|(?P<b>y)', r'\2', 'xx'), '')

        # The old behaviour was to raise it as an IndexError.
        self.assertRaisesRegex(regex.error, self.BAD_GROUP_NAME, lambda:
          regex.sub('(?P<a>x)', r'\g<-1>', 'xx'))

    def test_re_subn(self):
        self.assertEqual(regex.subn("(?i)b+", "x", "bbbb BBBB"), ('x x', 2))
        self.assertEqual(regex.subn("b+", "x", "bbbb BBBB"), ('x BBBB', 1))
        self.assertEqual(regex.subn("b+", "x", "xyz"), ('xyz', 0))
        self.assertEqual(regex.subn("b*", "x", "xyz"), ('xxxyxzx', 4))
        self.assertEqual(regex.subn("b*", "x", "xyz", 2), ('xxxyz', 2))

    def test_re_split(self):
        self.assertEqual(regex.split(":", ":a:b::c"), ['', 'a', 'b', '', 'c'])
        self.assertEqual(regex.split(":*", ":a:b::c"), ['', 'a', 'b', 'c'])
        self.assertEqual(regex.split("(:*)", ":a:b::c"), ['', ':', 'a', ':',
          'b', '::', 'c'])
        self.assertEqual(regex.split("(?::*)", ":a:b::c"), ['', 'a', 'b', 'c'])
        self.assertEqual(regex.split("(:)*", ":a:b::c"), ['', ':', 'a', ':',
          'b', ':', 'c'])
        self.assertEqual(regex.split("([b:]+)", ":a:b::c"), ['', ':', 'a',
          ':b::', 'c'])
        self.assertEqual(regex.split("(b)|(:+)", ":a:b::c"), ['', None, ':',
          'a', None, ':', '', 'b', None, '', None, '::', 'c'])
        self.assertEqual(regex.split("(?:b)|(?::+)", ":a:b::c"), ['', 'a', '',
          '', 'c'])

        self.assertEqual(regex.split("x", "xaxbxc"), ['', 'a', 'b', 'c'])
        self.assertEqual([m for m in regex.splititer("x", "xaxbxc")], ['', 'a',
          'b', 'c'])

        self.assertEqual(regex.split("(?r)x", "xaxbxc"), ['c', 'b', 'a', ''])
        self.assertEqual([m for m in regex.splititer("(?r)x", "xaxbxc")], ['c',
          'b', 'a', ''])

        self.assertEqual(regex.split("(x)|(y)", "xaxbxc"), ['', 'x', None, 'a',
          'x', None, 'b', 'x', None, 'c'])
        self.assertEqual([m for m in regex.splititer("(x)|(y)", "xaxbxc")],
          ['', 'x', None, 'a', 'x', None, 'b', 'x', None, 'c'])

        self.assertEqual(regex.split("(?r)(x)|(y)", "xaxbxc"), ['c', 'x', None,
          'b', 'x', None, 'a', 'x', None, ''])
        self.assertEqual([m for m in regex.splititer("(?r)(x)|(y)", "xaxbxc")],
          ['c', 'x', None, 'b', 'x', None, 'a', 'x', None, ''])

        self.assertEqual(regex.split(r"(?V1)\b", "a b c"), ['', 'a', ' ', 'b',
          ' ', 'c', ''])
        self.assertEqual(regex.split(r"(?V1)\m", "a b c"), ['', 'a ', 'b ',
          'c'])
        self.assertEqual(regex.split(r"(?V1)\M", "a b c"), ['a', ' b', ' c',
          ''])

    def test_qualified_re_split(self):
        self.assertEqual(regex.split(":", ":a:b::c", 2), ['', 'a', 'b::c'])
        self.assertEqual(regex.split(':', 'a:b:c:d', 2), ['a', 'b', 'c:d'])
        self.assertEqual(regex.split("(:)", ":a:b::c", 2), ['', ':', 'a', ':',
          'b::c'])

        self.assertEqual(regex.split("(:*)", ":a:b::c", 2), ['', ':', 'a', ':',
          'b::c'])

    def test_re_findall(self):
        self.assertEqual(regex.findall(":+", "abc"), [])
        self.assertEqual(regex.findall(":+", "a:b::c:::d"), [':', '::', ':::'])
        self.assertEqual(regex.findall("(:+)", "a:b::c:::d"), [':', '::',
          ':::'])
        self.assertEqual(regex.findall("(:)(:*)", "a:b::c:::d"), [(':', ''),
          (':', ':'), (':', '::')])

        self.assertEqual(regex.findall(r"\((?P<test>.{0,5}?TEST)\)",
          "(MY TEST)"), ["MY TEST"])
        self.assertEqual(regex.findall(r"\((?P<test>.{0,3}?TEST)\)",
          "(MY TEST)"), ["MY TEST"])
        self.assertEqual(regex.findall(r"\((?P<test>.{0,3}?T)\)", "(MY T)"),
          ["MY T"])

        self.assertEqual(regex.findall(r"[^a]{2}[A-Z]", "\n  S"), ['  S'])
        self.assertEqual(regex.findall(r"[^a]{2,3}[A-Z]", "\n  S"), ['\n  S'])
        self.assertEqual(regex.findall(r"[^a]{2,3}[A-Z]", "\n   S"), ['   S'])

        self.assertEqual(regex.findall(r"X(Y[^Y]+?){1,2}( |Q)+DEF",
          "XYABCYPPQ\nQ DEF"), [('YPPQ\n', ' ')])

        self.assertEqual(regex.findall(r"(\nTest(\n+.+?){0,2}?)?\n+End",
          "\nTest\nxyz\nxyz\nEnd"), [('\nTest\nxyz\nxyz', '\nxyz')])

    def test_bug_117612(self):
        self.assertEqual(regex.findall(r"(a|(b))", "aba"), [('a', ''), ('b',
          'b'), ('a', '')])

    def test_re_match(self):
        self.assertEqual(regex.match('a', 'a')[:], ('a',))
        self.assertEqual(regex.match('(a)', 'a')[:], ('a', 'a'))
        self.assertEqual(regex.match(r'(a)', 'a')[0], 'a')
        self.assertEqual(regex.match(r'(a)', 'a')[1], 'a')
        self.assertEqual(regex.match(r'(a)', 'a').group(1, 1), ('a', 'a'))

        pat = regex.compile('((a)|(b))(c)?')
        self.assertEqual(pat.match('a')[:], ('a', 'a', 'a', None, None))
        self.assertEqual(pat.match('b')[:], ('b', 'b', None, 'b', None))
        self.assertEqual(pat.match('ac')[:], ('ac', 'a', 'a', None, 'c'))
        self.assertEqual(pat.match('bc')[:], ('bc', 'b', None, 'b', 'c'))
        self.assertEqual(pat.match('bc')[:], ('bc', 'b', None, 'b', 'c'))

        # A single group.
        m = regex.match('(a)', 'a')
        self.assertEqual(m.group(), 'a')
        self.assertEqual(m.group(0), 'a')
        self.assertEqual(m.group(1), 'a')
        self.assertEqual(m.group(1, 1), ('a', 'a'))

        pat = regex.compile('(?:(?P<a1>a)|(?P<b2>b))(?P<c3>c)?')
        self.assertEqual(pat.match('a').group(1, 2, 3), ('a', None, None))
        self.assertEqual(pat.match('b').group('a1', 'b2', 'c3'), (None, 'b',
          None))
        self.assertEqual(pat.match('ac').group(1, 'b2', 3), ('a', None, 'c'))

    def test_re_groupref_exists(self):
        self.assertEqual(regex.match(r'^(\()?([^()]+)(?(1)\))$', '(a)')[:],
          ('(a)', '(', 'a'))
        self.assertEqual(regex.match(r'^(\()?([^()]+)(?(1)\))$', 'a')[:], ('a',
          None, 'a'))
        self.assertEqual(regex.match(r'^(\()?([^()]+)(?(1)\))$', 'a)'), None)
        self.assertEqual(regex.match(r'^(\()?([^()]+)(?(1)\))$', '(a'), None)
        self.assertEqual(regex.match('^(?:(a)|c)((?(1)b|d))$', 'ab')[:], ('ab',
          'a', 'b'))
        self.assertEqual(regex.match('^(?:(a)|c)((?(1)b|d))$', 'cd')[:], ('cd',
          None, 'd'))
        self.assertEqual(regex.match('^(?:(a)|c)((?(1)|d))$', 'cd')[:], ('cd',
          None, 'd'))
        self.assertEqual(regex.match('^(?:(a)|c)((?(1)|d))$', 'a')[:], ('a',
          'a', ''))

        # Tests for bug #1177831: exercise groups other than the first group.
        p = regex.compile('(?P<g1>a)(?P<g2>b)?((?(g2)c|d))')
        self.assertEqual(p.match('abc')[:], ('abc', 'a', 'b', 'c'))
        self.assertEqual(p.match('ad')[:], ('ad', 'a', None, 'd'))
        self.assertEqual(p.match('abd'), None)
        self.assertEqual(p.match('ac'), None)

    def test_re_groupref(self):
        self.assertEqual(regex.match(r'^(\|)?([^()]+)\1$', '|a|')[:], ('|a|',
          '|', 'a'))
        self.assertEqual(regex.match(r'^(\|)?([^()]+)\1?$', 'a')[:], ('a',
          None, 'a'))
        self.assertEqual(regex.match(r'^(\|)?([^()]+)\1$', 'a|'), None)
        self.assertEqual(regex.match(r'^(\|)?([^()]+)\1$', '|a'), None)
        self.assertEqual(regex.match(r'^(?:(a)|c)(\1)$', 'aa')[:], ('aa', 'a',
          'a'))
        self.assertEqual(regex.match(r'^(?:(a)|c)(\1)?$', 'c')[:], ('c', None,
          None))

        self.assertEqual(regex.findall(r"(?i)(.{1,40}?),(.{1,40}?)(?:;)+(.{1,80}).{1,40}?\3(\ |;)+(.{1,80}?)\1",
          "TEST, BEST; LEST ; Lest 123 Test, Best"), [('TEST', ' BEST',
          ' LEST', ' ', '123 ')])

    def test_groupdict(self):
        self.assertEqual(regex.match('(?P<first>first) (?P<second>second)',
          'first second').groupdict(), {'first': 'first', 'second': 'second'})

    def test_expand(self):
        self.assertEqual(regex.match("(?P<first>first) (?P<second>second)",
          "first second").expand(r"\2 \1 \g<second> \g<first>"),
          'second first second first')

    def test_repeat_minmax(self):
        self.assertEqual(regex.match(r"^(\w){1}$", "abc"), None)
        self.assertEqual(regex.match(r"^(\w){1}?$", "abc"), None)
        self.assertEqual(regex.match(r"^(\w){1,2}$", "abc"), None)
        self.assertEqual(regex.match(r"^(\w){1,2}?$", "abc"), None)

        self.assertEqual(regex.match(r"^(\w){3}$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){1,3}$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){1,4}$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){3,4}?$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){3}?$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){1,3}?$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){1,4}?$", "abc")[1], 'c')
        self.assertEqual(regex.match(r"^(\w){3,4}?$", "abc")[1], 'c')

        self.assertEqual(regex.match("^x{1}$", "xxx"), None)
        self.assertEqual(regex.match("^x{1}?$", "xxx"), None)
        self.assertEqual(regex.match("^x{1,2}$", "xxx"), None)
        self.assertEqual(regex.match("^x{1,2}?$", "xxx"), None)

        self.assertEqual(regex.match("^x{1}", "xxx")[0], 'x')
        self.assertEqual(regex.match("^x{1}?", "xxx")[0], 'x')
        self.assertEqual(regex.match("^x{0,1}", "xxx")[0], 'x')
        self.assertEqual(regex.match("^x{0,1}?", "xxx")[0], '')

        self.assertEqual(bool(regex.match("^x{3}$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{1,3}$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{1,4}$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{3,4}?$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{3}?$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{1,3}?$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{1,4}?$", "xxx")), True)
        self.assertEqual(bool(regex.match("^x{3,4}?$", "xxx")), True)

        self.assertEqual(regex.match("^x{}$", "xxx"), None)
        self.assertEqual(bool(regex.match("^x{}$", "x{}")), True)

    def test_getattr(self):
        self.assertEqual(regex.compile("(?i)(a)(b)").pattern, '(?i)(a)(b)')
        self.assertEqual(regex.compile("(?i)(a)(b)").flags, regex.A | regex.I |
          regex.DEFAULT_VERSION)
        self.assertEqual(regex.compile(u"(?i)(a)(b)").flags, regex.I | regex.U
          | regex.DEFAULT_VERSION)
        self.assertEqual(regex.compile("(?i)(a)(b)").groups, 2)
        self.assertEqual(regex.compile("(?i)(a)(b)").groupindex, {})

        self.assertEqual(regex.compile("(?i)(?P<first>a)(?P<other>b)").groupindex,
          {'first': 1, 'other': 2})

        self.assertEqual(regex.match("(a)", "a").pos, 0)
        self.assertEqual(regex.match("(a)", "a").endpos, 1)

        self.assertEqual(regex.search("b(c)", "abcdef").pos, 0)
        self.assertEqual(regex.search("b(c)", "abcdef").endpos, 6)
        self.assertEqual(regex.search("b(c)", "abcdef").span(), (1, 3))
        self.assertEqual(regex.search("b(c)", "abcdef").span(1), (2, 3))

        self.assertEqual(regex.match("(a)", "a").string, 'a')
        self.assertEqual(regex.match("(a)", "a").regs, ((0, 1), (0, 1)))
        self.assertEqual(repr(type(regex.match("(a)", "a").re)),
          self.PATTERN_CLASS)

        # Issue 14260.
        p = regex.compile(r'abc(?P<n>def)')
        p.groupindex["n"] = 0
        self.assertEqual(p.groupindex["n"], 1)

    def test_special_escapes(self):
        self.assertEqual(regex.search(r"\b(b.)\b", "abcd abc bcd bx")[1], 'bx')
        self.assertEqual(regex.search(r"\B(b.)\B", "abc bcd bc abxd")[1], 'bx')
        self.assertEqual(regex.search(r"\b(b.)\b", "abcd abc bcd bx",
          regex.LOCALE)[1], 'bx')
        self.assertEqual(regex.search(r"\B(b.)\B", "abc bcd bc abxd",
          regex.LOCALE)[1], 'bx')
        self.assertEqual(regex.search(ur"\b(b.)\b", u"abcd abc bcd bx",
          regex.UNICODE)[1], u'bx')
        self.assertEqual(regex.search(ur"\B(b.)\B", u"abc bcd bc abxd",
          regex.UNICODE)[1], u'bx')

        self.assertEqual(regex.search(r"^abc$", "\nabc\n", regex.M)[0], 'abc')
        self.assertEqual(regex.search(r"^\Aabc\Z$", "abc", regex.M)[0], 'abc')
        self.assertEqual(regex.search(r"^\Aabc\Z$", "\nabc\n", regex.M), None)

        self.assertEqual(regex.search(ur"\b(b.)\b", u"abcd abc bcd bx")[1],
          u'bx')
        self.assertEqual(regex.search(ur"\B(b.)\B", u"abc bcd bc abxd")[1],
          u'bx')
        self.assertEqual(regex.search(ur"^abc$", u"\nabc\n", regex.M)[0],
          u'abc')
        self.assertEqual(regex.search(ur"^\Aabc\Z$", u"abc", regex.M)[0],
          u'abc')
        self.assertEqual(regex.search(ur"^\Aabc\Z$", u"\nabc\n", regex.M),
          None)

        self.assertEqual(regex.search(r"\d\D\w\W\s\S", "1aa! a")[0], '1aa! a')
        self.assertEqual(regex.search(r"\d\D\w\W\s\S", "1aa! a",
          regex.LOCALE)[0], '1aa! a')
        self.assertEqual(regex.search(ur"\d\D\w\W\s\S", u"1aa! a",
          regex.UNICODE)[0], u'1aa! a')

    def test_bigcharset(self):
        self.assertEqual(regex.match(ur"(?u)([\u2222\u2223])", u"\u2222")[1],
          u'\u2222')
        self.assertEqual(regex.match(ur"(?u)([\u2222\u2223])", u"\u2222",
          regex.UNICODE)[1], u'\u2222')
        self.assertEqual(u"".join(regex.findall(u".",
          u"e\xe8\xe9\xea\xeb\u0113\u011b\u0117", flags=regex.UNICODE)),
          u'e\xe8\xe9\xea\xeb\u0113\u011b\u0117')
        self.assertEqual(u"".join(regex.findall(ur"[e\xe8\xe9\xea\xeb\u0113\u011b\u0117]",
          u"e\xe8\xe9\xea\xeb\u0113\u011b\u0117", flags=regex.UNICODE)),
          u'e\xe8\xe9\xea\xeb\u0113\u011b\u0117')
        self.assertEqual(u"".join(regex.findall(ur"e|\xe8|\xe9|\xea|\xeb|\u0113|\u011b|\u0117",
          u"e\xe8\xe9\xea\xeb\u0113\u011b\u0117", flags=regex.UNICODE)),
          u'e\xe8\xe9\xea\xeb\u0113\u011b\u0117')

    def test_anyall(self):
        self.assertEqual(regex.match("a.b", "a\nb", regex.DOTALL)[0], "a\nb")
        self.assertEqual(regex.match("a.*b", "a\n\nb", regex.DOTALL)[0],
          "a\n\nb")

    def test_non_consuming(self):
        self.assertEqual(regex.match(r"(a(?=\s[^a]))", "a b")[1], 'a')
        self.assertEqual(regex.match(r"(a(?=\s[^a]*))", "a b")[1], 'a')
        self.assertEqual(regex.match(r"(a(?=\s[abc]))", "a b")[1], 'a')
        self.assertEqual(regex.match(r"(a(?=\s[abc]*))", "a bc")[1], 'a')
        self.assertEqual(regex.match(r"(a)(?=\s\1)", "a a")[1], 'a')
        self.assertEqual(regex.match(r"(a)(?=\s\1*)", "a aa")[1], 'a')
        self.assertEqual(regex.match(r"(a)(?=\s(abc|a))", "a a")[1], 'a')

        self.assertEqual(regex.match(r"(a(?!\s[^a]))", "a a")[1], 'a')
        self.assertEqual(regex.match(r"(a(?!\s[abc]))", "a d")[1], 'a')
        self.assertEqual(regex.match(r"(a)(?!\s\1)", "a b")[1], 'a')
        self.assertEqual(regex.match(r"(a)(?!\s(abc|a))", "a b")[1], 'a')

    def test_ignore_case(self):
        self.assertEqual(regex.match("abc", "ABC", regex.I)[0], 'ABC')
        self.assertEqual(regex.match(u"abc", u"ABC", regex.I)[0], u'ABC')

        self.assertEqual(regex.match(r"(a\s[^a]*)", "a bb", regex.I)[1],
          'a bb')
        self.assertEqual(regex.match(r"(a\s[abc])", "a b", regex.I)[1], 'a b')
        self.assertEqual(regex.match(r"(a\s[abc]*)", "a bb", regex.I)[1],
          'a bb')
        self.assertEqual(regex.match(r"((a)\s\2)", "a a", regex.I)[1], 'a a')
        self.assertEqual(regex.match(r"((a)\s\2*)", "a aa", regex.I)[1],
          'a aa')
        self.assertEqual(regex.match(r"((a)\s(abc|a))", "a a", regex.I)[1],
          'a a')
        self.assertEqual(regex.match(r"((a)\s(abc|a)*)", "a aa", regex.I)[1],
          'a aa')

        # Issue 3511.
        self.assertEqual(regex.match(r"[Z-a]", "_").span(), (0, 1))
        self.assertEqual(regex.match(r"(?i)[Z-a]", "_").span(), (0, 1))

        self.assertEqual(bool(regex.match(ur"(?iu)nao", u"nAo")), True)
        self.assertEqual(bool(regex.match(ur"(?iu)n\xE3o", u"n\xC3o")), True)
        self.assertEqual(bool(regex.match(ur"(?iu)n\xE3o", u"N\xC3O")), True)
        self.assertEqual(bool(regex.match(ur"(?iu)s", u"\u017F")), True)

    def test_case_folding(self):
        self.assertEqual(regex.search(ur"(?fiu)ss", u"SS").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)SS", u"ss").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)SS",
          u"\N{LATIN SMALL LETTER SHARP S}").span(), (0, 1))
        self.assertEqual(regex.search(ur"(?fi)\N{LATIN SMALL LETTER SHARP S}",
          u"SS").span(), (0, 2))

        self.assertEqual(regex.search(ur"(?fiu)\N{LATIN SMALL LIGATURE ST}",
          u"ST").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)ST",
          u"\N{LATIN SMALL LIGATURE ST}").span(), (0, 1))
        self.assertEqual(regex.search(ur"(?fiu)ST",
          u"\N{LATIN SMALL LIGATURE LONG S T}").span(), (0, 1))

        self.assertEqual(regex.search(ur"(?fiu)SST",
          u"\N{LATIN SMALL LETTER SHARP S}t").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)SST",
          u"s\N{LATIN SMALL LIGATURE LONG S T}").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)SST",
          u"s\N{LATIN SMALL LIGATURE ST}").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)\N{LATIN SMALL LIGATURE ST}",
          u"SST").span(), (1, 3))
        self.assertEqual(regex.search(ur"(?fiu)SST",
          u"s\N{LATIN SMALL LIGATURE ST}").span(), (0, 2))

        self.assertEqual(regex.search(ur"(?fiu)FFI",
          u"\N{LATIN SMALL LIGATURE FFI}").span(), (0, 1))
        self.assertEqual(regex.search(ur"(?fiu)FFI",
          u"\N{LATIN SMALL LIGATURE FF}i").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)FFI",
          u"f\N{LATIN SMALL LIGATURE FI}").span(), (0, 2))
        self.assertEqual(regex.search(ur"(?fiu)\N{LATIN SMALL LIGATURE FFI}",
          u"FFI").span(), (0, 3))
        self.assertEqual(regex.search(ur"(?fiu)\N{LATIN SMALL LIGATURE FF}i",
          u"FFI").span(), (0, 3))
        self.assertEqual(regex.search(ur"(?fiu)f\N{LATIN SMALL LIGATURE FI}",
          u"FFI").span(), (0, 3))

        sigma = u"\u03A3\u03C3\u03C2"
        for ch1 in sigma:
            for ch2 in sigma:
                if not regex.match(ur"(?fiu)" + ch1, ch2):
                    self.fail()

        self.assertEqual(bool(regex.search(ur"(?iuV1)ff", u"\uFB00\uFB01")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)ff", u"\uFB01\uFB00")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)fi", u"\uFB00\uFB01")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)fi", u"\uFB01\uFB00")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)fffi", u"\uFB00\uFB01")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)f\uFB03",
          u"\uFB00\uFB01")), True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)ff", u"\uFB00\uFB01")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)fi", u"\uFB00\uFB01")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)fffi", u"\uFB00\uFB01")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)f\uFB03",
          u"\uFB00\uFB01")), True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)f\uFB01", u"\uFB00i")),
          True)
        self.assertEqual(bool(regex.search(ur"(?iuV1)f\uFB01", u"\uFB00i")),
          True)

        self.assertEqual(regex.findall(ur"(?iuV0)\m(?:word){e<=3}\M(?<!\m(?:word){e<=1}\M)",
          u"word word2 word word3 word word234 word23 word"), [u"word234",
          u"word23"])
        self.assertEqual(regex.findall(ur"(?iuV1)\m(?:word){e<=3}\M(?<!\m(?:word){e<=1}\M)",
          u"word word2 word word3 word word234 word23 word"), [u"word234",
          u"word23"])

        self.assertEqual(regex.search(ur"(?fi)a\N{LATIN SMALL LIGATURE FFI}ne",
          u"  affine  ").span(), (2, 8))
        self.assertEqual(regex.search(ur"(?fi)a(?:\N{LATIN SMALL LIGATURE FFI}|x)ne",
           u"  affine  ").span(), (2, 8))
        self.assertEqual(regex.search(ur"(?fi)a(?:\N{LATIN SMALL LIGATURE FFI}|xy)ne",
           u"  affine  ").span(), (2, 8))
        self.assertEqual(regex.search(ur"(?fi)a\L<options>ne", u"affine",
          options=[u"\N{LATIN SMALL LIGATURE FFI}"]).span(), (0, 6))
        self.assertEqual(regex.search(ur"(?fi)a\L<options>ne",
          u"a\N{LATIN SMALL LIGATURE FFI}ne", options=[u"ffi"]).span(), (0, 4))

    def test_category(self):
        self.assertEqual(regex.match(r"(\s)", " ")[1], ' ')

    def test_not_literal(self):
        self.assertEqual(regex.search(r"\s([^a])", " b")[1], 'b')
        self.assertEqual(regex.search(r"\s([^a]*)", " bb")[1], 'bb')

    def test_search_coverage(self):
        self.assertEqual(regex.search(r"\s(b)", " b")[1], 'b')
        self.assertEqual(regex.search(r"a\s", "a ")[0], 'a ')

    def test_re_escape(self):
        p = ""
        self.assertEqual(regex.escape(p), p)
        for i in range(0, 256):
            p += chr(i)
            self.assertEqual(bool(regex.match(regex.escape(chr(i)), chr(i))),
              True)
            self.assertEqual(regex.match(regex.escape(chr(i)), chr(i)).span(),
              (0, 1))

        pat = regex.compile(regex.escape(p))
        self.assertEqual(pat.match(p).span(), (0, 256))

    def test_constants(self):
        if regex.I != regex.IGNORECASE:
            self.fail()
        if regex.L != regex.LOCALE:
            self.fail()
        if regex.M != regex.MULTILINE:
            self.fail()
        if regex.S != regex.DOTALL:
            self.fail()
        if regex.X != regex.VERBOSE:
            self.fail()

    def test_flags(self):
        for flag in [regex.I, regex.M, regex.X, regex.S, regex.L]:
            self.assertEqual(repr(type(regex.compile('^pattern$', flag))),
              self.PATTERN_CLASS)

    def test_sre_character_literals(self):
        for i in [0, 8, 16, 32, 64, 127, 128, 255]:
            self.assertEqual(bool(regex.match(r"\%03o" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"\%03o0" % i, chr(i) + "0")),
              True)
            self.assertEqual(bool(regex.match(r"\%03o8" % i, chr(i) + "8")),
              True)
            self.assertEqual(bool(regex.match(r"\x%02x" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"\x%02x0" % i, chr(i) + "0")),
              True)
            self.assertEqual(bool(regex.match(r"\x%02xz" % i, chr(i) + "z")),
              True)

        self.assertRaisesRegex(regex.error, self.INVALID_GROUP_REF, lambda:
          regex.match(r"\911", ""))

    def test_sre_character_class_literals(self):
        for i in [0, 8, 16, 32, 64, 127, 128, 255]:
            self.assertEqual(bool(regex.match(r"[\%03o]" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"[\%03o0]" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"[\%03o8]" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"[\x%02x]" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"[\x%02x0]" % i, chr(i))), True)
            self.assertEqual(bool(regex.match(r"[\x%02xz]" % i, chr(i))), True)

        self.assertRaisesRegex(regex.error, self.BAD_OCTAL_ESCAPE, lambda:
          regex.match(r"[\911]", ""))

    def test_bug_113254(self):
        self.assertEqual(regex.match(r'(a)|(b)', 'b').start(1), -1)
        self.assertEqual(regex.match(r'(a)|(b)', 'b').end(1), -1)
        self.assertEqual(regex.match(r'(a)|(b)', 'b').span(1), (-1, -1))

    def test_bug_527371(self):
        # Bug described in patches 527371/672491.
        self.assertEqual(regex.match(r'(a)?a','a').lastindex, None)
        self.assertEqual(regex.match(r'(a)(b)?b','ab').lastindex, 1)
        self.assertEqual(regex.match(r'(?P<a>a)(?P<b>b)?b','ab').lastgroup,
          'a')
        self.assertEqual(regex.match("(?P<a>a(b))", "ab").lastgroup, 'a')
        self.assertEqual(regex.match("((a))", "a").lastindex, 1)

    def test_bug_545855(self):
        # Bug 545855 -- This pattern failed to cause a compile error as it
        # should, instead provoking a TypeError.
        self.assertRaisesRegex(regex.error, self.BAD_SET, lambda:
          regex.compile('foo[a-'))

    def test_bug_418626(self):
        # Bugs 418626 at al. -- Testing Greg Chapman's addition of op code
        # SRE_OP_MIN_REPEAT_ONE for eliminating recursion on simple uses of
        # pattern '*?' on a long string.
        self.assertEqual(regex.match('.*?c', 10000 * 'ab' + 'cd').end(0),
          20001)
        self.assertEqual(regex.match('.*?cd', 5000 * 'ab' + 'c' + 5000 * 'ab' +
          'cde').end(0), 20003)
        self.assertEqual(regex.match('.*?cd', 20000 * 'abc' + 'de').end(0),
          60001)
        # Non-simple '*?' still used to hit the recursion limit, before the
        # non-recursive scheme was implemented.
        self.assertEqual(regex.search('(a|b)*?c', 10000 * 'ab' + 'cd').end(0),
          20001)

    def test_bug_612074(self):
        pat = u"[" + regex.escape(u"\u2039") + u"]"
        self.assertEqual(regex.compile(pat) and 1, 1)

    def test_stack_overflow(self):
        # Nasty cases that used to overflow the straightforward recursive
        # implementation of repeated groups.
        self.assertEqual(regex.match('(x)*', 50000 * 'x')[1], 'x')
        self.assertEqual(regex.match('(x)*y', 50000 * 'x' + 'y')[1], 'x')
        self.assertEqual(regex.match('(x)*?y', 50000 * 'x' + 'y')[1], 'x')

    def test_scanner(self):
        def s_ident(scanner, token): return token
        def s_operator(scanner, token): return "op%s" % token
        def s_float(scanner, token): return float(token)
        def s_int(scanner, token): return int(token)

        scanner = regex.Scanner([(r"[a-zA-Z_]\w*", s_ident), (r"\d+\.\d*",
          s_float), (r"\d+", s_int), (r"=|\+|-|\*|/", s_operator), (r"\s+",
            None), ])

        self.assertEqual(repr(type(scanner.scanner.scanner("").pattern)),
          self.PATTERN_CLASS)

        self.assertEqual(scanner.scan("sum = 3*foo + 312.50 + bar"), (['sum',
          'op=', 3, 'op*', 'foo', 'op+', 312.5, 'op+', 'bar'], ''))

    def test_bug_448951(self):
        # Bug 448951 (similar to 429357, but with single char match).
        # (Also test greedy matches.)
        for op in '', '?', '*':
            self.assertEqual(regex.match(r'((.%s):)?z' % op, 'z')[:], ('z',
              None, None))
            self.assertEqual(regex.match(r'((.%s):)?z' % op, 'a:z')[:], ('a:z',
              'a:', 'a'))

    def test_bug_725106(self):
        # Capturing groups in alternatives in repeats.
        self.assertEqual(regex.match('^((a)|b)*', 'abc')[:], ('ab', 'b', 'a'))
        self.assertEqual(regex.match('^(([ab])|c)*', 'abc')[:], ('abc', 'c',
          'b'))
        self.assertEqual(regex.match('^((d)|[ab])*', 'abc')[:], ('ab', 'b',
          None))
        self.assertEqual(regex.match('^((a)c|[ab])*', 'abc')[:], ('ab', 'b',
          None))
        self.assertEqual(regex.match('^((a)|b)*?c', 'abc')[:], ('abc', 'b',
          'a'))
        self.assertEqual(regex.match('^(([ab])|c)*?d', 'abcd')[:], ('abcd',
          'c', 'b'))
        self.assertEqual(regex.match('^((d)|[ab])*?c', 'abc')[:], ('abc', 'b',
          None))
        self.assertEqual(regex.match('^((a)c|[ab])*?c', 'abc')[:], ('abc', 'b',
          None))

    def test_bug_725149(self):
        # Mark_stack_base restoring before restoring marks.
        self.assertEqual(regex.match('(a)(?:(?=(b)*)c)*', 'abb')[:], ('a', 'a',
          None))
        self.assertEqual(regex.match('(a)((?!(b)*))*', 'abb')[:], ('a', 'a',
          None, None))

    def test_bug_764548(self):
        # Bug 764548, regex.compile() barfs on str/unicode subclasses.
        class my_unicode(str): pass
        pat = regex.compile(my_unicode("abc"))
        self.assertEqual(pat.match("xyz"), None)

    def test_finditer(self):
        it = regex.finditer(r":+", "a:b::c:::d")
        self.assertEqual([item[0] for item in it], [':', '::', ':::'])

    def test_bug_926075(self):
        if regex.compile('bug_926075') is regex.compile(u'bug_926075'):
            self.fail()

    def test_bug_931848(self):
        pattern = u"[\u002E\u3002\uFF0E\uFF61]"
        self.assertEqual(regex.compile(pattern).split("a.b.c"), ['a', 'b',
          'c'])

    def test_bug_581080(self):
        it = regex.finditer(r"\s", "a b")
        self.assertEqual(it.next().span(), (1, 2))
        self.assertRaises(StopIteration, lambda: it.next())

        scanner = regex.compile(r"\s").scanner("a b")
        self.assertEqual(scanner.search().span(), (1, 2))
        self.assertEqual(scanner.search(), None)

    def test_bug_817234(self):
        it = regex.finditer(r".*", "asdf")
        self.assertEqual(it.next().span(), (0, 4))
        self.assertEqual(it.next().span(), (4, 4))
        self.assertRaises(StopIteration, lambda: it.next())

    def test_empty_array(self):
        # SF buf 1647541.
        import array
        for typecode in 'cbBuhHiIlLfd':
            a = array.array(typecode)
            self.assertEqual(regex.compile("bla").match(a), None)
            self.assertEqual(regex.compile("").match(a)[1 : ], ())

    def test_inline_flags(self):
        # Bug #1700.
        upper_char = unichr(0x1ea0) # Latin Capital Letter A with Dot Below
        lower_char = unichr(0x1ea1) # Latin Small Letter A with Dot Below

        p = regex.compile(upper_char, regex.I | regex.U)
        self.assertEqual(bool(p.match(lower_char)), True)

        p = regex.compile(lower_char, regex.I | regex.U)
        self.assertEqual(bool(p.match(upper_char)), True)

        p = regex.compile('(?i)' + upper_char, regex.U)
        self.assertEqual(bool(p.match(lower_char)), True)

        p = regex.compile('(?i)' + lower_char, regex.U)
        self.assertEqual(bool(p.match(upper_char)), True)

        p = regex.compile('(?iu)' + upper_char)
        self.assertEqual(bool(p.match(lower_char)), True)

        p = regex.compile('(?iu)' + lower_char)
        self.assertEqual(bool(p.match(upper_char)), True)

        self.assertEqual(bool(regex.match(r"(?i)a", "A")), True)
        self.assertEqual(bool(regex.match(r"a(?i)", "A")), True)
        self.assertEqual(bool(regex.match(r"(?iV1)a", "A")), True)
        self.assertEqual(regex.match(r"a(?iV1)", "A"), None)

    def test_dollar_matches_twice(self):
        # $ matches the end of string, and just before the terminating \n.
        pattern = regex.compile('$')
        self.assertEqual(pattern.sub('#', 'a\nb\n'), 'a\nb#\n#')
        self.assertEqual(pattern.sub('#', 'a\nb\nc'), 'a\nb\nc#')
        self.assertEqual(pattern.sub('#', '\n'), '#\n#')

        pattern = regex.compile('$', regex.MULTILINE)
        self.assertEqual(pattern.sub('#', 'a\nb\n' ), 'a#\nb#\n#')
        self.assertEqual(pattern.sub('#', 'a\nb\nc'), 'a#\nb#\nc#')
        self.assertEqual(pattern.sub('#', '\n'), '#\n#')

    def test_ascii_and_unicode_flag(self):
        # Unicode patterns.
        for flags in (0, regex.UNICODE):
            pat = regex.compile(u'\xc0', flags | regex.IGNORECASE)
            self.assertEqual(bool(pat.match(u'\xe0')), True)
            pat = regex.compile(ur'\w', flags)
            self.assertEqual(bool(pat.match(u'\xe0')), True)

        pat = regex.compile(u'\xc0', regex.ASCII | regex.IGNORECASE)
        self.assertEqual(pat.match(u'\xe0'), None)
        pat = regex.compile(u'(?a)\xc0', regex.IGNORECASE)
        self.assertEqual(pat.match(u'\xe0'), None)
        pat = regex.compile(ur'\w', regex.ASCII)
        self.assertEqual(pat.match(u'\xe0'), None)
        pat = regex.compile(ur'(?a)\w')
        self.assertEqual(pat.match(u'\xe0'), None)

        # String patterns.
        for flags in (0, regex.ASCII):
            pat = regex.compile('\xc0', flags | regex.IGNORECASE)
            self.assertEqual(pat.match('\xe0'), None)
            pat = regex.compile(r'\w')
            self.assertEqual(pat.match('\xe0'), None)

        self.assertRaisesRegex(ValueError, self.MIXED_FLAGS, lambda:
          regex.compile(r'(?au)\w'))

    def test_subscripting_match(self):
        m = regex.match(r'(?<a>\w)', 'xy')
        if not m:
            self.fail("Failed: expected match but returned None")
        elif not m or m[0] != m.group(0) or m[1] != m.group(1):
            self.fail("Failed")
        if not m:
            self.fail("Failed: expected match but returned None")
        elif m[:] != ('x', 'x'):
            self.fail("Failed: expected \"('x', 'x')\" but got %s instead" %
              repr(m[:]))

    def test_new_named_groups(self):
        m0 = regex.match(r'(?P<a>\w)', 'x')
        m1 = regex.match(r'(?<a>\w)', 'x')
        if not (m0 and m1 and m0[:] == m1[:]):
            self.fail("Failed")

    def test_properties(self):
        self.assertEqual(regex.match('(?i)\xC0', '\xE0'), None)
        self.assertEqual(regex.match(r'(?i)\xC0', '\xE0'), None)
        self.assertEqual(regex.match(r'\w', '\xE0'), None)
        self.assertEqual(bool(regex.match(ur'(?u)\w', u'\xE0')), True)

        # Dropped the following test. It's not possible to determine what the
        # correct result should be in the general case.
#        self.assertEqual(bool(regex.match(r'(?L)\w', '\xE0')),
#          '\xE0'.isalnum())

        self.assertEqual(bool(regex.match(r'(?L)\d', '0')), True)
        self.assertEqual(bool(regex.match(r'(?L)\s', ' ')), True)
        self.assertEqual(bool(regex.match(r'(?L)\w', 'a')), True)
        self.assertEqual(regex.match(r'(?L)\d', '?'), None)
        self.assertEqual(regex.match(r'(?L)\s', '?'), None)
        self.assertEqual(regex.match(r'(?L)\w', '?'), None)

        self.assertEqual(regex.match(r'(?L)\D', '0'), None)
        self.assertEqual(regex.match(r'(?L)\S', ' '), None)
        self.assertEqual(regex.match(r'(?L)\W', 'a'), None)
        self.assertEqual(bool(regex.match(r'(?L)\D', '?')), True)
        self.assertEqual(bool(regex.match(r'(?L)\S', '?')), True)
        self.assertEqual(bool(regex.match(r'(?L)\W', '?')), True)

        self.assertEqual(bool(regex.match(ur'(?u)\p{Cyrillic}',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)(?iu)\p{Cyrillic}',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{IsCyrillic}',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script=Cyrillic}',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{InCyrillic}',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Block=Cyrillic}',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:Cyrillic:]]',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:IsCyrillic:]]',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:Script=Cyrillic:]]',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:InCyrillic:]]',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:Block=Cyrillic:]]',
          u'\N{CYRILLIC CAPITAL LETTER A}')), True)

        self.assertEqual(bool(regex.match(ur'(?u)\P{Cyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\P{IsCyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\P{Script=Cyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\P{InCyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\P{Block=Cyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{^Cyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{^IsCyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{^Script=Cyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{^InCyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{^Block=Cyrillic}',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:^Cyrillic:]]',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:^IsCyrillic:]]',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:^Script=Cyrillic:]]',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:^InCyrillic:]]',
          u'\N{LATIN CAPITAL LETTER A}')), True)
        self.assertEqual(bool(regex.match(ur'(?u)[[:^Block=Cyrillic:]]',
          u'\N{LATIN CAPITAL LETTER A}')), True)

        self.assertEqual(bool(regex.match(ur'(?u)\d', u'0')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\s', u' ')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\w', u'A')), True)
        self.assertEqual(regex.match(ur"(?u)\d", u"?"), None)
        self.assertEqual(regex.match(ur"(?u)\s", u"?"), None)
        self.assertEqual(regex.match(ur"(?u)\w", u"?"), None)
        self.assertEqual(regex.match(ur"(?u)\D", u"0"), None)
        self.assertEqual(regex.match(ur"(?u)\S", u" "), None)
        self.assertEqual(regex.match(ur"(?u)\W", u"A"), None)
        self.assertEqual(bool(regex.match(ur'(?u)\D', u'?')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\S', u'?')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\W', u'?')), True)

        self.assertEqual(bool(regex.match(ur'(?u)\p{L}', u'A')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{L}', u'a')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Lu}', u'A')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Ll}', u'a')), True)

        self.assertEqual(bool(regex.match(ur'(?u)(?i)a', u'a')), True)
        self.assertEqual(bool(regex.match(ur'(?u)(?i)a', u'A')), True)

        self.assertEqual(bool(regex.match(ur'(?u)\w', u'0')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\w', u'a')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\w', u'_')), True)

        self.assertEqual(regex.match(ur"(?u)\X", u"\xE0").span(), (0, 1))
        self.assertEqual(regex.match(ur"(?u)\X", u"a\u0300").span(), (0, 2))
        self.assertEqual(regex.findall(ur"(?u)\X",
          u"a\xE0a\u0300e\xE9e\u0301"), [u'a', u'\xe0', u'a\u0300', u'e',
          u'\xe9', u'e\u0301'])
        self.assertEqual(regex.findall(ur"(?u)\X{3}",
          u"a\xE0a\u0300e\xE9e\u0301"), [u'a\xe0a\u0300', u'e\xe9e\u0301'])
        self.assertEqual(regex.findall(ur"(?u)\X", u"\r\r\n\u0301A\u0301"),
          [u'\r', u'\r\n', u'\u0301', u'A\u0301'])

        self.assertEqual(bool(regex.match(ur'(?u)\p{Ll}', u'a')), True)

        chars_u = u"-09AZaz_\u0393\u03b3"
        chars_b = "-09AZaz_"
        word_set = set("Ll Lm Lo Lt Lu Mc Me Mn Nd Nl No Pc".split())

        tests = [
            (ur"(?u)\w", chars_u, u"09AZaz_\u0393\u03b3"),
            (ur"(?u)[[:word:]]", chars_u, u"09AZaz_\u0393\u03b3"),
            (ur"(?u)\W", chars_u, u"-"),
            (ur"(?u)[[:^word:]]", chars_u, u"-"),
            (ur"(?u)\d", chars_u, u"09"),
            (ur"(?u)[[:digit:]]", chars_u, u"09"),
            (ur"(?u)\D", chars_u, u"-AZaz_\u0393\u03b3"),
            (ur"(?u)[[:^digit:]]", chars_u, u"-AZaz_\u0393\u03b3"),
            (ur"(?u)[[:alpha:]]", chars_u, u"AZaz\u0393\u03b3"),
            (ur"(?u)[[:^alpha:]]", chars_u, u"-09_"),
            (ur"(?u)[[:alnum:]]", chars_u, u"09AZaz\u0393\u03b3"),
            (ur"(?u)[[:^alnum:]]", chars_u, u"-_"),
            (ur"(?u)[[:xdigit:]]", chars_u, u"09Aa"),
            (ur"(?u)[[:^xdigit:]]", chars_u, u"-Zz_\u0393\u03b3"),
            (ur"(?u)\p{InBasicLatin}", u"a\xE1", u"a"),
            (ur"(?u)\P{InBasicLatin}", u"a\xE1", u"\xE1"),
            (ur"(?iu)\p{InBasicLatin}", u"a\xE1", u"a"),
            (ur"(?iu)\P{InBasicLatin}", u"a\xE1", u"\xE1"),

            (r"(?L)\w", chars_b, "09AZaz_"),
            (r"(?L)[[:word:]]", chars_b, "09AZaz_"),
            (r"(?L)\W", chars_b, "-"),
            (r"(?L)[[:^word:]]", chars_b, "-"),
            (r"(?L)\d", chars_b, "09"),
            (r"(?L)[[:digit:]]", chars_b, "09"),
            (r"(?L)\D", chars_b, "-AZaz_"),
            (r"(?L)[[:^digit:]]", chars_b, "-AZaz_"),
            (r"(?L)[[:alpha:]]", chars_b, "AZaz"),
            (r"(?L)[[:^alpha:]]", chars_b, "-09_"),
            (r"(?L)[[:alnum:]]", chars_b, "09AZaz"),
            (r"(?L)[[:^alnum:]]", chars_b, "-_"),
            (r"(?L)[[:xdigit:]]", chars_b, "09Aa"),
            (r"(?L)[[:^xdigit:]]", chars_b, "-Zz_"),

            (r"\w", chars_b, "09AZaz_"),
            (r"[[:word:]]", chars_b, "09AZaz_"),
            (r"\W", chars_b, "-"),
            (r"[[:^word:]]", chars_b, "-"),
            (r"\d", chars_b, "09"),
            (r"[[:digit:]]", chars_b, "09"),
            (r"\D", chars_b, "-AZaz_"),
            (r"[[:^digit:]]", chars_b, "-AZaz_"),
            (r"[[:alpha:]]", chars_b, "AZaz"),
            (r"[[:^alpha:]]", chars_b, "-09_"),
            (r"[[:alnum:]]", chars_b, "09AZaz"),
            (r"[[:^alnum:]]", chars_b, "-_"),
            (r"[[:xdigit:]]", chars_b, "09Aa"),
            (r"[[:^xdigit:]]", chars_b, "-Zz_"),
        ]
        for pattern, chars, expected in tests:
            try:
                if chars[ : 0].join(regex.findall(pattern, chars)) != expected:
                    self.fail("Failed: %s" % pattern)
            except Exception, e:
                self.fail("Failed: %s raised %s" % (pattern, repr(e)))

        self.assertEqual(bool(regex.match(ur"(?u)\p{NumericValue=0}", u"0")),
          True)
        self.assertEqual(bool(regex.match(ur"(?u)\p{NumericValue=1/2}",
          u"\N{VULGAR FRACTION ONE HALF}")), True)
        self.assertEqual(bool(regex.match(ur"(?u)\p{NumericValue=0.5}",
          u"\N{VULGAR FRACTION ONE HALF}")), True)

    def test_word_class(self):
        self.assertEqual(regex.findall(ur"(?u)\w+",
          u" \u0939\u093f\u0928\u094d\u0926\u0940,"),
          [u'\u0939\u093f\u0928\u094d\u0926\u0940'])
        self.assertEqual(regex.findall(ur"(?u)\W+",
          u" \u0939\u093f\u0928\u094d\u0926\u0940,"), [u' ', u','])
        self.assertEqual(regex.split(ur"(?uV1)\b",
          u" \u0939\u093f\u0928\u094d\u0926\u0940,"), [u' ',
          u'\u0939\u093f\u0928\u094d\u0926\u0940', u','])
        self.assertEqual(regex.split(ur"(?uV1)\B",
          u" \u0939\u093f\u0928\u094d\u0926\u0940,"), [u'', u' \u0939',
          u'\u093f', u'\u0928', u'\u094d', u'\u0926', u'\u0940,', u''])

    def test_search_anchor(self):
        self.assertEqual(regex.findall(r"\G\w{2}", "abcd ef"), ['ab', 'cd'])

    def test_search_reverse(self):
        self.assertEqual(regex.findall(r"(?r).", "abc"), ['c', 'b', 'a'])
        self.assertEqual(regex.findall(r"(?r).", "abc", overlapped=True), ['c',
          'b', 'a'])
        self.assertEqual(regex.findall(r"(?r)..", "abcde"), ['de', 'bc'])
        self.assertEqual(regex.findall(r"(?r)..", "abcde", overlapped=True),
          ['de', 'cd', 'bc', 'ab'])
        self.assertEqual(regex.findall(r"(?r)(.)(-)(.)", "a-b-c",
          overlapped=True), [("b", "-", "c"), ("a", "-", "b")])

        self.assertEqual([m[0] for m in regex.finditer(r"(?r).", "abc")], ['c',
          'b', 'a'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)..", "abcde",
          overlapped=True)], ['de', 'cd', 'bc', 'ab'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r).", "abc")], ['c',
          'b', 'a'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)..", "abcde",
          overlapped=True)], ['de', 'cd', 'bc', 'ab'])

        self.assertEqual(regex.findall(r"^|\w+", "foo bar"), ['', 'foo',
          'bar'])
        self.assertEqual(regex.findall(r"(?V1)^|\w+", "foo bar"), ['', 'foo',
          'bar'])
        self.assertEqual(regex.findall(r"(?r)^|\w+", "foo bar"), ['bar', 'foo',
          ''])
        self.assertEqual(regex.findall(r"(?rV1)^|\w+", "foo bar"), ['bar',
          'foo', ''])

        self.assertEqual([m[0] for m in regex.finditer(r"^|\w+", "foo bar")],
          ['', 'foo', 'bar'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?V1)^|\w+",
          "foo bar")], ['', 'foo', 'bar'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)^|\w+",
          "foo bar")], ['bar', 'foo', ''])
        self.assertEqual([m[0] for m in regex.finditer(r"(?rV1)^|\w+",
          "foo bar")], ['bar', 'foo', ''])

        self.assertEqual(regex.findall(r"\G\w{2}", "abcd ef"), ['ab', 'cd'])
        self.assertEqual(regex.findall(r".{2}(?<=\G.*)", "abcd"), ['ab', 'cd'])
        self.assertEqual(regex.findall(r"(?r)\G\w{2}", "abcd ef"), [])
        self.assertEqual(regex.findall(r"(?r)\w{2}\G", "abcd ef"), ['ef'])

        self.assertEqual(regex.findall(r"q*", "qqwe"), ['qq', '', '', ''])
        self.assertEqual(regex.findall(r"(?V1)q*", "qqwe"), ['qq', '', '', ''])
        self.assertEqual(regex.findall(r"(?r)q*", "qqwe"), ['', '', 'qq', ''])
        self.assertEqual(regex.findall(r"(?rV1)q*", "qqwe"), ['', '', 'qq',
          ''])

        self.assertEqual(regex.findall(".", "abcd", pos=1, endpos=3), ['b',
          'c'])
        self.assertEqual(regex.findall(".", "abcd", pos=1, endpos=-1), ['b',
          'c'])
        self.assertEqual([m[0] for m in regex.finditer(".", "abcd", pos=1,
          endpos=3)], ['b', 'c'])
        self.assertEqual([m[0] for m in regex.finditer(".", "abcd", pos=1,
          endpos=-1)], ['b', 'c'])

        self.assertEqual([m[0] for m in regex.finditer("(?r).", "abcd", pos=1,
          endpos=3)], ['c', 'b'])
        self.assertEqual([m[0] for m in regex.finditer("(?r).", "abcd", pos=1,
          endpos=-1)], ['c', 'b'])
        self.assertEqual(regex.findall("(?r).", "abcd", pos=1, endpos=3), ['c',
          'b'])
        self.assertEqual(regex.findall("(?r).", "abcd", pos=1, endpos=-1),
          ['c', 'b'])

        self.assertEqual(regex.findall(r"[ab]", "aB", regex.I), ['a', 'B'])
        self.assertEqual(regex.findall(r"(?r)[ab]", "aB", regex.I), ['B', 'a'])

        self.assertEqual(regex.findall(r"(?r).{2}", "abc"), ['bc'])
        self.assertEqual(regex.findall(r"(?r).{2}", "abc", overlapped=True),
          ['bc', 'ab'])
        self.assertEqual(regex.findall(r"(\w+) (\w+)",
          "first second third fourth fifth"), [('first', 'second'), ('third',
          'fourth')])
        self.assertEqual(regex.findall(r"(?r)(\w+) (\w+)",
          "first second third fourth fifth"), [('fourth', 'fifth'), ('second',
          'third')])

        self.assertEqual([m[0] for m in regex.finditer(r"(?r).{2}", "abc")],
          ['bc'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r).{2}", "abc",
          overlapped=True)], ['bc', 'ab'])
        self.assertEqual([m[0] for m in regex.finditer(r"(\w+) (\w+)",
          "first second third fourth fifth")], ['first second',
          'third fourth'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)(\w+) (\w+)",
          "first second third fourth fifth")], ['fourth fifth',
          'second third'])

        self.assertEqual(regex.search("abcdef", "abcdef").span(), (0, 6))
        self.assertEqual(regex.search("(?r)abcdef", "abcdef").span(), (0, 6))
        self.assertEqual(regex.search("(?i)abcdef", "ABCDEF").span(), (0, 6))
        self.assertEqual(regex.search("(?ir)abcdef", "ABCDEF").span(), (0, 6))

        self.assertEqual(regex.sub(r"(.)", r"\1", "abc"), 'abc')
        self.assertEqual(regex.sub(r"(?r)(.)", r"\1", "abc"), 'abc')

    def test_atomic(self):
        # Issue 433030.
        self.assertEqual(regex.search(r"(?>a*)a", "aa"), None)

    def test_possessive(self):
        # Single-character non-possessive.
        self.assertEqual(regex.search(r"a?a", "a").span(), (0, 1))
        self.assertEqual(regex.search(r"a*a", "aaa").span(), (0, 3))
        self.assertEqual(regex.search(r"a+a", "aaa").span(), (0, 3))
        self.assertEqual(regex.search(r"a{1,3}a", "aaa").span(), (0, 3))

        # Multiple-character non-possessive.
        self.assertEqual(regex.search(r"(?:ab)?ab", "ab").span(), (0, 2))
        self.assertEqual(regex.search(r"(?:ab)*ab", "ababab").span(), (0, 6))
        self.assertEqual(regex.search(r"(?:ab)+ab", "ababab").span(), (0, 6))
        self.assertEqual(regex.search(r"(?:ab){1,3}ab", "ababab").span(), (0,
          6))

        # Single-character possessive.
        self.assertEqual(regex.search(r"a?+a", "a"), None)
        self.assertEqual(regex.search(r"a*+a", "aaa"), None)
        self.assertEqual(regex.search(r"a++a", "aaa"), None)
        self.assertEqual(regex.search(r"a{1,3}+a", "aaa"), None)

        # Multiple-character possessive.
        self.assertEqual(regex.search(r"(?:ab)?+ab", "ab"), None)
        self.assertEqual(regex.search(r"(?:ab)*+ab", "ababab"), None)
        self.assertEqual(regex.search(r"(?:ab)++ab", "ababab"), None)
        self.assertEqual(regex.search(r"(?:ab){1,3}+ab", "ababab"), None)

    def test_zerowidth(self):
        # Issue 3262.
        self.assertEqual(regex.split(r"\b", "a b"), ['a b'])
        self.assertEqual(regex.split(r"(?V1)\b", "a b"), ['', 'a', ' ', 'b',
          ''])

        # Issue 1647489.
        self.assertEqual(regex.findall(r"^|\w+", "foo bar"), ['', 'foo',
          'bar'])
        self.assertEqual([m[0] for m in regex.finditer(r"^|\w+", "foo bar")],
          ['', 'foo', 'bar'])
        self.assertEqual(regex.findall(r"(?r)^|\w+", "foo bar"), ['bar',
          'foo', ''])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)^|\w+",
          "foo bar")], ['bar', 'foo', ''])
        self.assertEqual(regex.findall(r"(?V1)^|\w+", "foo bar"), ['', 'foo',
          'bar'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?V1)^|\w+",
          "foo bar")], ['', 'foo', 'bar'])
        self.assertEqual(regex.findall(r"(?rV1)^|\w+", "foo bar"), ['bar',
          'foo', ''])
        self.assertEqual([m[0] for m in regex.finditer(r"(?rV1)^|\w+",
          "foo bar")], ['bar', 'foo', ''])

        self.assertEqual(regex.split("", "xaxbxc"), ['xaxbxc'])
        self.assertEqual([m for m in regex.splititer("", "xaxbxc")],
          ['xaxbxc'])

        self.assertEqual(regex.split("(?r)", "xaxbxc"), ['xaxbxc'])
        self.assertEqual([m for m in regex.splititer("(?r)", "xaxbxc")],
          ['xaxbxc'])

        self.assertEqual(regex.split("(?V1)", "xaxbxc"), ['', 'x', 'a', 'x',
          'b', 'x', 'c', ''])
        self.assertEqual([m for m in regex.splititer("(?V1)", "xaxbxc")], ['',
          'x', 'a', 'x', 'b', 'x', 'c', ''])

        self.assertEqual(regex.split("(?rV1)", "xaxbxc"), ['', 'c', 'x', 'b',
          'x', 'a', 'x', ''])
        self.assertEqual([m for m in regex.splititer("(?rV1)", "xaxbxc")], ['',
          'c', 'x', 'b', 'x', 'a', 'x', ''])

    def test_scoped_and_inline_flags(self):
        # Issues 433028, 433024, 433027.
        self.assertEqual(regex.search(r"(?i)Ab", "ab").span(), (0, 2))
        self.assertEqual(regex.search(r"(?i:A)b", "ab").span(), (0, 2))
        self.assertEqual(regex.search(r"A(?i)b", "ab").span(), (0, 2))
        self.assertEqual(regex.search(r"A(?iV1)b", "ab"), None)

        self.assertRaisesRegex(regex.error, self.CANT_TURN_OFF, lambda:
          regex.search(r"(?V0-i)Ab", "ab", flags=regex.I))

        self.assertEqual(regex.search(r"(?V0)Ab", "ab"), None)
        self.assertEqual(regex.search(r"(?V1)Ab", "ab"), None)
        self.assertEqual(regex.search(r"(?V1-i)Ab", "ab", flags=regex.I), None)
        self.assertEqual(regex.search(r"(?-i:A)b", "ab", flags=regex.I), None)
        self.assertEqual(regex.search(r"A(?V1-i)b", "ab",
          flags=regex.I).span(), (0, 2))

    def test_repeated_repeats(self):
        # Issue 2537.
        self.assertEqual(regex.search(r"(?:a+)+", "aaa").span(), (0, 3))
        self.assertEqual(regex.search(r"(?:(?:ab)+c)+", "abcabc").span(), (0,
          6))

        # Hg issue 286.
        self.assertEqual(regex.search(r"(?:a+){2,}", "aaa").span(), (0, 3))

    def test_lookbehind(self):
        self.assertEqual(regex.search(r"123(?<=a\d+)", "a123").span(), (1, 4))
        self.assertEqual(regex.search(r"123(?<=a\d+)", "b123"), None)
        self.assertEqual(regex.search(r"123(?<!a\d+)", "a123"), None)
        self.assertEqual(regex.search(r"123(?<!a\d+)", "b123").span(), (1, 4))

        self.assertEqual(bool(regex.match("(a)b(?<=b)(c)", "abc")), True)
        self.assertEqual(regex.match("(a)b(?<=c)(c)", "abc"), None)
        self.assertEqual(bool(regex.match("(a)b(?=c)(c)", "abc")), True)
        self.assertEqual(regex.match("(a)b(?=b)(c)", "abc"), None)

        self.assertEqual(regex.match("(?:(a)|(x))b(?<=(?(2)x|c))c", "abc"),
          None)
        self.assertEqual(regex.match("(?:(a)|(x))b(?<=(?(2)b|x))c", "abc"),
          None)
        self.assertEqual(bool(regex.match("(?:(a)|(x))b(?<=(?(2)x|b))c",
          "abc")), True)
        self.assertEqual(regex.match("(?:(a)|(x))b(?<=(?(1)c|x))c", "abc"),
          None)
        self.assertEqual(bool(regex.match("(?:(a)|(x))b(?<=(?(1)b|x))c",
          "abc")), True)

        self.assertEqual(bool(regex.match("(?:(a)|(x))b(?=(?(2)x|c))c",
          "abc")), True)
        self.assertEqual(regex.match("(?:(a)|(x))b(?=(?(2)c|x))c", "abc"),
          None)
        self.assertEqual(bool(regex.match("(?:(a)|(x))b(?=(?(2)x|c))c",
          "abc")), True)
        self.assertEqual(regex.match("(?:(a)|(x))b(?=(?(1)b|x))c", "abc"),
          None)
        self.assertEqual(bool(regex.match("(?:(a)|(x))b(?=(?(1)c|x))c",
          "abc")), True)

        self.assertEqual(regex.match("(a)b(?<=(?(2)x|c))(c)", "abc"), None)
        self.assertEqual(regex.match("(a)b(?<=(?(2)b|x))(c)", "abc"), None)
        self.assertEqual(regex.match("(a)b(?<=(?(1)c|x))(c)", "abc"), None)
        self.assertEqual(bool(regex.match("(a)b(?<=(?(1)b|x))(c)", "abc")),
          True)

        self.assertEqual(bool(regex.match("(a)b(?=(?(2)x|c))(c)", "abc")),
          True)
        self.assertEqual(regex.match("(a)b(?=(?(2)b|x))(c)", "abc"), None)
        self.assertEqual(bool(regex.match("(a)b(?=(?(1)c|x))(c)", "abc")),
          True)

        self.assertEqual(repr(type(regex.compile(r"(a)\2(b)"))),
          self.PATTERN_CLASS)

    def test_unmatched_in_sub(self):
        # Issue 1519638.

        self.assertEqual(regex.sub(r"(?V0)(x)?(y)?", r"\2-\1", "xy"), 'y-x')
        self.assertEqual(regex.sub(r"(?V1)(x)?(y)?", r"\2-\1", "xy"), 'y-x-')
        self.assertEqual(regex.sub(r"(?V0)(x)?(y)?", r"\2-\1", "x"), '-x')
        self.assertEqual(regex.sub(r"(?V1)(x)?(y)?", r"\2-\1", "x"), '-x-')
        self.assertEqual(regex.sub(r"(?V0)(x)?(y)?", r"\2-\1", "y"), 'y-')
        self.assertEqual(regex.sub(r"(?V1)(x)?(y)?", r"\2-\1", "y"), 'y--')

    def test_bug_10328 (self):
        # Issue 10328.
        pat = regex.compile(r'(?mV0)(?P<trailing_ws>[ \t]+\r*$)|(?P<no_final_newline>(?<=[^\n])\Z)')
        self.assertEqual(pat.subn(lambda m: '<' + m.lastgroup + '>',
          'foobar '), ('foobar<trailing_ws>', 1))
        self.assertEqual([m.group() for m in pat.finditer('foobar ')], [' ',
          ''])
        pat = regex.compile(r'(?mV1)(?P<trailing_ws>[ \t]+\r*$)|(?P<no_final_newline>(?<=[^\n])\Z)')
        self.assertEqual(pat.subn(lambda m: '<' + m.lastgroup + '>',
          'foobar '), ('foobar<trailing_ws><no_final_newline>', 2))
        self.assertEqual([m.group() for m in pat.finditer('foobar ')], [' ',
          ''])

    def test_overlapped(self):
        self.assertEqual(regex.findall(r"..", "abcde"), ['ab', 'cd'])
        self.assertEqual(regex.findall(r"..", "abcde", overlapped=True), ['ab',
          'bc', 'cd', 'de'])
        self.assertEqual(regex.findall(r"(?r)..", "abcde"), ['de', 'bc'])
        self.assertEqual(regex.findall(r"(?r)..", "abcde", overlapped=True),
          ['de', 'cd', 'bc', 'ab'])
        self.assertEqual(regex.findall(r"(.)(-)(.)", "a-b-c", overlapped=True),
          [("a", "-", "b"), ("b", "-", "c")])

        self.assertEqual([m[0] for m in regex.finditer(r"..", "abcde")], ['ab',
          'cd'])
        self.assertEqual([m[0] for m in regex.finditer(r"..", "abcde",
          overlapped=True)], ['ab', 'bc', 'cd', 'de'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)..", "abcde")],
          ['de', 'bc'])
        self.assertEqual([m[0] for m in regex.finditer(r"(?r)..", "abcde",
          overlapped=True)], ['de', 'cd', 'bc', 'ab'])

        self.assertEqual([m.groups() for m in regex.finditer(r"(.)(-)(.)",
          "a-b-c", overlapped=True)], [("a", "-", "b"), ("b", "-", "c")])
        self.assertEqual([m.groups() for m in regex.finditer(r"(?r)(.)(-)(.)",
          "a-b-c", overlapped=True)], [("b", "-", "c"), ("a", "-", "b")])

    def test_splititer(self):
        self.assertEqual(regex.split(r",", "a,b,,c,"), ['a', 'b', '', 'c', ''])
        self.assertEqual([m for m in regex.splititer(r",", "a,b,,c,")], ['a',
          'b', '', 'c', ''])

    def test_grapheme(self):
        self.assertEqual(regex.match(ur"(?u)\X", u"\xE0").span(), (0, 1))
        self.assertEqual(regex.match(ur"(?u)\X", u"a\u0300").span(), (0, 2))

        self.assertEqual(regex.findall(ur"(?u)\X",
          u"a\xE0a\u0300e\xE9e\u0301"), [u'a', u'\xe0', u'a\u0300', u'e',
          u'\xe9', u'e\u0301'])
        self.assertEqual(regex.findall(ur"(?u)\X{3}",
          u"a\xE0a\u0300e\xE9e\u0301"), [u'a\xe0a\u0300', u'e\xe9e\u0301'])
        self.assertEqual(regex.findall(ur"(?u)\X", u"\r\r\n\u0301A\u0301"),
          [u'\r', u'\r\n', u'\u0301', u'A\u0301'])

    def test_word_boundary(self):
        text = u'The quick ("brown") fox can\'t jump 32.3 feet, right?'
        self.assertEqual(regex.split(ur'(?V1)\b', text), [u'', u'The', u' ',
          u'quick', u' ("', u'brown', u'") ', u'fox', u' ', u'can', u"'", u't',
          u' ', u'jump', u' ', u'32', u'.', u'3', u' ', u'feet', u', ',
          u'right', u'?'])
        self.assertEqual(regex.split(ur'(?V1w)\b', text), [u'', u'The', u' ',
          u'quick', u' ', u'(', u'"', u'brown', u'"', u')', u' ', u'fox', u' ',
          u"can't", u' ', u'jump', u' ', u'32.3', u' ', u'feet', u',', u' ',
          u'right', u'?', u''])

        text = u"The  fox"
        self.assertEqual(regex.split(ur'(?V1)\b', text), [u'', u'The', u'  ',
          u'fox', u''])
        self.assertEqual(regex.split(ur'(?V1w)\b', text), [u'', u'The', u'  ',
          u'fox', u''])

        text = u"can't aujourd'hui l'objectif"
        self.assertEqual(regex.split(ur'(?V1)\b', text), [u'', u'can', u"'",
          u't', u' ', u'aujourd', u"'", u'hui', u' ', u'l', u"'", u'objectif',
          u''])
        self.assertEqual(regex.split(ur'(?V1w)\b', text), [u'', u"can't", u' ',
          u"aujourd'hui", u' ', u"l'objectif", u''])

    def test_line_boundary(self):
        self.assertEqual(regex.findall(r".+", "Line 1\nLine 2\n"), ["Line 1",
          "Line 2"])
        self.assertEqual(regex.findall(r".+", "Line 1\rLine 2\r"),
          ["Line 1\rLine 2\r"])
        self.assertEqual(regex.findall(r".+", "Line 1\r\nLine 2\r\n"),
          ["Line 1\r", "Line 2\r"])
        self.assertEqual(regex.findall(r"(?w).+", "Line 1\nLine 2\n"),
          ["Line 1", "Line 2"])
        self.assertEqual(regex.findall(r"(?w).+", "Line 1\rLine 2\r"),
          ["Line 1", "Line 2"])
        self.assertEqual(regex.findall(r"(?w).+", "Line 1\r\nLine 2\r\n"),
          ["Line 1", "Line 2"])

        self.assertEqual(regex.search(r"^abc", "abc").start(), 0)
        self.assertEqual(regex.search(r"^abc", "\nabc"), None)
        self.assertEqual(regex.search(r"^abc", "\rabc"), None)
        self.assertEqual(regex.search(r"(?w)^abc", "abc").start(), 0)
        self.assertEqual(regex.search(r"(?w)^abc", "\nabc"), None)
        self.assertEqual(regex.search(r"(?w)^abc", "\rabc"), None)

        self.assertEqual(regex.search(r"abc$", "abc").start(), 0)
        self.assertEqual(regex.search(r"abc$", "abc\n").start(), 0)
        self.assertEqual(regex.search(r"abc$", "abc\r"), None)
        self.assertEqual(regex.search(r"(?w)abc$", "abc").start(), 0)
        self.assertEqual(regex.search(r"(?w)abc$", "abc\n").start(), 0)
        self.assertEqual(regex.search(r"(?w)abc$", "abc\r").start(), 0)

        self.assertEqual(regex.search(r"(?m)^abc", "abc").start(), 0)
        self.assertEqual(regex.search(r"(?m)^abc", "\nabc").start(), 1)
        self.assertEqual(regex.search(r"(?m)^abc", "\rabc"), None)
        self.assertEqual(regex.search(r"(?mw)^abc", "abc").start(), 0)
        self.assertEqual(regex.search(r"(?mw)^abc", "\nabc").start(), 1)
        self.assertEqual(regex.search(r"(?mw)^abc", "\rabc").start(), 1)

        self.assertEqual(regex.search(r"(?m)abc$", "abc").start(), 0)
        self.assertEqual(regex.search(r"(?m)abc$", "abc\n").start(), 0)
        self.assertEqual(regex.search(r"(?m)abc$", "abc\r"), None)
        self.assertEqual(regex.search(r"(?mw)abc$", "abc").start(), 0)
        self.assertEqual(regex.search(r"(?mw)abc$", "abc\n").start(), 0)
        self.assertEqual(regex.search(r"(?mw)abc$", "abc\r").start(), 0)

    def test_branch_reset(self):
        self.assertEqual(regex.match(r"(?:(a)|(b))(c)", "ac").groups(), ('a',
          None, 'c'))
        self.assertEqual(regex.match(r"(?:(a)|(b))(c)", "bc").groups(), (None,
          'b', 'c'))
        self.assertEqual(regex.match(r"(?:(?<a>a)|(?<b>b))(?<c>c)",
          "ac").groups(), ('a', None, 'c'))
        self.assertEqual(regex.match(r"(?:(?<a>a)|(?<b>b))(?<c>c)",
          "bc").groups(), (None, 'b', 'c'))

        self.assertEqual(regex.match(r"(?<a>a)(?:(?<b>b)|(?<c>c))(?<d>d)",
          "abd").groups(), ('a', 'b', None, 'd'))
        self.assertEqual(regex.match(r"(?<a>a)(?:(?<b>b)|(?<c>c))(?<d>d)",
          "acd").groups(), ('a', None, 'c', 'd'))
        self.assertEqual(regex.match(r"(a)(?:(b)|(c))(d)", "abd").groups(),
          ('a', 'b', None, 'd'))

        self.assertEqual(regex.match(r"(a)(?:(b)|(c))(d)", "acd").groups(),
          ('a', None, 'c', 'd'))
        self.assertEqual(regex.match(r"(a)(?|(b)|(b))(d)", "abd").groups(),
          ('a', 'b', 'd'))
        self.assertEqual(regex.match(r"(?|(?<a>a)|(?<b>b))(c)", "ac").groups(),
          ('a', None, 'c'))
        self.assertEqual(regex.match(r"(?|(?<a>a)|(?<b>b))(c)", "bc").groups(),
          (None, 'b', 'c'))
        self.assertEqual(regex.match(r"(?|(?<a>a)|(?<a>b))(c)", "ac").groups(),
          ('a', 'c'))

        self.assertEqual(regex.match(r"(?|(?<a>a)|(?<a>b))(c)", "bc").groups(),
          ('b', 'c'))

        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(?<b>c)(?<a>d))(e)",
          "abe").groups(), ('a', 'b', 'e'))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(?<b>c)(?<a>d))(e)",
          "cde").groups(), ('d', 'c', 'e'))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(?<b>c)(d))(e)",
          "abe").groups(), ('a', 'b', 'e'))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(?<b>c)(d))(e)",
          "cde").groups(), ('d', 'c', 'e'))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(c)(d))(e)",
          "abe").groups(), ('a', 'b', 'e'))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(c)(d))(e)",
          "cde").groups(), ('c', 'd', 'e'))

        # Hg issue 87: Allow duplicate names of groups
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(c)(?<a>d))(e)",
          "abe").groups(), ("a", "b", "e"))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(c)(?<a>d))(e)",
          "abe").capturesdict(), {"a": ["a"], "b": ["b"]})
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(c)(?<a>d))(e)",
          "cde").groups(), ("d", None, "e"))
        self.assertEqual(regex.match(r"(?|(?<a>a)(?<b>b)|(c)(?<a>d))(e)",
          "cde").capturesdict(), {"a": ["c", "d"], "b": []})

    def test_set(self):
        self.assertEqual(regex.match(r"[a]", "a").span(), (0, 1))
        self.assertEqual(regex.match(r"(?i)[a]", "A").span(), (0, 1))
        self.assertEqual(regex.match(r"[a-b]", r"a").span(), (0, 1))
        self.assertEqual(regex.match(r"(?i)[a-b]", r"A").span(), (0, 1))

        self.assertEqual(regex.sub(r"(?V0)([][])", r"-", "a[b]c"), "a-b-c")

        self.assertEqual(regex.findall(ur"[\p{Alpha}]", u"a0"), [u"a"])
        self.assertEqual(regex.findall(ur"(?i)[\p{Alpha}]", u"A0"), [u"A"])

        self.assertEqual(regex.findall(ur"[a\p{Alpha}]", u"ab0"), [u"a", u"b"])
        self.assertEqual(regex.findall(ur"[a\P{Alpha}]", u"ab0"), [u"a", u"0"])
        self.assertEqual(regex.findall(ur"(?i)[a\p{Alpha}]", u"ab0"), [u"a",
          u"b"])
        self.assertEqual(regex.findall(ur"(?i)[a\P{Alpha}]", u"ab0"), [u"a",
          u"0"])

        self.assertEqual(regex.findall(ur"[a-b\p{Alpha}]", u"abC0"), [u"a",
          u"b", u"C"])
        self.assertEqual(regex.findall(ur"(?i)[a-b\p{Alpha}]", u"AbC0"), [u"A",
          u"b", u"C"])

        self.assertEqual(regex.findall(ur"[\p{Alpha}]", u"a0"), [u"a"])
        self.assertEqual(regex.findall(ur"[\P{Alpha}]", u"a0"), [u"0"])
        self.assertEqual(regex.findall(ur"[^\p{Alpha}]", u"a0"), [u"0"])
        self.assertEqual(regex.findall(ur"[^\P{Alpha}]", u"a0"), [u"a"])

        self.assertEqual("".join(regex.findall(r"[^\d-h]", "a^b12c-h")),
          'a^bc')
        self.assertEqual("".join(regex.findall(r"[^\dh]", "a^b12c-h")),
          'a^bc-')
        self.assertEqual("".join(regex.findall(r"[^h\s\db]", "a^b 12c-h")),
          'a^c-')
        self.assertEqual("".join(regex.findall(r"[^b\w]", "a b")), ' ')
        self.assertEqual("".join(regex.findall(r"[^b\S]", "a b")), ' ')
        self.assertEqual("".join(regex.findall(r"[^8\d]", "a 1b2")), 'a b')

        all_chars = u"".join(unichr(c) for c in range(0x100))
        self.assertEqual(len(regex.findall(ur"(?u)\p{ASCII}", all_chars)), 128)
        self.assertEqual(len(regex.findall(ur"(?u)\p{Letter}", all_chars)),
          117)
        self.assertEqual(len(regex.findall(ur"(?u)\p{Digit}", all_chars)), 10)

        # Set operators
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{ASCII}&&\p{Letter}]",
          all_chars)), 52)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{ASCII}&&\p{Alnum}&&\p{Letter}]",
          all_chars)), 52)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{ASCII}&&\p{Alnum}&&\p{Digit}]",
          all_chars)), 10)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{ASCII}&&\p{Cc}]",
          all_chars)), 33)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{ASCII}&&\p{Graph}]",
          all_chars)), 94)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{ASCII}--\p{Cc}]",
          all_chars)), 95)
        self.assertEqual(len(regex.findall(ur"(?u)[\p{Letter}\p{Digit}]",
          all_chars)), 127)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{Letter}||\p{Digit}]",
          all_chars)), 127)
        self.assertEqual(len(regex.findall(ur"(?u)\p{HexDigit}", all_chars)),
          22)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{HexDigit}~~\p{Digit}]",
          all_chars)), 12)
        self.assertEqual(len(regex.findall(ur"(?uV1)[\p{Digit}~~\p{HexDigit}]",
          all_chars)), 12)

        self.assertEqual(repr(type(regex.compile(r"(?V0)([][-])"))),
          self.PATTERN_CLASS)
        self.assertEqual(regex.findall(r"(?V1)[[a-z]--[aei]]", "abc"), ["b",
          "c"])
        self.assertEqual(regex.findall(r"(?iV1)[[a-z]--[aei]]", "abc"), ["b",
          "c"])
        self.assertEqual(regex.findall(r"(?V1)[\w--a]","abc"), ["b", "c"])
        self.assertEqual(regex.findall(r"(?iV1)[\w--a]","abc"), ["b", "c"])

    def test_various(self):
        tests = [
            # Test ?P< and ?P= extensions.
            ('(?P<foo_123', '', '', regex.error, self.MISSING_GT),      # Unterminated group identifier.
            ('(?P<1>a)', '', '', regex.error, self.BAD_GROUP_NAME),     # Begins with a digit.
            ('(?P<!>a)', '', '', regex.error, self.BAD_GROUP_NAME),     # Begins with an illegal char.
            ('(?P<foo!>a)', '', '', regex.error, self.BAD_GROUP_NAME),  # Begins with an illegal char.

            # Same tests, for the ?P= form.
            ('(?P<foo_123>a)(?P=foo_123', 'aa', '', regex.error,
              self.MISSING_RPAREN),
            ('(?P<foo_123>a)(?P=1)', 'aa', '1', repr('a')),
            ('(?P<foo_123>a)(?P=0)', 'aa', '', regex.error,
              self.BAD_GROUP_NAME),
            ('(?P<foo_123>a)(?P=-1)', 'aa', '', regex.error,
              self.BAD_GROUP_NAME),
            ('(?P<foo_123>a)(?P=!)', 'aa', '', regex.error,
              self.BAD_GROUP_NAME),
            ('(?P<foo_123>a)(?P=foo_124)', 'aa', '', regex.error,
              self.UNKNOWN_GROUP),  # Backref to undefined group.

            ('(?P<foo_123>a)', 'a', '1', repr('a')),
            ('(?P<foo_123>a)(?P=foo_123)', 'aa', '1', repr('a')),

            # Mal-formed \g in pattern treated as literal for compatibility.
            (r'(?<foo_123>a)\g<foo_123', 'aa', '', repr(None)),
            (r'(?<foo_123>a)\g<1>', 'aa', '1', repr('a')),
            (r'(?<foo_123>a)\g<!>', 'aa', '', repr(None)),
            (r'(?<foo_123>a)\g<foo_124>', 'aa', '', regex.error,
              self.UNKNOWN_GROUP),  # Backref to undefined group.

            ('(?<foo_123>a)', 'a', '1', repr('a')),
            (r'(?<foo_123>a)\g<foo_123>', 'aa', '1', repr('a')),

            # Test octal escapes.
            ('\\1', 'a', '', regex.error, self.INVALID_GROUP_REF),    # Backreference.
            ('[\\1]', '\1', '0', "'\\x01'"),  # Character.
            ('\\09', chr(0) + '9', '0', repr(chr(0) + '9')),
            ('\\141', 'a', '0', repr('a')),
            ('(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)\\119', 'abcdefghijklk9',
              '0,11', repr(('abcdefghijklk9', 'k'))),

            # Test \0 is handled everywhere.
            (r'\0', '\0', '0', repr('\0')),
            (r'[\0a]', '\0', '0', repr('\0')),
            (r'[a\0]', '\0', '0', repr('\0')),
            (r'[^a\0]', '\0', '', repr(None)),

            # Test various letter escapes.
            (r'\a[\b]\f\n\r\t\v', '\a\b\f\n\r\t\v', '0',
              repr('\a\b\f\n\r\t\v')),
            (r'[\a][\b][\f][\n][\r][\t][\v]', '\a\b\f\n\r\t\v', '0',
              repr('\a\b\f\n\r\t\v')),
            (r'\c\e\g\i\j\k\o\p\q\y\z', 'cegijkopqyz', '0',
              repr('cegijkopqyz')),
            (r'\xff', '\377', '0', repr(chr(255))),

            # New \x semantics.
            (r'\x00ffffffffffffff', '\377', '', repr(None)),
            (r'\x00f', '\017', '', repr(None)),
            (r'\x00fe', '\376', '', repr(None)),

            (r'\x00ff', '\377', '', repr(None)),
            (r'\t\n\v\r\f\a\g', '\t\n\v\r\f\ag', '0', repr('\t\n\v\r\f\ag')),
            ('\t\n\v\r\f\a\\g', '\t\n\v\r\f\ag', '0', repr('\t\n\v\r\f\ag')),
            (r'\t\n\v\r\f\a', '\t\n\v\r\f\a', '0', repr(chr(9) + chr(10) +
              chr(11) + chr(13) + chr(12) + chr(7))),
            (r'[\t][\n][\v][\r][\f][\b]', '\t\n\v\r\f\b', '0',
              repr('\t\n\v\r\f\b')),

            (r"^\w+=(\\[\000-\277]|[^\n\\])*",
              "SRC=eval.c g.c blah blah blah \\\\\n\tapes.c", '0',
              repr("SRC=eval.c g.c blah blah blah \\\\")),

            # Test that . only matches \n in DOTALL mode.
            ('a.b', 'acb', '0', repr('acb')),
            ('a.b', 'a\nb', '', repr(None)),
            ('a.*b', 'acc\nccb', '', repr(None)),
            ('a.{4,5}b', 'acc\nccb', '', repr(None)),
            ('a.b', 'a\rb', '0', repr('a\rb')),
            # The new behaviour is that the inline flag affects only what follows.
            ('a.b(?s)', 'a\nb', '0', repr('a\nb')),
            ('a.b(?sV1)', 'a\nb', '', repr(None)),
            ('(?s)a.b', 'a\nb', '0', repr('a\nb')),
            ('a.*(?s)b', 'acc\nccb', '0', repr('acc\nccb')),
            ('a.*(?sV1)b', 'acc\nccb', '', repr(None)),
            ('(?s)a.*b', 'acc\nccb', '0', repr('acc\nccb')),
            ('(?s)a.{4,5}b', 'acc\nccb', '0', repr('acc\nccb')),

            (')', '', '', regex.error, self.TRAILING_CHARS),           # Unmatched right bracket.
            ('', '', '0', "''"),    # Empty pattern.
            ('abc', 'abc', '0', repr('abc')),
            ('abc', 'xbc', '', repr(None)),
            ('abc', 'axc', '', repr(None)),
            ('abc', 'abx', '', repr(None)),
            ('abc', 'xabcy', '0', repr('abc')),
            ('abc', 'ababc', '0', repr('abc')),
            ('ab*c', 'abc', '0', repr('abc')),
            ('ab*bc', 'abc', '0', repr('abc')),

            ('ab*bc', 'abbc', '0', repr('abbc')),
            ('ab*bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab+bc', 'abbc', '0', repr('abbc')),
            ('ab+bc', 'abc', '', repr(None)),
            ('ab+bc', 'abq', '', repr(None)),
            ('ab+bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab?bc', 'abbc', '0', repr('abbc')),
            ('ab?bc', 'abc', '0', repr('abc')),
            ('ab?bc', 'abbbbc', '', repr(None)),
            ('ab?c', 'abc', '0', repr('abc')),

            ('^abc$', 'abc', '0', repr('abc')),
            ('^abc$', 'abcc', '', repr(None)),
            ('^abc', 'abcc', '0', repr('abc')),
            ('^abc$', 'aabc', '', repr(None)),
            ('abc$', 'aabc', '0', repr('abc')),
            ('^', 'abc', '0', repr('')),
            ('$', 'abc', '0', repr('')),
            ('a.c', 'abc', '0', repr('abc')),
            ('a.c', 'axc', '0', repr('axc')),
            ('a.*c', 'axyzc', '0', repr('axyzc')),

            ('a.*c', 'axyzd', '', repr(None)),
            ('a[bc]d', 'abc', '', repr(None)),
            ('a[bc]d', 'abd', '0', repr('abd')),
            ('a[b-d]e', 'abd', '', repr(None)),
            ('a[b-d]e', 'ace', '0', repr('ace')),
            ('a[b-d]', 'aac', '0', repr('ac')),
            ('a[-b]', 'a-', '0', repr('a-')),
            ('a[\\-b]', 'a-', '0', repr('a-')),
            ('a[b-]', 'a-', '0', repr('a-')),
            ('a[]b', '-', '', regex.error, self.BAD_SET),

            ('a[', '-', '', regex.error, self.BAD_SET),
            ('a\\', '-', '', regex.error, self.BAD_ESCAPE),
            ('abc)', '-', '', regex.error, self.TRAILING_CHARS),
            ('(abc', '-', '', regex.error, self.MISSING_RPAREN),
            ('a]', 'a]', '0', repr('a]')),
            ('a[]]b', 'a]b', '0', repr('a]b')),
            ('a[]]b', 'a]b', '0', repr('a]b')),
            ('a[^bc]d', 'aed', '0', repr('aed')),
            ('a[^bc]d', 'abd', '', repr(None)),
            ('a[^-b]c', 'adc', '0', repr('adc')),

            ('a[^-b]c', 'a-c', '', repr(None)),
            ('a[^]b]c', 'a]c', '', repr(None)),
            ('a[^]b]c', 'adc', '0', repr('adc')),
            ('\\ba\\b', 'a-', '0', repr('a')),
            ('\\ba\\b', '-a', '0', repr('a')),
            ('\\ba\\b', '-a-', '0', repr('a')),
            ('\\by\\b', 'xy', '', repr(None)),
            ('\\by\\b', 'yz', '', repr(None)),
            ('\\by\\b', 'xyz', '', repr(None)),
            ('x\\b', 'xyz', '', repr(None)),

            ('x\\B', 'xyz', '0', repr('x')),
            ('\\Bz', 'xyz', '0', repr('z')),
            ('z\\B', 'xyz', '', repr(None)),
            ('\\Bx', 'xyz', '', repr(None)),
            ('\\Ba\\B', 'a-', '', repr(None)),
            ('\\Ba\\B', '-a', '', repr(None)),
            ('\\Ba\\B', '-a-', '', repr(None)),
            ('\\By\\B', 'xy', '', repr(None)),
            ('\\By\\B', 'yz', '', repr(None)),
            ('\\By\\b', 'xy', '0', repr('y')),

            ('\\by\\B', 'yz', '0', repr('y')),
            ('\\By\\B', 'xyz', '0', repr('y')),
            ('ab|cd', 'abc', '0', repr('ab')),
            ('ab|cd', 'abcd', '0', repr('ab')),
            ('()ef', 'def', '0,1', repr(('ef', ''))),
            ('$b', 'b', '', repr(None)),
            ('a\\(b', 'a(b', '', repr(('a(b',))),
            ('a\\(*b', 'ab', '0', repr('ab')),
            ('a\\(*b', 'a((b', '0', repr('a((b')),
            ('a\\\\b', 'a\\b', '0', repr('a\\b')),

            ('((a))', 'abc', '0,1,2', repr(('a', 'a', 'a'))),
            ('(a)b(c)', 'abc', '0,1,2', repr(('abc', 'a', 'c'))),
            ('a+b+c', 'aabbabc', '0', repr('abc')),
            ('(a+|b)*', 'ab', '0,1', repr(('ab', 'b'))),
            ('(a+|b)+', 'ab', '0,1', repr(('ab', 'b'))),
            ('(a+|b)?', 'ab', '0,1', repr(('a', 'a'))),
            (')(', '-', '', regex.error, self.TRAILING_CHARS),
            ('[^ab]*', 'cde', '0', repr('cde')),
            ('abc', '', '', repr(None)),
            ('a*', '', '0', repr('')),

            ('a|b|c|d|e', 'e', '0', repr('e')),
            ('(a|b|c|d|e)f', 'ef', '0,1', repr(('ef', 'e'))),
            ('abcd*efg', 'abcdefg', '0', repr('abcdefg')),
            ('ab*', 'xabyabbbz', '0', repr('ab')),
            ('ab*', 'xayabbbz', '0', repr('a')),
            ('(ab|cd)e', 'abcde', '0,1', repr(('cde', 'cd'))),
            ('[abhgefdc]ij', 'hij', '0', repr('hij')),
            ('^(ab|cd)e', 'abcde', '', repr(None)),
            ('(abc|)ef', 'abcdef', '0,1', repr(('ef', ''))),
            ('(a|b)c*d', 'abcd', '0,1', repr(('bcd', 'b'))),

            ('(ab|ab*)bc', 'abc', '0,1', repr(('abc', 'a'))),
            ('a([bc]*)c*', 'abc', '0,1', repr(('abc', 'bc'))),
            ('a([bc]*)(c*d)', 'abcd', '0,1,2', repr(('abcd', 'bc', 'd'))),
            ('a([bc]+)(c*d)', 'abcd', '0,1,2', repr(('abcd', 'bc', 'd'))),
            ('a([bc]*)(c+d)', 'abcd', '0,1,2', repr(('abcd', 'b', 'cd'))),
            ('a[bcd]*dcdcde', 'adcdcde', '0', repr('adcdcde')),
            ('a[bcd]+dcdcde', 'adcdcde', '', repr(None)),
            ('(ab|a)b*c', 'abc', '0,1', repr(('abc', 'ab'))),
            ('((a)(b)c)(d)', 'abcd', '1,2,3,4', repr(('abc', 'a', 'b', 'd'))),
            ('[a-zA-Z_][a-zA-Z0-9_]*', 'alpha', '0', repr('alpha')),

            ('^a(bc+|b[eh])g|.h$', 'abh', '0,1', repr(('bh', None))),
            ('(bc+d$|ef*g.|h?i(j|k))', 'effgz', '0,1,2', repr(('effgz',
              'effgz', None))),
            ('(bc+d$|ef*g.|h?i(j|k))', 'ij', '0,1,2', repr(('ij', 'ij',
              'j'))),
            ('(bc+d$|ef*g.|h?i(j|k))', 'effg', '', repr(None)),
            ('(bc+d$|ef*g.|h?i(j|k))', 'bcdd', '', repr(None)),
            ('(bc+d$|ef*g.|h?i(j|k))', 'reffgz', '0,1,2', repr(('effgz',
              'effgz', None))),
            ('(((((((((a)))))))))', 'a', '0', repr('a')),
            ('multiple words of text', 'uh-uh', '', repr(None)),
            ('multiple words', 'multiple words, yeah', '0',
              repr('multiple words')),
            ('(.*)c(.*)', 'abcde', '0,1,2', repr(('abcde', 'ab', 'de'))),

            ('\\((.*), (.*)\\)', '(a, b)', '2,1', repr(('b', 'a'))),
            ('[k]', 'ab', '', repr(None)),
            ('a[-]?c', 'ac', '0', repr('ac')),
            ('(abc)\\1', 'abcabc', '1', repr('abc')),
            ('([a-c]*)\\1', 'abcabc', '1', repr('abc')),
            ('^(.+)?B', 'AB', '1', repr('A')),
            ('(a+).\\1$', 'aaaaa', '0,1', repr(('aaaaa', 'aa'))),
            ('^(a+).\\1$', 'aaaa', '', repr(None)),
            ('(abc)\\1', 'abcabc', '0,1', repr(('abcabc', 'abc'))),
            ('([a-c]+)\\1', 'abcabc', '0,1', repr(('abcabc', 'abc'))),

            ('(a)\\1', 'aa', '0,1', repr(('aa', 'a'))),
            ('(a+)\\1', 'aa', '0,1', repr(('aa', 'a'))),
            ('(a+)+\\1', 'aa', '0,1', repr(('aa', 'a'))),
            ('(a).+\\1', 'aba', '0,1', repr(('aba', 'a'))),
            ('(a)ba*\\1', 'aba', '0,1', repr(('aba', 'a'))),
            ('(aa|a)a\\1$', 'aaa', '0,1', repr(('aaa', 'a'))),
            ('(a|aa)a\\1$', 'aaa', '0,1', repr(('aaa', 'a'))),
            ('(a+)a\\1$', 'aaa', '0,1', repr(('aaa', 'a'))),
            ('([abc]*)\\1', 'abcabc', '0,1', repr(('abcabc', 'abc'))),
            ('(a)(b)c|ab', 'ab', '0,1,2', repr(('ab', None, None))),

            ('(a)+x', 'aaax', '0,1', repr(('aaax', 'a'))),
            ('([ac])+x', 'aacx', '0,1', repr(('aacx', 'c'))),
            ('([^/]*/)*sub1/', 'd:msgs/tdir/sub1/trial/away.cpp', '0,1',
              repr(('d:msgs/tdir/sub1/', 'tdir/'))),
            ('([^.]*)\\.([^:]*):[T ]+(.*)', 'track1.title:TBlah blah blah',
              '0,1,2,3', repr(('track1.title:TBlah blah blah', 'track1',
              'title', 'Blah blah blah'))),
            ('([^N]*N)+', 'abNNxyzN', '0,1', repr(('abNNxyzN', 'xyzN'))),
            ('([^N]*N)+', 'abNNxyz', '0,1', repr(('abNN', 'N'))),
            ('([abc]*)x', 'abcx', '0,1', repr(('abcx', 'abc'))),
            ('([abc]*)x', 'abc', '', repr(None)),
            ('([xyz]*)x', 'abcx', '0,1', repr(('x', ''))),
            ('(a)+b|aac', 'aac', '0,1', repr(('aac', None))),

            # Test symbolic groups.
            ('(?P<i d>aaa)a', 'aaaa', '', regex.error, self.BAD_GROUP_NAME),
            ('(?P<id>aaa)a', 'aaaa', '0,id', repr(('aaaa', 'aaa'))),
            ('(?P<id>aa)(?P=id)', 'aaaa', '0,id', repr(('aaaa', 'aa'))),
            ('(?P<id>aa)(?P=xd)', 'aaaa', '', regex.error, self.UNKNOWN_GROUP),

            # Character properties.
            (ur"\g", u"g", '0', repr(u'g')),
            (ur"\g<1>", u"g", '', regex.error, self.INVALID_GROUP_REF),
            (ur"(.)\g<1>", u"gg", '0', repr(u'gg')),
            (ur"(.)\g<1>", u"gg", '', repr((u'gg', u'g'))),
            (ur"\N", u"N", '0', repr(u'N')),
            (ur"\N{LATIN SMALL LETTER A}", u"a", '0', repr(u'a')),
            (ur"\p", u"p", '0', repr(u'p')),
            (ur"\p{Ll}", u"a", '0', repr(u'a')),
            (ur"\P", u"P", '0', repr(u'P')),
            (ur"\P{Lu}", u"p", '0', repr(u'p')),

            # All tests from Perl.
            ('abc', 'abc', '0', repr('abc')),
            ('abc', 'xbc', '', repr(None)),
            ('abc', 'axc', '', repr(None)),
            ('abc', 'abx', '', repr(None)),
            ('abc', 'xabcy', '0', repr('abc')),
            ('abc', 'ababc', '0', repr('abc')),

            ('ab*c', 'abc', '0', repr('abc')),
            ('ab*bc', 'abc', '0', repr('abc')),
            ('ab*bc', 'abbc', '0', repr('abbc')),
            ('ab*bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab{0,}bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab+bc', 'abbc', '0', repr('abbc')),
            ('ab+bc', 'abc', '', repr(None)),
            ('ab+bc', 'abq', '', repr(None)),
            ('ab{1,}bc', 'abq', '', repr(None)),
            ('ab+bc', 'abbbbc', '0', repr('abbbbc')),

            ('ab{1,}bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab{1,3}bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab{3,4}bc', 'abbbbc', '0', repr('abbbbc')),
            ('ab{4,5}bc', 'abbbbc', '', repr(None)),
            ('ab?bc', 'abbc', '0', repr('abbc')),
            ('ab?bc', 'abc', '0', repr('abc')),
            ('ab{0,1}bc', 'abc', '0', repr('abc')),
            ('ab?bc', 'abbbbc', '', repr(None)),
            ('ab?c', 'abc', '0', repr('abc')),
            ('ab{0,1}c', 'abc', '0', repr('abc')),

            ('^abc$', 'abc', '0', repr('abc')),
            ('^abc$', 'abcc', '', repr(None)),
            ('^abc', 'abcc', '0', repr('abc')),
            ('^abc$', 'aabc', '', repr(None)),
            ('abc$', 'aabc', '0', repr('abc')),
            ('^', 'abc', '0', repr('')),
            ('$', 'abc', '0', repr('')),
            ('a.c', 'abc', '0', repr('abc')),
            ('a.c', 'axc', '0', repr('axc')),
            ('a.*c', 'axyzc', '0', repr('axyzc')),

            ('a.*c', 'axyzd', '', repr(None)),
            ('a[bc]d', 'abc', '', repr(None)),
            ('a[bc]d', 'abd', '0', repr('abd')),
            ('a[b-d]e', 'abd', '', repr(None)),
            ('a[b-d]e', 'ace', '0', repr('ace')),
            ('a[b-d]', 'aac', '0', repr('ac')),
            ('a[-b]', 'a-', '0', repr('a-')),
            ('a[b-]', 'a-', '0', repr('a-')),
            ('a[b-a]', '-', '', regex.error, self.BAD_CHAR_RANGE),
            ('a[]b', '-', '', regex.error, self.BAD_SET),

            ('a[', '-', '', regex.error, self.BAD_SET),
            ('a]', 'a]', '0', repr('a]')),
            ('a[]]b', 'a]b', '0', repr('a]b')),
            ('a[^bc]d', 'aed', '0', repr('aed')),
            ('a[^bc]d', 'abd', '', repr(None)),
            ('a[^-b]c', 'adc', '0', repr('adc')),
            ('a[^-b]c', 'a-c', '', repr(None)),
            ('a[^]b]c', 'a]c', '', repr(None)),
            ('a[^]b]c', 'adc', '0', repr('adc')),
            ('ab|cd', 'abc', '0', repr('ab')),

            ('ab|cd', 'abcd', '0', repr('ab')),
            ('()ef', 'def', '0,1', repr(('ef', ''))),
            ('*a', '-', '', regex.error, self.NOTHING_TO_REPEAT),
            ('(*)b', '-', '', regex.error, self.NOTHING_TO_REPEAT),
            ('$b', 'b', '', repr(None)),
            ('a\\', '-', '', regex.error, self.BAD_ESCAPE),
            ('a\\(b', 'a(b', '', repr(('a(b',))),
            ('a\\(*b', 'ab', '0', repr('ab')),
            ('a\\(*b', 'a((b', '0', repr('a((b')),
            ('a\\\\b', 'a\\b', '0', repr('a\\b')),

            ('abc)', '-', '', regex.error, self.TRAILING_CHARS),
            ('(abc', '-', '', regex.error, self.MISSING_RPAREN),
            ('((a))', 'abc', '0,1,2', repr(('a', 'a', 'a'))),
            ('(a)b(c)', 'abc', '0,1,2', repr(('abc', 'a', 'c'))),
            ('a+b+c', 'aabbabc', '0', repr('abc')),
            ('a{1,}b{1,}c', 'aabbabc', '0', repr('abc')),
            ('a**', '-', '', regex.error, self.MULTIPLE_REPEAT),
            ('a.+?c', 'abcabc', '0', repr('abc')),
            ('(a+|b)*', 'ab', '0,1', repr(('ab', 'b'))),
            ('(a+|b){0,}', 'ab', '0,1', repr(('ab', 'b'))),

            ('(a+|b)+', 'ab', '0,1', repr(('ab', 'b'))),
            ('(a+|b){1,}', 'ab', '0,1', repr(('ab', 'b'))),
            ('(a+|b)?', 'ab', '0,1', repr(('a', 'a'))),
            ('(a+|b){0,1}', 'ab', '0,1', repr(('a', 'a'))),
            (')(', '-', '', regex.error, self.TRAILING_CHARS),
            ('[^ab]*', 'cde', '0', repr('cde')),
            ('abc', '', '', repr(None)),
            ('a*', '', '0', repr('')),
            ('([abc])*d', 'abbbcd', '0,1', repr(('abbbcd', 'c'))),
            ('([abc])*bcd', 'abcd', '0,1', repr(('abcd', 'a'))),

            ('a|b|c|d|e', 'e', '0', repr('e')),
            ('(a|b|c|d|e)f', 'ef', '0,1', repr(('ef', 'e'))),
            ('abcd*efg', 'abcdefg', '0', repr('abcdefg')),
            ('ab*', 'xabyabbbz', '0', repr('ab')),
            ('ab*', 'xayabbbz', '0', repr('a')),
            ('(ab|cd)e', 'abcde', '0,1', repr(('cde', 'cd'))),
            ('[abhgefdc]ij', 'hij', '0', repr('hij')),
            ('^(ab|cd)e', 'abcde', '', repr(None)),
            ('(abc|)ef', 'abcdef', '0,1', repr(('ef', ''))),
            ('(a|b)c*d', 'abcd', '0,1', repr(('bcd', 'b'))),

            ('(ab|ab*)bc', 'abc', '0,1', repr(('abc', 'a'))),
            ('a([bc]*)c*', 'abc', '0,1', repr(('abc', 'bc'))),
            ('a([bc]*)(c*d)', 'abcd', '0,1,2', repr(('abcd', 'bc', 'd'))),
            ('a([bc]+)(c*d)', 'abcd', '0,1,2', repr(('abcd', 'bc', 'd'))),
            ('a([bc]*)(c+d)', 'abcd', '0,1,2', repr(('abcd', 'b', 'cd'))),
            ('a[bcd]*dcdcde', 'adcdcde', '0', repr('adcdcde')),
            ('a[bcd]+dcdcde', 'adcdcde', '', repr(None)),
            ('(ab|a)b*c', 'abc', '0,1', repr(('abc', 'ab'))),
            ('((a)(b)c)(d)', 'abcd', '1,2,3,4', repr(('abc', 'a', 'b', 'd'))),
            ('[a-zA-Z_][a-zA-Z0-9_]*', 'alpha', '0', repr('alpha')),

            ('^a(bc+|b[eh])g|.h$', 'abh', '0,1', repr(('bh', None))),
            ('(bc+d$|ef*g.|h?i(j|k))', 'effgz', '0,1,2', repr(('effgz',
              'effgz', None))),
            ('(bc+d$|ef*g.|h?i(j|k))', 'ij', '0,1,2', repr(('ij', 'ij',
              'j'))),
            ('(bc+d$|ef*g.|h?i(j|k))', 'effg', '', repr(None)),
            ('(bc+d$|ef*g.|h?i(j|k))', 'bcdd', '', repr(None)),
            ('(bc+d$|ef*g.|h?i(j|k))', 'reffgz', '0,1,2', repr(('effgz',
              'effgz', None))),
            ('((((((((((a))))))))))', 'a', '10', repr('a')),
            ('((((((((((a))))))))))\\10', 'aa', '0', repr('aa')),

            # Python does not have the same rules for \\41 so this is a syntax error
            #    ('((((((((((a))))))))))\\41', 'aa', '', repr(None)),
            #    ('((((((((((a))))))))))\\41', 'a!', '0', repr('a!')),
            ('((((((((((a))))))))))\\41', '', '', regex.error,
              self.INVALID_GROUP_REF),
            ('(?i)((((((((((a))))))))))\\41', '', '', regex.error,
              self.INVALID_GROUP_REF),

            ('(((((((((a)))))))))', 'a', '0', repr('a')),
            ('multiple words of text', 'uh-uh', '', repr(None)),
            ('multiple words', 'multiple words, yeah', '0',
              repr('multiple words')),
            ('(.*)c(.*)', 'abcde', '0,1,2', repr(('abcde', 'ab', 'de'))),
            ('\\((.*), (.*)\\)', '(a, b)', '2,1', repr(('b', 'a'))),
            ('[k]', 'ab', '', repr(None)),
            ('a[-]?c', 'ac', '0', repr('ac')),
            ('(abc)\\1', 'abcabc', '1', repr('abc')),
            ('([a-c]*)\\1', 'abcabc', '1', repr('abc')),
            ('(?i)abc', 'ABC', '0', repr('ABC')),

            ('(?i)abc', 'XBC', '', repr(None)),
            ('(?i)abc', 'AXC', '', repr(None)),
            ('(?i)abc', 'ABX', '', repr(None)),
            ('(?i)abc', 'XABCY', '0', repr('ABC')),
            ('(?i)abc', 'ABABC', '0', repr('ABC')),
            ('(?i)ab*c', 'ABC', '0', repr('ABC')),
            ('(?i)ab*bc', 'ABC', '0', repr('ABC')),
            ('(?i)ab*bc', 'ABBC', '0', repr('ABBC')),
            ('(?i)ab*?bc', 'ABBBBC', '0', repr('ABBBBC')),
            ('(?i)ab{0,}?bc', 'ABBBBC', '0', repr('ABBBBC')),

            ('(?i)ab+?bc', 'ABBC', '0', repr('ABBC')),
            ('(?i)ab+bc', 'ABC', '', repr(None)),
            ('(?i)ab+bc', 'ABQ', '', repr(None)),
            ('(?i)ab{1,}bc', 'ABQ', '', repr(None)),
            ('(?i)ab+bc', 'ABBBBC', '0', repr('ABBBBC')),
            ('(?i)ab{1,}?bc', 'ABBBBC', '0', repr('ABBBBC')),
            ('(?i)ab{1,3}?bc', 'ABBBBC', '0', repr('ABBBBC')),
            ('(?i)ab{3,4}?bc', 'ABBBBC', '0', repr('ABBBBC')),
            ('(?i)ab{4,5}?bc', 'ABBBBC', '', repr(None)),
            ('(?i)ab??bc', 'ABBC', '0', repr('ABBC')),

            ('(?i)ab??bc', 'ABC', '0', repr('ABC')),
            ('(?i)ab{0,1}?bc', 'ABC', '0', repr('ABC')),
            ('(?i)ab??bc', 'ABBBBC', '', repr(None)),
            ('(?i)ab??c', 'ABC', '0', repr('ABC')),
            ('(?i)ab{0,1}?c', 'ABC', '0', repr('ABC')),
            ('(?i)^abc$', 'ABC', '0', repr('ABC')),
            ('(?i)^abc$', 'ABCC', '', repr(None)),
            ('(?i)^abc', 'ABCC', '0', repr('ABC')),
            ('(?i)^abc$', 'AABC', '', repr(None)),
            ('(?i)abc$', 'AABC', '0', repr('ABC')),

            ('(?i)^', 'ABC', '0', repr('')),
            ('(?i)$', 'ABC', '0', repr('')),
            ('(?i)a.c', 'ABC', '0', repr('ABC')),
            ('(?i)a.c', 'AXC', '0', repr('AXC')),
            ('(?i)a.*?c', 'AXYZC', '0', repr('AXYZC')),
            ('(?i)a.*c', 'AXYZD', '', repr(None)),
            ('(?i)a[bc]d', 'ABC', '', repr(None)),
            ('(?i)a[bc]d', 'ABD', '0', repr('ABD')),
            ('(?i)a[b-d]e', 'ABD', '', repr(None)),
            ('(?i)a[b-d]e', 'ACE', '0', repr('ACE')),

            ('(?i)a[b-d]', 'AAC', '0', repr('AC')),
            ('(?i)a[-b]', 'A-', '0', repr('A-')),
            ('(?i)a[b-]', 'A-', '0', repr('A-')),
            ('(?i)a[b-a]', '-', '', regex.error, self.BAD_CHAR_RANGE),
            ('(?i)a[]b', '-', '', regex.error, self.BAD_SET),
            ('(?i)a[', '-', '', regex.error, self.BAD_SET),
            ('(?i)a]', 'A]', '0', repr('A]')),
            ('(?i)a[]]b', 'A]B', '0', repr('A]B')),
            ('(?i)a[^bc]d', 'AED', '0', repr('AED')),
            ('(?i)a[^bc]d', 'ABD', '', repr(None)),

            ('(?i)a[^-b]c', 'ADC', '0', repr('ADC')),
            ('(?i)a[^-b]c', 'A-C', '', repr(None)),
            ('(?i)a[^]b]c', 'A]C', '', repr(None)),
            ('(?i)a[^]b]c', 'ADC', '0', repr('ADC')),
            ('(?i)ab|cd', 'ABC', '0', repr('AB')),
            ('(?i)ab|cd', 'ABCD', '0', repr('AB')),
            ('(?i)()ef', 'DEF', '0,1', repr(('EF', ''))),
            ('(?i)*a', '-', '', regex.error, self.NOTHING_TO_REPEAT),
            ('(?i)(*)b', '-', '', regex.error, self.NOTHING_TO_REPEAT),
            ('(?i)$b', 'B', '', repr(None)),

            ('(?i)a\\', '-', '', regex.error, self.BAD_ESCAPE),
            ('(?i)a\\(b', 'A(B', '', repr(('A(B',))),
            ('(?i)a\\(*b', 'AB', '0', repr('AB')),
            ('(?i)a\\(*b', 'A((B', '0', repr('A((B')),
            ('(?i)a\\\\b', 'A\\B', '0', repr('A\\B')),
            ('(?i)abc)', '-', '', regex.error, self.TRAILING_CHARS),
            ('(?i)(abc', '-', '', regex.error, self.MISSING_RPAREN),
            ('(?i)((a))', 'ABC', '0,1,2', repr(('A', 'A', 'A'))),
            ('(?i)(a)b(c)', 'ABC', '0,1,2', repr(('ABC', 'A', 'C'))),
            ('(?i)a+b+c', 'AABBABC', '0', repr('ABC')),

            ('(?i)a{1,}b{1,}c', 'AABBABC', '0', repr('ABC')),
            ('(?i)a**', '-', '', regex.error, self.MULTIPLE_REPEAT),
            ('(?i)a.+?c', 'ABCABC', '0', repr('ABC')),
            ('(?i)a.*?c', 'ABCABC', '0', repr('ABC')),
            ('(?i)a.{0,5}?c', 'ABCABC', '0', repr('ABC')),
            ('(?i)(a+|b)*', 'AB', '0,1', repr(('AB', 'B'))),
            ('(?i)(a+|b){0,}', 'AB', '0,1', repr(('AB', 'B'))),
            ('(?i)(a+|b)+', 'AB', '0,1', repr(('AB', 'B'))),
            ('(?i)(a+|b){1,}', 'AB', '0,1', repr(('AB', 'B'))),
            ('(?i)(a+|b)?', 'AB', '0,1', repr(('A', 'A'))),

            ('(?i)(a+|b){0,1}', 'AB', '0,1', repr(('A', 'A'))),
            ('(?i)(a+|b){0,1}?', 'AB', '0,1', repr(('', None))),
            ('(?i))(', '-', '', regex.error, self.TRAILING_CHARS),
            ('(?i)[^ab]*', 'CDE', '0', repr('CDE')),
            ('(?i)abc', '', '', repr(None)),
            ('(?i)a*', '', '0', repr('')),
            ('(?i)([abc])*d', 'ABBBCD', '0,1', repr(('ABBBCD', 'C'))),
            ('(?i)([abc])*bcd', 'ABCD', '0,1', repr(('ABCD', 'A'))),
            ('(?i)a|b|c|d|e', 'E', '0', repr('E')),
            ('(?i)(a|b|c|d|e)f', 'EF', '0,1', repr(('EF', 'E'))),

            ('(?i)abcd*efg', 'ABCDEFG', '0', repr('ABCDEFG')),
            ('(?i)ab*', 'XABYABBBZ', '0', repr('AB')),
            ('(?i)ab*', 'XAYABBBZ', '0', repr('A')),
            ('(?i)(ab|cd)e', 'ABCDE', '0,1', repr(('CDE', 'CD'))),
            ('(?i)[abhgefdc]ij', 'HIJ', '0', repr('HIJ')),
            ('(?i)^(ab|cd)e', 'ABCDE', '', repr(None)),
            ('(?i)(abc|)ef', 'ABCDEF', '0,1', repr(('EF', ''))),
            ('(?i)(a|b)c*d', 'ABCD', '0,1', repr(('BCD', 'B'))),
            ('(?i)(ab|ab*)bc', 'ABC', '0,1', repr(('ABC', 'A'))),
            ('(?i)a([bc]*)c*', 'ABC', '0,1', repr(('ABC', 'BC'))),

            ('(?i)a([bc]*)(c*d)', 'ABCD', '0,1,2', repr(('ABCD', 'BC', 'D'))),
            ('(?i)a([bc]+)(c*d)', 'ABCD', '0,1,2', repr(('ABCD', 'BC', 'D'))),
            ('(?i)a([bc]*)(c+d)', 'ABCD', '0,1,2', repr(('ABCD', 'B', 'CD'))),
            ('(?i)a[bcd]*dcdcde', 'ADCDCDE', '0', repr('ADCDCDE')),
            ('(?i)a[bcd]+dcdcde', 'ADCDCDE', '', repr(None)),
            ('(?i)(ab|a)b*c', 'ABC', '0,1', repr(('ABC', 'AB'))),
            ('(?i)((a)(b)c)(d)', 'ABCD', '1,2,3,4', repr(('ABC', 'A', 'B',
              'D'))),
            ('(?i)[a-zA-Z_][a-zA-Z0-9_]*', 'ALPHA', '0', repr('ALPHA')),
            ('(?i)^a(bc+|b[eh])g|.h$', 'ABH', '0,1', repr(('BH', None))),
            ('(?i)(bc+d$|ef*g.|h?i(j|k))', 'EFFGZ', '0,1,2', repr(('EFFGZ',
              'EFFGZ', None))),

            ('(?i)(bc+d$|ef*g.|h?i(j|k))', 'IJ', '0,1,2', repr(('IJ', 'IJ',
              'J'))),
            ('(?i)(bc+d$|ef*g.|h?i(j|k))', 'EFFG', '', repr(None)),
            ('(?i)(bc+d$|ef*g.|h?i(j|k))', 'BCDD', '', repr(None)),
            ('(?i)(bc+d$|ef*g.|h?i(j|k))', 'REFFGZ', '0,1,2', repr(('EFFGZ',
              'EFFGZ', None))),
            ('(?i)((((((((((a))))))))))', 'A', '10', repr('A')),
            ('(?i)((((((((((a))))))))))\\10', 'AA', '0', repr('AA')),
            #('(?i)((((((((((a))))))))))\\41', 'AA', '', repr(None)),
            #('(?i)((((((((((a))))))))))\\41', 'A!', '0', repr('A!')),
            ('(?i)(((((((((a)))))))))', 'A', '0', repr('A')),
            ('(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a))))))))))', 'A', '1',
              repr('A')),
            ('(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a|b|c))))))))))', 'C', '1',
              repr('C')),
            ('(?i)multiple words of text', 'UH-UH', '', repr(None)),

            ('(?i)multiple words', 'MULTIPLE WORDS, YEAH', '0',
             repr('MULTIPLE WORDS')),
            ('(?i)(.*)c(.*)', 'ABCDE', '0,1,2', repr(('ABCDE', 'AB', 'DE'))),
            ('(?i)\\((.*), (.*)\\)', '(A, B)', '2,1', repr(('B', 'A'))),
            ('(?i)[k]', 'AB', '', repr(None)),
        #    ('(?i)abcd', 'ABCD', SUCCEED, 'found+"-"+\\found+"-"+\\\\found', repr(ABCD-$&-\\ABCD)),
        #    ('(?i)a(bc)d', 'ABCD', SUCCEED, 'g1+"-"+\\g1+"-"+\\\\g1', repr(BC-$1-\\BC)),
            ('(?i)a[-]?c', 'AC', '0', repr('AC')),
            ('(?i)(abc)\\1', 'ABCABC', '1', repr('ABC')),
            ('(?i)([a-c]*)\\1', 'ABCABC', '1', repr('ABC')),
            ('a(?!b).', 'abad', '0', repr('ad')),
            ('a(?=d).', 'abad', '0', repr('ad')),
            ('a(?=c|d).', 'abad', '0', repr('ad')),

            ('a(?:b|c|d)(.)', 'ace', '1', repr('e')),
            ('a(?:b|c|d)*(.)', 'ace', '1', repr('e')),
            ('a(?:b|c|d)+?(.)', 'ace', '1', repr('e')),
            ('a(?:b|(c|e){1,2}?|d)+?(.)', 'ace', '1,2', repr(('c', 'e'))),

            # Lookbehind: split by : but not if it is escaped by -.
            ('(?<!-):(.*?)(?<!-):', 'a:bc-:de:f', '1', repr('bc-:de')),
            # Escaping with \ as we know it.
            ('(?<!\\\\):(.*?)(?<!\\\\):', 'a:bc\\:de:f', '1', repr('bc\\:de')),
            # Terminating with ' and escaping with ? as in edifact.
            ("(?<!\\?)'(.*?)(?<!\\?)'", "a'bc?'de'f", '1', repr("bc?'de")),

            # Comments using the (?#...) syntax.

            ('w(?# comment', 'w', '', regex.error, self.MISSING_RPAREN),
            ('w(?# comment 1)xy(?# comment 2)z', 'wxyz', '0', repr('wxyz')),

            # Check odd placement of embedded pattern modifiers.

            # Not an error under PCRE/PRE:
            # When the new behaviour is turned on positional inline flags affect
            # only what follows.
            ('w(?i)', 'W', '0', repr('W')),
            ('w(?iV1)', 'W', '0', repr(None)),
            ('w(?i)', 'w', '0', repr('w')),
            ('w(?iV1)', 'w', '0', repr('w')),
            ('(?i)w', 'W', '0', repr('W')),
            ('(?iV1)w', 'W', '0', repr('W')),

            # Comments using the x embedded pattern modifier.
            ("""(?x)w# comment 1
x y
# comment 2
z""", 'wxyz', '0', repr('wxyz')),

            # Using the m embedded pattern modifier.
            ('^abc', """jkl
abc
xyz""", '', repr(None)),
            ('(?m)^abc', """jkl
abc
xyz""", '0', repr('abc')),

            ('(?m)abc$', """jkl
xyzabc
123""", '0', repr('abc')),

            # Using the s embedded pattern modifier.
            ('a.b', 'a\nb', '', repr(None)),
            ('(?s)a.b', 'a\nb', '0', repr('a\nb')),

            # Test \w, etc. both inside and outside character classes.
            ('\\w+', '--ab_cd0123--', '0', repr('ab_cd0123')),
            ('[\\w]+', '--ab_cd0123--', '0', repr('ab_cd0123')),
            ('\\D+', '1234abc5678', '0', repr('abc')),
            ('[\\D]+', '1234abc5678', '0', repr('abc')),
            ('[\\da-fA-F]+', '123abc', '0', repr('123abc')),
            # Not an error under PCRE/PRE:
            # ('[\\d-x]', '-', '', regex.error, self.BAD_CHAR_RANGE),
            (r'([\s]*)([\S]*)([\s]*)', ' testing!1972', '3,2,1', repr(('',
              'testing!1972', ' '))),
            (r'(\s*)(\S*)(\s*)', ' testing!1972', '3,2,1', repr(('',
              'testing!1972', ' '))),

            #
            # Post-1.5.2 additions.

            # xmllib problem.
            (r'(([a-z]+):)?([a-z]+)$', 'smil', '1,2,3', repr((None, None,
              'smil'))),
            # Bug 110866: reference to undefined group.
            (r'((.)\1+)', '', '', regex.error, self.OPEN_GROUP),
            # Bug 111869: search (PRE/PCRE fails on this one, SRE doesn't).
            (r'.*d', 'abc\nabd', '0', repr('abd')),
            # Bug 112468: various expected syntax errors.
            (r'(', '', '', regex.error, self.MISSING_RPAREN),
            (r'[\41]', '!', '0', repr('!')),
            # Bug 114033: nothing to repeat.
            (r'(x?)?', 'x', '0', repr('x')),
            # Bug 115040: rescan if flags are modified inside pattern.
            # If the new behaviour is turned on then positional inline flags
            # affect only what follows.
            (r' (?x)foo ', 'foo', '0', repr('foo')),
            (r' (?V1x)foo ', 'foo', '0', repr(None)),
            (r'(?x) foo ', 'foo', '0', repr('foo')),
            (r'(?V1x) foo ', 'foo', '0', repr('foo')),
            (r'(?x)foo ', 'foo', '0', repr('foo')),
            (r'(?V1x)foo ', 'foo', '0', repr('foo')),
            # Bug 115618: negative lookahead.
            (r'(?<!abc)(d.f)', 'abcdefdof', '0', repr('dof')),
            # Bug 116251: character class bug.
            (r'[\w-]+', 'laser_beam', '0', repr('laser_beam')),
            # Bug 123769+127259: non-greedy backtracking bug.
            (r'.*?\S *:', 'xx:', '0', repr('xx:')),
            (r'a[ ]*?\ (\d+).*', 'a   10', '0', repr('a   10')),
            (r'a[ ]*?\ (\d+).*', 'a    10', '0', repr('a    10')),
            # Bug 127259: \Z shouldn't depend on multiline mode.
            (r'(?ms).*?x\s*\Z(.*)','xx\nx\n', '1', repr('')),
            # Bug 128899: uppercase literals under the ignorecase flag.
            (r'(?i)M+', 'MMM', '0', repr('MMM')),
            (r'(?i)m+', 'MMM', '0', repr('MMM')),
            (r'(?i)[M]+', 'MMM', '0', repr('MMM')),
            (r'(?i)[m]+', 'MMM', '0', repr('MMM')),
            # Bug 130748: ^* should be an error (nothing to repeat).
            # In 'regex' we won't bother to complain about this.
            # (r'^*', '', '', regex.error, self.NOTHING_TO_REPEAT),
            # Bug 133283: minimizing repeat problem.
            (r'"(?:\\"|[^"])*?"', r'"\""', '0', repr(r'"\""')),
            # Bug 477728: minimizing repeat problem.
            (r'^.*?$', 'one\ntwo\nthree\n', '', repr(None)),
            # Bug 483789: minimizing repeat problem.
            (r'a[^>]*?b', 'a>b', '', repr(None)),
            # Bug 490573: minimizing repeat problem.
            (r'^a*?$', 'foo', '', repr(None)),
            # Bug 470582: nested groups problem.
            (r'^((a)c)?(ab)$', 'ab', '1,2,3', repr((None, None, 'ab'))),
            # Another minimizing repeat problem (capturing groups in assertions).
            ('^([ab]*?)(?=(b)?)c', 'abc', '1,2', repr(('ab', None))),
            ('^([ab]*?)(?!(b))c', 'abc', '1,2', repr(('ab', None))),
            ('^([ab]*?)(?<!(a))c', 'abc', '1,2', repr(('ab', None))),
            # Bug 410271: \b broken under locales.
            (r'\b.\b', 'a', '0', repr('a')),
            (ur'(?u)\b.\b', u'\N{LATIN CAPITAL LETTER A WITH DIAERESIS}', '0',
              repr(u'\xc4')),
            (ur'(?u)\w', u'\N{LATIN CAPITAL LETTER A WITH DIAERESIS}', '0',
              repr(u'\xc4')),
        ]

        for t in tests:
            excval = None
            try:
                if len(t) == 4:
                    pattern, string, groups, expected = t
                else:
                    pattern, string, groups, expected, excval = t
            except ValueError:
                fields = ", ".join([repr(f) for f in t[ : 3]] + ["..."])
                self.fail("Incorrect number of test fields: (%s)" % fields)
            else:
                group_list = []
                if groups:
                    for group in groups.split(","):
                        try:
                            group_list.append(int(group))
                        except ValueError:
                            group_list.append(group)

                if excval is not None:
                    self.assertRaisesRegex(expected, excval, regex.search,
                      pattern, string)
                else:
                    m = regex.search(pattern, string)
                    if m:
                        if group_list:
                            actual = repr(m.group(*group_list))
                        else:
                            actual = repr(m[:])
                    else:
                        actual = repr(m)

                    self.assertEqual(actual, expected)

    def test_replacement(self):
        self.assertEqual(regex.sub(r"test\?", "result\\?\\.\a\\q\\m\n", "test?"),
          "result\\?\\.\a\\q\\m\n")

        self.assertEqual(regex.sub('(.)', r"\1\1", 'x'), 'xx')
        self.assertEqual(regex.sub('(.)', regex.escape(r"\1\1"), 'x'), r"\1\1")
        self.assertEqual(regex.sub('(.)', r"\\1\\1", 'x'), r"\1\1")
        self.assertEqual(regex.sub('(.)', lambda m: r"\1\1", 'x'), r"\1\1")

    def test_common_prefix(self):
        # Very long common prefix
        all = string.ascii_lowercase + string.digits + string.ascii_uppercase
        side = all * 4
        regexp = '(' + side + '|' + side + ')'
        self.assertEqual(repr(type(regex.compile(regexp))), self.PATTERN_CLASS)

    def test_captures(self):
        self.assertEqual(regex.search(r"(\w)+", "abc").captures(1), ['a', 'b',
          'c'])
        self.assertEqual(regex.search(r"(\w{3})+", "abcdef").captures(0, 1),
          (['abcdef'], ['abc', 'def']))
        self.assertEqual(regex.search(r"^(\d{1,3})(?:\.(\d{1,3})){3}$",
          "192.168.0.1").captures(1, 2), (['192', ], ['168', '0', '1']))
        self.assertEqual(regex.match(r"^([0-9A-F]{2}){4} ([a-z]\d){5}$",
          "3FB52A0C a2c4g3k9d3").captures(1, 2), (['3F', 'B5', '2A', '0C'],
          ['a2', 'c4', 'g3', 'k9', 'd3']))
        self.assertEqual(regex.match("([a-z]W)([a-z]X)+([a-z]Y)",
          "aWbXcXdXeXfY").captures(1, 2, 3), (['aW'], ['bX', 'cX', 'dX', 'eX'],
          ['fY']))

        self.assertEqual(regex.search(r".*?(?=(.)+)b", "ab").captures(1),
          ['b'])
        self.assertEqual(regex.search(r".*?(?>(.){0,2})d", "abcd").captures(1),
          ['b', 'c'])
        self.assertEqual(regex.search(r"(.)+", "a").captures(1), ['a'])

    def test_guards(self):
        m = regex.search(r"(X.*?Y\s*){3}(X\s*)+AB:",
          "XY\nX Y\nX  Y\nXY\nXX AB:")
        self.assertEqual(m.span(0, 1, 2), ((3, 21), (12, 15), (16, 18)))

        m = regex.search(r"(X.*?Y\s*){3,}(X\s*)+AB:",
          "XY\nX Y\nX  Y\nXY\nXX AB:")
        self.assertEqual(m.span(0, 1, 2), ((0, 21), (12, 15), (16, 18)))

        m = regex.search(r'\d{4}(\s*\w)?\W*((?!\d)\w){2}', "9999XX")
        self.assertEqual(m.span(0, 1, 2), ((0, 6), (-1, -1), (5, 6)))

        m = regex.search(r'A\s*?.*?(\n+.*?\s*?){0,2}\(X', 'A\n1\nS\n1 (X')
        self.assertEqual(m.span(0, 1), ((0, 10), (5, 8)))

        m = regex.search(r'Derde\s*:', 'aaaaaa:\nDerde:')
        self.assertEqual(m.span(), (8, 14))
        m = regex.search(r'Derde\s*:', 'aaaaa:\nDerde:')
        self.assertEqual(m.span(), (7, 13))

    def test_turkic(self):
        # Turkish has dotted and dotless I/i.
        pairs = u"I=i;I=\u0131;i=\u0130"

        all_chars = set()
        matching = set()
        for pair in pairs.split(";"):
            ch1, ch2 = pair.split("=")
            all_chars.update((ch1, ch2))
            matching.add((ch1, ch1))
            matching.add((ch1, ch2))
            matching.add((ch2, ch1))
            matching.add((ch2, ch2))

        for ch1 in all_chars:
            for ch2 in all_chars:
                m = regex.match(ur"(?iu)\A" + ch1 + ur"\Z", ch2)
                if m:
                    if (ch1, ch2) not in matching:
                        self.fail("%s matching %s" % (repr(ch1), repr(ch2)))
                else:
                    if (ch1, ch2) in matching:
                        self.fail("%s not matching %s" % (repr(ch1),
                          repr(ch2)))

    def test_named_lists(self):
        options = [u"one", u"two", u"three"]
        self.assertEqual(regex.match(ur"333\L<bar>444", u"333one444",
          bar=options).group(), u"333one444")
        self.assertEqual(regex.match(ur"(?i)333\L<bar>444", u"333TWO444",
          bar=options).group(), u"333TWO444")
        self.assertEqual(regex.match(ur"333\L<bar>444", u"333four444",
          bar=options), None)

        options = ["one", "two", "three"]
        self.assertEqual(regex.match(r"333\L<bar>444", "333one444",
          bar=options).group(), "333one444")
        self.assertEqual(regex.match(r"(?i)333\L<bar>444", "333TWO444",
          bar=options).group(), "333TWO444")
        self.assertEqual(regex.match(r"333\L<bar>444", "333four444",
          bar=options), None)

        self.assertEqual(repr(type(regex.compile(r"3\L<bar>4\L<bar>+5",
          bar=["one", "two", "three"]))), self.PATTERN_CLASS)

        self.assertEqual(regex.findall(r"^\L<options>", "solid QWERT",
          options=set(['good', 'brilliant', '+s\\ol[i}d'])), [])
        self.assertEqual(regex.findall(r"^\L<options>", "+solid QWERT",
          options=set(['good', 'brilliant', '+solid'])), ['+solid'])

        options = [u"STRASSE"]
        self.assertEqual(regex.match(ur"(?fiu)\L<words>",
          u"stra\N{LATIN SMALL LETTER SHARP S}e", words=options).span(), (0,
          6))

        options = [u"STRASSE", u"stress"]
        self.assertEqual(regex.match(ur"(?fiu)\L<words>",
          u"stra\N{LATIN SMALL LETTER SHARP S}e", words=options).span(), (0,
          6))

        options = [u"stra\N{LATIN SMALL LETTER SHARP S}e"]
        self.assertEqual(regex.match(ur"(?fiu)\L<words>", u"STRASSE",
          words=options).span(), (0, 7))

        options = ["kit"]
        self.assertEqual(regex.search(ur"(?iu)\L<words>", u"SKITS",
          words=options).span(), (1, 4))
        self.assertEqual(regex.search(ur"(?iu)\L<words>",
          u"SK\N{LATIN CAPITAL LETTER I WITH DOT ABOVE}TS",
          words=options).span(), (1, 4))

        self.assertEqual(regex.search(ur"(?fiu)\b(\w+) +\1\b",
          u" stra\N{LATIN SMALL LETTER SHARP S}e STRASSE ").span(), (1, 15))
        self.assertEqual(regex.search(ur"(?fiu)\b(\w+) +\1\b",
          u" STRASSE stra\N{LATIN SMALL LETTER SHARP S}e ").span(), (1, 15))

        self.assertEqual(regex.search(r"^\L<options>$", "", options=[]).span(),
          (0, 0))

    def test_fuzzy(self):
        # Some tests borrowed from TRE library tests.
        self.assertEqual(repr(type(regex.compile('(fou){s,e<=1}'))),
          self.PATTERN_CLASS)
        self.assertEqual(repr(type(regex.compile('(fuu){s}'))),
          self.PATTERN_CLASS)
        self.assertEqual(repr(type(regex.compile('(fuu){s,e}'))),
          self.PATTERN_CLASS)
        self.assertEqual(repr(type(regex.compile('(anaconda){1i+1d<1,s<=1}'))),
          self.PATTERN_CLASS)
        self.assertEqual(repr(type(regex.compile('(anaconda){1i+1d<1,s<=1,e<=10}'))),
          self.PATTERN_CLASS)
        self.assertEqual(repr(type(regex.compile('(anaconda){s<=1,e<=1,1i+1d<1}'))),
          self.PATTERN_CLASS)

        text = 'molasses anaconda foo bar baz smith anderson '
        self.assertEqual(regex.search('(znacnda){s<=1,e<=3,1i+1d<1}', text),
          None)
        self.assertEqual(regex.search('(znacnda){s<=1,e<=3,1i+1d<2}',
          text).span(0, 1), ((9, 17), (9, 17)))
        self.assertEqual(regex.search('(ananda){1i+1d<2}', text), None)
        self.assertEqual(regex.search(r"(?:\bznacnda){e<=2}", text)[0],
          "anaconda")
        self.assertEqual(regex.search(r"(?:\bnacnda){e<=2}", text)[0],
          "anaconda")

        text = 'anaconda foo bar baz smith anderson'
        self.assertEqual(regex.search('(fuu){i<=3,d<=3,e<=5}', text).span(0,
          1), ((0, 0), (0, 0)))
        self.assertEqual(regex.search('(?b)(fuu){i<=3,d<=3,e<=5}',
          text).span(0, 1), ((9, 10), (9, 10)))
        self.assertEqual(regex.search('(fuu){i<=2,d<=2,e<=5}', text).span(0,
          1), ((7, 10), (7, 10)))
        self.assertEqual(regex.search('(?e)(fuu){i<=2,d<=2,e<=5}',
          text).span(0, 1), ((9, 10), (9, 10)))
        self.assertEqual(regex.search('(fuu){i<=3,d<=3,e}', text).span(0, 1),
          ((0, 0), (0, 0)))
        self.assertEqual(regex.search('(?b)(fuu){i<=3,d<=3,e}', text).span(0,
          1), ((9, 10), (9, 10)))

        self.assertEqual(repr(type(regex.compile('(approximate){s<=3,1i+1d<3}'))),
          self.PATTERN_CLASS)

        # No cost limit.
        self.assertEqual(regex.search('(foobar){e}',
          'xirefoabralfobarxie').span(0, 1), ((0, 6), (0, 6)))
        self.assertEqual(regex.search('(?e)(foobar){e}',
          'xirefoabralfobarxie').span(0, 1), ((0, 3), (0, 3)))
        self.assertEqual(regex.search('(?b)(foobar){e}',
          'xirefoabralfobarxie').span(0, 1), ((11, 16), (11, 16)))

        # At most two errors.
        self.assertEqual(regex.search('(foobar){e<=2}',
          'xirefoabrzlfd').span(0, 1), ((4, 9), (4, 9)))
        self.assertEqual(regex.search('(foobar){e<=2}', 'xirefoabzlfd'), None)

        # At most two inserts or substitutions and max two errors total.
        self.assertEqual(regex.search('(foobar){i<=2,s<=2,e<=2}',
          'oobargoobaploowap').span(0, 1), ((5, 11), (5, 11)))

        # Find best whole word match for "foobar".
        self.assertEqual(regex.search('\\b(foobar){e}\\b', 'zfoobarz').span(0,
          1), ((0, 8), (0, 8)))
        self.assertEqual(regex.search('\\b(foobar){e}\\b',
          'boing zfoobarz goobar woop').span(0, 1), ((0, 6), (0, 6)))
        self.assertEqual(regex.search('(?b)\\b(foobar){e}\\b',
          'boing zfoobarz goobar woop').span(0, 1), ((15, 21), (15, 21)))

        # Match whole string, allow only 1 error.
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'foobar').span(0, 1),
          ((0, 6), (0, 6)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'xfoobar').span(0,
          1), ((0, 7), (0, 7)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'foobarx').span(0,
          1), ((0, 7), (0, 7)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'fooxbar').span(0,
          1), ((0, 7), (0, 7)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'foxbar').span(0, 1),
          ((0, 6), (0, 6)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'xoobar').span(0, 1),
          ((0, 6), (0, 6)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'foobax').span(0, 1),
          ((0, 6), (0, 6)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'oobar').span(0, 1),
          ((0, 5), (0, 5)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'fobar').span(0, 1),
          ((0, 5), (0, 5)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'fooba').span(0, 1),
          ((0, 5), (0, 5)))
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'xfoobarx'), None)
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'foobarxx'), None)
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'xxfoobar'), None)
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'xfoxbar'), None)
        self.assertEqual(regex.search('^(foobar){e<=1}$', 'foxbarx'), None)

        # At most one insert, two deletes, and three substitutions.
        # Additionally, deletes cost two and substitutes one, and total
        # cost must be less than 4.
        self.assertEqual(regex.search('(foobar){i<=1,d<=2,s<=3,2d+1s<4}',
          '3oifaowefbaoraofuiebofasebfaobfaorfeoaro').span(0, 1), ((6, 13), (6,
          13)))
        self.assertEqual(regex.search('(?b)(foobar){i<=1,d<=2,s<=3,2d+1s<4}',
          '3oifaowefbaoraofuiebofasebfaobfaorfeoaro').span(0, 1), ((34, 39),
          (34, 39)))

        # Partially fuzzy matches.
        self.assertEqual(regex.search('foo(bar){e<=1}zap', 'foobarzap').span(0,
          1), ((0, 9), (3, 6)))
        self.assertEqual(regex.search('foo(bar){e<=1}zap', 'fobarzap'), None)
        self.assertEqual(regex.search('foo(bar){e<=1}zap', 'foobrzap').span(0,
          1), ((0, 8), (3, 5)))

        text = ('www.cnn.com 64.236.16.20\nwww.slashdot.org 66.35.250.150\n'
          'For useful information, use www.slashdot.org\nthis is demo data!\n')
        self.assertEqual(regex.search(r'(?s)^.*(dot.org){e}.*$', text).span(0,
          1), ((0, 120), (120, 120)))
        self.assertEqual(regex.search(r'(?es)^.*(dot.org){e}.*$', text).span(0,
          1), ((0, 120), (93, 100)))
        self.assertEqual(regex.search(r'^.*(dot.org){e}.*$', text).span(0, 1),
          ((0, 119), (24, 101)))

        # Behaviour is unexpected, but arguably not wrong. It first finds the
        # best match, then the best in what follows, etc.
        self.assertEqual(regex.findall(r"\b\L<words>{e<=1}\b",
          " book cot dog desk ", words="cat dog".split()), ["cot", "dog"])
        self.assertEqual(regex.findall(r"\b\L<words>{e<=1}\b",
          " book dog cot desk ", words="cat dog".split()), [" dog", "cot"])
        self.assertEqual(regex.findall(r"(?e)\b\L<words>{e<=1}\b",
          " book dog cot desk ", words="cat dog".split()), ["dog", "cot"])
        self.assertEqual(regex.findall(r"(?r)\b\L<words>{e<=1}\b",
          " book cot dog desk ", words="cat dog".split()), ["dog ", "cot"])
        self.assertEqual(regex.findall(r"(?er)\b\L<words>{e<=1}\b",
          " book cot dog desk ", words="cat dog".split()), ["dog", "cot"])
        self.assertEqual(regex.findall(r"(?r)\b\L<words>{e<=1}\b",
          " book dog cot desk ", words="cat dog".split()), ["cot", "dog"])
        self.assertEqual(regex.findall(ur"\b\L<words>{e<=1}\b",
          u" book cot dog desk ", words=u"cat dog".split()), [u"cot", u"dog"])
        self.assertEqual(regex.findall(ur"\b\L<words>{e<=1}\b",
          u" book dog cot desk ", words=u"cat dog".split()), [u" dog", u"cot"])
        self.assertEqual(regex.findall(ur"(?e)\b\L<words>{e<=1}\b",
          u" book dog cot desk ", words=u"cat dog".split()), [u"dog", u"cot"])
        self.assertEqual(regex.findall(ur"(?r)\b\L<words>{e<=1}\b",
          u" book cot dog desk ", words=u"cat dog".split()), [u"dog ", u"cot"])
        self.assertEqual(regex.findall(ur"(?er)\b\L<words>{e<=1}\b",
          u" book cot dog desk ", words=u"cat dog".split()), [u"dog", u"cot"])
        self.assertEqual(regex.findall(ur"(?r)\b\L<words>{e<=1}\b",
          u" book dog cot desk ", words=u"cat dog".split()), [u"cot", u"dog"])

        self.assertEqual(regex.search(r"(\w+) (\1{e<=1})", "foo fou").groups(),
          ("foo", "fou"))
        self.assertEqual(regex.search(r"(?r)(\2{e<=1}) (\w+)",
          "foo fou").groups(), ("foo", "fou"))
        self.assertEqual(regex.search(ur"(\w+) (\1{e<=1})",
          u"foo fou").groups(), (u"foo", u"fou"))

        self.assertEqual(regex.findall(r"(?:(?:QR)+){e}", "abcde"), ["abcde",
          ""])
        self.assertEqual(regex.findall(r"(?:Q+){e}", "abc"), ["abc", ""])

        # Hg issue 41: = for fuzzy matches
        self.assertEqual(regex.match(r"(?:service detection){0<e<5}",
          "servic detection").span(), (0, 16))
        self.assertEqual(regex.match(r"(?:service detection){0<e<5}",
          "service detect").span(), (0, 14))
        self.assertEqual(regex.match(r"(?:service detection){0<e<5}",
          "service detecti").span(), (0, 15))
        self.assertEqual(regex.match(r"(?:service detection){0<e<5}",
          "service detection"), None)
        self.assertEqual(regex.match(r"(?:service detection){0<e<5}",
          "in service detection").span(), (0, 20))

        # Hg issue 109: Edit distance of fuzzy match
        self.assertEqual(regex.fullmatch(r"(?:cats|cat){e<=1}",
          "cat").fuzzy_counts, (0, 0, 1))
        self.assertEqual(regex.fullmatch(r"(?e)(?:cats|cat){e<=1}",
          "cat").fuzzy_counts, (0, 0, 0))

        self.assertEqual(regex.fullmatch(r"(?:cat|cats){e<=1}",
          "cats").fuzzy_counts, (0, 1, 0))
        self.assertEqual(regex.fullmatch(r"(?e)(?:cat|cats){e<=1}",
          "cats").fuzzy_counts, (0, 0, 0))

        self.assertEqual(regex.fullmatch(r"(?:cat){e<=1} (?:cat){e<=1}",
          "cat cot").fuzzy_counts, (1, 0, 0))

        # Incorrect fuzzy changes
        self.assertEqual(regex.search(r"(?e)(GTTTTCATTCCTCATA){i<=4,d<=4,s<=4,i+d+s<=8}",
          "ATTATTTATTTTTCATA").fuzzy_changes, ([0, 6, 10, 11], [3], []))

        # Fuzzy constraints ignored when checking for prefix/suffix in branches
        self.assertEqual(bool(regex.match('(?:fo){e<=1}|(?:fo){e<=2}', 'FO')),
          True)

    def test_recursive(self):
        self.assertEqual(regex.search(r"(\w)(?:(?R)|(\w?))\1", "xx")[ : ],
          ("xx", "x", ""))
        self.assertEqual(regex.search(r"(\w)(?:(?R)|(\w?))\1", "aba")[ : ],
          ("aba", "a", "b"))
        self.assertEqual(regex.search(r"(\w)(?:(?R)|(\w?))\1", "abba")[ : ],
          ("abba", "a", None))
        self.assertEqual(regex.search(r"(\w)(?:(?R)|(\w?))\1", "kayak")[ : ],
          ("kayak", "k", None))
        self.assertEqual(regex.search(r"(\w)(?:(?R)|(\w?))\1", "paper")[ : ],
          ("pap", "p", "a"))
        self.assertEqual(regex.search(r"(\w)(?:(?R)|(\w?))\1", "dontmatchme"),
          None)

        self.assertEqual(regex.search(r"(?r)\2(?:(\w?)|(?R))(\w)", "xx")[ : ],
          ("xx", "", "x"))
        self.assertEqual(regex.search(r"(?r)\2(?:(\w?)|(?R))(\w)", "aba")[ : ],
          ("aba", "b", "a"))
        self.assertEqual(regex.search(r"(?r)\2(?:(\w?)|(?R))(\w)", "abba")[ :
          ], ("abba", None, "a"))
        self.assertEqual(regex.search(r"(?r)\2(?:(\w?)|(?R))(\w)", "kayak")[ :
          ], ("kayak", None, "k"))
        self.assertEqual(regex.search(r"(?r)\2(?:(\w?)|(?R))(\w)", "paper")[ :
          ], ("pap", "a", "p"))
        self.assertEqual(regex.search(r"(?r)\2(?:(\w?)|(?R))(\w)",
          "dontmatchme"), None)

        self.assertEqual(regex.search(r"\(((?>[^()]+)|(?R))*\)", "(ab(cd)ef)")[
          : ], ("(ab(cd)ef)", "ef"))
        self.assertEqual(regex.search(r"\(((?>[^()]+)|(?R))*\)",
          "(ab(cd)ef)").captures(1), ["ab", "cd", "(cd)", "ef"])

        self.assertEqual(regex.search(r"(?r)\(((?R)|(?>[^()]+))*\)",
          "(ab(cd)ef)")[ : ], ("(ab(cd)ef)", "ab"))
        self.assertEqual(regex.search(r"(?r)\(((?R)|(?>[^()]+))*\)",
          "(ab(cd)ef)").captures(1), ["ef", "cd", "(cd)", "ab"])

        self.assertEqual(regex.search(r"\(([^()]+|(?R))*\)",
          "some text (a(b(c)d)e) more text")[ : ], ("(a(b(c)d)e)", "e"))

        self.assertEqual(regex.search(r"(?r)\(((?R)|[^()]+)*\)",
          "some text (a(b(c)d)e) more text")[ : ], ("(a(b(c)d)e)", "a"))

        self.assertEqual(regex.search(r"(foo(\(((?:(?>[^()]+)|(?2))*)\)))",
          "foo(bar(baz)+baz(bop))")[ : ], ("foo(bar(baz)+baz(bop))",
          "foo(bar(baz)+baz(bop))", "(bar(baz)+baz(bop))",
          "bar(baz)+baz(bop)"))

        self.assertEqual(regex.search(r"(?r)(foo(\(((?:(?2)|(?>[^()]+))*)\)))",
          "foo(bar(baz)+baz(bop))")[ : ], ("foo(bar(baz)+baz(bop))",
          "foo(bar(baz)+baz(bop))", "(bar(baz)+baz(bop))",
          "bar(baz)+baz(bop)"))

        rgx = regex.compile(r"""^\s*(<\s*([a-zA-Z:]+)(?:\s*[a-zA-Z:]*\s*=\s*(?:'[^']*'|"[^"]*"))*\s*(/\s*)?>(?:[^<>]*|(?1))*(?(3)|<\s*/\s*\2\s*>))\s*$""")
        self.assertEqual(bool(rgx.search('<foo><bar></bar></foo>')), True)
        self.assertEqual(bool(rgx.search('<foo><bar></foo></bar>')), False)
        self.assertEqual(bool(rgx.search('<foo><bar/></foo>')), True)
        self.assertEqual(bool(rgx.search('<foo><bar></foo>')), False)
        self.assertEqual(bool(rgx.search('<foo bar=baz/>')), False)

        self.assertEqual(bool(rgx.search('<foo bar="baz">')), False)
        self.assertEqual(bool(rgx.search('<foo bar="baz"/>')), True)
        self.assertEqual(bool(rgx.search('<    fooo   /  >')), True)
        # The next regex should and does match. Perl 5.14 agrees.
        #self.assertEqual(bool(rgx.search('<foo/>foo')), False)
        self.assertEqual(bool(rgx.search('foo<foo/>')), False)

        self.assertEqual(bool(rgx.search('<foo>foo</foo>')), True)
        self.assertEqual(bool(rgx.search('<foo><bar/>foo</foo>')), True)
        self.assertEqual(bool(rgx.search('<a><b><c></c></b></a>')), True)

    def test_copy(self):
        # PatternObjects are immutable, therefore there's no need to clone them.
        r = regex.compile("a")
        self.assertTrue(copy.copy(r) is r)
        self.assertTrue(copy.deepcopy(r) is r)

        # MatchObjects are normally mutable because the target string can be
        # detached. However, after the target string has been detached, a
        # MatchObject becomes immutable, so there's no need to clone it.
        m = r.match("a")
        self.assertTrue(copy.copy(m) is not m)
        self.assertTrue(copy.deepcopy(m) is not m)

        self.assertTrue(m.string is not None)
        m2 = copy.copy(m)
        m2.detach_string()
        self.assertTrue(m.string is not None)
        self.assertTrue(m2.string is None)

        # The following behaviour matches that of the re module.
        it = regex.finditer(".", "ab")
        it2 = copy.copy(it)
        self.assertEqual(it.next().group(), "a")
        self.assertEqual(it2.next().group(), "b")

        # The following behaviour matches that of the re module.
        it = regex.finditer(".", "ab")
        it2 = copy.deepcopy(it)
        self.assertEqual(it.next().group(), "a")
        self.assertEqual(it2.next().group(), "b")

        # The following behaviour is designed to match that of copying 'finditer'.
        it = regex.splititer(" ", "a b")
        it2 = copy.copy(it)
        self.assertEqual(it.next(), "a")
        self.assertEqual(it2.next(), "b")

        # The following behaviour is designed to match that of copying 'finditer'.
        it = regex.splititer(" ", "a b")
        it2 = copy.deepcopy(it)
        self.assertEqual(it.next(), "a")
        self.assertEqual(it2.next(), "b")

    def test_format(self):
        self.assertEqual(regex.subf(r"(\w+) (\w+)", "{0} => {2} {1}",
          "foo bar"), "foo bar => bar foo")
        self.assertEqual(regex.subf(r"(?<word1>\w+) (?<word2>\w+)",
          "{word2} {word1}", "foo bar"), "bar foo")

        self.assertEqual(regex.subfn(r"(\w+) (\w+)", "{0} => {2} {1}",
          "foo bar"), ("foo bar => bar foo", 1))
        self.assertEqual(regex.subfn(r"(?<word1>\w+) (?<word2>\w+)",
          "{word2} {word1}", "foo bar"), ("bar foo", 1))

        self.assertEqual(regex.match(r"(\w+) (\w+)",
          "foo bar").expandf("{0} => {2} {1}"), "foo bar => bar foo")

    def test_fullmatch(self):
        self.assertEqual(bool(regex.fullmatch(r"abc", "abc")), True)
        self.assertEqual(bool(regex.fullmatch(r"abc", "abcx")), False)
        self.assertEqual(bool(regex.fullmatch(r"abc", "abcx", endpos=3)), True)

        self.assertEqual(bool(regex.fullmatch(r"abc", "xabc", pos=1)), True)
        self.assertEqual(bool(regex.fullmatch(r"abc", "xabcy", pos=1)), False)
        self.assertEqual(bool(regex.fullmatch(r"abc", "xabcy", pos=1,
          endpos=4)), True)

        self.assertEqual(bool(regex.fullmatch(r"(?r)abc", "abc")), True)
        self.assertEqual(bool(regex.fullmatch(r"(?r)abc", "abcx")), False)
        self.assertEqual(bool(regex.fullmatch(r"(?r)abc", "abcx", endpos=3)),
          True)

        self.assertEqual(bool(regex.fullmatch(r"(?r)abc", "xabc", pos=1)),
          True)
        self.assertEqual(bool(regex.fullmatch(r"(?r)abc", "xabcy", pos=1)),
          False)
        self.assertEqual(bool(regex.fullmatch(r"(?r)abc", "xabcy", pos=1,
          endpos=4)), True)

    def test_hg_bugs(self):
        # Hg issue 28: regex.compile("(?>b)") causes "TypeError: 'Character'
        # object is not subscriptable"
        self.assertEqual(bool(regex.compile("(?>b)", flags=regex.V1)), True)

        # Hg issue 29: regex.compile("^((?>\w+)|(?>\s+))*$") causes
        # "TypeError: 'GreedyRepeat' object is not iterable"
        self.assertEqual(bool(regex.compile(r"^((?>\w+)|(?>\s+))*$",
          flags=regex.V1)), True)

        # Hg issue 31: atomic and normal groups in recursive patterns
        self.assertEqual(regex.findall(r"\((?:(?>[^()]+)|(?R))*\)",
          "a(bcd(e)f)g(h)"), ['(bcd(e)f)', '(h)'])
        self.assertEqual(regex.findall(r"\((?:(?:[^()]+)|(?R))*\)",
          "a(bcd(e)f)g(h)"), ['(bcd(e)f)', '(h)'])
        self.assertEqual(regex.findall(r"\((?:(?>[^()]+)|(?R))*\)",
          "a(b(cd)e)f)g)h"), ['(b(cd)e)'])
        self.assertEqual(regex.findall(r"\((?:(?>[^()]+)|(?R))*\)",
          "a(bc(d(e)f)gh"), ['(d(e)f)'])
        self.assertEqual(regex.findall(r"(?r)\((?:(?>[^()]+)|(?R))*\)",
          "a(bc(d(e)f)gh"), ['(d(e)f)'])
        self.assertEqual([m.group() for m in
          regex.finditer(r"\((?:[^()]*+|(?0))*\)", "a(b(c(de)fg)h")],
          ['(c(de)fg)'])

        # Hg issue 32: regex.search("a(bc)d", "abcd", regex.I|regex.V1) returns
        # None
        self.assertEqual(regex.search("a(bc)d", "abcd", regex.I |
          regex.V1).group(0), "abcd")

        # Hg issue 33: regex.search("([\da-f:]+)$", "E", regex.I|regex.V1)
        # returns None
        self.assertEqual(regex.search(r"([\da-f:]+)$", "E", regex.I |
          regex.V1).group(0), "E")
        self.assertEqual(regex.search(r"([\da-f:]+)$", "e", regex.I |
          regex.V1).group(0), "e")

        # Hg issue 34: regex.search("^(?=ab(de))(abd)(e)", "abde").groups()
        # returns (None, 'abd', 'e') instead of ('de', 'abd', 'e')
        self.assertEqual(regex.search("^(?=ab(de))(abd)(e)", "abde").groups(),
          ('de', 'abd', 'e'))

        # Hg issue 35: regex.compile("\ ", regex.X) causes "_regex_core.error:
        # bad escape"
        self.assertEqual(bool(regex.match(r"\ ", " ", flags=regex.X)), True)

        # Hg issue 36: regex.search("^(a|)\1{2}b", "b") returns None
        self.assertEqual(regex.search(r"^(a|)\1{2}b", "b").group(0, 1), ('b',
          ''))

        # Hg issue 37: regex.search("^(a){0,0}", "abc").group(0,1) returns
        # ('a', 'a') instead of ('', None)
        self.assertEqual(regex.search("^(a){0,0}", "abc").group(0, 1), ('',
          None))

        # Hg issue 38: regex.search("(?>.*/)b", "a/b") returns None
        self.assertEqual(regex.search("(?>.*/)b", "a/b").group(0), "a/b")

        # Hg issue 39: regex.search("((?i)blah)\\s+\\1", "blah BLAH") doesn't
        # return None
        self.assertEqual(regex.search(r"(?V0)((?i)blah)\s+\1",
          "blah BLAH").group(0, 1), ("blah BLAH", "blah"))
        self.assertEqual(regex.search(r"(?V1)((?i)blah)\s+\1", "blah BLAH"),
          None)

        # Hg issue 40: regex.search("(\()?[^()]+(?(1)\)|)", "(abcd").group(0)
        # returns "bcd" instead of "abcd"
        self.assertEqual(regex.search(r"(\()?[^()]+(?(1)\)|)",
          "(abcd").group(0), "abcd")

        # Hg issue 42: regex.search("(a*)*", "a", flags=regex.V1).span(1)
        # returns (0, 1) instead of (1, 1)
        self.assertEqual(regex.search("(a*)*", "a").span(1), (1, 1))
        self.assertEqual(regex.search("(a*)*", "aa").span(1), (2, 2))
        self.assertEqual(regex.search("(a*)*", "aaa").span(1), (3, 3))

        # Hg issue 43: regex.compile("a(?#xxx)*") causes "_regex_core.error:
        # nothing to repeat"
        self.assertEqual(regex.search("a(?#xxx)*", "aaa").group(), "aaa")

        # Hg issue 44: regex.compile("(?=abc){3}abc") causes
        # "_regex_core.error: nothing to repeat"
        self.assertEqual(regex.search("(?=abc){3}abc", "abcabcabc").span(), (0,
          3))

        # Hg issue 45: regex.compile("^(?:a(?:(?:))+)+") causes
        # "_regex_core.error: nothing to repeat"
        self.assertEqual(regex.search("^(?:a(?:(?:))+)+", "a").span(), (0, 1))
        self.assertEqual(regex.search("^(?:a(?:(?:))+)+", "aa").span(), (0, 2))

        # Hg issue 46: regex.compile("a(?x: b c )d") causes
        # "_regex_core.error: missing )"
        self.assertEqual(regex.search("a(?x: b c )d", "abcd").group(0), "abcd")

        # Hg issue 47: regex.compile("a#comment\n*", flags=regex.X) causes
        # "_regex_core.error: nothing to repeat"
        self.assertEqual(regex.search("a#comment\n*", "aaa",
          flags=regex.X).group(0), "aaa")

        # Hg issue 48: regex.search("(a(?(1)\\1)){4}", "a"*10,
        # flags=regex.V1).group(0,1) returns ('aaaaa', 'a') instead of ('aaaaaaaaaa', 'aaaa')
        self.assertEqual(regex.search(r"(?V1)(a(?(1)\1)){1}",
          "aaaaaaaaaa").span(0, 1), ((0, 1), (0, 1)))
        self.assertEqual(regex.search(r"(?V1)(a(?(1)\1)){2}",
          "aaaaaaaaaa").span(0, 1), ((0, 3), (1, 3)))
        self.assertEqual(regex.search(r"(?V1)(a(?(1)\1)){3}",
          "aaaaaaaaaa").span(0, 1), ((0, 6), (3, 6)))
        self.assertEqual(regex.search(r"(?V1)(a(?(1)\1)){4}",
          "aaaaaaaaaa").span(0, 1), ((0, 10), (6, 10)))

        # Hg issue 49: regex.search("(a)(?<=b(?1))", "baz", regex.V1) returns
        # None incorrectly
        self.assertEqual(regex.search("(?V1)(a)(?<=b(?1))", "baz").group(0),
          "a")

        # Hg issue 50: not all keywords are found by named list with
        # overlapping keywords when full Unicode casefolding is required
        self.assertEqual(regex.findall(ur'(?fi)\L<keywords>',
          u'POST, Post, post, po\u017Ft, po\uFB06, and po\uFB05',
          keywords=['post','pos']), [u'POST', u'Post', u'post', u'po\u017Ft',
          u'po\uFB06', u'po\uFB05'])
        self.assertEqual(regex.findall(ur'(?fi)pos|post',
          u'POST, Post, post, po\u017Ft, po\uFB06, and po\uFB05'), [u'POS',
          u'Pos', u'pos', u'po\u017F', u'po\uFB06', u'po\uFB05'])
        self.assertEqual(regex.findall(ur'(?fi)post|pos',
          u'POST, Post, post, po\u017Ft, po\uFB06, and po\uFB05'), [u'POST',
          u'Post', u'post', u'po\u017Ft', u'po\uFB06', u'po\uFB05'])
        self.assertEqual(regex.findall(ur'(?fi)post|another',
          u'POST, Post, post, po\u017Ft, po\uFB06, and po\uFB05'), [u'POST',
          u'Post', u'post', u'po\u017Ft', u'po\uFB06', u'po\uFB05'])

        # Hg issue 51: regex.search("((a)(?1)|(?2))", "a", flags=regex.V1)
        # returns None incorrectly
        self.assertEqual(regex.search("(?V1)((a)(?1)|(?2))", "a").group(0, 1,
          2), ('a', 'a', None))

        # Hg issue 52: regex.search("(\\1xx|){6}", "xx",
        # flags=regex.V1).span(0,1) returns incorrect value
        self.assertEqual(regex.search(r"(?V1)(\1xx|){6}", "xx").span(0, 1),
          ((0, 2), (2, 2)))

        # Hg issue 53: regex.search("(a|)+", "a") causes MemoryError
        self.assertEqual(regex.search("(a|)+", "a").group(0, 1), ("a", ""))

        # Hg issue 54: regex.search("(a|)*\\d", "a"*80) causes MemoryError
        self.assertEqual(regex.search(r"(a|)*\d", "a" * 80), None)

        # Hg issue 55: regex.search("^(?:a?b?)*$", "ac") take a very long time.
        self.assertEqual(regex.search("^(?:a?b?)*$", "ac"), None)

        # Hg issue 58: bad named character escape sequences like "\\N{1}"
        # treats as "N"
        self.assertRaisesRegex(regex.error, self.UNDEF_CHAR_NAME, lambda:
          regex.compile("\\N{1}"))

        # Hg issue 59: regex.search("\\Z", "a\na\n") returns None incorrectly
        self.assertEqual(regex.search("\\Z", "a\na\n").span(0), (4, 4))

        # Hg issue 60: regex.search("(q1|.)*(q2|.)*(x(a|bc)*y){2,}", "xayxay")
        # returns None incorrectly
        self.assertEqual(regex.search("(q1|.)*(q2|.)*(x(a|bc)*y){2,}",
          "xayxay").group(0), "xayxay")

        # Hg issue 61: regex.search("[^a]", "A", regex.I).group(0) returns ''
        # incorrectly
        self.assertEqual(regex.search("(?i)[^a]", "A"), None)

        # Hg issue 63: regex.search("[[:ascii:]]", "\N{KELVIN SIGN}",
        # flags=regex.I|regex.V1) doesn't return None
        self.assertEqual(regex.search(u"(?iu)[[:ascii:]]", u"\N{KELVIN SIGN}"),
          None)

        # Hg issue 66: regex.search("((a|b(?1)c){3,5})", "baaaaca",
        # flags=regex.V1).groups() returns ('baaaac', 'baaaac') instead of ('aaaa', 'a')
        self.assertEqual(regex.search("((a|b(?1)c){3,5})", "baaaaca").group(0,
          1, 2), ('aaaa', 'aaaa', 'a'))

        # Hg issue 71: non-greedy quantifier in lookbehind
        self.assertEqual(regex.findall(r"(?<=:\S+ )\w+", ":9 abc :10 def"),
          ['abc', 'def'])
        self.assertEqual(regex.findall(r"(?<=:\S* )\w+", ":9 abc :10 def"),
          ['abc', 'def'])
        self.assertEqual(regex.findall(r"(?<=:\S+? )\w+", ":9 abc :10 def"),
          ['abc', 'def'])
        self.assertEqual(regex.findall(r"(?<=:\S*? )\w+", ":9 abc :10 def"),
          ['abc', 'def'])

        # Hg issue 73: conditional patterns
        self.assertEqual(regex.search(r"(?:fe)?male", "female").group(),
          "female")
        self.assertEqual([m.group() for m in
          regex.finditer(r"(fe)?male: h(?(1)(er)|(is)) (\w+)",
          "female: her dog; male: his cat. asdsasda")], ['female: her dog',
          'male: his cat'])

        # Hg issue 78: "Captures"doesn't work for recursive calls
        self.assertEqual(regex.search(r'(?<rec>\((?:[^()]++|(?&rec))*\))',
          'aaa(((1+0)+1)+1)bbb').captures('rec'), ['(1+0)', '((1+0)+1)',
          '(((1+0)+1)+1)'])

        # Hg issue 80: Escape characters throws an exception
        self.assertRaisesRegex(regex.error, self.BAD_ESCAPE, lambda:
          regex.sub('x', '\\', 'x'), )

        # Hg issue 82: error range does not work
        fz = "(CAGCCTCCCATTTCAGAATATACATCC){1<e<=2}"
        seq = "tcagacgagtgcgttgtaaaacgacggccagtCAGCCTCCCATTCAGAATATACATCCcgacggccagttaaaaacaatgccaaggaggtcatagctgtttcctgccagttaaaaacaatgccaaggaggtcatagctgtttcctgacgcactcgtctgagcgggctggcaagg"
        self.assertEqual(regex.search(fz, seq, regex.BESTMATCH)[0],
          "tCAGCCTCCCATTCAGAATATACATCC")

        # Hg issue 83: slash handling in presence of a quantifier
        self.assertEqual(regex.findall(r"c..+/c", "cA/c\ncAb/c"), ['cAb/c'])

        # Hg issue 85: Non-conformance to Unicode UAX#29 re: ZWJ / ZWNJ
        self.assertEqual(repr(regex.sub(ur"(?u)(\w+)", ur"[\1]",
          u'\u0905\u0928\u094d\u200d\u0928 \u0d28\u0d4d\u200d \u0915\u093f\u0928',
          regex.WORD)),
          repr(u'[\u0905\u0928\u094d\u200d\u0928] [\u0d28\u0d4d\u200d] [\u0915\u093f\u0928]'))

        # Hg issue 88: regex.match() hangs
        self.assertEqual(regex.match(r".*a.*ba.*aa", "ababba"), None)

        # Hg issue 87: Allow duplicate names of groups
        self.assertEqual(regex.match(r'(?<x>a(?<x>b))', "ab").spans("x"), [(1,
          2), (0, 2)])

        # Hg issue 91: match.expand is extremely slow
        # Check that the replacement cache works.
        self.assertEqual(regex.sub(r'(-)', lambda m: m.expand(r'x'), 'a-b-c'),
          'axbxc')

        # Hg issue 94: Python crashes when executing regex updates
        # pattern.findall
        rx = regex.compile(r'\bt(est){i<2}', flags=regex.V1)
        self.assertEqual(rx.search("Some text"), None)
        self.assertEqual(rx.findall("Some text"), [])

        # Hg issue 95: 'pos' for regex.error
        self.assertRaisesRegex(regex.error, self.MULTIPLE_REPEAT, lambda:
          regex.compile(r'.???'))

        # Hg issue 97: behaviour of regex.escape's special_only is wrong
        #
        # Hg issue 244: Make `special_only=True` the default in
        # `regex.escape()`
        self.assertEqual(regex.escape(u'foo!?', special_only=False), u'foo\\!\\?')
        self.assertEqual(regex.escape(u'foo!?', special_only=True), u'foo!\\?')
        self.assertEqual(regex.escape(u'foo!?'), u'foo!\\?')

        self.assertEqual(regex.escape('foo!?', special_only=False), 'foo\\!\\?')
        self.assertEqual(regex.escape('foo!?', special_only=True),
          'foo!\\?')
        self.assertEqual(regex.escape('foo!?'), 'foo!\\?')

        # Hg issue 100: strange results from regex.search
        self.assertEqual(regex.search('^([^z]*(?:WWWi|W))?$',
          'WWWi').groups(), ('WWWi', ))
        self.assertEqual(regex.search('^([^z]*(?:WWWi|w))?$',
          'WWWi').groups(), ('WWWi', ))
        self.assertEqual(regex.search('^([^z]*?(?:WWWi|W))?$',
          'WWWi').groups(), ('WWWi', ))

        # Hg issue 101: findall() broken (seems like memory corruption)
        pat = regex.compile(r'xxx', flags=regex.FULLCASE | regex.UNICODE)
        self.assertEqual([x.group() for x in pat.finditer('yxxx')], ['xxx'])
        self.assertEqual(pat.findall('yxxx'), ['xxx'])

        raw = 'yxxx'
        self.assertEqual([x.group() for x in pat.finditer(raw)], ['xxx'])
        self.assertEqual(pat.findall(raw), ['xxx'])

        pat = regex.compile(r'xxx', flags=regex.FULLCASE | regex.IGNORECASE |
          regex.UNICODE)
        self.assertEqual([x.group() for x in pat.finditer('yxxx')], ['xxx'])
        self.assertEqual(pat.findall('yxxx'), ['xxx'])

        raw = 'yxxx'
        self.assertEqual([x.group() for x in pat.finditer(raw)], ['xxx'])
        self.assertEqual(pat.findall(raw), ['xxx'])

        # Hg issue 106: * operator not working correctly with sub()
        self.assertEqual(regex.sub('(?V0).*', 'x', 'test'), 'x')
        self.assertEqual(regex.sub('(?V1).*', 'x', 'test'), 'xx')

        self.assertEqual(regex.sub('(?V0).*?', '|', 'test'), '|t|e|s|t|')
        self.assertEqual(regex.sub('(?V1).*?', '|', 'test'), '|||||||||')

        # Hg issue 112: re: OK, but regex: SystemError
        self.assertEqual(regex.sub(r'^(@)\n(?!.*?@)(.*)',
          r'\1\n==========\n\2', '@\n', flags=regex.DOTALL), '@\n==========\n')

        # Hg issue 109: Edit distance of fuzzy match
        self.assertEqual(regex.match(r'(?:cats|cat){e<=1}',
         'caz').fuzzy_counts, (1, 0, 0))
        self.assertEqual(regex.match(r'(?e)(?:cats|cat){e<=1}',
          'caz').fuzzy_counts, (1, 0, 0))
        self.assertEqual(regex.match(r'(?b)(?:cats|cat){e<=1}',
          'caz').fuzzy_counts, (1, 0, 0))

        self.assertEqual(regex.match(r'(?:cat){e<=1}', 'caz').fuzzy_counts,
          (1, 0, 0))
        self.assertEqual(regex.match(r'(?e)(?:cat){e<=1}',
          'caz').fuzzy_counts, (1, 0, 0))
        self.assertEqual(regex.match(r'(?b)(?:cat){e<=1}',
          'caz').fuzzy_counts, (1, 0, 0))

        self.assertEqual(regex.match(r'(?:cats){e<=2}', 'c ats').fuzzy_counts,
          (1, 1, 0))
        self.assertEqual(regex.match(r'(?e)(?:cats){e<=2}',
          'c ats').fuzzy_counts, (0, 1, 0))
        self.assertEqual(regex.match(r'(?b)(?:cats){e<=2}',
          'c ats').fuzzy_counts, (0, 1, 0))

        self.assertEqual(regex.match(r'(?:cats){e<=2}',
          'c a ts').fuzzy_counts, (0, 2, 0))
        self.assertEqual(regex.match(r'(?e)(?:cats){e<=2}',
          'c a ts').fuzzy_counts, (0, 2, 0))
        self.assertEqual(regex.match(r'(?b)(?:cats){e<=2}',
          'c a ts').fuzzy_counts, (0, 2, 0))

        self.assertEqual(regex.match(r'(?:cats){e<=1}', 'c ats').fuzzy_counts,
          (0, 1, 0))
        self.assertEqual(regex.match(r'(?e)(?:cats){e<=1}',
          'c ats').fuzzy_counts, (0, 1, 0))
        self.assertEqual(regex.match(r'(?b)(?:cats){e<=1}',
          'c ats').fuzzy_counts, (0, 1, 0))

        # Hg issue 115: Infinite loop when processing backreferences
        self.assertEqual(regex.findall(r'\bof ([a-z]+) of \1\b',
          'To make use of one of these modules'), [])

        # Hg issue 125: Reference to entire match (\g&lt;0&gt;) in
        # Pattern.sub() doesn't work as of 2014.09.22 release.
        self.assertEqual(regex.sub(r'x', r'\g<0>', 'x'), 'x')

        # Unreported issue: no such builtin as 'ascii' in Python 2.
        self.assertEqual(bool(regex.match(r'a', 'a', regex.DEBUG)), True)

        # Hg issue 131: nested sets behaviour
        self.assertEqual(regex.findall(r'(?V1)[[b-e]--cd]', 'abcdef'), ['b',
          'e'])
        self.assertEqual(regex.findall(r'(?V1)[b-e--cd]', 'abcdef'), ['b',
          'e'])
        self.assertEqual(regex.findall(r'(?V1)[[bcde]--cd]', 'abcdef'), ['b',
          'e'])
        self.assertEqual(regex.findall(r'(?V1)[bcde--cd]', 'abcdef'), ['b',
          'e'])

        # Hg issue 132: index out of range on null property \p{}
        self.assertRaisesRegex(regex.error, '^unknown property at position 4$',
          lambda: regex.compile(ur'\p{}'))

        # Issue 23692.
        self.assertEqual(regex.match('(?:()|(?(1)()|z)){2}(?(2)a|z)',
          'a').group(0, 1, 2), ('a', '', ''))
        self.assertEqual(regex.match('(?:()|(?(1)()|z)){0,2}(?(2)a|z)',
          'a').group(0, 1, 2), ('a', '', ''))

        # Hg issue 137: Posix character class :punct: does not seem to be
        # supported.

        # Posix compatibility as recommended here:
        # http://www.unicode.org/reports/tr18/#Compatibility_Properties

        # Posix in Unicode.
        chars = u''.join(unichr(c) for c in range(0x10000))

        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:alnum:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[\p{Alpha}\p{PosixDigit}]+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:alpha:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)\p{Alpha}+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:ascii:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[\p{InBasicLatin}]+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:blank:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[\p{gc=Space_Separator}\t]+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:cntrl:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)\p{gc=Control}+''', chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:digit:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[0-9]+''', chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:graph:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[^\p{Space}\p{gc=Control}\p{gc=Surrogate}\p{gc=Unassigned}]+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:lower:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)\p{Lower}+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:print:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?uV1)[\p{Graph}\p{Blank}--\p{Cntrl}]+''', chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:punct:]]+''',
          chars))),
          repr(u''.join(regex.findall(ur'''(?uV1)[\p{gc=Punctuation}\p{gc=Symbol}--\p{Alpha}]+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:space:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)\p{Whitespace}+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:upper:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)\p{Upper}+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:word:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[\p{Alpha}\p{gc=Mark}\p{Digit}\p{gc=Connector_Punctuation}\p{Join_Control}]+''',
          chars))))
        self.assertEqual(repr(u''.join(regex.findall(ur'''(?u)[[:xdigit:]]+''',
          chars))), repr(u''.join(regex.findall(ur'''(?u)[0-9A-Fa-f]+''',
          chars))))

        # Posix in ASCII.
        chars = ''.join(chr(c) for c in range(0x100))

        self.assertEqual(repr(''.join(regex.findall(r'''[[:alnum:]]+''',
          chars))), repr(''.join(regex.findall(r'''[\p{Alpha}\p{PosixDigit}]+''',
          chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:alpha:]]+''',
          chars))), repr(''.join(regex.findall(r'''\p{Alpha}+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:ascii:]]+''',
          chars))), repr(''.join(regex.findall(r'''[\x00-\x7F]+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:blank:]]+''',
          chars))), repr(''.join(regex.findall(r'''[\p{gc=Space_Separator}\t]+''',
          chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:cntrl:]]+''',
          chars))), repr(''.join(regex.findall(r'''\p{gc=Control}+''',
          chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:digit:]]+''',
          chars))), repr(''.join(regex.findall(r'''[0-9]+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:graph:]]+''',
          chars))), repr(''.join(regex.findall(r'''[^\p{Space}\p{gc=Control}\p{gc=Surrogate}\p{gc=Unassigned}]+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:lower:]]+''',
          chars))), repr(''.join(regex.findall(r'''\p{Lower}+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:print:]]+''',
          chars))), repr(''.join(regex.findall(r'''(?V1)[\p{Graph}\p{Blank}--\p{Cntrl}]+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:punct:]]+''',
          chars))), repr(''.join(regex.findall(r'''(?V1)[\p{gc=Punctuation}\p{gc=Symbol}--\p{Alpha}]+''',
          chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:space:]]+''',
          chars))), repr(''.join(regex.findall(r'''\p{Whitespace}+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:upper:]]+''',
          chars))), repr(''.join(regex.findall(r'''\p{Upper}+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:word:]]+''',
          chars))), repr(''.join(regex.findall(r'''[\p{Alpha}\p{gc=Mark}\p{Digit}\p{gc=Connector_Punctuation}\p{Join_Control}]+''', chars))))
        self.assertEqual(repr(''.join(regex.findall(r'''[[:xdigit:]]+''',
          chars))), repr(''.join(regex.findall(r'''[0-9A-Fa-f]+''', chars))))

        # Hg issue 138: grapheme anchored search not working properly.
        self.assertEqual(repr(regex.search(ur'(?u)\X$', u'ab\u2103').group()),
          repr(u'\u2103'))

        # Hg issue 139: Regular expression with multiple wildcards where first
        # should match empty string does not always work.
        self.assertEqual(regex.search("([^L]*)([^R]*R)", "LtR").groups(), ('',
          'LtR'))

        # Hg issue 140: Replace with REVERSE and groups has unexpected
        # behavior.
        self.assertEqual(regex.sub(r'(.)', r'x\1y', 'ab'), 'xayxby')
        self.assertEqual(regex.sub(r'(?r)(.)', r'x\1y', 'ab'), 'xayxby')

        # Hg issue 141: Crash on a certain partial match.
        self.assertEqual(regex.fullmatch('(a)*abc', 'ab',
          partial=True).span(), (0, 2))
        self.assertEqual(regex.fullmatch('(a)*abc', 'ab',
          partial=True).partial, True)

        # Hg issue 143: Partial matches have incorrect span if prefix is '.'
        # wildcard.
        self.assertEqual(regex.search('OXRG', 'OOGOX', partial=True).span(),
          (3, 5))
        self.assertEqual(regex.search('.XRG', 'OOGOX', partial=True).span(),
          (3, 5))
        self.assertEqual(regex.search('.{1,3}XRG', 'OOGOX',
          partial=True).span(), (1, 5))

        # Hg issue 144: Latest version problem with matching 'R|R'.
        self.assertEqual(regex.match('R|R', 'R').span(), (0, 1))

        # Hg issue 146: Forced-fail (?!) works improperly in conditional.
        self.assertEqual(regex.match(r'(.)(?(1)(?!))', 'xy'), None)

        # Groups cleared after failure.
        self.assertEqual(regex.findall(r'(y)?(\d)(?(1)\b\B)', 'ax1y2z3b'),
          [('', '1'), ('', '2'), ('', '3')])
        self.assertEqual(regex.findall(r'(y)?+(\d)(?(1)\b\B)', 'ax1y2z3b'),
          [('', '1'), ('', '2'), ('', '3')])

        # Hg issue 147: Fuzzy match can return match points beyond buffer end.
        self.assertEqual([m.span() for m in regex.finditer(r'(?i)(?:error){e}',
          'regex failure')], [(0, 5), (5, 10), (10, 13), (13, 13)])
        self.assertEqual([m.span() for m in
          regex.finditer(r'(?fi)(?:error){e}', 'regex failure')], [(0, 5), (5,
          10), (10, 13), (13, 13)])

        # Hg issue 150: Have an option for POSIX-compatible longest match of
        # alternates.
        self.assertEqual(regex.search(r'(?p)\d+(\w(\d*)?|[eE]([+-]\d+))',
          '10b12')[0], '10b12')
        self.assertEqual(regex.search(r'(?p)\d+(\w(\d*)?|[eE]([+-]\d+))',
          '10E+12')[0], '10E+12')

        self.assertEqual(regex.search(r'(?p)(\w|ae|oe|ue|ss)', 'ae')[0], 'ae')
        self.assertEqual(regex.search(r'(?p)one(self)?(selfsufficient)?',
          'oneselfsufficient')[0], 'oneselfsufficient')

        # Hg issue 151: Request: \K.
        self.assertEqual(regex.search(r'(ab\Kcd)', 'abcd').group(0, 1), ('cd',
          'abcd'))
        self.assertEqual(regex.findall(r'\w\w\K\w\w', 'abcdefgh'), ['cd',
          'gh'])
        self.assertEqual(regex.findall(r'(\w\w\K\w\w)', 'abcdefgh'), ['abcd',
          'efgh'])

        self.assertEqual(regex.search(r'(?r)(ab\Kcd)', 'abcd').group(0, 1),
          ('ab', 'abcd'))
        self.assertEqual(regex.findall(r'(?r)\w\w\K\w\w', 'abcdefgh'), ['ef',
          'ab'])
        self.assertEqual(regex.findall(r'(?r)(\w\w\K\w\w)', 'abcdefgh'),
          ['efgh', 'abcd'])

        # Hg issue 152: Request: Request: (?(DEFINE)...).
        self.assertEqual(regex.search(r'(?(DEFINE)(?<quant>\d+)(?<item>\w+))(?&quant) (?&item)',
          '5 elephants')[0], '5 elephants')

        # Hg issue 153: Request: (*SKIP).
        self.assertEqual(regex.search(r'12(*FAIL)|3', '123')[0], '3')
        self.assertEqual(regex.search(r'(?r)12(*FAIL)|3', '123')[0], '3')

        self.assertEqual(regex.search(r'\d+(*PRUNE)\d', '123'), None)
        self.assertEqual(regex.search(r'\d+(?=(*PRUNE))\d', '123')[0], '123')
        self.assertEqual(regex.search(r'\d+(*PRUNE)bcd|[3d]', '123bcd')[0],
          '123bcd')
        self.assertEqual(regex.search(r'\d+(*PRUNE)bcd|[3d]', '123zzd')[0],
          'd')
        self.assertEqual(regex.search(r'\d+?(*PRUNE)bcd|[3d]', '123bcd')[0],
          '3bcd')
        self.assertEqual(regex.search(r'\d+?(*PRUNE)bcd|[3d]', '123zzd')[0],
          'd')
        self.assertEqual(regex.search(r'\d++(?<=3(*PRUNE))zzd|[4d]$',
          '123zzd')[0], '123zzd')
        self.assertEqual(regex.search(r'\d++(?<=3(*PRUNE))zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'\d++(?<=(*PRUNE)3)zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'\d++(?<=2(*PRUNE)3)zzd|[3d]$',
          '124zzd')[0], 'd')

        self.assertEqual(regex.search(r'(?r)\d(*PRUNE)\d+', '123'), None)
        self.assertEqual(regex.search(r'(?r)\d(?<=(*PRUNE))\d+', '123')[0],
          '123')
        self.assertEqual(regex.search(r'(?r)\d+(*PRUNE)bcd|[3d]',
          '123bcd')[0], '123bcd')
        self.assertEqual(regex.search(r'(?r)\d+(*PRUNE)bcd|[3d]',
          '123zzd')[0], 'd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=3(*PRUNE))zzd|[4d]$',
          '123zzd')[0], '123zzd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=3(*PRUNE))zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=(*PRUNE)3)zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=2(*PRUNE)3)zzd|[3d]$',
          '124zzd')[0], 'd')

        self.assertEqual(regex.search(r'\d+(*SKIP)bcd|[3d]', '123bcd')[0],
          '123bcd')
        self.assertEqual(regex.search(r'\d+(*SKIP)bcd|[3d]', '123zzd')[0],
          'd')
        self.assertEqual(regex.search(r'\d+?(*SKIP)bcd|[3d]', '123bcd')[0],
          '3bcd')
        self.assertEqual(regex.search(r'\d+?(*SKIP)bcd|[3d]', '123zzd')[0],
          'd')
        self.assertEqual(regex.search(r'\d++(?<=3(*SKIP))zzd|[4d]$',
          '123zzd')[0], '123zzd')
        self.assertEqual(regex.search(r'\d++(?<=3(*SKIP))zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'\d++(?<=(*SKIP)3)zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'\d++(?<=2(*SKIP)3)zzd|[3d]$',
          '124zzd')[0], 'd')

        self.assertEqual(regex.search(r'(?r)\d+(*SKIP)bcd|[3d]', '123bcd')[0],
          '123bcd')
        self.assertEqual(regex.search(r'(?r)\d+(*SKIP)bcd|[3d]', '123zzd')[0],
          'd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=3(*SKIP))zzd|[4d]$',
          '123zzd')[0], '123zzd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=3(*SKIP))zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=(*SKIP)3)zzd|[4d]$',
          '124zzd')[0], 'd')
        self.assertEqual(regex.search(r'(?r)\d++(?<=2(*SKIP)3)zzd|[3d]$',
          '124zzd')[0], 'd')

        # Hg issue 154: Segmentation fault 11 when working with an atomic group
        text = """June 30, December 31, 2013 2012
some words follow:
more words and numbers 1,234,567 9,876,542
more words and numbers 1,234,567 9,876,542"""
        self.assertEqual(len(regex.findall(r'(?<!\d)(?>2014|2013 ?2012)', text)), 1)

        # Hg issue 156: regression on atomic grouping
        self.assertEqual(regex.match('1(?>2)', '12').span(), (0, 2))

        # Hg issue 157: regression: segfault on complex lookaround
        self.assertEqual(regex.match(r'(?V1w)(?=(?=[^A-Z]*+[A-Z])(?=[^a-z]*+[a-z]))(?=\D*+\d)(?=\p{Alphanumeric}*+\P{Alphanumeric})\A(?s:.){8,255}+\Z',
          'AAaa11!!')[0], 'AAaa11!!')

        # Hg issue 158: Group issue with (?(DEFINE)...)
        TEST_REGEX = regex.compile(r'''(?smx)
(?(DEFINE)
  (?<subcat>
   ^,[^,]+,
   )
)

# Group 2 is defined on this line
^,([^,]+),

(?:(?!(?&subcat)[\r\n]+(?&subcat)).)+
''')

        TEST_DATA = '''
,Cat 1,
,Brand 1,
some
thing
,Brand 2,
other
things
,Cat 2,
,Brand,
Some
thing
'''

        self.assertEqual([m.span(1, 2) for m in
          TEST_REGEX.finditer(TEST_DATA)], [((-1, -1), (2, 7)), ((-1, -1), (54,
          59))])

        # Hg issue 161: Unexpected fuzzy match results
        self.assertEqual(regex.search('(abcdefgh){e}',
          '******abcdefghijklmnopqrtuvwxyz', regex.BESTMATCH).span(), (6, 14))
        self.assertEqual(regex.search('(abcdefghi){e}',
          '******abcdefghijklmnopqrtuvwxyz', regex.BESTMATCH).span(), (6, 15))

        # Hg issue 163: allow lookarounds in conditionals.
        self.assertEqual(regex.match(r'(?:(?=\d)\d+\b|\w+)', '123abc').span(),
          (0, 6))
        self.assertEqual(regex.match(r'(?(?=\d)\d+\b|\w+)', '123abc'), None)
        self.assertEqual(regex.search(r'(?(?<=love\s)you|(?<=hate\s)her)',
          "I love you").span(), (7, 10))
        self.assertEqual(regex.findall(r'(?(?<=love\s)you|(?<=hate\s)her)',
          "I love you but I don't hate her either"), ['you', 'her'])

        # Hg issue 180: bug of POSIX matching.
        self.assertEqual(regex.search(r'(?p)a*(.*?)', 'aaabbb').group(0, 1),
          ('aaabbb', 'bbb'))
        self.assertEqual(regex.search(r'(?p)a*(.*)', 'aaabbb').group(0, 1),
          ('aaabbb', 'bbb'))
        self.assertEqual(regex.sub(r'(?p)a*(.*?)', r'\1', 'aaabbb'), 'bbb')
        self.assertEqual(regex.sub(r'(?p)a*(.*)', r'\1', 'aaabbb'), 'bbb')

        # Hg issue 192: Named lists reverse matching doesn't work with
        # IGNORECASE and V1
        self.assertEqual(regex.match(r'(?irV0)\L<kw>', '21', kw=['1']).span(),
          (1, 2))
        self.assertEqual(regex.match(r'(?irV1)\L<kw>', '21', kw=['1']).span(),
          (1, 2))

        # Hg issue 193: Alternation and .REVERSE flag.
        self.assertEqual(regex.search('a|b', '111a222').span(), (3, 4))
        self.assertEqual(regex.search('(?r)a|b', '111a222').span(), (3, 4))

        # Hg issue 194: .FULLCASE and Backreference
        self.assertEqual(regex.search(r'(?if)<(CLI)><\1>',
          '<cli><cli>').span(), (0, 10))
        self.assertEqual(regex.search(r'(?if)<(CLI)><\1>',
          '<cli><clI>').span(), (0, 10))
        self.assertEqual(regex.search(r'(?ifr)<\1><(CLI)>',
          '<cli><clI>').span(), (0, 10))

        # Hg issue 195: Pickle (or otherwise serial) the compiled regex
        r = regex.compile(r'\L<options>', options=['foo', 'bar'])
        p = pickle.dumps(r)
        r = pickle.loads(p)
        self.assertEqual(r.match('foo').span(), (0, 3))

        # Hg issue 196: Fuzzy matching on repeated regex not working as
        # expected
        self.assertEqual(regex.match('(x{6}){e<=1}', 'xxxxxx',
          flags=regex.BESTMATCH).span(), (0, 6))
        self.assertEqual(regex.match('(x{6}){e<=1}', 'xxxxx',
          flags=regex.BESTMATCH).span(), (0, 5))
        self.assertEqual(regex.match('(x{6}){e<=1}', 'x',
          flags=regex.BESTMATCH), None)
        self.assertEqual(regex.match('(?r)(x{6}){e<=1}', 'xxxxxx',
          flags=regex.BESTMATCH).span(), (0, 6))
        self.assertEqual(regex.match('(?r)(x{6}){e<=1}', 'xxxxx',
          flags=regex.BESTMATCH).span(), (0, 5))
        self.assertEqual(regex.match('(?r)(x{6}){e<=1}', 'x',
          flags=regex.BESTMATCH), None)

        # Hg issue 197: ValueError in regex.compile
        self.assertRaises(regex.error, lambda:
          regex.compile('00000\\0\\00\\^\50\\00\\U05000000'))

        # Hg issue 198: ValueError in regex.compile
        self.assertRaises(regex.error, lambda: regex.compile("{e<l"))

        # Hg issue 199: Segfault in re.compile
        self.assertEqual(bool(regex.compile('((?0)){e}')), True)

        # Hg issue 200: AttributeError in regex.compile with latest regex
        self.assertEqual(bool(regex.compile('\x00?(?0){e}')), True)

        # Hg issue 201: ENHANCEMATCH crashes interpreter
        self.assertEqual(regex.findall(r'((brown)|(lazy)){1<=e<=3} ((dog)|(fox)){1<=e<=3}',
          'The quick borwn fax jumped over the lzy hog', regex.ENHANCEMATCH),
          [('borwn', 'borwn', '', 'fax', '', 'fax'), ('lzy', '', 'lzy', 'hog',
          'hog', '')])

        # Hg issue 203: partial matching bug
        self.assertEqual(regex.search(r'\d\d\d-\d\d-\d\d\d\d',
          "My SSN is 999-89-76, but don't tell.", partial=True).span(), (36,
          36))

        # Hg issue 204: confusion of (?aif) flags
        upper_i = u'\N{CYRILLIC CAPITAL LETTER SHORT I}'
        lower_i = u'\N{CYRILLIC SMALL LETTER SHORT I}'

        self.assertEqual(bool(regex.match(ur'(?ui)' + upper_i,
          lower_i)), True)
        self.assertEqual(bool(regex.match(ur'(?ui)' + lower_i,
          upper_i)), True)

        self.assertEqual(bool(regex.match(ur'(?ai)' + upper_i,
          lower_i)), False)
        self.assertEqual(bool(regex.match(ur'(?ai)' + lower_i,
          upper_i)), False)

        self.assertEqual(bool(regex.match(ur'(?afi)' + upper_i,
          lower_i)), False)
        self.assertEqual(bool(regex.match(ur'(?afi)' + lower_i,
          upper_i)), False)

        # Hg issue 205: Named list and (?ri) flags
        self.assertEqual(bool(regex.search(r'(?i)\L<aa>', '22', aa=['121',
          '22'])), True)
        self.assertEqual(bool(regex.search(r'(?ri)\L<aa>', '22', aa=['121',
          '22'])), True)
        self.assertEqual(bool(regex.search(r'(?fi)\L<aa>', '22', aa=['121',
          '22'])), True)
        self.assertEqual(bool(regex.search(r'(?fri)\L<aa>', '22', aa=['121',
          '22'])), True)

        # Hg issue 208: Named list, (?ri) flags, Backreference
        self.assertEqual(regex.search(r'(?r)\1dog..(?<=(\L<aa>))$', 'ccdogcc',
          aa=['bcb', 'cc']). span(), (0, 7))
        self.assertEqual(regex.search(r'(?ir)\1dog..(?<=(\L<aa>))$',
          'ccdogcc', aa=['bcb', 'cc']). span(), (0, 7))

        # Hg issue 210: Fuzzy matching and Backreference
        self.assertEqual(regex.search(r'(2)(?:\1{5}){e<=1}',
          '3222212').span(), (1, 7))
        self.assertEqual(regex.search(r'(\d)(?:\1{5}){e<=1}',
          '3222212').span(), (1, 7))

        # Hg issue 211: Segmentation fault with recursive matches and atomic
        # groups
        self.assertEqual(regex.match(r'''\A(?P<whole>(?>\((?&whole)\)|[+\-]))\Z''',
          '((-))').span(), (0, 5))
        self.assertEqual(regex.match(r'''\A(?P<whole>(?>\((?&whole)\)|[+\-]))\Z''',
          '((-)+)'), None)

        # Hg issue 212: Unexpected matching difference with .*? between re and
        # regex
        self.assertEqual(regex.match(r"x.*? (.).*\1(.*)\1",
          'x  |y| z|').span(), (0, 9))
        self.assertEqual(regex.match(r"\.sr (.*?) (.)(.*)\2(.*)\2(.*)",
          r'.sr  h |<nw>|<span class="locked">|').span(), (0, 35))

        # Hg issue 213: Segmentation Fault
        a = '"\\xF9\\x80\\xAEqdz\\x95L\\xA7\\x89[\\xFE \\x91)\\xF9]\\xDB\'\\x99\\x09=\\x00\\xFD\\x98\\x22\\xDD\\xF1\\xB6\\xC3 Z\\xB6gv\\xA5x\\x93P\\xE1r\\x14\\x8Cv\\x0C\\xC0w\\x15r\\xFFc%" '
        py_regex_pattern = r'''(?P<http_referer>((?>(?<!\\)(?>"(?>\\.|[^\\"]+)+"|""|(?>'(?>\\.|[^\\']+)+')|''|(?>`(?>\\.|[^\\`]+)+`)|``)))) (?P<useragent>((?>(?<!\\)(?>"(?>\\.|[^\\"]+)+"|""|(?>'(?>\\.|[^\\']+)+')|''|(?>`(?>\\.|[^\\`]+)+`)|``))))'''
        self.assertEqual(bool(regex.search(py_regex_pattern, a)), False)

        # Hg Issue 216: Invalid match when using negative lookbehind and pipe
        self.assertEqual(bool(regex.match('foo(?<=foo)', 'foo')), True)
        self.assertEqual(bool(regex.match('foo(?<!foo)', 'foo')), False)
        self.assertEqual(bool(regex.match('foo(?<=foo|x)', 'foo')), True)
        self.assertEqual(bool(regex.match('foo(?<!foo|x)', 'foo')), False)

        # Hg issue 217: Core dump in conditional ahead match and matching \!
        # character
        self.assertEqual(bool(regex.match(r'(?(?=.*\!.*)(?P<true>.*\!\w*\:.*)|(?P<false>.*))',
          '!')), False)

        # Hg issue 220: Misbehavior of group capture with OR operand
        self.assertEqual(regex.match(r'\w*(ea)\w*|\w*e(?!a)\w*',
          'easier').groups(), ('ea', ))

        # Hg issue 225: BESTMATCH in fuzzy match not working
        self.assertEqual(regex.search('(^1234$){i,d}', '12234',
          regex.BESTMATCH).span(), (0, 5))
        self.assertEqual(regex.search('(^1234$){i,d}', '12234',
          regex.BESTMATCH).fuzzy_counts, (0, 1, 0))

        self.assertEqual(regex.search('(^1234$){s,i,d}', '12234',
          regex.BESTMATCH).span(), (0, 5))
        self.assertEqual(regex.search('(^1234$){s,i,d}', '12234',
          regex.BESTMATCH).fuzzy_counts, (0, 1, 0))

        # Hg issue 226: Error matching at start of string
        self.assertEqual(regex.search('(^123$){s,i,d}', 'xxxxxxxx123',
          regex.BESTMATCH).span(), (0, 11))
        self.assertEqual(regex.search('(^123$){s,i,d}', 'xxxxxxxx123',
          regex.BESTMATCH).fuzzy_counts, (0, 8, 0))

        # Hg issue 227: Incorrect behavior for ? operator with UNICODE +
        # IGNORECASE
        self.assertEqual(regex.search(r'a?yz', 'xxxxyz', flags=regex.FULLCASE |
          regex.IGNORECASE).span(), (4, 6))

        # Hg issue 230: Is it a bug of (?(DEFINE)...)
        self.assertEqual(regex.findall(r'(?:(?![a-d]).)+', 'abcdefgh'),
          ['efgh'])
        self.assertEqual(regex.findall(r'''(?(DEFINE)(?P<mydef>(?:(?![a-d]).)))(?&mydef)+''',
          'abcdefgh'), ['efgh'])

        # Hg issue 238: Not fully re backward compatible
        self.assertEqual(regex.findall(r'((\w{1,3})(\.{2,10})){1,3}',
          '"Erm....yes. T..T...Thank you for that."'), [('Erm....', 'Erm',
          '....'), ('T...', 'T', '...')])
        self.assertEqual(regex.findall(r'((\w{1,3})(\.{2,10})){3}',
          '"Erm....yes. T..T...Thank you for that."'), [])
        self.assertEqual(regex.findall(r'((\w{1,3})(\.{2,10})){2}',
          '"Erm....yes. T..T...Thank you for that."'), [('T...', 'T', '...')])
        self.assertEqual(regex.findall(r'((\w{1,3})(\.{2,10})){1}',
          '"Erm....yes. T..T...Thank you for that."'), [('Erm....', 'Erm',
          '....'), ('T..', 'T', '..'), ('T...', 'T', '...')])

        # Hg issue 247: Unexpected result with fuzzy matching and lookahead
        # expression
        self.assertEqual(regex.search(r'(?:ESTONIA(?!\w)){e<=1}',
          'ESTONIAN WORKERS').group(), 'ESTONIAN')
        self.assertEqual(regex.search(r'(?:ESTONIA(?=\W)){e<=1}',
          'ESTONIAN WORKERS').group(), 'ESTONIAN')

        self.assertEqual(regex.search(r'(?:(?<!\w)ESTONIA){e<=1}',
          'BLUB NESTONIA').group(), 'NESTONIA')
        self.assertEqual(regex.search(r'(?:(?<=\W)ESTONIA){e<=1}',
          'BLUB NESTONIA').group(), 'NESTONIA')

        self.assertEqual(regex.search(r'(?r)(?:ESTONIA(?!\w)){e<=1}',
          'ESTONIAN WORKERS').group(), 'ESTONIAN')
        self.assertEqual(regex.search(r'(?r)(?:ESTONIA(?=\W)){e<=1}',
          'ESTONIAN WORKERS').group(), 'ESTONIAN')

        self.assertEqual(regex.search(r'(?r)(?:(?<!\w)ESTONIA){e<=1}',
          'BLUB NESTONIA').group(), 'NESTONIA')
        self.assertEqual(regex.search(r'(?r)(?:(?<=\W)ESTONIA){e<=1}',
          'BLUB NESTONIA').group(), 'NESTONIA')

        # Hg issue 248: Unexpected result with fuzzy matching and more than one
        # non-greedy quantifier
        self.assertEqual(regex.search(r'(?:A.*B.*CDE){e<=2}',
          'A B CYZ').group(), 'A B CYZ')
        self.assertEqual(regex.search(r'(?:A.*B.*?CDE){e<=2}',
          'A B CYZ').group(), 'A B CYZ')
        self.assertEqual(regex.search(r'(?:A.*?B.*CDE){e<=2}',
          'A B CYZ').group(), 'A B CYZ')
        self.assertEqual(regex.search(r'(?:A.*?B.*?CDE){e<=2}',
          'A B CYZ').group(), 'A B CYZ')

        # Hg issue 249: Add an option to regex.escape() to not escape spaces
        self.assertEqual(regex.escape(' ,0A[', special_only=False, literal_spaces=False), '\\ \\,0A\\[')
        self.assertEqual(regex.escape(' ,0A[', special_only=False, literal_spaces=True), ' \\,0A\\[')
        self.assertEqual(regex.escape(' ,0A[', special_only=True, literal_spaces=False), '\\ ,0A\\[')
        self.assertEqual(regex.escape(' ,0A[', special_only=True, literal_spaces=True), ' ,0A\\[')

        self.assertEqual(regex.escape(' ,0A['), '\\ ,0A\\[')

        # Hg issue 251: Segfault with a particular expression
        self.assertEqual(regex.search(r'(?(?=A)A|B)', 'A').span(), (0, 1))
        self.assertEqual(regex.search(r'(?(?=A)A|B)', 'B').span(), (0, 1))
        self.assertEqual(regex.search(r'(?(?=A)A|)', 'B').span(), (0, 0))
        self.assertEqual(regex.search(r'(?(?=X)X|)', '').span(), (0, 0))
        self.assertEqual(regex.search(r'(?(?=X))', '').span(), (0, 0))

        # Hg issue 252: Empty capture strings when using DEFINE group reference
        # within look-behind expression
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.))(?&func)',
          'abc').groups(), (None, ))
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.))(?&func)',
          'abc').groupdict(), {'func': None})
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.))(?&func)',
          'abc').capturesdict(), {'func': ['a']})

        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.))(?=(?&func))',
          'abc').groups(), (None, ))
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.))(?=(?&func))',
          'abc').groupdict(), {'func': None})
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.))(?=(?&func))',
          'abc').capturesdict(), {'func': ['a']})

        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.)).(?<=(?&func))',
          'abc').groups(), (None, ))
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.)).(?<=(?&func))',
          'abc').groupdict(), {'func': None})
        self.assertEqual(regex.search(r'(?(DEFINE)(?<func>.)).(?<=(?&func))',
          'abc').capturesdict(), {'func': ['a']})

        # Hg issue 271: Comment logic different between Re and Regex
        self.assertEqual(bool(regex.match(r'ab(?#comment\))cd', 'abcd')), True)

        # Hg issue 276: Partial Matches yield incorrect matches and bounds
        self.assertEqual(regex.search(r'[a-z]+ [a-z]*?:', 'foo bar',
          partial=True).span(), (0, 7))
        self.assertEqual(regex.search(r'(?r):[a-z]*? [a-z]+', 'foo bar',
          partial=True).span(), (0, 7))

        # Hg issue 291: Include Script Extensions as a supported Unicode property
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script:Beng}',
          u'\u09EF')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script:Bengali}',
          u'\u09EF')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script_Extensions:Bengali}',
          u'\u09EF')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script_Extensions:Beng}',
          u'\u09EF')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script_Extensions:Cakm}',
          u'\u09EF')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{Script_Extensions:Sylo}',
          u'\u09EF')), True)

        # Hg issue #293: scx (Script Extensions) property currently matches
        # incorrectly
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Latin}', u'P')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Ahom}', u'P')), False)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Common}', u'4')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Caucasian_Albanian}', u'4')),
          False)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Arabic}', u'\u062A')), True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Balinese}', u'\u062A')),
          False)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Devanagari}', u'\u091C')),
          True)
        self.assertEqual(bool(regex.match(ur'(?u)\p{scx:Batak}', u'\u091C')), False)

        # Hg issue 296: Group references are not taken into account when group is reporting the last match
        self.assertEqual(regex.fullmatch('(?P<x>.)*(?&x)', 'abc').captures('x'),
          ['a', 'b', 'c'])
        self.assertEqual(regex.fullmatch('(?P<x>.)*(?&x)', 'abc').group('x'),
          'b')

        self.assertEqual(regex.fullmatch('(?P<x>.)(?P<x>.)(?P<x>.)',
          'abc').captures('x'), ['a', 'b', 'c'])
        self.assertEqual(regex.fullmatch('(?P<x>.)(?P<x>.)(?P<x>.)',
          'abc').group('x'), 'c')

        # Hg issue 299: Partial gives misleading results with "open ended" regexp
        self.assertEqual(regex.match('(?:ab)*', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)*', 'abab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)*?', '', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)*+', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)*+', 'abab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)+', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)+', 'abab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)+?', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)++', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?:ab)++', 'abab', partial=True).partial,
          False)

        self.assertEqual(regex.match('(?r)(?:ab)*', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)*', 'abab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)*?', '', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)*+', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)*+', 'abab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)+', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)+', 'abab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)+?', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)++', 'ab', partial=True).partial,
          False)
        self.assertEqual(regex.match('(?r)(?:ab)++', 'abab', partial=True).partial,
          False)

        self.assertEqual(regex.match('a*', '', partial=True).partial, False)
        self.assertEqual(regex.match('a*?', '', partial=True).partial, False)
        self.assertEqual(regex.match('a*+', '', partial=True).partial, False)
        self.assertEqual(regex.match('a+', '', partial=True).partial, True)
        self.assertEqual(regex.match('a+?', '', partial=True).partial, True)
        self.assertEqual(regex.match('a++', '', partial=True).partial, True)
        self.assertEqual(regex.match('a+', 'a', partial=True).partial, False)
        self.assertEqual(regex.match('a+?', 'a', partial=True).partial, False)
        self.assertEqual(regex.match('a++', 'a', partial=True).partial, False)

        self.assertEqual(regex.match('(?r)a*', '', partial=True).partial, False)
        self.assertEqual(regex.match('(?r)a*?', '', partial=True).partial, False)
        self.assertEqual(regex.match('(?r)a*+', '', partial=True).partial, False)
        self.assertEqual(regex.match('(?r)a+', '', partial=True).partial, True)
        self.assertEqual(regex.match('(?r)a+?', '', partial=True).partial, True)
        self.assertEqual(regex.match('(?r)a++', '', partial=True).partial, True)
        self.assertEqual(regex.match('(?r)a+', 'a', partial=True).partial, False)
        self.assertEqual(regex.match('(?r)a+?', 'a', partial=True).partial, False)
        self.assertEqual(regex.match('(?r)a++', 'a', partial=True).partial, False)

        self.assertEqual(regex.match(r"(?:\s*\w+'*)+", 'whatever', partial=True).partial,
          False)

        # Hg issue 300: segmentation fault
        pattern = ('(?P<termini5>GGCGTCACACTTTGCTATGCCATAGCAT[AG]TTTATCCATAAGA'
          'TTAGCGGATCCTACCTGACGCTTTTTATCGCAACTCTCTACTGTTTCTCCATAACAGAACATATTGA'
          'CTATCCGGTATTACCCGGCATGACAGGAGTAAAA){e<=1}'
          '(?P<gene>[ACGT]{1059}){e<=2}'
          '(?P<spacer>TAATCGTCTTGTTTGATACACAAGGGTCGCATCTGCGGCCCTTTTGCTTTTTTAAG'
          'TTGTAAGGATATGCCATTCTAGA){e<=0}'
          '(?P<barcode>[ACGT]{18}){e<=0}'
          '(?P<termini3>AGATCGG[CT]AGAGCGTCGTGTAGGGAAAGAGTGTGG){e<=1}')

        text = ('GCACGGCGTCACACTTTGCTATGCCATAGCATATTTATCCATAAGATTAGCGGATCCTACC'
          'TGACGCTTTTTATCGCAACTCTCTACTGTTTCTCCATAACAGAACATATTGACTATCCGGTATTACC'
          'CGGCATGACAGGAGTAAAAATGGCTATCGACGAAAACAAACAGAAAGCGTTGGCGGCAGCACTGGGC'
          'CAGATTGAGAAACAATTTGGTAAAGGCTCCATCATGCGCCTGGGTGAAGACCGTTCCATGGATGTGG'
          'AAACCATCTCTACCGGTTCGCTTTCACTGGATATCGCGCTTGGGGCAGGTGGTCTGCCGATGGGCCG'
          'TATCGTCGAAATCTACGGACCGGAATCTTCCGGTAAAACCACGCTGACGCTGCAGGTGATCGCCGCA'
          'GCGCAGCGTGAAGGTAAAACCTGTGCGTTTATCGATGCTGAACACGCGCTGGACCCAATCTACGCAC'
          'GTAAACTGGGCGTCGATATCGACAACCTGCTGTGCTCCCAGCCGGACACCGGCGAGCAGGCACTGGA'
          'AATCTGTGACGCCCTGGCGCGTTCTGGCGCAGTAGACGTTATCGTCGTTGACTCCGTGGCGGCACTG'
          'ACGCCGAAAGCGGAAATCGAAGGCGAAATCGGCGACTCTCATATGGGCCTTGCGGCACGTATGATGA'
          'GCCAGGCGATGCGTAAGCTGGCGGGTAACCTGAAGCAGTCCAACACGCTGCTGATCTTCATCAACCC'
          'CATCCGTATGAAAATTGGTGTGATGTTCGGCAACCCGGAAACCACTTACCGGTGGTAACGCGCTGAA'
          'ATTCTACGCCTCTGTTCGTCTCGACATCCGTTAAATCGGCGCGGTGAAAGAGGGCGAAAACGTGGTG'
          'GGTAGCGAAACCCGCGTGAAAGTGGTGAAGAACAAAATCGCTGCGCCGTTTAAACAGGCTGAATTCC'
          'AGATCCTCTACGGCGAAGGTATCAACTTCTACCCCGAACTGGTTGACCTGGGCGTAAAAGAGAAGCT'
          'GATCGAGAAAGCAGGCGCGTGGTACAGCTACAAAGGTGAGAAGATCGGTCAGGGTAAAGCGAATGCG'
          'ACTGCCTGGCTGAAATTTAACCCGGAAACCGCGAAAGAGATCGAGTGAAAAGTACGTGAGTTGCTGC'
          'TGAGCAACCCGAACTCAACGCCGGATTTCTCTGTAGATGATAGCGAAGGCGTAGCAGAAACTAACGA'
          'AGATTTTTAATCGTCTTGTTTGATACACAAGGGTCGCATCTGCGGCCCTTTTGCTTTTTTAAGTTGT'
          'AAGGATATGCCATTCTAGACAGTTAACACACCAACAAAGATCGGTAGAGCGTCGTGTAGGGAAAGAG'
          'TGTGGTACC')

        m = regex.search(pattern, text, flags=regex.BESTMATCH)
        self.assertEqual(m.fuzzy_counts, (0, 1, 0))
        self.assertEqual(m.fuzzy_changes, ([], [1206], []))

        # Hg issue 306: Fuzzy match parameters not respecting quantifier scope
        self.assertEqual(regex.search(r'(?e)(dogf(((oo){e<1})|((00){e<1}))d){e<2}',
          'dogfood').fuzzy_counts, (0, 0, 0))
        self.assertEqual(regex.search(r'(?e)(dogf(((oo){e<1})|((00){e<1}))d){e<2}',
          'dogfoot').fuzzy_counts, (1, 0, 0))

        # Hg issue 312: \X not matching graphemes with zero-width-joins
        import sys
        if sys.maxunicode > 0xFFFF:
            # Works only on a wide build.
            self.assertEqual(regex.findall(ur'(?u)\X',
              u'\U0001F468\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466'),
              [u'\U0001F468\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466'])

        # Hg issue 320: Abnormal performance
        self.assertEqual(bool(regex.search(r'(?=a)a', 'a')), True)
        self.assertEqual(bool(regex.search(r'(?!b)a', 'a')), True)

        # Hg issue 327: .fullmatch() causes MemoryError
        self.assertEqual(regex.fullmatch(r'((\d)*?)*?', '123').span(), (0, 3))

        # Hg issue 329: Wrong group matches when question mark quantifier is used within a look behind
        self.assertEqual(regex.search(r'''(?(DEFINE)(?<mydef>(?<wrong>THIS_SHOULD_NOT_MATCHx?)|(?<right>right))).*(?<=(?&mydef).*)''',
          'x right').capturesdict(), {'mydef': ['right'], 'wrong': [], 'right':
          ['right']})

        # Hg issue 353: fuzzy changes negative indexes
        self.assertEqual(regex.search(r'(?be)(AGTGTTCCCCGCGCCAGCGGGGATAAACCG){s<=5,i<=5,d<=5,s+i+d<=10}',
          'TTCCCCGCGCCAGCGGGGATAAACCG').fuzzy_changes, ([], [], [0, 1, 3, 5]))

        # Git issue 364: Contradictory values in fuzzy_counts and fuzzy_changes
        self.assertEqual(regex.match(r'(?:bc){e}', 'c').fuzzy_counts, (1, 0,
          1))
        self.assertEqual(regex.match(r'(?:bc){e}', 'c').fuzzy_changes, ([0],
          [], [1]))
        self.assertEqual(regex.match(r'(?e)(?:bc){e}', 'c').fuzzy_counts, (0,
          0, 1))
        self.assertEqual(regex.match(r'(?e)(?:bc){e}', 'c').fuzzy_changes,
          ([], [], [0]))
        self.assertEqual(regex.match(r'(?b)(?:bc){e}', 'c').fuzzy_counts, (0,
          0, 1))
        self.assertEqual(regex.match(r'(?b)(?:bc){e}', 'c').fuzzy_changes,
          ([], [], [0]))

        # Git issue 370: Confusions about Fuzzy matching behavior
        self.assertEqual(regex.match('(?e)(?:^(\\$ )?\\d{1,3}(,\\d{3})*(\\.\\d{2})$){e}',
          '$ 10,112.111.12').fuzzy_counts, (6, 0, 5))
        self.assertEqual(regex.match('(?e)(?:^(\\$ )?\\d{1,3}(,\\d{3})*(\\.\\d{2})$){s<=1}',
          '$ 10,112.111.12').fuzzy_counts, (1, 0, 0))
        self.assertEqual(regex.match('(?e)(?:^(\\$ )?\\d{1,3}(,\\d{3})*(\\.\\d{2})$){s<=1,i<=1,d<=1}',
          '$ 10,112.111.12').fuzzy_counts, (1, 0, 0))
        self.assertEqual(regex.match('(?e)(?:^(\\$ )?\\d{1,3}(,\\d{3})*(\\.\\d{2})$){s<=3}',
          '$ 10,1a2.111.12').fuzzy_counts, (2, 0, 0))
        self.assertEqual(regex.match('(?e)(?:^(\\$ )?\\d{1,3}(,\\d{3})*(\\.\\d{2})$){s<=2}',
          '$ 10,1a2.111.12').fuzzy_counts, (2, 0, 0))

        self.assertEqual(regex.fullmatch(r'(?e)(?:0?,0(?:,0)?){s<=1,d<=1}',
          ',0;0').fuzzy_counts, (1, 0, 0))
        self.assertEqual(regex.fullmatch(r'(?e)(?:0??,0(?:,0)?){s<=1,d<=1}',
          ',0;0').fuzzy_counts, (1, 0, 0))

        # Git issue 371: Specifying character set when fuzzy-matching allows characters not in the set
        self.assertEqual(regex.search(r"\b(?e)(?:\d{6,20}){i<=5:[\-\\\/]}\b",
          "cat dog starting at 00:01132.000. hello world"), None)

        # Git issue 385: Comments in expressions
        self.assertEqual(bool(regex.compile('(?#)')), True)
        self.assertEqual(bool(regex.compile('(?x)(?#)')), True)

        # Git issue 394: Unexpected behaviour in fuzzy matching with limited character set with IGNORECASE flag
        self.assertEqual(regex.findall(r'(\d+){i<=2:[ab]}', '123X4Y5'),
          ['123', '4', '5'])
        self.assertEqual(regex.findall(r'(?i)(\d+){i<=2:[ab]}', '123X4Y5'),
          ['123', '4', '5'])

        # Git issue 403: Fuzzy matching with wrong distance (unnecessary substitutions)
        self.assertEqual(regex.match(r'^(test){e<=5}$', 'terstin',
          flags=regex.B).fuzzy_counts, (0, 3, 0))

        # Git issue 408: regex fails with a quantified backreference but succeeds with repeated backref
        self.assertEqual(bool(regex.match(r"(?:(x*)\1\1\1)*x$", "x" * 5)), True)
        self.assertEqual(bool(regex.match(r"(?:(x*)\1{3})*x$", "x" * 5)), True)

        # Git issue 415: Fuzzy character restrictions don't apply to insertions at "right edge"
        self.assertEqual(regex.match(r't(?:es){s<=1:\d}t', 'te5t').group(),
          'te5t')
        self.assertEqual(regex.match(r't(?:es){s<=1:\d}t', 'tezt'), None)
        self.assertEqual(regex.match(r't(?:es){i<=1:\d}t', 'tes5t').group(),
          'tes5t')
        self.assertEqual(regex.match(r't(?:es){i<=1:\d}t', 'teszt'), None)
        self.assertEqual(regex.match(r't(?:es){i<=1:\d}t',
          'tes5t').fuzzy_changes, ([], [3], []))
        self.assertEqual(regex.match(r't(es){i<=1,0<e<=1}t', 'tes5t').group(),
          'tes5t')
        self.assertEqual(regex.match(r't(?:es){i<=1,0<e<=1:\d}t',
          'tes5t').fuzzy_changes, ([], [3], []))

        # Git issue 421: Fatal Python error: Segmentation fault
        self.assertEqual(regex.compile("(\d+ week|\d+ days)").split("7 days"), ['', '7 days', ''])
        self.assertEqual(regex.compile("(\d+ week|\d+ days)").split("10 days"), ['', '10 days', ''])

        self.assertEqual(regex.compile("[ ]* Name[ ]*\* ").search("  Name *"), None)

        self.assertEqual(regex.compile('a|\\.*pb\\.py').search('.geojs'), None)

        p = regex.compile('(?<=(?:\\A|\\W|_))(\\d+ decades? ago|\\d+ minutes ago|\\d+ seconds ago|in \\d+ decades?|\\d+ months ago|in \\d+ minutes|\\d+ minute ago|in \\d+ seconds|\\d+ second ago|\\d+ years ago|in \\d+ months|\\d+ month ago|\\d+ weeks ago|\\d+ hours ago|in \\d+ minute|in \\d+ second|in \\d+ years|\\d+ year ago|in \\d+ month|in \\d+ weeks|\\d+ week ago|\\d+ days ago|in \\d+ hours|\\d+ hour ago|in \\d+ year|in \\d+ week|in \\d+ days|\\d+ day ago|in \\d+ hour|\\d+ min ago|\\d+ sec ago|\\d+ yr ago|\\d+ mo ago|\\d+ wk ago|in \\d+ day|\\d+ hr ago|in \\d+ min|in \\d+ sec|in \\d+ yr|in \\d+ mo|in \\d+ wk|in \\d+ hr)(?=(?:\\Z|\\W|_))', flags=regex.I | regex.V0)
        self.assertEqual(p.search('1 month ago').group(), '1 month ago')
        self.assertEqual(p.search('9 hours 1 minute ago').group(), '1 minute ago')
        self.assertEqual(p.search('10 months 1 hour ago').group(), '1 hour ago')
        self.assertEqual(p.search('1 month 10 hours ago').group(), '10 hours ago')

        # Git issue 427: Possible bug with BESTMATCH
        sequence = 'TTCAGACGTGTGCTCTTCCGATCTCAATACCGACTCCTCACTGTGTGTCT'
        pattern = r'(?P<insert>.*)(?P<anchor>CTTCC){e<=1}(?P<umi>([ACGT]){4,6})(?P<sid>CAATACCGACTCCTCACTGTGT){e<=2}(?P<end>([ACGT]){0,6}$)'

        m = regex.match(pattern, sequence, flags=regex.BESTMATCH)
        self.assertEqual(m.span(), (0, 50))
        self.assertEqual(m.groupdict(), {'insert': 'TTCAGACGTGTGCT', 'anchor': 'CTTCC', 'umi': 'GATCT', 'sid': 'CAATACCGACTCCTCACTGTGT', 'end': 'GTCT'})

        m = regex.match(pattern, sequence, flags=regex.ENHANCEMATCH)
        self.assertEqual(m.span(), (0, 50))
        self.assertEqual(m.groupdict(), {'insert': 'TTCAGACGTGTGCT', 'anchor': 'CTTCC', 'umi': 'GATCT', 'sid': 'CAATACCGACTCCTCACTGTGT', 'end': 'GTCT'})

        # Git issue 433: Disagreement between fuzzy_counts and fuzzy_changes
        pattern = r'(?P<insert>.*)(?P<anchor>AACACTGG){e<=1}(?P<umi>([AT][CG]){5}){e<=2}(?P<sid>GTAACCGAAG){e<=2}(?P<end>([ACGT]){0,6}$)'

        sequence = 'GGAAAACACTGGTCTCAGTCTCGTAACCGAAGTGGTCG'
        m = regex.match(pattern, sequence, flags=regex.BESTMATCH)
        self.assertEqual(m.fuzzy_counts, (0, 0, 0))
        self.assertEqual(m.fuzzy_changes, ([], [], []))

        sequence = 'GGAAAACACTGGTCTCAGTCTCGTCCCCGAAGTGGTCG'
        m = regex.match(pattern, sequence, flags=regex.BESTMATCH)
        self.assertEqual(m.fuzzy_counts, (2, 0, 0))
        self.assertEqual(m.fuzzy_changes, ([24, 25], [], []))

        # Git issue 439: Unmatched groups: sub vs subf
        self.assertEqual(regex.sub(r'(test1)|(test2)', r'matched: \1\2', 'test1'), 'matched: test1')
        self.assertEqual(regex.subf(r'(test1)|(test2)', r'matched: {1}{2}', 'test1'), 'matched: test1')
        self.assertEqual(regex.search(r'(test1)|(test2)', 'matched: test1').expand(r'matched: \1\2'), 'matched: test1'),
        self.assertEqual(regex.search(r'(test1)|(test2)', 'matched: test1').expandf(r'matched: {1}{2}'), 'matched: test1')

        # Git issue 442: Fuzzy regex matching doesn't seem to test insertions correctly
        self.assertEqual(regex.search(r"(?:\bha\b){i:[ ]}", "having"), None)
        self.assertEqual(regex.search(r"(?:\bha\b){i:[ ]}", "having", flags=regex.I), None)

    def test_fuzzy_ext(self):
        self.assertEqual(bool(regex.fullmatch(r'(?r)(?:a){e<=1:[a-z]}', 'e')),
          True)
        self.assertEqual(bool(regex.fullmatch(r'(?:a){e<=1:[a-z]}', 'e')),
          True)
        self.assertEqual(bool(regex.fullmatch(r'(?:a){e<=1:[a-z]}', '-')),
          False)
        self.assertEqual(bool(regex.fullmatch(r'(?r)(?:a){e<=1:[a-z]}', '-')),
          False)

        self.assertEqual(bool(regex.fullmatch(r'(?:a){e<=1:[a-z]}', 'ae')),
          True)
        self.assertEqual(bool(regex.fullmatch(r'(?r)(?:a){e<=1:[a-z]}',
          'ae')), True)
        self.assertEqual(bool(regex.fullmatch(r'(?:a){e<=1:[a-z]}', 'a-')),
          False)
        self.assertEqual(bool(regex.fullmatch(r'(?r)(?:a){e<=1:[a-z]}',
          'a-')), False)

        self.assertEqual(bool(regex.fullmatch(r'(?:ab){e<=1:[a-z]}', 'ae')),
           True)
        self.assertEqual(bool(regex.fullmatch(r'(?r)(?:ab){e<=1:[a-z]}',
           'ae')), True)
        self.assertEqual(bool(regex.fullmatch(r'(?:ab){e<=1:[a-z]}', 'a-')),
           False)
        self.assertEqual(bool(regex.fullmatch(r'(?r)(?:ab){e<=1:[a-z]}',
           'a-')), False)

        self.assertEqual(bool(regex.fullmatch(r'(a)\1{e<=1:[a-z]}', 'ae')),
           True)
        self.assertEqual(bool(regex.fullmatch(r'(?r)\1{e<=1:[a-z]}(a)',
           'ea')), True)
        self.assertEqual(bool(regex.fullmatch(r'(a)\1{e<=1:[a-z]}', 'a-')),
           False)
        self.assertEqual(bool(regex.fullmatch(r'(?r)\1{e<=1:[a-z]}(a)',
           '-a')), False)

        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u'ts')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u'st')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u'st')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u'ts')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u'-s')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u's-')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u's-')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(?:\N{LATIN SMALL LETTER SHARP S}){e<=1:[a-z]}',
          u'-s')), False)

        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(\N{LATIN SMALL LETTER SHARP S})\1{e<=1:[a-z]}',
           u'ssst')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(\N{LATIN SMALL LETTER SHARP S})\1{e<=1:[a-z]}',
           u'ssts')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)\1{e<=1:[a-z]}(\N{LATIN SMALL LETTER SHARP S})',
           u'stss')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)\1{e<=1:[a-z]}(\N{LATIN SMALL LETTER SHARP S})',
           u'tsss')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(\N{LATIN SMALL LETTER SHARP S})\1{e<=1:[a-z]}',
           u'ss-s')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(\N{LATIN SMALL LETTER SHARP S})\1{e<=1:[a-z]}',
           u'sss-')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(\N{LATIN SMALL LETTER SHARP S})\1{e<=1:[a-z]}',
           u'-s')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(\N{LATIN SMALL LETTER SHARP S})\1{e<=1:[a-z]}',
           u's-')), False)

        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(ss)\1{e<=1:[a-z]}',
           u'\N{LATIN SMALL LETTER SHARP S}ts')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(ss)\1{e<=1:[a-z]}',
           u'\N{LATIN SMALL LETTER SHARP S}st')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)\1{e<=1:[a-z]}(ss)',
           u'st\N{LATIN SMALL LETTER SHARP S}')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)\1{e<=1:[a-z]}(ss)',
           u'ts\N{LATIN SMALL LETTER SHARP S}')), True)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(ss)\1{e<=1:[a-z]}',
           u'\N{LATIN SMALL LETTER SHARP S}-s')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?fiu)(ss)\1{e<=1:[a-z]}',
           u'\N{LATIN SMALL LETTER SHARP S}s-')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(ss)\1{e<=1:[a-z]}',
           u's-\N{LATIN SMALL LETTER SHARP S}')), False)
        self.assertEqual(bool(regex.fullmatch(ur'(?firu)(ss)\1{e<=1:[a-z]}',
           u'-s\N{LATIN SMALL LETTER SHARP S}')), False)

    def test_subscripted_captures(self):
        self.assertEqual(regex.match(r'(?P<x>.)+',
          'abc').expandf('{0} {0[0]} {0[-1]}'), 'abc abc abc')
        self.assertEqual(regex.match(r'(?P<x>.)+',
          'abc').expandf('{1} {1[0]} {1[1]} {1[2]} {1[-1]} {1[-2]} {1[-3]}'),
          'c a b c c b a')
        self.assertEqual(regex.match(r'(?P<x>.)+',
          'abc').expandf('{x} {x[0]} {x[1]} {x[2]} {x[-1]} {x[-2]} {x[-3]}'),
          'c a b c c b a')

        self.assertEqual(regex.subf(r'(?P<x>.)+', r'{0} {0[0]} {0[-1]}',
          'abc'), 'abc abc abc')
        self.assertEqual(regex.subf(r'(?P<x>.)+',
          '{1} {1[0]} {1[1]} {1[2]} {1[-1]} {1[-2]} {1[-3]}', 'abc'),
          'c a b c c b a')
        self.assertEqual(regex.subf(r'(?P<x>.)+',
          '{x} {x[0]} {x[1]} {x[2]} {x[-1]} {x[-2]} {x[-3]}', 'abc'),
          'c a b c c b a')

if not hasattr(str, "format"):
    # Strings don't have the .format method (below Python 2.6).
    del RegexTests.test_format
    del RegexTests.test_subscripted_captures

# def test_main():
#     run_unittest(RegexTests)
#
# if __name__ == "__main__":
#     test_main()
