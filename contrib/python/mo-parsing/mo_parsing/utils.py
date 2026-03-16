# encoding: utf-8
from __future__ import absolute_import, division, unicode_literals

import inspect
import json
import re
import string
import sys
import warnings
from collections import namedtuple
from math import isnan
from types import FunctionType

from mo_dots import is_null, Null, is_many
from mo_future import unichr, text, generator_types, get_function_name
from mo_imports import expect

ParseResults, ParseException, Many = expect("ParseResults", "ParseException", "Many")


def append_config(base, *slots):
    dups = set(slots) & set(base.Config._fields)
    if dups:
        Log.error("Duplicate config fields: {{dups}}", dups=dups)

    fields = base.Config._fields + slots
    return namedtuple("Config", fields)


try:
    from mo_logs import Log, Except
except Exception:

    class Log(object):
        @classmethod
        def note(cls, template, cause=None, **params):
            pass

        @classmethod
        def warning(cls, template, cause=None, **params):
            print(f"WARNING: {template}")

        @classmethod
        def alert(cls, template, cause=None, **params):
            print()

        @classmethod
        def error(cls, template, cause=None, **params):
            raise ParseException(Null, -1, "", msg=template, cause=cause)


MAX_INT = sys.maxsize
empty_list = []
empty_tuple = tuple()
_prec = {"|": 0, "+": 1, "*": 2}


def regex_compile(pattern):
    """REGEX COMPILE WITHOUT THE ON-A-SINGLE-LINE ASSUMPTION"""
    try:
        return re.compile(pattern, re.DOTALL)
    except Exception as cause:
        Log.error("could not compile {{pattern}}", pattern=pattern, cause=cause)


regex_type = type(regex_compile("[A-Z]"))


def regex_iso(curr_prec, expr, new_prec):
    """
    RETURN NON-CAPTURING GROUP (TO ENSURE ORDER OF OPERATIONS)
    """
    if _prec[curr_prec] < _prec[new_prec]:
        return f"(?:{expr})"
    else:
        return expr


def regex_caseless(literal):
    """
    RETURN REGEX FOR CASELESS VERSION OF GIVEN LITERAL (SO WE DO NOT NEED CASELESS MODE)
    """
    lower = literal.lower()
    upper = literal.upper()
    return "".join(
        f"[{re.escape(l)}{re.escape(u)}]" if l != u else re.escape(u)
        for l, u in zip(lower, upper)
    )


_escapes = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}
_escapes.update({c: "\\" + c for c in r".^$*?+-{}[]\|()"})


def regex_range(s, exclude=False):
    def esc(s):
        return _escapes.get(s, s)

    if not s:
        return ""
    if len(s) == 1:
        if exclude:
            return f"[^{esc(s)}]"
        else:
            return esc(s)

    start = None
    prev = None
    acc = ["[^"] if exclude else ["["]
    for c in list(sorted(set(s))) + ["\a"]:
        if prev and ord(prev) == ord(c) - 1:
            if not start:
                start = prev
        elif start:
            if start == prev:
                acc.append(esc(prev))
            else:
                acc.append(esc(start))
                acc.append("-")
                acc.append(esc(prev))
            start = None
        elif prev:
            acc.append(esc(prev))
        prev = c
    acc.append("]")

    return "".join(acc)


def indent(value, prefix="\t", indent=None):
    """
    indent given string, using prefix * indent as prefix for each line
    :param value:
    :param prefix:
    :param indent:
    :return:
    """
    if indent != None:
        prefix = prefix * indent

    value = text(value)
    try:
        content = value.rstrip()
        suffix = value[len(content) :]
        lines = content.splitlines()
        return prefix + ("\n" + prefix).join(lines) + suffix
    except Exception as e:
        raise Exception(
            "Problem with indent of value (" + e.message + ")\n" + text(value)
        ) from None


def quote(value):
    """
    return JSON-quoted value
    :param value:
    :return:
    """
    if is_null(value):
        output = ""
    else:
        output = json.dumps(value)
    return output


def is_number(s):
    if s is True or s is False or is_null(s):
        return False

    try:
        s = float(s)
        return not isnan(s)
    except Exception:
        return False


def enlist(value):
    """
    PERFORMS THE FOLLOWING TRANSLATION
    None -> []
    value -> [value]
    [...] -> [...]  (unchanged list)
    """
    if is_null(value):
        return []
    elif is_many(value):
        return value
    else:
        return [value]
listwrap = enlist


def coalesce(*args):
    # pick the first not null value
    # http://en.wikipedia.org/wiki/Null_coalescing_operator
    for a in args:
        if a != None:
            return a
    return None


# build list of single arg builtins, that can be used as parse actions
singleArgBuiltins = [
    sum,
    len,
    sorted,
    reversed,
    list,
    tuple,
    set,
    any,
    all,
    min,
    max,
]

singleArgTypes = [
    int,
    float,
    str,
    bool,
    complex,
    dict,
]

builtin_lookup = {"".join.__name__: ("iterable",)}


def is_forward(expr):
    return expr.__class__.__name__ == "Forward"


def is_backtracking(expr):
    """
    RETURN true IF THIS CAN BE EXPENSIVE BACKTRACKER
    """
    return (
        isinstance(expr, Many)
        and expr.parser_config.max_match - expr.parser_config.min_match > 3
    )


def stack_depth():
    count = 0
    f = sys._getframe()
    while f:
        f = f.f_back
        count += 1
    return count


def get_function_arguments(func):
    try:
        return func.__code__.co_varnames[: func.__code__.co_argcount]
    except Exception as e:
        return builtin_lookup.get(func.__name__, ("unknown",))


alphas = string.ascii_uppercase + string.ascii_lowercase
nums = "0123456789"
hexnums = nums + "ABCDEFabcdef"
alphanums = alphas + nums
printables = "".join(c for c in string.printable if c not in string.whitespace)


def col(loc, string):
    """Returns current column within a string, counting newlines as line separators.
    The first column is number 1.

    Note: the default parsing behavior is to expand tabs in the input string
    before starting the parsing process.  See
    `ParserElement.parse_string` for more
    information on parsing strings containing ``<TAB>`` s, and suggested
    methods to maintain a consistent view of the parsed string, the parse
    location, and line and column positions within the parsed string.
    """
    s = string
    return 1 if 0 < loc < len(s) and s[loc - 1] == "\n" else loc - s.rfind("\n", 0, loc)


def lineno(loc, string):
    """Returns current line number within a string, counting newlines as line separators.
    The first line is number 1.

    Note - the default parsing behavior is to expand tabs in the input string
    before starting the parsing process.  See `ParserElement.parse_string`
    for more information on parsing strings containing ``<TAB>`` s, and
    suggested methods to maintain a consistent view of the parsed string, the
    parse location, and line and column positions within the parsed string.
    """
    return string.count("\n", 0, loc) + 1


def line(loc, string):
    """Returns the line of text containing loc within a string, counting newlines as line separators."""
    lastCR = string.rfind("\n", 0, loc)
    nextCR = string.find("\n", loc)
    if nextCR >= 0:
        return string[lastCR + 1 : nextCR]
    else:
        return string[lastCR + 1 :]


"decorator to trim function calls to match the arity of the target"


def wrap_parse_action(func):
    if func in singleArgBuiltins:
        spec = inspect.getfullargspec(func)
    elif func.__class__.__name__ == "staticmethod":
        func = func.__func__
        spec = inspect.getfullargspec(func)
    elif func.__class__.__name__ == "builtin_function_or_method":
        spec = inspect.getfullargspec(func)
    elif func in singleArgTypes:
        spec = inspect.FullArgSpec(["value"], None, None, None, [], None, {})
    elif isinstance(func, type):
        spec = inspect.getfullargspec(func.__init__)
        func = func.__call__
    elif isinstance(func, FunctionType):
        spec = inspect.getfullargspec(func)
    elif hasattr(func, "__call__"):
        spec = inspect.getfullargspec(func)

    if spec.varargs:
        num_args = 3
    elif spec.args and spec.args[0] in ["cls", "self"]:
        num_args = len(spec.args) - 1
    else:
        num_args = len(spec.args)

    def wrapper(token, index, string):
        try:
            args = token, index, string
            result = func(*args[:num_args])
            if result is None:
                return token
            elif isinstance(result, ParseResults):
                if (result.start < token.start) or (token.end < result.end):
                    Log.error("Tokens must be ordered")
                result.failures.extend(token.failures)
                return result

            if isinstance(result, (list, tuple)):
                return ParseResults(
                    token.type, token.start, token.end, result, token.failures
                )
            else:
                return ParseResults(
                    token.type, token.start, token.end, [result], token.failures
                )
        except ParseException as pe:
            raise
        except Exception as cause:
            Log.error("parse action {{name}} should not raise exception", name=func_name, cause=cause)

    # copy func name to wrapper for sensible debug output
    try:
        func_name = getattr(func, "__name__", getattr(func, "__class__").__name__)
    except Exception:
        func_name = str(func)
    wrapper.__name__ = func_name

    return wrapper


def _xml_escape(data):
    """Escape &, <, >, ", ', etc. in a string of data."""

    # ampersand must be replaced first
    from_symbols = "&><\"'"
    to_symbols = ("&" + s + ";" for s in "amp gt lt quot apos".split())
    for from_, to_ in zip(from_symbols, to_symbols):
        data = data.replace(from_, to_)
    return data


class _lazyclassproperty(object):
    def __init__(self, fn):
        self.fn = fn
        self.__doc__ = fn.__doc__
        self.__name__ = fn.__name__

    def __get__(self, obj, cls):
        if cls is None:
            cls = type(obj)
        if not hasattr(cls, "_intern") or any(
            cls._intern is getattr(superclass, "_intern", [])
            for superclass in cls.__mro__[1:]
        ):
            cls._intern = {}
        attrname = self.fn.__name__
        if attrname not in cls._intern:
            cls._intern[attrname] = self.fn(cls)
        return cls._intern[attrname]


class unicode_set(object):
    """
    A set of Unicode characters, for language-specific strings for
    ``alphas``, ``nums``, ``alphanums``, and ``printables``.
    A unicode_set is defined by a list of ranges in the Unicode character
    set, in a class attribute ``_ranges``, such as::

        _ranges = [(0x0020, 0x007e), (0x00a0, 0x00ff),]

    A unicode set can also be defined using multiple inheritance of other unicode sets::

        class CJK(Chinese, Japanese, Korean):
            pass
    """

    _ranges = []

    @classmethod
    def _get_chars_for_ranges(cls):
        ret = []
        for cc in cls.__mro__:
            if cc is unicode_set:
                break
            for rr in cc._ranges:
                ret.extend(range(rr[0], rr[-1] + 1))
        return [unichr(c) for c in sorted(set(ret))]

    @_lazyclassproperty
    def printables(cls):
        "all non-whitespace characters in this range"
        return "".join(sorted(filter(
            lambda c: not c.isspace(), cls._get_chars_for_ranges()
        )))

    @_lazyclassproperty
    def alphas(cls):
        "all alphabetic characters in this range"
        return "".join(filter(text.isalpha, cls._get_chars_for_ranges()))

    @_lazyclassproperty
    def nums(cls):
        "all numeric digit characters in this range"
        return "".join(filter(text.isdigit, cls._get_chars_for_ranges()))

    @_lazyclassproperty
    def alphanums(cls):
        "all alphanumeric characters in this range"
        return cls.alphas + cls.nums


class parsing_unicode(unicode_set):
    """
    A namespace class for defining common language unicode_sets.
    """

    _ranges = [(32, sys.maxunicode)]

    class Latin1(unicode_set):
        "Unicode set for Latin-1 Unicode Character Range"
        _ranges = [
            (0x0020, 0x007E),
            (0x00A0, 0x00FF),
        ]

    class LatinA(unicode_set):
        "Unicode set for Latin-A Unicode Character Range"
        _ranges = [
            (0x0100, 0x017F),
        ]

    class LatinB(unicode_set):
        "Unicode set for Latin-B Unicode Character Range"
        _ranges = [
            (0x0180, 0x024F),
        ]

    class Greek(unicode_set):
        "Unicode set for Greek Unicode Character Ranges"
        _ranges = [
            (0x0370, 0x03FF),
            (0x1F00, 0x1F15),
            (0x1F18, 0x1F1D),
            (0x1F20, 0x1F45),
            (0x1F48, 0x1F4D),
            (0x1F50, 0x1F57),
            (0x1F59,),
            (0x1F5B,),
            (0x1F5D,),
            (0x1F5F, 0x1F7D),
            (0x1F80, 0x1FB4),
            (0x1FB6, 0x1FC4),
            (0x1FC6, 0x1FD3),
            (0x1FD6, 0x1FDB),
            (0x1FDD, 0x1FEF),
            (0x1FF2, 0x1FF4),
            (0x1FF6, 0x1FFE),
        ]

    class Cyrillic(unicode_set):
        "Unicode set for Cyrillic Unicode Character Range"
        _ranges = [(0x0400, 0x04FF)]

    class Chinese(unicode_set):
        "Unicode set for Chinese Unicode Character Range"
        _ranges = [
            (0x4E00, 0x9FFF),
            (0x3000, 0x303F),
        ]

    class Japanese(unicode_set):
        "Unicode set for Japanese Unicode Character Range, combining Kanji, Hiragana, and Katakana ranges"
        _ranges = []

        class Kanji(unicode_set):
            "Unicode set for Kanji Unicode Character Range"
            _ranges = [
                (0x4E00, 0x9FBF),
                (0x3000, 0x303F),
            ]

        class Hiragana(unicode_set):
            "Unicode set for Hiragana Unicode Character Range"
            _ranges = [
                (0x3040, 0x309F),
            ]

        class Katakana(unicode_set):
            "Unicode set for Katakana  Unicode Character Range"
            _ranges = [
                (0x30A0, 0x30FF),
            ]

    class Korean(unicode_set):
        "Unicode set for Korean Unicode Character Range"
        _ranges = [
            (0xAC00, 0xD7AF),
            (0x1100, 0x11FF),
            (0x3130, 0x318F),
            (0xA960, 0xA97F),
            (0xD7B0, 0xD7FF),
            (0x3000, 0x303F),
        ]

    class CJK(Chinese, Japanese, Korean):
        "Unicode set for combined Chinese, Japanese, and Korean (CJK) Unicode Character Range"
        pass

    class Thai(unicode_set):
        "Unicode set for Thai Unicode Character Range"
        _ranges = [
            (0x0E01, 0x0E3A),
            (0x0E3F, 0x0E5B),
        ]

    class Arabic(unicode_set):
        "Unicode set for Arabic Unicode Character Range"
        _ranges = [
            (0x0600, 0x061B),
            (0x061E, 0x06FF),
            (0x0700, 0x077F),
        ]

    class Hebrew(unicode_set):
        "Unicode set for Hebrew Unicode Character Range"
        _ranges = [
            (0x0590, 0x05FF),
        ]

    class Devanagari(unicode_set):
        "Unicode set for Devanagari Unicode Character Range"
        _ranges = [(0x0900, 0x097F), (0xA8E0, 0xA8FF)]


parsing_unicode.Japanese._ranges = (
    parsing_unicode.Japanese.Kanji._ranges
    + parsing_unicode.Japanese.Hiragana._ranges
    + parsing_unicode.Japanese.Katakana._ranges
)

# define ranges in language character sets
setattr(parsing_unicode, "العربية", parsing_unicode.Arabic)
setattr(parsing_unicode, "中文", parsing_unicode.Chinese)
setattr(parsing_unicode, "кириллица", parsing_unicode.Cyrillic)
setattr(parsing_unicode, "Ελληνικά", parsing_unicode.Greek)
setattr(parsing_unicode, "עִברִית", parsing_unicode.Hebrew)
setattr(parsing_unicode, "日本語", parsing_unicode.Japanese)
setattr(parsing_unicode.Japanese, "漢字", parsing_unicode.Japanese.Kanji)
setattr(parsing_unicode.Japanese, "カタカナ", parsing_unicode.Japanese.Katakana)
setattr(parsing_unicode.Japanese, "ひらがな", parsing_unicode.Japanese.Hiragana)
setattr(parsing_unicode, "한국어", parsing_unicode.Korean)
setattr(parsing_unicode, "ไทย", parsing_unicode.Thai)
setattr(parsing_unicode, "देवनागरी", parsing_unicode.Devanagari)
