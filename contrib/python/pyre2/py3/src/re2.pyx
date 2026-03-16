# cython: infer_types(False)
r"""Regular expressions using Google's RE2 engine.

Compared to Python's ``re``, the RE2 engine compiles regular expressions to
deterministic finite automata, which guarantees linear-time behavior.

Intended as a drop-in replacement for ``re``. Unicode is supported by encoding
to UTF-8, and bytes strings are treated as UTF-8 when the UNICODE flag is given.
For best performance, work with UTF-8 encoded bytes strings.

Regular expressions that are not compatible with RE2 are processed with a
fallback module (default is ``re``). Examples of features not supported by RE2:

    - lookahead assertions ``(?!...)``
    - backreferences (``\\n`` in search pattern)
    - \W and \S not supported inside character classes

On the other hand, unicode character classes are supported (e.g., ``\p{Greek}``).
Syntax reference: https://github.com/google/re2/wiki/Syntax

What follows is a reference for the regular expression syntax supported by this
module (i.e., without requiring fallback to `re` or compatible module).

Regular expressions can contain both special and ordinary characters.
Most ordinary characters, like "A", "a", or "0", are the simplest
regular expressions; they simply match themselves.

The special characters are::

    "."      Matches any character except a newline.
    "^"      Matches the start of the string.
    "$"      Matches the end of the string or just before the newline at
             the end of the string.
    "*"      Matches 0 or more (greedy) repetitions of the preceding RE.
             Greedy means that it will match as many repetitions as possible.
    "+"      Matches 1 or more (greedy) repetitions of the preceding RE.
    "?"      Matches 0 or 1 (greedy) of the preceding RE.
    *?,+?,?? Non-greedy versions of the previous three special characters.
    {m,n}    Matches from m to n repetitions of the preceding RE.
    {m,n}?   Non-greedy version of the above.
    "\\"     Either escapes special characters or signals a special sequence.
    []       Indicates a set of characters.
             A "^" as the first character indicates a complementing set.
    "|"      A|B, creates an RE that will match either A or B.
    (...)    Matches the RE inside the parentheses.
             The contents can be retrieved or matched later in the string.
    (?:...)  Non-grouping version of regular parentheses.
    (?imsux) Set the I, M, S, U, or X flag for the RE (see below).

The special sequences consist of "\\" and a character from the list
below.  If the ordinary character is not on the list, then the
resulting RE will match the second character::

    \A         Matches only at the start of the string.
    \Z         Matches only at the end of the string.
    \b         Matches the empty string, but only at the start or end of a word.
    \B         Matches the empty string, but not at the start or end of a word.
    \d         Matches any decimal digit.
    \D         Matches any non-digit character.
    \s         Matches any whitespace character.
    \S         Matches any non-whitespace character.
    \w         Matches any alphanumeric character.
    \W         Matches the complement of \w.
    \\         Matches a literal backslash.
    \pN        Unicode character class (one-letter name)
    \p{Greek}  Unicode character class
    \PN        negated Unicode character class (one-letter name)
    \P{Greek}  negated Unicode character class

This module exports the following functions::

    count     Count all occurrences of a pattern in a string.
    match     Match a regular expression pattern to the beginning of a string.
    fullmatch Match a regular expression pattern to all of a string.
    search    Search a string for a pattern and return Match object.
    contains  Same as search, but only return bool.
    sub       Substitute occurrences of a pattern found in a string.
    subn      Same as sub, but also return the number of substitutions made.
    split     Split a string by the occurrences of a pattern.
    findall   Find all occurrences of a pattern in a string.
    finditer  Return an iterator yielding a match object for each match.
    compile   Compile a pattern into a RegexObject.
    purge     Clear the regular expression cache.
    escape    Backslash all non-alphanumerics in a string.

Some of the functions in this module takes flags as optional parameters::

    A  ASCII       Make \w, \W, \b, \B, \d, \D match the corresponding ASCII
                   character categories (rather than the whole Unicode
                   categories, which is the default).
    I  IGNORECASE  Perform case-insensitive matching.
    M  MULTILINE   "^" matches the beginning of lines (after a newline)
                   as well as the string.
                   "$" matches the end of lines (before a newline) as well
                   as the end of the string.
    S  DOTALL      "." matches any character at all, including the newline.
    X  VERBOSE     Ignore whitespace and comments for nicer looking RE's.
    U  UNICODE     Enable Unicode character classes and make \w, \W, \b, \B,
                   Unicode-aware (default for unicode patterns).

This module also defines an exception 'RegexError' (also available under the
alias 'error').

"""

include "includes.pxi"

import re
import sys
import types
import warnings
from re import error as RegexError

error = re.error

# Import re flags to be compatible.
I, M, S, U, X, L = re.I, re.M, re.S, re.U, re.X, re.L
IGNORECASE = re.IGNORECASE
MULTILINE = re.MULTILINE
DOTALL = re.DOTALL
UNICODE = re.UNICODE
VERBOSE = re.VERBOSE
LOCALE = re.LOCALE
DEBUG = re.DEBUG
NOFLAG = 0  # Python 3.11
ASCII = 256  # Python 3

if sys.version_info[:2] >= (3, 11):
    import enum

    @enum.global_enum
    @enum._simple_enum(enum.IntFlag, boundary=enum.KEEP)
    class RegexFlag:
        NOFLAG = 0
        ASCII = A = re.ASCII # assume ascii "locale"
        IGNORECASE = I = re.IGNORECASE # ignore case
        LOCALE = L = re.LOCALE # assume current 8-bit locale
        UNICODE = U = re.UNICODE # assume unicode "locale"
        MULTILINE = M = re.MULTILINE # make anchors look for newline
        DOTALL = S = re.DOTALL # make dot match newline
        VERBOSE = X = re.VERBOSE # ignore whitespace and comments
        DEBUG = re.DEBUG # dump pattern after compilation
        __str__ = object.__str__
        _numeric_repr_ = hex

FALLBACK_QUIETLY = 0
FALLBACK_WARNING = 1
FALLBACK_EXCEPTION = 2

VERSION = (0, 2, 23)
VERSION_HEX = 0x000217

cdef int _I = I, _M = M, _S = S, _U = U, _X = X, _L = L
cdef object fallback_module = re
cdef int current_notification = FALLBACK_QUIETLY

# Type of compiled re object from Python stdlib
SREPattern = type(re.compile(''))

_cache = {}
_cache_repl = {}

_MAXCACHE = 100


include "compile.pxi"
include "pattern.pxi"
include "match.pxi"


def purge():
    """Clear the regular expression caches."""
    _cache.clear()
    _cache_repl.clear()


def search(pattern, string, int flags=0):
    """Scan through string looking for a match to the pattern, returning
    a ``Match`` object or none if no match was found."""
    return compile(pattern, flags).search(string)


def match(pattern, string, int flags=0):
    """Try to apply the pattern at the start of the string, returning
    a ``Match`` object, or ``None`` if no match was found."""
    return compile(pattern, flags).match(string)


def fullmatch(pattern, string, int flags=0):
    """Try to apply the pattern to the entire string, returning
    a ``Match`` object, or ``None`` if no match was found."""
    return compile(pattern, flags).fullmatch(string)


def contains(pattern, string, int flags=0):
    """Scan through string looking for a match to the pattern, returning
    True or False."""
    return compile(pattern, flags).contains(string)


def finditer(pattern, string, int flags=0):
    """Yield all non-overlapping matches in the string.

    For each match, the iterator returns a ``Match`` object.
    Empty matches are included in the result."""
    return compile(pattern, flags).finditer(string)


def findall(pattern, string, int flags=0):
    """Return a list of all non-overlapping matches in the string.

    Each match is represented as a string or a tuple (when there are two ore
    more groups). Empty matches are included in the result."""
    return compile(pattern, flags).findall(string)


def count(pattern, string, int flags=0):
    """Return number of non-overlapping matches in the string.

    Empty matches are included in the count."""
    return compile(pattern, flags).count(string)


def split(pattern, string, int maxsplit=0, int flags=0):
    """Split the source string by the occurrences of the pattern,
    returning a list containing the resulting substrings."""
    return compile(pattern, flags).split(string, maxsplit)


def sub(pattern, repl, string, int count=0, int flags=0):
    """Return the string obtained by replacing the leftmost
    non-overlapping occurrences of the pattern in string by the
    replacement ``repl``. ``repl`` can be either a string or a callable;
    if a string, backslash escapes in it are processed. If it is
    a callable, it's passed the ``Match`` object and must return
    a replacement string to be used."""
    return compile(pattern, flags).sub(repl, string, count)


def subn(pattern, repl, string, int count=0, int flags=0):
    """Return a 2-tuple containing ``(new_string, number)``.
    new_string is the string obtained by replacing the leftmost
    non-overlapping occurrences of the pattern in the source
    string by the replacement ``repl``. ``number`` is the number of
    substitutions that were made. ``repl`` can be either a string or a
    callable; if a string, backslash escapes in it are processed.
    If it is a callable, it's passed the ``Match`` object and must
    return a replacement string to be used."""
    return compile(pattern, flags).subn(repl, string, count)


def escape(pattern):
    """Escape all non-alphanumeric characters in pattern."""
    cdef bint uni = isinstance(pattern, unicode)
    cdef list s
    if uni:
        s = list(pattern)
    else:
        s = [bytes([c]) for c in pattern]
    for i in range(len(pattern)):
        # c = pattern[i]
        c = s[i]
        if ord(c) < 0x80 and not c.isalnum():
            if uni:
                if c == u'\000':
                    s[i] = u'\\000'
                else:
                    s[i] = u"\\" + c
            else:
                if c == b'\000':
                    s[i] = b'\\000'
                else:
                    s[i] = b'\\' + c
    return u''.join(s) if uni else b''.join(s)


class BackreferencesException(Exception):
    """Search pattern contains backreferences."""
    pass


class CharClassProblemException(Exception):
    """Search pattern contains unsupported character class."""
    pass


def set_fallback_module(module):
    """Set the fallback module; defaults to ``re`` and must be
    ``re``-compatible."""
    global fallback_module
    if not isinstance(module, types.ModuleType):
        raise TypeError("fallback is not a module")
    if not hasattr(module, "compile") or not callable(getattr(module, "compile")):
        raise ValueError("fallback module does not contain compile method")
    if module != fallback_module:
        purge()  # cache may contain items from a different fallback module
    fallback_module = module


def set_fallback_notification(level):
    """Set the fallback notification to a level; one of:
        FALLBACK_QUIETLY
        FALLBACK_WARNING
        FALLBACK_EXCEPTION
    """
    global current_notification
    level = int(level)
    if level < 0 or level > 2:
        raise ValueError("This function expects a valid notification level.")
    current_notification = level


cdef bint ishex(unsigned char c):
    """Test whether ``c`` is in ``[0-9a-fA-F]``"""
    return (b'0' <= c <= b'9' or b'a' <= c <= b'f' or b'A' <= c <= b'F')


cdef bint isoct(unsigned char c):
    """Test whether ``c`` is in ``[0-7]``"""
    return b'0' <= c <= b'7'


cdef bint isdigit(unsigned char c):
    """Test whether ``c`` is in ``[0-9]``"""
    return b'0' <= c <= b'9'


cdef bint isident(unsigned char c):
    """Test whether ``c`` is in ``[a-zA-Z0-9_]``"""
    return (b'a' <= c <= b'z' or b'A' <= c <= b'Z'
        or b'0' <= c <= b'9' or c == b'_')


cdef inline bytes cpp_to_bytes(cpp_string input):
    """Convert from a std::string object to a python string."""
    # By taking the slice we go to the right size,
    # despite spurious or missing null characters.
    return input.data()[:input.length()]


cdef inline unicode cpp_to_unicode(cpp_string input):
    """Convert a std::string object to a unicode string."""
    return cpython.unicode.PyUnicode_DecodeUTF8(
            input.data(), input.length(), 'strict')


cdef inline unicode char_to_unicode(const char * input, int length):
    """Convert a C string to a unicode string."""
    return cpython.unicode.PyUnicode_DecodeUTF8(input, length, 'strict')


cdef inline unicode_to_bytes(object pystring, int * encoded,
        int checkotherencoding):
    """Convert a unicode string to a utf8 bytes object, if necessary.

    If pystring is a bytes string or a buffer, return unchanged.
    If checkotherencoding is 0 or 1 and using Python 3, raise an error
    if its truth value is not equal to that of encoded.
    encoded is set to 1 if encoded string can be treated as ASCII,
    and 2 if it contains multibyte unicode characters."""
    if cpython.unicode.PyUnicode_Check(pystring):
        origlen = len(pystring)
        pystring = pystring.encode('utf8')
        encoded[0] = 1 if origlen == len(pystring) else 2
    else:
        encoded[0] = 0
    if checkotherencoding > 0 and not encoded[0]:
        raise TypeError("can't use a string pattern on a bytes-like object")
    elif checkotherencoding == 0 and encoded[0]:
        raise TypeError("can't use a bytes pattern on a string-like object")
    return pystring


cdef inline int pystring_to_cstring(
        object pystring, char ** cstring, Py_ssize_t * size,
        Py_buffer * buf):
    """Get a pointer from bytes/buffer object ``pystring``.

    On success, return 0, and set ``cstring``, ``size``, and ``buf``."""
    cdef int result = -1
    cstring[0] = NULL
    size[0] = 0
    if PyObject_CheckBuffer(pystring) == 1:  # new-style Buffer interface
        result = PyObject_GetBuffer(pystring, buf, PyBUF_SIMPLE)
        if result == 0:
            cstring[0] = <char *>buf.buf
            size[0] = buf.len
    return result


cdef inline void release_cstring(Py_buffer *buf):
    """Release buffer if necessary."""
    PyBuffer_Release(buf)


cdef utf8indices(char * cstring, int size, int *pos, int *endpos):
    """Convert unicode indices ``pos`` and ``endpos`` to UTF-8 indices.

    If the indices are out of range, leave them unchanged."""
    cdef unsigned char * data = <unsigned char *>cstring
    cdef int newpos = pos[0], newendpos = -1
    cdef int cpos = 0, upos = 0
    while cpos < size:
        if data[cpos] < 0x80:
            cpos += 1
            upos += 1
        elif data[cpos] < 0xe0:
            cpos += 2
            upos += 1
        elif data[cpos] < 0xf0:
            cpos += 3
            upos += 1
        else:
            cpos += 4
            upos += 1
            # wide unicode chars get 2 unichars when Python <3.3 is compiled
            # with --enable-unicode=ucs2
            emit_if_narrow_unicode()
            upos += 1
            emit_endif()

        if upos == pos[0]:
            newpos = cpos
            if endpos[0] == -1:
                break
        elif upos == endpos[0]:
            newendpos = cpos
            break
    pos[0] = newpos
    endpos[0] = newendpos


cdef void unicodeindices(map[int, int] &positions,
        char * cstring, int size, int * cpos, int * upos):
    """Convert UTF-8 byte indices to unicode indices."""
    cdef unsigned char * s = <unsigned char *>cstring
    cdef map[int, int].iterator it = positions.begin()

    if dereference(it).first == -1:
        dereference(it).second = -1
        postincrement(it)
        if it == positions.end():
            return
    if dereference(it).first == cpos[0]:
        dereference(it).second = upos[0]
        postincrement(it)
        if it == positions.end():
            return

    while cpos[0] < size:
        if s[cpos[0]] < 0x80:
            cpos[0] += 1
            upos[0] += 1
        elif s[cpos[0]] < 0xe0:
            cpos[0] += 2
            upos[0] += 1
        elif s[cpos[0]] < 0xf0:
            cpos[0] += 3
            upos[0] += 1
        else:
            cpos[0] += 4
            upos[0] += 1
            # wide unicode chars get 2 unichars when Python <3.3 is compiled
            # with --enable-unicode=ucs2
            emit_if_narrow_unicode()
            upos[0] += 1
            emit_endif()

        if dereference(it).first == cpos[0]:
            dereference(it).second = upos[0]
            postincrement(it)
            if it == positions.end():
                break


__all__ = [
        # exceptions
        'BackreferencesException', 'CharClassProblemException',
        'RegexError', 'error',
        # constants
        'FALLBACK_EXCEPTION', 'FALLBACK_QUIETLY', 'FALLBACK_WARNING', 'DEBUG',
        'S', 'DOTALL', 'I', 'IGNORECASE', 'L', 'LOCALE', 'M', 'MULTILINE',
        'U', 'UNICODE', 'X', 'VERBOSE', 'VERSION', 'VERSION_HEX',
        'NOFLAG', 'RegexFlag',
        # classes
        'Match', 'Pattern', 'SREPattern',
        # functions
        'compile', 'count', 'escape', 'findall', 'finditer', 'fullmatch',
        'match', 'purge', 'search', 'split', 'sub', 'subn',
        'set_fallback_notification',
        ]
