
import re
import sys

try:
    from itertools import tee, filterfalse
except ImportError:
    # Accommodate prior name for filterfalse in Python 2
    from itertools import tee, ifilterfalse as filterfalse

_PY2 = sys.version_info[0] == 2
if not _PY2:
    basestring = str


# regex to find Python comments in the middle of (multiline) strings
CSTRIP = re.compile(r'#.*$', re.MULTILINE)  # comment stripping regex


def ensure_text(source):
    """
    Given either text or an iterable, return the corresponding text. This
    common pre-process function allows ``textdata`` routines to take varied
    input, yet confidently process considering only the text case.
    """
    if isinstance(source, basestring):
        return source
    else:
        # a list, tuple, iterator, or generator giving lines of text;
        # convert to a single text for standard cleanups
        return "\n".join(list(source))


QUOTES = ("'", '"')


def noquotes(s):
    """
    If the given string starts and ends with a quote symbol, return its 'middle' with
    that quote symbol stripped off both ends.

    :param str s: Input string
    :return: String without quotes
    :rtype: str
    """
    if s.startswith(QUOTES) and s.endswith(QUOTES):
        return s.strip(s[0])
    else:
        return s


def partition(pred, iterable):
    """
    Use a predicate to partition entries into false entries and true entries.
    Derived from Python itertools cookbook at
    https://docs.python.org/3/library/itertools.html
    But unlike default definition, returns lists rather than generators.
    """
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return list(filterfalse(pred, t1)), list(filter(pred, t2))
