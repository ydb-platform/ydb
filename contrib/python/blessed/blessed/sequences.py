"""Module providing 'sequence awareness'."""
from __future__ import annotations

# std imports
import re
from typing import TYPE_CHECKING, Tuple, Pattern, Iterator, Optional, SupportsIndex

# 3rd party
from wcwidth import SequenceTextWrapper  # noqa: F401  # re-exported for API compatibility
from wcwidth import clip as wcwidth_clip
from wcwidth import ljust as wcwidth_ljust
from wcwidth import rjust as wcwidth_rjust
from wcwidth import width as wcwidth_width
from wcwidth import center as wcwidth_center

# local
from blessed._capabilities import CAPABILITIES_CAUSE_MOVEMENT, CAPABILITIES_HORIZONTAL_DISTANCE

if TYPE_CHECKING:  # pragma: no cover
    # local
    from blessed.terminal import Terminal

__all__ = ('Sequence', 'SequenceTextWrapper', 'iter_parse', 'measure_length')

# Translation table to remove C0 and C1 control characters.
# These cause wcswidth() to return -1, but should be ignored for width calculation
# since terminal sequences are already stripped before measurement.
_CONTROL_CHAR_TABLE = str.maketrans('', '', (
    ''.join(chr(c) for c in range(0x00, 0x20)) +  # C0: U+0000-U+001F
    '\x7f' +                                      # DEL
    ''.join(chr(c) for c in range(0x80, 0xA0))    # C1: U+0080-U+009F
))


class Termcap():
    """Terminal capability of given variable name and pattern."""

    def __init__(self, name: str, pattern: str, attribute: str, nparams: int = 0) -> None:
        """
        Class initializer.

        :arg str name: name describing capability.
        :arg str pattern: regular expression string.
        :arg str attribute: :class:`~.Terminal` attribute used to build
            this terminal capability.
        :arg int nparams: number of positional arguments for callable.
        """
        self.name = name
        self.pattern = pattern
        self.attribute = attribute
        self.nparams = nparams
        self._re_compiled: Optional[Pattern[str]] = None

    def __repr__(self) -> str:
        return f'<Termcap {self.name}:{self.pattern!r}>'

    @property
    def named_pattern(self) -> str:
        """Regular expression pattern for capability with named group."""
        return f'(?P<{self.name}>{self.pattern})'

    @property
    def re_compiled(self) -> Pattern[str]:
        """Compiled regular expression pattern for capability."""
        if self._re_compiled is None:
            self._re_compiled = re.compile(self.pattern)
        return self._re_compiled

    @property
    def will_move(self) -> bool:
        """Whether capability causes cursor movement."""
        return self.name in CAPABILITIES_CAUSE_MOVEMENT

    def horizontal_distance(self, text: str) -> int:
        """
        Horizontal carriage adjusted by capability, may be negative.

        :rtype: int
        :arg str text: for capabilities *parm_left_cursor*, *parm_right_cursor*, provide the
            matching sequence text, its interpreted distance is returned.
        :returns: 0 except for matching '
        :raises ValueError: ``text`` does not match regex for capability
        """
        value = CAPABILITIES_HORIZONTAL_DISTANCE.get(self.name)
        if value is None:
            return 0

        if self.nparams:
            match = self.re_compiled.match(text)
            if match:
                return value * int(match.group(1))
            raise ValueError(f'Invalid parameters for termccap {self.name}: {text!r}')

        return value

    # pylint: disable=too-many-positional-arguments
    @classmethod
    def build(cls, name: str, capability: str, attribute: str, nparams: int = 0,
              numeric: int = 99, match_grouped: bool = False, match_any: bool = False,
              match_optional: bool = False) -> "Termcap":
        r"""
        Class factory builder for given capability definition.

        :arg str name: Variable name given for this pattern.
        :arg str capability: A unicode string representing a terminal
            capability to build for. When ``nparams`` is non-zero, it
            must be a callable unicode string (such as the result from
            ``getattr(term, 'bold')``.
        :arg str attribute: The terminfo(5) capability name by which this
            pattern is known.
        :arg int nparams: number of positional arguments for callable.
        :arg int numeric: Value to substitute into capability to when generating pattern
        :arg bool match_grouped: If the numeric pattern should be
            grouped, ``(\d+)`` when ``True``, ``\d+`` default.
        :arg bool match_any: When keyword argument ``nparams`` is given,
            *any* numeric found in output is suitable for building as
            pattern ``(\d+)``.  Otherwise, only the first matching value of
            range *(numeric - 1)* through *(numeric + 1)* will be replaced by
            pattern ``(\d+)`` in builder.
        :arg bool match_optional: When ``True``, building of numeric patterns
            containing ``(\d+)`` will be built as optional, ``(\d+)?``.
        :rtype: blessed.sequences.Termcap
        :returns: Terminal capability instance for given capability definition
        """
        _numeric_regex = r'\d+'
        if match_grouped:
            _numeric_regex = r'(\d+)'
        if match_optional:
            _numeric_regex = r'(\d+)?'
        numeric = 99 if numeric is None else numeric

        # basic capability attribute, not used as a callable
        if nparams == 0:
            return cls(name, re.escape(capability), attribute, nparams)

        # a callable capability accepting numeric argument
        _outp = re.escape(capability(*(numeric,) * nparams))
        if not match_any:
            for num in range(numeric - 1, numeric + 2):
                if str(num) in _outp:
                    pattern = _outp.replace(str(num), _numeric_regex)
                    return cls(name, pattern, attribute, nparams)

        pattern = r'(\d+)' if match_grouped else r'\d+'
        return cls(name, re.sub(pattern, lambda x: _numeric_regex, _outp), attribute, nparams)


class Sequence(str):
    """
    A "sequence-aware" version of the base :class:`str` class.

    This unicode-derived class understands the effect of escape sequences
    of printable length, allowing a properly implemented :meth:`rjust`,
    :meth:`ljust`, :meth:`center`, and :meth:`length`.

    .. note:: that other than :meth:`Sequence.padd`, this is just a thin layer
        over :mod:`wcwidth`, kept by name for API compatibility.
    """

    def __new__(cls, sequence_text: str, term: 'Terminal') -> Sequence:
        """
        Class constructor.

        :arg str sequence_text: A string that may contain sequences.
        :arg blessed.Terminal term: :class:`~.Terminal` instance.
        """
        new = str.__new__(cls, sequence_text)
        new._term = term
        return new

    def ljust(self, width: SupportsIndex, fillchar: str = ' ') -> str:
        """
        Return string containing sequences, left-adjusted.

        :arg int width: Total width given to left-adjust ``text``.
        :arg str fillchar: String for padding right-of ``text``.
        :returns: String of ``text``, left-aligned by ``width``.
        :rtype: str
        """
        return wcwidth_ljust(self, width.__index__(), fillchar, control_codes='ignore')

    def rjust(self, width: SupportsIndex, fillchar: str = ' ') -> str:
        """
        Return string containing sequences, right-adjusted.

        :arg int width: Total width given to right-adjust ``text``.
        :arg str fillchar: String for padding left-of ``text``.
        :returns: String of ``text``, right-aligned by ``width``.
        :rtype: str
        """
        return wcwidth_rjust(self, width.__index__(), fillchar, control_codes='ignore')

    def center(self, width: SupportsIndex, fillchar: str = ' ') -> str:
        """
        Return string containing sequences, centered.

        :arg int width: Total width given to center ``text``.
        :arg str fillchar: String for padding left and right-of ``text``.
        :returns: String of ``text``, centered by ``width``.
        :rtype: str
        """
        return wcwidth_center(self, width.__index__(), fillchar, control_codes='ignore')

    def truncate(self, width: SupportsIndex) -> str:
        """
        Truncate a string in a sequence-aware manner.

        Any printable characters beyond ``width`` are removed, while all
        sequences remain in place. Horizontal sequences are first expanded
        by :meth:`padd`.

        Wide characters (such as CJK or emoji) that would partially exceed
        ``width`` are replaced with space padding to maintain exact width.

        SGR (terminal styling) sequences are propagated: the result begins
        with any active style at the start position and ends with a reset
        sequence if styles were active.

        :arg int width: The printable width to truncate the string to.
        :rtype: str
        :returns: String truncated to exactly ``width`` printable characters.
        """
        # Use padd() to expand terminal-specific cursor movements to spaces,
        # then use wcwidth's clip() to truncate while preserving all sequences.
        return wcwidth_clip(self.padd(), 0, width.__index__())

    def length(self) -> int:
        r"""
        Return the printable length of string containing sequences.

        Returns the maximum horizontal cursor extent reached while processing
        the string. Backspace and cursor-left movements do not reduce the
        length below the maximum position reached.

        Some characters may consume more than one cell, mainly CJK (Chinese,
        Japanese, Korean) and Emojis and some kinds of symbols.

        For example:

            >>> from blessed import Terminal
            >>> from blessed.sequences import Sequence
            >>> term = Terminal()
            >>> msg = term.clear + term.red('コンニチハ')
            >>> Sequence(msg, term).length()
            10

        .. note:: Although accounted for, strings containing sequences such
            as ``term.clear`` will not give accurate returns, it is not
            considered lengthy (a length of 0).
        """
        return wcwidth_width(self)

    def strip(self, chars: Optional[str] = None) -> str:
        """
        Return string of sequences, leading and trailing whitespace removed.

        :arg str chars: Remove characters in chars instead of whitespace.
        :rtype: str
        :returns: string of sequences with leading and trailing whitespace removed.
        """
        return self.strip_seqs().strip(chars)

    def lstrip(self, chars: Optional[str] = None) -> str:
        """
        Return string of all sequences and leading whitespace removed.

        :arg str chars: Remove characters in chars instead of whitespace.
        :rtype: str
        :returns: string of sequences with leading removed.
        """
        return self.strip_seqs().lstrip(chars)

    def rstrip(self, chars: Optional[str] = None) -> str:
        """
        Return string of all sequences and trailing whitespace removed.

        :arg str chars: Remove characters in chars instead of whitespace.
        :rtype: str
        :returns: string of sequences with trailing removed.
        """
        return self.strip_seqs().rstrip(chars)

    def strip_seqs(self) -> str:
        """
        Return ``text`` stripped of only its terminal sequences.

        :rtype: str
        :returns: Text with terminal sequences removed
        """
        return self.padd(strip=True)

    def padd(self, strip: bool = False) -> str:
        """
        Return non-destructive horizontal movement as destructive spacing.

        :arg bool strip: Strip terminal sequences
        :rtype: str
        :returns: Text adjusted for horizontal movement
        """
        data = self
        if self._term.caps_compiled.search(data) is None:
            return str(data)
        if strip:  # strip all except CAPABILITIES_HORIZONTAL_DISTANCE
            # pylint: disable-next=protected-access
            data = self._term._caps_compiled_without_hdist.sub("", data)

            if self._term.caps_compiled.search(data) is None:
                return str(data)

            # pylint: disable-next=protected-access
            caps = self._term._hdist_caps_named_compiled
        else:
            # pylint: disable-next=protected-access
            caps = self._term._caps_named_compiled

        outp = ''
        last_end = 0

        for match in caps.finditer(data):

            # Capture unmatched text between matched capabilities
            if match.start() > last_end:
                outp += data[last_end:match.start()]

            last_end = match.end()
            text = match.group(match.lastgroup)
            value = self._term.caps[match.lastgroup].horizontal_distance(text)

            if value > 0:
                outp += ' ' * value
            elif value < 0:
                outp = outp[:value]
            else:
                outp += text

        # Capture any remaining unmatched text
        if last_end < len(data):
            outp += data[last_end:]

        return outp


def iter_parse(term: 'Terminal', text: str) -> Iterator[Tuple[str, Optional[Termcap]]]:
    """
    Generator yields (text, capability) for characters of ``text``.

    value for ``capability`` may be ``None``, where ``text`` is
    :class:`str` of length 1.  Otherwise, ``text`` is a full
    matching sequence of given capability.

    .. note:: Previously used by SequenceTextWrapper, sequence-aware text
       wrapping now exists in wcwidth as :func:`wcwidth.wrap`. This function
       is kept for API compatibility and used by :func:`measure_length`.
    """
    for match in term._caps_compiled_any.finditer(text):  # pylint: disable=protected-access
        name = match.lastgroup
        value = match.group(name) if name else ''
        if name == 'MISMATCH':
            yield (value, None)
        else:
            yield value, term.caps.get(name, '')


def measure_length(text: str, term: 'Terminal') -> int:
    """
    Kept for API compatibility.

    :rtype: int
    :returns: Length of the first sequence in the string

    .. deprecated:: 1.30.0
    """
    try:
        text, capability = next(iter_parse(term, text))
        if capability:
            return len(text)
    except StopIteration:
        return 0
    return 0
