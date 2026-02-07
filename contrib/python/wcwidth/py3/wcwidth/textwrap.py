"""
Sequence-aware text wrapping functions.

This module provides functions for wrapping text that may contain terminal escape sequences, with
proper handling of Unicode grapheme clusters and character display widths.
"""
# std imports
import textwrap

from typing import List

# local
from .wcwidth import width as _width
from .wcwidth import iter_sequences
from .grapheme import iter_graphemes
from .escape_sequences import ZERO_WIDTH_PATTERN


class SequenceTextWrapper(textwrap.TextWrapper):
    """
    Sequence-aware text wrapper extending :class:`textwrap.TextWrapper`.

    This wrapper properly handles terminal escape sequences and Unicode grapheme clusters when
    calculating text width for wrapping.

    This implementation is based on the SequenceTextWrapper from the 'blessed' library, with
    contributions from Avram Lubkin and grayjk.

    The key difference from the blessed implementation is the addition of grapheme cluster support
    via :func:`~.iter_graphemes`, providing width calculation for ZWJ emoji sequences, VS-16 emojis
    and variations, regional indicator flags, and combining characters.
    """

    def __init__(self, width: int = 70, *,
                 control_codes: str = 'parse',
                 tabsize: int = 8,
                 ambiguous_width: int = 1,
                 **kwargs):
        """
        Initialize the wrapper.

        :param width: Maximum line width in display cells.
        :param control_codes: How to handle control sequences (see :func:`~.width`).
        :param tabsize: Tab stop width for tab expansion.
        :param ambiguous_width: Width to use for East Asian Ambiguous (A) characters.
        :param kwargs: Additional arguments passed to :class:`textwrap.TextWrapper`.
        """
        super().__init__(width=width, **kwargs)
        self.control_codes = control_codes
        self.tabsize = tabsize
        self.ambiguous_width = ambiguous_width

    def _width(self, text: str) -> int:
        """Measure text width accounting for sequences."""
        return _width(text, control_codes=self.control_codes, tabsize=self.tabsize,
                      ambiguous_width=self.ambiguous_width)

    def _strip_sequences(self, text: str) -> str:
        """Strip all terminal sequences from text."""
        result = []
        for segment, is_seq in iter_sequences(text):
            if not is_seq:
                result.append(segment)
        return ''.join(result)

    def _extract_sequences(self, text: str) -> str:
        """Extract only terminal sequences from text."""
        result = []
        for segment, is_seq in iter_sequences(text):
            if is_seq:
                result.append(segment)
        return ''.join(result)

    def _split(self, text: str) -> List[str]:  # pylint: disable=too-many-locals
        """
        Sequence-aware variant of :meth:`textwrap.TextWrapper._split`.

        This method ensures that terminal escape sequences don't interfere with the text splitting
        logic, particularly for hyphen-based word breaking. It builds a position mapping from
        stripped text to original text, calls the parent's _split on stripped text, then maps chunks
        back.
        """
        # pylint: disable=too-many-locals,too-many-branches
        # Build a mapping from stripped text positions to original text positions.
        # We track where each character ENDS so that sequences between characters
        # attach to the following text (not preceding text). This ensures sequences
        # aren't lost when whitespace is dropped.
        #
        # char_end[i] = position in original text right after the i-th stripped char
        char_end: List[int] = []
        stripped_text = ''
        original_pos = 0

        for segment, is_seq in iter_sequences(text):
            if not is_seq:
                for char in segment:
                    original_pos += 1
                    char_end.append(original_pos)
                    stripped_text += char
            else:
                # Escape sequences advance position but don't add to stripped text
                original_pos += len(segment)

        # Add sentinel for final position
        char_end.append(original_pos)

        # Use parent's _split on the stripped text
        # pylint: disable-next=protected-access
        stripped_chunks = textwrap.TextWrapper._split(self, stripped_text)

        # Handle text that contains only sequences (no visible characters).
        # Return the sequences as a single chunk to preserve them.
        if not stripped_chunks and text:
            return [text]

        # Map the chunks back to the original text with sequences
        result: List[str] = []
        stripped_pos = 0
        num_chunks = len(stripped_chunks)

        for idx, chunk in enumerate(stripped_chunks):
            chunk_len = len(chunk)

            # Start is where previous character ended (or 0 for first chunk)
            start_orig = 0 if stripped_pos == 0 else char_end[stripped_pos - 1]

            # End is where next character starts. For last chunk, use sentinel
            # to include any trailing sequences.
            if idx == num_chunks - 1:
                end_orig = char_end[-1]  # sentinel includes trailing sequences
            else:
                end_orig = char_end[stripped_pos + chunk_len - 1]

            # Extract the corresponding portion from the original text
            result.append(text[start_orig:end_orig])
            stripped_pos += chunk_len

        return result

    def _wrap_chunks(self, chunks: List[str]) -> List[str]:  # pylint: disable=too-many-branches
        """
        Wrap chunks into lines using sequence-aware width.

        Override TextWrapper._wrap_chunks to use _width instead of len. Follows stdlib's algorithm:
        greedily fill lines, handle long words.
        """
        # pylint: disable=too-many-branches
        if not chunks:
            return []

        lines = []
        is_first_line = True

        # Arrange in reverse order so items can be efficiently popped
        chunks = list(reversed(chunks))

        while chunks:
            current_line: List[str] = []
            current_width = 0

            # Get the indent and available width for current line
            indent = self.initial_indent if is_first_line else self.subsequent_indent
            line_width = self.width - self._width(indent)

            # Drop leading whitespace (except at very start)
            # When dropping, transfer any sequences to the next chunk.
            # Only drop if there's actual whitespace text, not if it's only sequences.
            stripped = self._strip_sequences(chunks[-1])
            if self.drop_whitespace and lines and stripped and not stripped.strip():
                sequences = self._extract_sequences(chunks[-1])
                del chunks[-1]
                if sequences and chunks:
                    chunks[-1] = sequences + chunks[-1]

            # Greedily add chunks that fit
            while chunks:
                chunk = chunks[-1]
                chunk_width = self._width(chunk)

                if current_width + chunk_width <= line_width:
                    current_line.append(chunks.pop())
                    current_width += chunk_width
                else:
                    break

            # Handle chunk that's too long for any line
            if chunks and self._width(chunks[-1]) > line_width:
                self._handle_long_word(
                    chunks, current_line, current_width, line_width
                )
                current_width = self._width(''.join(current_line))
                # Remove any empty chunks left by _handle_long_word
                while chunks and not chunks[-1]:
                    del chunks[-1]

            # Drop trailing whitespace
            # When dropping, transfer any sequences to the previous chunk.
            # Only drop if there's actual whitespace text, not if it's only sequences.
            stripped_last = self._strip_sequences(current_line[-1]) if current_line else ''
            if (self.drop_whitespace and current_line and
                    stripped_last and not stripped_last.strip()):
                sequences = self._extract_sequences(current_line[-1])
                current_width -= self._width(current_line[-1])
                del current_line[-1]
                if sequences and current_line:
                    current_line[-1] = current_line[-1] + sequences

            if current_line:
                line_content = ''.join(current_line)
                # Strip trailing whitespace when drop_whitespace is enabled
                # (matches CPython #140627 fix behavior)
                if self.drop_whitespace:
                    line_content = line_content.rstrip()
                lines.append(indent + line_content)
                is_first_line = False

        return lines

    def _handle_long_word(self, reversed_chunks: List[str],
                          cur_line: List[str], cur_len: int,
                          width: int) -> None:
        """
        Sequence-aware :meth:`textwrap.TextWrapper._handle_long_word`.

        This method ensures that word boundaries are not broken mid-sequence, and respects grapheme
        cluster boundaries when breaking long words.
        """
        if width < 1:
            space_left = 1
        else:
            space_left = width - cur_len

        if self.break_long_words:
            chunk = reversed_chunks[-1]
            break_at_hyphen = False
            hyphen_end = 0

            # Handle break_on_hyphens: find last hyphen within space_left
            if self.break_on_hyphens:
                # Strip sequences to find hyphen in logical text
                stripped = self._strip_sequences(chunk)
                if len(stripped) > space_left:
                    # Find last hyphen in the portion that fits
                    hyphen_pos = stripped.rfind('-', 0, space_left)
                    if hyphen_pos > 0 and any(c != '-' for c in stripped[:hyphen_pos]):
                        # Map back to original position including sequences
                        hyphen_end = self._map_stripped_pos_to_original(chunk, hyphen_pos + 1)
                        break_at_hyphen = True

            # Break at grapheme boundaries to avoid splitting multi-codepoint characters
            if break_at_hyphen:
                actual_end = hyphen_end
            else:
                actual_end = self._find_break_position(chunk, space_left)
                # If no progress possible (e.g., wide char exceeds line width),
                # force at least one grapheme to avoid infinite loop.
                # Only force when cur_line is empty; if line has content,
                # appending nothing is safe and the line will be committed.
                if actual_end == 0 and not cur_line:
                    actual_end = self._find_first_grapheme_end(chunk)
            cur_line.append(chunk[:actual_end])
            reversed_chunks[-1] = chunk[actual_end:]

        elif not cur_line:
            cur_line.append(reversed_chunks.pop())

    def _map_stripped_pos_to_original(self, text: str, stripped_pos: int) -> int:
        """Map a position in stripped text back to original text position."""
        stripped_idx = 0
        original_idx = 0

        for segment, is_seq in iter_sequences(text):
            if is_seq:
                original_idx += len(segment)
            elif stripped_idx + len(segment) > stripped_pos:
                # Position is within this segment
                return original_idx + (stripped_pos - stripped_idx)
            else:
                stripped_idx += len(segment)
                original_idx += len(segment)

        # Caller guarantees stripped_pos < total stripped chars, so we always
        # return from within the loop. This line satisfies the type checker.
        return original_idx  # pragma: no cover

    def _find_break_position(self, text: str, max_width: int) -> int:
        """Find string index in text that fits within max_width cells."""
        idx = 0
        width_so_far = 0

        while idx < len(text):
            char = text[idx]

            # Skip escape sequences (they don't add width)
            if char == '\x1b':
                match = ZERO_WIDTH_PATTERN.match(text, idx)
                if match:
                    idx = match.end()
                    continue

            # Get grapheme
            grapheme = next(iter_graphemes(text[idx:]))

            grapheme_width = self._width(grapheme)
            if width_so_far + grapheme_width > max_width:
                return idx  # Found break point

            width_so_far += grapheme_width
            idx += len(grapheme)

        # Caller guarantees chunk_width > max_width, so a grapheme always
        # exceeds and we return from within the loop. Type checker requires this.
        return idx  # pragma: no cover

    def _find_first_grapheme_end(self, text: str) -> int:
        """Find the end position of the first grapheme."""
        return len(next(iter_graphemes(text)))


def wrap(text: str, width: int = 70, *,
         control_codes: str = 'parse',
         tabsize: int = 8,
         ambiguous_width: int = 1,
         initial_indent: str = '',
         subsequent_indent: str = '',
         break_long_words: bool = True,
         break_on_hyphens: bool = True) -> List[str]:
    r"""
    Wrap text to fit within given width, returning a list of wrapped lines.

    Like :func:`textwrap.wrap`, but measures width in display cells rather than
    characters, correctly handling wide characters, combining marks, and terminal
    escape sequences.

    :param str text: Text to wrap, may contain terminal sequences.
    :param int width: Maximum line width in display cells.
    :param str control_codes: How to handle terminal sequences (see :func:`~.width`).
    :param int tabsize: Tab stop width for tab expansion.
    :param int ambiguous_width: Width to use for East Asian Ambiguous (A)
        characters. Default is ``1`` (narrow). Set to ``2`` for CJK contexts.
    :param str initial_indent: String prepended to first line.
    :param str subsequent_indent: String prepended to subsequent lines.
    :param bool break_long_words: If True, break words longer than width.
    :param bool break_on_hyphens: If True, allow breaking at hyphens.
    :returns: List of wrapped lines without trailing newlines.
    :rtype: list[str]

    Like :func:`textwrap.wrap`, newlines in the input text are treated as
    whitespace and collapsed. To preserve paragraph breaks, wrap each
    paragraph separately::

        >>> text = 'First line.\\nSecond line.'
        >>> wrap(text, 40)  # newline collapsed to space
        ['First line. Second line.']
        >>> [line for para in text.split('\\n')
        ...  for line in (wrap(para, 40) if para else [''])]
        ['First line.', 'Second line.']

    .. seealso::

       :func:`textwrap.wrap`, :class:`textwrap.TextWrapper`
           Standard library text wrapping (character-based).

       :class:`.SequenceTextWrapper`
           Class interface for advanced wrapping options.

    .. versionadded:: 0.3.0

    Example::

        >>> from wcwidth import wrap
        >>> wrap('hello world', 5)
        ['hello', 'world']
        >>> wrap('中文字符', 4)  # CJK characters (2 cells each)
        ['中文', '字符']
    """
    wrapper = SequenceTextWrapper(
        width=width,
        control_codes=control_codes,
        tabsize=tabsize,
        ambiguous_width=ambiguous_width,
        initial_indent=initial_indent,
        subsequent_indent=subsequent_indent,
        break_long_words=break_long_words,
        break_on_hyphens=break_on_hyphens,
    )
    return wrapper.wrap(text)
