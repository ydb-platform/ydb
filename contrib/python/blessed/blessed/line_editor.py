"""
Headless line editor with history, auto-suggest, and grapheme-aware editing.

This module provides :class:`LineEditor` for single-line input with readline-style
editing and :class:`LineHistory` for command recall.
"""
from __future__ import annotations

# std imports
from typing import TYPE_CHECKING, Dict, List, Deque, Tuple, Union, Callable, Optional
from collections import deque
from dataclasses import dataclass

if TYPE_CHECKING:  # pragma: no cover
    from .terminal import Terminal

# 3rd party
from wcwidth import width as wcswidth
from wcwidth import iter_graphemes

PASSWORD_CHAR = "\u273b"

__all__ = (
    "DEFAULT_KEYMAP",
    "DisplayState",
    "LineEditResult",
    "LineHistory",
    "LineEditor",
)


@dataclass
class LineEditResult:
    """Result of processing a keystroke."""

    #: Accepted line text when Enter was pressed, otherwise ``None``.
    line: Optional[str] = None
    #: ``True`` when Ctrl+D pressed on an empty line.
    eof: bool = False
    #: ``True`` when Ctrl+C pressed.
    interrupt: bool = False
    #: ``True`` when the display needs redrawing.
    changed: bool = False
    #: Bell string to emit (e.g. ``"\\a"``), empty when silent.
    bell: str = ""


@dataclass
class DisplayState:  # pylint: disable=too-many-instance-attributes
    """Current visual state of the editor for rendering."""

    #: Visible buffer text (masked in password mode, clipped when scrolling).
    text: str = ""
    #: Cursor column within the visible window.
    cursor: int = 0
    #: Auto-suggest suffix (rendered dim/grey after text).
    suggestion: str = ""
    #: ``True`` when content extends beyond the left edge.
    overflow_left: bool = False
    #: ``True`` when content extends beyond the right edge.
    overflow_right: bool = False
    #: SGR sequence applied to buffer text.
    text_sgr: str = ""
    #: SGR sequence applied to suggestion text.
    suggestion_sgr: str = ""
    #: SGR sequence applied to fill the background.
    bg_sgr: str = ""
    #: SGR sequence applied to the ellipsis indicator.
    ellipsis_sgr: str = ""


def _is_control(grapheme: str) -> bool:
    """Return ``True`` if *grapheme* is a C0 or C1 control character."""
    cp = ord(grapheme[0])
    return cp < 0x20 or 0x7F <= cp < 0xA0


class LineHistory:
    """
    In-memory command history with navigation and prefix search.

    :param max_entries: Maximum number of history entries retained.
    """

    def __init__(self, max_entries: int = 5000) -> None:
        """Initialize history with given maximum capacity."""
        #: History entries list (most recent last).
        self.entries: List[str] = []
        self._max_entries = max_entries
        self._nav_idx: int = -1
        self._nav_saved: str = ""

    def add(self, line: str) -> None:
        """Append *line* to history, skipping empty and consecutive duplicates."""
        if not line:
            return
        if self.entries and self.entries[-1] == line:
            return
        self.entries.append(line)
        if len(self.entries) > self._max_entries:
            del self.entries[:-self._max_entries]

    def search_prefix(self, prefix: str) -> Optional[str]:
        """Return the most recent entry starting with *prefix*, or ``None``."""
        if not prefix:
            return None
        for entry in reversed(self.entries):
            if entry.startswith(prefix) and entry != prefix:
                return entry
        return None

    def nav_start(self, current_line: str) -> None:
        """Begin history navigation, saving *current_line*."""
        self._nav_idx = len(self.entries)
        self._nav_saved = current_line

    def nav_up(self) -> Optional[str]:
        """Navigate to the previous (older) history entry."""
        if self._nav_idx <= 0:
            return None
        self._nav_idx -= 1
        return self.entries[self._nav_idx]

    def nav_down(self) -> Optional[str]:
        """Navigate to the next (newer) history entry."""
        if self._nav_idx >= len(self.entries):
            return None
        self._nav_idx += 1
        if self._nav_idx >= len(self.entries):
            return self._nav_saved
        return self.entries[self._nav_idx]


class LineEditor:  # pylint: disable=too-many-instance-attributes
    """
    Headless single-line editor with grapheme-aware cursor movement.

    Feed keystrokes via :meth:`feed_key`, read display state via
    :attr:`display`.  Accepts blessed :class:`~.Keystroke` objects
    directly (dispatching on ``.name``), or plain strings for testing.

    Custom keymap handlers must be callables accepting a single
    :class:`LineEditor` argument and returning a :class:`LineEditResult`::

        def my_handler(editor: LineEditor) -> LineEditResult:
            ...
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        history: Optional[LineHistory] = None,
        password: bool = False,
        password_char: str = PASSWORD_CHAR,
        max_width: int = 0,
        ellipsis: str = "\u2026",
        limit: int = 65536,
        limit_bell: str = "\a",
        scroll_jump: float = 0.5,
        text_sgr: str = "\x1b[38;2;230;225;220m",
        suggestion_sgr: str = "\x1b[30m",
        bg_sgr: str = "",
        ellipsis_sgr: str = "",
        keymap: Optional[Dict[str, Optional[Callable[..., LineEditResult]]]] = None,
    ) -> None:
        """Initialize editor with optional history, display, and keymap settings."""
        self._buf: List[str] = []
        self._cursor: int = 0
        self._history = history or LineHistory()
        self._password_mode: bool = password
        self.password_char: str = password_char
        self._kill_ring: Deque[str] = deque(maxlen=64)
        self._undo_stack: List[Tuple[List[str], int]] = []
        self._navigating_history: bool = False
        self.max_width: int = max_width
        self.ellipsis: str = ellipsis
        self.limit: int = limit
        self.limit_bell: str = limit_bell
        self._limit_bell_fired: bool = False
        self.scroll_jump: float = scroll_jump
        self._scroll_offset: int = 0
        self.text_sgr: str = text_sgr
        self.suggestion_sgr: str = suggestion_sgr
        self.bg_sgr: str = bg_sgr
        self.ellipsis_sgr: str = ellipsis_sgr
        self.keymap: Dict[str, Optional[Callable[..., LineEditResult]]] = dict(DEFAULT_KEYMAP)
        if keymap:
            self.keymap.update(keymap)
        self._prev_cursor: int = 0
        self._prev_content_w: int = 0
        self._prev_overflow: Tuple[bool, bool] = (False, False)
        self._prev_scroll_offset: int = 0

    @property
    def history(self) -> LineHistory:
        """Return the attached :class:`LineHistory` instance."""
        return self._history

    @property
    def line(self) -> str:
        """Return the current buffer contents as a string."""
        return "".join(self._buf)

    @property
    def password_mode(self) -> bool:
        """Return whether password mode is currently active."""
        return self._password_mode

    def _apply_sgr(self, state: DisplayState) -> DisplayState:
        """Populate SGR fields on a :class:`DisplayState` from editor style."""
        state.text_sgr = self.text_sgr
        state.suggestion_sgr = self.suggestion_sgr
        state.bg_sgr = self.bg_sgr
        state.ellipsis_sgr = self.ellipsis_sgr
        return state

    @property
    def display(self) -> DisplayState:
        """Return the current :class:`DisplayState` for rendering."""
        if self.password_mode:
            text = self.password_char * len(self._buf)
            cursor_col = self._cursor * wcswidth(self.password_char)
            suggestion = ""
        else:
            text = self.line
            cursor_col = self._cursor_display_col()
            suggestion = self._get_suggestion()
        if self._needs_hscroll():
            content_w = wcswidth(text) + wcswidth(suggestion)
            offset = self._compute_scroll(cursor_col, content_w)
            return self._apply_sgr(_apply_hscroll(
                text, suggestion, cursor_col, self.max_width,
                self.ellipsis, scroll_offset=offset))
        return self._apply_sgr(
            DisplayState(text=text, cursor=cursor_col, suggestion=suggestion))

    def _update_render_state(self, cur: DisplayState, content_w: int) -> None:
        """Update previous-frame tracking state after rendering."""
        self._prev_cursor = cur.cursor
        self._prev_content_w = content_w
        self._prev_overflow = (cur.overflow_left, cur.overflow_right)
        self._prev_scroll_offset = self._scroll_offset

    def render(self, term: Terminal, row: int, width: int) -> str:
        """
        Build escape sequences to render the current display state.

        :param term: Blessed :class:`~.Terminal` instance for cursor/SGR.
        :param row: Terminal row for the input line.
        :param width: Available columns.
        :returns: Escape-sequence string; caller writes/encodes it.
        """
        cur = self.display
        ellipsis_w = wcswidth(self.ellipsis)
        parts: List[str] = [term.move_yx(row, 0), cur.bg_sgr]
        rendered = 0

        if cur.overflow_left:
            parts.extend((cur.ellipsis_sgr, self.ellipsis, cur.bg_sgr))
            rendered += ellipsis_w

        if cur.text:
            parts.extend((cur.text_sgr, cur.text))
            rendered += wcswidth(cur.text)

        if cur.suggestion:
            parts.extend((cur.suggestion_sgr, cur.suggestion))
            rendered += wcswidth(cur.suggestion)

        if cur.overflow_right:
            parts.extend((cur.ellipsis_sgr, self.ellipsis))
            rendered += ellipsis_w

        pad = width - rendered
        if pad > 0:
            parts.extend((cur.bg_sgr, " " * pad))

        parts.extend((term.normal, term.move_yx(row, cur.cursor)))
        self._update_render_state(cur, rendered)
        return "".join(parts)

    def render_insert(
        self, term: Terminal, row: int, grapheme: str
    ) -> Optional[str]:
        """
        Fast-path render for a single grapheme inserted at end of buffer.

        :param term: Blessed :class:`~.Terminal` instance.
        :param row: Terminal row for the input line.
        :param grapheme: The grapheme cluster just inserted.
        :returns: Escape-sequence string, or ``None`` if a full redraw is needed.
        """
        if self._cursor != len(self._buf):
            return None
        cur = self.display
        if (cur.overflow_left, cur.overflow_right) != self._prev_overflow:
            return None
        if self._scroll_offset != self._prev_scroll_offset:
            return None
        col = self._prev_cursor
        parts: List[str] = [term.move_yx(row, col), cur.text_sgr, grapheme]
        new_content_w = wcswidth(cur.text) + wcswidth(cur.suggestion)
        if cur.suggestion:
            parts.extend((cur.suggestion_sgr, cur.suggestion))
        trail = self._prev_content_w - new_content_w
        if trail > 0:
            parts.extend((cur.bg_sgr, " " * trail))
        parts.extend((term.normal, term.move_yx(row, cur.cursor)))
        self._update_render_state(cur, new_content_w)
        return "".join(parts)

    def render_backspace(self, term: Terminal, row: int) -> Optional[str]:
        """
        Fast-path render after a backspace at end of buffer.

        :param term: Blessed :class:`~.Terminal` instance.
        :param row: Terminal row for the input line.
        :returns: Escape-sequence string, or ``None`` if a full redraw is needed.
        """
        if self._cursor != len(self._buf):
            return None
        cur = self.display
        if (cur.overflow_left, cur.overflow_right) != self._prev_overflow:
            return None
        if self._scroll_offset != self._prev_scroll_offset:
            return None
        col = cur.cursor
        new_content_w = wcswidth(cur.text) + wcswidth(cur.suggestion)
        erase = self._prev_content_w - new_content_w
        parts: List[str] = [term.move_yx(row, col)]
        if cur.suggestion:
            parts.extend((cur.suggestion_sgr, cur.suggestion))
        if erase > 0:
            parts.extend((cur.bg_sgr, " " * erase))
        parts.extend((term.normal, term.move_yx(row, cur.cursor)))
        self._update_render_state(cur, new_content_w)
        return "".join(parts)

    def feed_key(self, key: Union["Keystroke", str]) -> LineEditResult:  # noqa: F821
        """Process one keystroke and return a :class:`LineEditResult`."""
        name = getattr(key, "name", None)
        if name:
            handler = self.keymap.get(name)
            if handler is not None:
                return handler(self)
            return LineEditResult()
        key_str = str(key)
        if key_str and key_str.isprintable():
            if self._at_limit():
                return LineEditResult(
                    changed=False, bell=self._fire_limit_bell())
            self._save_undo()
            self._insert_at_cursor(key_str)
            self._navigating_history = False
            return LineEditResult(changed=True)
        return LineEditResult()

    def insert_text(self, text: str) -> LineEditResult:
        """Insert *text* at cursor position (for bracketed paste)."""
        if self._at_limit():
            return LineEditResult(
                changed=False, bell=self._fire_limit_bell())
        self._save_undo()
        count = self._insert_at_cursor(text, filter_control=True)
        if count == 0:
            self._undo_stack.pop()
            return LineEditResult(changed=False)
        return LineEditResult(changed=True)

    def clear(self) -> None:
        """Clear the buffer and reset cursor to start."""
        self._buf.clear()
        self._cursor = 0
        self._scroll_offset = 0
        self._navigating_history = False

    def set_password_mode(self, enabled: bool) -> None:
        """Toggle password masking on or off."""
        self._password_mode = enabled

    def _needs_hscroll(self) -> bool:
        return self.max_width > 0 and not 0 < self.limit <= self.max_width

    def _compute_scroll(self, cursor_col: int, content_width: int) -> int:
        usable = self.max_width
        if content_width < usable and cursor_col < usable:
            self._scroll_offset = 0
            return 0
        jump = max(1, int(usable * self.scroll_jump))
        ellipsis_w = wcswidth(self.ellipsis)
        left_margin = ellipsis_w if self._scroll_offset > 0 else 0
        right_edge = self._scroll_offset + usable - left_margin
        if cursor_col >= right_edge:
            self._scroll_offset = cursor_col - usable + jump + 1 + ellipsis_w
        elif cursor_col <= self._scroll_offset and self._scroll_offset > 0:
            self._scroll_offset = max(0, cursor_col - jump)
        return self._scroll_offset

    def _at_limit(self) -> bool:
        return self.limit > 0 and len(self._buf) >= self.limit

    def _fire_limit_bell(self) -> str:
        if not self._limit_bell_fired:
            self._limit_bell_fired = True
            return self.limit_bell
        return ""

    def _maybe_reset_limit_bell(self) -> None:
        if self._limit_bell_fired and not self._at_limit():
            self._limit_bell_fired = False

    def _cursor_display_col(self) -> int:
        return sum(wcswidth(g) for g in self._buf[:self._cursor])

    def _save_undo(self) -> None:
        self._undo_stack.append((list(self._buf), self._cursor))
        if len(self._undo_stack) > 100:
            del self._undo_stack[:-100]

    def _insert_at_cursor(self, text: str, check_limit: bool = True,
                          filter_control: bool = False) -> int:
        """Insert *text* graphemes at cursor, returning count inserted."""
        count = 0
        for grapheme in iter_graphemes(text):
            if check_limit and self._at_limit():
                break
            if filter_control and _is_control(grapheme):
                continue
            self._buf.insert(self._cursor, grapheme)
            self._cursor += 1
            count += 1
        return count

    def _set_text(self, text: str) -> None:
        self._buf = list(iter_graphemes(text))
        self._cursor = len(self._buf)

    def _undo(self) -> LineEditResult:
        if not self._undo_stack:
            return LineEditResult()
        self._buf, self._cursor = self._undo_stack.pop()
        return LineEditResult(changed=True)

    def _handle_enter(self) -> LineEditResult:
        line = self.line
        if line and not self.password_mode:
            self._history.add(line)
        self.clear()
        self._undo_stack.clear()
        return LineEditResult(line=line, changed=True)

    def _handle_ctrl_c(self) -> LineEditResult:
        self.clear()
        self._undo_stack.clear()
        return LineEditResult(interrupt=True, changed=True)

    def _handle_ctrl_d(self) -> LineEditResult:
        if not self._buf:
            return LineEditResult(eof=True)
        return self._delete_at_cursor()

    def _move_left(self) -> LineEditResult:
        if self._cursor > 0:
            self._cursor -= 1
            return LineEditResult(changed=True)
        return LineEditResult()

    def _move_right(self) -> LineEditResult:
        if self._cursor < len(self._buf):
            self._cursor += 1
            return LineEditResult(changed=True)
        return LineEditResult()

    def _move_home(self) -> LineEditResult:
        if self._cursor > 0:
            self._cursor = 0
            return LineEditResult(changed=True)
        return LineEditResult()

    def _move_end(self) -> LineEditResult:
        if self._cursor < len(self._buf):
            self._cursor = len(self._buf)
            return LineEditResult(changed=True)
        return LineEditResult()

    def _find_word_left(self) -> int:
        pos = self._cursor - 1
        while pos > 0 and not self._buf[pos - 1].isalnum():
            pos -= 1
        while pos > 0 and self._buf[pos - 1].isalnum():
            pos -= 1
        return pos

    def _move_word_left(self) -> LineEditResult:
        if self._cursor == 0:
            return LineEditResult()
        self._cursor = self._find_word_left()
        return LineEditResult(changed=True)

    def _move_word_right(self) -> LineEditResult:
        n = len(self._buf)
        if self._cursor >= n:
            return LineEditResult()
        pos = self._cursor
        while pos < n and not self._buf[pos].isalnum():
            pos += 1
        while pos < n and self._buf[pos].isalnum():
            pos += 1
        self._cursor = pos
        return LineEditResult(changed=True)

    def _backspace(self) -> LineEditResult:
        if self._cursor > 0:
            self._save_undo()
            self._cursor -= 1
            del self._buf[self._cursor]
            self._maybe_reset_limit_bell()
            return LineEditResult(changed=True)
        return LineEditResult()

    def _delete_at_cursor(self) -> LineEditResult:
        if self._cursor < len(self._buf):
            self._save_undo()
            del self._buf[self._cursor]
            self._maybe_reset_limit_bell()
            return LineEditResult(changed=True)
        return LineEditResult()

    def _kill_to_end(self) -> LineEditResult:
        if self._cursor < len(self._buf):
            self._save_undo()
            killed = "".join(self._buf[self._cursor:])
            del self._buf[self._cursor:]
            self._kill_ring.append(killed)
            self._maybe_reset_limit_bell()
            return LineEditResult(changed=True)
        return LineEditResult()

    def _kill_line(self) -> LineEditResult:
        if self._buf and self._cursor > 0:
            self._save_undo()
            killed = "".join(self._buf[:self._cursor])
            del self._buf[:self._cursor]
            self._cursor = 0
            self._kill_ring.append(killed)
            self._maybe_reset_limit_bell()
            return LineEditResult(changed=True)
        return LineEditResult()

    def _kill_word_back(self) -> LineEditResult:
        if self._cursor == 0:
            return LineEditResult()
        self._save_undo()
        end = self._cursor
        pos = self._find_word_left()
        killed = "".join(self._buf[pos:end])
        del self._buf[pos:end]
        self._cursor = pos
        self._kill_ring.append(killed)
        self._maybe_reset_limit_bell()
        return LineEditResult(changed=True)

    def _yank(self) -> LineEditResult:
        if not self._kill_ring:
            return LineEditResult()
        self._save_undo()
        self._insert_at_cursor(self._kill_ring[-1], check_limit=False)
        return LineEditResult(changed=True)

    def _history_prev(self) -> LineEditResult:
        if not self._navigating_history:
            self._history.nav_start(self.line)
            self._navigating_history = True
        entry = self._history.nav_up()
        if entry is not None:
            self._set_text(entry)
            return LineEditResult(changed=True)
        return LineEditResult()

    def _history_next(self) -> LineEditResult:
        if not self._navigating_history:
            return LineEditResult()
        entry = self._history.nav_down()
        if entry is not None:
            self._set_text(entry)
            return LineEditResult(changed=True)
        return LineEditResult()

    def _accept_suggestion(self) -> LineEditResult:
        if self._cursor == len(self._buf):
            suggestion = self._get_suggestion()
            if suggestion:
                self._save_undo()
                self._insert_at_cursor(suggestion, check_limit=False)
                return LineEditResult(changed=True)
        return self._move_right()

    def _get_suggestion(self) -> str:
        if self.password_mode:
            return ""
        line = self.line
        if not line or self._cursor != len(self._buf):
            return ""
        match = self._history.search_prefix(line)
        if match is not None:
            return match[len(line):]
        return ""


def _clip_graphemes(
    combined: str, scroll_offset: int, usable: int
) -> Tuple[List[str], int]:
    """
    Collect visible graphemes from *combined* within a scroll window.

    :param combined: Full text (buffer + suggestion).
    :param scroll_offset: Column offset of the left edge.
    :param usable: Available display columns.
    :returns: ``(visible_parts, visible_width)`` tuple.
    """
    vis_parts: List[str] = []
    vis_width = 0
    col = 0
    for grapheme in iter_graphemes(combined):
        g_w = wcswidth(grapheme)
        if col + g_w <= scroll_offset:
            col += g_w
            continue
        if vis_width + g_w > usable:
            break
        vis_parts.append(grapheme)
        vis_width += g_w
        col += g_w
    return vis_parts, vis_width


def _split_text_suggestion(
    vis_parts: List[str], scroll_offset: int, text_w: int
) -> Tuple[str, str]:
    """
    Split visible graphemes into text and suggestion portions.

    :param vis_parts: Graphemes within the visible window.
    :param scroll_offset: Column offset of the left edge.
    :param text_w: Display width of the buffer text (before suggestion).
    :returns: ``(visible_text, visible_suggestion)`` tuple.
    """
    vis_text_parts: List[str] = []
    vis_suggest_parts: List[str] = []
    pos = 0
    for grapheme in vis_parts:
        g_w = wcswidth(grapheme)
        if scroll_offset + pos < text_w:
            vis_text_parts.append(grapheme)
        else:
            vis_suggest_parts.append(grapheme)
        pos += g_w
    return "".join(vis_text_parts), "".join(vis_suggest_parts)


def _default_scroll_offset(cursor_col: int, max_width: int) -> int:
    """
    Compute an initial scroll offset when none is provided.

    :param cursor_col: Cursor display column.
    :param max_width: Available display columns.
    :returns: Scroll offset in columns.
    """
    if cursor_col >= max_width:
        jump = max(1, max_width // 2)
        return cursor_col - max_width + jump + 1
    return 0


def _trim_right_overflow(
    vis_parts: List[str], vis_width: int, ellipsis_w: int, usable: int
) -> int:
    """
    Remove trailing graphemes to make room for a right-side ellipsis.

    :param vis_parts: Visible grapheme list (mutated in place).
    :param vis_width: Current total display width of *vis_parts*.
    :param ellipsis_w: Display width of the ellipsis character.
    :param usable: Available display columns.
    :returns: Updated visible width after trimming.
    """
    while vis_parts and vis_width + ellipsis_w > usable:
        vis_width -= wcswidth(vis_parts.pop())
    return vis_width


def _apply_hscroll(  # pylint: disable=too-many-positional-arguments
    text: str,
    suggestion: str,
    cursor_col: int,
    max_width: int,
    ellipsis: str = "\u2026",
    scroll_offset: Optional[int] = None,
) -> DisplayState:
    """Build a :class:`DisplayState` with horizontal scrolling applied."""
    text_w = wcswidth(text)

    if text_w + wcswidth(suggestion) < max_width and cursor_col < max_width:
        return DisplayState(
            text=text, cursor=cursor_col, suggestion=suggestion)

    if scroll_offset is None:
        scroll_offset = _default_scroll_offset(cursor_col, max_width)

    overflow_left = scroll_offset > 0
    ellipsis_w = wcswidth(ellipsis)
    usable = max_width - (ellipsis_w if overflow_left else 0)

    vis_parts, vis_width = _clip_graphemes(
        text + suggestion, scroll_offset, usable)

    overflow_right = (scroll_offset + usable) < text_w + wcswidth(suggestion)
    if overflow_right:
        _trim_right_overflow(vis_parts, vis_width, ellipsis_w, usable)

    vis_text, vis_suggestion = _split_text_suggestion(
        vis_parts, scroll_offset, text_w)

    return DisplayState(
        text=vis_text,
        cursor=cursor_col - scroll_offset + (ellipsis_w if overflow_left else 0),
        suggestion=vis_suggestion,
        overflow_left=overflow_left,
        overflow_right=overflow_right,
    )


# pylint: disable=protected-access
DEFAULT_KEYMAP: Dict[str, Callable[..., LineEditResult]] = {
    "KEY_ENTER": LineEditor._handle_enter,
    "KEY_CTRL_C": LineEditor._handle_ctrl_c,
    "KEY_CTRL_D": LineEditor._handle_ctrl_d,
    "KEY_LEFT": LineEditor._move_left,
    "KEY_RIGHT": LineEditor._accept_suggestion,
    "KEY_HOME": LineEditor._move_home,
    "KEY_END": LineEditor._move_end,
    "KEY_CTRL_A": LineEditor._move_home,
    "KEY_CTRL_B": LineEditor._move_left,
    "KEY_CTRL_E": LineEditor._move_end,
    "KEY_CTRL_F": LineEditor._move_right,
    "KEY_SLEFT": LineEditor._move_word_left,
    "KEY_SRIGHT": LineEditor._move_word_right,
    "KEY_CTRL_LEFT": LineEditor._move_word_left,
    "KEY_CTRL_RIGHT": LineEditor._move_word_right,
    "KEY_BACKSPACE": LineEditor._backspace,
    "KEY_DELETE": LineEditor._delete_at_cursor,
    "KEY_CTRL_K": LineEditor._kill_to_end,
    "KEY_CTRL_U": LineEditor._kill_line,
    "KEY_CTRL_W": LineEditor._kill_word_back,
    "KEY_CTRL_Y": LineEditor._yank,
    "KEY_UP": LineEditor._history_prev,
    "KEY_DOWN": LineEditor._history_next,
    "KEY_CTRL_N": LineEditor._history_next,
    "KEY_CTRL_P": LineEditor._history_prev,
    "KEY_CTRL_Z": LineEditor._undo,
}
# pylint: enable=protected-access
