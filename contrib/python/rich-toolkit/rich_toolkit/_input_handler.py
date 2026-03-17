"""Unified input handler for all platforms."""

import sys
import unicodedata


class TextInputHandler:
    """Input handler with platform-specific key code support."""

    # Platform-specific key codes
    if sys.platform == "win32":
        # Windows uses \xe0 prefix for special keys when using msvcrt.getwch
        DOWN_KEY = "\xe0P"  # Down arrow
        UP_KEY = "\xe0H"  # Up arrow
        LEFT_KEY = "\xe0K"  # Left arrow
        RIGHT_KEY = "\xe0M"  # Right arrow
        DELETE_KEY = "\xe0S"  # Delete key
        BACKSPACE_KEY = "\x08"  # Backspace
        TAB_KEY = "\t"
        SHIFT_TAB_KEY = "\x00\x0f"  # Shift+Tab
        ENTER_KEY = "\r"

        # Alternative codes that might be sent
        ALT_BACKSPACE = "\x7f"
        ALT_DELETE = "\x00S"
    else:
        # Unix/Linux key codes (ANSI escape sequences)
        DOWN_KEY = "\x1b[B"
        UP_KEY = "\x1b[A"
        LEFT_KEY = "\x1b[D"
        RIGHT_KEY = "\x1b[C"
        BACKSPACE_KEY = "\x7f"
        DELETE_KEY = "\x1b[3~"
        TAB_KEY = "\t"
        SHIFT_TAB_KEY = "\x1b[Z"
        ENTER_KEY = "\r"

        # Alternative codes
        ALT_BACKSPACE = "\x08"
        ALT_DELETE = None

    def __init__(self):
        self.text = ""
        self._cursor_index = 0  # Character index in the text string

    @property
    def cursor_left(self) -> int:
        """Visual cursor position in display columns."""
        return self._get_text_width(self.text[: self._cursor_index])

    @staticmethod
    def _get_char_width(char: str) -> int:
        """Get the display width of a character (1 for normal, 2 for CJK/fullwidth)."""
        if not char:
            return 0

        # Check East Asian Width property
        east_asian_width = unicodedata.east_asian_width(char)
        # F (Fullwidth) and W (Wide) characters take 2 columns
        if east_asian_width in ("F", "W"):
            return 2
        # A (Ambiguous) characters are typically 2 columns in CJK contexts
        # but for simplicity we'll treat them as 1 (can be made configurable)
        return 1

    def _get_text_width(self, text: str) -> int:
        """Get the total display width of a text string."""
        return sum(self._get_char_width(char) for char in text)

    def _move_cursor_left(self) -> None:
        self._cursor_index = max(0, self._cursor_index - 1)

    def _move_cursor_right(self) -> None:
        self._cursor_index = min(len(self.text), self._cursor_index + 1)

    def _insert_char(self, char: str) -> None:
        self.text = (
            self.text[: self._cursor_index] + char + self.text[self._cursor_index :]
        )
        self._cursor_index += 1

    def _delete_char(self) -> None:
        """Delete character before cursor (backspace)."""
        if self._cursor_index == 0:
            return

        self.text = (
            self.text[: self._cursor_index - 1] + self.text[self._cursor_index :]
        )
        self._cursor_index -= 1

    def _delete_forward(self) -> None:
        """Delete character at cursor (delete key)."""
        if self._cursor_index >= len(self.text):
            return

        self.text = (
            self.text[: self._cursor_index] + self.text[self._cursor_index + 1 :]
        )

    def handle_key(self, key: str) -> None:
        # Handle backspace (both possible codes)
        if key == self.BACKSPACE_KEY or (
            self.ALT_BACKSPACE and key == self.ALT_BACKSPACE
        ):
            self._delete_char()
        # Handle delete key
        elif key == self.DELETE_KEY or (self.ALT_DELETE and key == self.ALT_DELETE):
            self._delete_forward()
        elif key == self.LEFT_KEY:
            self._move_cursor_left()
        elif key == self.RIGHT_KEY:
            self._move_cursor_right()
        elif key in (
            self.UP_KEY,
            self.DOWN_KEY,
            self.ENTER_KEY,
            self.SHIFT_TAB_KEY,
            self.TAB_KEY,
        ):
            pass
        else:
            # Handle regular text input
            # Special keys on Windows start with \x00 or \xe0
            if sys.platform == "win32" and key and key[0] in ("\x00", "\xe0"):
                # Skip special key sequences
                return

            # Even if we call this handle_key, in some cases we might receive
            # multiple keys at once (e.g., during paste operations)
            for char in key:
                self._insert_char(char)
