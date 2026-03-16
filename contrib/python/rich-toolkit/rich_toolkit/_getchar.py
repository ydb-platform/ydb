"""
Unified getchar implementation for all platforms.

Combines approaches from:
- Textual (Unix/Linux): Copyright (c) 2023 Textualize Inc., MIT License
- Click (Windows fallback): Copyright 2014 Pallets, BSD-3-Clause License
"""

import os
import sys
from codecs import getincrementaldecoder
from typing import Optional, TextIO


def getchar() -> str:
    """
    Read input from stdin with support for longer pasted text.

    On Windows:
    - Uses msvcrt for native Windows console input
    - Handles special keys that send two-byte sequences
    - Reads up to 4096 characters for paste support

    On Unix/Linux:
    - Uses Textual's approach with manual termios configuration
    - Reads up to 4096 bytes with proper UTF-8 decoding
    - Provides fine-grained terminal control

    Returns:
        str: The input character(s) read from stdin

    Raises:
        KeyboardInterrupt: When CTRL+C is pressed
    """
    if sys.platform == "win32":
        # Windows implementation
        try:
            import msvcrt
        except ImportError:
            # Fallback if msvcrt is not available
            return sys.stdin.read(1)

        # Use getwch for Unicode support
        func = msvcrt.getwch  # type: ignore

        # Read first character
        rv = func()

        # Check for special keys (they send two characters)
        if rv in ("\x00", "\xe0"):
            # Special key, read the second character
            rv += func()
            return rv

        # Check if more input is available (for paste support)
        chars = [rv]
        max_chars = 4096

        # Keep reading while characters are available
        while len(chars) < max_chars and msvcrt.kbhit():  # type: ignore
            next_char = func()

            # Handle special keys during paste
            if next_char in ("\x00", "\xe0"):
                # Stop here, let this be handled in next call
                break

            chars.append(next_char)

            # Check for CTRL+C
            if next_char == "\x03":
                raise KeyboardInterrupt()

        result = "".join(chars)

        # Check for CTRL+C in the full result
        if "\x03" in result:
            raise KeyboardInterrupt()

        return result

    else:
        # Unix/Linux implementation (Textual approach)
        import termios
        import tty

        f: Optional[TextIO] = None
        fd: int

        # Get the file descriptor
        if not sys.stdin.isatty():
            f = open("/dev/tty")
            fd = f.fileno()
        else:
            fd = sys.stdin.fileno()

        try:
            # Save current terminal settings
            attrs_before = termios.tcgetattr(fd)

            try:
                # Configure terminal settings (Textual-style)
                newattr = termios.tcgetattr(fd)

                # Patch LFLAG (local flags)
                # Disable:
                # - ECHO: Don't echo input characters
                # - ICANON: Disable canonical mode (line-by-line input)
                # - IEXTEN: Disable extended processing
                # - ISIG: Disable signal generation
                newattr[tty.LFLAG] &= ~(
                    termios.ECHO | termios.ICANON | termios.IEXTEN | termios.ISIG
                )

                # Patch IFLAG (input flags)
                # Disable:
                # - IXON/IXOFF: XON/XOFF flow control
                # - ICRNL/INLCR/IGNCR: Various newline translations
                newattr[tty.IFLAG] &= ~(
                    termios.IXON
                    | termios.IXOFF
                    | termios.ICRNL
                    | termios.INLCR
                    | termios.IGNCR
                )

                # Set VMIN to 1 (minimum number of characters to read)
                # This ensures we get at least 1 character
                newattr[tty.CC][termios.VMIN] = 1

                # Apply the new terminal settings
                termios.tcsetattr(fd, termios.TCSANOW, newattr)

                # Read up to 4096 bytes (same as Textual)
                raw_data = os.read(fd, 1024 * 4)

                # Use incremental UTF-8 decoder for proper Unicode handling
                decoder = getincrementaldecoder("utf-8")()
                result = decoder.decode(raw_data, final=True)

                # Check for CTRL+C (ASCII 3)
                if "\x03" in result:
                    raise KeyboardInterrupt()

                return result

            finally:
                # Restore original terminal settings
                termios.tcsetattr(fd, termios.TCSANOW, attrs_before)
                sys.stdout.flush()

                if f is not None:
                    f.close()

        except termios.error:
            # If we can't control the terminal, fall back to simple read
            return sys.stdin.read(1)
