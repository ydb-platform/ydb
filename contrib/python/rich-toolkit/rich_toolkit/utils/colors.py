from rich.color import Color
from rich.color_triplet import ColorTriplet
from rich.style import Style
from rich.text import Text
from typing_extensions import Literal


def lighten(color: Color, amount: float) -> Color:
    triplet = color.triplet

    if not triplet:
        triplet = color.get_truecolor()

    r, g, b = triplet

    r = int(r + (255 - r) * amount)
    g = int(g + (255 - g) * amount)
    b = int(b + (255 - b) * amount)

    return Color.from_triplet(ColorTriplet(r, g, b))


def darken(color: Color, amount: float) -> Color:
    triplet = color.triplet

    if not triplet:
        triplet = color.get_truecolor()

    r, g, b = triplet

    r = int(r * (1 - amount))
    g = int(g * (1 - amount))
    b = int(b * (1 - amount))

    return Color.from_triplet(ColorTriplet(r, g, b))


def fade_color(
    color: Color, background_color: Color, brightness_multiplier: float
) -> Color:
    """
    Fade a color towards the background color based on a brightness multiplier.

    Args:
        color: The original color (Rich Color object)
        background_color: The background color to fade towards
        brightness_multiplier: Float between 0.0 and 1.0 where:
            - 1.0 = original color (no fading)
            - 0.0 = completely faded to background color

    Returns:
        A new Color object with the faded color
    """
    # Extract RGB components from the original color

    color_triplet = color.triplet

    if color_triplet is None:
        color_triplet = color.get_truecolor()

    r, g, b = color_triplet

    assert background_color.triplet is not None
    # Extract RGB components from the background color
    bg_r, bg_g, bg_b = background_color.triplet

    # Blend the original color with the background color based on the brightness multiplier
    new_r = int(r * brightness_multiplier + bg_r * (1 - brightness_multiplier))
    new_g = int(g * brightness_multiplier + bg_g * (1 - brightness_multiplier))
    new_b = int(b * brightness_multiplier + bg_b * (1 - brightness_multiplier))

    # Ensure values are within valid RGB range (0-255)
    new_r = max(0, min(255, new_r))
    new_g = max(0, min(255, new_g))
    new_b = max(0, min(255, new_b))

    # Return a new Color object with the calculated RGB values
    return Color.from_rgb(new_r, new_g, new_b)


def fade_text(
    text: Text,
    text_color: Color,
    background_color: str,
    brightness_multiplier: float,
) -> Text:
    bg_color = Color.parse(background_color)

    new_spans = []
    for span in text._spans:
        style: Style | str = span.style

        if isinstance(style, str):
            style = Style.parse(style)

        if style.color:
            color = style.color

            if color == Color.default():
                color = text_color

            style = style.copy()
            style._color = fade_color(color, bg_color, brightness_multiplier)

        new_spans.append(span._replace(style=style))
    text = text.copy()
    text._spans = new_spans
    text.style = Style(color=fade_color(text_color, bg_color, brightness_multiplier))
    return text


def _get_terminal_color(
    color_type: Literal["text", "background"], default_color: str
) -> str:
    import os
    import re
    import select

    # Set appropriate OSC code and default color based on color_type
    if color_type.lower() == "text":
        osc_code = "10"
    elif color_type.lower() == "background":
        osc_code = "11"
    else:
        raise ValueError("color_type must be either 'text' or 'background'")

    try:
        import fcntl
        import termios
        import tty
    except ImportError:
        # Not on a Unix-like system
        return default_color

    # Use a dedicated fd via /dev/tty instead of sys.stdin so we don't
    # affect the process's stdin/stdout/stderr. On Linux, fds 0/1/2 share
    # the same open file description when connected to the same terminal,
    # so setting non-blocking or raw mode on stdin would also affect stdout,
    # breaking logging in forked worker processes (e.g. uvicorn with --workers).
    try:
        tty_fd = os.open("/dev/tty", os.O_RDWR | os.O_NOCTTY)
    except OSError:
        return default_color

    try:
        # Serialize access across forked workers. termios settings are
        # per-terminal-device, not per-fd, so concurrent setcbreak/restore
        # calls from different processes race and can cause the terminal's
        # OSC response to be echoed visibly.
        fcntl.flock(tty_fd, fcntl.LOCK_EX)
    except OSError:
        os.close(tty_fd)
        return default_color

    old_settings = termios.tcgetattr(tty_fd)

    try:
        # Use setcbreak instead of setraw to keep ISIG enabled so that
        # Ctrl+C continues to generate SIGINT. setraw disables ISIG which
        # breaks signal handling in multi-process contexts (e.g. uvicorn
        # workers). setcbreak only disables ECHO and ICANON, which is all
        # we need for reading the OSC response character-by-character.
        tty.setcbreak(tty_fd)

        # Send OSC escape sequence to query color
        os.write(tty_fd, f"\033]{osc_code};?\033\\".encode())

        # Wait for response with timeout
        if select.select([tty_fd], [], [], 1.0)[0]:
            # Read response
            response = b""
            while True:
                if select.select([tty_fd], [], [], 1.0)[0]:
                    data = os.read(tty_fd, 32)
                    if not data:
                        break
                    response += data
                    # Terminal response ends with BEL (\a) or ST (\033\\)
                    if b"\a" in response or b"\033\\" in response:
                        break
                    if len(response) > 50:  # Safety limit
                        break
                else:
                    break

            # Parse the response (format: \033]10;rgb:RRRR/GGGG/BBBB\033\\)
            # Color components can be 1-4 hex digits depending on terminal
            match = re.search(
                rb"rgb:([0-9a-f]+)/([0-9a-f]+)/([0-9a-f]+)",
                response,
                re.IGNORECASE,
            )
            if match:
                r_hex, g_hex, b_hex = match.groups()
                # Convert to 8-bit by taking the first 2 hex digits
                r = int(r_hex[:2], 16)
                g = int(g_hex[:2], 16)
                b = int(b_hex[:2], 16)
                return f"#{r:02x}{g:02x}{b:02x}"

            return default_color
        else:
            return default_color
    except KeyboardInterrupt:
        # This can happen when a worker process is interrupted (Ctrl+C)
        # while in the middle of querying the terminal. Return the default
        # color gracefully — the interrupt will be handled by the caller.
        return default_color
    finally:
        # Restore terminal settings using TCSAFLUSH to discard any
        # unread response bytes left in the input buffer, then release
        # the lock and close our dedicated fd.
        termios.tcsetattr(tty_fd, termios.TCSAFLUSH, old_settings)
        fcntl.flock(tty_fd, fcntl.LOCK_UN)
        os.close(tty_fd)


def get_terminal_text_color(default_color: str = "#FFFFFF") -> str:
    """Get the terminal text (foreground) color."""
    return _get_terminal_color("text", default_color)


def get_terminal_background_color(default_color: str = "#000000") -> str:
    """Get the terminal background color."""
    return _get_terminal_color("background", default_color)


if __name__ == "__main__":
    print(get_terminal_background_color())
    print(get_terminal_text_color())
