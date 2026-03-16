import msvcrt

from ._config import config


def readchar() -> str:
    """Reads a single utf8-character from the input stream.
    Blocks until a character is available."""

    # read a single wide character from the input
    return msvcrt.getwch()


def readkey() -> str:
    """Reads the next keypress. If an escaped key is pressed, the full
    sequence is read and returned as noted in `_win_key.py`."""

    # read first character
    ch = readchar()

    # keys like CTRL+C should cause a interrupt
    if ch in config.INTERRUPT_KEYS:
        raise KeyboardInterrupt

    # parse special multi character keys (see key module)
    # https://learn.microsoft.com/cpp/c-runtime-library/reference/getch-getwch#remarks
    if ch in "\x00\xe0":
        # read the second half
        # we always return the 0x00 prefix, this avoids duplications in the key module
        ch = "\x00" + readchar()

    # parse unicode surrogates
    # https://docs.python.org/3/c-api/unicode.html#c.Py_UNICODE_IS_SURROGATE
    if "\uD800" <= ch <= "\uDFFF":
        ch += readchar()

        # combine the characters into a single utf-16 encoded string.
        # this prevents the character from being treated as a surrogate pair again.
        ch = ch.encode("utf-16", errors="surrogatepass").decode("utf-16")

    return ch
