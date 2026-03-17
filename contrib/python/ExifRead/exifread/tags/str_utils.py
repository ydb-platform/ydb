"""
Misc utilities.
"""


def make_string_uc(seq) -> str:
    """
    Special version to deal with the code in the first 8 bytes of a user comment.
    First 8 bytes gives coding system e.g. ASCII vs. JIS vs Unicode.
    """
    if not isinstance(seq, str):
        # Remove code from sequence only if it is valid
        if make_string(seq[:8]).upper() in ("ASCII", "UNICODE", "JIS", ""):
            seq = seq[8:]
    # Of course, this is only correct if ASCII, and the standard explicitly
    # allows JIS and Unicode.
    return make_string(seq)


def make_string(seq) -> str:
    """
    Don't throw an exception when given an out of range character.
    """
    string = ""
    for char in seq:
        # Screen out non-printing characters
        try:
            if 32 <= char < 256:
                string += chr(char)
        except TypeError:
            pass

    # If no printing chars
    if not string:
        if isinstance(seq, list):
            string = "".join(map(str, seq))
            # Some UserComment lists only contain null bytes, nothing valuable to return
            if set(string) == {"0"}:
                return ""
        else:
            string = str(seq)

    # Clean undesirable characters on any end
    return string.strip(" \x00")
