def ensure_str(s):
    if isinstance(s, bytes):
        s = str(s)
        # b'Enum8(\'\xe6\x88\x91\' = 1, \'\x90"\' = 2)'
        if s[1] == "'":
            s = s[2:-1].replace("\\'", "'")
        # b"Enum8('\xe6\x88\x91' = 1, '\x90' = 2)"
        else:
            s = s[2:-1]
    return s
