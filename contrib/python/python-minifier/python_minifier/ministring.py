BACKSLASH = '\\'


class MiniString(object):
    """
    Create a representation of a string object

    :param str string: The string to minify

    """

    def __init__(self, string, quote="'"):
        self._s = string
        self.safe_mode = False
        self.quote = quote

    def __str__(self):
        """
        The smallest python literal representation of a string

        :rtype: str

        """

        if self._s == '':
            return ''

        if len(self.quote) == 1:
            s = self.to_short()
        else:
            s = self.to_long()

        try:
            eval(self.quote + s + self.quote)
        except (UnicodeDecodeError, UnicodeEncodeError):
            if self.safe_mode:
                raise

            self.safe_mode = True
            if len(self.quote) == 1:
                s = self.to_short()
            else:
                s = self.to_long()

        assert eval(self.quote + s + self.quote) == self._s

        return s

    def to_short(self):
        s = ''

        escaped = {
            '\n': BACKSLASH + 'n',
            '\\': BACKSLASH + BACKSLASH,
            '\a': BACKSLASH + 'a',
            '\b': BACKSLASH + 'b',
            '\f': BACKSLASH + 'f',
            '\r': BACKSLASH + 'r',
            '\t': BACKSLASH + 't',
            '\v': BACKSLASH + 'v',
            '\0': BACKSLASH + 'x00',
            self.quote: BACKSLASH + self.quote,
        }

        for c in self._s:
            if c in escaped:
                s += escaped[c]
            else:
                if self.safe_mode:
                    unicode_value = ord(c)
                    if unicode_value <= 0x7F:
                        s += c
                    elif unicode_value <= 0xFFFF:
                        s += BACKSLASH + 'u' + format(unicode_value, '04x')
                    else:
                        s += BACKSLASH + 'U' + format(unicode_value, '08x')
                else:
                    s += c

        return s

    def to_long(self):
        s = ''

        escaped = {
            '\\': BACKSLASH + BACKSLASH,
            '\a': BACKSLASH + 'a',
            '\b': BACKSLASH + 'b',
            '\f': BACKSLASH + 'f',
            '\r': BACKSLASH + 'r',
            '\t': BACKSLASH + 't',
            '\v': BACKSLASH + 'v',
            '\0': BACKSLASH + 'x00',
            self.quote[0]: BACKSLASH + self.quote[0],
        }

        for c in self._s:
            if c in escaped:
                s += escaped[c]
            else:
                if self.safe_mode:
                    unicode_value = ord(c)
                    if unicode_value <= 0x7F:
                        s += c
                    elif unicode_value <= 0xFFFF:
                        s += BACKSLASH + 'u' + format(unicode_value, '04x')
                    else:
                        s += BACKSLASH + 'U' + format(unicode_value, '08x')
                else:
                    s += c

        return s


class MiniBytes(object):
    """
    Create a representation of a bytes object

    :param bytes string: The string to minify

    """

    def __init__(self, string, quote="'"):
        self._b = string
        self.quote = quote

    def __str__(self):
        """
        The smallest python literal representation of a string

        :rtype: str

        """

        if self._b == b'':
            return ''

        if len(self.quote) == 1:
            s = self.to_short()
        else:
            s = self.to_long()

        assert eval('b' + self.quote + s + self.quote) == self._b

        return s

    def to_short(self):
        b = ''

        for c in self._b:
            if c == b'\\':
                b += BACKSLASH
            elif c == b'\n':
                b += BACKSLASH + 'n'
            elif c == self.quote:
                b += BACKSLASH + self.quote
            else:
                if c >= 128:
                    b += BACKSLASH + chr(c)
                else:
                    b += chr(c)

        return b

    def to_long(self):
        b = ''

        for c in self._b:
            if c == b'\\':
                b += BACKSLASH
            elif c == self.quote:
                b += BACKSLASH + self.quote
            else:
                if c >= 128:
                    b += BACKSLASH + chr(c)
                else:
                    b += chr(c)

        return b
