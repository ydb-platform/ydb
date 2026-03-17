# -*- coding: utf-8 -*-
# vi:tabstop=4:expandtab:sw=4
"""Transliterate Unicode text into plain 7-bit ASCII.

Example usage:
>>> from unidecode import unidecode
>>> unidecode(u"\u5317\u4EB0")
"Bei Jing "

The transliteration uses a straightforward map, and doesn't have alternatives
for the same character based on language, position, or anything else.

In Python 3, a standard string object will be returned. If you need bytes, use:
>>> unidecode("Κνωσός").encode("ascii")
b'Knosos'
"""
import warnings
from sys import version_info

Cache = {}

class UnidecodeError(ValueError):
    def __init__(self, message, index=None):
        """Raised for Unidecode-related errors.

        The index attribute contains the index of the character that caused
        the error.
        """
        super(UnidecodeError, self).__init__(message)
        self.index = index

def _warn_if_not_unicode(string):
    if version_info[0] < 3 and not isinstance(string, unicode):
        warnings.warn(  "Argument %r is not an unicode object. "
                        "Passing an encoded string will likely have "
                        "unexpected results." % (type(string),),
                        RuntimeWarning, 2)


def unidecode_expect_ascii(string, errors='ignore', replace_str='?'):
    """Transliterate an Unicode object into an ASCII string

    >>> unidecode(u"\u5317\u4EB0")
    "Bei Jing "

    This function first tries to convert the string using ASCII codec.
    If it fails (because of non-ASCII characters), it falls back to
    transliteration using the character tables.

    This is approx. five times faster if the string only contains ASCII
    characters, but slightly slower than unicode_expect_nonascii if
    non-ASCII characters are present.

    errors specifies what to do with characters that have not been
    found in replacement tables. The default is 'ignore' which ignores
    the character. 'strict' raises an UnidecodeError. 'replace'
    substitutes the character with replace_str (default is '?').
    'preserve' keeps the original character.

    Note that if 'preserve' is used the returned string might not be
    ASCII!
    """

    _warn_if_not_unicode(string)
    try:
        bytestring = string.encode('ASCII')
    except UnicodeEncodeError:
        pass
    else:
        if version_info[0] >= 3:
            return string
        else:
            return bytestring

    return _unidecode(string, errors, replace_str)

def unidecode_expect_nonascii(string, errors='ignore', replace_str='?'):
    """Transliterate an Unicode object into an ASCII string

    >>> unidecode(u"\u5317\u4EB0")
    "Bei Jing "

    See unidecode_expect_ascii.
    """

    _warn_if_not_unicode(string)
    return _unidecode(string, errors, replace_str)

unidecode = unidecode_expect_ascii

def _get_repl_str(char):
    codepoint = ord(char)

    if codepoint < 0x80:
        # Already ASCII
        return str(char)

    if codepoint > 0xeffff:
        # No data on characters in Private Use Area and above.
        return None

    if 0xd800 <= codepoint <= 0xdfff:
        warnings.warn(  "Surrogate character %r will be ignored. "
                        "You might be using a narrow Python build." % (char,),
                        RuntimeWarning, 2)

    section = codepoint >> 8   # Chop off the last two hex digits
    position = codepoint % 256 # Last two hex digits

    try:
        table = Cache[section]
    except KeyError:
        try:
            mod = __import__('unidecode.x%03x'%(section), globals(), locals(), ['data'])
        except ImportError:
            # No data on this character
            Cache[section] = None
            return None

        Cache[section] = table = mod.data

    if table and len(table) > position:
        return table[position]
    else:
        return None

def _unidecode(string, errors, replace_str):
    retval = []

    for index, char in enumerate(string):
        repl = _get_repl_str(char)

        if repl is None:
            if errors == 'ignore':
                repl = ''
            elif errors == 'strict':
                raise UnidecodeError('no replacement found for character %r '
                        'in position %d' % (char, index), index)
            elif errors == 'replace':
                repl = replace_str
            elif errors == 'preserve':
                repl = char
            else:
                raise UnidecodeError('invalid value for errors parameter %r' % (errors,))

        retval.append(repl)

    return ''.join(retval)
