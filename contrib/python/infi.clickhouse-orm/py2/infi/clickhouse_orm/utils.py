from __future__ import unicode_literals
from six import string_types, binary_type, text_type, PY3
import codecs
import re


SPECIAL_CHARS = {
    "\b" : "\\b",
    "\f" : "\\f",
    "\r" : "\\r",
    "\n" : "\\n",
    "\t" : "\\t",
    "\0" : "\\0",
    "\\" : "\\\\",
    "'"  : "\\'"
}

SPECIAL_CHARS_REGEX = re.compile("[" + ''.join(SPECIAL_CHARS.values()) + "]")



def escape(value, quote=True):
    '''
    If the value is a string, escapes any special characters and optionally
    surrounds it with single quotes. If the value is not a string (e.g. a number),
    converts it to one.
    '''
    def escape_one(match):
        return SPECIAL_CHARS[match.group(0)]

    if isinstance(value, string_types):
        value = SPECIAL_CHARS_REGEX.sub(escape_one, value)
        if quote:
            value = "'" + value + "'"
    return text_type(value)


def unescape(value):
    return codecs.escape_decode(value)[0].decode('utf-8')


def parse_tsv(line):
    if PY3 and isinstance(line, binary_type):
        line = line.decode()
    if line and line[-1] == '\n':
        line = line[:-1]
    return [unescape(value) for value in line.split(str('\t'))]


def parse_array(array_string):
    """
    Parse an array string as returned by clickhouse. For example:
        "['hello', 'world']" ==> ["hello", "world"]
        "[1,2,3]"            ==> [1, 2, 3]
    """
    # Sanity check
    if len(array_string) < 2 or array_string[0] != '[' or array_string[-1] != ']':
        raise ValueError('Invalid array string: "%s"' % array_string)
    # Drop opening brace
    array_string = array_string[1:]
    # Go over the string, lopping off each value at the beginning until nothing is left
    values = []
    while True:
        if array_string == ']':
            # End of array
            return values
        elif array_string[0] in ', ':
            # In between values
            array_string = array_string[1:]
        elif array_string[0] == "'":
            # Start of quoted value, find its end
            match = re.search(r"[^\\]'", array_string)
            if match is None:
                raise ValueError('Missing closing quote: "%s"' % array_string)
            values.append(array_string[1 : match.start() + 1])
            array_string = array_string[match.end():]
        else:
            # Start of non-quoted value, find its end
            match = re.search(r",|\]", array_string)
            values.append(array_string[0 : match.start()])
            array_string = array_string[match.end() - 1:]


def import_submodules(package_name):
    """
    Import all submodules of a module.
    """
    import importlib, pkgutil
    package = importlib.import_module(package_name)
    return {
        name: importlib.import_module(package_name + '.' + name)
        for _, name, _ in pkgutil.iter_modules(package.__path__)
    }


def comma_join(items):
    """
    Joins an iterable of strings with commas.
    """
    return ', '.join(items)
