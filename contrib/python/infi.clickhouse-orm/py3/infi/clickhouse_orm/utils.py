import codecs
import re
from datetime import date, datetime, tzinfo, timedelta


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

    if isinstance(value, str):
        value = SPECIAL_CHARS_REGEX.sub(escape_one, value)
        if quote:
            value = "'" + value + "'"
    return str(value)


def unescape(value):
    return codecs.escape_decode(value)[0].decode('utf-8')


def string_or_func(obj):
    return obj.to_sql() if hasattr(obj, 'to_sql') else obj


def arg_to_sql(arg):
    """
    Converts a function argument to SQL string according to its type.
    Supports functions, model fields, strings, dates, datetimes, timedeltas, booleans,
    None, numbers, timezones, arrays/iterables.
    """
    from infi.clickhouse_orm import Field, StringField, DateTimeField, DateField, F, QuerySet
    if isinstance(arg, F):
        return arg.to_sql()
    if isinstance(arg, Field):
        return "`%s`" % arg
    if isinstance(arg, str):
        return StringField().to_db_string(arg)
    if isinstance(arg, datetime):
        return "toDateTime(%s)" % DateTimeField().to_db_string(arg)
    if isinstance(arg, date):
        return "toDate('%s')" % arg.isoformat()
    if isinstance(arg, timedelta):
        return "toIntervalSecond(%d)" % int(arg.total_seconds())
    if isinstance(arg, bool):
        return str(int(arg))
    if isinstance(arg, tzinfo):
        return StringField().to_db_string(arg.tzname(None))
    if arg is None:
        return 'NULL'
    if isinstance(arg, QuerySet):
        return "(%s)" % arg
    if isinstance(arg, tuple):
        return '(' + comma_join(arg_to_sql(x) for x in arg) + ')'
    if is_iterable(arg):
        return '[' + comma_join(arg_to_sql(x) for x in arg) + ']'
    return str(arg)


def parse_tsv(line):
    if isinstance(line, bytes):
        line = line.decode()
    if line and line[-1] == '\n':
        line = line[:-1]
    return [unescape(value) for value in line.split(str('\t'))]


def parse_array(array_string):
    """
    Parse an array or tuple string as returned by clickhouse. For example:
        "['hello', 'world']" ==> ["hello", "world"]
        "(1,2,3)"            ==> [1, 2, 3]
    """
    # Sanity check
    if len(array_string) < 2 or array_string[0] not in '[(' or array_string[-1] not in '])':
        raise ValueError('Invalid array string: "%s"' % array_string)
    # Drop opening brace
    array_string = array_string[1:]
    # Go over the string, lopping off each value at the beginning until nothing is left
    values = []
    while True:
        if array_string in '])':
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


def comma_join(items, stringify=False):
    """
    Joins an iterable of strings with commas.
    """
    if stringify:
        return ', '.join(str(item) for item in items)
    else:
        return ', '.join(items)


def is_iterable(obj):
    """
    Checks if the given object is iterable.
    """
    try:
        iter(obj)
        return True
    except TypeError:
        return False


def get_subclass_names(locals, base_class):
    from inspect import isclass
    return [c.__name__ for c in locals.values() if isclass(c) and issubclass(c, base_class)]


class NoValue:
    '''
    A sentinel for fields with an expression for a default value,
    that were not assigned a value yet.
    '''
    def __repr__(self):
        return 'NO_VALUE'

NO_VALUE = NoValue()
