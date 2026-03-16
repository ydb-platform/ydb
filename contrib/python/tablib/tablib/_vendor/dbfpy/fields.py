import datetime
import struct
from functools import total_ordering

from . import utils

__version__ = "$Revision: 1.14 $"[11:-2]
__date__ = "$Date: 2009/05/26 05:16:51 $"[7:-2]

__all__ = ["lookupFor"]  # field classes added at the end of the module


"""DBF fields definitions.

TODO:
  - make memos work
"""
"""History (most recent first):
26-may-2009 [als]   DbfNumericFieldDef.decodeValue: strip zero bytes
05-feb-2009 [als]   DbfDateFieldDef.encodeValue: empty arg produces empty date
16-sep-2008 [als]   DbfNumericFieldDef decoding looks for decimal point
                    in the value to select float or integer return type
13-mar-2008 [als]   check field name length in constructor
11-feb-2007 [als]   handle value conversion errors
10-feb-2007 [als]   DbfFieldDef: added .rawFromRecord()
01-dec-2006 [als]   Timestamp columns use None for empty values
31-oct-2006 [als]   support field types 'F' (float), 'I' (integer)
                    and 'Y' (currency);
                    automate export and registration of field classes
04-jul-2006 [als]   added export declaration
10-mar-2006 [als]   decode empty values for Date and Logical fields;
                    show field name in errors
10-mar-2006 [als]   fix Numeric value decoding: according to spec,
                    value always is string representation of the number;
                    ensure that encoded Numeric value fits into the field
20-dec-2005 [yc]    use field names in upper case
15-dec-2005 [yc]    field definitions moved from `dbf`.
"""

# abstract definitions


@total_ordering
class DbfFieldDef:
    """Abstract field definition.

    Child classes must override ``type`` class attribute to provide datatype
    information of the field definition. For more info about types visit
    `https://www.clicketyclick.dk/databases/xbase/format/data_types.html`

    Also child classes must override ``defaultValue`` field to provide
    default value for the field value.

    If child class has fixed length ``length`` class attribute must be
    overridden and set to the valid value. None value means, that field
    isn't of fixed length.

    Note: ``name`` field must not be changed after instantiation.

    """

    __slots__ = ("name", "decimalCount", "start", "end", "ignoreErrors")

    # length of the field, None in case of variable-length field,
    # or a number if this field is a fixed-length field
    length = None

    # field type. for more information about fields types visit
    # `https://www.clicketyclick.dk/databases/xbase/format/data_types.html`
    # must be overridden in child classes
    typeCode = None

    # default value for the field. this field must be
    # overridden in child classes
    defaultValue = None

    def __init__(self, name, length=None, decimalCount=None,
                 start=None, stop=None, ignoreErrors=False):
        """Initialize instance."""
        assert self.typeCode is not None, "Type code must be overridden"
        assert self.defaultValue is not None, "Default value must be overridden"
        # fix arguments
        if len(name) > 10:
            raise ValueError(f"Field name \"{name}\" is too long")
        name = str(name).upper()
        if self.__class__.length is None:
            if length is None:
                raise ValueError(f"[{name}] Length isn't specified")
            length = int(length)
            if length <= 0:
                raise ValueError(f"[{name}] Length must be a positive integer")
        else:
            length = self.length
        if decimalCount is None:
            decimalCount = 0
        # set fields
        self.name = name
        # FIXME: validate length according to the specification at
        # https://www.clicketyclick.dk/databases/xbase/format/data_types.html
        self.length = length
        self.decimalCount = decimalCount
        self.ignoreErrors = ignoreErrors
        self.start = start
        self.end = stop

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return repr(self) != repr(other)

    def __lt__(self, other):
        return repr(self) < repr(other)

    def __hash__(self):
        return hash(self.name)

    def fromString(cls, string, start, ignoreErrors=False):
        """Decode dbf field definition from the string data.

        Arguments:
            string:
                a string, dbf definition is decoded from. length of
                the string must be 32 bytes.
            start:
                position in the database file.
            ignoreErrors:
                initial error processing mode for the new field (boolean)

        """
        assert len(string) == 32
        _length = string[16]
        return cls(utils.unzfill(string)[:11].decode('utf-8'), _length,
                   string[17], start, start + _length, ignoreErrors=ignoreErrors)
    fromString = classmethod(fromString)

    def toString(self):
        """Return encoded field definition.

        Return:
            Return value is a string object containing encoded
            definition of this field.

        """
        _name = self.name.ljust(11, '\0')
        return (
            _name +
            self.typeCode +
            # data address
            chr(0) * 4 +
            chr(self.length) +
            chr(self.decimalCount) +
            chr(0) * 14
        )

    def __repr__(self):
        return "%-10s %1s %3d %3d" % self.fieldInfo()  # noqa: UP031

    def fieldInfo(self):
        """Return field information.

        Return:
            Return value is a (name, type, length, decimals) tuple.

        """
        return self.name, self.typeCode, self.length, self.decimalCount

    def rawFromRecord(self, record):
        """Return a "raw" field value from the record string."""
        return record[self.start:self.end]

    def decodeFromRecord(self, record):
        """Return decoded field value from the record string."""
        try:
            return self.decodeValue(self.rawFromRecord(record))
        except Exception:
            if self.ignoreErrors:
                return utils.INVALID_VALUE
            else:
                raise

    def decodeValue(self, value):
        """Return decoded value from string value.

        This method shouldn't be used publicly. It's called from the
        `decodeFromRecord` method.

        This is an abstract method and it must be overridden in child classes.
        """
        raise NotImplementedError

    def encodeValue(self, value):
        """Return str object containing encoded field value.

        This is an abstract method and it must be overridden in child classes.
        """
        raise NotImplementedError

# real classes


class DbfCharacterFieldDef(DbfFieldDef):
    """Definition of the character field."""

    typeCode = "C"
    defaultValue = b''

    def decodeValue(self, value):
        """Return string object.

        Return value is a ``value`` argument with stripped right spaces.

        """
        return value.rstrip(b' ').decode('utf-8')

    def encodeValue(self, value):
        """Return raw data string encoded from a ``value``."""
        return str(value)[:self.length].ljust(self.length)


class DbfNumericFieldDef(DbfFieldDef):
    """Definition of the numeric field."""

    typeCode = "N"
    # XXX: now I'm not sure it was a good idea to make a class field
    # `defaultValue` instead of a generic method as it was implemented
    # previously -- it's ok with all types except number, cuz
    # if self.decimalCount is 0, we should return 0 and 0.0 otherwise.
    defaultValue = 0

    def decodeValue(self, value):
        """Return a number decoded from ``value``.

        If decimals is zero, value will be decoded as an integer;
        or as a float otherwise.

        Return:
            Return value is a int (long) or float instance.

        """
        value = value.strip(b' \0')
        if b'.' in value:
            # a float (has decimal separator)
            return float(value)
        elif value:
            # must be an integer
            return int(value)
        else:
            return 0

    def encodeValue(self, value):
        """Return string containing encoded ``value``."""
        _rv = ("%*.*f" % (self.length, self.decimalCount, value))  # noqa: UP031
        if len(_rv) > self.length:
            _ppos = _rv.find(".")
            if 0 <= _ppos <= self.length:
                _rv = _rv[:self.length]
            else:
                raise ValueError(
                    f"[{self.name}] Numeric overflow: {_rv} (field width: {self.length})"
                )
        return _rv


class DbfFloatFieldDef(DbfNumericFieldDef):
    """Definition of the float field - same as numeric."""

    typeCode = "F"


class DbfIntegerFieldDef(DbfFieldDef):
    """Definition of the integer field."""

    typeCode = "I"
    length = 4
    defaultValue = 0

    def decodeValue(self, value):
        """Return an integer number decoded from ``value``."""
        return struct.unpack("<i", value)[0]

    def encodeValue(self, value):
        """Return string containing encoded ``value``."""
        return struct.pack("<i", int(value))


class DbfCurrencyFieldDef(DbfFieldDef):
    """Definition of the currency field."""

    typeCode = "Y"
    length = 8
    defaultValue = 0.0

    def decodeValue(self, value):
        """Return float number decoded from ``value``."""
        return struct.unpack("<q", value)[0] / 10000.

    def encodeValue(self, value):
        """Return string containing encoded ``value``."""
        return struct.pack("<q", round(value * 10000))


class DbfLogicalFieldDef(DbfFieldDef):
    """Definition of the logical field."""

    typeCode = "L"
    defaultValue = -1
    length = 1

    def decodeValue(self, value):
        """Return True, False or -1 decoded from ``value``."""
        # Note: value always is 1-char string
        if value == "?":
            return -1
        if value in "NnFf ":
            return False
        if value in "YyTt":
            return True
        raise ValueError(f"[{self.name}] Invalid logical value {value!r}")

    def encodeValue(self, value):
        """Return a character from the "TF?" set.

        Return:
            Return value is "T" if ``value`` is True
            "?" if value is -1 or False otherwise.

        """
        if value is True:
            return "T"
        if value == -1:
            return "?"
        return "F"


class DbfMemoFieldDef(DbfFieldDef):
    """Definition of the memo field.

    Note: memos aren't currently completely supported.

    """

    typeCode = "M"
    defaultValue = " " * 10
    length = 10

    def decodeValue(self, value):
        """Return int .dbt block number decoded from the string object."""
        # return int(value)
        raise NotImplementedError

    def encodeValue(self, value):
        """Return raw data string encoded from a ``value``.

        Note: this is an internal method.

        """
        # return str(value)[:self.length].ljust(self.length)
        raise NotImplementedError


class DbfDateFieldDef(DbfFieldDef):
    """Definition of the date field."""

    typeCode = "D"
    defaultValue = utils.classproperty(lambda cls: datetime.date.today())
    # "yyyymmdd" gives us 8 characters
    length = 8

    def decodeValue(self, value):
        """Return a ``datetime.date`` instance decoded from ``value``."""
        if value.strip():
            return utils.getDate(value)
        else:
            return None

    def encodeValue(self, value):
        """Return a string-encoded value.

        ``value`` argument should be a value suitable for the
        `utils.getDate` call.

        Return:
            Return value is a string in format "yyyymmdd".

        """
        if value:
            return utils.getDate(value).strftime("%Y%m%d")
        else:
            return " " * self.length


class DbfDateTimeFieldDef(DbfFieldDef):
    """Definition of the timestamp field."""

    # a difference between JDN (Julian Day Number)
    # and GDN (Gregorian Day Number). note, that GDN < JDN
    JDN_GDN_DIFF = 1721425
    typeCode = "T"
    defaultValue = utils.classproperty(lambda cls: datetime.datetime.now())
    # two 32-bits integers representing JDN and amount of
    # milliseconds respectively gives us 8 bytes.
    # note, that values must be encoded in LE byteorder.
    length = 8

    def decodeValue(self, value):
        """Return a `datetime.datetime` instance."""
        assert len(value) == self.length
        # LE byteorder
        _jdn, _msecs = struct.unpack("<2I", value)
        if _jdn >= 1:
            _rv = datetime.datetime.fromordinal(_jdn - self.JDN_GDN_DIFF)
            _rv += datetime.timedelta(0, _msecs / 1000.0)
        else:
            # empty date
            _rv = None
        return _rv

    def encodeValue(self, value):
        """Return a string-encoded ``value``."""
        if value:
            value = utils.getDateTime(value)
            # LE byteorder
            _rv = struct.pack("<2I", value.toordinal() + self.JDN_GDN_DIFF,
                              (value.hour * 3600 + value.minute * 60 + value.second) * 1000)
        else:
            _rv = "\0" * self.length
        assert len(_rv) == self.length
        return _rv


_fieldsRegistry = {}


def registerField(fieldCls):
    """Register field definition class.

    ``fieldCls`` should be subclass of the `DbfFieldDef`.

    Use `lookupFor` to retrieve field definition class
    by the type code.

    """
    assert fieldCls.typeCode is not None, "Type code isn't defined"
    # XXX: use fieldCls.typeCode.upper()? in case of any decign
    # don't forget to look to the same comment in ``lookupFor`` method
    _fieldsRegistry[fieldCls.typeCode] = fieldCls


def lookupFor(typeCode):
    """Return field definition class for the given type code.

    ``typeCode`` must be a single character. That type should be
    previously registered.

    Use `registerField` to register new field class.

    Return:
        Return value is a subclass of the `DbfFieldDef`.

    """
    # XXX: use typeCode.upper()? in case of any decign don't
    # forget to look to the same comment in ``registerField``
    return _fieldsRegistry[chr(typeCode)]

# register generic types


for (_name, _val) in list(globals().items()):
    if isinstance(_val, type) and issubclass(_val, DbfFieldDef) \
       and (_name != "DbfFieldDef"):
        __all__.append(_name)
        registerField(_val)
del _name, _val
