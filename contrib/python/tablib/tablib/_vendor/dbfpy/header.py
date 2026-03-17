import datetime
import io
import struct
import sys

from . import fields
from .utils import getDate

__version__ = "$Revision: 1.6 $"[11:-2]
__date__ = "$Date: 2010/09/16 05:06:39 $"[7:-2]

__all__ = ["DbfHeader"]


"""DBF header definition.

TODO:
  - handle encoding of the character fields
    (encoding information stored in the DBF header)

"""
"""History (most recent first):
16-sep-2010 [als]   fromStream: fix century of the last update field
11-feb-2007 [als]   added .ignoreErrors
10-feb-2007 [als]   added __getitem__: return field definitions
                    by field name or field number (zero-based)
04-jul-2006 [als]   added export declaration
15-dec-2005 [yc]    created
"""


class DbfHeader:
    """Dbf header definition.

    For more information about dbf header format visit
    `http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_STRUCT`

    Examples:
        Create an empty dbf header and add some field definitions:
            dbfh = DbfHeader()
            dbfh.addField(("name", "C", 10))
            dbfh.addField(("date", "D"))
            dbfh.addField(DbfNumericFieldDef("price", 5, 2))
        Create a dbf header with field definitions:
            dbfh = DbfHeader([
                ("name", "C", 10),
                ("date", "D"),
                DbfNumericFieldDef("price", 5, 2),
            ])

    """

    __slots__ = ("signature", "fields", "lastUpdate", "recordLength",
                 "recordCount", "headerLength", "changed", "_ignore_errors")

    # instance construction and initialization methods

    def __init__(self, fields=None, headerLength=0, recordLength=0,
                 recordCount=0, signature=0x03, lastUpdate=None, ignoreErrors=False):
        """Initialize instance.

        Arguments:
            fields:
                a list of field definitions;
            recordLength:
                size of the records;
            headerLength:
                size of the header;
            recordCount:
                number of records stored in DBF;
            signature:
                version number (aka signature). using 0x03 as a default meaning
                "File without DBT". for more information about this field visit
                ``http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_1_TARGET``
            lastUpdate:
                date of the DBF's update. this could be a string ('yymmdd' or
                'yyyymmdd'), timestamp (int or float), datetime/date value,
                a sequence (assuming (yyyy, mm, dd, ...)) or an object having
                callable ``ticks`` field.
            ignoreErrors:
                error processing mode for DBF fields (boolean)

        """
        self.signature = signature
        if fields is None:
            self.fields = []
        else:
            self.fields = list(fields)
        self.lastUpdate = getDate(lastUpdate)
        self.recordLength = recordLength
        self.headerLength = headerLength
        self.recordCount = recordCount
        self.ignoreErrors = ignoreErrors
        # XXX: I'm not sure this is safe to
        # initialize `self.changed` in this way
        self.changed = bool(self.fields)

    # @classmethod
    def fromString(cls, string):
        """Return header instance from the string object."""
        return cls.fromStream(io.StringIO(str(string)))
    fromString = classmethod(fromString)

    # @classmethod
    def fromStream(cls, stream):
        """Return header object from the stream."""
        stream.seek(0)
        first_32 = stream.read(32)
        if not isinstance(first_32, bytes):
            _data = bytes(first_32, sys.getfilesystemencoding())
        _data = first_32
        (_cnt, _hdrLen, _recLen) = struct.unpack("<I2H", _data[4:12])
        # reserved = _data[12:32]
        _year = _data[1]
        if _year < 80:
            # dBase II started at 1980.  It is quite unlikely
            # that actual last update date is before that year.
            _year += 2000
        else:
            _year += 1900
        # create header object
        _obj = cls(None, _hdrLen, _recLen, _cnt, _data[0],
                   (_year, _data[2], _data[3]))
        # append field definitions
        # position 0 is for the deletion flag
        _pos = 1
        _data = stream.read(1)
        while _data != b'\r':
            _data += stream.read(31)
            _fld = fields.lookupFor(_data[11]).fromString(_data, _pos)
            _obj._addField(_fld)
            _pos = _fld.end
            _data = stream.read(1)
        return _obj
    fromStream = classmethod(fromStream)

    # properties

    year = property(lambda self: self.lastUpdate.year)
    month = property(lambda self: self.lastUpdate.month)
    day = property(lambda self: self.lastUpdate.day)

    def ignoreErrors(self, value):
        """Update `ignoreErrors` flag on self and all fields"""
        self._ignore_errors = value = bool(value)
        for _field in self.fields:
            _field.ignoreErrors = value
    ignoreErrors = property(
        lambda self: self._ignore_errors,
        ignoreErrors,
        doc="""Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """)

    # object representation

    def __repr__(self):
        _rv = """\
Version (signature): 0x%02x
        Last update: %s
      Header length: %d
      Record length: %d
       Record count: %d
 FieldName Type Len Dec
"""
        _rv = _rv % (self.signature, self.lastUpdate, self.headerLength,
                     self.recordLength, self.recordCount)
        _rv += "\n".join(
            ["%10s %4s %3s %3s" % _fld.fieldInfo() for _fld in self.fields]  # noqa: UP031
        )
        return _rv

    # internal methods

    def _addField(self, *defs):
        """Internal variant of the `addField` method.

        This method doesn't set `self.changed` field to True.

        Return value is a length of the appended records.
        Note: this method doesn't modify ``recordLength`` and
        ``headerLength`` fields. Use `addField` instead of this
        method if you don't exactly know what you're doing.

        """
        # insure we have dbf.DbfFieldDef instances first (instantiation
        # from the tuple could raise an error, in such a case I don't
        # wanna add any of the definitions -- all will be ignored)
        _defs = []
        _recordLength = 0
        for _def in defs:
            if isinstance(_def, fields.DbfFieldDef):
                _obj = _def
            else:
                (_name, _type, _len, _dec) = (tuple(_def) + (None,) * 4)[:4]
                _cls = fields.lookupFor(_type)
                _obj = _cls(_name, _len, _dec, ignoreErrors=self._ignore_errors)
            _recordLength += _obj.length
            _defs.append(_obj)
        # and now extend field definitions and
        # update record length
        self.fields += _defs
        return _recordLength

    # interface methods

    def addField(self, *defs):
        """Add field definition to the header.

        Examples:
            dbfh.addField(
                ("name", "C", 20),
                dbf.DbfCharacterFieldDef("surname", 20),
                dbf.DbfDateFieldDef("birthdate"),
                ("member", "L"),
            )
            dbfh.addField(("price", "N", 5, 2))
            dbfh.addField(dbf.DbfNumericFieldDef("origprice", 5, 2))

        """
        _oldLen = self.recordLength
        self.recordLength += self._addField(*defs)
        if not _oldLen:
            self.recordLength += 1
            # XXX: may be just use:
            # self.recordeLength += self._addField(*defs) + bool(not _oldLen)
        # recalculate headerLength
        self.headerLength = 32 + (32 * len(self.fields)) + 1
        self.changed = True

    def write(self, stream):
        """Encode and write header to the stream."""
        stream.seek(0)
        stream.write(self.toString())
        fields = [_fld.toString() for _fld in self.fields]
        stream.write(''.join(fields).encode(sys.getfilesystemencoding()))
        stream.write(b'\x0D')   # cr at end of all header data
        self.changed = False

    def toString(self):
        """Returned 32 chars length string with encoded header."""
        return struct.pack("<4BI2H",
                           self.signature,
                           self.year - 1900,
                           self.month,
                           self.day,
                           self.recordCount,
                           self.headerLength,
                           self.recordLength) + (b'\x00' * 20)
        # TODO: figure out if bytes(utf-8) is correct here.

    def setCurrentDate(self):
        """Update ``self.lastUpdate`` field with current date value."""
        self.lastUpdate = datetime.date.today()

    def __getitem__(self, item):
        """Return a field definition by numeric index or name string"""
        if isinstance(item, str):
            _name = item.upper()
            for _field in self.fields:
                if _field.name == _name:
                    return _field
            else:
                raise KeyError(item)
        else:
            # item must be field index
            return self.fields[item]

# vim: et sts=4 sw=4 :
