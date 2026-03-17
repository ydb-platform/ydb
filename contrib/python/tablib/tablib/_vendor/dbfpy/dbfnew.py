#!/usr/bin/python

__version__ = "$Revision: 1.4 $"[11:-2]
__date__ = "$Date: 2006/07/04 08:18:18 $"[7:-2]

__all__ = ["dbf_new"]

from .dbf import Dbf
from .fields import (
    DbfCharacterFieldDef,
    DbfDateFieldDef,
    DbfDateTimeFieldDef,
    DbfLogicalFieldDef,
    DbfNumericFieldDef,
)
from .header import DbfHeader
from .record import DbfRecord

""".DBF creation helpers.

Note: this is a legacy interface.  New code should use Dbf class
    for table creation (see examples in dbf.py)

TODO:
  - handle Memo fields.
  - check length of the fields according to the
    `https://www.clicketyclick.dk/databases/xbase/format/data_types.html`

"""
"""History (most recent first)
04-jul-2006 [als]   added export declaration;
                    updated for dbfpy 2.0
15-dec-2005 [yc]    define dbf_new.__slots__
14-dec-2005 [yc]    added vim modeline; retab'd; added doc-strings;
                    dbf_new now is a new class (inherited from object)
??-jun-2000 [--]    added by Hans Fiby
"""


class _FieldDefinition:
    """Field definition.

    This is a simple structure, which contains ``name``, ``type``,
    ``len``, ``dec`` and ``cls`` fields.

    Objects also implement get/setitem magic functions, so fields
    could be accessed via sequence interface, where 'name' has
    index 0, 'type' index 1, 'len' index 2, 'dec' index 3 and
    'cls' could be located at index 4.

    """

    __slots__ = "name", "type", "len", "dec", "cls"

    # WARNING: be attentive - dictionaries are mutable!
    FLD_TYPES = {
        # type: (cls, len)
        "C": (DbfCharacterFieldDef, None),
        "N": (DbfNumericFieldDef, None),
        "L": (DbfLogicalFieldDef, 1),
        # FIXME: support memos
        # "M": (DbfMemoFieldDef),
        "D": (DbfDateFieldDef, 8),
        # FIXME: I'm not sure length should be 14 characters!
        # but temporary I use it, cuz date is 8 characters
        # and time 6 (hhmmss)
        "T": (DbfDateTimeFieldDef, 14),
    }

    def __init__(self, name, type, len=None, dec=0):
        _cls, _len = self.FLD_TYPES[type]
        if _len is None:
            if len is None:
                raise ValueError("Field length must be defined")
            _len = len
        self.name = name
        self.type = type
        self.len = _len
        self.dec = dec
        self.cls = _cls

    def getDbfField(self):
        """Return `DbfFieldDef` instance from the current definition."""
        return self.cls(self.name, self.len, self.dec)

    def appendToHeader(self, dbfh):
        """Create a `DbfFieldDef` instance and append it to the dbf header.

        Arguments:
            dbfh: `DbfHeader` instance.

        """
        _dbff = self.getDbfField()
        dbfh.addField(_dbff)


class dbf_new:
    """New .DBF creation helper.

    Example Usage:

        dbfn = dbf_new()
        dbfn.add_field("name",'C',80)
        dbfn.add_field("price",'N',10,2)
        dbfn.add_field("date",'D',8)
        dbfn.write("tst.dbf")

    Note:
        This module cannot handle Memo-fields,
        they are special.

    """

    __slots__ = ("fields",)

    FieldDefinitionClass = _FieldDefinition

    def __init__(self):
        self.fields = []

    def add_field(self, name, typ, len, dec=0):
        """Add field definition.

        Arguments:
            name:
                field name (str object). field name must not
                contain ASCII NULs and it's length shouldn't
                exceed 10 characters.
            typ:
                type of the field. this must be a single character
                from the "CNLMDT" set meaning character, numeric,
                logical, memo, date and date/time respectively.
            len:
                length of the field. this argument is used only for
                the character and numeric fields. all other fields
                have fixed length.
                FIXME: use None as a default for this argument?
            dec:
                decimal precision. used only for the numric fields.

        """
        self.fields.append(self.FieldDefinitionClass(name, typ, len, dec))

    def write(self, filename):
        """Create empty .DBF file using current structure."""
        _dbfh = DbfHeader()
        _dbfh.setCurrentDate()
        for _fldDef in self.fields:
            _fldDef.appendToHeader(_dbfh)

        _dbfStream = open(filename, "wb")
        _dbfh.write(_dbfStream)
        _dbfStream.close()


if __name__ == '__main__':
    # create a new DBF-File
    dbfn = dbf_new()
    dbfn.add_field("name", 'C', 80)
    dbfn.add_field("price", 'N', 10, 2)
    dbfn.add_field("date", 'D', 8)
    dbfn.write("tst.dbf")
    # test new dbf
    print("*** created tst.dbf: ***")
    dbft = Dbf('tst.dbf', readOnly=0)
    print(repr(dbft))
    # add a record
    rec = DbfRecord(dbft)
    rec['name'] = 'something'
    rec['price'] = 10.5
    rec['date'] = (2000, 1, 12)
    rec.store()
    # add another record
    rec = DbfRecord(dbft)
    rec['name'] = 'foo and bar'
    rec['price'] = 12234
    rec['date'] = (1992, 7, 15)
    rec.store()

    # show the records
    print("*** inserted 2 records into tst.dbf: ***")
    print(repr(dbft))
    for i1 in range(len(dbft)):
        rec = dbft[i1]
        for fldName in dbft.fieldNames:
            print(f'{fldName}:\t {rec[fldName]}')
        print()
    dbft.close()
