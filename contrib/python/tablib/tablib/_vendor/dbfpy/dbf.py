#! /usr/bin/env python

from . import header, record
from .utils import INVALID_VALUE

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2007/02/11 09:23:13 $"[7:-2]
__author__ = "Jeff Kunce <kuncej@mail.conservation.state.mo.us>"
__all__ = ["Dbf"]

"""DBF accessing helpers.

FIXME: more documentation needed

Examples:

    Create new table, setup structure, add records:

        dbf = Dbf(filename, new=True)
        dbf.addField(
            ("NAME", "C", 15),
            ("SURNAME", "C", 25),
            ("INITIALS", "C", 10),
            ("BIRTHDATE", "D"),
        )
        for (n, s, i, b) in (
            ("John", "Miller", "YC", (1980, 10, 11)),
            ("Andy", "Larkin", "", (1980, 4, 11)),
        ):
            rec = dbf.newRecord()
            rec["NAME"] = n
            rec["SURNAME"] = s
            rec["INITIALS"] = i
            rec["BIRTHDATE"] = b
            rec.store()
        dbf.close()

    Open existed dbf, read some data:

        dbf = Dbf(filename, True)
        for rec in dbf:
            for fldName in dbf.fieldNames:
                print('%s:\t %s (%s)' % (fldName, rec[fldName],
                    type(rec[fldName])))
            print()
        dbf.close()

"""
"""History (most recent first):
11-feb-2007 [als]   export INVALID_VALUE;
                    Dbf: added .ignoreErrors, .INVALID_VALUE
04-jul-2006 [als]   added export declaration
20-dec-2005 [yc]    removed fromStream and newDbf methods:
                    use argument of __init__ call must be used instead;
                    added class fields pointing to the header and
                    record classes.
17-dec-2005 [yc]    split to several modules; reimplemented
13-dec-2005 [yc]    adapted to the changes of the `strutil` module.
13-sep-2002 [als]   support FoxPro Timestamp datatype
15-nov-1999 [jjk]   documentation updates, add demo
24-aug-1998 [jjk]   add some encodeValue methods (not tested), other tweaks
08-jun-1998 [jjk]   fix problems, add more features
20-feb-1998 [jjk]   fix problems, add more features
19-feb-1998 [jjk]   add create/write capabilities
18-feb-1998 [jjk]   from dbfload.py
"""


class Dbf:
    """DBF accessor.

    FIXME:
        docs and examples needed (dont' forget to tell
        about problems adding new fields on the fly)

    Implementation notes:
        ``_new`` field is used to indicate whether this is
        a new data table. `addField` could be used only for
        the new tables! If at least one record was appended
        to the table it's structure couldn't be changed.

    """

    __slots__ = ("name", "header", "stream",
                 "_changed", "_new", "_ignore_errors")

    HeaderClass = header.DbfHeader
    RecordClass = record.DbfRecord
    INVALID_VALUE = INVALID_VALUE

    # initialization and creation helpers

    def __init__(self, f, readOnly=False, new=False, ignoreErrors=False):
        """Initialize instance.

        Arguments:
            f:
                Filename or file-like object.
            new:
                True if new data table must be created. Assume
                data table exists if this argument is False.
            readOnly:
                if ``f`` argument is a string file will
                be opend in read-only mode; in other cases
                this argument is ignored. This argument is ignored
                even if ``new`` argument is True.
            headerObj:
                `header.DbfHeader` instance or None. If this argument
                is None, new empty header will be used with the
                all fields set by default.
            ignoreErrors:
                if set, failing field value conversion will return
                ``INVALID_VALUE`` instead of raising conversion error.

        """
        if isinstance(f, str):
            # a filename
            self.name = f
            if new:
                # new table (table file must be
                # created or opened and truncated)
                self.stream = open(f, "w+b")
            else:
                # table file must exist
                self.stream = open(f, ("r+b", "rb")[bool(readOnly)])
        else:
            # a stream
            self.name = getattr(f, "name", "")
            self.stream = f
        if new:
            # if this is a new table, header will be empty
            self.header = self.HeaderClass()
        else:
            # or instantiated using stream
            self.header = self.HeaderClass.fromStream(self.stream)
        self.ignoreErrors = ignoreErrors
        self._new = bool(new)
        self._changed = False

    # properties

    closed = property(lambda self: self.stream.closed)
    recordCount = property(lambda self: self.header.recordCount)
    fieldNames = property(
        lambda self: [_fld.name for _fld in self.header.fields])
    fieldDefs = property(lambda self: self.header.fields)
    changed = property(lambda self: self._changed or self.header.changed)

    def ignoreErrors(self, value):
        """Update `ignoreErrors` flag on the header object and self"""
        self.header.ignoreErrors = self._ignore_errors = bool(value)

    ignoreErrors = property(
        lambda self: self._ignore_errors,
        ignoreErrors,
        doc="""Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """)

    # protected methods

    def _fixIndex(self, index):
        """Return fixed index.

        This method fails if index isn't a numeric object
        (long or int). Or index isn't in a valid range
        (less or equal to the number of records in the db).

        If ``index`` is a negative number, it will be
        treated as a negative indexes for list objects.

        Return:
            Return value is numeric object maning valid index.

        """
        if not isinstance(index, int):
            raise TypeError("Index must be a numeric object")
        if index < 0:
            # index from the right side
            # fix it to the left-side index
            index += len(self) + 1
        if index >= len(self):
            raise IndexError("Record index out of range")
        return index

    # interface methods

    def close(self):
        self.flush()
        self.stream.close()

    def flush(self):
        """Flush data to the associated stream."""
        if self.changed:
            self.header.setCurrentDate()
            self.header.write(self.stream)
            self.stream.flush()
            self._changed = False

    def indexOfFieldName(self, name):
        """Index of field named ``name``."""
        # FIXME: move this to header class
        names = [f.name for f in self.header.fields]
        return names.index(name.upper())

    def newRecord(self):
        """Return new record, which belong to this table."""
        return self.RecordClass(self)

    def append(self, record):
        """Append ``record`` to the database."""
        record.index = self.header.recordCount
        record._write()
        self.header.recordCount += 1
        self._changed = True
        self._new = False

    def addField(self, *defs):
        """Add field definitions.

        For more information see `header.DbfHeader.addField`.

        """
        if self._new:
            self.header.addField(*defs)
        else:
            raise TypeError("At least one record was added, "
                            "structure can't be changed")

    # 'magic' methods (representation and sequence interface)

    def __repr__(self):
        return f"Dbf stream '{self.stream}'\n" + repr(self.header)

    def __len__(self):
        """Return number of records."""
        return self.recordCount

    def __getitem__(self, index):
        """Return `DbfRecord` instance."""
        return self.RecordClass.fromStream(self, self._fixIndex(index))

    def __setitem__(self, index, record):
        """Write `DbfRecord` instance to the stream."""
        record.index = self._fixIndex(index)
        record._write()
        self._changed = True
        self._new = False

        # def __del__(self):
        #    """Flush stream upon deletion of the object."""
        #    self.flush()


def demo_read(filename):
    _dbf = Dbf(filename, True)
    for _rec in _dbf:
        print()
        print(repr(_rec))
    _dbf.close()


def demo_create(filename):
    _dbf = Dbf(filename, new=True)
    _dbf.addField(
        ("NAME", "C", 15),
        ("SURNAME", "C", 25),
        ("INITIALS", "C", 10),
        ("BIRTHDATE", "D"),
    )
    for (_n, _s, _i, _b) in (
            ("John", "Miller", "YC", (1981, 1, 2)),
            ("Andy", "Larkin", "AL", (1982, 3, 4)),
            ("Bill", "Clinth", "", (1983, 5, 6)),
            ("Bobb", "McNail", "", (1984, 7, 8)),
    ):
        _rec = _dbf.newRecord()
        _rec["NAME"] = _n
        _rec["SURNAME"] = _s
        _rec["INITIALS"] = _i
        _rec["BIRTHDATE"] = _b
        _rec.store()
    print(repr(_dbf))
    _dbf.close()


if __name__ == '__main__':
    import sys

    _name = len(sys.argv) > 1 and sys.argv[1] or "county.dbf"
    demo_create(_name)
    demo_read(_name)
