import sys

from . import utils

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2007/02/11 09:05:49 $"[7:-2]

__all__ = ["DbfRecord"]


"""DBF record definition.

"""
"""History (most recent first):
11-feb-2007 [als]   __repr__: added special case for invalid field values
10-feb-2007 [als]   added .rawFromStream()
30-oct-2006 [als]   fix record length in .fromStream()
04-jul-2006 [als]   added export declaration
20-dec-2005 [yc]    DbfRecord.write() -> DbfRecord._write();
                    added delete() method.
16-dec-2005 [yc]    record definition moved from `dbf`.
"""


class DbfRecord:
    """DBF record.

    Instances of this class shouldn't be created manually,
    use `dbf.Dbf.newRecord` instead.

    Class implements mapping/sequence interface, so
    fields could be accessed via their names or indexes
    (names is a preferred way to access fields).

    Hint:
        Use `store` method to save modified record.

    Examples:
        Add new record to the database:
            db = Dbf(filename)
            rec = db.newRecord()
            rec["FIELD1"] = value1
            rec["FIELD2"] = value2
            rec.store()
        Or the same, but modify existed
        (second in this case) record:
            db = Dbf(filename)
            rec = db[2]
            rec["FIELD1"] = value1
            rec["FIELD2"] = value2
            rec.store()

    """

    __slots__ = "dbf", "index", "deleted", "fieldData"

    # creation and initialization

    def __init__(self, dbf, index=None, deleted=False, data=None):
        """Instance initialization.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance this record belongs to.
            index:
                An integer record index or None. If this value is
                None, record will be appended to the DBF.
            deleted:
                Boolean flag indicating whether this record
                is a deleted record.
            data:
                A sequence or None. This is a data of the fields.
                If this argument is None, default values will be used.

        """
        self.dbf = dbf
        # XXX: I'm not sure ``index`` is necessary
        self.index = index
        self.deleted = deleted
        if data is None:
            self.fieldData = [_fd.defaultValue for _fd in dbf.header.fields]
        else:
            self.fieldData = list(data)

    # XXX: validate self.index before calculating position?
    position = property(lambda self: self.dbf.header.headerLength +
                        self.index * self.dbf.header.recordLength)

    def rawFromStream(cls, dbf, index):
        """Return raw record contents read from the stream.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance containing the record.
            index:
                Index of the record in the records' container.
                This argument can't be None in this call.

        Return value is a string containing record data in DBF format.

        """
        # XXX: may be write smth assuming, that current stream
        # position is the required one? it could save some
        # time required to calculate where to seek in the file
        dbf.stream.seek(dbf.header.headerLength + index * dbf.header.recordLength)
        return dbf.stream.read(dbf.header.recordLength)

    rawFromStream = classmethod(rawFromStream)

    def fromStream(cls, dbf, index):
        """Return a record read from the stream.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance new record should belong to.
            index:
                Index of the record in the records' container.
                This argument can't be None in this call.

        Return value is an instance of the current class.

        """
        return cls.fromString(dbf, cls.rawFromStream(dbf, index), index)

    fromStream = classmethod(fromStream)

    def fromString(cls, dbf, string, index=None):
        """Return record read from the string object.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance new record should belong to.
            string:
                A string new record should be created from.
            index:
                Index of the record in the container. If this
                argument is None, record will be appended.

        Return value is an instance of the current class.

        """
        return cls(dbf, index, string[0] == "*",
                   [_fd.decodeFromRecord(string) for _fd in dbf.header.fields])

    fromString = classmethod(fromString)

    # object representation

    def __repr__(self):
        _template = "%%%ds: %%s (%%s)" % max(len(_fld) for _fld in self.dbf.fieldNames)  # noqa: UP031
        _rv = []
        for _fld in self.dbf.fieldNames:
            _val = self[_fld]
            if _val is utils.INVALID_VALUE:
                _rv.append(_template % (_fld, "None", "value cannot be decoded"))
            else:
                _rv.append(_template % (_fld, _val, type(_val)))
        return "\n".join(_rv)

    # protected methods

    def _write(self):
        """Write data to the dbf stream.

        Note:
            This isn't a public method, it's better to
            use 'store' instead publicly.
            Be design ``_write`` method should be called
            only from the `Dbf` instance.

        """
        self._validateIndex(False)
        self.dbf.stream.seek(self.position)
        self.dbf.stream.write(bytes(self.toString(),
                              sys.getfilesystemencoding()))
        # FIXME: may be move this write somewhere else?
        # why we should check this condition for each record?
        if self.index == len(self.dbf):
            # this is the last record,
            # we should write SUB (ASCII 26)
            self.dbf.stream.write(b"\x1A")

    # utility methods

    def _validateIndex(self, allowUndefined=True, checkRange=False):
        """Valid ``self.index`` value.

        If ``allowUndefined`` argument is True functions does nothing
        in case of ``self.index`` pointing to None object.

        """
        if self.index is None:
            if not allowUndefined:
                raise ValueError("Index is undefined")
        elif self.index < 0:
            raise ValueError(f"Index can't be negative ({self.index})")
        elif checkRange and self.index <= self.dbf.header.recordCount:
            raise ValueError(
                f"There are only {self.dbf.header.recordCount} records in the DBF"
            )

    # interface methods

    def store(self):
        """Store current record in the DBF.

        If ``self.index`` is None, this record will be appended to the
        records of the DBF this records belongs to; or replaced otherwise.

        """
        self._validateIndex()
        if self.index is None:
            self.index = len(self.dbf)
            self.dbf.append(self)
        else:
            self.dbf[self.index] = self

    def delete(self):
        """Mark method as deleted."""
        self.deleted = True

    def toString(self):
        """Return string packed record values."""
        return "".join([" *"[self.deleted]] + [
           _def.encodeValue(_dat) for (_def, _dat) in zip(self.dbf.header.fields, self.fieldData)
        ])

    def asList(self):
        """Return a flat list of fields.

        Note:
            Change of the list's values won't change
            real values stored in this object.

        """
        return self.fieldData[:]

    def asDict(self):
        """Return a dictionary of fields.

        Note:
            Change of the dicts's values won't change
            real values stored in this object.

        """
        return dict(list(zip(self.dbf.fieldNames, self.fieldData)))

    def __getitem__(self, key):
        """Return value by field name or field index."""
        if isinstance(key, int):
            # integer index of the field
            return self.fieldData[key]
        # assuming string field name
        return self.fieldData[self.dbf.indexOfFieldName(key)]

    def __setitem__(self, key, value):
        """Set field value by integer index of the field or string name."""
        if isinstance(key, int):
            # integer index of the field
            return self.fieldData[key]
        # assuming string field name
        self.fieldData[self.dbf.indexOfFieldName(key)] = value
