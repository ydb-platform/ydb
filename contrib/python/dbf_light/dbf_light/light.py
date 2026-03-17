# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

import struct
from os import path, listdir
from collections import namedtuple
from contextlib import contextmanager
from functools import partial
from zipfile import ZipFile

from .utils import string_types, pick_name
from .definitions import get_format_description, Field
from .exceptions import DbfException


class Dbf(object):
    """Represents data from .dbf file."""

    def __init__(self, fileobj, encoding=None, fieldnames_lower=True):
        """
        :param fileobj: Python file-like object containing .dbf file data.

        :param str|unicode encoding: Encoding used by DB.
            This will be used if there's no encoding information in the DB itself.

        :param bool fieldnames_lower: Lowercase field names.

        """
        self._fileobj = fileobj
        self._lower = fieldnames_lower

        cls_prolog, self.signature = get_format_description(fileobj)

        self.cls_prolog = cls_prolog
        self.cls_field = cls_prolog.cls_field

        prolog = self.cls_prolog.from_file(fileobj)
        self.prolog = prolog

        if encoding is None:
            encoding = prolog.encoding

        self._encoding = encoding or 'cp866'

        self.fields, self.cls_row = self._read_fields()

    def __iter__(self):
        return iter(self.iter_rows())

    @classmethod
    @contextmanager
    def open(cls, dbfile, encoding=None, fieldnames_lower=True, case_sensitive=True):
        """Context manager. Allows opening a .dbf file.

        .. code-block::

            with Dbf.open('some.dbf') as dbf:
                ...

        :param str|unicode|file dbfile: .dbf filepath or a file-like object.

        :param str|unicode encoding: Encoding used by DB.
            This will be used if there's no encoding information in the DB itself.

        :param bool fieldnames_lower: Lowercase field names.

        :param bool case_sensitive: Whether DB filename is case sensitive.

        :rtype: Dbf
        """
        if not case_sensitive:
            if isinstance(dbfile, string_types):
                dbfile = pick_name(dbfile, listdir(path.dirname(dbfile)))

        with open(dbfile, 'rb') as f:
            yield cls(f, encoding=encoding, fieldnames_lower=fieldnames_lower)

    @classmethod
    @contextmanager
    def open_zip(cls, dbname, zipped, encoding=None, fieldnames_lower=True, case_sensitive=True):
        """Context manager. Allows opening a .dbf file from zip archive.

        .. code-block::

            with Dbf.open_zip('some.dbf', 'myarch.zip') as dbf:
                ...

        :param str|unicode dbname: .dbf file name

        :param str|unicode|file zipped: .zip file path or a file-like object.

        :param str|unicode encoding: Encoding used by DB.
            This will be used if there's no encoding information in the DB itself.

        :param bool fieldnames_lower: Lowercase field names.

        :param bool case_sensitive: Whether DB filename is case sensitive.

        :rtype: Dbf
        """
        with ZipFile(zipped, 'r') as zip_:

            if not case_sensitive:
                dbname = pick_name(dbname, zip_.namelist())

            with zip_.open(dbname) as f:
                yield cls(f, encoding=encoding, fieldnames_lower=fieldnames_lower)

    def iter_rows(self):
        """Generator reading .dbf row one by one.

        Yields named tuple Row object.

        :rtype: Row
        """
        fileobj = self._fileobj
        cls_row = self.cls_row
        fields = self.fields

        for idx in range(self.prolog.records_count):
            data = fileobj.read(1)

            marker = struct.unpack('<1s', data)[0]
            is_deleted = marker == b'*'

            if is_deleted:
                continue

            row_values = []
            for field in fields:
                val = field.cast(fileobj.read(field.len))
                row_values.append(val)

            yield cls_row(*row_values)

    def _read_fields(self):
        fh = self._fileobj
        field_from_file = partial(self.cls_field.from_file, name_lower=self._lower, encoding=self._encoding)

        fields = []
        field_names = []
        for idx in range(self.prolog.fields_count):
            field = field_from_file(fh)  # type: Field
            name = field.name

            if name in field_names:
                # Handle duplicates.
                name = name + '_'
                field.set_name(name)

            fields.append(field)
            field_names.append(name)

        terminator = struct.unpack('<c', fh.read(1))[0]

        if terminator != b'\r':
            raise DbfException(
                'Header termination byte not found. '
                'Seems to be an unsupported format. Signature: %s' % self.signature)

        cls_row = namedtuple('Row', field_names)

        return fields, cls_row


@contextmanager
def open_db(db, zipped=None, encoding=None, fieldnames_lower=True, case_sensitive=True):
    """Context manager. Allows reading DBF file (maybe even from zip).

    :param str|unicode|file db: .dbf file name or a file-like object.

    :param str|unicode zipped: .zip file path or a file-like object.

    :param str|unicode encoding: Encoding used by DB.
        This will be used if there's no encoding information in the DB itself.

    :param bool fieldnames_lower: Lowercase field names.

    :param bool case_sensitive: Whether DB filename is case sensitive.

    :rtype: Dbf
    """
    kwargs = dict(
        encoding=encoding,
        fieldnames_lower=fieldnames_lower,
        case_sensitive=case_sensitive,
    )

    if zipped:
        with Dbf.open_zip(db, zipped, **kwargs) as dbf:
            yield dbf

    else:
        with Dbf.open(db, **kwargs) as dbf:
            yield dbf
