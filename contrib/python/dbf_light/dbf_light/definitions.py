# -*- encoding: utf-8 -*-
from __future__ import unicode_literals, division

import struct

from ._base import Definition
from .cast import CAST_MAP
from .utils import bytes_to_int

CODE_PAGES = {
    1: 'cp437',
    2: 'cp850',
    3: 'cp1252',
    87: 'ascii',
    101: 'cp866',
    201: 'cp1251',
}


class Field(Definition):

    _definition = (
        ('name', '11s'),
        ('type', 'c'),
        ('reserved1', '4s'),
        ('len', 'c'),
        ('decimal_count', 'B'),
        ('reserved2', '13s'),
        ('mdx', '?'),  # dBASE IV only
    )

    @classmethod
    def from_file(cls, fileobj, name_lower=True, encoding=None):
        field = super(Field, cls).from_file(fileobj)

        if name_lower:
            field.set_name(field.data['name'].lower())

        field.encoding = encoding

        return field

    def __init__(self, data):
        super(Field, self).__init__(data)
        data = self.data

        data['name'] = data['name'].decode().rstrip('\0')

        self.name = data['name']
        self.len = bytes_to_int(data['len'])
        self.type = data['type']

    def __str__(self):
        return self.name

    def set_name(self, name):
        self.name = self.data['name'] = name

    def cast(self, value):
        return CAST_MAP.get(self.data['type'], lambda field, val: val)(self, value)


class Prolog(Definition):

    cls_field = Field

    _definition = (
        ('y', 'c'),
        ('m', 'c'),
        ('d', 'c'),
        ('records', 'I'),
        ('len_head', 'H'),
        ('len_rec', 'H'),
        ('reserved1', '2s'),
        ('incomplete_tr', '?'),  # dBASE IV
        ('encrypted', '?'),  # dBASE IV
        ('reserved2', '12s'),  # Reserved for multi-user processing.
        ('mdx_exists', '?'),
        ('code_page', 'B'),  # dBASE IV, Visual FoxPro, XBase
        ('reserved3', '2s'),
    )

    def __init__(self, data):
        super(Prolog, self).__init__(data)

        data = self.data

        self.records_count = data['records']

        # +2 -> 1 byte for signature + 1 step
        count = (data['len_head'] - (self._struct_size+2)) / self.cls_field._struct_size

        assert count.is_integer(), 'Unexpected records count. It seems that file format is misinterpreted.'

        self.fields_count = int(count)

    @property
    def encoding(self):
        return CODE_PAGES.get(self.data['code_page'])


class FieldFoxpro(Field):

    _definition = (
        ('name', '32s'),
        ('type', 'c'),
        ('len', 'c'),
        ('decimal_count', 'B'),
        ('reserved1', '2s'),
        ('mdx', '?'),
        ('reserved2', '2s'),
        ('autoinc', 'I'),
        ('reserved2', '4s'),
    )


class PrologFoxpro(Prolog):

    cls_field = FieldFoxpro

    _definition = tuple(
        list(Prolog._definition) +
        [
            ('language_driver', '32s'),
            ('reserved4', '4s'),
        ]
    )


Prolog.init_cache()
Field.init_cache()

PrologFoxpro.init_cache()
FieldFoxpro.init_cache()


def get_format_description(fileobj):
    """

    https://www.loc.gov/preservation/digital/formats/fdd/fdd000325.shtml
    https://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_STRUCT

    http://www.autopark.ru/ASBProgrammerGuide/DBFSTRUC.HTM

    Сигнатура	СУБД	Описание
    3	0x03	00000011	dBASE III, dBASE IV, dBASE 5, dBASE 7, FoxPro, FoxBASE+	Таблица без memo-полей
    4	0x04	00000100	dBASE 7	Таблица без memo-полей
    67	0x43	01000011	dBASE IV, dBASE 5	SQL-таблица dBASE IV без memo-полей
    99	0x63	01100011	dBASE IV, dBASE 5	Системная SQL-таблица dBASE IV без memo-полей
    131	0x83	10000011	dBASE III, FoxBASE+, FoxPro	Таблица с memo-полями .DBT
    139	0x8B	10001011	dBASE IV, dBASE 5	Таблица с memo-полями .DBT формата dBASE IV
    140	0x8C	10001100	dBASE 7	Таблица с memo-полями .DBT формата dBASE IV
    203	0xE5	11100101	SMT	Таблица с memo-полями .SMT
    235	0xEB	11101011	dBASE IV, dBASE 5	Системная SQL-таблица dBASE IV с memo-полями .DBT

    Custom signatures:

    2	0x02	00000010	FoxBASE	Таблица без memo-полей
    48	0x30	00110000	Visual FoxPro	Таблица (признак наличия memo-поля .FPT не предусмотрен )
    49	0x31	00110001	Visual FoxPro	Таблица с автоинкрементными полями
    203	0xCB	11001011	dBASE IV, dBASE 5	SQL-таблица dBASE IV с memo-полями .DBT
    245	0xF5	11110101	FoxPro	Таблица с memo-полями .FPT
    251	0xFB	11111011	FoxBASE	Таблица с memo-полями .???

    :param fileobj:
    :rtype: tuple(Prolog, bytes)

    """
    signature = struct.unpack('<B', fileobj.read(1))[0]

    if signature in {48, 49}:  # pragma: nocover
        # todo return PrologFoxpro when it's debugged (records count error exists).
        raise NotImplementedError('FoxPro dbf support is not implemented.')

    return Prolog, signature
