"""
This module implements various data types supported by Microsoft SQL Server
"""
from __future__ import annotations

import itertools
import datetime
import decimal
import struct
import re
import uuid
import functools
from io import StringIO, BytesIO
from typing import Callable

from pytds.tds_base import read_chunks
from . import tds_base
from .collate import ucs2_codec, raw_collation
from . import tz


_flt4_struct = struct.Struct("f")
_flt8_struct = struct.Struct("d")
_utc = tz.utc


TzInfoFactoryType = Callable[[int], datetime.tzinfo]


def _applytz(dt, tzinfo):
    if not tzinfo:
        return dt
    dt = dt.replace(tzinfo=tzinfo)
    return dt


def _decode_num(buf):
    """Decodes little-endian integer from buffer

    Buffer can be of any size
    """
    return functools.reduce(
        lambda acc, val: acc * 256 + tds_base.my_ord(val), reversed(buf), 0
    )


class PlpReader(object):
    """Partially length prefixed reader

    Spec: http://msdn.microsoft.com/en-us/library/dd340469.aspx
    """

    def __init__(self, r):
        """
        :param r: An instance of :class:`_TdsReader`
        """
        self._rdr = r
        size = r.get_uint8()
        self._size = size

    def is_null(self):
        """
        :return: True if stored value is NULL
        """
        return self._size == tds_base.PLP_NULL

    def is_unknown_len(self):
        """
        :return: True if total size is unknown upfront
        """
        return self._size == tds_base.PLP_UNKNOWN

    def size(self):
        """
        :return: Total size in bytes if is_uknown_len and is_null are both False
        """
        return self._size

    def chunks(self):
        """Generates chunks from stream, each chunk is an instace of bytes."""
        if self.is_null():
            return
        total = 0
        while True:
            chunk_len = self._rdr.get_uint()
            if chunk_len == 0:
                if not self.is_unknown_len() and total != self._size:
                    msg = (
                        "PLP actual length (%d) doesn't match reported length (%d)"
                        % (total, self._size)
                    )
                    self._rdr.session.bad_stream(msg)

                return

            total += chunk_len
            left = chunk_len
            while left:
                buf = self._rdr.recv(left)
                yield buf
                left -= len(buf)


class _StreamChunkedHandler(object):
    def __init__(self, stream):
        self.stream = stream

    def add_chunk(self, val):
        self.stream.write(val)

    def end(self):
        return self.stream


class _DefaultChunkedHandler(object):
    def __init__(self, stream):
        self.stream = stream

    def add_chunk(self, val):
        self.stream.write(val)

    def end(self):
        value = self.stream.getvalue()
        self.stream.seek(0)
        self.stream.truncate()
        return value

    def __eq__(self, other):
        return self.stream.getvalue() == other.stream.getvalue()

    def __ne__(self, other):
        return not self.__eq__(other)


class SqlTypeMetaclass(tds_base.CommonEqualityMixin):
    def __repr__(self):
        return "<sqltype:{}>".format(self.get_declaration())

    def get_declaration(self):
        raise NotImplementedError()


class ImageType(SqlTypeMetaclass):
    def get_declaration(self):
        return "IMAGE"


class BinaryType(SqlTypeMetaclass):
    def __init__(self, size=30):
        self._size = size

    @property
    def size(self):
        return self._size

    def get_declaration(self):
        return "BINARY({})".format(self._size)


class VarBinaryType(SqlTypeMetaclass):
    def __init__(self, size=30):
        self._size = size

    @property
    def size(self):
        return self._size

    def get_declaration(self):
        return "VARBINARY({})".format(self._size)


class VarBinaryMaxType(SqlTypeMetaclass):
    def get_declaration(self):
        return "VARBINARY(MAX)"


class CharType(SqlTypeMetaclass):
    def __init__(self, size=30):
        self._size = size

    @property
    def size(self):
        return self._size

    def get_declaration(self):
        return "CHAR({})".format(self._size)


class VarCharType(SqlTypeMetaclass):
    def __init__(self, size=30):
        self._size = size

    @property
    def size(self):
        return self._size

    def get_declaration(self):
        return "VARCHAR({})".format(self._size)


class VarCharMaxType(SqlTypeMetaclass):
    def get_declaration(self):
        return "VARCHAR(MAX)"


class NCharType(SqlTypeMetaclass):
    def __init__(self, size=30):
        self._size = size

    @property
    def size(self):
        return self._size

    def get_declaration(self):
        return "NCHAR({})".format(self._size)


class NVarCharType(SqlTypeMetaclass):
    def __init__(self, size=30):
        self._size = size

    @property
    def size(self):
        return self._size

    def get_declaration(self):
        return "NVARCHAR({})".format(self._size)


class NVarCharMaxType(SqlTypeMetaclass):
    def get_declaration(self):
        return "NVARCHAR(MAX)"


class TextType(SqlTypeMetaclass):
    def get_declaration(self):
        return "TEXT"


class NTextType(SqlTypeMetaclass):
    def get_declaration(self):
        return "NTEXT"


class XmlType(SqlTypeMetaclass):
    def get_declaration(self):
        return "XML"


class SmallMoneyType(SqlTypeMetaclass):
    def get_declaration(self):
        return "SMALLMONEY"


class MoneyType(SqlTypeMetaclass):
    def get_declaration(self):
        return "MONEY"


class DecimalType(SqlTypeMetaclass):
    def __init__(self, precision=18, scale=0):
        self._precision = precision
        self._scale = scale

    @classmethod
    def from_value(cls, value):
        if not (-(10**38) + 1 <= value <= 10**38 - 1):
            raise tds_base.DataError("Decimal value is out of range")
        with decimal.localcontext() as context:
            context.prec = 38
            value = value.normalize()
            _, digits, exp = value.as_tuple()
            if exp > 0:
                scale = 0
                prec = len(digits) + exp
            else:
                scale = -exp
                prec = max(len(digits), scale)
            return cls(precision=prec, scale=scale)

    @property
    def precision(self):
        return self._precision

    @property
    def scale(self):
        return self._scale

    def get_declaration(self):
        return "DECIMAL({}, {})".format(self._precision, self._scale)


class UniqueIdentifierType(SqlTypeMetaclass):
    def get_declaration(self):
        return "UNIQUEIDENTIFIER"


class VariantType(SqlTypeMetaclass):
    def get_declaration(self):
        return "SQL_VARIANT"


class SqlValueMetaclass(tds_base.CommonEqualityMixin):
    pass


class BaseTypeSerializer(tds_base.CommonEqualityMixin):
    """Base type for TDS data types.

    All TDS types should derive from it.
    In addition actual types should provide the following:

    - type - class variable storing type identifier
    """

    type = 0

    def __init__(self, precision=None, scale=None, size=None):
        self._precision = precision
        self._scale = scale
        self._size = size

    @property
    def precision(self):
        return self._precision

    @property
    def scale(self):
        return self._scale

    @property
    def size(self):
        return self._size

    def get_typeid(self):
        """Returns type identifier of type."""
        return self.type

    @classmethod
    def from_stream(cls, r):
        """Class method that reads and returns a type instance.

        :param r: An instance of :class:`_TdsReader` to read type from.

        Should be implemented in actual types.
        """
        raise NotImplementedError

    def write_info(self, w):
        """Writes type info into w stream.

        :param w: An instance of :class:`_TdsWriter` to write into.

        Should be symmetrical to from_stream method.
        Should be implemented in actual types.
        """
        raise NotImplementedError

    def write(self, w, value):
        """Writes type's value into stream

        :param w: An instance of :class:`_TdsWriter` to write into.
        :param value: A value to be stored, should be compatible with the type

        Should be implemented in actual types.
        """
        raise NotImplementedError

    def read(self, r):
        """Reads value from the stream.

        :param r: An instance of :class:`_TdsReader` to read value from.
        :return: A read value.

        Should be implemented in actual types.
        """
        raise NotImplementedError

    def set_chunk_handler(self, chunk_handler):
        raise ValueError("Column type does not support chunk handler")


class BasePrimitiveTypeSerializer(BaseTypeSerializer):
    """Base type for primitive TDS data types.

    Primitive type is a fixed size type with no type arguments.
    All primitive TDS types should derive from it.
    In addition actual types should provide the following:

    - type - class variable storing type identifier
    - declaration - class variable storing name of sql type
    - isntance - class variable storing instance of class
    """

    def write(self, w, value):
        raise NotImplementedError

    def read(self, r):
        raise NotImplementedError

    instance: BaseTypeSerializer | None = None

    @classmethod
    def from_stream(cls, r):
        return cls.instance

    def write_info(self, w):
        pass


class BaseTypeSerializerN(BaseTypeSerializer):
    """Base type for nullable TDS data types.

    All nullable TDS types should derive from it.
    In addition actual types should provide the following:

    - type - class variable storing type identifier
    - subtypes - class variable storing dict {subtype_size: subtype_instance}
    """

    subtypes: dict[int, BaseTypeSerializer] = {}

    def __init__(self, size):
        super(BaseTypeSerializerN, self).__init__(size=size)
        assert size in self.subtypes
        self._current_subtype = self.subtypes[size]

    def get_typeid(self):
        return self._current_subtype.get_typeid()

    @classmethod
    def from_stream(cls, r):
        size = r.get_byte()
        if size not in cls.subtypes:
            raise tds_base.InterfaceError("Invalid %s size" % cls.type, size)
        return cls(size)

    def write_info(self, w):
        w.put_byte(self.size)

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        if size not in self.subtypes:
            raise r.session.bad_stream("Invalid %s size" % self.type, size)
        return self.subtypes[size].read(r)

    def write(self, w, val):
        if val is None:
            w.put_byte(0)
            return
        w.put_byte(self.size)
        self._current_subtype.write(w, val)


class BitType(SqlTypeMetaclass):
    type = tds_base.SYBBITN

    def get_declaration(self):
        return "BIT"


class TinyIntType(SqlTypeMetaclass):
    type = tds_base.SYBINTN
    size = 1

    def get_declaration(self):
        return "TINYINT"


class SmallIntType(SqlTypeMetaclass):
    type = tds_base.SYBINTN
    size = 2

    def get_declaration(self):
        return "SMALLINT"


class IntType(SqlTypeMetaclass):
    """
    Integer type, corresponds to `INT <https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql>`_
    type in the MSSQL server.
    """

    type = tds_base.SYBINTN
    size = 4

    def get_declaration(self):
        return "INT"


class BigIntType(SqlTypeMetaclass):
    type = tds_base.SYBINTN
    size = 8

    def get_declaration(self):
        return "BIGINT"


class RealType(SqlTypeMetaclass):
    def get_declaration(self):
        return "REAL"


class FloatType(SqlTypeMetaclass):
    def get_declaration(self):
        return "FLOAT"


class BitSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBBIT
    declaration = "BIT"

    def write(self, w, value):
        w.put_byte(1 if value else 0)

    def read(self, r):
        return bool(r.get_byte())


BitSerializer.instance = bit_serializer = BitSerializer()


class BitNSerializer(BaseTypeSerializerN):
    type = tds_base.SYBBITN
    subtypes = {1: bit_serializer}

    def __init__(self, typ):
        super(BitNSerializer, self).__init__(size=1)
        self._typ = typ

    def __repr__(self):
        return "BitNSerializer({})".format(self._typ)


# BitNSerializer.instance = BitNSerializer(BitType())


class TinyIntSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBINT1
    declaration = "TINYINT"

    def write(self, w, val):
        w.put_byte(val)

    def read(self, r):
        return r.get_byte()


TinyIntSerializer.instance = tiny_int_serializer = TinyIntSerializer()


class SmallIntSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBINT2
    declaration = "SMALLINT"

    def write(self, w, val):
        w.put_smallint(val)

    def read(self, r):
        return r.get_smallint()


SmallIntSerializer.instance = small_int_serializer = SmallIntSerializer()


class IntSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBINT4
    declaration = "INT"

    def write(self, w, val):
        w.put_int(val)

    def read(self, r):
        return r.get_int()


IntSerializer.instance = int_serializer = IntSerializer()


class BigIntSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBINT8
    declaration = "BIGINT"

    def write(self, w, val):
        w.put_int8(val)

    def read(self, r):
        return r.get_int8()


BigIntSerializer.instance = big_int_serializer = BigIntSerializer()


class IntNSerializer(BaseTypeSerializerN):
    type = tds_base.SYBINTN

    subtypes = {
        1: tiny_int_serializer,
        2: small_int_serializer,
        4: int_serializer,
        8: big_int_serializer,
    }

    type_by_size = {
        1: TinyIntType(),
        2: SmallIntType(),
        4: IntType(),
        8: BigIntType(),
    }

    def __init__(self, typ):
        super(IntNSerializer, self).__init__(size=typ.size)
        self._typ = typ

    @classmethod
    def from_stream(cls, r):
        size = r.get_byte()
        if size not in cls.subtypes:
            raise tds_base.InterfaceError("Invalid %s size" % cls.type, size)
        return cls(cls.type_by_size[size])

    def __repr__(self):
        return "IntN({})".format(self.size)


class RealSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBREAL
    declaration = "REAL"

    def write(self, w, val):
        w.pack(_flt4_struct, val)

    def read(self, r):
        return r.unpack(_flt4_struct)[0]


RealSerializer.instance = real_serializer = RealSerializer()


class FloatSerializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBFLT8
    declaration = "FLOAT"

    def write(self, w, val):
        w.pack(_flt8_struct, val)

    def read(self, r):
        return r.unpack(_flt8_struct)[0]


FloatSerializer.instance = float_serializer = FloatSerializer()


class FloatNSerializer(BaseTypeSerializerN):
    type = tds_base.SYBFLTN

    subtypes = {
        4: real_serializer,
        8: float_serializer,
    }


class VarChar(SqlValueMetaclass):
    def __init__(self, val, collation=raw_collation):
        self._val = val
        self._collation = collation

    @property
    def collation(self):
        return self._collation

    @property
    def val(self):
        return self._val

    def __str__(self):
        return self._val


class VarChar70Serializer(BaseTypeSerializer):
    type = tds_base.XSYBVARCHAR

    def __init__(self, size, collation=raw_collation, codec=None):
        super(VarChar70Serializer, self).__init__(size=size)
        self._collation = collation
        if codec:
            self._codec = codec
        else:
            self._codec = collation.get_codec()

    @classmethod
    def from_stream(cls, r):
        size = r.get_smallint()
        return cls(size, codec=r.session.conn.server_codec)

    def write_info(self, w):
        w.put_smallint(self.size)

    def write(self, w, val):
        if val is None:
            w.put_smallint(-1)
        else:
            if w._tds._tds._login.bytes_to_unicode:
                val = tds_base.force_unicode(val)
            if isinstance(val, str):
                val, _ = self._codec.encode(val)
            w.put_smallint(len(val))
            w.write(val)

    def read(self, r):
        size = r.get_smallint()
        if size < 0:
            return None
        if r._session._tds._login.bytes_to_unicode:
            return r.read_str(size, self._codec)
        else:
            return tds_base.readall(r, size)


class VarChar71Serializer(VarChar70Serializer):
    @classmethod
    def from_stream(cls, r):
        size = r.get_smallint()
        collation = r.get_collation()
        return cls(size, collation)

    def write_info(self, w):
        super(VarChar71Serializer, self).write_info(w)
        w.put_collation(self._collation)


class VarChar72Serializer(VarChar71Serializer):
    @classmethod
    def from_stream(cls, r):
        size = r.get_usmallint()
        collation = r.get_collation()
        if size == 0xFFFF:
            return VarCharMaxSerializer(collation)
        return cls(size, collation)


class VarCharMaxSerializer(VarChar72Serializer):
    def __init__(self, collation=raw_collation):
        super(VarChar72Serializer, self).__init__(0, collation)
        self._chunk_handler = None

    def write_info(self, w):
        w.put_usmallint(tds_base.PLP_MARKER)
        w.put_collation(self._collation)

    def write(self, w, val):
        if val is None:
            w.put_uint8(tds_base.PLP_NULL)
        else:
            if w._tds._tds._login.bytes_to_unicode:
                val = tds_base.force_unicode(val)
            if isinstance(val, str):
                val, _ = self._codec.encode(val)

            # Putting the actual length here causes an error when bulk inserting:
            #
            # While reading current row from host, a premature end-of-message
            # was encountered--an incoming data stream was interrupted when
            # the server expected to see more data. The host program may have
            # terminated. Ensure that you are using a supported client
            # application programming interface (API).
            #
            # See https://github.com/tediousjs/tedious/issues/197
            # It is not known why this happens, but Microsoft's bcp tool
            # uses PLP_UNKNOWN for varchar(max) as well.
            w.put_uint8(tds_base.PLP_UNKNOWN)
            if len(val) > 0:
                w.put_uint(len(val))
                w.write(val)
            w.put_uint(0)

    def read(self, r):
        login = r._session._tds._login
        r = PlpReader(r)
        if r.is_null():
            return None
        if self._chunk_handler is None:
            if login.bytes_to_unicode:
                self._chunk_handler = _DefaultChunkedHandler(StringIO())
            else:
                self._chunk_handler = _DefaultChunkedHandler(BytesIO())
        if login.bytes_to_unicode:
            for chunk in tds_base.iterdecode(r.chunks(), self._codec):
                self._chunk_handler.add_chunk(chunk)
        else:
            for chunk in r.chunks():
                self._chunk_handler.add_chunk(chunk)
        return self._chunk_handler.end()

    def set_chunk_handler(self, chunk_handler):
        self._chunk_handler = chunk_handler


class NVarChar70Serializer(BaseTypeSerializer):
    type = tds_base.XSYBNVARCHAR

    def __init__(self, size, collation=raw_collation):
        super(NVarChar70Serializer, self).__init__(size=size)
        self._collation = collation

    @classmethod
    def from_stream(cls, r):
        size = r.get_usmallint()
        return cls(size / 2)

    def write_info(self, w):
        w.put_usmallint(self.size * 2)

    def write(self, w, val):
        if val is None:
            w.put_usmallint(0xFFFF)
        else:
            if isinstance(val, bytes):
                val = tds_base.force_unicode(val)
            buf, _ = ucs2_codec.encode(val)
            length = len(buf)
            w.put_usmallint(length)
            w.write(buf)

    def read(self, r):
        size = r.get_usmallint()
        if size == 0xFFFF:
            return None
        return r.read_str(size, ucs2_codec)


class NVarChar71Serializer(NVarChar70Serializer):
    @classmethod
    def from_stream(cls, r):
        size = r.get_usmallint()
        collation = r.get_collation()
        return cls(size / 2, collation)

    def write_info(self, w):
        super(NVarChar71Serializer, self).write_info(w)
        w.put_collation(self._collation)


class NVarChar72Serializer(NVarChar71Serializer):
    @classmethod
    def from_stream(cls, r):
        size = r.get_usmallint()
        collation = r.get_collation()
        if size == 0xFFFF:
            return NVarCharMaxSerializer(collation=collation)
        return cls(size / 2, collation=collation)


class NVarCharMaxSerializer(NVarChar72Serializer):
    def __init__(self, collation=raw_collation):
        super(NVarCharMaxSerializer, self).__init__(size=-1, collation=collation)
        self._chunk_handler = _DefaultChunkedHandler(StringIO())

    def __repr__(self):
        return "NVarCharMax(s={},c={})".format(self.size, repr(self._collation))

    def get_typeid(self):
        return tds_base.SYBNTEXT

    def write_info(self, w):
        w.put_usmallint(tds_base.PLP_MARKER)
        w.put_collation(self._collation)

    def write(self, w, val):
        if val is None:
            w.put_uint8(tds_base.PLP_NULL)
        else:
            if isinstance(val, bytes):
                val = tds_base.force_unicode(val)
            val, _ = ucs2_codec.encode(val)

            # Putting the actual length here causes an error when bulk inserting:
            #
            # While reading current row from host, a premature end-of-message
            # was encountered--an incoming data stream was interrupted when
            # the server expected to see more data. The host program may have
            # terminated. Ensure that you are using a supported client
            # application programming interface (API).
            #
            # See https://github.com/tediousjs/tedious/issues/197
            # It is not known why this happens, but Microsoft's bcp tool
            # uses PLP_UNKNOWN for nvarchar(max) as well.
            w.put_uint8(tds_base.PLP_UNKNOWN)
            if len(val) > 0:
                w.put_uint(len(val))
                w.write(val)
            w.put_uint(0)

    def read(self, r):
        r = PlpReader(r)
        if r.is_null():
            return None
        for chunk in tds_base.iterdecode(r.chunks(), ucs2_codec):
            self._chunk_handler.add_chunk(chunk)
        return self._chunk_handler.end()

    def set_chunk_handler(self, chunk_handler):
        self._chunk_handler = chunk_handler


class XmlSerializer(NVarCharMaxSerializer):
    type = tds_base.SYBMSXML
    declaration = "XML"

    def __init__(self, schema=None):
        super(XmlSerializer, self).__init__(0)
        self._schema = schema or {}

    def __repr__(self):
        return "XmlSerializer(schema={})".format(repr(self._schema))

    def get_typeid(self):
        return self.type

    @classmethod
    def from_stream(cls, r):
        has_schema = r.get_byte()
        schema = {}
        if has_schema:
            schema["dbname"] = r.read_ucs2(r.get_byte())
            schema["owner"] = r.read_ucs2(r.get_byte())
            schema["collection"] = r.read_ucs2(r.get_smallint())
        return cls(schema)

    def write_info(self, w):
        if self._schema:
            w.put_byte(1)
            w.put_byte(len(self._schema["dbname"]))
            w.write_ucs2(self._schema["dbname"])
            w.put_byte(len(self._schema["owner"]))
            w.write_ucs2(self._schema["owner"])
            w.put_usmallint(len(self._schema["collection"]))
            w.write_ucs2(self._schema["collection"])
        else:
            w.put_byte(0)


class Text70Serializer(BaseTypeSerializer):
    type = tds_base.SYBTEXT
    declaration = "TEXT"

    def __init__(self, size=0, table_name="", collation=raw_collation, codec=None):
        super(Text70Serializer, self).__init__(size=size)
        self._table_name = table_name
        self._collation = collation
        if codec:
            self._codec = codec
        else:
            self._codec = collation.get_codec()
        self._chunk_handler = None

    def __repr__(self):
        return "Text70(size={},table_name={},codec={})".format(
            self.size, self._table_name, self._codec
        )

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        table_name = r.read_ucs2(r.get_smallint())
        return cls(size, table_name, codec=r.session.conn.server_codec)

    def write_info(self, w):
        w.put_int(self.size)

    def write(self, w, val):
        if val is None:
            w.put_int(-1)
        else:
            if w._tds._tds._login.bytes_to_unicode:
                val = tds_base.force_unicode(val)
            if isinstance(val, str):
                val, _ = self._codec.encode(val)
            w.put_int(len(val))
            w.write(val)

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        tds_base.readall(r, size)  # textptr
        tds_base.readall(r, 8)  # timestamp
        colsize = r.get_int()
        if self._chunk_handler is None:
            if r._session._tds._login.bytes_to_unicode:
                self._chunk_handler = _DefaultChunkedHandler(StringIO())
            else:
                self._chunk_handler = _DefaultChunkedHandler(BytesIO())
        if r._session._tds._login.bytes_to_unicode:
            for chunk in tds_base.iterdecode(read_chunks(r, colsize), self._codec):
                self._chunk_handler.add_chunk(chunk)
        else:
            for chunk in read_chunks(r, colsize):
                self._chunk_handler.add_chunk(chunk)
        return self._chunk_handler.end()

    def set_chunk_handler(self, chunk_handler):
        self._chunk_handler = chunk_handler


class Text71Serializer(Text70Serializer):
    def __repr__(self):
        return "Text71(size={}, table_name={}, collation={})".format(
            self.size, self._table_name, repr(self._collation)
        )

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        collation = r.get_collation()
        table_name = r.read_ucs2(r.get_smallint())
        return cls(size, table_name, collation)

    def write_info(self, w):
        w.put_int(self.size)
        w.put_collation(self._collation)


class Text72Serializer(Text71Serializer):
    def __init__(self, size=0, table_name_parts=(), collation=raw_collation):
        super(Text72Serializer, self).__init__(
            size=size, table_name=".".join(table_name_parts), collation=collation
        )
        self._table_name_parts = table_name_parts

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        collation = r.get_collation()
        num_parts = r.get_byte()
        parts = []
        for _ in range(num_parts):
            parts.append(r.read_ucs2(r.get_smallint()))
        return cls(size, parts, collation)


class NText70Serializer(BaseTypeSerializer):
    type = tds_base.SYBNTEXT
    declaration = "NTEXT"

    def __init__(self, size=0, table_name="", collation=raw_collation):
        super(NText70Serializer, self).__init__(size=size)
        self._collation = collation
        self._table_name = table_name
        self._chunk_handler = _DefaultChunkedHandler(StringIO())

    def __repr__(self):
        return "NText70(size={}, table_name={})".format(self.size, self._table_name)

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        table_name = r.read_ucs2(r.get_smallint())
        return cls(size, table_name)

    def read(self, r):
        textptr_size = r.get_byte()
        if textptr_size == 0:
            return None
        tds_base.readall(r, textptr_size)  # textptr
        tds_base.readall(r, 8)  # timestamp
        colsize = r.get_int()
        for chunk in tds_base.iterdecode(read_chunks(r, colsize), ucs2_codec):
            self._chunk_handler.add_chunk(chunk)
        return self._chunk_handler.end()

    def write_info(self, w):
        w.put_int(self.size * 2)

    def write(self, w, val):
        if val is None:
            w.put_int(-1)
        else:
            w.put_int(len(val) * 2)
            w.write_ucs2(val)

    def set_chunk_handler(self, chunk_handler):
        self._chunk_handler = chunk_handler


class NText71Serializer(NText70Serializer):
    def __repr__(self):
        return "NText71(size={}, table_name={}, collation={})".format(
            self.size, self._table_name, repr(self._collation)
        )

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        collation = r.get_collation()
        table_name = r.read_ucs2(r.get_smallint())
        return cls(size, table_name, collation)

    def write_info(self, w):
        w.put_int(self.size)
        w.put_collation(self._collation)


class NText72Serializer(NText71Serializer):
    def __init__(self, size=0, table_name_parts=(), collation=raw_collation):
        super(NText72Serializer, self).__init__(size=size, collation=collation)
        self._table_name_parts = table_name_parts

    def __repr__(self):
        return "NText72Serializer(s={},table_name={},coll={})".format(
            self.size, self._table_name_parts, self._collation
        )

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        collation = r.get_collation()
        num_parts = r.get_byte()
        parts = []
        for _ in range(num_parts):
            parts.append(r.read_ucs2(r.get_smallint()))
        return cls(size, parts, collation)


class Binary(bytes, SqlValueMetaclass):
    def __repr__(self):
        return "Binary({0})".format(super(Binary, self).__repr__())


class VarBinarySerializer(BaseTypeSerializer):
    type = tds_base.XSYBVARBINARY

    def __init__(self, size):
        super(VarBinarySerializer, self).__init__(size=size)

    def __repr__(self):
        return "VarBinary({})".format(self.size)

    @classmethod
    def from_stream(cls, r):
        size = r.get_usmallint()
        return cls(size)

    def write_info(self, w):
        w.put_usmallint(self.size)

    def write(self, w, val):
        if val is None:
            w.put_usmallint(0xFFFF)
        else:
            w.put_usmallint(len(val))
            w.write(val)

    def read(self, r):
        size = r.get_usmallint()
        if size == 0xFFFF:
            return None
        return tds_base.readall(r, size)


class VarBinarySerializer72(VarBinarySerializer):
    def __repr__(self):
        return "VarBinary72({})".format(self.size)

    @classmethod
    def from_stream(cls, r):
        size = r.get_usmallint()
        if size == 0xFFFF:
            return VarBinarySerializerMax()
        return cls(size)


class VarBinarySerializerMax(VarBinarySerializer):
    def __init__(self):
        super(VarBinarySerializerMax, self).__init__(0)
        self._chunk_handler = _DefaultChunkedHandler(BytesIO())

    def __repr__(self):
        return "VarBinaryMax()"

    def write_info(self, w):
        w.put_usmallint(tds_base.PLP_MARKER)

    def write(self, w, val):
        if val is None:
            w.put_uint8(tds_base.PLP_NULL)
        else:
            w.put_uint8(len(val))
            if val:
                w.put_uint(len(val))
                w.write(val)
            w.put_uint(0)

    def read(self, r):
        r = PlpReader(r)
        if r.is_null():
            return None
        for chunk in r.chunks():
            self._chunk_handler.add_chunk(chunk)
        return self._chunk_handler.end()

    def set_chunk_handler(self, chunk_handler):
        self._chunk_handler = chunk_handler


class UDT72Serializer(BaseTypeSerializer):
    # Data type definition stream used for UDT_INFO in TYPE_INFO
    # https://msdn.microsoft.com/en-us/library/a57df60e-d0a6-4e7e-a2e5-ccacd277c673/
    def __init__(
        self, max_byte_size, db_name, schema_name, type_name, assembly_qualified_name
    ):
        self.max_byte_size = max_byte_size
        self.db_name = db_name
        self.schema_name = schema_name
        self.type_name = type_name
        self.assembly_qualified_name = assembly_qualified_name
        super(UDT72Serializer, self).__init__()

    def __repr__(self):
        return (
            "UDT72Serializer(max_byte_size={}, db_name={}, "
            "schema_name={}, type_name={}, "
            "assembly_qualified_name={})".format(
                *map(
                    repr,
                    (
                        self.max_byte_size,
                        self.db_name,
                        self.schema_name,
                        self.type_name,
                        self.assembly_qualified_name,
                    ),
                )
            )
        )

    @classmethod
    def from_stream(cls, r):
        # MAX_BYTE_SIZE
        max_byte_size = r.get_usmallint()
        assert max_byte_size == 0xFFFF or 1 < max_byte_size < 8000
        # DB_NAME -- B_VARCHAR
        db_name = r.read_ucs2(r.get_byte())
        # SCHEMA_NAME -- B_VARCHAR
        schema_name = r.read_ucs2(r.get_byte())
        # TYPE_NAME -- B_VARCHAR
        type_name = r.read_ucs2(r.get_byte())
        # UDT_METADATA --
        # a US_VARCHAR (2 bytes length prefix)
        # containing ASSEMBLY_QUALIFIED_NAME
        assembly_qualified_name = r.read_ucs2(r.get_smallint())
        return cls(
            max_byte_size, db_name, schema_name, type_name, assembly_qualified_name
        )

    def read(self, r):
        r = PlpReader(r)
        if r.is_null():
            return None
        return b"".join(r.chunks())


class UDT72SerializerMax(UDT72Serializer):
    def __init__(self, *args, **kwargs):
        super(UDT72SerializerMax, self).__init__(0, *args, **kwargs)


class Image70Serializer(BaseTypeSerializer):
    type = tds_base.SYBIMAGE
    declaration = "IMAGE"

    def __init__(self, size=0, table_name=""):
        super(Image70Serializer, self).__init__(size=size)
        self._table_name = table_name
        self._chunk_handler = _DefaultChunkedHandler(BytesIO())

    def __repr__(self):
        return "Image70(tn={},s={})".format(repr(self._table_name), self.size)

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        table_name = r.read_ucs2(r.get_smallint())
        return cls(size, table_name)

    def read(self, r):
        size = r.get_byte()
        if size == 16:  # Jeff's hack
            tds_base.readall(r, 16)  # textptr
            tds_base.readall(r, 8)  # timestamp
            colsize = r.get_int()
            for chunk in read_chunks(r, colsize):
                self._chunk_handler.add_chunk(chunk)
            return self._chunk_handler.end()
        else:
            return None

    def write(self, w, val):
        if val is None:
            w.put_int(-1)
            return
        w.put_int(len(val))
        w.write(val)

    def write_info(self, w):
        w.put_int(self.size)

    def set_chunk_handler(self, chunk_handler):
        self._chunk_handler = chunk_handler


class Image72Serializer(Image70Serializer):
    def __init__(self, size=0, parts=()):
        super(Image72Serializer, self).__init__(size=size, table_name=".".join(parts))
        self._parts = parts

    def __repr__(self):
        return "Image72(p={},s={})".format(self._parts, self.size)

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        num_parts = r.get_byte()
        parts = []
        for _ in range(num_parts):
            parts.append(r.read_ucs2(r.get_usmallint()))
        return Image72Serializer(size, parts)


_datetime_base_date = datetime.datetime(1900, 1, 1)


class SmallDateTimeType(SqlTypeMetaclass):
    def get_declaration(self):
        return "SMALLDATETIME"


class DateTimeType(SqlTypeMetaclass):
    def get_declaration(self):
        return "DATETIME"


class SmallDateTime(SqlValueMetaclass):
    """Corresponds to MSSQL smalldatetime"""

    def __init__(self, days, minutes):
        """

        @param days: Days since 1900-01-01
        @param minutes: Minutes since 00:00:00
        """
        self._days = days
        self._minutes = minutes

    @property
    def days(self):
        return self._days

    @property
    def minutes(self):
        return self._minutes

    def to_pydatetime(self):
        return _datetime_base_date + datetime.timedelta(
            days=self._days, minutes=self._minutes
        )

    @classmethod
    def from_pydatetime(cls, dt):
        days = (dt - _datetime_base_date).days
        minutes = dt.hour * 60 + dt.minute
        return cls(days=days, minutes=minutes)


class BaseDateTimeSerializer(BaseTypeSerializer):
    def write(self, w, value):
        raise NotImplementedError

    def write_info(self, w):
        raise NotImplementedError

    def read(self, r):
        raise NotImplementedError

    @classmethod
    def from_stream(cls, r):
        raise NotImplementedError


class SmallDateTimeSerializer(BasePrimitiveTypeSerializer, BaseDateTimeSerializer):
    type = tds_base.SYBDATETIME4
    declaration = "SMALLDATETIME"

    _struct = struct.Struct("<HH")

    def write(self, w, val):
        if val.tzinfo:
            if not w.session.use_tz:
                raise tds_base.DataError(
                    "Timezone-aware datetime is used without specifying use_tz"
                )
            val = val.astimezone(w.session.use_tz).replace(tzinfo=None)
        dt = SmallDateTime.from_pydatetime(val)
        w.pack(self._struct, dt.days, dt.minutes)

    def read(self, r):
        days, minutes = r.unpack(self._struct)
        dt = SmallDateTime(days=days, minutes=minutes)
        tzinfo = None
        if r._session.tzinfo_factory is not None:
            tzinfo = r._session.tzinfo_factory(0)
        return dt.to_pydatetime().replace(tzinfo=tzinfo)


SmallDateTimeSerializer.instance = (
    small_date_time_serializer
) = SmallDateTimeSerializer()


class DateTime(SqlValueMetaclass):
    """Corresponds to MSSQL datetime"""

    MIN_PYDATETIME = datetime.datetime(1753, 1, 1, 0, 0, 0)
    MAX_PYDATETIME = datetime.datetime(9999, 12, 31, 23, 59, 59, 997000)

    def __init__(self, days, time_part):
        """

        @param days: Days since 1900-01-01
        @param time_part: Number of 1/300 of seconds since 00:00:00
        """
        self._days = days
        self._time_part = time_part

    @property
    def days(self):
        return self._days

    @property
    def time_part(self):
        return self._time_part

    def to_pydatetime(self):
        ms = int(round(self._time_part % 300 * 10 / 3.0))
        secs = self._time_part // 300
        return _datetime_base_date + datetime.timedelta(
            days=self._days, seconds=secs, milliseconds=ms
        )

    @classmethod
    def from_pydatetime(cls, dt):
        if not (cls.MIN_PYDATETIME <= dt <= cls.MAX_PYDATETIME):
            raise tds_base.DataError("Datetime is out of range")
        days = (dt - _datetime_base_date).days
        ms = dt.microsecond // 1000
        tm = (dt.hour * 60 * 60 + dt.minute * 60 + dt.second) * 300 + int(
            round(ms * 3 / 10.0)
        )
        return cls(days=days, time_part=tm)


class DateTimeSerializer(BasePrimitiveTypeSerializer, BaseDateTimeSerializer):
    type = tds_base.SYBDATETIME
    declaration = "DATETIME"

    _struct = struct.Struct("<ll")

    def write(self, w, val):
        if val.tzinfo:
            if not w.session.use_tz:
                raise tds_base.DataError(
                    "Timezone-aware datetime is used without specifying use_tz"
                )
            val = val.astimezone(w.session.use_tz).replace(tzinfo=None)
        w.write(self.encode(val))

    def read(self, r):
        days, t = r.unpack(self._struct)
        tzinfo = None
        if r.session.tzinfo_factory is not None:
            tzinfo = r.session.tzinfo_factory(0)
        return _applytz(self.decode(days, t), tzinfo)

    @classmethod
    def encode(cls, value):
        if type(value) is datetime.date:
            value = datetime.datetime.combine(value, datetime.time(0, 0, 0))
        dt = DateTime.from_pydatetime(value)
        return cls._struct.pack(dt.days, dt.time_part)

    @classmethod
    def decode(cls, days, time_part):
        dt = DateTime(days=days, time_part=time_part)
        return dt.to_pydatetime()


DateTimeSerializer.instance = date_time_serializer = DateTimeSerializer()


class DateTimeNSerializer(BaseTypeSerializerN, BaseDateTimeSerializer):
    type = tds_base.SYBDATETIMN
    subtypes = {
        4: small_date_time_serializer,
        8: date_time_serializer,
    }


_datetime2_base_date = datetime.datetime(1, 1, 1)


class DateType(SqlTypeMetaclass):
    type = tds_base.SYBMSDATE

    def get_declaration(self):
        return "DATE"


class Date(SqlValueMetaclass):
    MIN_PYDATE = datetime.date(1, 1, 1)
    MAX_PYDATE = datetime.date(9999, 12, 31)

    def __init__(self, days):
        """
        Creates sql date object
        @param days: Days since 0001-01-01
        """
        self._days = days

    @property
    def days(self):
        return self._days

    def to_pydate(self):
        """
        Converts sql date to Python date
        @return: Python date
        """
        return (_datetime2_base_date + datetime.timedelta(days=self._days)).date()

    @classmethod
    def from_pydate(cls, pydate):
        """
        Creates sql date object from Python date object.
        @param pydate: Python date
        @return: sql date
        """
        return cls(
            days=(
                datetime.datetime.combine(pydate, datetime.time(0, 0, 0))
                - _datetime2_base_date
            ).days
        )


class TimeType(SqlTypeMetaclass):
    type = tds_base.SYBMSTIME

    def __init__(self, precision=7):
        self._precision = precision

    @property
    def precision(self):
        return self._precision

    def get_declaration(self):
        return "TIME({0})".format(self.precision)


class Time(SqlValueMetaclass):
    def __init__(self, nsec):
        """
        Creates sql time object.
        Maximum precision which sql server supports is 100 nanoseconds.
        Values more precise than 100 nanoseconds will be truncated.
        @param nsec: Nanoseconds from 00:00:00
        """
        self._nsec = nsec

    @property
    def nsec(self):
        return self._nsec

    def to_pytime(self):
        """
        Converts sql time object into Python's time object
        this will truncate nanoseconds to microseconds
        @return: naive time
        """
        nanoseconds = self._nsec
        hours = nanoseconds // 1000000000 // 60 // 60
        nanoseconds -= hours * 60 * 60 * 1000000000
        minutes = nanoseconds // 1000000000 // 60
        nanoseconds -= minutes * 60 * 1000000000
        seconds = nanoseconds // 1000000000
        nanoseconds -= seconds * 1000000000
        return datetime.time(hours, minutes, seconds, nanoseconds // 1000)

    @classmethod
    def from_pytime(cls, pytime):
        """
        Converts Python time object to sql time object
        ignoring timezone
        @param pytime: Python time object
        @return: sql time object
        """
        secs = pytime.hour * 60 * 60 + pytime.minute * 60 + pytime.second
        nsec = secs * 10**9 + pytime.microsecond * 1000
        return cls(nsec=nsec)


class DateTime2Type(SqlTypeMetaclass):
    type = tds_base.SYBMSDATETIME2

    def __init__(self, precision=7):
        self._precision = precision

    @property
    def precision(self):
        return self._precision

    def get_declaration(self):
        return "DATETIME2({0})".format(self.precision)


class DateTime2(SqlValueMetaclass):
    type = tds_base.SYBMSDATETIME2

    def __init__(self, date, time):
        """
        Creates datetime2 object
        @param date: sql date object
        @param time: sql time object
        """
        self._date = date
        self._time = time

    @property
    def date(self):
        return self._date

    @property
    def time(self):
        return self._time

    def to_pydatetime(self):
        """
        Converts datetime2 object into Python's datetime.datetime object
        @return: naive datetime.datetime
        """
        return datetime.datetime.combine(self._date.to_pydate(), self._time.to_pytime())

    @classmethod
    def from_pydatetime(cls, pydatetime):
        """
        Creates sql datetime2 object from Python datetime object
        ignoring timezone
        @param pydatetime: Python datetime object
        @return: sql datetime2 object
        """
        return cls(
            date=Date.from_pydate(pydatetime.date),
            time=Time.from_pytime(pydatetime.time),
        )


class DateTimeOffsetType(SqlTypeMetaclass):
    type = tds_base.SYBMSDATETIMEOFFSET

    def __init__(self, precision=7):
        self._precision = precision

    @property
    def precision(self):
        return self._precision

    def get_declaration(self):
        return "DATETIMEOFFSET({0})".format(self.precision)


class DateTimeOffset(SqlValueMetaclass):
    def __init__(self, date, time, offset):
        """
        Creates datetime2 object
        @param date: sql date object in UTC
        @param time: sql time object in UTC
        @param offset: time zone offset in minutes
        """
        self._date = date
        self._time = time
        self._offset = offset

    def to_pydatetime(self):
        """
        Converts datetimeoffset object into Python's datetime.datetime object
        @return: time zone aware datetime.datetime
        """
        dt = datetime.datetime.combine(self._date.to_pydate(), self._time.to_pytime())
        from .tz import FixedOffsetTimezone

        return dt.replace(tzinfo=_utc).astimezone(FixedOffsetTimezone(self._offset))


class BaseDateTime73Serializer(BaseTypeSerializer):
    def write(self, w, value):
        raise NotImplementedError

    def write_info(self, w):
        raise NotImplementedError

    def read(self, r):
        raise NotImplementedError

    @classmethod
    def from_stream(cls, r):
        raise NotImplementedError

    _precision_to_len = {
        0: 3,
        1: 3,
        2: 3,
        3: 4,
        4: 4,
        5: 5,
        6: 5,
        7: 5,
    }

    def _write_time(self, w, t, prec):
        val = t.nsec // (10 ** (9 - prec))
        w.write(struct.pack("<Q", val)[: self._precision_to_len[prec]])

    @staticmethod
    def _read_time(r, size, prec):
        time_buf = tds_base.readall(r, size)
        val = _decode_num(time_buf)
        val *= 10 ** (7 - prec)
        nanoseconds = val * 100
        return Time(nsec=nanoseconds)

    @staticmethod
    def _write_date(w, value):
        days = value.days
        buf = struct.pack("<l", days)[:3]
        w.write(buf)

    @staticmethod
    def _read_date(r):
        days = _decode_num(tds_base.readall(r, 3))
        return Date(days=days)


class MsDateSerializer(BasePrimitiveTypeSerializer, BaseDateTime73Serializer):
    type = tds_base.SYBMSDATE
    declaration = "DATE"

    def __init__(self, typ):
        super(MsDateSerializer, self).__init__()
        self._typ = typ

    @classmethod
    def from_stream(cls, r):
        return cls(DateType())

    def write(self, w, value):
        if value is None:
            w.put_byte(0)
        else:
            w.put_byte(3)
            self._write_date(w, Date.from_pydate(value))

    def read_fixed(self, r):
        return self._read_date(r).to_pydate()

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        return self._read_date(r).to_pydate()


class MsTimeSerializer(BaseDateTime73Serializer):
    type = tds_base.SYBMSTIME

    def __init__(self, typ):
        super(MsTimeSerializer, self).__init__(
            precision=typ.precision, size=self._precision_to_len[typ.precision]
        )
        self._typ = typ

    @classmethod
    def read_type(cls, r):
        prec = r.get_byte()
        return TimeType(precision=prec)

    @classmethod
    def from_stream(cls, r):
        return cls(cls.read_type(r))

    def write_info(self, w):
        w.put_byte(self._typ.precision)

    def write(self, w, value):
        if value is None:
            w.put_byte(0)
        else:
            if value.tzinfo:
                if not w.session.use_tz:
                    raise tds_base.DataError(
                        "Timezone-aware datetime is used without specifying use_tz"
                    )
                value = value.astimezone(w.session.use_tz).replace(tzinfo=None)
            w.put_byte(self.size)
            self._write_time(w, Time.from_pytime(value), self._typ.precision)

    def read_fixed(self, r, size):
        res = self._read_time(r, size, self._typ.precision).to_pytime()
        if r.session.tzinfo_factory is not None:
            tzinfo = r.session.tzinfo_factory(0)
            res = res.replace(tzinfo=tzinfo)
        return res

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        return self.read_fixed(r, size)


class DateTime2Serializer(BaseDateTime73Serializer):
    type = tds_base.SYBMSDATETIME2

    def __init__(self, typ):
        super(DateTime2Serializer, self).__init__(
            precision=typ.precision, size=self._precision_to_len[typ.precision] + 3
        )
        self._typ = typ

    @classmethod
    def from_stream(cls, r):
        prec = r.get_byte()
        return cls(DateTime2Type(precision=prec))

    def write_info(self, w):
        w.put_byte(self._typ.precision)

    def write(self, w, value):
        if value is None:
            w.put_byte(0)
        else:
            if value.tzinfo:
                if not w.session.use_tz:
                    raise tds_base.DataError(
                        "Timezone-aware datetime is used without specifying use_tz"
                    )
                value = value.astimezone(w.session.use_tz).replace(tzinfo=None)
            w.put_byte(self.size)
            self._write_time(w, Time.from_pytime(value), self._typ.precision)
            self._write_date(w, Date.from_pydate(value))

    def read_fixed(self, r, size):
        time = self._read_time(r, size - 3, self._typ.precision)
        date = self._read_date(r)
        dt = DateTime2(date=date, time=time)
        res = dt.to_pydatetime()
        if r.session.tzinfo_factory is not None:
            tzinfo = r.session.tzinfo_factory(0)
            res = res.replace(tzinfo=tzinfo)
        return res

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        return self.read_fixed(r, size)


class DateTimeOffsetSerializer(BaseDateTime73Serializer):
    type = tds_base.SYBMSDATETIMEOFFSET

    def __init__(self, typ):
        super(DateTimeOffsetSerializer, self).__init__(
            precision=typ.precision, size=self._precision_to_len[typ.precision] + 5
        )
        self._typ = typ

    @classmethod
    def from_stream(cls, r):
        prec = r.get_byte()
        return cls(DateTimeOffsetType(precision=prec))

    def write_info(self, w):
        w.put_byte(self._typ.precision)

    def write(self, w, value):
        if value is None:
            w.put_byte(0)
        else:
            utcoffset = value.utcoffset()
            value = value.astimezone(_utc).replace(tzinfo=None)

            w.put_byte(self.size)
            self._write_time(w, Time.from_pytime(value), self._typ.precision)
            self._write_date(w, Date.from_pydate(value))
            w.put_smallint(int(tds_base.total_seconds(utcoffset)) // 60)

    def read_fixed(self, r, size):
        time = self._read_time(r, size - 5, self._typ.precision)
        date = self._read_date(r)
        offset = r.get_smallint()
        dt = DateTimeOffset(date=date, time=time, offset=offset)
        return dt.to_pydatetime()

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        return self.read_fixed(r, size)


class MsDecimalSerializer(BaseTypeSerializer):
    type = tds_base.SYBDECIMAL

    _max_size = 17

    _bytes_per_prec = [
        #
        # precision can't be 0 but using a value > 0 assure no
        # core if for some bug it's 0...
        #
        1,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
        5,
        9,
        9,
        9,
        9,
        9,
        9,
        9,
        9,
        9,
        9,
        13,
        13,
        13,
        13,
        13,
        13,
        13,
        13,
        13,
        17,
        17,
        17,
        17,
        17,
        17,
        17,
        17,
        17,
        17,
    ]

    _info_struct = struct.Struct("BBB")

    def __init__(self, precision=18, scale=0):
        super(MsDecimalSerializer, self).__init__(
            precision=precision, scale=scale, size=self._bytes_per_prec[precision]
        )
        if precision > 38:
            raise tds_base.DataError("Precision of decimal value is out of range")

    def __repr__(self):
        return "MsDecimal(scale={}, prec={})".format(self.scale, self.precision)

    @classmethod
    def from_value(cls, value):
        sql_type = DecimalType.from_value(value)
        return cls(scale=sql_type.scale, prec=sql_type.precision)

    @classmethod
    def from_stream(cls, r):
        size, prec, scale = r.unpack(cls._info_struct)
        return cls(scale=scale, precision=prec)

    def write_info(self, w):
        w.pack(self._info_struct, self.size, self.precision, self.scale)

    def write(self, w, value):
        with decimal.localcontext() as context:
            context.prec = 38
            if value is None:
                w.put_byte(0)
                return
            if not isinstance(value, decimal.Decimal):
                value = decimal.Decimal(value)
            value = value.normalize()
            scale = self.scale
            size = self.size
            w.put_byte(size)
            val = value
            positive = 1 if val > 0 else 0
            w.put_byte(positive)  # sign
            if not positive:
                val *= -1
            size -= 1
            val *= 10**scale
            for i in range(size):
                w.put_byte(int(val % 256))
                val //= 256
            assert val == 0

    def _decode(self, positive, buf):
        val = _decode_num(buf)
        val = decimal.Decimal(val)
        with decimal.localcontext() as ctx:
            ctx.prec = 38
            if not positive:
                val *= -1
            val /= 10**self._scale
        return val

    def read_fixed(self, r, size):
        positive = r.get_byte()
        buf = tds_base.readall(r, size - 1)
        return self._decode(positive, buf)

    def read(self, r):
        size = r.get_byte()
        if size <= 0:
            return None
        return self.read_fixed(r, size)


class Money4Serializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBMONEY4
    declaration = "SMALLMONEY"

    def read(self, r):
        return decimal.Decimal(r.get_int()) / 10000

    def write(self, w, val):
        val = int(val * 10000)
        w.put_int(val)


Money4Serializer.instance = money4_serializer = Money4Serializer()


class Money8Serializer(BasePrimitiveTypeSerializer):
    type = tds_base.SYBMONEY
    declaration = "MONEY"

    _struct = struct.Struct("<lL")

    def read(self, r):
        hi, lo = r.unpack(self._struct)
        val = hi * (2**32) + lo
        return decimal.Decimal(val) / 10000

    def write(self, w, val):
        val *= 10000
        hi = int(val // (2**32))
        lo = int(val % (2**32))
        w.pack(self._struct, hi, lo)


Money8Serializer.instance = money8_serializer = Money8Serializer()


class MoneyNSerializer(BaseTypeSerializerN):
    type = tds_base.SYBMONEYN

    subtypes = {
        4: money4_serializer,
        8: money8_serializer,
    }


class MsUniqueSerializer(BaseTypeSerializer):
    type = tds_base.SYBUNIQUE
    declaration = "UNIQUEIDENTIFIER"
    instance: MsUniqueSerializer

    def __repr__(self):
        return "MsUniqueSerializer()"

    @classmethod
    def from_stream(cls, r):
        size = r.get_byte()
        if size != 16:
            raise tds_base.InterfaceError("Invalid size of UNIQUEIDENTIFIER field")
        return cls.instance

    def write_info(self, w):
        w.put_byte(16)

    def write(self, w, value):
        if value is None:
            w.put_byte(0)
        else:
            w.put_byte(16)
            w.write(value.bytes_le)

    @staticmethod
    def read_fixed(r, size):
        return uuid.UUID(bytes_le=tds_base.readall(r, size))

    def read(self, r):
        size = r.get_byte()
        if size == 0:
            return None
        if size != 16:
            raise tds_base.InterfaceError("Invalid size of UNIQUEIDENTIFIER field")
        return self.read_fixed(r, size)


MsUniqueSerializer.instance = ms_unique_serializer = MsUniqueSerializer()


def _variant_read_str(r, size):
    collation = r.get_collation()
    r.get_usmallint()
    return r.read_str(size, collation.get_codec())


def _variant_read_nstr(r, size):
    r.get_collation()
    r.get_usmallint()
    return r.read_str(size, ucs2_codec)


def _variant_read_decimal(r, size):
    prec, scale = r.unpack(VariantSerializer.decimal_info_struct)
    return MsDecimalSerializer(precision=prec, scale=scale).read_fixed(r, size)


def _variant_read_binary(r, size):
    r.get_usmallint()
    return tds_base.readall(r, size)


class VariantSerializer(BaseTypeSerializer):
    type = tds_base.SYBVARIANT
    declaration = "SQL_VARIANT"

    decimal_info_struct = struct.Struct("BB")

    _type_map = {
        tds_base.GUIDTYPE: lambda r, size: ms_unique_serializer.read_fixed(r, size),
        tds_base.BITTYPE: lambda r, size: bit_serializer.read(r),
        tds_base.INT1TYPE: lambda r, size: tiny_int_serializer.read(r),
        tds_base.INT2TYPE: lambda r, size: small_int_serializer.read(r),
        tds_base.INT4TYPE: lambda r, size: int_serializer.read(r),
        tds_base.INT8TYPE: lambda r, size: big_int_serializer.read(r),
        tds_base.DATETIMETYPE: lambda r, size: date_time_serializer.read(r),
        tds_base.DATETIM4TYPE: lambda r, size: small_date_time_serializer.read(r),
        tds_base.FLT4TYPE: lambda r, size: real_serializer.read(r),
        tds_base.FLT8TYPE: lambda r, size: float_serializer.read(r),
        tds_base.MONEYTYPE: lambda r, size: money8_serializer.read(r),
        tds_base.MONEY4TYPE: lambda r, size: money4_serializer.read(r),
        tds_base.DATENTYPE: lambda r, size: MsDateSerializer(DateType()).read_fixed(r),
        tds_base.TIMENTYPE: lambda r, size: MsTimeSerializer(
            TimeType(precision=r.get_byte())
        ).read_fixed(r, size),
        tds_base.DATETIME2NTYPE: lambda r, size: DateTime2Serializer(
            DateTime2Type(precision=r.get_byte())
        ).read_fixed(r, size),
        tds_base.DATETIMEOFFSETNTYPE: lambda r, size: DateTimeOffsetSerializer(
            DateTimeOffsetType(precision=r.get_byte())
        ).read_fixed(r, size),
        tds_base.BIGVARBINTYPE: _variant_read_binary,
        tds_base.BIGBINARYTYPE: _variant_read_binary,
        tds_base.NUMERICNTYPE: _variant_read_decimal,
        tds_base.DECIMALNTYPE: _variant_read_decimal,
        tds_base.BIGVARCHRTYPE: _variant_read_str,
        tds_base.BIGCHARTYPE: _variant_read_str,
        tds_base.NVARCHARTYPE: _variant_read_nstr,
        tds_base.NCHARTYPE: _variant_read_nstr,
    }

    @classmethod
    def from_stream(cls, r):
        size = r.get_int()
        return VariantSerializer(size)

    def write_info(self, w):
        w.put_int(self.size)

    def read(self, r):
        size = r.get_int()
        if size == 0:
            return None

        type_id = r.get_byte()
        prop_bytes = r.get_byte()
        type_factory = self._type_map.get(type_id)
        if not type_factory:
            r.session.bad_stream("Variant type invalid", type_id)
        return type_factory(r, size - prop_bytes - 2)

    def write(self, w, val):
        if val is None:
            w.put_int(0)
            return
        raise NotImplementedError


class TableType(SqlTypeMetaclass):
    """
    Used to serialize table valued parameters

    spec: https://msdn.microsoft.com/en-us/library/dd304813.aspx
    """

    def __init__(self, typ_schema, typ_name, columns):
        """
        @param typ_schema: Schema where TVP type defined
        @param typ_name: Name of TVP type
        @param columns: List of column types
        """
        if len(typ_schema) > 128:
            raise ValueError(
                "Schema part of TVP name should be no longer than 128 characters"
            )
        if len(typ_name) > 128:
            raise ValueError(
                "Name part of TVP name should be no longer than 128 characters"
            )
        if columns is not None:
            if len(columns) > 1024:
                raise ValueError("TVP cannot have more than 1024 columns")
            if len(columns) < 1:
                raise ValueError("TVP must have at least one column")
        self._typ_dbname = (
            ""  # dbname should always be empty string for TVP according to spec
        )
        self._typ_schema = typ_schema
        self._typ_name = typ_name
        self._columns = columns

    def __repr__(self):
        return "TableType(s={},n={},cols={})".format(
            self._typ_schema, self._typ_name, repr(self._columns)
        )

    def get_declaration(self):
        assert not self._typ_dbname
        if self._typ_schema:
            full_name = "{}.{}".format(self._typ_schema, self._typ_name)
        else:
            full_name = self._typ_name
        return "{} READONLY".format(full_name)

    @property
    def typ_schema(self):
        return self._typ_schema

    @property
    def typ_name(self):
        return self._typ_name

    @property
    def columns(self):
        return self._columns


class TableValuedParam(SqlValueMetaclass):
    """
    Used to represent a value of table-valued parameter
    """

    def __init__(self, type_name=None, columns=None, rows=None):
        # parsing type name
        self._typ_schema = ""
        self._typ_name = ""
        if type_name:
            parts = type_name.split(".")
            if len(parts) > 2:
                raise ValueError(
                    "Type name should consist of at most 2 parts, e.g. dbo.MyType"
                )
            self._typ_name = parts[-1]
            if len(parts) > 1:
                self._typ_schema = parts[0]

        self._columns = columns
        self._rows = rows

    @property
    def typ_name(self):
        return self._typ_name

    @property
    def typ_schema(self):
        return self._typ_schema

    @property
    def columns(self):
        return self._columns

    @property
    def rows(self):
        return self._rows

    def is_null(self):
        return self._rows is None

    def peek_row(self):
        try:
            rows = iter(self._rows)
        except TypeError:
            raise tds_base.DataError("rows should be iterable")

        try:
            row = next(rows)
        except StopIteration:
            # no rows
            raise tds_base.DataError(
                "Cannot infer columns from rows for TVP because there are no rows"
            )
        else:
            # put row back
            self._rows = itertools.chain([row], rows)
        return row


class TableSerializer(BaseTypeSerializer):
    """
    Used to serialize table valued parameters

    spec: https://msdn.microsoft.com/en-us/library/dd304813.aspx
    """

    type = tds_base.TVPTYPE

    def read(self, r):
        """According to spec TDS does not support output TVP values"""
        raise NotImplementedError

    @classmethod
    def from_stream(cls, r):
        """According to spec TDS does not support output TVP values"""
        raise NotImplementedError

    def __init__(self, table_type, columns_serializers):
        super(TableSerializer, self).__init__()
        self._table_type = table_type
        self._columns_serializers = columns_serializers

    @property
    def table_type(self):
        return self._table_type

    def __repr__(self):
        return "TableSerializer(t={},c={})".format(
            repr(self._table_type), repr(self._columns_serializers)
        )

    def write_info(self, w):
        """
        Writes TVP_TYPENAME structure

        spec: https://msdn.microsoft.com/en-us/library/dd302994.aspx
        @param w: TdsWriter
        @return:
        """
        w.write_b_varchar("")  # db_name, should be empty
        w.write_b_varchar(self._table_type.typ_schema)
        w.write_b_varchar(self._table_type.typ_name)

    def write(self, w, val):
        """
        Writes remaining part of TVP_TYPE_INFO structure, resuming from TVP_COLMETADATA

        specs:
        https://msdn.microsoft.com/en-us/library/dd302994.aspx
        https://msdn.microsoft.com/en-us/library/dd305261.aspx
        https://msdn.microsoft.com/en-us/library/dd303230.aspx

        @param w: TdsWriter
        @param val: TableValuedParam or None
        @return:
        """
        if val.is_null():
            w.put_usmallint(tds_base.TVP_NULL_TOKEN)
        else:
            columns = self._table_type.columns
            w.put_usmallint(len(columns))
            for i, column in enumerate(columns):
                w.put_uint(column.column_usertype)

                w.put_usmallint(column.flags)

                # TYPE_INFO structure: https://msdn.microsoft.com/en-us/library/dd358284.aspx

                serializer = self._columns_serializers[i]
                type_id = serializer.type
                w.put_byte(type_id)
                serializer.write_info(w)

                w.write_b_varchar("")  # ColName, must be empty in TVP according to spec

        # here can optionally send TVP_ORDER_UNIQUE and TVP_COLUMN_ORDERING
        # https://msdn.microsoft.com/en-us/library/dd305261.aspx

        # terminating optional metadata
        w.put_byte(tds_base.TVP_END_TOKEN)

        # now sending rows using TVP_ROW
        # https://msdn.microsoft.com/en-us/library/dd305261.aspx
        if val.rows:
            for row in val.rows:
                w.put_byte(tds_base.TVP_ROW_TOKEN)
                for i, col in enumerate(self._table_type.columns):
                    if not col.flags & tds_base.TVP_COLUMN_DEFAULT_FLAG:
                        self._columns_serializers[i].write(w, row[i])

        # terminating rows
        w.put_byte(tds_base.TVP_END_TOKEN)


_type_map = {
    tds_base.SYBINT1: TinyIntSerializer,
    tds_base.SYBINT2: SmallIntSerializer,
    tds_base.SYBINT4: IntSerializer,
    tds_base.SYBINT8: BigIntSerializer,
    tds_base.SYBINTN: IntNSerializer,
    tds_base.SYBBIT: BitSerializer,
    tds_base.SYBBITN: BitNSerializer,
    tds_base.SYBREAL: RealSerializer,
    tds_base.SYBFLT8: FloatSerializer,
    tds_base.SYBFLTN: FloatNSerializer,
    tds_base.SYBMONEY4: Money4Serializer,
    tds_base.SYBMONEY: Money8Serializer,
    tds_base.SYBMONEYN: MoneyNSerializer,
    tds_base.XSYBCHAR: VarChar70Serializer,
    tds_base.XSYBVARCHAR: VarChar70Serializer,
    tds_base.XSYBNCHAR: NVarChar70Serializer,
    tds_base.XSYBNVARCHAR: NVarChar70Serializer,
    tds_base.SYBTEXT: Text70Serializer,
    tds_base.SYBNTEXT: NText70Serializer,
    tds_base.SYBMSXML: XmlSerializer,
    tds_base.XSYBBINARY: VarBinarySerializer,
    tds_base.XSYBVARBINARY: VarBinarySerializer,
    tds_base.SYBIMAGE: Image70Serializer,
    tds_base.SYBNUMERIC: MsDecimalSerializer,
    tds_base.SYBDECIMAL: MsDecimalSerializer,
    tds_base.SYBVARIANT: VariantSerializer,
    tds_base.SYBMSDATE: MsDateSerializer,
    tds_base.SYBMSTIME: MsTimeSerializer,
    tds_base.SYBMSDATETIME2: DateTime2Serializer,
    tds_base.SYBMSDATETIMEOFFSET: DateTimeOffsetSerializer,
    tds_base.SYBDATETIME4: SmallDateTimeSerializer,
    tds_base.SYBDATETIME: DateTimeSerializer,
    tds_base.SYBDATETIMN: DateTimeNSerializer,
    tds_base.SYBUNIQUE: MsUniqueSerializer,
}

_type_map71 = _type_map.copy()
_type_map71.update(
    {
        tds_base.XSYBCHAR: VarChar71Serializer,
        tds_base.XSYBNCHAR: NVarChar71Serializer,
        tds_base.XSYBVARCHAR: VarChar71Serializer,
        tds_base.XSYBNVARCHAR: NVarChar71Serializer,
        tds_base.SYBTEXT: Text71Serializer,
        tds_base.SYBNTEXT: NText71Serializer,
    }
)

_type_map72 = _type_map.copy()
_type_map72.update(
    {
        tds_base.XSYBCHAR: VarChar72Serializer,
        tds_base.XSYBNCHAR: NVarChar72Serializer,
        tds_base.XSYBVARCHAR: VarChar72Serializer,
        tds_base.XSYBNVARCHAR: NVarChar72Serializer,
        tds_base.SYBTEXT: Text72Serializer,
        tds_base.SYBNTEXT: NText72Serializer,
        tds_base.XSYBBINARY: VarBinarySerializer72,
        tds_base.XSYBVARBINARY: VarBinarySerializer72,
        tds_base.SYBIMAGE: Image72Serializer,
        tds_base.UDTTYPE: UDT72Serializer,
    }
)

_type_map73 = _type_map72.copy()
_type_map73.update(
    {
        tds_base.TVPTYPE: TableSerializer,
    }
)


def sql_type_by_declaration(declaration):
    return _declarations_parser.parse(declaration)


class SerializerFactory(object):
    """
    Factory class for TDS data types
    """

    def __init__(self, tds_ver):
        self._tds_ver = tds_ver
        if self._tds_ver >= tds_base.TDS73:
            self._type_map = _type_map73
        elif self._tds_ver >= tds_base.TDS72:
            self._type_map = _type_map72
        elif self._tds_ver >= tds_base.TDS71:
            self._type_map = _type_map71
        else:
            self._type_map = _type_map

    def get_type_serializer(self, tds_type_id):
        type_class = self._type_map.get(tds_type_id)
        if not type_class:
            raise tds_base.InterfaceError("Invalid type id {}".format(tds_type_id))
        return type_class

    def long_binary_type(self):
        if self._tds_ver >= tds_base.TDS72:
            return VarBinaryMaxType()
        else:
            return ImageType()

    def long_varchar_type(self):
        if self._tds_ver >= tds_base.TDS72:
            return VarCharMaxType()
        else:
            return TextType()

    def long_string_type(self):
        if self._tds_ver >= tds_base.TDS72:
            return NVarCharMaxType()
        else:
            return NTextType()

    def datetime(self, precision):
        if self._tds_ver >= tds_base.TDS72:
            return DateTime2Type(precision=precision)
        else:
            return DateTimeType()

    def has_datetime_with_tz(self):
        return self._tds_ver >= tds_base.TDS72

    def datetime_with_tz(self, precision):
        if self._tds_ver >= tds_base.TDS72:
            return DateTimeOffsetType(precision=precision)
        else:
            raise tds_base.DataError(
                "Given TDS version does not support DATETIMEOFFSET type"
            )

    def date(self):
        if self._tds_ver >= tds_base.TDS72:
            return DateType()
        else:
            return DateTimeType()

    def time(self, precision):
        if self._tds_ver >= tds_base.TDS72:
            return TimeType(precision=precision)
        else:
            raise tds_base.DataError("Given TDS version does not support TIME type")

    def serializer_by_declaration(self, declaration, connection):
        sql_type = sql_type_by_declaration(declaration)
        return self.serializer_by_type(
            sql_type=sql_type, collation=connection.collation
        )

    def serializer_by_type(self, sql_type, collation=raw_collation):
        typ = sql_type
        if isinstance(typ, BitType):
            return BitNSerializer(typ)
        elif isinstance(typ, TinyIntType):
            return IntNSerializer(typ)
        elif isinstance(typ, SmallIntType):
            return IntNSerializer(typ)
        elif isinstance(typ, IntType):
            return IntNSerializer(typ)
        elif isinstance(typ, BigIntType):
            return IntNSerializer(typ)
        elif isinstance(typ, RealType):
            return FloatNSerializer(size=4)
        elif isinstance(typ, FloatType):
            return FloatNSerializer(size=8)
        elif isinstance(typ, SmallMoneyType):
            return self._type_map[tds_base.SYBMONEYN](size=4)
        elif isinstance(typ, MoneyType):
            return self._type_map[tds_base.SYBMONEYN](size=8)
        elif isinstance(typ, CharType):
            return self._type_map[tds_base.XSYBCHAR](size=typ.size, collation=collation)
        elif isinstance(typ, VarCharType):
            return self._type_map[tds_base.XSYBVARCHAR](
                size=typ.size, collation=collation
            )
        elif isinstance(typ, VarCharMaxType):
            return VarCharMaxSerializer(collation=collation)
        elif isinstance(typ, NCharType):
            return self._type_map[tds_base.XSYBNCHAR](
                size=typ.size, collation=collation
            )
        elif isinstance(typ, NVarCharType):
            return self._type_map[tds_base.XSYBNVARCHAR](
                size=typ.size, collation=collation
            )
        elif isinstance(typ, NVarCharMaxType):
            return NVarCharMaxSerializer(collation=collation)
        elif isinstance(typ, TextType):
            return self._type_map[tds_base.SYBTEXT](collation=collation)
        elif isinstance(typ, NTextType):
            return self._type_map[tds_base.SYBNTEXT](collation=collation)
        elif isinstance(typ, XmlType):
            return self._type_map[tds_base.SYBMSXML]()
        elif isinstance(typ, BinaryType):
            return self._type_map[tds_base.XSYBBINARY]()
        elif isinstance(typ, VarBinaryType):
            return self._type_map[tds_base.XSYBVARBINARY](size=typ.size)
        elif isinstance(typ, VarBinaryMaxType):
            return VarBinarySerializerMax()
        elif isinstance(typ, ImageType):
            return self._type_map[tds_base.SYBIMAGE]()
        elif isinstance(typ, DecimalType):
            return self._type_map[tds_base.SYBDECIMAL](
                scale=typ.scale, precision=typ.precision
            )
        elif isinstance(typ, VariantType):
            return self._type_map[tds_base.SYBVARIANT](size=0)
        elif isinstance(typ, SmallDateTimeType):
            return self._type_map[tds_base.SYBDATETIMN](size=4)
        elif isinstance(typ, DateTimeType):
            return self._type_map[tds_base.SYBDATETIMN](size=8)
        elif isinstance(typ, DateType):
            return self._type_map[tds_base.SYBMSDATE](typ)
        elif isinstance(typ, TimeType):
            return self._type_map[tds_base.SYBMSTIME](typ)
        elif isinstance(typ, DateTime2Type):
            return self._type_map[tds_base.SYBMSDATETIME2](typ)
        elif isinstance(typ, DateTimeOffsetType):
            return self._type_map[tds_base.SYBMSDATETIMEOFFSET](typ)
        elif isinstance(typ, UniqueIdentifierType):
            return self._type_map[tds_base.SYBUNIQUE]()
        elif isinstance(typ, TableType):
            columns_serializers = None
            if typ.columns is not None:
                columns_serializers = [
                    self.serializer_by_type(col.type) for col in typ.columns
                ]
            return TableSerializer(
                table_type=typ, columns_serializers=columns_serializers
            )
        else:
            raise ValueError("Cannot map type {} to serializer.".format(typ))


class DeclarationsParser(object):
    def __init__(self):
        declaration_parsers = [
            ("bit", BitType),
            ("tinyint", TinyIntType),
            ("smallint", SmallIntType),
            ("(?:int|integer)", IntType),
            ("bigint", BigIntType),
            ("real", RealType),
            ("(?:float|double precision)", FloatType),
            ("(?:char|character)", CharType),
            (
                r"(?:char|character)\((\d+)\)",
                lambda size_str: CharType(size=int(size_str)),
            ),
            (r"(?:varchar|char(?:|acter)\s+varying)", VarCharType),
            (
                r"(?:varchar|char(?:|acter)\s+varying)\((\d+)\)",
                lambda size_str: VarCharType(size=int(size_str)),
            ),
            (r"varchar\(max\)", VarCharMaxType),
            (r"(?:nchar|national\s+(?:char|character))", NCharType),
            (
                r"(?:nchar|national\s+(?:char|character))\((\d+)\)",
                lambda size_str: NCharType(size=int(size_str)),
            ),
            (r"(?:nvarchar|national\s+(?:char|character)\s+varying)", NVarCharType),
            (
                r"(?:nvarchar|national\s+(?:char|character)\s+varying)\((\d+)\)",
                lambda size_str: NVarCharType(size=int(size_str)),
            ),
            (r"nvarchar\(max\)", NVarCharMaxType),
            ("xml", XmlType),
            ("text", TextType),
            (r"(?:ntext|national\s+text)", NTextType),
            ("binary", BinaryType),
            (r"binary\((\d+)\)", lambda size_str: BinaryType(size=int(size_str))),
            ("(?:varbinary|binary varying)", VarBinaryType),
            (
                r"(?:varbinary|binary varying)\((\d+)\)",
                lambda size_str: VarBinaryType(size=int(size_str)),
            ),
            (r"varbinary\(max\)", VarBinaryMaxType),
            ("image", ImageType),
            ("smalldatetime", SmallDateTimeType),
            ("datetime", DateTimeType),
            ("date", DateType),
            (r"time", TimeType),
            (
                r"time\((\d+)\)",
                lambda precision_str: TimeType(precision=int(precision_str)),
            ),
            ("datetime2", DateTime2Type),
            (
                r"datetime2\((\d+)\)",
                lambda precision_str: DateTime2Type(precision=int(precision_str)),
            ),
            ("datetimeoffset", DateTimeOffsetType),
            (
                r"datetimeoffset\((\d+)\)",
                lambda precision_str: DateTimeOffsetType(precision=int(precision_str)),
            ),
            ("(?:decimal|dec|numeric)", DecimalType),
            (
                r"(?:decimal|dec|numeric)\((\d+)\)",
                lambda precision_str: DecimalType(precision=int(precision_str)),
            ),
            (
                r"(?:decimal|dec|numeric)\((\d+), ?(\d+)\)",
                lambda precision_str, scale_str: DecimalType(
                    precision=int(precision_str), scale=int(scale_str)
                ),
            ),
            ("smallmoney", SmallMoneyType),
            ("money", MoneyType),
            ("uniqueidentifier", UniqueIdentifierType),
            ("sql_variant", VariantType),
        ]
        self._compiled = [
            (re.compile(r"^" + regex + "$", re.IGNORECASE), constructor)
            for regex, constructor in declaration_parsers
        ]

    def parse(self, declaration):
        """
        Parse sql type declaration, e.g. varchar(10) and return instance of corresponding type class,
        e.g. VarCharType(10)

        @param declaration: Sql declaration to parse, e.g. varchar(10)
        @return: instance of SqlTypeMetaclass
        """
        declaration = declaration.strip()
        for regex, constructor in self._compiled:
            m = regex.match(declaration)
            if m:
                return constructor(*m.groups())
        raise ValueError("Unable to parse type declaration", declaration)


_declarations_parser = DeclarationsParser()


class TdsTypeInferrer(object):
    def __init__(
        self, type_factory, collation=None, bytes_to_unicode=False, allow_tz=False
    ):
        """
        Class used to do TDS type inference

        :param type_factory: Instance of TypeFactory
        :param collation: Collation to use for strings
        :param bytes_to_unicode: Treat bytes type as unicode string
        :param allow_tz: Allow usage of DATETIMEOFFSET type
        """
        self._type_factory = type_factory
        self._collation = collation
        self._bytes_to_unicode = bytes_to_unicode
        self._allow_tz = allow_tz

    def from_value(self, value):
        """Function infers TDS type from Python value.

        :param value: value from which to infer TDS type
        :return: An instance of subclass of :class:`BaseType`
        """
        if value is None:
            sql_type = NVarCharType(size=1)
        else:
            sql_type = self._from_class_value(value, type(value))
        return sql_type

    def from_class(self, cls):
        """Function infers TDS type from Python class.

        :param cls: Class from which to infer type
        :return: An instance of subclass of :class:`BaseType`
        """
        return self._from_class_value(None, cls)

    def _from_class_value(self, value, value_type):
        type_factory = self._type_factory
        bytes_to_unicode = self._bytes_to_unicode
        allow_tz = self._allow_tz

        if issubclass(value_type, bool):
            return BitType()
        elif issubclass(value_type, int):
            if value is None:
                return IntType()
            if -(2**31) <= value <= 2**31 - 1:
                return IntType()
            elif -(2**63) <= value <= 2**63 - 1:
                return BigIntType()
            elif -(10**38) + 1 <= value <= 10**38 - 1:
                return DecimalType(precision=38)
            else:
                return VarCharMaxType()
        elif issubclass(value_type, float):
            return FloatType()
        elif issubclass(value_type, Binary):
            if value is None or len(value) <= 8000:
                return VarBinaryType(size=8000)
            else:
                return type_factory.long_binary_type()
        elif issubclass(value_type, bytes):
            if bytes_to_unicode:
                return type_factory.long_string_type()
            else:
                return type_factory.long_varchar_type()
        elif issubclass(value_type, str):
            return type_factory.long_string_type()
        elif issubclass(value_type, datetime.datetime):
            if value and value.tzinfo and allow_tz:
                return type_factory.datetime_with_tz(precision=6)
            else:
                return type_factory.datetime(precision=6)
        elif issubclass(value_type, datetime.date):
            return type_factory.date()
        elif issubclass(value_type, datetime.time):
            return type_factory.time(precision=6)
        elif issubclass(value_type, decimal.Decimal):
            if value is None:
                return DecimalType()
            else:
                return DecimalType.from_value(value)
        elif issubclass(value_type, uuid.UUID):
            return UniqueIdentifierType()
        elif issubclass(value_type, TableValuedParam):
            columns = value.columns
            rows = value.rows
            if columns is None:
                # trying to auto detect columns using data from first row
                if rows is None:
                    # rows are not present too, this means
                    # entire tvp has value of NULL
                    pass
                else:
                    # use first row to infer types of columns
                    row = value.peek_row()
                    columns = []
                    try:
                        cell_iter = iter(row)
                    except TypeError:
                        raise tds_base.DataError(
                            "Each row in table should be an iterable"
                        )
                    for cell in cell_iter:
                        if isinstance(cell, TableValuedParam):
                            raise tds_base.DataError(
                                "TVP type cannot have nested TVP types"
                            )
                        col_type = self.from_value(cell)
                        col = tds_base.Column(type=col_type)
                        columns.append(col)

            return TableType(
                typ_schema=value.typ_schema, typ_name=value.typ_name, columns=columns
            )
        else:
            raise tds_base.DataError(
                "Cannot infer TDS type from Python value: {!r}".format(value)
            )
