#cython: language_level=3
import json
import re
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

from cpython cimport PyList_Append, PyUnicode_AsEncodedString, PyUnicode_Join
from cpython.datetime cimport date, datetime
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from libc.stdint cimport (
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
)

from aiochclient.exceptions import ChClientError


cdef datetime _datetime_parse(str string):
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

cdef datetime _datetime_parse_f(str string):
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S.%f')

cdef date _date_parse(str string):
    return datetime.strptime(string, '%Y-%m-%d')


try:
    import ciso8601
except ImportError:
    date_parse = _date_parse
    datetime_parse = _datetime_parse
    datetime_parse_f = _datetime_parse_f
else:
    date_parse = datetime_parse = datetime_parse_f = ciso8601.parse_datetime


cdef extern from *:
    ctypedef int int128 "__int128_t"
    ctypedef int uint128 "__uint128_t"


__all__ = ["what_py_converter", "rows2ch", "json2ch", "py2ch"]


DEF DQ = "'"
DEF CM = ","
DEF ESCAPE_OP = '\\'
DEF TUP_OP = '('
DEF TUP_CLS = ')'
DEF ARR_OP = '['
DEF ARR_CLS = ']'

RE_TUPLE = re.compile(r"^Tuple\((.*)\)$")
RE_ARRAY = re.compile(r"^Array\((.*)\)$")
RE_NESTED = re.compile(r"^Nested\((.*)\)$")
RE_NULLABLE = re.compile(r"^Nullable\((.*)\)$")
RE_LOW_CARDINALITY = re.compile(r"^LowCardinality\((.*)\)$")
RE_MAP = re.compile(r"^Map\((.*)\)$")
RE_REPLACE_QUOTE = re.compile(r"(?<!\\)'")


cdef str remove_single_quotes(str string):
    if string[0] == string[-1] == "'":
        return string[1:-1]
    return string


cdef str decode(char* val):
    """
    Converting bytes from clickhouse with
    backslash-escaped special characters
    to pythonic string format
    """
    cdef:
        int current_chr
        str result
        Py_ssize_t i, current_i = 0, length = len(val)
        char* c_value_buffer = <char *> PyMem_Malloc(length * sizeof(char))
        bint escape = False

    try:
        for i in range(length):
            current_chr = val[i]
            if escape:
                # cython efficiently replaces it with switch/case
                if current_chr == ord("b"):
                    c_value_buffer[current_i] = ord("\b")
                elif current_chr == ord("N"):
                    c_value_buffer[current_i] = ord("\\")
                    current_i += 1
                    c_value_buffer[current_i] = ord("N")
                elif current_chr == ord("f"):
                    c_value_buffer[current_i] = ord("\f")
                elif current_chr == ord("r"):
                    c_value_buffer[current_i] = ord("\r")
                elif current_chr == ord("n"):
                    c_value_buffer[current_i] = ord("\n")
                elif current_chr == ord("t"):
                    c_value_buffer[current_i] = ord("\t")
                elif current_chr == ord("0"):
                    c_value_buffer[current_i] = ord(" ")
                elif current_chr == ord("'"):
                    c_value_buffer[current_i] = ord("'")
                elif current_chr == ord("\\"):
                    c_value_buffer[current_i] = ord("\\")
                else:
                    c_value_buffer[current_i] = current_chr
                escape = False
                current_i += 1
            elif current_chr == ord("\\"):
                escape = True
            else:
                c_value_buffer[current_i] = current_chr
                current_i += 1
        result = c_value_buffer[:current_i].decode()
        return result
    finally:
        PyMem_Free(c_value_buffer)


cdef list seq_parser(str raw):
    """
    Function for parsing tuples and arrays
    """
    cdef:
        list res = [], cur = []
        bint in_str = False, in_arr = False, in_tup = False, escape_char = False
    if not raw:
        return res
    for sym in raw:
        if not (in_str or in_arr or in_tup):
            if sym == CM:
                PyList_Append(res, PyUnicode_Join("", cur))
                del cur[:]
                continue
            elif sym == DQ:
                in_str = not in_str
            elif sym == ARR_OP:
                in_arr = True
            elif sym == TUP_OP:
                in_tup = True
        elif in_str and sym == DQ:
            if not escape_char:
                in_str = not in_str
        elif in_arr and sym == ARR_CLS:
            in_arr = False
        elif in_tup and sym == TUP_CLS:
            in_tup = False
        if in_str and sym == ESCAPE_OP:
            escape_char = not escape_char
        else:
            escape_char = False
        PyList_Append(cur, sym)
    PyList_Append(res, PyUnicode_Join("", cur))
    return res


cdef class StrType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef str _convert(self, str string):
        if self.container:
            return remove_single_quotes(string)
        return string

    cpdef str p_type(self, str string):
        return self._convert(string)

    cpdef str convert(self, bytes value):
        return self._convert(decode(value))


cdef class BoolType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef bint p_type(self, str string):
        if string == "true":
            return True
        elif string == "false":
            return False
        else:
            raise ValueError("invalid boolean value: {!r}".format(string))

    cpdef bint convert(self, bytes value):
        return self.p_type(value.decode())


cdef class Int8Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int8_t p_type(self, str string):
        return int(string)

    cpdef int8_t convert(self, bytes value):
        return int(value)


cdef class Int16Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int16_t p_type(self, str string):
        return int(string)

    cpdef int16_t convert(self, bytes value):
        return int(value)


cdef class Int32Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int32_t p_type(self, str string):
        return int(string)

    cpdef int32_t convert(self, bytes value):
        return int(value)


cdef class Int64Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int64_t p_type(self, str string):
        return int(string)

    cpdef int64_t convert(self, bytes value):
        return int(value)


cdef class Int128Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef int128 p_type(self, str string):
        return int(string)

    cpdef int128 convert(self, bytes value):
        return int(value)


cdef class Int256Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef p_type(self, str string):
        return int(string)

    cpdef convert(self, bytes value):
        return int(value)


cdef class UInt8Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint8_t p_type(self, str string):
        return int(string)

    cpdef uint8_t convert(self, bytes value):
        return int(value)


cdef class UInt16Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint16_t p_type(self, str string):
        return int(string)

    cpdef uint16_t convert(self, bytes value):
        return int(value)


cdef class UInt32Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint32_t p_type(self, str string):
        return int(string)

    cpdef uint32_t convert(self, bytes value):
        return int(value)


cdef class UInt64Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint64_t p_type(self, str string):
        return int(string)

    cpdef uint64_t convert(self, bytes value):
        return int(value)


cdef class UInt128Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef uint128 p_type(self, str string):
        return int(string)

    cpdef uint128 convert(self, bytes value):
        return int(value)


cdef class UInt256Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef p_type(self, str string):
        return int(string)

    cpdef convert(self, bytes value):
        return int(value)


cdef class FloatType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef double p_type(self, str string):
        return float(string)

    cpdef double convert(self, bytes value):
        return float(value)


cdef class DateType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return date_parse(string).date()
        except ValueError:
            # In case of 0000-00-00
            if string == "0000-00-00":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class DateTimeType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return datetime_parse(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00
            if string == "0000-00-00 00:00:00":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class DateTime64Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        string = string.strip("'")
        try:
            return datetime_parse_f(string)
        except ValueError:
            # In case of 0000-00-00 00:00:00.000
            if string == "0000-00-00 00:00:00.000":
                return None
            raise

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class TupleType:

    cdef:
        str name
        bint container
        tuple types

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        cdef str tps = RE_TUPLE.findall(name)[0]
        self.types = tuple(what_py_type(tp.rpartition(" ")[2], container=True).p_type for tp in tps.split(","))

    cdef tuple _convert(self, str string):
        return tuple(
            tp(decode(val.encode()))
            for tp, val in zip(self.types, seq_parser(string[1:-1]))
        )

    cpdef tuple p_type(self, str string):
        return self._convert(string)

    cpdef tuple convert(self, bytes value):
        return self._convert(value.decode())


cdef class MapType:

    cdef:
        str name
        bint container
        key_type
        value_type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        tps = RE_MAP.findall(name)[0]
        comma_index = tps.index(",")
        self.key_type = what_py_type(tps[:comma_index], container=True)
        self.value_type = what_py_type(tps[comma_index + 1:], container=True)

    cdef dict _convert(self, str string):
        key, value = string[1:-1].split(':', 1)
        return {
            self.key_type.p_type(decode(key.encode())): self.value_type.p_type(decode(value.encode()))
        }

    cpdef dict p_type(self, string):
        return self._convert(string)

    cpdef dict convert(self, bytes value):
        return self._convert(value.decode())


cdef class ArrayType:

    cdef:
        str name
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(
            RE_ARRAY.findall(name)[0], container=True
        )

    cdef list _convert(self, str string):
        return [self.type.p_type(decode(val.encode())) for val in seq_parser(string[1:-1])]

    cpdef list p_type(self, str string):
        return self._convert(string)

    cpdef list convert(self, bytes value):
        return self.p_type(value.decode())


cdef class NestedType:
    
    cdef:
        str name
        bint container
        tuple types

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.types = tuple(
            what_py_type(i.split()[1], container=True)
            for i in RE_NESTED.findall(name)[0].split(',')
        )

    cdef list _convert(self, str string):
        return self.p_type(string)

    cpdef list p_type(self, str string):
        result = []
        for val in seq_parser(string[1:-1]):
            temp = []
            for tp, elem in zip(self.types, seq_parser(val.strip("()"))):
                temp.append(tp.p_type(decode(elem.encode())))
            result.append(tuple(temp))
        return result
    
    cpdef list convert(self, bytes value):
        return self._convert(value.decode())

cdef class NullableType:

    cdef:
        str name
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(RE_NULLABLE.findall(name)[0], container)

    cdef _convert(self, str string):
        if string == r"\N" or string == "NULL":
            return None
        return self.type.p_type(string)

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(decode(value))


cdef class NothingType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef void p_type(self, str string):
        pass

    cpdef void convert(self, bytes value):
        pass


cdef class UUIDType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        return UUID(string.strip("'"))

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class IPv4Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        return IPv4Address(string.strip("'"))

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class IPv6Type:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cdef object _convert(self, str string):
        return IPv6Address(string.strip("'"))

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(value.decode())


cdef class LowCardinalityType:

    cdef:
        str name
        bint container
        type

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container
        self.type = what_py_type(RE_LOW_CARDINALITY.findall(name)[0], container)

    cdef _convert(self, str string):
        return self.type.p_type(string)

    cpdef object p_type(self, str string):
        return self._convert(string)

    cpdef object convert(self, bytes value):
        return self._convert(decode(value))


cdef class DecimalType:

    cdef:
        str name
        bint container

    def __cinit__(self, str name, bint container):
        self.name = name
        self.container = container

    cpdef object p_type(self, str string):
        return Decimal(string)

    cpdef object convert(self, bytes value):
        return Decimal(value.decode())


cdef dict CH_TYPES_MAPPING = {
    "Bool": BoolType,
    "UInt8": UInt8Type,
    "UInt16": UInt16Type,
    "UInt32": UInt32Type,
    "UInt64": UInt64Type,
    "UInt128": Int128Type,
    "UInt256": Int256Type,
    "Int8": Int8Type,
    "Int16": Int16Type,
    "Int32": Int32Type,
    "Int64": Int64Type,
    "Int128": Int128Type,
    "Int256": Int256Type,
    "Float32": FloatType,
    "Float64": FloatType,
    "String": StrType,
    "FixedString": StrType,
    "Enum8": StrType,
    "Enum16": StrType,
    "Date": DateType,
    "DateTime": DateTimeType,
    "DateTime64": DateTime64Type,
    "Tuple": TupleType,
    "Array": ArrayType,
    "Map": MapType,
    "Nullable": NullableType,
    "Nothing": NothingType,
    "UUID": UUIDType,
    "LowCardinality": LowCardinalityType,
    "Decimal": DecimalType,
    "Decimal32": DecimalType,
    "Decimal64": DecimalType,
    "Decimal128": DecimalType,
    "IPv4": IPv4Type,
    "IPv6": IPv6Type,
    "Nested": NestedType,
}


cdef what_py_type(str name, bint container = False):
    """ Returns needed type class from clickhouse type name """
    name = name.strip()
    try:
        if name.startswith('SimpleAggregateFunction') or name.startswith('AggregateFunction'):
            ch_type = re.findall(r',(.*)\)', name)[0].strip()
        else:
            ch_type = name.split("(")[0]
        return CH_TYPES_MAPPING[ch_type](name, container=container)
    except KeyError:
        raise ChClientError(f"Unrecognized type name: '{name}'")


cpdef what_py_converter(str name, bint container = False):
    """ Returns needed type class from clickhouse type name """
    return what_py_type(name, container).convert


cdef bytes unconvert_str(str value):
    cdef:
        list res = ["'"]
        int i, sl = len(value)
    for i in range(sl):
        if value[i] == "\\":
            res.append("\\\\")
        elif value[i] == "'":
            res.append("\\'")
        else:
            res.append(value[i])
    res.append("'")
    return PyUnicode_AsEncodedString(PyUnicode_Join("", res), NULL, NULL)


cdef bytes unconvert_bool(object value):
    # We use an integer representation here to be compatible with older
    # ClickHouse versions that represent booleans as UInt8 values.
    return b"1" if value else b"0"


cdef bytes unconvert_int(object value):
    return b"%d" % value


cdef bytes unconvert_float(double value):
    return f"{value}".encode('latin-1')


cdef bytes unconvert_date(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_datetime(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_tuple(tuple value):
    return b"(" + b",".join(py2ch(elem) for elem in value) + b")"

cdef bytes unconvert_dict(dict value):
    return (
        b"{" +
        b','.join(py2ch(key) + b':' + py2ch(val) for key, val in value.items()) +
        b"}"
    )

cdef bytes unconvert_array(list value):
    return b"[" + b",".join(py2ch(elem) for elem in value) + b"]"


cdef bytes unconvert_nullable(object value):
    return b"NULL"


cdef bytes unconvert_uuid(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_ipaddress(object value):
    return f"'{value}'".encode('latin-1')


cdef bytes unconvert_decimal(object value):
    return f'{value}'.encode('latin-1')


cdef dict PY_TYPES_MAPPING = {
    bool: unconvert_bool,
    int: unconvert_int,
    float: unconvert_float,
    str: unconvert_str,
    date: unconvert_date,
    datetime: unconvert_datetime,
    tuple: unconvert_tuple,
    dict: unconvert_dict,
    list: unconvert_array,
    type(None): unconvert_nullable,
    UUID: unconvert_uuid,
    Decimal: unconvert_decimal,
    IPv4Address: unconvert_ipaddress,
    IPv6Address: unconvert_ipaddress,
}


cpdef bytes py2ch(value):
    try:
        return PY_TYPES_MAPPING[type(value)](value)
    except KeyError:
        raise ChClientError(
            f"Unrecognized type: '{type(value)}'. "
            f"The value type should be exactly one of "
            f"int, float, str, dt.date, dt.datetime, dict, tuple, list, uuid.UUID (or None). "
            f"No subclasses yet."
        )

def rows2ch(*rows):
    return b",".join(unconvert_tuple(tuple(row)) for row in rows)


def json2ch(*records, dumps):
    return dumps(records)[1:-1]

def empty_convertor(bytes value):
    return value
