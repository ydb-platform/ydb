from datetime import (
    date as Date,
    datetime as Datetime,
    time as Time,
    timedelta as Timedelta,
    timezone as Timezone,
)
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from ipaddress import (
    IPv4Address,
    IPv4Network,
    IPv6Address,
    IPv6Network,
    ip_address,
    ip_network,
)
from json import dumps, loads
from uuid import UUID

from dateutil.parser import ParserError, parse

from pg8000.exceptions import InterfaceError
from pg8000.types import PGInterval, Range


ANY_ARRAY = 2277
BIGINT = 20
BIGINT_ARRAY = 1016
BOOLEAN = 16
BOOLEAN_ARRAY = 1000
BYTES = 17
BYTES_ARRAY = 1001
CHAR = 1042
CHAR_ARRAY = 1014
CIDR = 650
CIDR_ARRAY = 651
CSTRING = 2275
CSTRING_ARRAY = 1263
DATE = 1082
DATE_ARRAY = 1182
DATEMULTIRANGE = 4535
DATEMULTIRANGE_ARRAY = 6155
DATERANGE = 3912
DATERANGE_ARRAY = 3913
FLOAT = 701
FLOAT_ARRAY = 1022
INET = 869
INET_ARRAY = 1041
INT2VECTOR = 22
INT4MULTIRANGE = 4451
INT4MULTIRANGE_ARRAY = 6150
INT4RANGE = 3904
INT4RANGE_ARRAY = 3905
INT8MULTIRANGE = 4536
INT8MULTIRANGE_ARRAY = 6157
INT8RANGE = 3926
INT8RANGE_ARRAY = 3927
INTEGER = 23
INTEGER_ARRAY = 1007
INTERVAL = 1186
INTERVAL_ARRAY = 1187
OID = 26
JSON = 114
JSON_ARRAY = 199
JSONB = 3802
JSONB_ARRAY = 3807
MACADDR = 829
MONEY = 790
MONEY_ARRAY = 791
NAME = 19
NAME_ARRAY = 1003
NUMERIC = 1700
NUMERIC_ARRAY = 1231
NUMRANGE = 3906
NUMRANGE_ARRAY = 3907
NUMMULTIRANGE = 4532
NUMMULTIRANGE_ARRAY = 6151
NULLTYPE = -1
OID = 26
POINT = 600
REAL = 700
REAL_ARRAY = 1021
RECORD = 2249
SMALLINT = 21
SMALLINT_ARRAY = 1005
SMALLINT_VECTOR = 22
STRING = 1043
TEXT = 25
TEXT_ARRAY = 1009
TIME = 1083
TIME_ARRAY = 1183
TIMESTAMP = 1114
TIMESTAMP_ARRAY = 1115
TIMESTAMPTZ = 1184
TIMESTAMPTZ_ARRAY = 1185
TSMULTIRANGE = 4533
TSMULTIRANGE_ARRAY = 6152
TSRANGE = 3908
TSRANGE_ARRAY = 3909
TSTZMULTIRANGE = 4534
TSTZMULTIRANGE_ARRAY = 6153
TSTZRANGE = 3910
TSTZRANGE_ARRAY = 3911
UNKNOWN = 705
UUID_TYPE = 2950
UUID_ARRAY = 2951
VARCHAR = 1043
VARCHAR_ARRAY = 1015
XID = 28


MIN_INT2, MAX_INT2 = -(2**15), 2**15
MIN_INT4, MAX_INT4 = -(2**31), 2**31
MIN_INT8, MAX_INT8 = -(2**63), 2**63


def bool_in(data):
    return data == "t"


def bool_out(v):
    return "true" if v else "false"


def bytes_in(data):
    return bytes.fromhex(data[2:])


def bytes_out(v):
    return "\\x" + v.hex()


def cidr_out(v):
    return str(v)


def cidr_in(data):
    return ip_network(data, False) if "/" in data else ip_address(data)


def date_in(data):
    if data in ("infinity", "-infinity"):
        return data
    else:
        try:
            return Datetime.strptime(data, "%Y-%m-%d").date()
        except ValueError:
            # pg date can overflow Python Datetime
            return data


def date_out(v):
    return v.isoformat()


def datetime_out(v):
    if v.tzinfo is None:
        return v.isoformat()
    else:
        return v.astimezone(Timezone.utc).isoformat()


def enum_out(v):
    return str(v.value)


def float_out(v):
    return str(v)


def inet_in(data):
    return ip_network(data, False) if "/" in data else ip_address(data)


def inet_out(v):
    return str(v)


def int_in(data):
    return int(data)


def int_out(v):
    return str(v)


def interval_in(data):
    pg_interval = PGInterval.from_str(data)
    try:
        return pg_interval.to_timedelta()
    except ValueError:
        return pg_interval


def interval_out(v):
    return f"{v.days} days {v.seconds} seconds {v.microseconds} microseconds"


def json_in(data):
    return loads(data)


def json_out(v):
    return dumps(v)


def null_out(v):
    return None


def numeric_in(data):
    return Decimal(data)


def numeric_out(d):
    return str(d)


def point_in(data):
    return tuple(map(float, data[1:-1].split(",")))


def pg_interval_in(data):
    return PGInterval.from_str(data)


def pg_interval_out(v):
    return str(v)


def range_out(v):
    if v.is_empty:
        return "empty"
    else:
        le = v.lower
        val_lower = "" if le is None else make_param(PY_TYPES, le)
        ue = v.upper
        val_upper = "" if ue is None else make_param(PY_TYPES, ue)
        return f"{v.bounds[0]}{val_lower},{val_upper}{v.bounds[1]}"


def string_in(data):
    return data


def string_out(v):
    return v


def time_in(data):
    pattern = "%H:%M:%S.%f" if "." in data else "%H:%M:%S"
    return Datetime.strptime(data, pattern).time()


def time_out(v):
    return v.isoformat()


def timestamp_in(data):
    if data in ("infinity", "-infinity"):
        return data

    try:
        pattern = "%Y-%m-%d %H:%M:%S.%f" if "." in data else "%Y-%m-%d %H:%M:%S"
        return Datetime.strptime(data, pattern)
    except ValueError:
        try:
            return parse(data)
        except ParserError:
            # pg timestamp can overflow Python Datetime
            return data


def timestamptz_in(data):
    if data in ("infinity", "-infinity"):
        return data

    try:
        patt = "%Y-%m-%d %H:%M:%S.%f%z" if "." in data else "%Y-%m-%d %H:%M:%S%z"
        return Datetime.strptime(f"{data}00", patt)
    except ValueError:
        try:
            return parse(data)
        except ParserError:
            # pg timestamptz can overflow Python Datetime
            return data


def unknown_out(v):
    return str(v)


def vector_in(data):
    return [int(v) for v in data.split()]


def uuid_out(v):
    return str(v)


def uuid_in(data):
    return UUID(data)


def _range_in(elem_func):
    def range_in(data):
        if data == "empty":
            return Range(is_empty=True)
        else:
            le, ue = [None if v == "" else elem_func(v) for v in data[1:-1].split(",")]
            return Range(le, ue, bounds=f"{data[0]}{data[-1]}")

    return range_in


daterange_in = _range_in(date_in)
int4range_in = _range_in(int)
int8range_in = _range_in(int)
numrange_in = _range_in(Decimal)


def ts_in(data):
    return timestamp_in(data[1:-1])


def tstz_in(data):
    return timestamptz_in(data[1:-1])


tsrange_in = _range_in(ts_in)
tstzrange_in = _range_in(tstz_in)


def _multirange_in(adapter):
    def f(data):
        in_range = False
        result = []
        val = []
        for c in data:
            if in_range:
                val.append(c)
                if c in "])":
                    value = "".join(val)
                    val.clear()
                    result.append(adapter(value))
                    in_range = False
            elif c in "[(":
                val.append(c)
                in_range = True

        return result

    return f


datemultirange_in = _multirange_in(daterange_in)
int4multirange_in = _multirange_in(int4range_in)
int8multirange_in = _multirange_in(int8range_in)
nummultirange_in = _multirange_in(numrange_in)
tsmultirange_in = _multirange_in(tsrange_in)
tstzmultirange_in = _multirange_in(tstzrange_in)


class ParserState(Enum):
    InString = 1
    InEscape = 2
    InValue = 3
    Out = 4


def _parse_array(data, adapter):
    state = ParserState.Out
    stack = [[]]
    val = []
    for c in data:
        if state == ParserState.InValue:
            if c in ("}", ","):
                value = "".join(val)
                stack[-1].append(None if value == "NULL" else adapter(value))
                state = ParserState.Out
            else:
                val.append(c)

        if state == ParserState.Out:
            if c == "{":
                a = []
                stack[-1].append(a)
                stack.append(a)
            elif c == "}":
                stack.pop()
            elif c == ",":
                pass
            elif c == '"':
                val = []
                state = ParserState.InString
            else:
                val = [c]
                state = ParserState.InValue

        elif state == ParserState.InString:
            if c == '"':
                stack[-1].append(adapter("".join(val)))
                state = ParserState.Out
            elif c == "\\":
                state = ParserState.InEscape
            else:
                val.append(c)
        elif state == ParserState.InEscape:
            val.append(c)
            state = ParserState.InString

    return stack[0][0]


def _array_in(adapter):
    def f(data):
        return _parse_array(data, adapter)

    return f


bool_array_in = _array_in(bool_in)
bytes_array_in = _array_in(bytes_in)
cidr_array_in = _array_in(cidr_in)
date_array_in = _array_in(date_in)
datemultirange_array_in = _array_in(datemultirange_in)
daterange_array_in = _array_in(daterange_in)
inet_array_in = _array_in(inet_in)
int_array_in = _array_in(int)
int4multirange_array_in = _array_in(int4multirange_in)
int4range_array_in = _array_in(int4range_in)
int8multirange_array_in = _array_in(int8multirange_in)
int8range_array_in = _array_in(int8range_in)
interval_array_in = _array_in(interval_in)
json_array_in = _array_in(json_in)
float_array_in = _array_in(float)
numeric_array_in = _array_in(numeric_in)
nummultirange_array_in = _array_in(nummultirange_in)
numrange_array_in = _array_in(numrange_in)
string_array_in = _array_in(string_in)
time_array_in = _array_in(time_in)
timestamp_array_in = _array_in(timestamp_in)
timestamptz_array_in = _array_in(timestamptz_in)
tsrange_array_in = _array_in(tsrange_in)
tsmultirange_array_in = _array_in(tsmultirange_in)
tstzmultirange_array_in = _array_in(tstzmultirange_in)
tstzrange_array_in = _array_in(tstzrange_in)
uuid_array_in = _array_in(uuid_in)


def array_string_escape(v):
    cs = []
    for c in v:
        if c == "\\":
            cs.append("\\")
        elif c == '"':
            cs.append("\\")
        cs.append(c)
    val = "".join(cs)
    if (
        len(val) == 0
        or val == "NULL"
        or any(c.isspace() for c in val)
        or any(c in val for c in ("{", "}", ",", "\\"))
    ):
        val = f'"{val}"'
    return val


@singledispatch
def array_out(val):
    return make_param(PY_TYPES, val)


@array_out.register
def _(val: list):
    result = [array_out(v) for v in val]
    return f'{{{",".join(result)}}}'


@array_out.register
def _(val: tuple):
    return f'"{composite_out(val)}"'


@array_out.register
def _(val: None):
    return "NULL"


@array_out.register
def _(val: dict):
    return array_string_escape(json_out(val))


@array_out.register(bytes)
@array_out.register(bytearray)
def _(val):
    return f'"\\{bytes_out(val)}"'


@array_out.register
def _(val: str):
    return array_string_escape(val)


@singledispatch
def composite_out(val):
    return array_out(val)


@composite_out.register
def _(val: tuple):
    result = [composite_out(v) for v in val]

    return f'({",".join(result)})'


@composite_out.register
def _(val: None):
    return ""


def record_in(data):
    state = ParserState.Out
    results = []
    val = []
    for c in data:
        if state == ParserState.InValue:
            if c in (")", ","):
                value = "".join(val)
                val.clear()
                results.append(None if value == "" else value)
                state = ParserState.Out
            else:
                val.append(c)

        if state == ParserState.Out:
            if c in "(),":
                pass
            elif c == '"':
                state = ParserState.InString
            else:
                val.append(c)
                state = ParserState.InValue

        elif state == ParserState.InString:
            if c == '"':
                results.append("".join(val))
                val.clear()
                state = ParserState.Out
            elif c == "\\":
                state = ParserState.InEscape
            else:
                val.append(c)

        elif state == ParserState.InEscape:
            val.append(c)
            state = ParserState.InString

    return tuple(results)


PY_PG = {
    Date: DATE,
    Decimal: NUMERIC,
    IPv4Address: INET,
    IPv6Address: INET,
    IPv4Network: INET,
    IPv6Network: INET,
    PGInterval: INTERVAL,
    Time: TIME,
    Timedelta: INTERVAL,
    UUID: UUID_TYPE,
    bool: BOOLEAN,
    bytearray: BYTES,
    dict: JSONB,
    float: FLOAT,
    type(None): NULLTYPE,
    bytes: BYTES,
    str: TEXT,
}


PY_TYPES = {
    Date: date_out,  # date
    Datetime: datetime_out,
    Decimal: numeric_out,  # numeric
    Enum: enum_out,  # enum
    IPv4Address: inet_out,  # inet
    IPv6Address: inet_out,  # inet
    IPv4Network: inet_out,  # inet
    IPv6Network: inet_out,  # inet
    PGInterval: interval_out,  # interval
    Range: range_out,  # range types
    Time: time_out,  # time
    Timedelta: interval_out,  # interval
    UUID: uuid_out,  # uuid
    bool: bool_out,  # bool
    bytearray: bytes_out,  # bytea
    dict: json_out,  # jsonb
    float: float_out,  # float8
    type(None): null_out,  # null
    bytes: bytes_out,  # bytea
    str: string_out,  # unknown
    int: int_out,
    list: array_out,
    tuple: composite_out,
}


PG_TYPES = {
    BIGINT: int,  # int8
    BIGINT_ARRAY: int_array_in,  # int8[]
    BOOLEAN: bool_in,  # bool
    BOOLEAN_ARRAY: bool_array_in,  # bool[]
    BYTES: bytes_in,  # bytea
    BYTES_ARRAY: bytes_array_in,  # bytea[]
    CHAR: string_in,  # char
    CHAR_ARRAY: string_array_in,  # char[]
    CIDR_ARRAY: cidr_array_in,  # cidr[]
    CSTRING: string_in,  # cstring
    CSTRING_ARRAY: string_array_in,  # cstring[]
    DATE: date_in,  # date
    DATE_ARRAY: date_array_in,  # date[]
    DATEMULTIRANGE: datemultirange_in,  # datemultirange
    DATEMULTIRANGE_ARRAY: datemultirange_array_in,  # datemultirange[]
    DATERANGE: daterange_in,  # daterange
    DATERANGE_ARRAY: daterange_array_in,  # daterange[]
    FLOAT: float,  # float8
    FLOAT_ARRAY: float_array_in,  # float8[]
    INET: inet_in,  # inet
    INET_ARRAY: inet_array_in,  # inet[]
    INT4MULTIRANGE: int4multirange_in,  # int4multirange
    INT4MULTIRANGE_ARRAY: int4multirange_array_in,  # int4multirange[]
    INT4RANGE: int4range_in,  # int4range
    INT4RANGE_ARRAY: int4range_array_in,  # int4range[]
    INT8MULTIRANGE: int8multirange_in,  # int8multirange
    INT8MULTIRANGE_ARRAY: int8multirange_array_in,  # int8multirange[]
    INT8RANGE: int8range_in,  # int8range
    INT8RANGE_ARRAY: int8range_array_in,  # int8range[]
    INTEGER: int,  # int4
    INTEGER_ARRAY: int_array_in,  # int4[]
    JSON: json_in,  # json
    JSON_ARRAY: json_array_in,  # json[]
    JSONB: json_in,  # jsonb
    JSONB_ARRAY: json_array_in,  # jsonb[]
    MACADDR: string_in,  # MACADDR type
    MONEY: string_in,  # money
    MONEY_ARRAY: string_array_in,  # money[]
    NAME: string_in,  # name
    NAME_ARRAY: string_array_in,  # name[]
    NUMERIC: numeric_in,  # numeric
    NUMERIC_ARRAY: numeric_array_in,  # numeric[]
    NUMRANGE: numrange_in,  # numrange
    NUMRANGE_ARRAY: numrange_array_in,  # numrange[]
    NUMMULTIRANGE: nummultirange_in,  # nummultirange
    NUMMULTIRANGE_ARRAY: nummultirange_array_in,  # nummultirange[]
    OID: int,  # oid
    POINT: point_in,  # point
    INTERVAL: interval_in,  # interval
    INTERVAL_ARRAY: interval_array_in,  # interval[]
    REAL: float,  # float4
    REAL_ARRAY: float_array_in,  # float4[]
    RECORD: record_in,  # record
    SMALLINT: int,  # int2
    SMALLINT_ARRAY: int_array_in,  # int2[]
    SMALLINT_VECTOR: vector_in,  # int2vector
    TEXT: string_in,  # text
    TEXT_ARRAY: string_array_in,  # text[]
    TIME: time_in,  # time
    TIME_ARRAY: time_array_in,  # time[]
    TIMESTAMP: timestamp_in,  # timestamp
    TIMESTAMP_ARRAY: timestamp_array_in,  # timestamp
    TIMESTAMPTZ: timestamptz_in,  # timestamptz
    TIMESTAMPTZ_ARRAY: timestamptz_array_in,  # timestamptz
    TSMULTIRANGE: tsmultirange_in,  # tsmultirange
    TSMULTIRANGE_ARRAY: tsmultirange_array_in,  # tsmultirange[]
    TSRANGE: tsrange_in,  # tsrange
    TSRANGE_ARRAY: tsrange_array_in,  # tsrange[]
    TSTZMULTIRANGE: tstzmultirange_in,  # tstzmultirange
    TSTZMULTIRANGE_ARRAY: tstzmultirange_array_in,  # tstzmultirange[]
    TSTZRANGE: tstzrange_in,  # tstzrange
    TSTZRANGE_ARRAY: tstzrange_array_in,  # tstzrange[]
    UNKNOWN: string_in,  # unknown
    UUID_ARRAY: uuid_array_in,  # uuid[]
    UUID_TYPE: uuid_in,  # uuid
    VARCHAR: string_in,  # varchar
    VARCHAR_ARRAY: string_array_in,  # varchar[]
    XID: int,  # xid
}


# PostgreSQL encodings:
# https://www.postgresql.org/docs/current/multibyte.html
#
# Python encodings:
# https://docs.python.org/3/library/codecs.html
#
# Commented out encodings don't require a name change between PostgreSQL and
# Python.  If the py side is None, then the encoding isn't supported.
PG_PY_ENCODINGS = {
    # Not supported:
    "mule_internal": None,
    "euc_tw": None,
    # Name fine as-is:
    # "euc_jp",
    # "euc_jis_2004",
    # "euc_kr",
    # "gb18030",
    # "gbk",
    # "johab",
    # "sjis",
    # "shift_jis_2004",
    # "uhc",
    # "utf8",
    # Different name:
    "euc_cn": "gb2312",
    "iso_8859_5": "is8859_5",
    "iso_8859_6": "is8859_6",
    "iso_8859_7": "is8859_7",
    "iso_8859_8": "is8859_8",
    "koi8": "koi8_r",
    "latin1": "iso8859-1",
    "latin2": "iso8859_2",
    "latin3": "iso8859_3",
    "latin4": "iso8859_4",
    "latin5": "iso8859_9",
    "latin6": "iso8859_10",
    "latin7": "iso8859_13",
    "latin8": "iso8859_14",
    "latin9": "iso8859_15",
    "sql_ascii": "ascii",
    "win866": "cp886",
    "win874": "cp874",
    "win1250": "cp1250",
    "win1251": "cp1251",
    "win1252": "cp1252",
    "win1253": "cp1253",
    "win1254": "cp1254",
    "win1255": "cp1255",
    "win1256": "cp1256",
    "win1257": "cp1257",
    "win1258": "cp1258",
    "unicode": "utf-8",  # Needed for Amazon Redshift
}


def make_param(py_types, value):
    try:
        func = py_types[type(value)]
    except KeyError:
        func = str
        for k, v in py_types.items():
            try:
                if isinstance(value, k):
                    func = v
                    break
            except TypeError:
                pass

    return func(value)


def make_params(py_types, values):
    return tuple([make_param(py_types, v) for v in values])


def identifier(sql):
    if not isinstance(sql, str):
        raise InterfaceError("identifier must be a str")

    if len(sql) == 0:
        raise InterfaceError("identifier must be > 0 characters in length")

    if "\u0000" in sql:
        raise InterfaceError("identifier cannot contain the code zero character")

    sql = sql.replace('"', '""')
    return f'"{sql}"'


@singledispatch
def literal(value):
    val = str(value).replace("'", "''")
    return f"'{val}'"


@literal.register
def _(value: None):
    return "NULL"


@literal.register
def _(value: bool):
    return "TRUE" if value else "FALSE"


@literal.register(int)
@literal.register(float)
@literal.register(Decimal)
def _(value):
    return str(value)


@literal.register(bytes)
@literal.register(bytearray)
def _(value):
    return f"X'{value.hex()}'"


@literal.register
def _(value: Datetime):
    return f"'{datetime_out(value)}'"


@literal.register
def _(value: Date):
    return f"'{date_out(value)}'"


@literal.register
def _(value: Time):
    return f"'{time_out(value)}'"


@literal.register
def _(value: Timedelta):
    return f"'{interval_out(value)}'"


@literal.register
def _(value: list):
    return f"{literal(array_out(value))}"
