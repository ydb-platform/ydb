# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


# KNOWN TYPES
from mo_parsing import (
    Forward,
    Group,
    Optional,
    MatchFirst,
    Literal,
    ZeroOrMore,
    set_parser_names,
    delimited_list,
    RIGHT_ASSOC,
    LEFT_ASSOC,
    Keyword,
    Combine,
)

from mo_sql_parsing.keywords import (
    RB,
    LB,
    POS,
    NEG,
    NOT,
    BINARY_NOT,
    NULL,
    EQ,
    KNOWN_OPS,
    LT,
    GT,
    AS,
)
from mo_sql_parsing.utils import (
    keyword,
    to_json_call,
    int_num,
    ansi_string,
    ansi_ident,
    assign,
    flag,
    simple_ident,
    to_flat_column_type,
)

_size = Optional(LB + int_num("params") + RB)
_char_set = Optional(assign("character set", simple_ident))

_sizes = Optional(LB + delimited_list(int_num("params")) + RB)

_type_attrs = Optional(flag("unsigned")) + Optional(flag("zerofill"))

simple_types = Forward()

BIGINT = Group(keyword("bigint")("op") + Optional(_size) + _type_attrs) / to_json_call
BIT = (keyword("bit")("op") + _size) / to_json_call
BOOL = Group(keyword("bool")("op")) / to_json_call
BOOLEAN = Group(keyword("boolean")("op")) / to_json_call
DOUBLE = Group(keyword("double")("op") + Optional(_sizes) + _type_attrs) / to_json_call
BIGNUMERIC = Group(keyword("bignumeric")("op")) / to_json_call
BIGDECIMAL = Group(keyword("bigdecimal")("op")) / to_json_call
BYTEINT = Group(keyword("byteint")("op")) / to_json_call
FLOAT = Group(keyword("float")("op") + Optional(_sizes) + _type_attrs) / to_json_call
FLOAT64 = Group(keyword("float64")("op")) / to_json_call
GEOMETRY = Group(keyword("geometry")("op")) / to_json_call
INTEGER = Group(keyword("integer")("op") + _size + _type_attrs) / to_json_call
INTERVAL = Group(keyword("interval")("op")) / to_json_call
INT = (keyword("int")("op") + _size + _type_attrs) / to_json_call
INT32 = Group(keyword("int32")("op")) / to_json_call
INT64 = Group(keyword("int64")("op")) / to_json_call
MEDIUMINT = Group(keyword("mediumint")("op") + _size + _type_attrs) / to_json_call
SMALLINT = Group(keyword("smallint")("op") + _size + _type_attrs) / to_json_call
REAL = Group(keyword("real")("op") + Optional(flag("unsigned"))) / to_json_call
SIGNED = Group(flag("unsigned") + Optional(keyword("integer"))("op") / "bigint") / to_json_call
TINYINT = (keyword("tinyint")("op") + _size + _type_attrs) / to_json_call
UNSIGNED = (
    Group(Keyword("signed", caseless=True).suppress() + Optional(keyword("integer"))("op") / "bigint") / to_json_call
)
VARYING = Group(keyword("varying")("op")) / to_json_call

BLOB = (keyword("blob")("op") + _size) / to_json_call
BYTES = (keyword("bytes")("op") + _size) / to_json_call

CHAR = (
    Combine(
        (Keyword("char", caseless=True) | Keyword("character", caseless=True))
        + Optional(Keyword("varying", caseless=True) / "_varying")
    )("op")
    + _size
    + _char_set
) / to_json_call

NCHAR = (keyword("nchar")("op") + _size + _char_set) / to_json_call
NVARCHAR = (keyword("nvarchar")("op") + _size + _char_set) / to_json_call
VARCHAR = (keyword("varchar")("op") + _size + _char_set) / to_json_call
VARCHAR2 = (keyword("varchar2")("op") + _size + _char_set) / to_json_call
VARBINARY = (keyword("varbinary")("op") + _size + _char_set) / to_json_call
TEXT = Group(keyword("text")("op") + _char_set) / to_json_call
STRING = Group(keyword("string")("op") + _char_set) / to_json_call
USING_T = Group(keyword("using")("op") + simple_ident("params")) / to_json_call
UUID = Group(keyword("uuid")("op")) / to_json_call

DECIMAL = (keyword("decimal")("op") + _sizes + _type_attrs) / to_json_call
DOUBLE_PRECISION = Group((keyword("double precision") / "double_precision")("op")) / to_json_call
NUMERIC = (keyword("numeric")("op") + _sizes) / to_json_call
NUMBER = (keyword("number")("op") + _sizes) / to_json_call

MAP_TYPE = (keyword("map")("op") + LB + delimited_list(simple_types("params")) + RB) / to_json_call
ARRAY_TYPE = (keyword("array")("op") + LB + simple_types("params") + RB) / to_json_call
JSON = Group(keyword("json")("op")) / to_json_call
JSONB = Group(keyword("jsonb")("op")) / to_json_call

DATE = keyword("date")
DATETIME = keyword("datetime")
DATETIMEOFFSET = keyword("datetimeoffset")
DATETIME_W_TIMEZONE = keyword("datetime with time zone")
TIME = keyword("time")
TIMESTAMP = keyword("timestamp")
TIMESTAMP_W_TIMEZONE = keyword("timestamp with time zone")
TIMESTAMPTZ = keyword("timestamptz")
TIMETZ = keyword("timetz")

time_functions = DATE | DATETIME | TIME | TIMESTAMP | TIMESTAMPTZ | TIMETZ

# KNOWNN TIME TYPES
_format = Optional((ansi_string | ansi_ident)("params") | _size)

DATE_TYPE = (DATE("op") + _format) / to_json_call
DATETIME_TYPE = (DATETIME("op") + _format) / to_json_call
DATETIMEOFFSET_TYPE = (DATETIMEOFFSET("op") + _format) / to_json_call
DATETIME_W_TIMEZONE_TYPE = (DATETIME_W_TIMEZONE("op") + _format) / to_json_call
TIME_TYPE = (TIME("op") + _format) / to_json_call
TIMESTAMP_TYPE = (TIMESTAMP("op") + _format) / to_json_call
TIMESTAMP_W_TIMEZONE_TYPE = (TIMESTAMP_W_TIMEZONE("op") + _format) / to_json_call
TIMESTAMPTZ_TYPE = (TIMESTAMPTZ("op") + _format) / to_json_call
TIMETZ_TYPE = (TIMETZ("op") + _format) / to_json_call

simple_types << MatchFirst([
    ARRAY_TYPE,
    BIGINT,
    BIT,
    BOOL,
    BOOLEAN,
    BLOB,
    BYTES,
    CHAR,
    DATE_TYPE,
    DATETIME_W_TIMEZONE_TYPE,
    DATETIMEOFFSET_TYPE,
    DATETIME_TYPE,
    DECIMAL,
    DOUBLE_PRECISION,
    DOUBLE,
    FLOAT64,
    BIGNUMERIC,
    BIGDECIMAL,
    FLOAT,
    GEOMETRY,
    MAP_TYPE,
    INT,
    INTEGER,
    INTERVAL,
    INT32,
    INT64,
    BYTEINT,
    JSON,
    JSONB,
    NCHAR,
    NVARCHAR,
    NUMBER,
    NUMERIC,
    REAL,
    TEXT,
    MEDIUMINT,
    SMALLINT,
    STRING,
    SIGNED,
    UNSIGNED,
    TIME_TYPE,
    TIMESTAMP_W_TIMEZONE_TYPE,
    TIMESTAMP_TYPE,
    TIMESTAMPTZ_TYPE,
    TIMETZ_TYPE,
    TINYINT,
    USING_T,
    UUID,
    VARCHAR,
    VARCHAR2,
    VARBINARY,
])

CASTING = (Literal("::").suppress() + simple_types("params")).set_parser_name("cast")
KNOWN_OPS.insert(0, CASTING)

unary_ops = {
    POS: RIGHT_ASSOC,
    NEG: RIGHT_ASSOC,
    NOT: RIGHT_ASSOC,
    BINARY_NOT: RIGHT_ASSOC,
    CASTING: LEFT_ASSOC,
}

set_parser_names()


def get_column_type(expr, identifier, literal_string):
    column_definition = Forward()
    column_type = Forward()

    # eg  AS STRING FORMAT 'YYYY-MM'
    string_type = (keyword("string format")("op") + literal_string("params")) / to_json_call

    struct_type = (
        keyword("struct")("op") + LT.suppress() + Group(delimited_list(column_definition))("params") + GT.suppress()
    ) / to_json_call

    row_type = (keyword("row")("op") + LB + Group(delimited_list(column_definition))("params") + RB) / to_json_call

    array_type = (
        keyword("array")("op")
        + (
            (LT.suppress() + Group(delimited_list(column_type))("params") + GT.suppress())
            | (LB + Group(delimited_list(column_type))("params") + RB)
        )
    ) / to_json_call

    # ENUM('G','PG','PG-13','R','NC-17')
    enum_type = assign("enum", LB + delimited_list(literal_string) + RB)
    set_type = assign("set", LB + delimited_list(literal_string) + RB)

    column_type << (
        struct_type | row_type | array_type | enum_type | set_type | string_type | simple_types
    )("type") + Optional(AS + LB + expr("value") + RB)

    column_def_identity = (
        assign("generated", (keyword("always") | keyword("by default") / "by_default"),)
        + keyword("as identity").suppress()
        + Optional(assign("start with", int_num))
        + Optional(assign("increment by", int_num))
    )

    column_def_references = assign(
        "references", identifier("table") + LB + delimited_list(identifier)("columns") + RB,
    )

    column_options = (
        (NOT + NULL)("nullable") / False
        | keyword("not enforced")("enforced") / False
        | (NULL / True)("nullable")
        | flag("unique")
        | flag("auto_increment")
        | flag("autoincrement")
        | assign("comment", literal_string)
        | assign("encode", identifier)
        | assign("character set", identifier)
        | assign("collate", Optional(EQ) + identifier)
        | flag("primary key")
        | column_def_identity("identity")
        | column_def_references
        | assign("check", LB + expr + RB)
        | assign("default", expr)
        | assign("on update", expr)
    )

    column_definition << Group(
        identifier("name") + (column_type | identifier("type")) + ZeroOrMore(column_options)
    ) / to_flat_column_type

    variable_options = assign("default", expr) | EQ + expr("default")
    declare_variable = assign(
        "declare",
        delimited_list(Group(identifier("name") + Optional(AS) + simple_types("type") + ZeroOrMore(variable_options))),
    )

    set_parser_names()

    return column_type, column_definition, column_def_references, column_options, declare_variable
