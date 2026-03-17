# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# SQL CONSTANTS
from mo_parsing import *

from mo_sql_parsing.utils import SQL_NULL, keyword

NULL = keyword("null") / SQL_NULL
TRUE = keyword("true") / True
FALSE = keyword("false") / False

# SIMPLE KEYWORDS AND OPERATORS
ADD = Literal("+").set_parser_name("add")
ALL = keyword("all")
AND = keyword("and")
APPLY = keyword("apply")
AS = keyword("as").suppress()
ASC = keyword("asc")
ASSIGN = Literal(":=").set_parser_name("assign")
BEGIN = keyword("begin").suppress()
BETWEEN = keyword("between")
BINARY_AND = Literal("&").set_parser_name("binary_and")
BINARY_NOT = Literal("~").set_parser_name("binary_not")
BINARY_OR = Literal("|").set_parser_name("binary_or")
BY = keyword("by").suppress()
CASE = keyword("case").suppress()
CLUSTER = keyword("cluster")
COLLATE = keyword("collate")
CONCAT = Literal("||").set_parser_name("concat")
CONSTRAINT = keyword("constraint").suppress()
CREATE = keyword("create").suppress()
CROSS = keyword("cross")
DEQ = (
    # decisive equality
    # https://sparkbyexamples.com/apache-hive/hive-relational-arithmetic-logical-operators/
    # https://prestodb.io/docs/current/functions/comparison.html#is-distinct-from-and-is-not-distinct-from
    (keyword("is distinct from") | Literal("<=>")).set_parser_name("eq!")
)
DESC = keyword("desc")
DESCRIBE = keyword("describe")
DISTINCT = keyword("distinct")
DIV = Literal("/").set_parser_name("div")
EEQ = (
    # conservative equality  https://github.com/klahnakoski/jx-sqlite/blob/dev/docs/Logical%20Equality.md#definitions
    (Literal("==") | Literal("=")).set_parser_name("eq")
)
ELSE = keyword("else").suppress()
END = keyword("end").suppress()
EXCEPT = keyword("except")
EXPLAIN = keyword("explain")
FETCH = keyword("fetch").suppress()
FOR = keyword("for").suppress()
FOREIGN = keyword("foreign").suppress()
FROM = keyword("from").suppress()
FULL = keyword("full")
GROUP = keyword("group").suppress()
GT = Literal(">").set_parser_name("gt")
GTE = Literal(">=").set_parser_name("gte")
HAVING = keyword("having").suppress()
ILIKE = keyword("ilike")
IN = keyword("in")
INDEX = keyword("index").suppress()
INDF = (
    # decisive equality
    # https://prestodb.io/docs/current/functions/comparison.html#is-distinct-from-and-is-not-distinct-from
    keyword("is not distinct from").set_parser_name("ne!")
)
INNER = keyword("inner")
INTERSECT = keyword("intersect")
INTERVAL = keyword("interval")
INTO = keyword("into").suppress()
IREGEXP = Literal("~*").set_parser_name("regexp_i")
IS = keyword("is")
JOIN = keyword("join")
JSON_CONTAINS = Literal("?").set_parser_name("json_contains")
JSON_CONTAINS_ALL = Literal("?&").set_parser_name("json_contains_all")
JSON_CONTAINS_ANY = Literal("?|").set_parser_name("json_contains_any")
JSON_GET = Literal("->").set_parser_name("json_get")
JSON_GET_TEXT = Literal("->>").set_parser_name("json_get_text")
JSON_PATH = Literal("#>").set_parser_name("json_path")
JSON_PATH_DEL = Literal("#-").set_parser_name("json_path_del")
JSON_PATH_TEXT = Literal("#>>").set_parser_name("json_path_text")
JSON_SUBSUMED_BY = Literal("<@").set_parser_name("json_subsumed_by")
JSON_SUBSUMES = Literal("@>").set_parser_name("json_subsumes")
KEY = keyword("key").suppress()
LATERAL = keyword("lateral")
LEFT = keyword("left")
LIKE = keyword("like")
LIMIT = keyword("limit").suppress()
LT = Literal("<").set_parser_name("lt")
LTE = Literal("<=").set_parser_name("lte")
MINUS = keyword("minus")
MOD = Literal("%").set_parser_name("mod")
MUL = Literal("*").set_parser_name("mul")
NATURAL = keyword("natural")
NEG = Literal("-").set_parser_name("neg")
NEQ = (Literal("!=") | Literal("<>")).set_parser_name("neq")
NOCASE = keyword("nocase")
NOT = keyword("not")
NOT_IREGEXP = Literal("!~*").set_parser_name("not_regexp_i")
OFFSET = keyword("offset").suppress()
ON = keyword("on").suppress()
OR = keyword("or")
ORDER = keyword("order").suppress()
OUTER = keyword("outer")
OVER = keyword("over").suppress()
PARTITION = keyword("partition").suppress()
PIVOT = keyword("pivot")
POS = Literal("+").set_parser_name("pos")
PRIMARY = keyword("primary").suppress()
QUALIFY = keyword("qualify").suppress()
RECURSIVE = keyword("recursive").suppress()
REFERENCES = keyword("references").suppress()
_REGEXP = keyword("regexp")
REGEXP = (_REGEXP | Literal("~")).set_parser_name("regexp")
RIGHT = keyword("right")
RLIKE = keyword("rlike")
SAMPLE = keyword("sample").suppress()
SELECT = keyword("select").suppress()
SET = keyword("set").suppress()
_SIMILAR = keyword("similar")
SUB = Literal("-").set_parser_name("sub")
TABLE = keyword("table").suppress()
TABLESAMPLE = keyword("tablesample").suppress()
THEN = keyword("then").suppress()
TO = Keyword("to", caseless=True).suppress()
TOP = keyword("top").suppress()
UNION = keyword("union")
UNIQUE = keyword("unique").suppress()
UNNEST = keyword("unnest")
UNPIVOT = keyword("unpivot")
USING = keyword("using").suppress()
VALUES = keyword("values").suppress()
VIEW = keyword("view")
WHEN = keyword("when").suppress()
WHERE = keyword("where").suppress()
WINDOW = keyword("window")
WITH = keyword("with").suppress()
WITHIN = keyword("within").suppress()


# COMPOUND KEYWORDS AND OPERATORS
AT_TIME_ZONE = keyword("at time zone")
CLUSTER_BY = Group(CLUSTER + BY).set_parser_name("cluster by")
FOREIGN_KEY = Group(FOREIGN + KEY).set_parser_name("foreign_key")
GROUP_BY = Group(GROUP + BY).set_parser_name("group by")
IS_NOT = Group(IS + NOT).set_parser_name("is_not")
NOT_BETWEEN = Group(NOT + BETWEEN).set_parser_name("not_between")
NOT_ILIKE = Group(NOT + ILIKE).set_parser_name("not_ilike")
NOT_LIKE = Group(NOT + LIKE).set_parser_name("not_like")
NOT_RLIKE = Group(NOT + RLIKE).set_parser_name("not_rlike")
NOT_IN = Group(NOT + IN).set_parser_name("nin")
NOT_REGEXP = Group(NOT + _REGEXP | Literal("!~")).set_parser_name("not_regexp")
NOT_SIMILAR_TO = Group(NOT + _SIMILAR + TO).set_parser_name("not_similar_to")
ORDER_BY = Group(ORDER + BY).set_parser_name("order by")
PARTITION_BY = Group(PARTITION + BY).set_parser_name("partition by")
PRIMARY_KEY = Group(PRIMARY + KEY).set_parser_name("primary_key")
SELECT_DISTINCT = Group(SELECT + DISTINCT).set_parser_name("select distinct")
SIMILAR_TO = Group(_SIMILAR + TO).set_parser_name("similar_to")
STRAIGHT_JOIN = keyword("straight_join")
UNION_ALL = (UNION + ALL).set_parser_name("union_all")
WITHIN_GROUP = Group(WITHIN + GROUP).set_parser_name("within_group")
joins = (
    Literal(",").set_parser_name("cross_join") / "cross join"
    | (
        (
            Optional(CROSS | OUTER | INNER | NATURAL | ((FULL | LEFT | RIGHT) + Optional(INNER | OUTER))) + JOIN
            | STRAIGHT_JOIN
        )
        + Optional(LATERAL)
    )
    | LATERAL + Optional(VIEW + Optional(OUTER))
    | (CROSS | OUTER) + APPLY
) / (lambda tokens: " ".join(tokens).lower())

RESERVED = MatchFirst([
    # ONY INCLUDE SINGLE WORDS
    AND,
    AS,
    ASC,
    BEGIN,
    BETWEEN,
    BY,
    CASE,
    COLLATE,
    CONSTRAINT,
    CREATE,
    CROSS,
    DESC,
    DISTINCT,
    EXCEPT,
    ELSE,
    END,
    FALSE,
    FETCH,
    FOREIGN,
    FOR,
    FROM,
    FULL,
    GROUP_BY,
    GROUP,
    HAVING,
    INTO,
    INNER,
    INTERSECT,
    IN,
    IS_NOT,
    IS,
    JOIN,
    LATERAL,
    LEFT,
    LIKE,
    LIMIT,
    MINUS,
    NATURAL,
    NOCASE,
    NOT,
    NULL,
    OFFSET,
    ON,
    OR,
    ORDER,
    OUTER,
    OVER,
    PARTITION,
    PIVOT,
    QUALIFY,
    REFERENCES,
    RIGHT,
    RLIKE,
    SELECT,
    SET,
    STRAIGHT_JOIN,
    TABLESAMPLE,
    THEN,
    TRUE,
    UNION,
    UNIQUE,
    UNNEST,
    UNPIVOT,
    USING,
    WHEN,
    WHERE,
    WINDOW,
    WITH,
    WITHIN,
])

LB = Literal("(").suppress()
RB = Literal(")").suppress()
LK = Literal("[").suppress()
RK = Literal("]").suppress()
EQ = Char("=").suppress()
comma = Optional(",").suppress()

join_keywords = {
    "join",
    "full join",
    "cross join",
    "inner join",
    "left join",
    "right join",
    "full outer join",
    "right outer join",
    "left outer join",
    "cross apply",
    "outer apply",
}

pivot_keywords = {
    "pivot",
    "unpivot",
}

precedence = {
    # https://www.sqlite.org/lang_expr.html
    "literal": -1,
    "nliteral": -1,
    "create_array": -1,
    "get": 0,
    "interval": 0,
    "cast": 0,
    "try_cast": 0,
    "validate_conversion": 0,
    "collate": 0,
    "concat": 1,
    "json_get": 1.5,
    "json_get_text": 1.5,
    "json_path": 1.5,
    "json_path_text": 1.5,
    "json_subsumes": 1.5,
    "json_subsumed_by": 1.5,
    "json_contains": 1.5,
    "json_contains_any": 1.5,
    "json_contains_all": 1.5,
    "json_path_del": 1.5,
    "mul": 2,
    "div": 2,
    "mod": 2,
    "neg": 3,
    "add": 3,
    "sub": 3,
    "binary_not": 4,
    "binary_and": 4,
    "binary_or": 4,
    "gte": 5,
    "lte": 5,
    "lt": 5,
    "gt": 6,
    "eq": 7,
    "regexp": 7,
    "not_regexp": 7,
    "regexp_i": 7,
    "not_regexp_i": 7,
    "neq": 7,
    "missing": 7,
    "exists": 7,
    "at_time_zone": 8,
    "between": 8,
    "not_between": 8,
    "not": 8,
    "in": 8,
    "nin": 8,
    "is": 8,
    "like": 8,
    "not_like": 8,
    "rlike": 8,
    "not_rlike": 8,
    "ilike": 8,
    "not_ilike": 8,
    "similar_to": 8,
    "not_similar_to": 8,
    "and": 10,
    "or": 11,
    "lambda": 12,
    "assign": 13,
    "join": 18,
    "list": 18,
    "case": 19,
    "with": 30,
    "select": 30,
    "select_distinct": 30,
    "distinct_on": 31,
    "from": 32,
    "where": 33,
    "groupby": 34,
    "window": 35,
    "pivot": 36,
    "unpivot": 36,
    "having": 37,
    "union": 40,
    "union_all": 40,
    "except": 40,
    "minus": 40,
    "intersect": 40,
    "order": 50,
}

KNOWN_OPS = [
    COLLATE,
    CONCAT,
    JSON_GET_TEXT
    | JSON_GET
    | JSON_PATH_TEXT
    | JSON_PATH
    | JSON_SUBSUMES
    | JSON_SUBSUMED_BY
    | JSON_CONTAINS_ANY
    | JSON_CONTAINS_ALL
    | JSON_CONTAINS
    | JSON_PATH_DEL,
    POS,
    NEG,
    MUL | DIV | MOD,
    ADD | SUB,
    BINARY_NOT,
    BINARY_AND,
    BINARY_OR,
    GTE | LTE | LT | GT,
    EEQ | NEQ | DEQ | INDF,
    AT_TIME_ZONE,
    (BETWEEN, AND),
    (NOT_BETWEEN, AND),
    IN,
    NOT_IN,
    IS_NOT,
    IS,
    LIKE,
    ILIKE,
    NOT_LIKE,
    NOT_ILIKE,
    RLIKE,
    NOT_RLIKE,
    SIMILAR_TO,
    NOT_SIMILAR_TO,
    NOT,
    AND,
    OR,
    ASSIGN,
    NOT_IREGEXP | NOT_REGEXP | IREGEXP | REGEXP,
]

times = ["now", "today", "tomorrow", "eod"]

durations = {
    "microseconds": "microsecond",
    "microsecond": "microsecond",
    "microsecs": "microsecond",
    "microsec": "microsecond",
    "useconds": "microsecond",
    "usecond": "microsecond",
    "usecs": "microsecond",
    "usec": "microsecond",
    "us": "microsecond",
    "milliseconds": "millisecond",
    "millisecond": "millisecond",
    "millisecon": "millisecond",
    "mseconds": "millisecond",
    "msecond": "millisecond",
    "millisecs": "millisecond",
    "millisec": "millisecond",
    "msecs": "millisecond",
    "msec": "millisecond",
    "ms": "millisecond",
    "seconds": "second",
    "second": "second",
    "secs": "second",
    "sec": "second",
    "minutes": "minute",
    "minute": "minute",
    "mins": "minute",
    "min": "minute",
    "hours": "hour",
    "hour": "hour",
    "hrs": "hour",
    "hr": "hour",
    "day_of_week": "dow",
    "dayofweek": "dow",
    "dow": "dow",
    "day_of_month": "dom",
    "dayofmonth": "dom",
    "dom": "dow",
    "day_of_year": "doy",
    "dayofyear": "doy",
    "doy": "doy",
    "date": "date",
    "days": "day",
    "day": "day",
    "weekday": "dow",
    "weeks": "week",
    "week": "week",
    "isoweek": "isoweek",
    "months": "month",
    "month": "month",
    "mons": "month",
    "mon": "month",
    "quarters": "quarter",
    "quarter": "quarter",
    "year_of_week": "yow",
    "yearofweek": "yow",
    "years": "year",
    "year": "year",
    "yow": "yow",
    "isodow": "isodow",
    "isoyear": "isoyear",
    "decades": "decade",
    "decade": "decade",
    "decs": "decade",
    "dec": "decade",
    "centuries": "century",
    "century": "century",
    "cents": "century",
    "cent": "century",
    "millennia": "millennium",
    "millennium": "millennium",
    "mils": "millennium",
    "mil": "millennium",
    "epoch": "epoch",
    "julian": "julian",
    "timezone_minute": "timezone_minute",
    "timezone_hour": "timezone_hour",
    "timezone": "timezone",
    "s": "second",
    "m": "minute",
    "h": "hour",
    "d": "day",
    "w": "week",
    "M": "month",
    "y": "year",
    "c": "century",
}
