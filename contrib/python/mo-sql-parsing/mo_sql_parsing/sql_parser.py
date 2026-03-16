# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_dots import Null
from mo_parsing import debug
from mo_parsing.whitespaces import NO_WHITESPACE

from mo_sql_parsing import utils
from mo_sql_parsing.keywords import *
from mo_sql_parsing.types import get_column_type, time_functions, _sizes, unary_ops
from mo_sql_parsing.utils import *
from mo_sql_parsing.windows import window

delimiter_pattern = Literal(";").suppress()


def common_parser(all_columns):
    atomic_ident = ansi_ident | mysql_backtick_ident | simple_ident
    return parser(regex_string | ansi_string, atomic_ident, all_columns=all_columns)


def mysql_parser(all_columns):
    utils.emit_warning_for_double_quotes = False

    mysql_string = regex_string | ansi_string | mysql_doublequote_string
    atomic_ident = mysql_backtick_ident | sqlserver_ident | ident_w_dash_warning
    return parser(mysql_string, atomic_ident, all_columns=all_columns)


def sqlserver_parser(all_columns):
    atomic_ident = ansi_ident | mysql_backtick_ident | sqlserver_ident | sqlserver_local_ident
    return parser(regex_string | ansi_string, atomic_ident, sqlserver=True, all_columns=all_columns)


def bigquery_parser(all_columns):
    mysql_string = regex_string | ansi_string | mysql_doublequote_string
    atomic_ident = ansi_ident | mysql_backtick_ident | ident_w_dash
    return parser(mysql_string, atomic_ident, all_columns=all_columns)


def parser(literal_string, simple_ident, all_columns=None, sqlserver=False):
    debugger = debug.DEBUGGER or Null
    debugger.__exit__(None, None, None)

    ident = Combine(delimited_list(simple_ident, separator=".", combine=True))

    with Whitespace() as white:
        rest_of_line = Regex(r"[^\n]*")

        white.add_ignore(Literal("--") + rest_of_line)
        white.add_ignore(Literal("#") + rest_of_line)
        white.add_ignore(Literal("/*") + SkipTo("*/", include=True))

        with whitespaces.NO_WHITESPACE:
            identifier = ~RESERVED + ident
        function_name = ~(UNION | FROM | WHERE | SELECT) + ident

        # EXPRESSIONS
        expression = Forward()
        (column_type, column_definition, column_def_references, column_option, declare_variable) = get_column_type(
            expression, identifier, literal_string
        )
        proc_param = Group(
            Optional(IN | keyword("out") | keyword("inout") / ["in", "out"])("mode") + identifier("name") + column_type
        )

        # CASE
        case = (
            CASE
            + Group(ZeroOrMore((WHEN + expression("when") + THEN + expression("then")) / to_when_call))("case")
            + Optional(ELSE + expression("else"))
            + END
        ) / to_case_call

        switch = (
            CASE
            + expression("value")
            + Group(ZeroOrMore((WHEN + expression("when") + THEN + expression("then")) / to_when_call))("case")
            + Optional(ELSE + expression("else"))
            + END
        ) / to_switch_call

        casting = MatchFirst([
            (
                Group(
                    Keyword(c, caseless=True)("op")
                    + LB
                    + expression("params")
                    + Optional(AS | comma)
                    + column_type("params")
                    + RB
                )
                / to_json_call
            )
            for c in ["cast", "safe_cast", "try_cast", "validate_conversion", "convert"]
        ])

        oracle_casting = MatchFirst([
            (
                Group(
                    Keyword(c, caseless=True)("op")
                    + LB
                    + expression("params")
                    + Optional(
                        keyword("default").suppress()
                        + expression("on_conversion_error")
                        + keyword("ON CONVERSION ERROR").suppress()
                    )
                    + Optional(comma + literal_string("params") + Optional(comma + literal_string("params")))
                    + RB
                )
                / to_json_call
            )
            for c in ["to_date", "to_number", "to_timestamp", "to_timestamp_tz", "to_yminterval", "to_dsinterval"]
        ])

        substring = (
            Group(
                keyword("substring")("op")
                + LB
                + expression("params")
                + Optional(assign("from", expression))
                + Optional(assign("for", expression))
                + RB
            )
            / to_json_call
        )

        trim = (
            Group(
                keyword("trim").suppress()
                + LB
                + Optional(
                    (keyword("both") | keyword("trailing") | keyword("leading")) / (lambda t: t[0].lower())
                )("direction")
                + (assign("from", expression) | expression("chars") + Optional(assign("from", expression)))
                + RB
            )
            / to_trim_call
        )

        # INTERVAL TYPE
        # https://www.postgresql.org/docs/current/datatype-datetime.html
        time_interval_type = Forward()
        time_interval_type << MatchFirst([
            (
                (CaselessLiteral(d) / (lambda t: durations[t[0].lower()]))("op")
                + _sizes
                + Optional(TO + time_interval_type("kwargs"))
            )
            / to_interval_type
            for d in durations.keys()
        ])

        def matching(type):
            return Optional(
                (real_num | int_num)(type)
                + MatchFirst([
                    # CONSUME ALL THE NAME, BUT NOT THE "T" USED TO DESIGNATE TIME
                    CaselessKeyword(k, ident_chars=Regex("[a-su-z]")).suppress()
                    for k, v in durations.items()
                    if v == type
                ])
            )

        iso_datetime = (
            matching("year")
            + comma
            + matching("month")
            + comma
            + matching("week")
            + comma
            + matching("day")
            + comma
            + matching("hour")
            + comma
            + matching("minute")
            + comma
            + matching("second")
            + comma
            + matching("millisecond")
            + Optional(CaselessLiteral("ago")("ago"))
        ) / has_something

        ago = Optional(Regex("[+-]"))("ago")
        sql_date = MatchFirst([
            ago + int_pos("year") + "-" + int_pos("month") + Optional(ago("day-ago") + int_pos("day")),
            int_num("day"),
        ])

        sql_time = MatchFirst([
            ago
            + int_pos("day")
            + Optional(CaselessLiteral("T") | ",")
            + int_pos("hour")
            + ":"
            + int_pos("minute")
            + Optional(":" + int_pos("second") + Optional("." + int_pos("fraction"))),
            ago
            + int_pos("hour")
            + ":"
            + int_pos("minute")
            + Optional(":" + int_pos("second") + Optional("." + int_pos("fraction"))),
            ago + ":" + int_pos("minute") + Optional(":" + int_pos("second") + Optional("." + int_pos("fraction"))),
            ago + int_pos("minute") + ":" + int_pos("second") + Optional("." + int_pos("fraction")),
            (real_num | int_num)("expr"),
        ])

        formatted_duration = Regex("[@Pp]*") + (delimited_list(
            (sql_time ^ sql_date ^ iso_datetime) / to_interval_call, separator=Regex("[,TtPp]*"),
        ))

        interval = (
            INTERVAL
            + (
                Literal("'").suppress() + formatted_duration + Literal("'").suppress()
                | (expression ^ formatted_duration)
            )("expr")
            + Optional(time_interval_type("type"))
        ) / cast_interval_call

        timestamp = (
            time_functions("op")
            + (literal_string("params") | MatchFirst([keyword(t) / (lambda t: t.lower()) for t in times])("params"))
        ) / to_json_call

        extract = (
            keyword("extract")("op")
            + LB
            + (time_interval_type("params") | expression("params"))
            + FROM
            + expression("params")
            + RB
        ) / to_json_call

        single_quote_name = Regex(r"\'(?:\'\'|[^'])*\'") / (lambda x: single_literal(x)["literal"])

        alias = Optional((
            (
                (
                    AS + ((ident | single_quote_name)("name") + Optional(LB + delimited_list(ident("col")) + RB))
                    | (
                        (identifier | single_quote_name)("name")
                        + Optional((LB + delimited_list(ident("col")) + RB) | (AS + delimited_list(identifier("col"))))
                    )
                )
                + ~FollowedBy(LB)  # THIS IS NOT AN ALIAS
            )
            / to_alias
        )("name"))

        named_column = Group(Group(expression)("value") + alias)

        stack = (
            keyword("stack")("op") + LB + int_num("width") + "," + delimited_list(expression)("args") + RB
        ) / to_stack

        query = Forward()

        # ARRAY[foo],
        # ARRAY < STRING > [foo, bar], INVALID
        # ARRAY < STRING > [foo, bar],
        create_array = (
            keyword("array")("op")
            + Optional(LT.suppress() + column_type("type") / to_flat_column_type + GT.suppress())
            + (
                LB + (query | delimited_list(Group(expression)))("args") + RB
                | LK + Optional(delimited_list(Group(expression))("args")) + RK
            )
        )

        if not sqlserver:
            # SQL SERVER DOES NOT SUPPORT [] FOR ARRAY CONSTRUCTION (USED FOR IDENTIFIERS)
            create_array = LK + delimited_list(Group(expression))("args") + RK | create_array

        create_array = create_array / to_array

        create_map = (keyword("map") + LK + expression("keys") + "," + expression("values") + RK) / to_map

        if all_columns == "*":
            select_column = Group(Literal("*")("value") | expression("value") + alias) / to_select_call
        else:
            select_column = Group(expression("value") + alias) / to_select_call

        create_struct = (
            keyword("struct")("op")
            + Optional(LT.suppress() + delimited_list(column_type)("types") + GT.suppress())
            + LB
            + delimited_list(select_column)("args")
            + RB
        ) / to_struct

        distinct = (DISTINCT("op") + delimited_list(named_column)("params")) / to_json_call

        sort_column = (
            expression("value").set_parser_name("sort1")
            + Optional(DESC("sort") | ASC("sort"))
            + Optional(assign("nulls", keyword("first") | keyword("last")))
        )

        one_param = (
            # KEYWORD PARAMETERS?
            # https://docs.snowflake.com/en/sql-reference/functions/generator.html
            Group(ident / (lambda t: t[0].lower()) + Literal("=>").suppress() + Group(expression))("kwargs")
            / to_kwarg
        ) | Group(expression)("params")

        # https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls
        call_function = (
            function_name("op")
            + LB
            + Optional(flag("distinct"))
            + Optional(delimited_list(one_param) | Group(query)("params"))
            + Optional((keyword("respect") | keyword("ignore"))("nulls") + keyword("nulls").suppress())
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + Optional(assign("limit", expression))
            + Optional(assign("separator", expression))
            + RB
        ) / to_json_call

        dynamic_accessor = LK + expression + RK
        simple_accessor = Literal(".").suppress() + simple_ident / to_literal
        accessor = (
            Literal(":").suppress()
            + Group(simple_ident / to_literal | dynamic_accessor)
            + ZeroOrMore(Group(simple_accessor | dynamic_accessor))
        )

        _lambda = Group(
            LB + Group(delimited_list(identifier))("params") + RB + Literal("->").suppress() + expression("lambda")
        )

        with NO_WHITESPACE:

            def scale(tokens):
                return {"mul": [tokens[0], tokens[1]]}

            scale_function = ((real_num | int_num) + call_function) / scale
            scale_ident = ((real_num | int_num) + ident) / scale

        compound = (
            NULL
            | TRUE
            | FALSE
            | NOCASE
            | interval
            | timestamp
            | extract
            | case
            | switch
            | casting
            | oracle_casting
            | substring
            | distinct
            | trim
            | stack
            | create_array
            | create_map
            | create_struct
            | _lambda
            | (LB + Group(query) + RB)
            | (LB + Group(delimited_list(expression)) / to_tuple_call + RB)
            | literal_string
            | hex_num
            | scale_function
            | scale_ident
            | real_num
            | int_num
            | call_function
            | Combine(function_name + Optional(".*"))
        )

        window_clause, over_clause = window(expression, identifier, sort_column)

        expression << (
            (
                Literal("*")
                | infix_notation(
                    compound,
                    ([] if sqlserver else [(dynamic_accessor, 1, LEFT_ASSOC, to_offset,)])
                    + [
                        (simple_accessor, 1, LEFT_ASSOC, to_offset,),
                        (accessor, 1, LEFT_ASSOC, to_offset),
                        (window_clause, 1, LEFT_ASSOC, to_window_mod),
                        (assign("filter", LB + WHERE + expression + RB), 1, LEFT_ASSOC, to_window_mod,),
                    ]
                    + [
                        (
                            o,
                            1 if o in unary_ops else (3 if isinstance(o, tuple) else 2),
                            unary_ops.get(o, LEFT_ASSOC),
                            to_json_operator,
                        )
                        for o in KNOWN_OPS
                    ],
                )
            )("value").set_parser_name("expression")
        )

        table_source = Forward()

        pivot_join = assign(
            "pivot",
            (
                LB
                + Group(delimited_list(Group(expression("value") + alias) / to_pivot_column))("aggregate")
                + (assign("for", identifier) + assign("in", expression))
                + RB
                + alias
            ),
        )

        # https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
        unpivot_join = assign(
            "unpivot",
            (
                Optional(keyword("EXCLUDE NULLS")("nulls") / False | keyword("INCLUDE NULLS")("nulls") / True)
                + LB
                + expression("value")
                + assign("for", identifier)
                + assign(
                    "in",
                    (
                        LB
                        + Group(delimited_list(
                            Group(expression("value") + Optional(AS + literal_string("name"))) / to_unpivot_column
                        ))
                        + RB
                    ),
                )
                + RB
                + alias
            ),
        )

        join = Forward() / to_join_call
        join << (
            Group(joins)("op")
            + table_source("join")
            + Optional(Group(join)("child"))
            + Optional((ON + expression("on")) | (USING + expression("using")))
        )

        tops = (
            Optional(
                TOP
                + expression("value")
                + Optional(keyword("percent"))("percent")
                + Optional(WITH + keyword("ties"))("ties")
            )("top")
            / to_top_clause
        )

        if all_columns:
            selection = (
                (SELECT + "*" + EXCEPT.suppress())
                + (LB + delimited_list(select_column)("select_except") + RB)
                + Optional(comma + delimited_list(select_column)("select"))
                | (SELECT + DISTINCT + ON)
                + (LB + delimited_list(select_column)("distinct_on") + RB)
                + delimited_list(select_column)("select")
                | assign("select distinct", delimited_list(select_column))
                | assign("select as struct", delimited_list(select_column))
                | assign("select as value", delimited_list(select_column))
                | SELECT + tops + delimited_list(select_column)("select")
            ) + comma
        else:
            except_columns = Group(
                (Literal("*") / {} | ident + Suppress(".*"))("all_columns")
                + Optional(EXCEPT.suppress() + LB + delimited_list(ident)("except") + RB)
            )

            selection = (
                (SELECT + DISTINCT + ON + LB + delimited_list(select_column)("distinct_on") + RB)
                + delimited_list(select_column)("select")
                | assign("select distinct", delimited_list(select_column))
                | assign("select as struct", delimited_list(select_column))
                | assign("select as value", delimited_list(select_column))
                | SELECT + tops + delimited_list(except_columns | select_column)("select")
            ) + comma

        row = (LB + delimited_list(Group(expression)) + RB) / to_row
        values = (VALUES + delimited_list(row)) / to_values

        named_over_clause = Group(identifier("name") + AS + (identifier | over_clause)("value"))
        into = Optional(assign("into", Group(identifier + Optional(LB + delimited_list(ident) + RB))))

        unordered_sql = Group(
            (values | selection)
            + into
            + Optional((FROM + delimited_list(table_source) + ZeroOrMore(join))("from"))
            + Optional(WHERE + expression("where"))
            + Optional(pivot_join)
            + Optional(unpivot_join)
            + Optional(GROUP_BY + delimited_list(Group(named_column))("groupby"))
            + (
                Optional(HAVING + expression("having"))
                & Optional(WINDOW + delimited_list(named_over_clause)("window"))
                & Optional(QUALIFY + expression("qualify"))
            )
            + into
        )

        with NO_WHITESPACE:

            def mult(tokens):
                amount = tokens["bytes"]
                scale = tokens["scale"].lower()
                return {"bytes": amount * {"b": 1, "k": 1_000, "m": 1_000_000, "g": 1_000_000_000}[scale]}

            bytes_constraint = ((real_num | int_num)("bytes") + Char("bBkKmMgG")("scale")) / mult

        # https://wiki.postgresql.org/wiki/TABLESAMPLE_Implementation
        # https://docs.snowflake.com/en/sql-reference/constructs/sample.html
        # https://docs.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql?view=sql-server-ver16
        tablesample = (TABLESAMPLE | SAMPLE) + Group(
            Optional((keyword("bernoulli") | keyword("row") | keyword("system") | keyword("block")))("method")
            # / (lambda t: t if t else "bernoulli")
            + LB
            + (
                (
                    keyword("bucket")("op")
                    + int_num("params")
                    + keyword("out of")
                    + int_num("params")
                    + Optional(ON + expression("on"))
                )
                / to_json_call
                | (real_num | int_num)("percent") + keyword("percent")
                | int_num("rows") + keyword("rows")
                | bytes_constraint
                | (real_num | int_num)("percent")
            )
            + RB
            + Optional(assign("repeatable", LB + int_num + RB))
        )("tablesample")

        unnest = (UNNEST("op") + LB + expression("params") + RB) / to_json_call
        lateral_source = (LATERAL("op") + table_source("params")) / to_json_call

        table_source << Group(
            (
                lateral_source
                | (LB + query + RB)
                | (LB + table_source + ZeroOrMore(join) + RB)
                | unnest
                | stack
                | call_function
                | ident
            )("value")
            + MatchAll([
                Optional(flag("with ordinality")),
                Optional(WITH + LB + keyword("nolock")("hint") + RB),
                Optional(WITH + OFFSET + Optional(AS) + ident("with_offset")),
                Optional(tablesample),
                Optional(assign("for system_time as of", expression)),
                Optional(assign("for system time as of", expression)),
                alias,
            ])
        ) / to_table

        rows = Optional(keyword("row") | keyword("rows"))
        limit = ZeroOrMore(
            assign("offset", expression) + rows
            | FETCH
            + Optional(keyword("first") | keyword("next"))
            + expression("fetch")
            + rows
            + Optional(keyword("only"))
            | LIMIT + expression("offset") + "," + expression("limit")
            | LIMIT + expression("limit")
        )

        # https://www.postgresql.org/docs/current/sql-select.html
        #  [ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [, ...] ] [ NOWAIT | SKIP LOCKED ] [...] ]
        for_update = Optional(Group(
            FOR
            + (keyword("update") | keyword("share") | keyword("no key update") | keyword("key share"))("mode")
            + Optional(
                keyword("of").suppress() + identifier("value") + Optional(flag("nowait") | flag("skip locked"))
            )("table")
        ))("locking")

        ordered_sql = (
            (
                (unordered_sql | (LB + query + RB))
                + ZeroOrMore(
                    Group((UNION | INTERSECT | EXCEPT | MINUS) + Optional(ALL | DISTINCT))("op")
                    + (unordered_sql | (LB + query + RB))
                )
            )("union")
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + limit
            + for_update
            + Optional((UNION | INTERSECT | EXCEPT | MINUS) / bad_operator_on_ordered_sql)
        ) / to_union_call

        with_clause = delimited_list(Group(
            ((identifier("name") + Optional(LB + delimited_list(ident("col")) + RB)) / to_alias)("name")
            + (AS + LB + (query | expression)("value") + RB)
        ))

        using_external_function = (
            USING
            + delimited_list(Group(assign(
                "external function",
                identifier("name")
                + LB
                + Group(delimited_list(proc_param))("params")
                + RB
                + assign("returns", column_type / list)
                + assign("lambda", literal_string),
            )))
        )("using")

        query << (
            ZeroOrMore(MatchFirst([
                assign("with recursive", with_clause),
                assign("with", with_clause),
                using_external_function,
            ]))
            + Group(ordered_sql)("query")
        ) / to_query

        #####################################################################
        # DML STATEMENTS
        #####################################################################

        # MySQL's index_type := Using + ( "BTREE" | "HASH" )
        index_type = Optional(assign("using", ident))

        index_column_names = (
            LB
            + delimited_list((identifier("value") + Optional(LB + int_num("length") + RB)) / to_index_part)("columns")
            + RB
        )

        column_def_delete = assign(
            "on delete", (keyword("cascade") | keyword("restrict") | keyword("set null") | keyword("set default")),
        )
        table_def_foreign_key = FOREIGN_KEY + Optional(
            Optional(identifier("index_name"))
            + index_column_names
            + column_def_references
            + ZeroOrMore(column_def_delete | assign("on update", keyword("cascade")))
        )

        index_options = ZeroOrMore(identifier / (lambda t: {t[0]: True}))

        table_constraint_definition = Group(
            Optional(CONSTRAINT + identifier("name"))
            + (
                assign(
                    "primary key",
                    Group(
                        index_type
                        + index_column_names
                        + index_type
                        + Optional(assign("comment", literal_string))
                        + index_options,
                    ),
                )
                | Group(
                    Optional(flag("unique"))
                    + Optional(INDEX | KEY)
                    + Optional(identifier("name"))
                    + index_column_names
                    + index_type
                    + Optional(assign("comment", literal_string))
                    + index_options
                )("index")
                | assign("check", LB + expression + RB)
                | table_def_foreign_key("foreign_key")
                | assign("fulltext key", identifier("name") + index_column_names)
            )
        )

        table_element = table_constraint_definition("constraint") | column_definition("columns")
        temporary = Optional(
            (Keyword("temporary", caseless=True) | Keyword("temp", caseless=True))("temporary") / True
        ) + Optional(flag("transient"))

        create_table = Group(
            keyword("create")
            + Optional(keyword("or") + flag("replace"))
            + temporary
            + TABLE
            + Optional((keyword("if not exists") / False)("replace"))
            + identifier("name")
            + Optional(LB + delimited_list(table_element) + Optional(",") + RB)
            + ZeroOrMore(
                assign("engine", EQ + identifier)
                | assign("collate", EQ + identifier)
                | assign("auto_increment", EQ + int_num)
                | assign("autoincrement", EQ + int_num)
                | assign("comment", EQ + literal_string)
                | assign("default character set", EQ + identifier)
                | assign("default charset", EQ + identifier)
                | assign("row_format", EQ + identifier)
                | assign("checksum", EQ + int_num)
            )
            + Optional(AS.suppress() + infix_notation(query, [])("query"))
            + Optional(CLUSTER_BY.suppress() + LB + delimited_list(identifier) + RB)("cluster_by")
            + ZeroOrMore(
                assign("sortkey", LB + delimited_list(identifier) + RB) | assign("distkey", LB + identifier + RB)
            )
        )("create table")

        definer = Optional(keyword("definer").suppress() + EQ + identifier("definer"))

        create_view = Group(
            keyword("create")
            + Optional(keyword("or") + flag("replace"))
            + Optional(
                keyword("algorithm").suppress()
                + EQ
                + (keyword("merge") | keyword("temptable") | keyword("undefined"))("algorithm")
            )
            + definer
            + Optional(assign("sql security", (keyword("definer") | keyword("invoker"))))
            + temporary
            + VIEW.suppress()
            + Optional((keyword("if not exists") / False)("replace"))
            + identifier("name")
            + AS
            + query("query")
            + Optional(WITH + (keyword("cascaded") | keyword("local")) + keyword("check option").suppress())("check")
        )("create view")

        # CREATE INDEX a ON u USING btree (e);
        create_index = (
            keyword("create index")
            + Optional(keyword("or") + flag("replace"))(INDEX | KEY)
            + Optional((keyword("if not exists") / False)("replace"))
            + identifier("name")
            + ON
            + identifier("table")
            + index_type
            + index_column_names
            + index_options
        )("create index")

        create_schema = assign(
            "create schema",
            Group(
                Optional(keyword("or") + flag("replace"))(INDEX | KEY)
                + Optional((keyword("if not exists") / False)("replace"))
                + identifier("name")
            ),
        )

        use_schema = assign("use", identifier)
        open_cursor = assign("open", identifier)
        close_cursor = assign("close", identifier)
        fetch_cursor = assign("fetch", identifier) + INTO + delimited_list(ident)
        cache_options = Optional((
            keyword("options").suppress()
            + LB
            + Dict(delimited_list(Group(
                literal_string / (lambda tokens: tokens[0]["literal"]) + Optional(EQ) + identifier
            )))
            + RB
        )("options"))

        create_cache = (
            keyword("cache").suppress()
            + Optional(flag("lazy"))
            + TABLE
            + identifier("name")
            + cache_options
            + Optional(AS + query("query"))
        )("cache")

        drops = assign(
            "drop",
            temporary
            + MatchFirst([
                keyword(item).suppress() + Optional(flag("if exists")) + Group(identifier)(item)
                for item in ["table", "view", "index", "schema"]
            ]),
        )

        returning = Optional(delimited_list(select_column)("returning"))

        insert = (
            Optional(assign("with", with_clause))
            + keyword("insert").suppress()
            + Optional(flag("ignore"))
            + (
                flag("overwrite") + keyword("table").suppress()
                | keyword("into").suppress() + Optional(keyword("table").suppress())
            )
            + identifier("table")
            + Optional(LB + delimited_list(identifier)("columns") + RB)
            + Optional(flag("if exists"))
            + (values | query)("query")
            + returning
        ) / to_insert_call

        replace = (
            Optional(assign("with", with_clause))
            + keyword("replace").suppress()
            + Optional(keyword("into").suppress())
            + identifier("table")
            + Optional(LB + delimited_list(identifier)("columns") + RB)
            + (values | query)("query")
        ) / to_replace_call

        update = (
            keyword("update")("op")
            + (delimited_list(table_source) + ZeroOrMore(join))("params")
            + assign("set", Dict(delimited_list(Group(identifier + EQ + expression))))
            + Optional((FROM + delimited_list(table_source) + ZeroOrMore(join))("from"))
            + Optional(WHERE + expression("where"))
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + limit
            + returning
        ) / to_json_call

        delete_options = ["low_priority", "quick", "ignore"]

        delete = (
            keyword("delete")("op")
            + (
                # DELETE <options> FROM x USING y
                ZeroOrMore(flag("low_priority") | flag("quick") | flag("ignore"))
                + FROM
                + delimited_list(~MatchFirst([keyword(o) for o in delete_options]) + identifier)("params")
                + USING
                + (delimited_list(table_source) + ZeroOrMore(join))("from")
                # DELETE <options> y FROM x
                | delimited_list(~MatchFirst([keyword(o) for o in delete_options]) + identifier)("params")
                + ZeroOrMore(flag("low_priority") | flag("quick") | flag("ignore"))
                + FROM
                + (delimited_list(table_source) + ZeroOrMore(join))("from")
                # DELETE <options> FROM x
                | ZeroOrMore(flag("low_priority") | flag("quick") | flag("ignore"))
                + FROM
                + (delimited_list(table_source) + ZeroOrMore(join))("params")
            )
            + Optional(assign("where", expression))
            + Optional(ORDER_BY + delimited_list(Group(sort_column))("orderby"))
            + limit
            + returning
        ) / to_json_call

        MATCHED = Keyword("matched", caseless=True)
        matched_when = assign(
            "when",
            (
                (
                    NOT.suppress()
                    + MATCHED.suppress()
                    + (
                        keyword("by source") / "not_matched_by_source"
                        | Optional(keyword("by target")) / "not_matched_by_target"
                    )
                    | MATCHED
                )("cond")
                + Optional(AND + expression("expr"))
            )
            / to_match_expr,
        )
        merge = (
            keyword("merge")("op")
            + tops
            + Optional(Keyword("into", caseless=True).suppress())
            + Group(identifier("value") + alias)("target")
            + USING
            + Group(identifier("value") + alias)("source")
            + ON
            + expression("on")
            + Many(
                Group(
                    matched_when
                    + assign(
                        "then",
                        (
                            keyword("delete") / {"delete": {}}
                            | keyword("update set").suppress()
                            + Dict(delimited_list(Group(identifier + EQ + expression))) / (lambda t: {"update": t})
                            | (
                                keyword("insert")
                                + Optional(LB + delimited_list(identifier)("columns") + RB)
                                + (
                                    keyword("default values")
                                    | VALUES + (LB + delimited_list(Group(expression)) + RB)("values")
                                )
                            )
                            / to_insert_call
                        ),
                    )
                )
                / to_when_call
            )("params")
        ) / to_json_call

        truncate = (
            Keyword("truncate", caseless=True).suppress()
            + Optional(TABLE)
            + identifier("truncate")
            + Optional(
                WITH
                + LB
                + assign("partitions", LB + delimited_list(Group((int_num + TO + int_num)("range")) | int_num) + RB)
                + RB
            )
        )

        #############################################################
        # GET/SET
        #############################################################
        statement = Forward()
        special_ident = keyword("masking policy") | identifier / (lambda t: t[0].lower())
        set_one_variable = SET + (
            (special_ident + Optional(EQ) + expression)
            / (lambda t: {t[0].lower(): t[1].lower() if isinstance(t[1], str) else t[1]})
        )("set")
        set_variables = SET + delimited_list((
            (special_ident + Optional(EQ) + expression)
            / (lambda t: {t[0].lower(): t[1].lower() if isinstance(t[1], str) else t[1]})
        ))("set")
        unset_one_variable = assign("unset", special_ident)

        copy_options = Forward()
        copy_options << ZeroOrMore(MatchFirst(
            [keyword(n).suppress() + EQ + (LB + copy_options + RB | expression)(n.lower()) for n in copy_params]
            + [PARTITION_BY.suppress() + expression("partition_by")]
        ))

        with NO_WHITESPACE:
            file_name = Regex("[a-zA-Z0-9-_!.]+")
            file_path = Optional("/" + delimited_list(file_name, separator="/", combine=True) + Optional("/"))
            # @%load1/data1/
            file_source = Combine(
                Literal("@")
                + (
                    Literal("~") + file_path
                    | Literal("%") + file_name + file_path
                    | (simple_ident + Optional("." + Optional("%") + file_name) + file_path)
                )
                | (CaselessLiteral("azure") | CaselessLiteral("s3") | CaselessLiteral("gcs"))
                + "://"
                + file_name
                + file_path
            )

        copy = assign(
            "copy",
            (
                assign("into", file_source | expression)
                + Optional(assign("from", file_source | expression))
                + copy_options
            ),
        )

        column_modifications = delimited_list(Group(
            Optional(Keyword("column", caseless=True).suppress())
            + identifier("name")
            + (
                keyword("set data type") + column_type
                | keyword("data type") + column_type
                | keyword("type") + column_type
                | set_one_variable
                | unset_one_variable
                | assign("drop", column_option | special_ident)
                | Optional(keyword("set")) + column_option
            )
        ))

        # EXPLAIN
        explain_option = MatchFirst([
            (
                Keyword(option, caseless=True)
                + Optional(EQ)
                + (
                    TRUE
                    | FALSE
                    | Keyword("on") / True
                    | Keyword("off") / False
                    | Keyword("1") / True
                    | Keyword("0") / False
                    | Empty() / True
                )
            )
            / to_option
            for option in [
                "analyze",
                "buffers",
                "costs",
                "settings",
                "summary",
                "timing",
                "verbose",
                "wal",
                "with_recommendations",
            ]
        ])
        explain_format = (
            Keyword("format", caseless=True)
            + Optional(EQ)
            + MatchFirst([keyword(k) for k in ["json", "yaml", "xml", "tree", "text", "traditional"]])
        ) / to_option
        explain_into = (Keyword("into", caseless=True) + ident + Optional(file_source)) / to_option
        explain = (
            ((EXPLAIN | DESC | DESCRIBE) + Optional(keyword("query")) + Optional(keyword("plan")))("op") / "explain"
            + Optional(Group(
                (LB + delimited_list(explain_option | explain_format | explain_into) + RB)
                | delimited_list(explain_option | explain_format | explain_into)
            ))("kwargs")
            + Optional(FOR)
            + statement("params")
        ) / to_json_call

        #############################################################
        # ALTER TABLE
        #############################################################

        alter = assign(
            "alter",
            (
                assign("table", identifier)
                + delimited_list(
                    assign("rename to", identifier)
                    | assign("rename", assign("column", identifier("name") + TO + identifier("to")),)
                    | assign("swap with", identifier)
                    | assign(
                        "add",
                        ZeroOrMore(
                            assign("column", column_definition)
                            | assign("constraint", identifier("name") + ZeroOrMore(column_option),)
                            | assign(
                                "row access policy",
                                identifier("policy") + (ON + LB + delimited_list(identifier("on")) + RB),
                            )
                        ),
                    )
                    | assign("drop", assign("column", identifier) | assign("row access policy", identifier),)
                    | (
                        (Keyword("alter", caseless=True) | Keyword("modify", caseless=True)).suppress()
                        + (LB + column_modifications + RB | column_modifications)
                    )("modify")
                    | assign("cluster by", LB + delimited_list(identifier) + RB)
                    #  MODIFY COLUMN empl_id UNSET MASKING POLICY
                )
            ),
        )

        #############################################################
        # USER FUNCTIONS
        #############################################################

        many_command = Forward()
        block = BEGIN + Group(many_command)("block") + END
        if_block = (
            assign("if", expression)
            + assign("then", many_command)
            + Optional(assign("else", many_command))
            + keyword("end if").suppress()
        )
        leave = assign("leave", identifier)
        while_block = assign("while", expression) + assign("do", many_command) + keyword("end while").suppress()
        loop_block = keyword("loop").suppress() + many_command("loop") + keyword("end loop").suppress()

        create_trigger = assign(
            "create trigger",
            (
                identifier("name")
                + MatchFirst([keyword(k) for k in ["before", "after"]])("when")
                + MatchFirst([keyword(k) for k in ["insert", "update", "delete"]])("event")
                + ON
                + identifier("table")
                + keyword("for each row").suppress()
                + statement("body")
            ),
        )

        characteristic = ZeroOrMore(MatchFirst([
            assign("comment", literal_string),
            assign("language", keyword("sql")),
            keyword("not deterministic") / {"deterministic": False},
            keyword("deterministic") / {"deterministic": True},
            keyword("contains sql") / {"contains_sql": True},
            keyword("no sql") / {"contains_sql": False},
            keyword("reads sql data") / {"sql_data": "read"},
            keyword("modifies sql data") / {"sql_data": "write"},
            assign("sql security", keyword("definer") | keyword("invoker")),
        ]))

        create_procedure = Group(
            CREATE
            + definer
            + keyword("procedure")
            + identifier("name")
            + LB
            + Group(Optional(delimited_list(proc_param)))("params")
            + RB
            + characteristic
            + statement("body")
        )("create_procedure")

        create_function = Group(
            CREATE
            + definer
            + keyword("function")
            + identifier("name")
            + LB
            + Group(delimited_list(Group(identifier("name") + column_type)))("params")
            + RB
            + assign("returns", column_type / list)
            + characteristic
            + statement("body")
        )("create_function")

        handler_condition = Group(MatchFirst([
            keyword("sqlwarning"),
            keyword("not found"),
            keyword("sqlexception"),
            assign("sqlstate", expression),
            int_num("error_code"),
            expression,
        ]))

        declare_hanlder = Group(
            keyword("declare")
            + Group(keyword("continue") | keyword("exit") | keyword("undo"))("action")
            + keyword("handler for")
            + delimited_list(handler_condition)("conditions")
            + statement("body")
        )("declare_handler")

        declare_cursor = Group(
            keyword("declare").suppress() + identifier("name") + keyword("cursor for").suppress() + query("query")
        )("declare_cursor")

        call_sql_function = Group(
            keyword("call") + identifier("op") + Optional(LB + Optional(delimited_list(expression)("args")) + RB)
        )

        transact = (
            Group(keyword("start transaction")("op")) / to_json_call
            | Group(keyword("commit")("op")) / to_json_call
            | Group(keyword("rollback")("op")) / to_json_call
        )

        blocks = Group(Optional(identifier("label") + ":") + (block | if_block | while_block | loop_block))

        #############################################################
        # FINALLY ASSEMBLE THE PARSER
        #############################################################

        with NO_WHITESPACE:
            delimiter_command = assign("delimiter", Regex(r"[^\n]+") / (lambda t: t[0].strip()))

        set_parser_names()

        debugger.__enter__()

        statement << Group(
            query
            | (insert | replace | update | delete | merge | truncate | use_schema)
            | (create_table | create_view | create_cache | create_index | create_schema)
            | drops
            | (copy | alter)
            | call_sql_function
            | create_trigger
            | create_procedure
            | create_function
            | explain
            | delimiter_command
            | declare_hanlder
            | declare_cursor
            | leave
            | assign("return", expression)
            | transact
            | open_cursor
            | close_cursor
            | fetch_cursor
            | blocks
            | (Optional(keyword("alter session")).suppress() + (set_variables | unset_one_variable | declare_variable))
        )

        many_command << (
            ZeroOrMore(delimiter_pattern)
            + Optional(statement)
            + ZeroOrMore(OneOrMore(delimiter_pattern) + Optional(statement))
        )
        return many_command.finalize()
