UNITTEST_FOR(yql/essentials/sql/v1/translation)

SRCS(
    sql_aggregation_ut.cpp
    sql_ansi_ut.cpp
    sql_core_ut.cpp
    sql_ddl_table_ut.cpp
    sql_ddl_ut.cpp
    sql_ddl_view_ut.cpp
    sql_error_ut.cpp
    sql_json_ut.cpp
    sql_match_recognize_ut.cpp
    sql_materialize_ut.cpp
    sql_parsing_only_ut.cpp
    sql_select_ut.cpp
    sql_ut.cpp
    sql_utility_ut.cpp
    sql_yqlselect_ut.cpp
)

PEERDIR(
    library/cpp/regex/pcre
    yql/essentials/utils/string
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/sql_types
    yql/essentials/core/langver
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
