UNITTEST_FOR(ydb/core/fq/libs/row_dispatcher/format_handler)

SRCS(
    format_handler_ut.cpp
    topic_filter_ut.cpp
    topic_parser_ut.cpp
)

PEERDIR(
    ydb/core/fq/libs/row_dispatcher/format_handler
    ydb/core/fq/libs/row_dispatcher/format_handler/filters
    ydb/core/fq/libs/row_dispatcher/format_handler/parsers
    ydb/core/fq/libs/row_dispatcher/format_handler/ut/common

    yql/essentials/sql/pg_dummy
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
