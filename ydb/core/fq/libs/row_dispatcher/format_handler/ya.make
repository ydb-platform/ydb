LIBRARY()

SRCS(
    format_handler.cpp
)

PEERDIR(
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common
    ydb/core/fq/libs/row_dispatcher/format_handler/filters
    ydb/core/fq/libs/row_dispatcher/format_handler/parsers

    ydb/library/actors/core
    ydb/library/actors/util

    ydb/library/yql/dq/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
