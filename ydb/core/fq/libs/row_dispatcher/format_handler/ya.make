LIBRARY()

SRCS(
    data_packer.cpp
    format_handler.cpp
)

PEERDIR(
    ydb/core/fq/libs/row_dispatcher/common
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common
    ydb/core/fq/libs/row_dispatcher/format_handler/filters
    ydb/core/fq/libs/row_dispatcher/format_handler/parsers
    ydb/core/fq/libs/row_dispatcher/memory

    ydb/library/actors/core
    ydb/library/actors/util

    ydb/library/yql/dq/common
    ydb/library/yverify_stream
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
