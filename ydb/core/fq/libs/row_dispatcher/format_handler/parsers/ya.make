YQL_LIBRARY()

SRCS(
    parser_abstract.cpp
    parser_base.cpp
    json_parser.cpp
    raw_parser.cpp
)

PEERDIR(
    contrib/libs/simdjson

    library/cpp/containers/absl

    ydb/core/fq/libs/row_dispatcher/common
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common
    ydb/core/fq/libs/row_dispatcher/memory

    ydb/library/yql/providers/abstract/message_stream

    yql/essentials/minikql
    yql/essentials/minikql/dom
    yql/essentials/providers/common/schema
)

CFLAGS(
    -Wno-assume
)

END()
