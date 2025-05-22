LIBRARY()

SRCS(
    parser_abstract.cpp
    parser_base.cpp
    json_parser.cpp
    raw_parser.cpp
)

PEERDIR(
    contrib/libs/simdjson

    library/cpp/containers/absl_flat_hash

    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common

    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic

    yql/essentials/minikql
    yql/essentials/minikql/dom
    yql/essentials/minikql/invoke_builtins
    yql/essentials/providers/common/schema
)

CFLAGS(
    -Wno-assume
)

YQL_LAST_ABI_VERSION()

END()
