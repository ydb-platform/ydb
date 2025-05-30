LIBRARY()

SRCS(
    common.cpp
    decoder.cpp
    events.cpp
    fetcher.cpp
    kqp_common.cpp
    initialization.cpp
    parsing.cpp
    request_features.cpp
)

GENERATE_ENUM_SERIALIZATION(kqp_common.h)

PEERDIR(
    ydb/core/base
    ydb/library/accessor
    ydb/library/actors/core
    yql/essentials/core/expr_nodes
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/resources
)

END()
