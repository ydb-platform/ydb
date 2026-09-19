LIBRARY()

SRCS(
    common.cpp
    decoder.cpp
    events.cpp
    fetcher.cpp
    initialization.cpp
    kqp_common.cpp
    parsing.cpp
    request_features.cpp
    service.cpp
)

GENERATE_ENUM_SERIALIZATION(kqp_common.h)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/library/accessor
    ydb/library/aclib
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/resources
    yql/essentials/core/expr_nodes
)

END()
