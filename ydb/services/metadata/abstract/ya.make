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
    ydb/library/accessor
    library/cpp/actors/core
    ydb/services/metadata/request
    ydb/public/api/protos
    ydb/core/base
    ydb/library/yql/core/expr_nodes
)

END()
