YQL_LIBRARY()

SRCS(
    common.cpp
    decoder.cpp
    events.cpp
    fetcher.cpp
    initialization.cpp
    kqp_common.cpp
    parsing.cpp
    service_operator.cpp
    request_features.cpp  # gn: request_features
)
# gn: request_features headers request_features.h

GENERATE_ENUM_SERIALIZATION(kqp_common.h)

PEERDIR(
    ydb/core/base
    ydb/library/accessor
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/resources
    yql/essentials/core/expr_nodes
)

END()
