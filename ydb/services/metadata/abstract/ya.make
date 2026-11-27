YQL_LIBRARY()

SRCS(
    common.cpp
    decoder.cpp
    events.cpp
    fetcher.cpp
    initialization.cpp
    kqp_common.cpp
    parsing.cpp
    request_features.cpp
)

GENERATE_ENUM_SERIALIZATION(kqp_common.h)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/json
    library/cpp/json/writer
    ydb/core/base
    ydb/library/accessor
    ydb/library/actors/core
    ydb/public/api/protos
    yql/essentials/core/expr_nodes
)

END()
