LIBRARY()

SRCS(
    defs.cpp
)

PEERDIR(
    ydb/core/yq/libs/graph_params/proto
)

GENERATE_ENUM_SERIALIZATION(defs.h)

END()
