LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/util
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/proto
)

SRCS(
    dq_common.cpp
    dq_resource_quoter.h
    dq_value.cpp
    dq_serialized_batch.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_common.h)

END()
