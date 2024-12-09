LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/util
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/proto
    yql/essentials/utils
)

SRCS(
    dq_common.cpp
    dq_resource_quoter.h
    dq_value.cpp
    dq_serialized_batch.cpp
    rope_over_buffer.h
    rope_over_buffer.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_common.h)

END()
