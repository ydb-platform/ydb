LIBRARY()

PEERDIR(
    library/cpp/actors/core
    ydb/library/mkql_proto/protos
)

SRCS(
    dq_common.cpp
    dq_resource_quoter.h
    dq_value.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_common.h)

END()
