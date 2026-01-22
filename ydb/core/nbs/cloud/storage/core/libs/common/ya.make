LIBRARY()

GENERATE_ENUM_SERIALIZATION(error.h)

SRCS(
    context.cpp
    error.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/json/writer
    library/cpp/threading/future
)

END()
