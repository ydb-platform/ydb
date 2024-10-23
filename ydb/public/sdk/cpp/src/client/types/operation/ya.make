LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    operation.cpp
    out.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/types
)

END()
