LIBRARY()

SRCS(
    page.cpp
    writer.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    library/cpp/json
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/core/wrappers
    ydb/library/actors/core
    ydb/library/services
)

END()
