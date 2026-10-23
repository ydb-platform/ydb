LIBRARY()

SRCS(
    backup_actor.cpp
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
    ydb/library/actors/protos
    ydb/library/services
)

END()
