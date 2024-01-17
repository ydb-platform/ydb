LIBRARY()

SRCS(
    node_whiteboard.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/helpers
    ydb/library/actors/interconnect
    ydb/library/actors/protos
    library/cpp/deprecated/enum_codegen
    library/cpp/logger
    library/cpp/lwtrace/mon
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/core/base
    ydb/core/base/services
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/debug
    ydb/core/erasure
    ydb/core/protos
)

END()
