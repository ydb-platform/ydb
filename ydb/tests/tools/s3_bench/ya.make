PROGRAM(ydb_s3_bench)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/wrappers
    ydb/core/wrappers/events
    ydb/core/util
    ydb/core/protos
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    library/cpp/getopt
    library/cpp/threading/future
)

END()


