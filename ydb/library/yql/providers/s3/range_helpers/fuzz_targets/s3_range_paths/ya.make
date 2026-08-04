FUZZ()

SIZE(MEDIUM)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/yql/providers/s3/range_helpers
    ydb/library/yql/providers/s3/proto
    library/cpp/protobuf/util
)

END()
