LIBRARY()

YQL_ABI_VERSION(2 9 0)

SRCS(
    proto_builder.cpp
    module.cpp
    type_builder.cpp
    value_builder.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/yql
    ydb/library/yql/public/udf
    ydb/library/yql/minikql
    yt/cpp/mapreduce/interface
    yt/yt_proto/yt/formats
    yt/yt_proto/yt/formats
)

END()

RECURSE_FOR_TESTS(
    ut
)
