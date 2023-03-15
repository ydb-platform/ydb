LIBRARY()

SRCS(
    file_tree_builder.cpp
    path_list_reader.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/utils
    library/cpp/protobuf/util
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
