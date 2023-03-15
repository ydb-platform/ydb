UNITTEST_FOR(ydb/library/yql/providers/s3/range_helpers)

SRCS(
    file_tree_builder_ut.cpp
    path_list_reader_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/provider
)

YQL_LAST_ABI_VERSION()

END()
