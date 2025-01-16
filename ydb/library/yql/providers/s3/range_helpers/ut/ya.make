UNITTEST_FOR(ydb/library/yql/providers/s3/range_helpers)

SRCS(
    file_tree_builder_ut.cpp
    path_list_reader_ut.cpp
)

PEERDIR(
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
