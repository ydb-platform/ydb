UNITTEST_FOR(ydb/library/yql/providers/s3/range_helpers)

TAG(ya:manual)

SRCS(
    file_tree_builder_ut.cpp
    path_list_reader_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/provider
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
