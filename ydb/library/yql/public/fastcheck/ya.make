LIBRARY()

SRCS(
    fastcheck.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/user_data
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/providers/common/provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
