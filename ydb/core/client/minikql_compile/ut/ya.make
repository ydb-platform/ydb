UNITTEST_FOR(ydb/core/client/minikql_compile)

ALLOCATOR(J)

SRCS(
    yql_expr_minikql_compile_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/client/scheme_cache_lib
    ydb/core/client/server
    ydb/core/tablet
    ydb/core/testlib/default
    ydb/library/yql/minikql
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
