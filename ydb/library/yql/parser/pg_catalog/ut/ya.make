UNITTEST_FOR(ydb/library/yql/parser/pg_catalog)

TAG(ya:manual)

SRCS(
    catalog_ut.cpp
    catalog_consts_ut.cpp
)

ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
