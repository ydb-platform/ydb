UNITTEST_FOR(yql/essentials/parser/pg_catalog)

SRCS(
    catalog_ut.cpp
    catalog_consts_ut.cpp
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
