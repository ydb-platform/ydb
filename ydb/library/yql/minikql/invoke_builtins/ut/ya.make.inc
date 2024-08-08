FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SET(ORIG_SRC_DIR ydb/library/yql/minikql/invoke_builtins)

SET(ORIG_SOURCES
    mkql_builtins_ut.cpp
)

ADDINCL(
    ${ORIG_SRC_DIR}
)

COPY(
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES
        mkql_builtins.h
)
