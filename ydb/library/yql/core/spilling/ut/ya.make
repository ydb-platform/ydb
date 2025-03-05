UNITTEST_FOR(ydb/library/yql/core/spilling)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

# https://github.com/ydb-platform/ydb/issues/12513
IF (SANITIZER_TYPE == "address")
    TAG(ya:not_autocheck)
ENDIF()

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:10)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    spilling_ut.cpp
 )

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
