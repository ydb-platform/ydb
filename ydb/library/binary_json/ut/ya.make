UNITTEST_FOR(ydb/library/binary_json)

SRCS(
    container_ut.cpp
    identity_ut.cpp
    entry_ut.cpp
    test_base.cpp
    valid_ut.cpp
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    FORK_SUBTESTS()
    TIMEOUT(2400)
    SPLIT_FACTOR(20)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/library/binary_json
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/core/issue/protos
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
