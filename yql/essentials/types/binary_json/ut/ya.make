UNITTEST_FOR(yql/essentials/types/binary_json)

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
ENDIF()

PEERDIR(
    yql/essentials/types/binary_json
    yql/essentials/minikql/dom
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/issue/protos
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
