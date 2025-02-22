FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCDIR(yql/essentials/minikql/computation)

SRCS(
    block_layout_converter_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/threading/local_executor
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()
