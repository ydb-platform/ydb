UNITTEST_FOR(ydb/core/kqp/compile_service/helpers)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
   kqp_compile_cache_helpers_ut.cpp
)

PEERDIR(
   ydb/core/protos
   library/cpp/testing/unittest
)


END()
