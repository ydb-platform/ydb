UNITTEST_FOR(ydb/core/kqp/compile_service/helpers)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
   kqp_compile_cache_helpers_ut.cpp
)

PEERDIR(
   ydb/core/protos
   library/cpp/testing/unittest
)

END()
