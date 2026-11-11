UNITTEST_FOR(ydb/core/kqp/compile_service/helpers)

SIZE(MEDIUM)

SRCS(
   kqp_compile_cache_helpers_ut.cpp
)

PEERDIR(
   ydb/core/protos
   library/cpp/testing/unittest
)

END()
