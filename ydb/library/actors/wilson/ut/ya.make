UNITTEST_FOR(ydb/library/actors/wilson)

SRCS(
    wilson_span_ut.cpp
    wilson_trace_ut.cpp
)

PEERDIR(
    ydb/library/actors/testlib
    ydb/library/actors/wilson/test_util
)

END()
