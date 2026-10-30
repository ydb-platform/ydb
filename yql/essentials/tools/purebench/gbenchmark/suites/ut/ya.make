UNITTEST()

YQL_CURRENT_ABI_VERSION()

SRCS(
    suite_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yql/essentials/tools/purebench/gbenchmark/suites
)

DATA(
    arcadia/yql/essentials/tools/purebench/gbenchmark/suites/ut/test_suite
)

END()
