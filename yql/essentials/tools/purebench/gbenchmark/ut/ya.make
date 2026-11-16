UNITTEST()

YQL_CURRENT_ABI_VERSION()

SRCS(
    register_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/gbenchmark
    yql/essentials/tools/purebench/gbenchmark
)

END()
