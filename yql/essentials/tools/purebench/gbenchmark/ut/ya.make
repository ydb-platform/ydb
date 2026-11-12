UNITTEST()

YQL_CURRENT_ABI_VERSION()

SRCS(
    register_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yql/essentials/tools/purebench/gbenchmark
)

END()
