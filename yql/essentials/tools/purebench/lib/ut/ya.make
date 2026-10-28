UNITTEST()

SRCS(
    benchmark_program_ut.cpp
    benchmark_ut.cpp
)

PEERDIR(
    library/cpp/time_provider
    library/cpp/yson/node
    yql/essentials/tools/purebench/lib
)

SIZE(MEDIUM)

YQL_CURRENT_ABI_VERSION()

END()
