G_BENCHMARK()

SRCS(
    reserve.cpp
    saveload.cpp
    implicit_operators.cpp
)

PEERDIR(
    library/cpp/yson/node
)

RESOURCE(
    library/cpp/yson/node/benchmark/complex.yson complex.yson
)

SIZE(MEDIUM)

END()
