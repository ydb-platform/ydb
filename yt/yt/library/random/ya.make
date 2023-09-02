LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bernoulli_sampler.cpp
)

PEERDIR(
    yt/yt/core
)

END()
