LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    quantile_digest.cpp

    proto/quantile_digest.proto
)

PEERDIR(
    library/cpp/yt/memory
    library/cpp/tdigest
    yt/yt/core
)

END()
