GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    sensors_owner_ut.cpp
)

PEERDIR(
    yt/yt/library/profiling/sensors_owner
)

END()
