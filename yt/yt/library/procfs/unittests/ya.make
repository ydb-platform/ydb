GTEST(unittester-library-procfs)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    procfs_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/library/procfs
)

SIZE(SMALL)

END()
