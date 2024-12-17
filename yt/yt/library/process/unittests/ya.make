GTEST(unittester-library-process)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    pipes_ut.cpp
    process_ut.cpp
    subprocess_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/process
)

SIZE(MEDIUM)

END()
