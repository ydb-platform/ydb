GTEST(unittester-yt-library-numeric)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    double_array_ut.cpp
)

ADDINCL(
    yt/yt/library/numeric/serialize
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/numeric/serialize
    library/cpp/testing/gtest
)

END()
