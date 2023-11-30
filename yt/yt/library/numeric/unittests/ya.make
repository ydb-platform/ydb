GTEST(unittester-library-numeric)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    binary_search_ut.cpp
    double_array_ut.cpp
    piecewise_linear_function_ut.cpp
    util_ut.cpp
)

ADDINCL(
    yt/yt/library/numeric
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/numeric
    library/cpp/testing/gtest
)

END()
