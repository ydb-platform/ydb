GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    convert_ut.cpp
    saveload_ut.cpp
)

PEERDIR(
    library/cpp/yt/yson_string
    library/cpp/testing/gtest
    library/cpp/testing/gtest_extensions
)

END()
