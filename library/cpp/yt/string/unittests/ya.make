GTEST(unittester-library-string)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    enum_ut.cpp
    format_ut.cpp
    guid_ut.cpp
    string_ut.cpp
)

PEERDIR(
    library/cpp/yt/string
    library/cpp/testing/gtest
)

END()
