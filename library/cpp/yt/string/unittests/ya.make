GTEST(unittester-library-string-helpers)

OWNER(g:yt)

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
