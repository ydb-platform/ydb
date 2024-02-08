LIBRARY()

IF (NOT OPENSOURCE AND USE_ICONV != "local")
    CXXFLAGS(-DUSE_ICONV_EXTENSIONS)
ENDIF()

SRCS(
    decodeunknownplane.cpp
    recyr.hh
    recyr_int.hh
    wide.cpp
)

PEERDIR(
    library/cpp/charset/lite
)

IF (OS_ANDROID OR OS_IOS OR LIBRARY_CHARSET_WITHOUT_LIBICONV)
    SRCS(
        iconv_mock.cpp
    )
ELSE()
    SRCS(
        iconv.cpp
    )
    PEERDIR(
        contrib/libs/libiconv
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
    lite/ut
)
