LIBRARY()

IF (NOT OPENSOURCE)
    CXXFLAGS(-DUSE_ICONV_EXTENSIONS)
ENDIF()

SRCS(
    decodeunknownplane.cpp
    iconv.cpp
    recyr.hh
    recyr_int.hh
    wide.cpp
)

PEERDIR(
    library/cpp/charset/lite
    contrib/libs/libiconv
)

END()

RECURSE_FOR_TESTS(
    ut
    lite/ut
)
