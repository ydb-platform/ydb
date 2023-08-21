LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/library/erasure

    library/cpp/erasure
)

SRCS(
    codec.cpp
    codec_detail.cpp
)

IF (OPENSOURCE)
    SRCS(codec_opensource.cpp)
ELSE()
    SRCS(codec_yandex.cpp)
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
