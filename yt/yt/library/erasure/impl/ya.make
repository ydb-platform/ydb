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
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
