LIBRARY()

SRCS(
    public.cpp
    codec.cpp
    helpers.cpp

    isa_erasure.cpp

    reed_solomon.cpp
    reed_solomon_isa.cpp

    lrc.cpp
    lrc_isa.cpp
)

PEERDIR(
    contrib/libs/isa-l/erasure_code
    library/cpp/sse
    library/cpp/yt/assert
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

GENERATE_ENUM_SERIALIZATION(public.h)

END()
