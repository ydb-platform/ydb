LIBRARY()

NO_UTIL()

SRCS(
    generated/composition.cpp
    generated/decomposition.cpp
    decomposition_table.h
    normalization.cpp
)

IF(NOT OPENSOURCE)
    SRCS(
        custom_encoder.cpp
    )
    PEERDIR(
        library/cpp/charset/lite
    )
    GENERATE_ENUM_SERIALIZATION(normalization.h)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
