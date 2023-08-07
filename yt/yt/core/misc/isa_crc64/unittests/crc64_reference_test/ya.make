PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

NO_UTIL()

NO_COMPILER_WARNINGS()

PEERDIR(
    yt/yt/core/misc/isa_crc64
)

SRCS(
    crc64_reference_test.c
)

END()
