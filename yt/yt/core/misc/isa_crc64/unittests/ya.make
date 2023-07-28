EXECTEST(crc64_reference_test)

NO_UTIL()

SIZE(
    SMALL
)

RUN(
    crc64_reference_test
)

DEPENDS(
    yt/yt/core/misc/isa_crc64/unittests/crc64_reference_test
)

REQUIREMENTS(ram:12)

END()

RECURSE_FOR_TESTS(
    crc64_reference_test
)
