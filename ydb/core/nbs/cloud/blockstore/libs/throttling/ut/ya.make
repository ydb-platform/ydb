UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/throttling)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    simple_leaky_bucket_ut.cpp
)

PEERDIR(
)

IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(cpu:1)
ENDIF()

END()
