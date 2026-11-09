UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/throttling)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    simple_leaky_bucket_ut.cpp
)

PEERDIR(
)

IF (SANITIZER_TYPE)
ELSE()
    REQUIREMENTS(cpu:1)
ENDIF()

END()
