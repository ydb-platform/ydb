UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/throttling)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

REQUIREMENTS(cpu:1)
SRCS(
    simple_leaky_bucket_ut.cpp
)

PEERDIR(
)

END()
