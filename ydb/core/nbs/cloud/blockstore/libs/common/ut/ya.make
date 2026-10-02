UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/common)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    block_checksums_ut.cpp
    printable_params_ut.cpp
)

PEERDIR(
    contrib/libs/xxhash
)

END()
