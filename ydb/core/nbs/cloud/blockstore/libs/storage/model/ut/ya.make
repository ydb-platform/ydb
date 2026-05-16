UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/model)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    log_prefix_ut.cpp
    log_title_ut.cpp
)

PEERDIR(
)

END()
