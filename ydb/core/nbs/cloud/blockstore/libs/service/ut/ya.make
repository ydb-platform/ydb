UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/service)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    blocks_info_ut.cpp
    device_handler_ut.cpp
)

PEERDIR(

)

END()
