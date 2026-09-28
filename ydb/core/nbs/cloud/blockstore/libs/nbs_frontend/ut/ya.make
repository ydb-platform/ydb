UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/nbs_frontend)

SRCS(
    frontend_test.cpp
    frontend_registry_ut.cpp
    blockstore_facade_ut.cpp
)

PEERDIR(ydb/library/actors/testlib)

END()
