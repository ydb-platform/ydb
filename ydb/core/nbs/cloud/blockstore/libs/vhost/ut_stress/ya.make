UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/vhost)
REQUIREMENTS(cpu:2)


INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    server_ut_stress.cpp
    vhost_test.cpp
)

END()
