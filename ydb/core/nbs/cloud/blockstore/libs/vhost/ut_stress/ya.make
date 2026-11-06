UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/vhost)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    server_ut_stress.cpp
    vhost_test.cpp
)

IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(cpu:1)
ENDIF()

END()
