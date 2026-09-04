UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/compat/libs/server)

NO_LINT()


IF (WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    server_ut.cpp
    server_test.cpp
    ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/volume_stats_test.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/libs/client
)

DATA(
    arcadia/ydb/core/nbs/cloud/blockstore/tests/certs/server.crt
    arcadia/ydb/core/nbs/cloud/blockstore/tests/certs/server.key
    arcadia/ydb/core/nbs/cloud/blockstore/tests/certs/server_fallback.crt
)

END()
