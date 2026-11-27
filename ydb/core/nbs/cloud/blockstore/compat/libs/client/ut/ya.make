UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/compat/libs/client)

SRCDIR(ydb/core/nbs/cloud/blockstore/compat/libs/client)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    client_ut.cpp
    durable_ut.cpp
)

END()
