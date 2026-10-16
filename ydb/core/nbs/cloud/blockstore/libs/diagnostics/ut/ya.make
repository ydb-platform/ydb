UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/diagnostics)

REQUIREMENTS(cpu:1)
SRCS(
    dbg_counters_ut.cpp
    vchunk_counters_ut.cpp
    volume_counters_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/diagnostics

    library/cpp/testing/unittest
)

END()
