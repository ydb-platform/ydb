UNITTEST_FOR(ydb/core/memory_controller)

SIZE(SMALL)

TIMEOUT(60)

SRCS(
    memory_controller_ut.cpp
    memtable_collection_ut.cpp
)

END()
