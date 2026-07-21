FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    ydb/core/base
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
)

SRCS(
    main.cpp
    ../../memtable_collection.cpp
)

END()
