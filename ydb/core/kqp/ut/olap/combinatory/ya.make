LIBRARY()

SRCS(
    abstract.cpp
    check_counter.cpp
    execute.cpp
    actualization.cpp
    compaction.cpp
    executor.cpp
    variator.cpp
    select.cpp
    bulk_upsert.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
