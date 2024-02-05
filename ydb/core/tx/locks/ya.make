LIBRARY()

SRCS(
    locks.cpp
    locks_db.cpp
    time_counters.cpp
    range_treap.cpp
)


PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat
)

YQL_LAST_ABI_VERSION()

END()

