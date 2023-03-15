LIBRARY()

SRCS(
    db_counters.h
    db_counters.cpp
    ext_counters.h
    ext_counters.cpp
    query_history.h
    query_interval.h
    query_interval.cpp
    sysview_service.h
    sysview_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/library/aclib/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
