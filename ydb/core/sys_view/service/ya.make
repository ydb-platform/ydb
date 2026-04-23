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
    ydb/library/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/core/graph/api
    ydb/core/graph/service
    ydb/library/aclib/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
