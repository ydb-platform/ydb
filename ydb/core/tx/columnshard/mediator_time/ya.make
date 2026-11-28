LIBRARY()

SRCS(
    catch_up.cpp
    mediator_time.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tx/columnshard/counters
    ydb/core/tx/time_cast
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
