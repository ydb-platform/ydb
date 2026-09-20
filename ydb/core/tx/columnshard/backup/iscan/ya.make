LIBRARY()

SRCS(
    iscan.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/protos  # stopgap: columnshard_private_events.h transitively requires engines/protos; direct columnshard dep would create a cycle
    ydb/core/tx/datashard
    ydb/library/actors/core
    ydb/library/services
    ydb/library/signals
)

END()

RECURSE_FOR_TESTS(
    ut
)