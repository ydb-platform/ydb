LIBRARY()

SRCS(
    borders_flow_controller.cpp
    common.cpp
    context.cpp
    events.cpp
    executor.cpp
    filters.cpp
    manager.cpp
    merge.cpp
    private_events.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()

RECURSE_FOR_TESTS(
    ut
)
