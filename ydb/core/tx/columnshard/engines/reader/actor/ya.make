LIBRARY()

SRCS(
    actor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/conveyor_composite/usage
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/kqp/compute_actor
    yql/essentials/core/issue
)

END()
