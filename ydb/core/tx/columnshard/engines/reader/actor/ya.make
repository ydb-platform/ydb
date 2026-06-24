LIBRARY()

SRCS(
    actor.cpp
)

PEERDIR(
    ydb/core/kqp/compute_actor/events
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/library/yql/dq/actors
    yql/essentials/core/issue
)

END()
