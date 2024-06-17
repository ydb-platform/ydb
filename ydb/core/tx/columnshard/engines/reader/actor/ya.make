LIBRARY()

SRCS(
    actor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/kqp/compute_actor
    ydb/library/yql/core/issue
)

END()
