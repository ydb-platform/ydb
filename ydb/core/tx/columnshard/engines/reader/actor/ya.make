LIBRARY()

SRCS(
    actor.cpp
)

PEERDIR(
    ydb/core/kqp/common/events
    ydb/core/tx/columnshard/engines/reader/abstract
    yql/essentials/core/issue
)

END()
