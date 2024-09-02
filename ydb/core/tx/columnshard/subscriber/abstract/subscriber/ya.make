LIBRARY()

SRCS(
    subscriber.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/subscriber/events/writes_completed
)

END()
