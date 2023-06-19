LIBRARY()

SRCS(
    controller.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/engines/reader/order_control
)

END()
