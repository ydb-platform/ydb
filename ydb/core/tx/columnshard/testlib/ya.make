LIBRARY()

SRCS(
    controller.cpp
)

PEERDIR(
    ydb/core/testlib/controllers
    ydb/core/tx/columnshard/engines/reader/order_control
)

END()
