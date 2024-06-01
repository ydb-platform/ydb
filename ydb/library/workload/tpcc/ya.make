LIBRARY()

SRCS(
    workload.cpp
    load_task.cpp
    terminal.cpp
    load_data/customer.cpp
    load_data/district.cpp
    load_data/history.cpp
    load_data/item.cpp
    load_data/new_order.cpp
    load_data/oorder.cpp
    load_data/order_line.cpp
    load_data/query_generator.cpp
    load_data/stock.cpp
    load_data/warehouse.cpp
    procedures/delivery.cpp
    procedures/new_order.cpp
    procedures/order_status.cpp
    procedures/payment.cpp
    procedures/procedure.cpp
    procedures/stock_level.cpp
    procedures/tables.cpp
)

GENERATE_ENUM_SERIALIZATION(config.h)
GENERATE_ENUM_SERIALIZATION(procedures/delivery.h)
GENERATE_ENUM_SERIALIZATION(procedures/new_order.h)
GENERATE_ENUM_SERIALIZATION(procedures/order_status.h)
GENERATE_ENUM_SERIALIZATION(procedures/payment.h)
GENERATE_ENUM_SERIALIZATION(procedures/stock_level.h)


PEERDIR(
    library/cpp/retry
    library/cpp/threading/task_scheduler
    ydb/public/sdk/cpp/client/ydb_table
)

END()