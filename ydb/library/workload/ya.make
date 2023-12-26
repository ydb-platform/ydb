LIBRARY()

SRCS(
    GLOBAL stock_workload.cpp
    GLOBAL kv_workload.cpp
    workload_factory.cpp
    tpcc/tpcc_workload.cpp
    tpcc/load_data/customer.cpp
    tpcc/load_data/district.cpp
    tpcc/load_data/history.cpp
    tpcc/load_data/item.cpp
    tpcc/load_data/new_order.cpp
    tpcc/load_data/oorder.cpp
    tpcc/load_data/order_line.cpp
    tpcc/load_data/query_generator.cpp
    tpcc/load_data/stock.cpp
    tpcc/load_data/warehouse.cpp
    tpcc/load_data/load_thread_pool.cpp
)

GENERATE_ENUM_SERIALIZATION(tpcc/tpcc_config.h)
GENERATE_ENUM_SERIALIZATION_WITH_HEADER(kv_workload.h)
GENERATE_ENUM_SERIALIZATION_WITH_HEADER(stock_workload.h)

PEERDIR(
    library/cpp/getopt
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_table
)

END()
