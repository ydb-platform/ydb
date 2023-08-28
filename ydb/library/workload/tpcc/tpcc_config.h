#pragma once

#include <ydb/library/workload/workload_query_generator.h>

namespace NYdbWorkload {
namespace NTPCC {

static const std::vector<std::string> TPCCNameTokens {
    "BAR", "OUGHT", "ABLE", "PRI",
    "PRES", "ESE", "ANTI", "CALLY",
    "ATION", "EING"
};

enum ETPCCWorkloadConstants : i32 {
    TPCC_WAREHOUSES = 10,
    TPCC_DIST_PER_WH = 10,
    TPCC_CUST_PER_DIST = 3000,
    TPCC_ITEM_COUNT = 100000,
    TPCC_TERMINAL_PER_WH = 10,

    TPCC_THREADS = 10,
    TPCC_RETRY_COUNT = 10,
    TPCC_MIN_PARTITIONS = 50,
    TPCC_MAX_PARTITIONS = 500000,
    TPCC_AUTO_PARTITIONING = true,
    TPCC_LOAD_BATCH_SIZE = 128,

    TPCC_NEWORDER_WEIGHT = 45,
    TPCC_PAYMENT_WEIGHT = 43,
    TPCC_ORDERSTATUS_WEIGHT = 4,
    TPCC_DELIVERY_WEIGHT = 4,
    TPCC_STOCKLEVEL_WEIGHT = 4,

    TPCC_NEWORDER_PRE_EXEC_WAIT = 18000,
    TPCC_NEWORDER_POST_EXEC_WAIT = 12000,
    TPCC_PAYMENT_PRE_EXEC_WAIT = 3000,
    TPCC_PAYMENT_POST_EXEC_WAIT = 12000,
    TPCC_ORDERSTATUS_PRE_EXEC_WAIT = 2000,
    TPCC_ORDERSTATUS_POST_EXEC_WAIT = 10000,
    TPCC_DELIVERY_PRE_EXEC_WAIT = 2000,
    TPCC_DELIVERY_POST_EXEC_WAIT = 5000,
    TPCC_STOCKLEVEL_PRE_EXEC_WAIT = 2000,
    TPCC_STOCKLEVEL_POST_EXEC_WAIT = 5000,
    TPCC_WORKLOAD_DURATION_SECONDS = 600,

    TPCC_FIRST_UNPROCESSED_O_ID = 2101,
    TPCC_C_LAST_LOAD_C = 157,
    TPCC_C_LAST_RUN_C = 223,
    TPCC_INVALID_ITEM_ID = -12345,
};

struct TTPCCWorkloadParams : public TWorkloadParams {
    i32 Warehouses = ETPCCWorkloadConstants::TPCC_WAREHOUSES;
    i32 Threads = ETPCCWorkloadConstants::TPCC_THREADS;
    i32 MinPartitions = ETPCCWorkloadConstants::TPCC_MIN_PARTITIONS;
    i32 MaxPartitions = ETPCCWorkloadConstants::TPCC_MAX_PARTITIONS;
    bool AutoPartitioning = ETPCCWorkloadConstants::TPCC_AUTO_PARTITIONING;
    i32 LoadBatchSize = ETPCCWorkloadConstants::TPCC_LOAD_BATCH_SIZE;
    i32 RetryCount = ETPCCWorkloadConstants::TPCC_RETRY_COUNT;
    i32 Duration = ETPCCWorkloadConstants::TPCC_WORKLOAD_DURATION_SECONDS;
    i32 OrderLineIdConst;
};

enum ETPCCProcedureType {
    NewOrder,
    Payment,
    Delivery,
    OrderStatus,
    StockLevel
};

}
}
