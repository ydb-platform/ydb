#pragma once

#include <ydb/library/workload/workload_query_generator.h>

namespace NYdbWorkload {
namespace NTPCC {

static const std::vector<std::string> NameTokens {
    "BAR", "OUGHT", "ABLE", "PRI",
    "PRES", "ESE", "ANTI", "CALLY",
    "ATION", "EING"
};

enum EWorkloadConstants : i32 {
    TPCC_WAREHOUSES = 10,
    TPCC_DIST_PER_WH = 10,
    TPCC_CUST_PER_DIST = 3000,
    TPCC_ITEM_COUNT = 100000,
    TPCC_TERMINAL_PER_WH = 10,

    TPCC_THREADS = 10,
    TPCC_NETWORK_THREADS = 2,
    TPCC_SCHEDULER_THREADS = 4,
    TPCC_MAX_ACTIVE_SESSIONS = 500,

    // (2**attempt * EXP_MULTIPLIER)
    TPCC_RETRY_COUNT = 3,
    TPCC_INSTANT_RETRY_COUNT = 6,
    TPCC_FAST_RETRY_EXP_MULTIPLIER = 50,
    TPCC_SLOW_RETRY_EXP_MULTIPLIER = 500,

    TPCC_MAX_PARTITIONS = 500000,
    TPCC_LOAD_BATCH_SIZE = 512,

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
    TPCC_WORKLOAD_WARMUP_SECONDS = 300,

    TPCC_FIRST_UNPROCESSED_O_ID = 2101,
    TPCC_C_LAST_LOAD_C = 157,
    TPCC_C_LAST_RUN_C = 223,
    TPCC_INVALID_ITEM_ID = -12345,
};

struct TLoadParams {
    std::string DbPath;
    i32 Warehouses = EWorkloadConstants::TPCC_WAREHOUSES;
    i32 Threads = EWorkloadConstants::TPCC_THREADS;
    i32 NetworkThreads = EWorkloadConstants::TPCC_NETWORK_THREADS;
    i32 MaxPartitions = EWorkloadConstants::TPCC_MAX_PARTITIONS;
    i32 LoadBatchSize = EWorkloadConstants::TPCC_LOAD_BATCH_SIZE;
    i32 RetryCount = EWorkloadConstants::TPCC_RETRY_COUNT;
    i32 DurationS = EWorkloadConstants::TPCC_WORKLOAD_DURATION_SECONDS;
    i64 Seed = -1;
    bool DisableAutoPartitioning = false;
    bool OnlyConfiguring = false;
    bool OnlyCreatingIndices = false;
    bool DebugMode = false;
};

struct TRunParams {
    i32 Warehouses = EWorkloadConstants::TPCC_WAREHOUSES;
    i32 Threads = EWorkloadConstants::TPCC_THREADS;
    i32 NetworkThreads = EWorkloadConstants::TPCC_NETWORK_THREADS;
    i32 RetryCount = EWorkloadConstants::TPCC_RETRY_COUNT;
    i32 InstanyRetryCount = EWorkloadConstants::TPCC_INSTANT_RETRY_COUNT;
    i32 DurationS = EWorkloadConstants::TPCC_WORKLOAD_DURATION_SECONDS;
    i32 WarmupDurationS = EWorkloadConstants::TPCC_WORKLOAD_WARMUP_SECONDS;
    i32 SchedulerThreads = EWorkloadConstants::TPCC_SCHEDULER_THREADS;
    i32 MaxActiveSessions = EWorkloadConstants::TPCC_MAX_ACTIVE_SESSIONS;
    i32 WarehouseStartId = 1;
    i32 OrderLineItemIdDelta = 0;
    i32 CustomerIdDelta = 0;
    i32 OnlyOneType = -1;
    i64 Seed = -1;
    bool DebugMode = false;
    bool ErrorMode = false;
};

enum EProcedureType {
    NewOrder,
    Payment,
    Delivery,
    OrderStatus,
    StockLevel,
    COUNT
};

enum ETablesType {
    customer,
    district,
    history,
    item,
    new_order,
    oorder,
    order_line,
    stock,
    warehouse
};

}
}
