#pragma once

#include <cstddef>
#include <chrono>

namespace NYdb::NTPCC {

constexpr size_t TERMINALS_PER_WAREHOUSE = 10;

// Partitioning constants
constexpr size_t DEFAULT_SHARD_SIZE_MB = 2000;

// copy-pasted TPC-C constants from the Benchbase implementation

constexpr int DISTRICT_LOW_ID = 1;
constexpr int DISTRICT_HIGH_ID = 10;
constexpr int DISTRICT_COUNT = DISTRICT_HIGH_ID - DISTRICT_LOW_ID + 1;

constexpr int C_ID_C = 259; // in range [0, 1023]
constexpr int CUSTOMERS_PER_DISTRICT = 3000;

constexpr int OL_I_ID_C = 7911; // in range [0, 8191]
constexpr int ITEM_COUNT = 100000;

constexpr int INVALID_ITEM_ID = -12345;

// NewOrder transaction related constants
constexpr int MIN_ITEMS = 5;
constexpr int MAX_ITEMS = 15;

constexpr int C_LAST_LOAD_C = 157; // in range [0, 255]
constexpr int C_LAST_RUN_C = 223; // in range [0, 255]

constexpr int FIRST_UNPROCESSED_O_ID = 2101;

constexpr double DISTRICT_INITIAL_YTD = 30000.00;

// Transaction weights (percentage of mix)
constexpr double NEW_ORDER_WEIGHT = 45.0;
constexpr double PAYMENT_WEIGHT = 43.0;
constexpr double ORDER_STATUS_WEIGHT = 4.0;
constexpr double DELIVERY_WEIGHT = 4.0;
constexpr double STOCK_LEVEL_WEIGHT = 4.0;

// Transaction keying times (in seconds)
constexpr std::chrono::seconds NEW_ORDER_KEYING_TIME{18};
constexpr std::chrono::seconds PAYMENT_KEYING_TIME{3};
constexpr std::chrono::seconds ORDER_STATUS_KEYING_TIME{2};
constexpr std::chrono::seconds DELIVERY_KEYING_TIME{2};
constexpr std::chrono::seconds STOCK_LEVEL_KEYING_TIME{2};

// Transaction think times (in seconds)
constexpr std::chrono::seconds NEW_ORDER_THINK_TIME{12};
constexpr std::chrono::seconds PAYMENT_THINK_TIME{12};
constexpr std::chrono::seconds ORDER_STATUS_THINK_TIME{10};
constexpr std::chrono::seconds DELIVERY_THINK_TIME{5};
constexpr std::chrono::seconds STOCK_LEVEL_THINK_TIME{5};

// this constant is from TPC-C standard (#4.1). It is calculated based on
// thinking and keying times and number of terminals per warehouse
constexpr double MAX_TPMC_PER_WAREHOUSE = 12.86;

// Table names
constexpr const char* TABLE_CUSTOMER = "customer";
constexpr const char* TABLE_WAREHOUSE = "warehouse";
constexpr const char* TABLE_DISTRICT = "district";
constexpr const char* TABLE_NEW_ORDER = "new_order";
constexpr const char* TABLE_OORDER = "oorder";
constexpr const char* TABLE_ITEM = "item";
constexpr const char* TABLE_STOCK = "stock";
constexpr const char* TABLE_ORDER_LINE = "order_line";
constexpr const char* TABLE_HISTORY = "history";

constexpr std::array<const char*, 9> TPCC_TABLES = {
    TABLE_CUSTOMER,
    TABLE_WAREHOUSE,
    TABLE_DISTRICT,
    TABLE_NEW_ORDER,
    TABLE_OORDER,
    TABLE_ITEM,
    TABLE_STOCK,
    TABLE_ORDER_LINE,
    TABLE_HISTORY
};

// Index/View names
constexpr const char* INDEX_CUSTOMER_NAME = "idx_customer_name";
constexpr const char* INDEX_ORDER = "idx_order";

constexpr const size_t INDEX_COUNT = 2;

// don't change order!
enum class ETransactionType {
    NewOrder = 0,
    Delivery = 1,
    OrderStatus = 2,
    Payment = 3,
    StockLevel = 4
};

constexpr const size_t TUI_LOG_LINES = 1000;

// lower limit, real number is higher
constexpr const size_t WAREHOUSES_PER_CPU_CORE = 1500;

} // namespace NYdb::NTPCC
