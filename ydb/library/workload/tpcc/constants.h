#pragma once

#include <cstddef>
#include <chrono>

namespace NYdb::NTPCC {

constexpr size_t TERMINALS_PER_WAREHOUSE = 10;

// copy-pasted TPC-C constants from the Benchbase implementation

constexpr int DISTRICT_LOW_ID = 1;
constexpr int DISTRICT_HIGH_ID = 10;

constexpr int C_ID_C = 259; // in range [0, 1023]
constexpr int CUSTOMERS_PER_DISTRICT = 3000;

constexpr int OL_I_ID_C = 7911; // in range [0, 8191]
constexpr int ITEMS_COUNT = 100000;

constexpr int INVALID_ITEM_ID = -12345;

// NewOrder transaction related constants
constexpr int MIN_ITEMS = 5;
constexpr int MAX_ITEMS = 15;

constexpr int C_LAST_LOAD_C = 157; // in range [0, 255]
constexpr int C_LAST_RUN_C = 223; // in range [0, 255]

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

} // namespace NYdb::NTPCC
