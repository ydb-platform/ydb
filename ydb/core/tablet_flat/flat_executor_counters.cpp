#include "flat_executor_counters.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

#define FLAT_EXECUTOR_LATENCY_RANGES(XX) \
    XX(500,          "200-500 us") \
    XX(1000,         "0.5-1 ms")   \
    XX(5000,         "1-5 ms")     \
    XX(20000,        "5-20 ms")    \
    XX(50000,        "20-50 ms")   \
    XX(200000,       "50-200 ms")  \
    XX(500000,       "0.2-0.5 s")  \
    XX(1000000,      "0.5-1 s")    \
    XX(40000000,     "1-4 s")      \
    XX(100000000,    "4-10 s")     \
    XX(300000000,    "10-30 s")

#define FLAT_EXECUTOR_TOUCHED_BLOCKS(XX) \
    XX(0ULL,                      "0")             \
    XX(10*1024ULL,                "10240")         \
    XX(100*1024ULL,               "102400")        \
    XX(1024*1024ULL,              "1048576")       \
    XX(10*1024*1024ULL,           "10485760")      \
    XX(100*1024*1024ULL,          "104857600")     \
    XX(1024*1024*1024ULL,         "1073741824")    \
    XX(10*1024*1024*1024ULL,      "10737418240")   \
    XX(100*1024*1024*1024ULL,     "107374182400")  \
    XX(1024*1024*1024*1024ULL,    "1099511627776")

#define FLAT_EXECUTOR_DATA_SIZE(XX) \
    XX(0ULL,                      "0")             \
    XX(10*1024ULL,                "10240")         \
    XX(100*1024ULL,               "102400")        \
    XX(1024*1024ULL,              "1048576")       \
    XX(10*1024*1024ULL,           "10485760")      \
    XX(100*1024*1024ULL,          "104857600")     \
    XX(1024*1024*1024ULL,         "1073741824")    \
    XX(10*1024*1024*1024ULL,      "10737418240")   \
    XX(100*1024*1024*1024ULL,     "107374182400")  \
    XX(1024*1024*1024*1024ULL,    "1099511627776")

#define FLAT_EXECUTOR_DATA_RATE(XX) \
    XX(0ULL,                      "0")             \
    XX(10*1024ULL,                "10240")         \
    XX(100*1024ULL,               "102400")        \
    XX(1024*1024ULL,              "1048576")       \
    XX(10*1024*1024ULL,           "10485760")      \
    XX(100*1024*1024ULL,          "104857600")     \
    XX(1024*1024*1024ULL,         "1073741824")    \
    XX(10*1024*1024*1024ULL,      "10737418240")   \
    XX(100*1024*1024*1024ULL,     "107374182400")  \
    XX(1024*1024*1024*1024ULL,    "1099511627776")

#define FLAT_EXECUTOR_CONSUMED_CPU_RANGES(XX) \
    XX(0,          "0%")  \
    XX(100000,     "10%") \
    XX(200000,     "20%") \
    XX(300000,     "30%") \
    XX(400000,     "40%") \
    XX(500000,     "50%") \
    XX(600000,     "60%") \
    XX(700000,     "70%") \
    XX(800000,     "80%") \
    XX(900000,     "90%") \
    XX(1000000,    "100%")

#define FLAT_EXECUTOR_COMMIT_SIZE(XX) \
    XX(0ULL, "0") \
    XX(256ULL, "256") \
    XX(1024ULL, "1024") \
    XX(4096ULL, "4096") \
    XX(65536ULL, "65536") \
    XX(1048576ULL, "1048576") \
    XX(4194304ULL, "4194304") \
    XX(16777216ULL, "16777216") \
    XX(67108864ULL, "67108864") \
    XX(268435456ULL, "268435456") \
    XX(1073741824ULL, "1073741824")

const char* TExecutorCounters::SimpleCounterNames[TExecutorCounters::SIMPLE_COUNTER_SIZE] =
    {FLAT_EXECUTOR_SIMPLE_COUNTERS_MAP(COUNTER_TEXT_ARRAY)};
const char* TExecutorCounters::CumulativeCounterNames[TExecutorCounters::CUMULATIVE_COUNTER_SIZE] =
    {FLAT_EXECUTOR_CUMULATIVE_COUNTERS_MAP(COUNTER_TEXT_ARRAY)};
const char* TExecutorCounters::PercentileCounterNames[TExecutorCounters::PERCENTILE_COUNTER_SIZE] =
    {FLAT_EXECUTOR_PERCENTILE_COUNTERS_MAP(COUNTER_TEXT_ARRAY)};

TExecutorCounters::TExecutorCounters()
    : TTabletCountersBase(SIMPLE_COUNTER_SIZE, CUMULATIVE_COUNTER_SIZE, PERCENTILE_COUNTER_SIZE, SimpleCounterNames, CumulativeCounterNames, PercentileCounterNames)
{
    static TTabletPercentileCounter::TRangeDef txLatencyConfig[] = { FLAT_EXECUTOR_LATENCY_RANGES(COUNTER_PERCENTILE_CONFIG_ARRAY) };
    static TTabletPercentileCounter::TRangeDef txTouchedConfig[] = { FLAT_EXECUTOR_TOUCHED_BLOCKS(COUNTER_PERCENTILE_CONFIG_ARRAY) };
    static TTabletPercentileCounter::TRangeDef txDataSize[] = { FLAT_EXECUTOR_DATA_SIZE(COUNTER_PERCENTILE_CONFIG_ARRAY) };
    static TTabletPercentileCounter::TRangeDef txDataRate[] = { FLAT_EXECUTOR_DATA_RATE(COUNTER_PERCENTILE_CONFIG_ARRAY) };
    static TTabletPercentileCounter::TRangeDef txConsumedCpu[] = { FLAT_EXECUTOR_CONSUMED_CPU_RANGES(COUNTER_PERCENTILE_CONFIG_ARRAY) };
    static TTabletPercentileCounter::TRangeDef txCommitSize[] = { FLAT_EXECUTOR_COMMIT_SIZE(COUNTER_PERCENTILE_CONFIG_ARRAY) };

    Percentile()[TX_PERCENTILE_LATENCY_RO].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_LATENCY_RW].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_LATENCY_COMMIT].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_EXECUTE_CPUTIME].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_BOOKKEEPING_CPUTIME].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_COMMITED_CPUTIME].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_LOGSNAP_CPUTIME].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_PARTSWITCH_CPUTIME].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_TOUCHED_BLOCKS].Initialize(txTouchedConfig, false);
    Percentile()[TX_PERCENTILE_DB_DATA_BYTES].Initialize(txDataSize, false);
    Percentile()[TX_PERCENTILE_TABLET_BYTES_READ].Initialize(txDataRate, false);
    Percentile()[TX_PERCENTILE_TABLET_BYTES_WRITTEN].Initialize(txDataRate, false);
    Percentile()[TX_PERCENTILE_CONSUMED_CPU].Initialize(txConsumedCpu, false);
    Percentile()[TX_PERCENTILE_FOLLOWERSYNC_LATENCY].Initialize(txLatencyConfig, false);
    Percentile()[TX_PERCENTILE_COMMIT_REDO_BYTES].Initialize(txCommitSize, false);
}

}}
