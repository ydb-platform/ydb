#include "resource_pool_settings.h"


namespace NKikimr::NResourcePool {

std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings, bool restricted) {
    std::unordered_map<TString, TProperty> properties = {
        {"concurrent_query_limit", &settings.ConcurrentQueryLimit},
        {"queue_size", &settings.QueueSize},
        {"query_memory_limit_percent_per_node", &settings.QueryMemoryLimitPercentPerNode},
        {"database_load_cpu_threshold", &settings.DatabaseLoadCpuThreshold}
    };
    if (!restricted) {
        properties.insert({"query_cancel_after_seconds", &settings.QueryCancelAfter});
    }
    return properties;
}

}  // namespace NKikimr::NResourcePool
