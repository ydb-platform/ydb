#include "resource_pool_settings.h"


namespace NKikimr::NResourcePool {

std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings) {
    return {
        {"concurrent_query_limit", &settings.ConcurrentQueryLimit},
        {"queue_size", &settings.QueueSize},
        {"query_cancel_after_seconds", &settings.QueryCancelAfter},
        {"query_memory_limit_percent_per_node", &settings.QueryMemoryLimitPercentPerNode}
    };
}

}  // namespace NKikimr::NResourcePool
