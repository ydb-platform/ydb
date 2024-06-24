#include "resource_pool_settings.h"


namespace NKikimr::NResourcePool {

std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings) {
    return {
        {"concurrent_query_limit", &settings.ConcurrentQueryLimit},
        {"query_count_limit", &settings.QueryCountLimit},
        {"query_cancel_after_seconds", &settings.QueryCancelAfter},
        {"query_memory_limit_ratio_per_node", &settings.QueryMemoryLimitRatioPerNode}
    };
}

}  // namespace NKikimr::NResourcePool
