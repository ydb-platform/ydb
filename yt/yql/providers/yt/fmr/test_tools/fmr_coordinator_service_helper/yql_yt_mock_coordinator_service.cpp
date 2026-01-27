#include "yql_yt_mock_coordinator_service.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

std::pair<std::vector<TYtTableTaskRef>, bool> TMockYtCoordinatorService::PartitionYtTables(
    const std::vector<TYtTableRef>& ytTables,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    const TYtPartitionerSettings& settings
) {
    Y_UNUSED(settings);
    Y_UNUSED(clusterConnections);
    std::vector<TYtTableTaskRef> result;
    for (const auto& table : ytTables) {
        const TFmrTableId id(table.GetCluster(), table.GetPath());
        YQL_ENSURE(TableToResult_.contains(id), "No mock partitions configured for YT table " << id.Id);
        const auto& r = TableToResult_.at(id);
        if (!r.Status) {
            return {{}, false};
        }
        result.insert(result.end(), r.Partitions.begin(), r.Partitions.end());
    }

    return {result, true};
}

} // namespace NYql::NFmr


