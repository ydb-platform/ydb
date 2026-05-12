#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>

#include <unordered_map>

namespace NYql::NFmr {

class TMockYtCoordinatorService final : public IYtCoordinatorService {
public:
    TMockYtCoordinatorService() = default;

    void SetPartitionsForTable(const TFmrTableId& tableId, std::vector<TYtTableTaskRef> partitions, bool status = true) {
        TableToResult_[tableId] = {std::move(partitions), status};
    }

    std::pair<std::vector<TYtTableTaskRef>, bool> PartitionYtTables(
        const std::vector<TYtTableRef>& ytTables,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        const TYtPartitionerSettings& settings
    ) override;

private:
    struct TResult {
        std::vector<TYtTableTaskRef> Partitions;
        bool Status = true;
    };

private:
    std::unordered_map<TFmrTableId, TResult> TableToResult_;
};

} // namespace NYql::NFmr


