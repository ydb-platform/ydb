#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_fmr_partitioner.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

struct TOrderedPartitionSettings {
    TYtPartitionerSettings YtPartitionSettings;
    TFmrPartitionerSettings FmrPartitionSettings;
};

class TOrderedPartitioner {
public:
    TOrderedPartitioner(
        const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
        const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
        const TOrderedPartitionSettings& settings
    );

    std::pair<std::vector<TTaskTableInputRef>, bool> PartitionTablesIntoTasksOrdered(
        const std::vector<TOperationTableRef>& inputTables,
        IYtCoordinatorService::TPtr ytCoordinatorService,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
    );

private:
    struct TOrderedPartitionerOutput {
        std::vector<TTaskTableInputRef> TaskInputs;
        ui64 ChunksNum = 0;

        void append(const TTaskTableInputRef& taskInput) {
            TaskInputs.push_back(taskInput);
            ChunksNum += taskInput.Inputs.size();
        }
    };

    TOrderedPartitionerOutput PartitionFmrTablesScope(
        const std::vector<TFmrTableRef>& fmrTablesScope
    );

    TOrderedPartitionerOutput PartitionYtTablesScope(
        const std::vector<TYtTableRef>& ytTables,
        IYtCoordinatorService::TPtr ytCoordinatorService,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
    );

private:
    const std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsForTables_;
    const std::unordered_map<TString, std::vector<TChunkStats>> PartIdStats_;
    const TOrderedPartitionSettings Settings_;
};

TPartitionResult PartitionInputTablesIntoTasksOrdered(
    const std::vector<TOperationTableRef>& inputTables,
    TOrderedPartitioner& partitioner,
    IYtCoordinatorService::TPtr ytCoordinatorService,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections
);

} // namespace NYql::NFmr
