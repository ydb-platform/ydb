
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_fmr_partitioner.h>
#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_ordered_partitioner.h>
#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_sorted_partitioner.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

struct TFmrResourceTasksResult {
    std::vector<TFmrResourceTaskInfo> Tasks;
    TMaybe<TFmrError> Error;
};

inline TFmrPartitionerSettings GetFmrPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
    TFmrPartitionerSettings settings;
    auto& fmrPartitionSettings = fmrOperationSpec["partition"]["fmr_table"];
    settings.MaxDataWeightPerPart = fmrPartitionSettings["max_data_weight_per_part"].AsInt64();
    settings.MaxParts = fmrPartitionSettings["max_parts"].AsInt64();
    return settings;
}

inline TYtPartitionerSettings GetYtPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
    TYtPartitionerSettings settings;
    auto& ytPartitionSettings = fmrOperationSpec["partition"]["yt_table"];
    settings.MaxDataWeightPerPart = ytPartitionSettings["max_data_weight_per_part"].AsInt64();
    settings.MaxParts = ytPartitionSettings["max_parts"].AsInt64();
    return settings;
}

inline TOrderedPartitionSettings GetOrderedPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
    TOrderedPartitionSettings settings;
    settings.FmrPartitionSettings = GetFmrPartitionerSettings(fmrOperationSpec);
    settings.YtPartitionSettings = GetYtPartitionerSettings(fmrOperationSpec);
    settings.YtPartitionSettings.PartitionMode = NYT::ETablePartitionMode::Ordered;
    return settings;
}

inline TSortedPartitionSettings GetSortedPartitionerSettings(const NYT::TNode& fmrOperationSpec) {
    TSortedPartitionSettings settings;
    settings.FmrPartitionSettings = GetFmrPartitionerSettings(fmrOperationSpec);
    return settings;
}

inline TFmrResourceTasksResult PartitionFmrResourcesIntoTasks(
    const std::vector<TFmrResourceOperationInfo>& fmrResources,
    const NYT::TNode& fmrOperationSpec,
    const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats
) {
    std::vector<TFmrResourceTaskInfo> fmrResourceTasks;

    auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
    auto fmrPartitioner = TFmrPartitioner(partIdsForTables, partIdStats, fmrPartitionerSettings);
    for (auto& fmrResource: fmrResources) {
        TFmrResourceTaskInfo curFmrResourceTaskInfo;
        auto [partition, partitionSuccess] = fmrPartitioner.PartitionFmrTablesIntoTasks({fmrResource.FmrTable});
        if (!partitionSuccess) {
            return TFmrResourceTasksResult{.Error = TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "Failed to partition fmrResources into tasks"
            }};
        }

        for (auto& partitionTable: partition) {
            YQL_ENSURE(partitionTable.Inputs.size() == 1);
            TFmrTableInputRef fmrTableInputRef = std::get<TFmrTableInputRef>(partitionTable.Inputs[0]);
            curFmrResourceTaskInfo.FmrResourceTasks.emplace_back(fmrTableInputRef);
        }

        curFmrResourceTaskInfo.Alias = fmrResource.Alias;
        fmrResourceTasks.emplace_back(curFmrResourceTaskInfo);
    }
    return TFmrResourceTasksResult{.Tasks = fmrResourceTasks};
}

} // namespace NYql::NFmr
