#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>

namespace NYql::NFmr {

struct TPartitionResult {
    std::vector<TTaskTableInputRef> TaskInputs;
    bool PartitionStatus = false;
};

struct TFmrPartitionerSettings {
    ui64 MaxDataWeightPerPart = 0;
    ui64 MaxParts = 0;
};

class TFmrPartitioner {
public:
    TFmrPartitioner(
        const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
        const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
        const TFmrPartitionerSettings& settings
    );

    std::pair<std::vector<TTaskTableInputRef>, bool> PartitionFmrTablesIntoTasks(const std::vector<TFmrTableRef>& fmrTables);

private:
    struct TLeftoverRange {
        TString TableId;
        TTableRange TableRange;
        ui64 DataWeight;
    };

    void HandleFmrPartition(
        const TFmrTableId& fmrTable,
        const TString& partId,
        const std::vector<TChunkStats> stats,
        ui64 maxDataWeightPerPart,
        std::vector<TTaskTableInputRef>& currentFmrTasks,
        std::vector<TLeftoverRange>& leftoverRanges
    );

    void HandleFmrLeftoverRanges(
        ui64 maxDataWeightPerPart,
        std::vector<TTaskTableInputRef>& fmrTasks,
        std::vector<TLeftoverRange>& leftoverRanges
    );

    bool CheckMaxTasksSize(const std::vector<TTaskTableInputRef>& currentFmrTasks);

private:
    const std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsForTables_; // TableId -> all corresponding part ids.
    const std::unordered_map<TString, std::vector<TChunkStats>> PartIdStats_; // PartId -> statistics for all existing chunks in it.
    const TFmrPartitionerSettings Settings_;
};

TPartitionResult PartitionInputTablesIntoTasks(
    const std::vector<TYtTableRef>& ytInputTables,
    const std::vector<TFmrTableRef> fmrInputTables,
    TFmrPartitioner& partitioner,
    IYtCoordinatorService::TPtr ytCoordinatorService,
    const std::unordered_map<TFmrTableId, TClusterConnection> &clusterConnections,
    const TYtPartitionerSettings& ytPartitionSettings
);

} // namespace NYql::NFmr

