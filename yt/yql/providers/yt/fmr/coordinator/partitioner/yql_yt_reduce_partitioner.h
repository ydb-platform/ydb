#pragma once

#include "yql_yt_sorted_partitioner_base.h"

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_fmr_partitioner.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yql/essentials/utils/log/log.h>

#include <queue>

namespace NYql::NFmr {

struct TReducePartitionSettings {
    TFmrPartitionerSettings FmrPartitionSettings;
    ui64 MaxKeySizePerPart = 0;
};

class TReducePartitioner: public TSortedPartitionerBase {
public:
    TReducePartitioner(
        const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
        const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
        const TSortingColumns& reduceBy,
        const TReducePartitionSettings& settings
    );

private:
    TPartitionResult PartitionFmrTables(const std::vector<TFmrTableRef>& inputTables);

    TTaskTableInputRef CreateTaskInputFromSlices(
        const std::vector<TSlice>& slices,
        const std::vector<TFmrTableRef>& inputTables,
        bool isLastRange
    ) override;

    TFmrTableKeysRange GetReadRangeFromSlices(const std::vector<TSlice>& slices, bool isLastRange) override;

    void CheckMaxKeySizePerSlices(const std::vector<TSlice>& slices);

    void ChangeLeftKeyBoundaryIfNeeded(
        TFmrTableKeysBoundary& leftKey,
        bool& isLeftInclusive,
        const TPartitionerFilterBoundary& filterBoundary
    ) override;

    void ChangeRightKeyBoundaryIfNeeded(TFmrTableKeysBoundary& rightKey, const TFmrTableKeysBoundary& taskRangeLastKey) override;


    void ExtendChunksPerTable(std::unordered_map<TString, std::vector<TChunkUnit>>& chunksByTable) override;

private:
    const TSortingColumns ReduceBy_;
    const TReducePartitionSettings Settings_;
    TMaybe<TFmrTableKeysBoundary> LeftBoundary_; // key right boundary with which we non-inclusively chopped previous reduce job.
    std::queue<std::unordered_map<TString, std::vector<TChunkUnit>>> LeftBoundaryChunks_;
    ui64 CurrentLastKeyWeight_ = 0 ;
};

} // namespace NYql::NFmr
