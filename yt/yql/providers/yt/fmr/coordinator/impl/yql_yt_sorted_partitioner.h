#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_fmr_partitioner.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yql/essentials/utils/log/log.h>


namespace NYql::NFmr {

struct TSortedPartitionSettings {
    TFmrPartitionerSettings FmrPartitionSettings;
};

struct TSortedPartitionerFilterBoundary {
    TFmrTableKeysBoundary FilterBoundary;
    bool IsInclusive;
};

class TSortedPartitioner {
public:
    TSortedPartitioner(
        const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
        const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
        TSortingColumns KeyColumns,
        const TSortedPartitionSettings& settings
    );

    std::pair<std::vector<TTaskTableInputRef>, bool> PartitionTablesIntoTasksSorted(
        const std::vector<TOperationTableRef>& inputTables
    );

private:
    struct TChunkUnit {
        TString TableId;
        TString PartId;
        ui64 ChunkIndex = 0;
        ui64 DataWeight = 0;
        TFmrTableKeysRange KeyRange;
    };

    class TChunkContainer {
    public:
        TChunkContainer() = default;

        void Push(TChunkUnit chunk);
        bool IsEmpty() const;
        const std::vector<TChunkUnit>& GetChunks() const;
        void Clear();

        const TFmrTableKeysRange& GetKeysRange() const;
        void UpdateKeyRange(const TFmrTableKeysRange& KeyRange);

    private:
        std::vector<TChunkUnit> Chunks_;
        TFmrTableKeysRange KeysRange_;
    };

    class TFmrTablesChunkPool {
    public:
        TFmrTablesChunkPool(
            const std::vector<TFmrTableRef>& inputTables,
            const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
            const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
            const TSortingColumns& KeyColumns
        );

        void PutBack(TChunkUnit chunk);
        void UpdateFilterBoundary(const TString& tableId, const TSortedPartitionerFilterBoundary& FilterBoundary);
        bool IsNotEmpty() const;

        const std::vector<TString>& GetTableOrder() const;
        TMaybe<TChunkUnit> ReadNextChunk(const TString& tableId);

        TMaybe<TSortedPartitionerFilterBoundary> GetFilterBoundary(const TString& tableId) const;
        TSortedPartitionerFilterBoundary GetMaxFilterBoundary(const TString& tableId, const TSortedPartitionerFilterBoundary& FilterBoundary) const;
        TFmrTableKeysRange GetEffectiveKeysRange(const TChunkUnit& chunk) const;

    private:
        void InitTableInputs(const std::vector<TFmrTableRef>& inputTables);

        std::vector<TString> TableOrder_;
        std::unordered_map<TString, std::deque<TChunkUnit>> TableInputs_;
        std::unordered_map<TString, TSortedPartitionerFilterBoundary> FilterBoundaries_;
        const std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables_;
        const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats_;
        const TSortingColumns& KeyColumns_;
    };

    std::vector<TTaskTableInputRef> PartitionFmrTables(
        const std::vector<TFmrTableRef>& inputTables
    );

    struct TSlice {
        std::unordered_map<TString, std::vector<TChunkUnit>> ChunksByTable;
        TFmrTableKeysRange RangeForRead;
        std::unordered_map<TString, TSortedPartitionerFilterBoundary> PerTableLeft;
        ui64 Weight = 0;
    };

    TMaybe<TSlice> ReadSlice(TFmrTablesChunkPool& chunkPool);
    TTaskTableInputRef CreateTaskInputFromSlices(const std::vector<TSlice>& slices, const std::vector<TFmrTableRef>& inputTables) const;

    std::unordered_map<TString, std::vector<TTableRange>> ProcessChunkForSlice(
        TFmrTablesChunkPool& chunkPool,
        const TChunkContainer& container,
        const TFmrTableKeysRange& taskRange
    );

    ui64 CollectFmrTotalWeight(
        const std::vector<TFmrTableRef>& inputTables
    );

private:
    const std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsForTables_;
    const std::unordered_map<TString, std::vector<TChunkStats>> PartIdStats_;
    const TSortingColumns KeyColumns_;
    const TSortedPartitionSettings Settings_;
};

TPartitionResult PartitionInputTablesIntoTasksSorted(
    const std::vector<TOperationTableRef>& inputTables,
    TSortedPartitioner& partitioner
);

} // namespace NYql::NFmr
