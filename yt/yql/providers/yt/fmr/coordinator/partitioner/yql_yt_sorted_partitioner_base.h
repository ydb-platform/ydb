#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/coordinator/partitioner/yql_yt_fmr_partitioner.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>

namespace NYql::NFmr {

struct TPartitionerFilterBoundary {
    TFmrTableKeysBoundary FilterBoundary;
    bool IsInclusive;
};

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
    void UpdateFilterBoundary(const TString& tableId, const TPartitionerFilterBoundary& filterBoundary);
    bool IsNotEmpty() const;

    const std::vector<TString>& GetTableOrder() const;
    TMaybe<TChunkUnit> ReadNextChunk(const TString& tableId);

    TMaybe<TPartitionerFilterBoundary> GetFilterBoundary(const TString& tableId) const;
    TPartitionerFilterBoundary GetMaxFilterBoundary(const TString& tableId, const TPartitionerFilterBoundary& filterBoundary) const;
    TFmrTableKeysRange GetEffectiveKeysRange(const TChunkUnit& chunk) const;

    void SetError(TFmrError error);
    TMaybe<TFmrError> GetError() const;

private:
    void InitTableInputs(const std::vector<TFmrTableRef>& inputTables);

    std::vector<TString> TableOrder_;
    std::unordered_map<TString, std::deque<TChunkUnit>> TableInputs_;
    std::unordered_map<TString, TPartitionerFilterBoundary> FilterBoundaries_;
    const std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables_;
    const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats_;
    const TSortingColumns& KeyColumns_;
    TMaybe<TFmrError> Error_;
};

struct TSlice {
    std::unordered_map<TString, std::vector<TChunkUnit>> ChunksByTable;
    TFmrTableKeysRange RangeForRead;
    std::unordered_map<TString, TPartitionerFilterBoundary> PerTableLeft;
    ui64 Weight = 0;
};

struct TReadSliceResult {
    TMaybe<TSlice> Slice;
    TMaybe<TFmrError> Error;
};

class TSortedPartitionerBase: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TSortedPartitionerBase>;

    virtual ~TSortedPartitionerBase() = default;

    TSortedPartitionerBase(
        const std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
        const std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats,
        const TSortingColumns& keyColumns,
        const TFmrPartitionerSettings& fmrPartitionSettings
    );

    TPartitionResult PartitionTablesIntoTasks(const std::vector<TOperationTableRef>& inputTables);

protected:
    ui64 CollectFmrTotalWeight(const std::vector<TFmrTableRef>& inputTables);

    TReadSliceResult ReadSlice(TFmrTablesChunkPool& chunkPool);

    virtual TTaskTableInputRef CreateTaskInputFromSlices(
        const std::vector<TSlice>& slices,
        const std::vector<TFmrTableRef>& inputTables,
        bool isLastRange
    ) = 0;

    TPartitionResult PartitionFmrTables(
        const std::vector<TFmrTableRef>& inputTables
    );

    virtual TFmrTableKeysRange GetReadRangeFromSlices(const std::vector<TSlice>& slices, bool isLastRange) = 0;

    virtual void ChangeLeftKeyBoundaryIfNeeded(
        TFmrTableKeysBoundary& leftKey,
        bool& isLeftInclusive,
        const TPartitionerFilterBoundary& filterBoundary
    ) = 0;

    virtual void ChangeRightKeyBoundaryIfNeeded(TFmrTableKeysBoundary& rightKey, const TFmrTableKeysBoundary& taskRangeLastKey) = 0;

    TTaskTableInputRef CreateTaskInputFromSlicesImpl(
        const std::vector<TSlice>& slices,
        const std::vector<TFmrTableRef>& inputTables,
        bool isLastRange
    );

    virtual void ExtendChunksPerTable(std::unordered_map<TString, std::vector<TChunkUnit>>& chunksByTable) = 0;

private:
    const std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsForTables_;
    const std::unordered_map<TString, std::vector<TChunkStats>> PartIdStats_;
    TSortingColumns KeyColumns_;
    TFmrPartitionerSettings FmrPartitionSettings_;
};

} // namespace NYql::NFmr
