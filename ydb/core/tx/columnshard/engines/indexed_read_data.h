#pragma once
#include "defs.h"
#include "column_engine.h"
#include "predicate.h"
#include "reader/queue.h"
#include "reader/granule.h"
#include "reader/batch.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/counters.h>

namespace NKikimr::NColumnShard {
class TScanIteratorBase;
}

namespace NKikimr::NOlap {

struct TReadStats {
    TInstant BeginTimestamp;
    ui32 SelectedIndex{0};
    ui64 IndexGranules{0};
    ui64 IndexPortions{0};
    ui64 IndexBatches{0};
    ui64 CommittedBatches{0};
    ui64 PortionsBytes{ 0 };
    ui64 DataFilterBytes{ 0 };
    ui64 DataAdditionalBytes{ 0 };

    ui32 SchemaColumns = 0;
    ui32 FilterColumns = 0;
    ui32 AdditionalColumns = 0;

    ui32 SelectedRows = 0;

    TReadStats(ui32 indexNo)
        : BeginTimestamp(TInstant::Now())
        , SelectedIndex(indexNo)
    {}

    void PrintToLog() {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("event", "statistic")
            ("begin", BeginTimestamp)
            ("selected", SelectedIndex)
            ("index_granules", IndexGranules)
            ("index_portions", IndexPortions)
            ("index_batches", IndexBatches)
            ("committed_batches", CommittedBatches)
            ("schema_columns", SchemaColumns)
            ("filter_columns", FilterColumns)
            ("additional_columns", AdditionalColumns)
            ("portions_bytes", PortionsBytes)
            ("data_filter_bytes", DataFilterBytes)
            ("data_additional_bytes", DataAdditionalBytes)
            ("delta_bytes", PortionsBytes - DataFilterBytes - DataAdditionalBytes)
            ("selected_rows", SelectedRows)
            ;
    }

    TDuration Duration() {
        return TInstant::Now() - BeginTimestamp;
    }
};

// Holds all metadata that is needed to perform read/scan
struct TReadMetadataBase {
    using TConstPtr = std::shared_ptr<const TReadMetadataBase>;

    enum class ESorting {
        NONE = 0,
        ASC,
        DESC,
    };

    virtual ~TReadMetadataBase() = default;

    std::shared_ptr<NOlap::TPredicate> LessPredicate;
    std::shared_ptr<NOlap::TPredicate> GreaterPredicate;
    std::shared_ptr<arrow::Schema> BlobSchema;
    std::shared_ptr<arrow::Schema> LoadSchema; // ResultSchema + required for intermediate operations
    std::shared_ptr<arrow::Schema> ResultSchema; // TODO: add Program modifications
    std::shared_ptr<NSsa::TProgram> Program;
    std::shared_ptr<const THashSet<TUnifiedBlobId>> ExternBlobs;
    ESorting Sorting{ESorting::ASC}; // Sorting inside returned batches
    ui64 Limit{0}; // TODO

    bool IsAscSorted() const { return Sorting == ESorting::ASC; }
    bool IsDescSorted() const { return Sorting == ESorting::DESC; }
    bool IsSorted() const { return IsAscSorted() || IsDescSorted(); }
    void SetDescSorting() { Sorting = ESorting::DESC; }

    virtual TVector<std::pair<TString, NScheme::TTypeInfo>> GetResultYqlSchema() const = 0;
    virtual TVector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const = 0;
    virtual std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const = 0;
    virtual void Dump(IOutputStream& out) const { Y_UNUSED(out); };

    // TODO:  can this only be done for base class?
    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadataBase& meta) {
        meta.Dump(out);
        return out;
    }
};

// Holds all metadata that is needed to perform read/scan
struct TReadMetadata : public TReadMetadataBase, public std::enable_shared_from_this<TReadMetadata> {
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    TIndexInfo IndexInfo;
    ui64 PlanStep = 0;
    ui64 TxId = 0;
    std::shared_ptr<TSelectInfo> SelectInfo;
    std::vector<TCommittedBlob> CommittedBlobs;
    THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CommittedBatches;
    std::shared_ptr<TReadStats> ReadStats;

    TReadMetadata(const TIndexInfo& info)
        : IndexInfo(info)
        , ReadStats(std::make_shared<TReadStats>(info.GetId()))
    {}

    std::vector<std::string> GetColumnsOrder() const {
        std::vector<std::string> result;
        for (auto&& i : LoadSchema->fields()) {
            result.emplace_back(i->name());
        }
        return result;
    }

    std::set<ui32> GetEarlyFilterColumnIds(const bool noTrivial) const;
    std::set<ui32> GetUsedColumnIds() const;

    bool Empty() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->Portions.empty() && CommittedBlobs.empty();
    }

    std::shared_ptr<arrow::Schema> GetSortingKey() const {
        return IndexInfo.GetSortingKey();
    }

    std::shared_ptr<arrow::Schema> GetReplaceKey() const {
        return IndexInfo.GetReplaceKey();
    }

    TVector<TNameTypeInfo> GetResultYqlSchema() const override {
        TVector<NTable::TTag> columnIds;
        columnIds.reserve(ResultSchema->num_fields());
        for (const auto& field: ResultSchema->fields()) {
            TString name = TStringBuilder() << field->name();
            columnIds.emplace_back(IndexInfo.GetColumnId(name));
        }
        return IndexInfo.GetColumns(columnIds);
    }

    TVector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return IndexInfo.GetPrimaryKey();
    }

    size_t NumIndexedRecords() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->NumRecords();
    }

    size_t NumIndexedBlobs() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->Stats().Blobs;
    }

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const override;

    void Dump(IOutputStream& out) const override {
        out << "columns: " << (LoadSchema ? LoadSchema->num_fields() : 0)
            << " index records: " << NumIndexedRecords()
            << " index blobs: " << NumIndexedBlobs()
            << " committed blobs: " << CommittedBlobs.size()
            << " with program steps: " << (Program ? Program->Steps.size() : 0)
            << (Sorting == ESorting::NONE ? " not" : (Sorting == ESorting::ASC ? " asc" : " desc"))
            << " sorted, at snapshot: " << PlanStep << ":" << TxId;
        if (GreaterPredicate) {
            out << " from{" << *GreaterPredicate << "}";
        }
        if (LessPredicate) {
            out << " to{" << *LessPredicate << "}";
        }
        if (SelectInfo) {
            out << ", " << *SelectInfo;
        }
    }

    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadata& meta) {
        meta.Dump(out);
        return out;
    }
};

struct TReadStatsMetadata : public TReadMetadataBase, public std::enable_shared_from_this<TReadStatsMetadata> {
    using TConstPtr = std::shared_ptr<const TReadStatsMetadata>;

    const ui64 TabletId;
    TVector<ui32> ReadColumnIds;
    TVector<ui32> ResultColumnIds;
    THashMap<ui64, std::shared_ptr<NOlap::TColumnEngineStats>> IndexStats;

    explicit TReadStatsMetadata(ui64 tabletId)
        : TabletId(tabletId)
    {}

    TVector<std::pair<TString, NScheme::TTypeInfo>> GetResultYqlSchema() const override;

    TVector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const override;
};

// Represents a batch of rows produced by ASC or DESC scan with applied filters and partial aggregation
struct TPartialReadResult {
    std::shared_ptr<arrow::RecordBatch> ResultBatch;

    // This 1-row batch contains the last key that was read while producing the ResultBatch.
    // NOTE: it might be different from the Key of last row in ResulBatch in case of filtering/aggregation/limit
    std::shared_ptr<arrow::RecordBatch> LastReadKey;

    std::string ErrorString;
};

class TIndexedReadData {
private:
    std::set<ui32> EarlyFilterColumns;
    std::set<ui32> UsedColumns;
    YDB_READONLY_DEF(std::set<ui32>, PostFilterColumns);
    bool AbortedFlag = false;
    YDB_READONLY_DEF(NColumnShard::TScanCounters, Counters);
    std::vector<NIndexedReader::TBatch*> Batches;
    TFetchBlobsQueue& FetchBlobsQueue;
    friend class NIndexedReader::TBatch;
    friend class NIndexedReader::TGranule;
public:
    TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata, TFetchBlobsQueue& fetchBlobsQueue, const bool internalRead, const NColumnShard::TScanCounters& counters);

    NIndexedReader::TBatch& GetBatchInfo(const ui32 batchNo);

    /// Initial FetchBlobsQueue filling (queue from external scan iterator). Granules could be read independently
    void InitRead(ui32 numNotIndexed, bool inGranulesOrder = false);

    /// @returns batches and corresponding last keys in correct order (i.e. sorted by by PK)
    TVector<TPartialReadResult> GetReadyResults(const int64_t maxRowsInBatch);

    void AddNotIndexed(ui32 batchNo, TString blob, ui64 planStep, ui64 txId) {
        auto batch = NArrow::DeserializeBatch(blob, ReadMetadata->BlobSchema);
        AddNotIndexed(batchNo, batch, planStep, txId);
    }

    void AddNotIndexed(ui32 batchNo, const std::shared_ptr<arrow::RecordBatch>& batch, ui64 planStep, ui64 txId) {
        Y_VERIFY(batchNo < NotIndexed.size());
        if (!NotIndexed[batchNo]) {
            ++ReadyNotIndexed;
        }
        NotIndexed[batchNo] = MakeNotIndexedBatch(batch, planStep, txId);
    }

    void AddIndexed(const TBlobRange& blobRange, const TString& column, NColumnShard::TDataTasksProcessorContainer processor);
    bool IsInProgress() const { return Granules.size() > ReadyGranulesAccumulator.size(); }
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobs.contains(blobRange);
    }
    void Abort() {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "abort");
        for (auto&& i : Granules) {
            ReadyGranulesAccumulator.emplace(i.first);
        }
        AbortedFlag = true;
        Y_VERIFY(ReadyGranulesAccumulator.size() == Granules.size());
        Y_VERIFY(!IsInProgress());
    }
private:
    NOlap::TReadMetadata::TConstPtr ReadMetadata;

    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;

    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch);
    void OnGranuleReady(NIndexedReader::TGranule& granule) {
        Y_VERIFY(GranulesToOut.emplace(granule.GetGranuleId(), &granule).second);
        Y_VERIFY(ReadyGranulesAccumulator.emplace(granule.GetGranuleId()).second || AbortedFlag);
    }

    void OnBatchReady(const NIndexedReader::TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
        if (batch && batch->num_rows()) {
            ReadMetadata->ReadStats->SelectedRows += batch->num_rows();
            if (batchInfo.IsDuplicationsAvailable()) {
                Y_VERIFY(batchInfo.GetOwner().IsDuplicationsAvailable());
                BatchesToDedup.insert(batch.get());
            } else {
                Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, IndexInfo().GetReplaceKey(), false));
            }
        }
    }

    THashSet<const void*> BatchesToDedup;
    THashMap<TBlobRange, NIndexedReader::TBatch*> IndexedBlobSubscriber; // blobId -> batch
    THashMap<ui64, NIndexedReader::TGranule*> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
    THashSet<TBlobRange> IndexedBlobs;
    ui32 ReadyNotIndexed{0};
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> OutNotIndexed; // granule -> not indexed to append
    THashMap<ui64, NIndexedReader::TGranule> Granules;
    TDeque<NIndexedReader::TGranule*> GranulesOutOrder;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

    std::vector<NIndexedReader::TGranule*> DetachReadyInOrder();

    const TIndexInfo& IndexInfo() const {
        return ReadMetadata->IndexInfo;
    }

    std::shared_ptr<arrow::RecordBatch> MakeNotIndexedBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, ui64 planStep, ui64 txId) const;

    std::shared_ptr<arrow::RecordBatch> MergeNotIndexed(
        std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const;
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> ReadyToOut();
    TVector<TPartialReadResult> MakeResult(
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, int64_t maxRowsInBatch) const;
};

}
