#pragma once
#include "defs.h"
#include "column_engine.h"
#include "predicate.h"
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NColumnShard {
class TScanIteratorBase;
}

namespace NKikimr::NOlap {
class TIndexedReadData;
}

namespace NKikimr::NColumnShard {

class IDataTasksProcessor;

class IDataPreparationTask: public NConveyor::ITask {
private:
    std::shared_ptr<IDataTasksProcessor> OwnerOperator;
protected:
    virtual bool DoApply(NOlap::TIndexedReadData& indexedDataRead) const = 0;
    virtual bool DoExecuteImpl() = 0;

    virtual bool DoExecute() override final;
public:
    IDataPreparationTask(std::shared_ptr<IDataTasksProcessor> ownerOperator)
        : OwnerOperator(ownerOperator)
    {

    }
    using TPtr = std::shared_ptr<IDataPreparationTask>;
    virtual ~IDataPreparationTask() = default;
    bool Apply(NOlap::TIndexedReadData& indexedDataRead) const {
        return DoApply(indexedDataRead);
    }
};

class IDataTasksProcessor {
private:
    TAtomicCounter DataProcessorAddDataCounter = 0;
protected:
    virtual bool DoAdd(IDataPreparationTask::TPtr task) = 0;
    std::atomic<bool> Stopped = false;
public:
    i64 GetDataCounter() const {
        return DataProcessorAddDataCounter.Val();
    }

    void Stop() {
        Stopped = true;
    }
    bool IsStopped() const {
        return Stopped;
    }

    using TPtr = std::shared_ptr<IDataTasksProcessor>;
    virtual ~IDataTasksProcessor() = default;
    bool Add(IDataPreparationTask::TPtr task) {
        if (DoAdd(task)) {
            DataProcessorAddDataCounter.Inc();
            return true;
        }
        return false;

    }
};
}

namespace NKikimr::NOlap {

struct TReadStats {
    TInstant BeginTimestamp;
    ui32 SelectedIndex{0};
    ui64 IndexGranules{0};
    ui64 IndexPortions{0};
    ui64 IndexBatches{0};
    ui64 CommittedBatches{0};
    ui32 UsedColumns{0};
    ui64 DataBytes{0};

    TReadStats(ui32 indexNo)
        : BeginTimestamp(TInstant::Now())
        , SelectedIndex(indexNo)
    {}

    TDuration Duration() {
        return TInstant::Now() - BeginTimestamp;
    }
};

// Holds all metedata that is needed to perform read/scan
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
    virtual std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan() const = 0;
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

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan() const override;

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

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan() const override;
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
public:
    TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata)
        : ReadMetadata(readMetadata)
    {
        Y_VERIFY(ReadMetadata->SelectInfo);
    }

    /// @returns blobId -> granule map. Granules could be read independently
    THashMap<TBlobRange, ui64> InitRead(ui32 numNotIndexed, bool inGranulesOrder = false);

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

    void AddIndexed(const TBlobRange& blobRange, const TString& column, NColumnShard::IDataTasksProcessor::TPtr processor);
    size_t NumPortions() const { return PortionBatch.size(); }
    bool HasIndexRead() const { return WaitIndexed.size() || Indexed.size(); }
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobs.contains(blobRange);
    }
    void ForceFinishWaiting() {
        WaitIndexed.clear();
    }
    bool HasWaitIndexed() const {
        return WaitIndexed.size();
    }
private:
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    ui32 FirstIndexedBatch{0};
    THashMap<TBlobRange, TString> Data;
    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;
    THashMap<ui32, std::shared_ptr<arrow::RecordBatch>> Indexed;

    class TAssembledNotFiltered: public NColumnShard::IDataPreparationTask {
    private:
        using TBase = NColumnShard::IDataPreparationTask;
        TPortionInfo::TPreparedBatchData BatchConstructor;
        std::shared_ptr<arrow::RecordBatch> FilteredBatch;
        NOlap::TReadMetadata::TConstPtr ReadMetadata;
        ui32 BatchNo = 0;
        bool AllowEarlyFilter = false;
    protected:
        virtual bool DoApply(TIndexedReadData& owner) const override;
        virtual bool DoExecuteImpl() override;
    public:
        TAssembledNotFiltered(TPortionInfo::TPreparedBatchData&& batchConstructor, NOlap::TReadMetadata::TConstPtr readMetadata,
            const ui32 batchNo, const bool allowEarlyFilter, NColumnShard::IDataTasksProcessor::TPtr processor)
            : TBase(processor)
            , BatchConstructor(batchConstructor)
            , ReadMetadata(readMetadata)
            , BatchNo(batchNo)
            , AllowEarlyFilter(allowEarlyFilter)
        {

        }
    };
    void PortionFinished(const ui32 batchNo, std::shared_ptr<arrow::RecordBatch> batch);

    THashMap<ui32, THashSet<TBlobRange>> WaitIndexed;
    THashMap<TBlobRange, ui32> IndexedBlobs; // blobId -> batchNo
    ui32 ReadyNotIndexed{0};
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> OutNotIndexed; // granule -> not indexed to append
    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> ReadyGranules; // granule -> portions data
    THashMap<ui64, ui32> PortionBatch; // portion -> batch
    TVector<ui64> BatchPortion; // batch -> portion
    THashMap<ui64, ui64> PortionGranule; // portion -> granule
    THashMap<ui64, ui32> GranuleWaits; // granule -> num portions to wait
    TDeque<ui64> GranulesOutOrder;
    THashSet<ui64> GranulesWithDups;
    THashSet<ui64> PortionsWithDups;
    THashSet<const void*> BatchesToDedup;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

    const TIndexInfo& IndexInfo() const {
        return ReadMetadata->IndexInfo;
    }

    const TPortionInfo& Portion(ui32 batchNo) const {
        Y_VERIFY(batchNo >= FirstIndexedBatch);
        return ReadMetadata->SelectInfo->Portions[batchNo - FirstIndexedBatch];
    }

    ui64 BatchGranule(ui32 batchNo) const {
        Y_VERIFY(batchNo < BatchPortion.size());
        ui64 portion = BatchPortion[batchNo];
        Y_VERIFY(PortionGranule.count(portion));
        return PortionGranule.find(portion)->second;
    }

    std::shared_ptr<arrow::RecordBatch> MakeNotIndexedBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, ui64 planStep, ui64 txId) const;

    NColumnShard::IDataPreparationTask::TPtr AssembleIndexedBatch(ui32 batchNo, NColumnShard::IDataTasksProcessor::TPtr processor);
    void UpdateGranuleWaits(ui32 batchNo);
    std::shared_ptr<arrow::RecordBatch> MergeNotIndexed(
        std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const;
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> ReadyToOut();
    TVector<TPartialReadResult> MakeResult(
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, int64_t maxRowsInBatch) const;
};

}
