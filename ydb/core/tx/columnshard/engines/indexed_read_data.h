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

    std::set<std::string> GetFilterColumns(const bool early) const;

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
    std::vector<TBlobRange> InitRead(ui32 numNotIndexed, bool inGranulesOrder = false);

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
    bool IsInProgress() const { return Granules.size() > ReadyGranulesAccumulator.size(); }
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobs.contains(blobRange);
    }
    void Abort() {
        for (auto&& i : Granules) {
            ReadyGranulesAccumulator.emplace(i.first);
        }
        Y_VERIFY(ReadyGranulesAccumulator.size() == Granules.size());
        Y_VERIFY(!IsInProgress());
    }
private:
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;

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

    class TGranule;

    class TBatch {
    private:
        YDB_READONLY(ui64, BatchNo, 0);
        YDB_READONLY(ui64, Portion, 0);
        YDB_READONLY(ui64, Granule, 0);
        THashSet<TBlobRange> WaitIndexed;
        YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, FilteredBatch);
        YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
        THashMap<TBlobRange, TString> Data;
        TGranule* Owner = nullptr;
        const TPortionInfo* PortionInfo = nullptr;

        friend class TGranule;
        TBatch(const ui32 batchNo, TGranule& owner, const TPortionInfo & portionInfo);
        void FillBlobsForFetch(std::vector<TBlobRange>& result) const {
            for (auto&& i : WaitIndexed) {
                result.emplace_back(i);
            }
        }
    public:
        NColumnShard::IDataPreparationTask::TPtr AssembleIndexedBatch(NColumnShard::IDataTasksProcessor::TPtr processor, NOlap::TReadMetadata::TConstPtr readMetadata);

        const THashSet<TBlobRange>& GetWaitingBlobs() const {
            return WaitIndexed;
        }

        const TGranule& GetOwner() const {
            return *Owner;
        }

        bool IsFetchingReady() const {
            return WaitIndexed.empty();
        }

        const TPortionInfo& GetPortionInfo() const {
            return *PortionInfo;
        }

        void InitBatch(std::shared_ptr<arrow::RecordBatch> batch) {
            Y_VERIFY(!FilteredBatch);
            FilteredBatch = batch;
            Owner->OnBatchReady(*this, batch);
        }

        bool AddIndexedReady(const TBlobRange& bRange, const TString& blobData) {
            if (!WaitIndexed.erase(bRange)) {
                return false;
            }
            Data.emplace(bRange, blobData);
            return true;
        }
    };

    class TGranule {
    private:
        YDB_READONLY(ui64, GranuleId, 0);
        YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::RecordBatch>>, ReadyBatches);
        YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
        YDB_READONLY_FLAG(Ready, false);
        THashMap<ui32, TBatch> Batches;
        std::set<ui32> WaitBatches;
        TIndexedReadData* Owner = nullptr;
        void OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
            Y_VERIFY(!ReadyFlag);
            Y_VERIFY(WaitBatches.erase(batchInfo.GetBatchNo()));
            if (batch && batch->num_rows()) {
                ReadyBatches.emplace_back(batch);
            }
            Owner->OnBatchReady(batchInfo, batch);
            if (WaitBatches.empty()) {
                ReadyFlag = true;
                Owner->OnGranuleReady(*this);
            }
        }

    public:
        friend class TIndexedReadData::TBatch;
        TGranule(const ui64 granuleId, TIndexedReadData& owner)
            : GranuleId(granuleId)
            , Owner(&owner)
        {

        }

        void FillBlobsForFetch(std::vector<TBlobRange>& result) const {
            for (auto&& i : Batches) {
                i.second.FillBlobsForFetch(result);
            }
        }

        TBatch& AddBatch(const ui32 batchNo, const TPortionInfo& portionInfo) {
            Y_VERIFY(!ReadyFlag);
            WaitBatches.emplace(batchNo);
            auto infoEmplace = Batches.emplace(batchNo, TBatch(batchNo, *this, portionInfo));
            Y_VERIFY(infoEmplace.second);
            return infoEmplace.first->second;
        }
    };

    void OnGranuleReady(TGranule& granule) {
        Y_VERIFY(GranulesToOut.emplace(granule.GetGranuleId(), &granule).second);
        Y_VERIFY(ReadyGranulesAccumulator.emplace(granule.GetGranuleId()).second);
    }

    void OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
        if (batch && batch->num_rows()) {
            if (batchInfo.IsDuplicationsAvailable()) {
                Y_VERIFY(batchInfo.GetOwner().IsDuplicationsAvailable());
                BatchesToDedup.insert(batch.get());
            } else {
                Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, IndexInfo().GetReplaceKey(), false));
            }
        }
    }

    THashSet<const void*> BatchesToDedup;
    THashMap<TBlobRange, TBatch*> IndexedBlobSubscriber; // blobId -> batch
    THashMap<ui64, TGranule*> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
    THashSet<TBlobRange> IndexedBlobs;
    ui32 ReadyNotIndexed{0};
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> OutNotIndexed; // granule -> not indexed to append
    TVector<TBatch*> BatchInfo;
    THashMap<ui64, TGranule> Granules;
    TDeque<TGranule*> GranulesOutOrder;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

    std::vector<TGranule*> DetachReadyInOrder();

    const TIndexInfo& IndexInfo() const {
        return ReadMetadata->IndexInfo;
    }

    std::shared_ptr<arrow::RecordBatch> MakeNotIndexedBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, ui64 planStep, ui64 txId) const;

    NColumnShard::IDataPreparationTask::TPtr AssembleIndexedBatch(const TBatch& batch, NColumnShard::IDataTasksProcessor::TPtr processor);
    std::shared_ptr<arrow::RecordBatch> MergeNotIndexed(
        std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const;
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> ReadyToOut();
    TVector<TPartialReadResult> MakeResult(
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, int64_t maxRowsInBatch) const;
};

}
