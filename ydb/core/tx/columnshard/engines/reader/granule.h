#pragma once
#include "batch.h"
#include "read_metadata.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext;
class TGranulesLiveControl {
private:
    TAtomicCounter GranulesCounter = 0;
public:
    i64 GetCount() const {
        return GranulesCounter.Val();
    }

    void Inc() {
        GranulesCounter.Inc();
    }
    void Dec() {
        Y_VERIFY(GranulesCounter.Dec() >= 0);
    }
};

class TGranule {
public:
    using TPtr = std::shared_ptr<TGranule>;
private:
    ui64 GranuleId = 0;
    ui64 RawDataSize = 0;
    ui64 RawDataSizeReal = 0;

    bool NotIndexedBatchReadyFlag = false;
    bool InConstruction = false;
    std::shared_ptr<arrow::RecordBatch> NotIndexedBatch;
    std::shared_ptr<NArrow::TColumnFilter> NotIndexedBatchFutureFilter;

    std::vector<std::shared_ptr<arrow::RecordBatch>> RecordBatches;
    bool DuplicationsAvailableFlag = false;
    bool ReadyFlag = false;
    std::deque<TBatch> Batches;
    std::set<ui32> WaitBatches;
    std::set<ui32> GranuleBatchNumbers;
    std::shared_ptr<TGranulesLiveControl> LiveController;
    TGranulesFillingContext* Owner = nullptr;
    THashSet<const void*> BatchesToDedup;

    TScanMemoryLimiter::TGuard GranuleDataSize;
    void CheckReady();
public:
    TGranule(const ui64 granuleId, TGranulesFillingContext& owner);
    ~TGranule();

    ui64 GetGranuleId() const noexcept {
        return GranuleId;
    }

    void OnGranuleDataPrepared(std::vector<std::shared_ptr<arrow::RecordBatch>>&& data);

    const THashSet<const void*>& GetBatchesToDedup() const noexcept {
        return BatchesToDedup;
    }

    const std::shared_ptr<arrow::RecordBatch>& GetNotIndexedBatch() const noexcept {
        return NotIndexedBatch;
    }

    const std::shared_ptr<NArrow::TColumnFilter>& GetNotIndexedBatchFutureFilter() const noexcept {
        return NotIndexedBatchFutureFilter;
    }

    bool IsNotIndexedBatchReady() const noexcept {
        return NotIndexedBatchReadyFlag;
    }

    bool IsDuplicationsAvailable() const noexcept {
        return DuplicationsAvailableFlag;
    }

    void SetDuplicationsAvailable(bool val) noexcept {
        DuplicationsAvailableFlag = val;
    }

    bool IsReady() const noexcept {
        return ReadyFlag;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> GetReadyBatches() const {
        return RecordBatches;
    }

    TBatch& GetBatchInfo(const ui32 batchIdx) {
        Y_VERIFY(batchIdx < Batches.size());
        return Batches[batchIdx];
    }

    void AddNotIndexedBatch(std::shared_ptr<arrow::RecordBatch> batch);

    const TGranulesFillingContext& GetOwner() const {
        return *Owner;
    }

    TGranulesFillingContext& GetOwner() {
        return *Owner;
    }

    class TBatchForMerge {
    private:
        TBatch* Batch = nullptr;
        YDB_ACCESSOR_DEF(std::optional<ui32>, PoolId);
        YDB_ACCESSOR_DEF(std::shared_ptr<TSortableBatchPosition>, From);
        YDB_ACCESSOR_DEF(std::shared_ptr<TSortableBatchPosition>, To);
    public:
        TBatch* operator->() {
            return Batch;
        }

        TBatch& operator*() {
            return *Batch;
        }

        TBatchForMerge(TBatch* batch, std::shared_ptr<TSortableBatchPosition> from, std::shared_ptr<TSortableBatchPosition> to)
            : Batch(batch)
            , From(from)
            , To(to)
        {
            Y_VERIFY(Batch);
        }

        bool operator<(const TBatchForMerge& item) const {
            if (!From && !item.From) {
                return false;
            } else if (From && item.From) {
                return From->Compare(*item.From) == std::partial_ordering::less;
            } else if (!From) {
                return true;
            } else {
                return false;
            }
        }
    };

    std::deque<TBatchForMerge> SortBatchesByPK(const bool reverse, TReadMetadata::TConstPtr readMetadata);

    const std::set<ui32>& GetEarlyFilterColumns() const;
    void StartConstruction();
    void OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);
    void OnBlobReady(const TBlobRange& range) noexcept;
    bool OnFilterReady(TBatch& batchInfo);
    TBatch& RegisterBatchForFetching(const TPortionInfo& portionInfo);
    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) const;

};

}
