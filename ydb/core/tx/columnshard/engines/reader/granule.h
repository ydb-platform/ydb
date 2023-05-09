#pragma once
#include "batch.h"
#include "read_metadata.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext;

class TGranule {
private:
    ui64 GranuleId = 0;
    YDB_READONLY(ui64, GranuleIdx, 0);

    bool NotIndexedBatchReadyFlag = false;
    std::shared_ptr<arrow::RecordBatch> NotIndexedBatch;
    std::shared_ptr<NArrow::TColumnFilter> NotIndexedBatchFutureFilter;

    std::vector<std::shared_ptr<arrow::RecordBatch>> RecordBatches;
    bool DuplicationsAvailableFlag = false;
    bool ReadyFlag = false;
    std::deque<TBatch> Batches;
    std::set<ui32> WaitBatches;
    std::set<ui32> GranuleBatchNumbers;
    TGranulesFillingContext* Owner = nullptr;
    THashSet<const void*> BatchesToDedup;

    void CheckReady();
public:
    TGranule(const ui64 granuleId, const ui64 granuleIdx, TGranulesFillingContext& owner)
        : GranuleId(granuleId)
        , GranuleIdx(granuleIdx)
        , Owner(&owner) {
    }

    ui64 GetGranuleId() const noexcept {
        return GranuleId;
    }

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
                return From->Compare(*item.From) < 0;
            } else if (!From) {
                return true;
            } else {
                return false;
            }
        }
    };

    std::deque<TBatchForMerge> SortBatchesByPK(const bool reverse, TReadMetadata::TConstPtr readMetadata);

    const std::set<ui32>& GetEarlyFilterColumns() const;
    void OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);
    bool OnFilterReady(TBatch& batchInfo);
    TBatch& AddBatch(const TPortionInfo& portionInfo);
    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) const;

};

}
