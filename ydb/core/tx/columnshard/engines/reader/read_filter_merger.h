#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/hash.h>
#include <set>

namespace NKikimr::NOlap::NIndexedReader {

class TMergePartialStream {
private:
    class TBatchIterator {
    private:
        YDB_ACCESSOR(i64, Position, 0);
        YDB_OPT(ui32, PoolId);
        std::shared_ptr<arrow::RecordBatch> Batch;

        std::shared_ptr<NArrow::TColumnFilter> Filter;
        std::shared_ptr<NArrow::TColumnFilter::TIterator> FilterIterator;
        std::shared_ptr<NArrow::TColumnFilter::TReverseIterator> ReverseFilterIterator;

        std::vector<std::shared_ptr<arrow::Array>> Columns;
        std::vector<std::shared_ptr<arrow::Array>> VersionColumns;
        int ReverseSortKff;
        i64 RecordsCount;

        i32 GetFirstPosition() const {
            if (ReverseSortKff > 0) {
                return 0;
            } else {
                return RecordsCount - 1;
            }
        }

        i32 GetLastPosition() const {
            if (ReverseSortKff > 0) {
                return RecordsCount - 1;
            } else {
                return 0;
            }
        }

    public:
        TBatchIterator(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter,
            std::shared_ptr<arrow::Schema> sortSchema, const bool reverseSort, const std::optional<ui32> poolId)
            : PoolId(poolId)
            , Batch(batch)
            , Filter(filter)
            , ReverseSortKff(reverseSort ? -1 : 1)
            , RecordsCount(batch->num_rows()) {
            if (Filter) {
                if (reverseSort) {
                    ReverseFilterIterator = std::make_shared<NArrow::TColumnFilter::TReverseIterator>(Filter->GetReverseIterator());
                } else {
                    FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator());
                }
                Y_VERIFY(Filter->Size() == RecordsCount);
            }
            Position = GetFirstPosition();
            Y_UNUSED(Batch);
            Y_VERIFY(batch->num_rows());
            Y_VERIFY_DEBUG(batch->ValidateFull().ok());
            for (auto&& i : sortSchema->fields()) {
                auto c = batch->GetColumnByName(i->name());
                Y_VERIFY(c);
                Columns.emplace_back(c);
            }
            {
                auto c = batch->GetColumnByName(TIndexInfo::SPEC_COL_PLAN_STEP);
                Y_VERIFY(c);
                VersionColumns.emplace_back(c);
            }
            {
                auto c = batch->GetColumnByName(TIndexInfo::SPEC_COL_TX_ID);
                Y_VERIFY(c);
                VersionColumns.emplace_back(c);
            }
        }

        bool CheckNextBatch(const TBatchIterator& nextIterator) {
            Y_VERIFY_DEBUG(nextIterator.Columns.size() == Columns.size());
            return NArrow::ColumnsCompare(Columns, GetLastPosition(), nextIterator.Columns, nextIterator.GetFirstPosition()) * ReverseSortKff < 0;
        }

        class TPosition {
        private:
            const TBatchIterator* Owner;
            ui32 Position = 0;
            bool DeletedFlag = false;
        public:
            bool IsDeleted() const {
                return DeletedFlag;
            }

            void TakeIfMoreActual(const TBatchIterator& anotherIterator) {
                if (NArrow::ColumnsCompare(Owner->VersionColumns, Position, anotherIterator.VersionColumns, anotherIterator.Position) < 0) {
                    Owner = &anotherIterator;
                    Position = anotherIterator.Position;
                    DeletedFlag = Owner->IsDeleted();
                }
            }

            TPosition(const TBatchIterator& owner)
                : Owner(&owner)
                , Position(Owner->Position)
            {
                DeletedFlag = Owner->IsDeleted();
            }

            int CompareNoVersion(const TBatchIterator& item) const {
                Y_VERIFY_DEBUG(item.Columns.size() == Owner->Columns.size());
                return NArrow::ColumnsCompare(Owner->Columns, Position, item.Columns, item.Position);
            }
        };

        int CompareNoVersion(const TBatchIterator& item) const {
            Y_VERIFY_DEBUG(item.Columns.size() == Columns.size());
            return NArrow::ColumnsCompare(Columns, Position, item.Columns, item.Position);
        }

        bool IsDeleted() const {
            if (FilterIterator) {
                return FilterIterator->GetCurrentAcceptance();
            } else if (ReverseFilterIterator) {
                return ReverseFilterIterator->GetCurrentAcceptance();
            } else {
                return false;
            }
        }

        bool Next() {
            bool result = false;
            if (ReverseSortKff > 0) {
                result = ++Position < RecordsCount;
            } else {
                result = --Position >= 0;
            }
            if (FilterIterator) {
                Y_VERIFY(result == FilterIterator->Next(1));
            } else if (ReverseFilterIterator) {
                Y_VERIFY(result == ReverseFilterIterator->Next(1));
            }
            return result;
        }

        bool operator<(const TBatchIterator& item) const {
            const int result = CompareNoVersion(item) * ReverseSortKff;
            if (result == 0) {
                return NArrow::ColumnsCompare(VersionColumns, Position, item.VersionColumns, item.Position) < 0;
            } else {
                return result > 0;
            }
        }
    };

    class TIteratorData {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    public:
        TIteratorData(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter)
            : Batch(batch)
            , Filter(filter)
        {

        }
    };

    bool NextInHeap(const bool needPop) {
        if (SortHeap.empty()) {
            return false;
        }
        if (needPop) {
            std::pop_heap(SortHeap.begin(), SortHeap.end());
        }
        if (SortHeap.back().Next()) {
            std::push_heap(SortHeap.begin(), SortHeap.end());
        } else if (!SortHeap.back().HasPoolId()) {
            SortHeap.pop_back();
        } else {
            auto it = BatchPools.find(SortHeap.back().GetPoolIdUnsafe());
            Y_VERIFY(it->second.size());
            if (it->second.size() == 1) {
                BatchPools.erase(it);
                SortHeap.pop_back();
            } else {
                it->second.pop_front();
                TBatchIterator oldIterator = std::move(SortHeap.back());
                SortHeap.pop_back();
                AddToHeap(SortHeap.back().GetPoolIdUnsafe(), it->second.front().GetBatch(), it->second.front().GetFilter(), false);
                oldIterator.CheckNextBatch(SortHeap.back());
                std::push_heap(SortHeap.begin(), SortHeap.end());
            }
        }
        return SortHeap.size();
    }

    THashMap<ui32, std::deque<TIteratorData>> BatchPools;
    std::vector<std::shared_ptr<arrow::RecordBatch>> IndependentBatches;
    std::vector<TBatchIterator> SortHeap;
    std::shared_ptr<arrow::Schema> SortSchema;
    const bool Reverse;

    TBatchIterator::TPosition DrainCurrentPosition() {
        Y_VERIFY(SortHeap.size());
        auto position = TBatchIterator::TPosition(SortHeap.front());
        bool isFirst = true;
        while (SortHeap.size() && (isFirst || !position.CompareNoVersion(SortHeap.front()))) {
            if (!isFirst) {
                position.TakeIfMoreActual(SortHeap.front());
            }
            NextInHeap(true);
            isFirst = false;
        }
        return position;
    }

    void AddToHeap(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter, const bool restoreHeap) {
        if (!filter || filter->IsTotalAllowFilter()) {
            SortHeap.emplace_back(TBatchIterator(batch, nullptr, SortSchema, Reverse, poolId));
        } else if (filter->IsTotalDenyFilter()) {
            return;
        } else {
            SortHeap.emplace_back(TBatchIterator(batch, filter, SortSchema, Reverse, poolId));
        }
        if (restoreHeap) {
            std::push_heap(SortHeap.begin(), SortHeap.end());
        }
    }
public:
    TMergePartialStream(std::shared_ptr<arrow::Schema> sortSchema, const bool reverse)
        : SortSchema(sortSchema)
        , Reverse(reverse) {
        Y_VERIFY(SortSchema->num_fields());
    }

    bool IsValid() const {
        return SortHeap.size();
    }

    bool HasRecordsInPool(const ui32 poolId) const {
        auto it = BatchPools.find(poolId);
        if (it == BatchPools.end()) {
            return false;
        }
        return it->second.size();
    }

    void AddPoolSource(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter) {
        if (!batch || !batch->num_rows()) {
            return;
        }
        Y_VERIFY_DEBUG(NArrow::IsSorted(batch, SortSchema));
        if (!poolId) {
            IndependentBatches.emplace_back(batch);
            AddToHeap(poolId, batch, filter, true);
        } else {
            auto it = BatchPools.find(*poolId);
            if (it == BatchPools.end()) {
                it = BatchPools.emplace(*poolId, std::deque<TIteratorData>()).first;
            }
            it->second.emplace_back(batch, filter);
            if (it->second.size() == 1) {
                AddToHeap(poolId, batch, filter, true);
            }
        }
    }

    bool DrainCurrent() {
        if (SortHeap.empty()) {
            return false;
        }
        while (SortHeap.size()) {
            auto currentPosition = DrainCurrentPosition();
            if (currentPosition.IsDeleted()) {
                continue;
            }
            return true;
        }
        return false;
    }
};


}
