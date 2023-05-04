#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow_filter.h>
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/hash.h>
#include <set>

namespace NKikimr::NOlap::NIndexedReader {

class TMergePartialStream {
private:
    class TBatchIterator {
    private:
        i64 Position = 0;
        std::optional<ui32> PoolId;
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

        bool HasPoolId() const noexcept {
            return PoolId.has_value();
        }

        ui32 GetPoolIdUnsafe() const noexcept {
            return *PoolId;
        }

        bool CheckNextBatch(const TBatchIterator& nextIterator) {
            Y_VERIFY_DEBUG(nextIterator.Columns.size() == Columns.size());
            return NArrow::ColumnsCompare(Columns, GetLastPosition(), nextIterator.Columns, 0) * ReverseSortKff < 0;
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
                TBatchIterator newIterator(it->second.front(), nullptr, SortSchema, Reverse, SortHeap.back().GetPoolIdUnsafe());
                SortHeap.back().CheckNextBatch(newIterator);
                std::swap(SortHeap.back(), newIterator);
                std::push_heap(SortHeap.begin(), SortHeap.end());
            }
        }
        return SortHeap.size();
    }

    THashMap<ui32, std::deque<std::shared_ptr<arrow::RecordBatch>>> BatchPools;
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
public:
    TMergePartialStream(std::shared_ptr<arrow::Schema> sortSchema, const bool reverse)
        : SortSchema(sortSchema)
        , Reverse(reverse) {
        Y_VERIFY(SortSchema->num_fields());
    }

    bool IsValid() const {
        return SortHeap.size();
    }

    void AddIndependentSource(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter) {
        if (!batch || !batch->num_rows()) {
            return;
        }
        Y_VERIFY_DEBUG(NArrow::IsSorted(batch, SortSchema));
        IndependentBatches.emplace_back(batch);
        if (!filter || filter->IsTotalAllowFilter()) {
            SortHeap.emplace_back(TBatchIterator(batch, nullptr, SortSchema, Reverse, {}));
        } else if (filter->IsTotalDenyFilter()) {
            return;
        } else {
            SortHeap.emplace_back(TBatchIterator(batch, filter, SortSchema, Reverse, {}));
        }
        std::push_heap(SortHeap.begin(), SortHeap.end());
    }

    bool HasRecordsInPool(const ui32 poolId) const {
        auto it = BatchPools.find(poolId);
        if (it == BatchPools.end()) {
            return false;
        }
        return it->second.size();
    }

    void AddPoolSource(const ui32 poolId, std::shared_ptr<arrow::RecordBatch> batch) {
        if (!batch || !batch->num_rows()) {
            return;
        }
        auto it = BatchPools.find(poolId);
        if (it == BatchPools.end()) {
            it = BatchPools.emplace(poolId, std::deque<std::shared_ptr<arrow::RecordBatch>>()).first;
        }
        it->second.emplace_back(batch);
        if (it->second.size() == 1) {
            SortHeap.emplace_back(TBatchIterator(batch, nullptr, SortSchema, Reverse, poolId));
            std::push_heap(SortHeap.begin(), SortHeap.end());
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
