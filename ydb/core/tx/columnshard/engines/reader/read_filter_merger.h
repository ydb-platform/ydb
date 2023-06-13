#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/hash.h>
#include <set>

namespace NKikimr::NOlap::NIndexedReader {

class TSortableBatchPosition {
protected:
    i64 Position = 0;
    i64 RecordsCount = 0;
    bool ReverseSort = false;
    std::vector<std::shared_ptr<arrow::Array>> Columns;
    std::vector<std::shared_ptr<arrow::Field>> Fields;
    std::shared_ptr<arrow::RecordBatch> Batch;
public:
    TSortableBatchPosition() = default;

    bool IsSameSchema(const std::shared_ptr<arrow::Schema> schema) const;

    TSortableBatchPosition(std::shared_ptr<arrow::RecordBatch> batch, const ui32 position, const std::vector<std::string>& columns, const bool reverseSort)
        : Position(position)
        , RecordsCount(batch->num_rows())
        , ReverseSort(reverseSort)
        , Batch(batch)
    {
        Y_VERIFY(batch->num_rows());
        Y_VERIFY_DEBUG(batch->ValidateFull().ok());
        for (auto&& i : columns) {
            auto c = batch->GetColumnByName(i);
            Y_VERIFY(c);
            Columns.emplace_back(c);
            auto f = batch->schema()->GetFieldByName(i);
            Fields.emplace_back(f);
        }
        Y_VERIFY(Columns.size());
    }

    std::partial_ordering Compare(const TSortableBatchPosition& item) const {
        Y_VERIFY(item.ReverseSort == ReverseSort);
        Y_VERIFY_DEBUG(item.Columns.size() == Columns.size());
        const auto directResult = NArrow::ColumnsCompare(Columns, Position, item.Columns, item.Position);
        if (ReverseSort) {
            if (directResult == std::partial_ordering::less) {
                return std::partial_ordering::greater;
            } else if (directResult == std::partial_ordering::greater) {
                return std::partial_ordering::less;
            } else {
                return std::partial_ordering::equivalent;
            }
        } else {
            return directResult;
        }
    }

    bool operator<(const TSortableBatchPosition& item) const {
        return Compare(item) == std::partial_ordering::less;
    }

    bool NextPosition(const i64 delta) {
        return InitPosition(Position + delta);
    }

    bool InitPosition(const i64 position) {
        if (position < RecordsCount && position >= 0) {
            Position = position;
            return true;
        } else {
            return false;
        }
        
    }

};

class TMergePartialStream {
private:
    class TBatchIterator {
    private:
        bool ControlPointFlag;
        TSortableBatchPosition KeyColumns;
        TSortableBatchPosition VersionColumns;
        i64 RecordsCount;
        int ReverseSortKff;
        YDB_OPT(ui32, PoolId);

        std::shared_ptr<NArrow::TColumnFilter> Filter;
        std::shared_ptr<NArrow::TColumnFilter::TIterator> FilterIterator;

        i32 GetFirstPosition() const {
            if (ReverseSortKff > 0) {
                return 0;
            } else {
                return RecordsCount - 1;
            }
        }

    public:
        bool IsControlPoint() const {
            return ControlPointFlag;
        }

        const TSortableBatchPosition& GetKeyColumns() const {
            return KeyColumns;
        }

        TBatchIterator(const TSortableBatchPosition& keyColumns)
            : ControlPointFlag(true)
            , KeyColumns(keyColumns)
        {

        }

        TBatchIterator(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter,
            const std::vector<std::string>& keyColumns, const bool reverseSort, const std::optional<ui32> poolId)
            : ControlPointFlag(false)
            , KeyColumns(batch, 0, keyColumns, reverseSort)
            , VersionColumns(batch, 0, TIndexInfo::GetSpecialColumnNames(), false)
            , RecordsCount(batch->num_rows())
            , ReverseSortKff(reverseSort ? -1 : 1)
            , PoolId(poolId)
            , Filter(filter)
        {
            Y_VERIFY(KeyColumns.InitPosition(GetFirstPosition()));
            Y_VERIFY(VersionColumns.InitPosition(GetFirstPosition()));
            if (Filter) {
                FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator(reverseSort));
                Y_VERIFY(Filter->Size() == RecordsCount);
            }
        }

        bool CheckNextBatch(const TBatchIterator& nextIterator) {
            return KeyColumns.Compare(nextIterator.KeyColumns) == std::partial_ordering::less;
        }

        class TPosition {
        private:
            TSortableBatchPosition KeyColumns;
            TSortableBatchPosition VersionColumns;
            bool DeletedFlag;
            bool ControlPointFlag;
        public:
            const TSortableBatchPosition& GetKeyColumns() const {
                return KeyColumns;
            }

            bool IsControlPoint() const {
                return ControlPointFlag;
            }

            bool IsDeleted() const {
                return DeletedFlag;
            }

            void TakeIfMoreActual(const TBatchIterator& anotherIterator) {
                Y_VERIFY_DEBUG(KeyColumns.Compare(anotherIterator.KeyColumns) == std::partial_ordering::equivalent);
                if (VersionColumns.Compare(anotherIterator.VersionColumns) == std::partial_ordering::less) {
                    DeletedFlag = anotherIterator.IsDeleted();
                    ControlPointFlag = anotherIterator.IsControlPoint();
                }
            }

            TPosition(const TBatchIterator& owner)
                : KeyColumns(owner.KeyColumns)
                , VersionColumns(owner.VersionColumns)
                , DeletedFlag(owner.IsDeleted())
                , ControlPointFlag(owner.IsControlPoint())
            {
            }
        };

        bool IsDeleted() const {
            if (!FilterIterator) {
                return false;
            }
            return FilterIterator->GetCurrentAcceptance();
        }

        bool Next() {
            const bool result = KeyColumns.NextPosition(ReverseSortKff) && VersionColumns.NextPosition(ReverseSortKff);
            if (FilterIterator) {
                Y_VERIFY(result == FilterIterator->Next(1));
            }
            return result;
        }

        bool operator<(const TBatchIterator& item) const {
            const std::partial_ordering result = KeyColumns.Compare(item.KeyColumns);
            if (result == std::partial_ordering::equivalent) {
                if (IsControlPoint() && item.IsControlPoint()) {
                    return false;
                } else if (IsControlPoint()) {
                    return false;
                } else if (item.IsControlPoint()) {
                    return true;
                }
                //don't need inverse through we need maximal version at first (reverse analytic not included in VersionColumns)
                return VersionColumns.Compare(item.VersionColumns) == std::partial_ordering::less;
            } else {
                //inverse logic through we use max heap, but need minimal element if not reverse (reverse analytic included in KeyColumns)
                return result == std::partial_ordering::greater;
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
                AddNewToHeap(SortHeap.back().GetPoolIdUnsafe(), it->second.front().GetBatch(), it->second.front().GetFilter(), false);
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
    ui32 ControlPoints = 0;

    TBatchIterator::TPosition DrainCurrentPosition() {
        Y_VERIFY(SortHeap.size());
        auto position = TBatchIterator::TPosition(SortHeap.front());
        if (SortHeap.front().IsControlPoint()) {
            return position;
        }
        bool isFirst = true;
        while (SortHeap.size() && (isFirst || position.GetKeyColumns().Compare(SortHeap.front().GetKeyColumns()) == std::partial_ordering::equivalent)) {
            if (!isFirst) {
                position.TakeIfMoreActual(SortHeap.front());
            }
            Y_VERIFY(!SortHeap.front().IsControlPoint());
            NextInHeap(true);
            isFirst = false;
        }
        return position;
    }

    void AddNewToHeap(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter, const bool restoreHeap);
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

    void PutControlPoint(std::shared_ptr<TSortableBatchPosition> point);

    void RemoveControlPoint();

    bool ControlPointEnriched() const {
        return SortHeap.size() && SortHeap.front().IsControlPoint();
    }

    void AddPoolSource(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter);

    bool IsEmpty() const {
        return SortHeap.empty();
    }

    bool DrainCurrent() {
        if (SortHeap.empty()) {
            return false;
        }
        while (SortHeap.size()) {
            auto currentPosition = DrainCurrentPosition();
            if (currentPosition.IsControlPoint()) {
                return false;
            }
            if (currentPosition.IsDeleted()) {
                continue;
            }
            return true;
        }
        return false;
    }
};


}
