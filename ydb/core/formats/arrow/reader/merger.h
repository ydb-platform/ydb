#pragma once
#include "position.h"
#include "heap.h"
#include "result_builder.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NMerger {

class TMergePartialStream {
private:
#ifndef NDEBUG
    std::optional<TSortableBatchPosition> CurrentKeyColumns;
#endif
    bool PossibleSameVersionFlag = true;

    std::shared_ptr<arrow::Schema> SortSchema;
    std::shared_ptr<arrow::Schema> DataSchema;
    const bool Reverse;
    const std::vector<std::string> VersionColumnNames;
    ui32 ControlPoints = 0;

    class TBatchIterator {
    private:
        bool ControlPointFlag;
        TSortableBatchPosition KeyColumns;
        TSortableBatchPosition VersionColumns;
        i64 RecordsCount;
        int ReverseSortKff;

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
        NJson::TJsonValue DebugJson() const;

        const std::shared_ptr<NArrow::TColumnFilter>& GetFilter() const {
            return Filter;
        }

        bool IsControlPoint() const {
            return ControlPointFlag;
        }

        const TSortableBatchPosition& GetKeyColumns() const {
            return KeyColumns;
        }

        const TSortableBatchPosition& GetVersionColumns() const {
            return VersionColumns;
        }

        TBatchIterator(const TSortableBatchPosition& keyColumns)
            : ControlPointFlag(true)
            , KeyColumns(keyColumns)
        {

        }

        TBatchIterator(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter,
            const std::vector<std::string>& keyColumns, const std::vector<std::string>& dataColumns, const bool reverseSort, const std::vector<std::string>& versionColumnNames)
            : ControlPointFlag(false)
            , KeyColumns(batch, 0, keyColumns, dataColumns, reverseSort)
            , VersionColumns(batch, 0, versionColumnNames, {}, false)
            , RecordsCount(batch->num_rows())
            , ReverseSortKff(reverseSort ? -1 : 1)
            , Filter(filter)
        {
            Y_ABORT_UNLESS(KeyColumns.InitPosition(GetFirstPosition()));
            Y_ABORT_UNLESS(VersionColumns.InitPosition(GetFirstPosition()));
            if (Filter) {
                FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator(reverseSort, RecordsCount));
            }
        }

        bool CheckNextBatch(const TBatchIterator& nextIterator) {
            return KeyColumns.Compare(nextIterator.KeyColumns) == std::partial_ordering::less;
        }

        bool IsReverse() const {
            return ReverseSortKff < 0;
        }

        bool IsDeleted() const {
            if (!FilterIterator) {
                return false;
            }
            return !FilterIterator->GetCurrentAcceptance();
        }

        TSortableBatchPosition::TFoundPosition SkipToLower(const TSortableBatchPosition& pos) {
            const ui32 posStart = KeyColumns.GetPosition();
            auto result = KeyColumns.SkipToLower(pos);
            const i32 delta = IsReverse() ? (posStart - KeyColumns.GetPosition()) : (KeyColumns.GetPosition() - posStart);
            AFL_VERIFY(delta >= 0);
            AFL_VERIFY(VersionColumns.InitPosition(KeyColumns.GetPosition()));
            if (FilterIterator && delta) {
                AFL_VERIFY(FilterIterator->Next(delta));
            }
            return result;
        }

        bool Next() {
            const bool result = KeyColumns.NextPosition(ReverseSortKff) && VersionColumns.NextPosition(ReverseSortKff);
            if (FilterIterator) {
                Y_ABORT_UNLESS(result == FilterIterator->Next(1));
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

    TSortingHeap<TBatchIterator> SortHeap;

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
#ifndef NDEBUG
        if (CurrentKeyColumns) {
            result["current"] = CurrentKeyColumns->DebugJson();
        }
#endif
        result.InsertValue("heap", SortHeap.DebugJson());
        return result;
    }

    std::optional<TSortableBatchPosition> DrainCurrentPosition();

    void AddNewToHeap(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter);
    void CheckSequenceInDebug(const TSortableBatchPosition& nextKeyColumnsPosition);
public:
    TMergePartialStream(std::shared_ptr<arrow::Schema> sortSchema, std::shared_ptr<arrow::Schema> dataSchema, const bool reverse, const std::vector<std::string>& versionColumnNames)
        : SortSchema(sortSchema)
        , DataSchema(dataSchema)
        , Reverse(reverse)
        , VersionColumnNames(versionColumnNames)
    {
        Y_ABORT_UNLESS(SortSchema);
        Y_ABORT_UNLESS(SortSchema->num_fields());
        Y_ABORT_UNLESS(!DataSchema || DataSchema->num_fields());
    }

    void SkipToLowerBound(const TSortableBatchPosition& pos, const bool include) {
        if (SortHeap.Empty()) {
            return;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("pos", pos.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
        while (!SortHeap.Empty()) {
            const auto cmpResult = SortHeap.Current().GetKeyColumns().Compare(pos);
            if (cmpResult == std::partial_ordering::greater) {
                break;
            }
            if (cmpResult == std::partial_ordering::equivalent && include) {
                break;
            }
            const TSortableBatchPosition::TFoundPosition skipPos = SortHeap.MutableCurrent().SkipToLower(pos);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("pos", pos.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
            if (skipPos.IsEqual()) {
                if (!include && !SortHeap.MutableCurrent().Next()) {
                    SortHeap.RemoveTop();
                } else {
                    SortHeap.UpdateTop();
                }
            } else if (skipPos.IsLess()) {
                SortHeap.RemoveTop();
            } else {
                SortHeap.UpdateTop();
            }
        }
    }

    void SetPossibleSameVersion(const bool value) {
        PossibleSameVersionFlag = value;
    }

    bool IsValid() const {
        return SortHeap.Size();
    }

    ui32 GetSourcesCount() const {
        return SortHeap.Size();
    }

    TString DebugString() const {
        return TStringBuilder() << "sort_heap=" << SortHeap.DebugJson();
    }

    void PutControlPoint(std::shared_ptr<TSortableBatchPosition> point);

    void RemoveControlPoint();

    bool ControlPointEnriched() const {
        return SortHeap.Size() && SortHeap.Current().IsControlPoint();
    }

    void AddSource(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter);

    bool IsEmpty() const {
        return !SortHeap.Size();
    }

    bool DrainAll(TRecordBatchBuilder& builder);
    std::shared_ptr<arrow::RecordBatch> SingleSourceDrain(const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TSortableBatchPosition>* lastResultPosition = nullptr);
    bool DrainCurrentTo(TRecordBatchBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TSortableBatchPosition>* lastResultPosition = nullptr);
    std::vector<std::shared_ptr<arrow::RecordBatch>> DrainAllParts(const std::map<TSortableBatchPosition, bool>& positions,
        const std::vector<std::shared_ptr<arrow::Field>>& resultFields);
};

}
