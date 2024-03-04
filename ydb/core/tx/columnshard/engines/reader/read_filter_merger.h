#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/formats/arrow/reader/read_filter_merger.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/hash.h>
#include <util/string/join.h>
#include <set>

namespace NKikimr::NOlap::NIndexedReader {

class TRecordBatchBuilder;

template <class TSortCursor>
class TSortingHeap {
public:
    TSortingHeap() = default;

    template <typename TCursors>
    TSortingHeap(TCursors& cursors, bool notNull) {
        Queue.reserve(cursors.size());
        for (auto& cur : cursors) {
            if (!cur.Empty()) {
                Queue.emplace_back(TSortCursor(&cur, notNull));
            }
        }
        std::make_heap(Queue.begin(), Queue.end());
    }

    const TSortCursor& Current() const { return Queue.front(); }
    TSortCursor& MutableCurrent() { return Queue.front(); }
    size_t Size() const { return Queue.size(); }
    bool Empty() const { return Queue.empty(); }
    TSortCursor& NextChild() { return Queue[NextChildIndex()]; }

    void Next() {
        Y_ABORT_UNLESS(Size());

        if (Queue.front().Next()) {
            UpdateTop();
        } else {
            RemoveTop();
        }
    }

    void RemoveTop() {
        std::pop_heap(Queue.begin(), Queue.end());
        Queue.pop_back();
        NextIdx = 0;
    }

    void Push(TSortCursor&& cursor) {
        Queue.emplace_back(cursor);
        std::push_heap(Queue.begin(), Queue.end());
        NextIdx = 0;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_ARRAY;
        for (auto&& i : Queue) {
            result.AppendValue(i.DebugJson());
        }
        return result;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void UpdateTop() {
        size_t size = Queue.size();
        if (size < 2)
            return;

        auto begin = Queue.begin();

        size_t child_idx = NextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (*child_it < *begin)
            return;

        NextIdx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size)
                break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && *child_it < *(child_it + 1)) {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(*child_it < top));
        *curr_it = std::move(top);
    }
private:
    std::vector<TSortCursor> Queue;
    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t NextIdx = 0;

    size_t NextChildIndex() {
        if (NextIdx == 0) {
            NextIdx = 1;
            if (Queue.size() > 2 && Queue[1] < Queue[2]) {
                ++NextIdx;
            }
        }

        return NextIdx;
    }

};

class TMergePartialStream {
private:
#ifndef NDEBUG
    std::optional<TSortableBatchPosition> CurrentKeyColumns;
#endif
    bool PossibleSameVersionFlag = true;

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
            const std::vector<std::string>& keyColumns, const std::vector<std::string>& dataColumns, const bool reverseSort)
            : ControlPointFlag(false)
            , KeyColumns(batch, 0, keyColumns, dataColumns, reverseSort)
            , VersionColumns(batch, 0, TIndexInfo::GetSpecialColumnNames(), {}, false)
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

    TSortingHeap<TBatchIterator> SortHeap;
    std::shared_ptr<arrow::Schema> SortSchema;
    std::shared_ptr<arrow::Schema> DataSchema;
    const bool Reverse;
    ui32 ControlPoints = 0;

    std::optional<TSortableBatchPosition> DrainCurrentPosition();

    void AddNewToHeap(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter);
    void CheckSequenceInDebug(const TSortableBatchPosition& nextKeyColumnsPosition);
public:
    TMergePartialStream(std::shared_ptr<arrow::Schema> sortSchema, std::shared_ptr<arrow::Schema> dataSchema, const bool reverse)
        : SortSchema(sortSchema)
        , DataSchema(dataSchema)
        , Reverse(reverse) {
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
    std::shared_ptr<arrow::RecordBatch> SingleSourceDrain(const TSortableBatchPosition& readTo, const bool includeFinish);
    bool DrainCurrentTo(TRecordBatchBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish);
    std::vector<std::shared_ptr<arrow::RecordBatch>> DrainAllParts(const std::map<TSortableBatchPosition, bool>& positions,
        const std::vector<std::shared_ptr<arrow::Field>>& resultFields);
};

class TRecordBatchBuilder {
private:
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, Fields);
    YDB_READONLY(ui32, RecordsCount, 0);

    bool IsSameFieldsSequence(const std::vector<std::shared_ptr<arrow::Field>>& f1, const std::vector<std::shared_ptr<arrow::Field>>& f2) {
        if (f1.size() != f2.size()) {
            return false;
        }
        for (ui32 i = 0; i < f1.size(); ++i) {
            if (!f1[i]->Equals(f2[i])) {
                return false;
            }
        }
        return true;
    }

public:
    ui32 GetBuildersCount() const {
        return Builders.size();
    }

    TString GetColumnNames() const {
        TStringBuilder result;
        for (auto&& f : Fields) {
            result << f->name() << ",";
        }
        return result;
    }

    TRecordBatchBuilder(const std::vector<std::shared_ptr<arrow::Field>>& fields, const std::optional<ui32> rowsCountExpectation = {}, const THashMap<std::string, ui64>& fieldDataSizePreallocated = {})
        : Fields(fields) {
        Y_ABORT_UNLESS(Fields.size());
        for (auto&& f : fields) {
            Builders.emplace_back(NArrow::MakeBuilder(f));
            auto it = fieldDataSizePreallocated.find(f->name());
            if (it != fieldDataSizePreallocated.end()) {
                NArrow::ReserveData(*Builders.back(), it->second);
            }
            if (rowsCountExpectation) {
                NArrow::TStatusValidator::Validate(Builders.back()->Reserve(*rowsCountExpectation));
            }
        }
    }

    std::shared_ptr<arrow::RecordBatch> Finalize() {
        auto schema = std::make_shared<arrow::Schema>(Fields);
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (auto&& i : Builders) {
            columns.emplace_back(NArrow::TStatusValidator::GetValid(i->Finish()));
        }
        return arrow::RecordBatch::Make(schema, columns.front()->length(), columns);
    }

    void AddRecord(const TSortableBatchPosition& position);
    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& schema);
};

}
