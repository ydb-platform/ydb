#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/actors/core/log.h>
#include <util/generic/hash.h>
#include <util/string/join.h>
#include <set>

namespace NKikimr::NOlap::NIndexedReader {

class TRecordBatchBuilder;

class TSortableScanData {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Array>>, Columns);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, Fields);
public:
    TSortableScanData() = default;
    TSortableScanData(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columns);

    std::shared_ptr<arrow::RecordBatch> Slice(const ui64 offset, const ui64 count) const {
        std::vector<std::shared_ptr<arrow::Array>> slicedArrays;
        for (auto&& i : Columns) {
            AFL_VERIFY(offset + count <= (ui64)i->length())("offset", offset)("count", count)("length", i->length());
            slicedArrays.emplace_back(i->Slice(offset, count));
        }
        return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(Fields), count, slicedArrays);
    }

    bool IsSameSchema(const std::shared_ptr<arrow::Schema>& schema) const {
        if (Fields.size() != (size_t)schema->num_fields()) {
            return false;
        }
        for (ui32 i = 0; i < Fields.size(); ++i) {
            if (!Fields[i]->type()->Equals(schema->field(i)->type())) {
                return false;
            }
            if (Fields[i]->name() != schema->field(i)->name()) {
                return false;
            }
        }
        return true;
    }

    NJson::TJsonValue DebugJson(const i32 position) const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& jsonFields = result.InsertValue("fields", NJson::JSON_ARRAY);
        for (auto&& i : Fields) {
            jsonFields.AppendValue(i->ToString());
        }
        for (ui32 i = 0; i < Columns.size(); ++i) {
            auto& jsonColumn = result["sorting_columns"].AppendValue(NJson::JSON_MAP);
            jsonColumn["name"] = Fields[i]->name();
            if (position >= 0 && position < Columns[i]->length()) {
                jsonColumn["value"] = NArrow::DebugString(Columns[i], position);
            }
        }
        return result;
    }

    std::vector<std::string> GetFieldNames() const {
        std::vector<std::string> result;
        for (auto&& i : Fields) {
            result.emplace_back(i->name());
        }
        return result;
    }
};

class TSortableBatchPosition {
private:
    YDB_READONLY(i64, Position, 0);
    i64 RecordsCount = 0;
    bool ReverseSort = false;
    std::shared_ptr<TSortableScanData> Sorting;
    std::shared_ptr<TSortableScanData> Data;
public:
    TSortableBatchPosition() = default;

    std::shared_ptr<arrow::RecordBatch> Slice(const ui64 offset, const ui64 count) const {
        AFL_VERIFY(Data);
        return Data->Slice(offset, count);
    }

    class TFoundPosition {
    private:
        YDB_READONLY(ui32, Position, 0);
        std::optional<bool> GreaterIfNotEqual;
        explicit TFoundPosition(const ui32 pos, const bool greater)
            : Position(pos) {
            GreaterIfNotEqual = greater;
        }
        explicit TFoundPosition(const ui32 pos)
            : Position(pos) {
        }
    public:
        bool IsEqual() const {
            return !GreaterIfNotEqual;
        }
        bool IsLess() const {
            return !!GreaterIfNotEqual && !*GreaterIfNotEqual;
        }
        bool IsGreater() const {
            return !!GreaterIfNotEqual && *GreaterIfNotEqual;
        }

        static TFoundPosition Less(const ui32 pos) {
            return TFoundPosition(pos, false);
        }
        static TFoundPosition Greater(const ui32 pos) {
            return TFoundPosition(pos, true);
        }
        static TFoundPosition Equal(const ui32 pos) {
            return TFoundPosition(pos);
        }
    };

    //  (-inf, it1), [it1, it2), [it2, it3), ..., [itLast, +inf)
    template <class TBordersIterator>
    static std::vector<std::shared_ptr<arrow::RecordBatch>> SplitByBorders(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columnNames, TBordersIterator& it) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> result;
        if (!batch || batch->num_rows() == 0) {
            while (it.IsValid()) {
                result.emplace_back(nullptr);
            }
            result.emplace_back(nullptr);
            return result;
        }
        TSortableBatchPosition pos(batch, 0, columnNames, {}, false);
        bool batchFinished = false;
        i64 recordsCountSplitted = 0;
        for (; it.IsValid() && !batchFinished; it.Next()) {
            const ui32 startPos = pos.GetPosition();
            auto posFound = pos.SkipToLower(it.CurrentPosition());
            if (posFound.IsGreater() || posFound.IsEqual()) {
                if (posFound.GetPosition() == startPos) {
                    result.emplace_back(nullptr);
                } else {
                    result.emplace_back(batch->Slice(startPos, posFound.GetPosition() - startPos));
                    recordsCountSplitted += result.back()->num_rows();
                }
            } else {
                result.emplace_back(batch->Slice(startPos, posFound.GetPosition() - startPos + 1));
                recordsCountSplitted += result.back()->num_rows();
                batchFinished = true;
            }
        }
        if (batchFinished) {
            for (; it.IsValid(); it.Next()) {
                result.emplace_back(nullptr);
            }
            result.emplace_back(nullptr);
        } else {
            AFL_VERIFY(!it.IsValid());
            result.emplace_back(batch->Slice(pos.GetPosition()));
            AFL_VERIFY(result.back()->num_rows());
            recordsCountSplitted += result.back()->num_rows();
        }
        AFL_VERIFY(batch->num_rows() == recordsCountSplitted);
        return result;
    }

    template <class TContainer>
    class TAssociatedContainerIterator {
    private:
        typename TContainer::const_iterator Current;
        typename TContainer::const_iterator End;
    public:
        TAssociatedContainerIterator(const TContainer& container)
            : Current(container.begin())
            , End(container.end()) {
        }

        bool IsValid() const {
            return Current != End;
        }

        void Next() {
            ++Current;
        }

        const auto& CurrentPosition() const {
            return Current->first;
        }
    };

    template <class TContainer>
    static std::vector<std::shared_ptr<arrow::RecordBatch>> SplitByBordersInAssociativeContainer(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columnNames, const TContainer& container) {
        TAssociatedContainerIterator<TContainer> it(container);
        return SplitByBorders(batch, columnNames, it);
    }

    template <class TContainer>
    class TSequentialContainerIterator {
    private:
        typename TContainer::const_iterator Current;
        typename TContainer::const_iterator End;
    public:
        TSequentialContainerIterator(const TContainer& container)
            : Current(container.begin())
            , End(container.end()) {
        }

        bool IsValid() const {
            return Current != End;
        }

        void Next() {
            ++Current;
        }

        const auto& CurrentPosition() const {
            return *Current;
        }
    };

    template <class TContainer>
    static std::vector<std::shared_ptr<arrow::RecordBatch>> SplitByBordersInSequentialContainer(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columnNames, const TContainer& container) {
        TSequentialContainerIterator<TContainer> it(container);
        return SplitByBorders(batch, columnNames, it);
    }

    static std::optional<TFoundPosition> FindPosition(const std::shared_ptr<arrow::RecordBatch>& batch, const TSortableBatchPosition& forFound, const bool needGreater, const std::optional<ui32> includedStartPosition);
    static std::optional<TSortableBatchPosition::TFoundPosition> FindPosition(TSortableBatchPosition& position, const ui64 posStart, const ui64 posFinish, const TSortableBatchPosition& forFound, const bool greater);
    TSortableBatchPosition::TFoundPosition SkipToLower(const TSortableBatchPosition & forFound);

    const TSortableScanData& GetData() const {
        return *Data;
    }

    bool IsReverseSort() const {
        return ReverseSort;
    }
    NJson::TJsonValue DebugJson() const;

    TSortableBatchPosition BuildSame(std::shared_ptr<arrow::RecordBatch> batch, const ui32 position) const {
        std::vector<std::string> dataColumns;
        if (Data) {
            dataColumns = Data->GetFieldNames();
        }
        return TSortableBatchPosition(batch, position, Sorting->GetFieldNames(), dataColumns, ReverseSort);
    }

    bool IsSameSortingSchema(const std::shared_ptr<arrow::Schema>& schema) {
        return Sorting->IsSameSchema(schema);
    }

    TSortableBatchPosition(std::shared_ptr<arrow::RecordBatch> batch, const ui32 position, const std::vector<std::string>& sortingColumns, const std::vector<std::string>& dataColumns, const bool reverseSort)
        : Position(position)
        , ReverseSort(reverseSort)
    {
        Y_ABORT_UNLESS(batch);
        Y_ABORT_UNLESS(batch->num_rows());
        RecordsCount = batch->num_rows();

        if (dataColumns.size()) {
            Data = std::make_shared<TSortableScanData>(batch, dataColumns);
        }
        Sorting = std::make_shared<TSortableScanData>(batch, sortingColumns);
        Y_DEBUG_ABORT_UNLESS(batch->ValidateFull().ok());
        Y_ABORT_UNLESS(Sorting->GetColumns().size());
    }

    std::partial_ordering Compare(const TSortableBatchPosition& item) const {
        Y_ABORT_UNLESS(item.ReverseSort == ReverseSort);
        Y_ABORT_UNLESS(item.Sorting->GetColumns().size() == Sorting->GetColumns().size());
        const auto directResult = NArrow::ColumnsCompare(Sorting->GetColumns(), Position, item.Sorting->GetColumns(), item.Position);
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

    bool operator==(const TSortableBatchPosition& item) const {
        return Compare(item) == std::partial_ordering::equivalent;
    }

    bool operator!=(const TSortableBatchPosition& item) const {
        return Compare(item) != std::partial_ordering::equivalent;
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

}
