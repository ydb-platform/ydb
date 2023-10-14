#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
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
    TSortableScanData(std::shared_ptr<arrow::RecordBatch> batch, const std::vector<std::string>& columns);

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
protected:

    YDB_READONLY(i64, Position, 0);
    i64 RecordsCount = 0;
    bool ReverseSort = false;
    std::shared_ptr<TSortableScanData> Sorting;
    std::shared_ptr<TSortableScanData> Data;
    std::shared_ptr<arrow::RecordBatch> Batch;
    static std::optional<ui64> FindPosition(std::shared_ptr<arrow::RecordBatch> batch, const TSortableBatchPosition& forFound, const bool needGreater, const bool include);

public:
    TSortableBatchPosition() = default;

    const TSortableScanData& GetData() const {
        return *Data;
    }

    bool IsReverseSort() const {
        return ReverseSort;
    }
    NJson::TJsonValue DebugJson() const;

    TSortableBatchPosition BuildSame(std::shared_ptr<arrow::RecordBatch> batch, const ui32 position) const {
        return TSortableBatchPosition(batch, position, Sorting->GetFieldNames(), Data->GetFieldNames(), ReverseSort);
    }

    bool IsSameSortingSchema(const std::shared_ptr<arrow::Schema>& schema) {
        return Sorting->IsSameSchema(schema);
    }

    static std::shared_ptr<arrow::RecordBatch> SelectInterval(std::shared_ptr<arrow::RecordBatch> batch, const TSortableBatchPosition& from, const TSortableBatchPosition& to, const bool includeFrom, const bool includeTo) {
        if (!batch) {
            return nullptr;
        }
        Y_ABORT_UNLESS(from.Compare(to) != std::partial_ordering::greater);
        const std::optional<ui32> idxFrom = FindPosition(batch, from, true, includeFrom);
        const std::optional<ui32> idxTo = FindPosition(batch, to, false, includeTo);
        if (!idxFrom || !idxTo || *idxTo < *idxFrom) {
            return nullptr;
        }
        return batch->Slice(*idxFrom, *idxTo - *idxFrom + 1);
    }

    TSortableBatchPosition(std::shared_ptr<arrow::RecordBatch> batch, const ui32 position, const std::vector<std::string>& sortingColumns, const std::vector<std::string>& dataColumns, const bool reverseSort)
        : Position(position)
        , RecordsCount(batch->num_rows())
        , ReverseSort(reverseSort)
        , Sorting(std::make_shared<TSortableScanData>(batch, sortingColumns))
        , Batch(batch)
    {
        if (dataColumns.size()) {
            Data = std::make_shared<TSortableScanData>(batch, dataColumns);
        }
        Y_ABORT_UNLESS(batch->num_rows());
        Y_VERIFY_DEBUG(batch->ValidateFull().ok());
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
