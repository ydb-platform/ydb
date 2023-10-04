#include "read_filter_merger.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NIndexedReader {

NJson::TJsonValue TSortableBatchPosition::DebugJson() const {
    NJson::TJsonValue result;
    result["reverse"] = ReverseSort;
    result["records_count"] = RecordsCount;
    result["position"] = Position;
    result["sorting"] = Sorting->DebugJson(Position);
    if (Data) {
        result["data"] = Data->DebugJson(Position);
    }
    return result;
}

std::optional<ui64> TSortableBatchPosition::FindPosition(std::shared_ptr<arrow::RecordBatch> batch, const TSortableBatchPosition& forFound, const bool greater, const bool include) {
    if (!batch || !batch->num_rows()) {
        return {};
    }

    const auto checkEqualBorder = [batch, greater, include](const i64 position) ->std::optional<i64> {
        if (include) {
            return position;
        } else if (greater) {
            if (batch->num_rows() > position + 1) {
                return position + 1;
            } else {
                return {};
            }
        } else {
            if (position) {
                return position - 1;
            } else {
                return {};
            }
        }
    };

    i64 posStart = 0;
    i64 posFinish = batch->num_rows() - 1;
    TSortableBatchPosition position = forFound.BuildSame(batch, posStart);
    {
        position.InitPosition(posStart);
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::greater) {
            if (greater) {
                return posStart;
            } else {
                return {};
            }
        } else if (cmp == std::partial_ordering::equivalent) {
            return checkEqualBorder(posStart);
        }
    }
    {
        position.InitPosition(posFinish);
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::less) {
            if (greater) {
                return {};
            } else {
                return posFinish;
            }
        } else if (cmp == std::partial_ordering::equivalent) {
            return checkEqualBorder(posFinish);
        }
    }
    while (posFinish > posStart + 1) {
        Y_VERIFY(position.InitPosition(0.5 * (posStart + posFinish)));
        const auto comparision = position.Compare(forFound);
        if (comparision == std::partial_ordering::less) {
            posStart = position.Position;
        } else if (comparision == std::partial_ordering::greater) {
            posFinish = position.Position;
        } else {
            return checkEqualBorder(position.Position);
        }
    }
    Y_VERIFY(posFinish != posStart);
    if (greater) {
        return posFinish;
    } else {
        return posStart;
    }
}

TSortableScanData::TSortableScanData(std::shared_ptr<arrow::RecordBatch> batch, const std::vector<std::string>& columns) {
    for (auto&& i : columns) {
        auto c = batch->GetColumnByName(i);
        AFL_VERIFY(c)("column_name", i)("columns", JoinSeq(",", columns));
        Columns.emplace_back(c);
        auto f = batch->schema()->GetFieldByName(i);
        Fields.emplace_back(f);
    }
}

}
