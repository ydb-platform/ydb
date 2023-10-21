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

std::optional<TSortableBatchPosition::TFoundPosition> TSortableBatchPosition::FindPosition(const std::shared_ptr<arrow::RecordBatch>& batch, const TSortableBatchPosition& forFound, const bool greater, const std::optional<ui32> includedStartPosition) {
    if (!batch || !batch->num_rows()) {
        return {};
    }

    i64 posStart = 0;
    i64 posFinish = batch->num_rows() - 1;
    if (forFound.IsReverseSort()) {
        std::swap(posStart, posFinish);
    }
    if (includedStartPosition) {
        posStart = *includedStartPosition;
    }
    TSortableBatchPosition position = forFound.BuildSame(batch, posStart);
    {
        position.InitPosition(posStart);
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::greater) {
            return TFoundPosition::Greater(posStart);
        } else if (cmp == std::partial_ordering::equivalent) {
            return TFoundPosition::Equal(posStart);
        }
    }
    {
        position.InitPosition(posFinish);
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::less) {
            return TFoundPosition::Less(posFinish);
        } else if (cmp == std::partial_ordering::equivalent) {
            return TFoundPosition::Equal(posFinish);
        }
    }
    while (posFinish > posStart + 1) {
        Y_ABORT_UNLESS(position.InitPosition(0.5 * (posStart + posFinish)));
        const auto comparision = position.Compare(forFound);
        if (comparision == std::partial_ordering::less) {
            posStart = position.Position;
        } else if (comparision == std::partial_ordering::greater) {
            posFinish = position.Position;
        } else {
            return TFoundPosition::Equal(position.Position);
        }
    }
    Y_ABORT_UNLESS(posFinish != posStart);
    if (greater) {
        return TFoundPosition::Greater(posFinish);
    } else {
        return TFoundPosition::Less(posStart);
    }
}

TSortableBatchPosition::TFoundPosition TSortableBatchPosition::SkipToLower(const TSortableBatchPosition& forFound) {
    auto pos = FindPosition(Batch, forFound, true, Position);
    AFL_VERIFY(pos)("batch", NArrow::DebugJson(Batch, 1, 1))("found", forFound.DebugJson());
    if (ReverseSort) {
        AFL_VERIFY(pos->GetPosition() <= Position)("pos", Position)("pos_skip", pos->GetPosition())("reverse", true);
    } else {
        AFL_VERIFY(Position <= pos->GetPosition())("pos", Position)("pos_skip", pos->GetPosition())("reverse", false);
    }
    AFL_VERIFY(InitPosition(pos->GetPosition()));
    return *pos;
}

TSortableScanData::TSortableScanData(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columns) {
    for (auto&& i : columns) {
        auto c = batch->GetColumnByName(i);
        AFL_VERIFY(c)("column_name", i)("columns", JoinSeq(",", columns));
        Columns.emplace_back(c);
        auto f = batch->schema()->GetFieldByName(i);
        Fields.emplace_back(f);
    }
}

}
