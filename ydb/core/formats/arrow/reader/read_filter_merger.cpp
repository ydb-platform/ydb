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

std::optional<TSortableBatchPosition::TFoundPosition> TSortableBatchPosition::FindPosition(TSortableBatchPosition& position, const ui64 posStartExt, const ui64 posFinishExt, const TSortableBatchPosition& forFound, const bool greater) {
    ui64 posStart = posStartExt;
    ui64 posFinish = posFinishExt;
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
        Y_ABORT_UNLESS(position.InitPosition(posFinish));
        return TFoundPosition::Greater(posFinish);
    } else {
        Y_ABORT_UNLESS(position.InitPosition(posStart));
        return TFoundPosition::Less(posStart);
    }
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
    return FindPosition(position, posStart, posFinish, forFound, greater);
}

TSortableBatchPosition::TFoundPosition TSortableBatchPosition::SkipToLower(const TSortableBatchPosition& forFound) {
    const ui32 posStart = Position;
    auto pos = FindPosition(*this, posStart, ReverseSort ? 0 : (RecordsCount - 1), forFound, true);
    AFL_VERIFY(pos)("cursor", DebugJson())("found", forFound.DebugJson());
    if (ReverseSort) {
        AFL_VERIFY(Position <= posStart)("pos", Position)("pos_skip", pos->GetPosition())("reverse", true);
    } else {
        AFL_VERIFY(posStart <= Position)("pos", Position)("pos_skip", pos->GetPosition())("reverse", false);
    }
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
