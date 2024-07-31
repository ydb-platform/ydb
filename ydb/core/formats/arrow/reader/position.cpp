#include "position.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <util/string/join.h>

namespace NKikimr::NArrow::NMerger {

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

std::optional<TSortableBatchPosition::TFoundPosition> TSortableBatchPosition::FindPosition(TRWSortableBatchPosition& position,
    const ui64 posStartExt, const ui64 posFinishExt, const TSortableBatchPosition& forFound, const bool greater) {
    ui64 posStart = posStartExt;
    ui64 posFinish = posFinishExt;
    auto guard = position.CreateAsymmetricAccessGuard();
    {
        AFL_VERIFY(guard.InitSortingPosition(posStart));
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::greater) {
            return TFoundPosition::Greater(posStart);
        } else if (cmp == std::partial_ordering::equivalent) {
            return TFoundPosition::Equal(posStart);
        }
    }
    {
        AFL_VERIFY(guard.InitSortingPosition(posFinish));
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::less) {
            return TFoundPosition::Less(posFinish);
        } else if (cmp == std::partial_ordering::equivalent) {
            return TFoundPosition::Equal(posFinish);
        }
    }
    while (posFinish > posStart + 1) {
        AFL_VERIFY(guard.InitSortingPosition(0.5 * (posStart + posFinish)));
        const auto comparision = position.Compare(forFound);
        if (comparision == std::partial_ordering::less) {
            posStart = position.Position;
        } else if (comparision == std::partial_ordering::greater) {
            posFinish = position.Position;
        } else {
            return TFoundPosition::Equal(position.Position);
        }
    }
    AFL_VERIFY(posFinish != posStart);
    if (greater) {
        AFL_VERIFY(guard.InitSortingPosition(posFinish));
        return TFoundPosition::Greater(posFinish);
    } else {
        AFL_VERIFY(guard.InitSortingPosition(posStart));
        return TFoundPosition::Less(posStart);
    }
}

std::optional<TSortableBatchPosition::TFoundPosition> TSortableBatchPosition::FindPosition(const std::shared_ptr<arrow::RecordBatch>& batch,
    const TSortableBatchPosition& forFound, const bool greater, const std::optional<ui32> includedStartPosition) {
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

    TRWSortableBatchPosition position = forFound.BuildRWPosition(batch, posStart);
    return FindPosition(position, posStart, posFinish, forFound, greater);
}

NKikimr::NArrow::NMerger::TRWSortableBatchPosition TSortableBatchPosition::BuildRWPosition(const bool needData, const bool deepCopy) const {
    return TRWSortableBatchPosition(Position, RecordsCount, ReverseSort,
        deepCopy ? Sorting->BuildCopy(Position) : Sorting,
        (needData && Data) ? (deepCopy ? Data->BuildCopy(Position) : Data) : nullptr);
}

NKikimr::NArrow::NMerger::TRWSortableBatchPosition TSortableBatchPosition::BuildRWPosition(
    std::shared_ptr<arrow::RecordBatch> batch, const ui32 position) const {
    std::vector<std::string> dataColumns;
    if (Data) {
        dataColumns = Data->GetFieldNames();
    }
    return TRWSortableBatchPosition(batch, position, Sorting->GetFieldNames(), dataColumns, ReverseSort);
}

TSortableBatchPosition::TFoundPosition TRWSortableBatchPosition::SkipToLower(const TSortableBatchPosition& forFound) {
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

TSortableScanData::TSortableScanData(
    const ui64 position, const std::shared_ptr<TGeneralContainer>& batch, const std::vector<std::string>& columns) {
    for (auto&& i : columns) {
        auto c = batch->GetAccessorByNameOptional(i);
        AFL_VERIFY(c)("column_name", i)("columns", JoinSeq(",", columns))("batch", batch->DebugString());
        Columns.emplace_back(c);
        auto f = batch->GetSchema()->GetFieldByName(i);
        AFL_VERIFY(f);
        Fields.emplace_back(f);
    }
    BuildPosition(position);
}

TSortableScanData::TSortableScanData(
    const ui64 position, const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columns) {
    for (auto&& i : columns) {
        auto c = batch->GetColumnByName(i);
        AFL_VERIFY(c)("column_name", i)("columns", JoinSeq(",", columns));
        Columns.emplace_back(std::make_shared<NAccessor::TTrivialArray>(c));
        auto f = batch->schema()->GetFieldByName(i);
        AFL_VERIFY(f);
        Fields.emplace_back(f);
    }
    BuildPosition(position);
}

TSortableScanData::TSortableScanData(const ui64 position, const std::shared_ptr<arrow::Table>& batch, const std::vector<std::string>& columns) {
    for (auto&& i : columns) {
        auto c = batch->GetColumnByName(i);
        AFL_VERIFY(c)("batch_names", JoinSeq(",", batch->schema()->field_names()))("column_name", i)("columns", JoinSeq(",", columns));
        Columns.emplace_back(std::make_shared<NAccessor::TTrivialChunkedArray>(c));
        auto f = batch->schema()->GetFieldByName(i);
        AFL_VERIFY(f);
        Fields.emplace_back(f);
    }
    BuildPosition(position);
}

void TSortableScanData::AppendPositionTo(
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const ui64 position, ui64* recordSize) const {
    AFL_VERIFY(builders.size() == PositionAddress.size());
    for (ui32 i = 0; i < PositionAddress.size(); ++i) {
        AFL_VERIFY(NArrow::Append(*builders[i], *PositionAddress[i].GetArray(), PositionAddress[i].GetAddress().GetLocalIndex(position), recordSize));
    }
}

void TSortableScanData::BuildPosition(const ui64 position) {
    PositionAddress.clear();
    std::optional<ui64> recordsCount;
    FinishPosition = Max<ui64>();
    StartPosition = 0;
    LastInit = position;
    for (auto&& i : Columns) {
        PositionAddress.emplace_back(i->GetChunkSlow(position));
        StartPosition = std::max<ui64>(StartPosition, PositionAddress.back().GetAddress().GetGlobalStartPosition());
        FinishPosition = std::min<ui64>(FinishPosition, PositionAddress.back().GetAddress().GetGlobalFinishPosition());
        if (!recordsCount) {
            recordsCount = i->GetRecordsCount();
        } else {
            AFL_VERIFY(*recordsCount == i->GetRecordsCount());
        }
    }
    AFL_VERIFY(StartPosition < FinishPosition);
    AFL_VERIFY(recordsCount);
    RecordsCount = *recordsCount;
    AFL_VERIFY(position < RecordsCount);
}

bool TSortableScanData::InitPosition(const ui64 position) {
    AFL_VERIFY(position < RecordsCount);
    if (position < FinishPosition && StartPosition <= position) {
        return true;
    }
    LastInit = position;
    ui32 idx = 0;
    FinishPosition = Max<ui64>();
    StartPosition = 0;
    for (auto&& i : PositionAddress) {
        if (!i.GetAddress().Contains(position)) {
            i = Columns[idx]->GetChunk(i.GetAddress(), position);
        }
        StartPosition = std::max<ui64>(StartPosition, i.GetAddress().GetGlobalStartPosition());
        FinishPosition = std::min<ui64>(FinishPosition, i.GetAddress().GetGlobalFinishPosition());
        AFL_VERIFY(i.GetAddress().Contains(position));
        ++idx;
    }
    AFL_VERIFY(StartPosition < FinishPosition);
    return true;
}

std::partial_ordering TCursor::Compare(const TSortableScanData& item, const ui64 itemPosition) const {
    AFL_VERIFY(PositionAddress.size() == item.GetPositionAddress().size());
    for (ui32 idx = 0; idx < PositionAddress.size(); ++idx) {
        auto cmp = PositionAddress[idx].Compare(Position, item.GetPositionAddress()[idx], itemPosition);
        if (std::is_neq(cmp)) {
            return cmp;
        }
    }

    return std::partial_ordering::equivalent;
}

std::partial_ordering TCursor::Compare(const TCursor& item) const {
    AFL_VERIFY(PositionAddress.size() == item.PositionAddress.size());
    for (ui32 idx = 0; idx < PositionAddress.size(); ++idx) {
        auto cmp = PositionAddress[idx].Compare(Position, item.PositionAddress[idx], item.Position);
        if (std::is_neq(cmp)) {
            return cmp;
        }
    }

    return std::partial_ordering::equivalent;
}

void TCursor::AppendPositionTo(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, ui64* recordSize) const {
    AFL_VERIFY(builders.size() == PositionAddress.size());
    for (ui32 i = 0; i < PositionAddress.size(); ++i) {
        AFL_VERIFY_DEBUG(builders[i]->type()->Equals(PositionAddress[i].GetArray()->type()));
        AFL_VERIFY(NArrow::Append(*builders[i], *PositionAddress[i].GetArray(), PositionAddress[i].GetAddress().GetLocalIndex(Position), recordSize));
    }
}

TCursor::TCursor(const std::shared_ptr<arrow::Table>& table, const ui64 position, const std::vector<std::string>& columns)
    : Position(position) {
    PositionAddress = TSortableScanData(position, table, columns).GetPositionAddress();
}

}   // namespace NKikimr::NArrow::NMerger
