#include "logic.h"

namespace NKikimr::NOlap::NCompaction {

void TPlainMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) {
    for (auto&& p : input) {
        Cursors.emplace_back(NCompaction::TPortionColumnCursor(p));
    }
}

std::vector<NKikimr::NOlap::NCompaction::TColumnPortionResult> TPlainMerger::DoExecute(
    const NCompaction::TColumnMergeContext& context, const arrow::UInt16Array& pIdxArray, const arrow::UInt32Array& pRecordIdxArray) {
    NCompaction::TMergedColumn mColumn(context);

    std::optional<ui16> predPortionIdx;
    for (ui32 idx = 0; idx < pIdxArray.length(); ++idx) {
        const ui16 portionIdx = pIdxArray.Value(idx);
        const ui32 portionRecordIdx = pRecordIdxArray.Value(idx);
        auto& cursor = Cursors[portionIdx];
        cursor.Next(portionRecordIdx, mColumn);
        if (predPortionIdx && portionIdx != *predPortionIdx) {
            Cursors[*predPortionIdx].Fetch(mColumn);
        }
        if (idx + 1 == pIdxArray.length()) {
            cursor.Fetch(mColumn);
        }
        predPortionIdx = portionIdx;
    }
    AFL_VERIFY(pIdxArray.length() == mColumn.GetRecordsCount());
    return mColumn.BuildResult();
}

}   // namespace NKikimr::NOlap::NCompaction
