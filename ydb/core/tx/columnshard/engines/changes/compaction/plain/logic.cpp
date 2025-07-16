#include "logic.h"

namespace NKikimr::NOlap::NCompaction {

void TPlainMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& /*mContext*/) {
    for (auto&& p : input) {
        if (p) {
            Cursors.emplace_back(NCompaction::TPortionColumnCursor(p));
        } else {
            Cursors.emplace_back(
                NCompaction::TPortionColumnCursor(Context.GetLoader()->GetResultField()->type(), Context.GetLoader()->GetDefaultValue()));
        }
        
    }
}

TColumnPortionResult TPlainMerger::DoExecute(
    const TChunkMergeContext& chunkContext, TMergingContext& /*mContext*/) {
    NCompaction::TMergedColumn mColumn(Context);
    std::optional<ui16> predPortionIdx;
    for (ui32 idx = 0; idx < chunkContext.GetRemapper().GetRecordsCount(); ++idx) {
        const ui16 portionIdx = chunkContext.GetRemapper().GetIdxArray().Value(idx);
        const ui32 portionRecordIdx = chunkContext.GetRemapper().GetRecordIdxArray().Value(idx);
        auto& cursor = Cursors[portionIdx];
        cursor.Next(portionRecordIdx, mColumn);
        if (predPortionIdx && portionIdx != *predPortionIdx) {
            Cursors[*predPortionIdx].Fetch(mColumn);
        }
        if (idx + 1 == chunkContext.GetRemapper().GetRecordsCount()) {
            cursor.Fetch(mColumn);
        }
        predPortionIdx = portionIdx;
    }
    return mColumn.BuildResult();
}

}   // namespace NKikimr::NOlap::NCompaction
