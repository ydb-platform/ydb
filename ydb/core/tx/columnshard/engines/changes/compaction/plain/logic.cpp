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

std::vector<NKikimr::NOlap::NCompaction::TColumnPortionResult> TPlainMerger::DoExecute(
    const TChunkMergeContext& chunkContext, TMergingContext& mContext) {
    NCompaction::TMergedColumn mColumn(Context, chunkContext);
    auto& chunkInfo = mContext.GetChunk(chunkContext.GetBatchIdx());
    std::optional<ui16> predPortionIdx;
    for (ui32 idx = 0; idx < chunkInfo.GetIdxArray().length(); ++idx) {
        const ui16 portionIdx = chunkInfo.GetIdxArray().Value(idx);
        const ui32 portionRecordIdx = chunkInfo.GetRecordIdxArray().Value(idx);
        auto& cursor = Cursors[portionIdx];
        cursor.Next(portionRecordIdx, mColumn);
        if (predPortionIdx && portionIdx != *predPortionIdx) {
            Cursors[*predPortionIdx].Fetch(mColumn);
        }
        if (idx + 1 == chunkInfo.GetIdxArray().length()) {
            cursor.Fetch(mColumn);
        }
        predPortionIdx = portionIdx;
    }
    AFL_VERIFY(chunkInfo.GetIdxArray().length() == mColumn.GetRecordsCount());
    return mColumn.BuildResult();
}

}   // namespace NKikimr::NOlap::NCompaction
