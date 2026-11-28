#include "logic.h"

namespace NKikimr::NOlap::NCompaction {

void TPlainMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mContext) {
    ui32 idx = 0;
    for (auto&& p : input) {
        if (p) {
            Cursors.emplace_back(NCompaction::TPortionColumnCursor(p));
        } else {
            std::shared_ptr<arrow::Scalar> fill = Context.GetDefaultValue();
            const auto& portionSchemas = mContext.GetPortionSchemas();
            if (idx < portionSchemas.size() && portionSchemas[idx] && Context.GetSchemaInfo()) {
                fill = ISnapshotSchema::GetAbsentColumnFill(*portionSchemas[idx], *Context.GetSchemaInfo(), Context.GetColumnId());
            }
            Cursors.emplace_back(NCompaction::TPortionColumnCursor(Context.GetResultField()->type(), fill));
        }
        ++idx;
    }
}

TColumnPortionResult TPlainMerger::DoExecute(const TChunkMergeContext& chunkContext, TMergingContext& /*mContext*/) {
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
