#include "builder.h"
#include "logic.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>

namespace NKikimr::NOlap::NCompaction {

const TSubColumnsMerger::TSettings& TSubColumnsMerger::GetSettings() const {
    return Context.GetLoader()->GetAccessorConstructor().GetObjectPtrVerifiedAs<NArrow::NAccessor::NSubColumns::TConstructor>()->GetSettings();
}

void TSubColumnsMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) {
    for (auto&& i : mergeContext.GetChunks()) {
        OutputRecordsCount += i.GetRecordsCount();
    }
    for (auto&& i : input) {
        OrderedIterators.emplace_back(NSubColumns::TChunksIterator(i, Context.GetLoader(), RemapKeyIndex, OrderedIterators.size()));
    }
    std::vector<const TDictStats*> stats;
    for (auto&& i : OrderedIterators) {
        if (i.GetCurrentSubColumnsArray()) {
            stats.emplace_back(&i.GetCurrentSubColumnsArray()->GetColumnsData().GetStats());
            stats.emplace_back(&i.GetCurrentSubColumnsArray()->GetOthersData().GetStats());
        }
    }
    auto commonStats = TDictStats::Merge(stats);
    auto splitted = commonStats.SplitByVolume(GetSettings().GetColumnsLimit());
    ResultColumnStats = splitted.ExtractColumns();
    RemapKeyIndex.RegisterColumnStats(*ResultColumnStats);
    for (auto&& i : OrderedIterators) {
        i.Start();
    }
}

std::vector<TColumnPortionResult> TSubColumnsMerger::DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) {
    AFL_VERIFY(ResultColumnStats);
    auto& mergeChunkContext = mergeContext.GetChunk(context.GetBatchIdx());
    NSubColumns::TMergedBuilder builder(*ResultColumnStats, OutputRecordsCount, context, GetSettings(), RemapKeyIndex);
    for (ui32 i = 0; i < context.GetRecordsCount(); ++i) {
        const ui32 sourceIdx = mergeChunkContext.GetIdxArray().Value(i);
        const ui32 recordIdx = mergeChunkContext.GetRecordIdxArray().Value(i);
        const auto startRecord = [&](const ui32 /*sourceRecordIndex*/) {
            builder.StartRecord();
        };
        const auto addKV = [&](const ui32 sourceKeyIndex, const std::string_view value, const bool isColumnKey) {
            auto commonKeyInfo = RemapKeyIndex.RemapIndex(sourceIdx, sourceKeyIndex, isColumnKey);
            if (commonKeyInfo.GetIsColumnKey()) {
                builder.AddColumnKV(commonKeyInfo.GetCommonKeyIndex(), value);
            } else {
                builder.AddOtherKV(commonKeyInfo.GetCommonKeyIndex(), value);
            }
        };
        const auto finishRecord = [&]() {
            builder.FinishRecord();
        };
        OrderedIterators[sourceIdx].ReadRecord(recordIdx, startRecord, addKV, finishRecord);
    }
    return builder.Finish(Context);
}

}   // namespace NKikimr::NOlap::NCompaction
