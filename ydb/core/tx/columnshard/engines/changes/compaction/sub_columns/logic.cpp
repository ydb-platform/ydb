#include "builder.h"
#include "logic.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NCompaction {

const TSubColumnsMerger::TSettings& TSubColumnsMerger::GetSettings() const {
    return Context.GetLoader()->GetAccessorConstructor().GetObjectPtrVerifiedAs<NArrow::NAccessor::NSubColumns::TConstructor>()->GetSettings();
}

void TSubColumnsMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& /*mergeContext*/) {
    for (auto&& i : input) {
        OrderedIterators.emplace_back(NSubColumns::TChunksIterator(i, Context.GetLoader(), RemapKeyIndex, OrderedIterators.size()));
    }
    std::vector<const TDictStats*> columnsStats;
    std::vector<const TDictStats*> othersStats;
    ui32 statRecordsCount = 0;
    for (auto&& i : OrderedIterators) {
        if (i.GetCurrentSubColumnsArray()) {
            columnsStats.emplace_back(&i.GetCurrentSubColumnsArray()->GetColumnsData().GetStats());
            othersStats.emplace_back(&i.GetCurrentSubColumnsArray()->GetOthersData().GetStats());
            statRecordsCount += i.GetCurrentSubColumnsArray()->GetRecordsCount();
        }
    }
    AFL_VERIFY(columnsStats.size());
    AFL_VERIFY(statRecordsCount);
    auto commonStats = TDictStats::Merge(columnsStats, othersStats, GetSettings(), statRecordsCount);
    auto splitted = commonStats.SplitByVolume(GetSettings(), statRecordsCount);
    ResultColumnStats = splitted.ExtractColumns();
    ResultColumnStats->CreateJsonPathAccessorTrieCache();
    //    YDB_LOG_ERROR("",
    //          {"columns", ResultColumnStats->DebugJson()},
    //          {"others", splitted.ExtractOthers().DebugJson()});
    RemapKeyIndex.RegisterColumnStats(*ResultColumnStats);
    for (auto&& i : OrderedIterators) {
        i.Start();
    }
}

TColumnPortionResult TSubColumnsMerger::DoExecute(const TChunkMergeContext& context, TMergingContext& /*mergeContext*/) {
    AFL_VERIFY(ResultColumnStats);
    NSubColumns::TMergedBuilder builder(*ResultColumnStats, context, GetSettings(), RemapKeyIndex);
    NColumnShard::TSubColumnsStat columnStats;
    NColumnShard::TSubColumnsStat otherStats;
    for (ui32 i = 0; i < context.GetRemapper().GetRecordsCount(); ++i) {
        const ui32 sourceIdx = context.GetRemapper().GetIdxArray().Value(i);
        const ui32 recordIdx = context.GetRemapper().GetRecordIdxArray().Value(i);
        const auto startRecord = [&](const ui32 /*sourceRecordIndex*/) {
            builder.StartRecord();
        };
        const auto addKV = [&](const ui32 sourceKeyIndex, const NArrow::NAccessor::NSubColumns::TGeneralIterator& iter, const bool isColumnKey) {
            auto commonKeyInfo = RemapKeyIndex.RemapIndex(sourceIdx, sourceKeyIndex, isColumnKey);
            if (commonKeyInfo.GetIsColumnKey()) {
                columnStats.Add(builder.AddColumnKV(commonKeyInfo.GetCommonKeyIndex(), iter));
            } else {
                const auto bj = iter.GetValueAsBinaryJson();
                otherStats.Add(builder.AddOtherKV(commonKeyInfo.GetCommonKeyIndex(), TStringBuf(bj.data(), bj.size())));
            }
        };
        const auto finishRecord = [&]() {
            builder.FinishRecord();
        };
        OrderedIterators[sourceIdx].ReadRecord(recordIdx, startRecord, addKV, finishRecord);
    }
    context.GetCounters().SubColumnCounters->GetColumnCounters().OnWrite(columnStats);
    context.GetCounters().SubColumnCounters->GetOtherCounters().OnWrite(otherStats);
    return builder.Finish(Context);
}

}   // namespace NKikimr::NOlap::NCompaction
