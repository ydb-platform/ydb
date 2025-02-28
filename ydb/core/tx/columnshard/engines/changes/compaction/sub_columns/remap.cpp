#include "remap.h"

namespace NKikimr::NOlap::NCompaction::NSubColumns {

TRemapColumns::TOthersData::TFinishContext TRemapColumns::BuildRemapInfo(
    const std::vector<TDictStats::TRTStatsValue>& statsByKeyIndex, const TSettings& settings, const ui32 recordsCount) const {
    TDictStats::TBuilder builder;
    std::vector<ui32> remap;
    remap.resize(statsByKeyIndex.size(), Max<ui32>());
    ui32 idx = 0;
    for (auto&& i : TemporaryKeyIndex) {
        if (i.second >= statsByKeyIndex.size()) {
            continue;
        }
        if (!statsByKeyIndex[i.second].GetRecordsCount()) {
            continue;
        }
        builder.Add(i.first, statsByKeyIndex[i.second].GetRecordsCount(), statsByKeyIndex[i.second].GetDataSize(),
            settings.IsSparsed(statsByKeyIndex[i.second].GetRecordsCount(), recordsCount) ? NArrow::NAccessor::IChunkedArray::EType::SparsedArray
                                                                                          : NArrow::NAccessor::IChunkedArray::EType::Array);
        remap[i.second] = idx++;
    }
    return TOthersData::TFinishContext(builder.Finish(), remap);
}

void TRemapColumns::StartSourceChunk(const ui32 sourceIdx, const TDictStats& sourceColumnStats, const TDictStats& sourceOtherStats) {
    if (RemapInfo.size() <= sourceIdx) {
        RemapInfo.resize((sourceIdx + 1) * 2);
    }
    RemapInfo[sourceIdx].clear();
    auto& remapSourceInfo = RemapInfo[sourceIdx];
    remapSourceInfo.resize(2);
    auto& remapSourceInfoColumns = remapSourceInfo[1];
    AFL_VERIFY(ResultColumnStats);
    for (ui32 i = 0; i < sourceColumnStats.GetColumnsCount(); ++i) {
        if (remapSourceInfoColumns.size() <= i) {
            remapSourceInfoColumns.resize((i + 1) * 2);
        }
        AFL_VERIFY(!remapSourceInfoColumns[i]);
        if (auto commonKeyIndex = ResultColumnStats->GetKeyIndexOptional(sourceColumnStats.GetColumnName(i))) {
            remapSourceInfoColumns[i] = TRemapInfo(*commonKeyIndex, true);
        } else {
            commonKeyIndex = RegisterNewOtherIndex(sourceColumnStats.GetColumnName(i));
            remapSourceInfoColumns[i] = TRemapInfo(*commonKeyIndex, false);
        }
    }
    auto& remapSourceInfoOthers = remapSourceInfo[0];
    for (ui32 i = 0; i < sourceOtherStats.GetColumnsCount(); ++i) {
        if (remapSourceInfoOthers.size() <= i) {
            remapSourceInfoOthers.resize((i + 1) * 2);
        }
        AFL_VERIFY(!remapSourceInfoOthers[i]);
        if (auto commonKeyIndex = ResultColumnStats->GetKeyIndexOptional(sourceOtherStats.GetColumnName(i))) {
            remapSourceInfoOthers[i] = TRemapInfo(*commonKeyIndex, true);
        } else {
            commonKeyIndex = RegisterNewOtherIndex(sourceOtherStats.GetColumnName(i));
            remapSourceInfoOthers[i] = TRemapInfo(*commonKeyIndex, false);
        }
    }
}

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
