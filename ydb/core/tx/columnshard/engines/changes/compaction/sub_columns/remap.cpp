#include "remap.h"

namespace NKikimr::NOlap::NCompaction::NSubColumns {

TRemapColumns::TOthersData::TFinishContext TRemapColumns::BuildRemapInfo(const std::vector<TDictStats::TRTStatsValue>& statsByKeyIndex) const {
    TDictStats::TBuilder builder;
    std::vector<ui32> remap;
    remap.resize(statsByKeyIndex.size(), Max<ui32>());
    ui32 idx = 0;
    for (auto&& i : TemporaryKeyIndex) {
        AFL_VERIFY(i.second < statsByKeyIndex.size());
        if (statsByKeyIndex[i.second].GetRecordsCount()) {
            builder.Add(i.first, statsByKeyIndex[i.second].GetRecordsCount(), statsByKeyIndex[i.second].GetDataSize());
            remap[i.second] = idx++;
        }
    }
    return TOthersData::TFinishContext(builder.Finish(), remap);
}

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
