#include "remap.h"

namespace NKikimr::NOlap::NCompaction::NSubColumns {

TRemapColumns::TOthersData::TFinishContext TRemapColumns::BuildRemapInfo(const std::vector<TDictStats::TRTStatsValue>& statsByKeyIndex) const {
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
        builder.Add(i.first, statsByKeyIndex[i.second].GetRecordsCount(), statsByKeyIndex[i.second].GetDataSize());
        remap[i.second] = idx++;
    }
    return TOthersData::TFinishContext(builder.Finish(), remap);
}

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
