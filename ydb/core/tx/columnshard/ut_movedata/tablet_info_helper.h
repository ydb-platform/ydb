#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/testlib/basics/helpers.h>

namespace NKikimr::NTestMoveData {

// Every channel follows `history`: {fromGeneration, groupId} pairs, ascending by generation.
inline TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo(const ui64 tabletId, const std::vector<std::pair<ui32, ui32>>& history,
    const TBlobStorageGroupType::EErasureSpecies erasure = BootGroupErasure) {
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    // 0 log, 1 local DB, 2..4 data: several data channels so StartBlobBatch's round-robin spreads.
    constexpr ui32 channelCount = 5;
    info->Channels.resize(channelCount);
    for (ui64 channel = 0; channel < info->Channels.size(); ++channel) {
        info->Channels[channel].Channel = channel;
        info->Channels[channel].Type = TBlobStorageGroupType(erasure);
        for (const auto& [fromGeneration, groupId] : history) {
            info->Channels[channel].History.emplace_back(fromGeneration, groupId);
        }
    }
    return info;
}

}   // namespace NKikimr::NTestMoveData
