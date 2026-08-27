#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/testlib/basics/helpers.h>

namespace NKikimr::NTestMoveData {

// A ColumnShard storage info whose every channel follows `history`, given as
// {fromGeneration, groupId} pairs in ascending order of generation.
inline TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo(const ui64 tabletId, const std::vector<std::pair<ui32, ui32>>& history,
    const TBlobStorageGroupType::EErasureSpecies erasure = BootGroupErasure) {
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    info->Channels.resize(5);
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
