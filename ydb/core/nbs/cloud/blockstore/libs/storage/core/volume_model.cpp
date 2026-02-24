#include "volume_model.h"

// #include "config.h"

// #include <ydb/core/nbs/cloud/storage/core/libs/common/media.h>

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/channel_data_kind.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/size_literals.h>
#include <util/generic/ymath.h>

#include <cmath>

namespace NYdb::NBS::NStorage {

namespace {

void AddOrModifyChannel(const TString& poolKind, const ui32 channelId,
                        const ui64 size, const EChannelDataKind dataKind,
                        NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    while (channelId >= volumeConfig.ExplicitChannelProfilesSize()) {
        volumeConfig.AddExplicitChannelProfiles();
    }
    auto* profile = volumeConfig.MutableExplicitChannelProfiles(channelId);

    profile->SetPoolKind(poolKind);
    profile->SetDataKind(static_cast<ui32>(dataKind));
    profile->SetSize(size);
    profile->SetReadIops(volumeConfig.GetPerformanceProfileMaxReadIops());
    profile->SetReadBandwidth(
        volumeConfig.GetPerformanceProfileMaxReadBandwidth());
    profile->SetWriteIops(volumeConfig.GetPerformanceProfileMaxWriteIops());
    profile->SetWriteBandwidth(
        volumeConfig.GetPerformanceProfileMaxWriteBandwidth());
}

void SetupVolumeChannel(const TString& poolKind, const ui32 channelId,
                        const ui64 size, const EChannelDataKind dataKind,
                        NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    while (channelId >= volumeConfig.VolumeExplicitChannelProfilesSize()) {
        volumeConfig.AddVolumeExplicitChannelProfiles();
    }
    auto* profile =
        volumeConfig.MutableVolumeExplicitChannelProfiles(channelId);

    if (profile->GetPoolKind().empty()) {
        profile->SetPoolKind(poolKind);
    }
    profile->SetDataKind(static_cast<ui32>(dataKind));
    profile->SetSize(size);
    profile->SetReadIops(1);
    profile->SetReadBandwidth(1_MB);
    profile->SetWriteIops(1);
    profile->SetWriteBandwidth(1_MB);
}

struct TPoolKinds
{
    TString System;
    TString Log;
    TString Index;
};

TPoolKinds GetPoolKinds(const NProto::TStorageServiceConfig& config)
{
    return {
        config.GetSsdSystemChannelPoolKind(),
        config.GetSsdLogChannelPoolKind(),
        config.GetSsdIndexChannelPoolKind(),
    };
}

// Partition channels
void SetExplicitChannelProfiles(const NProto::TStorageServiceConfig& config,
                                NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    const auto poolKinds = GetPoolKinds(config);

    AddOrModifyChannel(poolKinds.System, 0, 128_MB, EChannelDataKind::System,
                       volumeConfig);

    AddOrModifyChannel(poolKinds.Log, 1, 1_MB, EChannelDataKind::Log,
                       volumeConfig);
}

// Volume channels
void SetVolumeExplicitChannelProfiles(
    const NProto::TStorageServiceConfig& config,
    NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    const auto poolKinds = GetPoolKinds(config);

    SetupVolumeChannel(poolKinds.System, 0, 1_MB, EChannelDataKind::System,
                       volumeConfig);
    SetupVolumeChannel(poolKinds.Log, 1, 1_MB, EChannelDataKind::Log,
                       volumeConfig);
    // Now schemeshard expects 3 channels for volume:
    // https://github.com/ydb-platform/ydb/blob/main/ydb/core/tx/schemeshard/schemeshard_info_types.h#L2503
    SetupVolumeChannel(poolKinds.Index, 2, 1_MB, EChannelDataKind::Index,
                       volumeConfig);
}

}   // namespace

// ////////////////////////////////////////////////////////////////////////////////

void ResizeVolume(const NProto::TStorageServiceConfig& config,
                  NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    SetExplicitChannelProfiles(config, volumeConfig);

    SetVolumeExplicitChannelProfiles(config, volumeConfig);
}

}   // namespace NYdb::NBS::NStorage
