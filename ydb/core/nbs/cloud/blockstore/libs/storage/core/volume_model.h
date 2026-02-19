#pragma once

#include "public.h"

// TODO: move enum definitions to a separate proto file to avoid the inclusion
// of big headers in other headers
// #include
// <ydb/core/nbs/cloud/blockstore/libs/storage/model/channel_data_kind.h>
// #include <ydb/core/nbs/cloud/blockstore/private/api/protos/volume.pb.h>
// #include <ydb/core/nbs/cloud/blockstore/public/api/protos/volume.pb.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <util/generic/vector.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}   // namespace NKikimrBlockStore

namespace NYdb::NBS::NProto {
class TVolumePerformanceProfile;
class TResizeVolumeRequestFlags;
}   // namespace NYdb::NBS::NProto

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// struct TChannelInfo
// {
//     TString PoolKind;
//     EChannelDataKind DataKind = EChannelDataKind::Max;
// };

struct TVolumeParams
{
    ui32 BlockSize = 0;
    ui64 BlocksCountPerPartition = 0;
    ui32 PartitionsCount = 0;
    // TVector<TChannelInfo> DataChannels;
    ui64 MaxReadBandwidth = 0;
    ui64 MaxWriteBandwidth = 0;
    ui32 MaxReadIops = 0;
    ui32 MaxWriteIops = 0;
    NYdb::NBS::NProto::EStorageMediaKind MediaKind =
        NYdb::NBS::NProto::STORAGE_MEDIA_DEFAULT;

    // NPrivateProto::TVolumeChannelsToPoolsKinds VolumeChannelsToPoolsKinds =
    // {};

    ui64 GetBlocksCount() const
    {
        return BlocksCountPerPartition * PartitionsCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionsInfo
{
    ui64 BlocksCountPerPartition = 0;
    ui32 PartitionsCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

ui64 ComputeBlocksCountPerPartition(const ui64 newBlocksCountPerVolume,
                                    const ui32 blocksPerStripe,
                                    const ui32 partitionsCount);

ui64 ComputeBlocksCountPerPartition(
    const ui64 newBlocksCountPerVolume, const ui32 bytesPerStripe,
    const ui32 blockSize, const ui32 partitionsCount);

TPartitionsInfo ComputePartitionsInfo(
    const TStorageConfig& config, const TString& cloudId,
    const TString& folderId, const TString& diskId,
    NYdb::NBS::NProto::EStorageMediaKind mediaKind, ui64 blocksCount,
    ui32 blockSize, bool isSystem, bool isOverlayDisk);

void ResizeVolume(const NProto::TStorageServiceConfig& config,
                  NKikimrBlockStore::TVolumeConfig& volumeConfig);

bool SetMissingParams(const TVolumeParams& volumeParams,
                      const NKikimrBlockStore::TVolumeConfig& prevConfig,
                      NKikimrBlockStore::TVolumeConfig& update);

ui64 ComputeMaxBlocks(const TStorageConfig& config,
                      const NYdb::NBS::NProto::EStorageMediaKind mediaKind,
                      ui32 currentPartitions);

TVolumeParams ComputeVolumeParams(
    const TStorageConfig& config, ui32 blockSize, ui64 blocksCount,
    NYdb::NBS::NProto::EStorageMediaKind mediaKind, ui32 partitionsCount,
    const TString& cloudId, const TString& folderId, const TString& diskId,
    bool isSystem, bool isOverlayDisk);

}   // namespace NYdb::NBS::NStorage
