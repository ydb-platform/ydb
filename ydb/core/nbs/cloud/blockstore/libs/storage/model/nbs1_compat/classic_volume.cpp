#include "classic_volume.h"

#include <util/generic/yexception.h>

namespace NYdb::NBS::NBlockStore {

NNbs1CompatApi::NBlockStore::NProto::TVolume MakeClassicVolume(
    const NKikimrBlockStore::TVolumeConfig& config)
{
    Y_ABORT_UNLESS(config.PartitionsSize() > 0);

    NNbs1CompatApi::NBlockStore::NProto::TVolume volume;
    volume.SetDiskId(config.GetDiskId());
    volume.SetBlockSize(config.GetBlockSize());
    volume.SetBlocksCount(config.GetPartitions(0).GetBlockCount());
    volume.SetPartitionsCount(config.PartitionsSize());
    // Registration accepts only native SSD; map explicitly to the wire enum.
    volume.SetStorageMediaKind(NNbs1CompatApi::NProto::STORAGE_MEDIA_SSD);
    // volume.SetStorageMediaKind(
    //     static_cast<NNbs1CompatApi::NProto::EStorageMediaKind>(
    //         config.GetStorageMediaKind()));
    volume.SetConfigVersion(config.GetVersion());
    volume.SetProjectId(config.GetProjectId());
    volume.SetFolderId(config.GetFolderId());
    volume.SetCloudId(config.GetCloudId());
    return volume;
}

}   // namespace NYdb::NBS::NBlockStore
