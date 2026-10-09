#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/volume.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

namespace NYdb::NBS::NBlockStore {

// Builds the classic API volume description from the stored config.
// Clients stays empty; the config must have at least one partition.
NNbs1CompatApi::NBlockStore::NProto::TVolume MakeClassicVolume(
    const NKikimrBlockStore::TVolumeConfig& config);

}   // namespace NYdb::NBS::NBlockStore
