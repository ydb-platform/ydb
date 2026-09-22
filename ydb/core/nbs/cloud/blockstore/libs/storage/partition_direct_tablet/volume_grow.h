#pragma once

#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// In-flight post-create grow: claim extra vchunks, persist, reply OK, restart.
struct TVolumeGrowInFlight
{
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    NActors::TActorId ReplyTo;
    ui64 Cookie = 0;
    ui64 TxId = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
