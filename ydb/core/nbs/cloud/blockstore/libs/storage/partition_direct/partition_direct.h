#pragma once

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionTablet(const NActors::TActorId& tablet,
                                       NKikimr::TTabletStorageInfo* info);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
