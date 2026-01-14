#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreatePartitionTablet(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId);

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
