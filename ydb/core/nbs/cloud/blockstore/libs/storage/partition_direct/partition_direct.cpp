#include "partition_direct_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

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
    ui64 volumeTabletId)
{
    // Create appropriate actor based on storage type
    return std::make_unique<TPartitionActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(partitionConfig),
        storageAccessMode,
        siblingCount,
        volumeActorId,
        volumeTabletId);
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
