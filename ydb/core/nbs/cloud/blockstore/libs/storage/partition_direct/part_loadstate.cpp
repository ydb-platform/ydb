#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    std::initializer_list<bool> results = {
        db.ReadVolumeConfig(args.VolumeConfig),
        db.ReadDirectBlockGroupsConnections(args.DirectBlockGroupsConnections),
        db.ReadAllVChunkConfigs(args.VChunkConfigs),
        db.ReadAddHostInProgress(args.AddHostInProgress),
    };

    bool ready = std::accumulate(
        results.begin(),
        results.end(),
        true,
        std::logical_and<>());

    return ready;
}

void TPartitionActor::ExecuteLoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TLoadState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxPartition::TLoadState& args)
{
    if (args.VolumeConfig.Defined()) {
        VolumeConfig = *args.VolumeConfig;

        if (args.DirectBlockGroupsConnections.Defined()) {
            DdiskBlockGroupAllocated = true;
            Start(
                ctx,
                std::move(*args.DirectBlockGroupsConnections),
                std::move(args.VChunkConfigs));

            // An add-host was in flight at the last restart: hold the single
            // in-flight slot and replay the BSController request once the fast
            // path service is ready (see HandleFastPathServiceReady).
            if (args.AddHostInProgress.Defined()) {
                AddHostInFlight = TAddHostInFlight{
                    .DirectBlockGroupId =
                        args.AddHostInProgress->GetDirectBlockGroupId(),
                    .NewHostIndex = static_cast<THostIndex>(
                        args.AddHostInProgress->GetNewHostIndex()),
                };
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
