#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
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
        db.ReadRemoveHostInProgress(args.RemoveHostInProgress),
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

    if (!args.DirectBlockGroupsConnections.Defined()) {
        return;
    }

    // Apply committed host removals to the persisted vchunk configs: BSC
    // compacted the group on delete (hosts above the removed index shifted
    // down), so configs still carrying the pre-remove host count shift the
    // same way. Clearing LastRemove in the same tx re-enables membership
    // ops.
    TPartitionDatabase db(tx.DB);
    auto& connections = *args.DirectBlockGroupsConnections;
    bool connectionsChanged = false;

    for (size_t dbgId = 0;
         dbgId < connections.DirectBlockGroupConnectionsSize();
         ++dbgId)
    {
        auto* dbgConnections =
            connections.MutableDirectBlockGroupConnections(dbgId);
        if (!dbgConnections->HasLastRemove()) {
            continue;
        }
        const auto removeIndex = static_cast<THostIndex>(
            dbgConnections->GetLastRemove().GetRemoveIndex());
        const auto hostCount =
            static_cast<size_t>(dbgConnections->GetConnections().size());

        for (auto& cfg: args.VChunkConfigs) {
            if (cfg.GetVChunkIndex() % DirectBlockGroupsCount !=
                static_cast<ui32>(dbgId))
            {
                continue;
            }
            if (cfg.GetHostCount() == hostCount) {
                continue;   // already at the post-remove layout
            }
            Y_ABORT_UNLESS(
                cfg.GetHostCount() == hostCount + 1,
                "vchunk %u config host count %lu does not match the "
                "connections (%lu) or the pre-remove layout",
                cfg.GetVChunkIndex(),
                cfg.GetHostCount(),
                hostCount);
            cfg.RemoveHost(removeIndex);
            db.StoreVChunkConfig(cfg);
        }

        dbgConnections->ClearLastRemove();
        connectionsChanged = true;
    }

    if (connectionsChanged) {
        db.StoreDirectBlockGroupsConnections(connections);
    }
}

void TPartitionActor::CompleteLoadState(
    const TActorContext& ctx,
    TTxPartition::TLoadState& args)
{
    if (args.VolumeConfig.Defined()) {
        VolumeConfig = *args.VolumeConfig;

        if (args.DirectBlockGroupsConnections.Defined()) {
            DDiskBlockGroupAllocated = true;
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

            // A remove-host was in flight at the last restart: hold the
            // single in-flight slot and re-send the deletion once the fast
            // path service is ready (an already-applied delete answers
            // NOT_FOUND, see HandleRemoveHostAllocationResult).
            if (args.RemoveHostInProgress.Defined()) {
                const auto& intent = *args.RemoveHostInProgress;
                RemoveHostInFlight = TRemoveHostInFlight{
                    .DirectBlockGroupId = intent.GetDirectBlockGroupId(),
                    .RemoveIndex =
                        static_cast<THostIndex>(intent.GetRemoveIndex()),
                    .DDiskId = intent.GetDDiskId(),
                    .PBufferId = intent.GetPersistentBufferId(),
                };
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
