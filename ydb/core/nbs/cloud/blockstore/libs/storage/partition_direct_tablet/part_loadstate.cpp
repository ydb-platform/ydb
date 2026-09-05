#include "part_database.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TDirectBlockGroupsConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;
using TDirectBlockGroupConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupConnections;

// Whether the vchunk belongs to this DirectBlockGroup.
bool BelongsToDirectBlockGroup(ui32 vChunkIndex, size_t dbgId)
{
    return GetDirectBlockGroupIndex(vChunkIndex, DirectBlockGroupsCount) ==
           dbgId;
}

// The slots the last run marked Removed.
THostMask FindDeadSlots(const TDirectBlockGroupConnections& connections)
{
    THostMask deadSlots;
    for (size_t slot = 0; slot < connections.ConnectionsSize(); ++slot) {
        if (connections.GetConnections(slot).GetRemoved()) {
            deadSlots.Set(static_cast<THostIndex>(slot));
        }
    }
    return deadSlots;
}

// The config without the dead slots: the live ones keep their order and move
// to the front.
TVChunkConfig MakeCompactedConfig(
    const TVChunkConfig& config,
    THostMask deadSlots)
{
    THostRoles pbufferHosts;
    THostRoles ddiskHosts;
    THostMask enabledHosts;
    TVector<std::optional<ui64>> watermarks;
    for (THostIndex slot = 0; slot < config.GetHostCount(); ++slot) {
        if (deadSlots.Get(slot)) {
            continue;
        }
        if (!config.GetDisabledHosts().Get(slot)) {
            enabledHosts.Set(static_cast<THostIndex>(watermarks.size()));
        }
        pbufferHosts.AppendRole(config.GetPBufferRole(slot));
        ddiskHosts.AppendRole(config.GetDDiskRole(slot));
        watermarks.push_back(config.GetWatermark(slot));
    }
    return TVChunkConfig::Make(
        config.GetVChunkIndex(),
        std::move(pbufferHosts),
        std::move(ddiskHosts),
        enabledHosts,
        std::move(watermarks));
}

// The dirty map matches its entries to hosts by position (see
// TBlocksDirtyMap::Load), so it is compacted by the same pass.
TDirtyMapStateProto MakeCompactedDirtyMapState(
    const TDirtyMapStateProto& state,
    THostMask deadSlots)
{
    TDirtyMapStateProto result;
    result.SetStateGeneration(state.GetStateGeneration());
    for (size_t slot = 0; slot < state.DDiskStatesSize(); ++slot) {
        if (!deadSlots.Get(static_cast<THostIndex>(slot))) {
            *result.AddDDiskStates() = state.GetDDiskStates(slot);
        }
    }
    return result;
}

void EraseDeadSlots(TDirectBlockGroupConnections* connections)
{
    for (size_t slot = connections->ConnectionsSize(); slot-- > 0;) {
        if (connections->GetConnections(slot).GetRemoved()) {
            connections->MutableConnections()->DeleteSubrange(slot, 1);
        }
    }
}

}   // namespace

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
        db.ReadAllDirtyMapStates(args.DirtyMapStates),
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

    // A membership plan is applied exactly as it was validated, so nothing may
    // renumber the slots while one is pending. Compaction waits for a start
    // that has no plan to finish.
    if (args.AddHostInProgress.Defined() || args.RemoveHostInProgress.Defined())
    {
        return;
    }

    // The dead slots leave the persisted state here, in the load tx: the
    // numbering may shift, and no vchunk exists yet to notice.
    TPartitionDatabase db(tx.DB);
    auto& connections = *args.DirectBlockGroupsConnections;
    bool connectionsChanged = false;

    for (size_t dbgId = 0;
         dbgId < connections.DirectBlockGroupConnectionsSize();
         ++dbgId)
    {
        auto* dbgConnections =
            connections.MutableDirectBlockGroupConnections(dbgId);
        const THostMask deadSlots = FindDeadSlots(*dbgConnections);
        if (deadSlots.Count() == 0) {
            continue;
        }

        for (auto& [vChunkIndex, config]: args.VChunkConfigs) {
            if (BelongsToDirectBlockGroup(vChunkIndex, dbgId)) {
                config = MakeCompactedConfig(config, deadSlots);
                db.StoreVChunkConfig(config);
            }
        }
        for (auto& [vChunkIndex, state]: args.DirtyMapStates) {
            if (BelongsToDirectBlockGroup(vChunkIndex, dbgId)) {
                state = MakeCompactedDirtyMapState(state, deadSlots);
                db.StoreDirtyMapState(vChunkIndex, state);
            }
        }

        EraseDeadSlots(dbgConnections);
        dbgConnections->SetConnectionConfigGeneration(
            dbgConnections->GetConnectionConfigGeneration() + 1);
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
                args.VChunkConfigs,
                args.DirtyMapStates);

            // An add-host was in flight at the last restart: hold the single
            // in-flight slot and replay the BSController request once the fast
            // path service is ready (see HandleFastPathServiceReady).
            if (args.AddHostInProgress.Defined()) {
                const auto& intent = *args.AddHostInProgress;
                const ui32 connectionConfigGeneration =
                    DirectBlockGroupsConnections
                        .GetDirectBlockGroupConnections(
                            intent.GetDirectBlockGroupId())
                        .GetConnectionConfigGeneration();

                // Nothing may renumber the slots while a plan is pending, so
                // the plan is applied exactly as it was validated.
                Y_ABORT_UNLESS(
                    intent.GetConnectionConfigGeneration() ==
                        connectionConfigGeneration,
                    "AddHost plan was decided on connection config generation "
                    "%u, the group is at %u",
                    intent.GetConnectionConfigGeneration(),
                    connectionConfigGeneration);

                AddHostInFlight = TAddHostInFlight{
                    .DirectBlockGroupId = intent.GetDirectBlockGroupId(),
                    .NewHostIndex =
                        static_cast<THostIndex>(intent.GetNewHostIndex()),
                    .ConnectionConfigGeneration =
                        intent.GetConnectionConfigGeneration(),
                };
            }

            // A remove-host was in flight at the last restart; re-send the
            // deletion once the fast path service is ready.
            if (args.RemoveHostInProgress.Defined()) {
                const auto& intent = *args.RemoveHostInProgress;
                const auto& dbgConn =
                    DirectBlockGroupsConnections.GetDirectBlockGroupConnections(
                        intent.GetDirectBlockGroupId());
                const auto removeIndex =
                    static_cast<THostIndex>(intent.GetRemoveIndex());

                // Compaction is skipped while a plan is pending, so the slot
                // the plan names is still the slot it named.
                Y_ABORT_UNLESS(
                    intent.GetConnectionConfigGeneration() ==
                        dbgConn.GetConnectionConfigGeneration(),
                    "RemoveHost plan was decided on connection config "
                    "generation %u, the group is at %u",
                    intent.GetConnectionConfigGeneration(),
                    dbgConn.GetConnectionConfigGeneration());
                Y_ABORT_UNLESS(removeIndex < dbgConn.ConnectionsSize());
                Y_ABORT_UNLESS(
                    dbgConn.GetConnections(removeIndex)
                        .GetDDiskId()
                        .SerializeAsString() ==
                    intent.GetDDiskId().SerializeAsString());

                RemoveHostInFlight = TRemoveHostInFlight{
                    .DirectBlockGroupId = intent.GetDirectBlockGroupId(),
                    .RemoveIndex = removeIndex,
                    .DDiskId = intent.GetDDiskId(),
                    .PBufferId = intent.GetPersistentBufferDDiskId(),
                    .ConnectionConfigGeneration =
                        intent.GetConnectionConfigGeneration(),
                };
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
