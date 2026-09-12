#include "part_database.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_events_private.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tabletid.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TDirectBlockGroupsConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

////////////////////////////////////////////////////////////////////////////////

namespace {

using TDirectBlockGroupConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupConnections;

// The index of the entry that holds this DDisk.
[[nodiscard]] THostIndex FindConnectionIndex(
    const TDirectBlockGroupConnections& connections,
    const NKikimrBlobStorage::NDDisk::TDDiskId& ddiskId)
{
    const TString ddiskIdBytes = ddiskId.SerializeAsString();
    for (size_t index = 0; index < connections.ConnectionsSize(); ++index) {
        const auto& connection = connections.GetConnections(index);
        if (connection.GetRemovedFromBSC()) {
            continue;
        }
        if (connection.GetDDiskId().SerializeAsString() == ddiskIdBytes) {
            return static_cast<THostIndex>(index);
        }
    }
    Y_ABORT(
        "RemoveHost: the DDisk %s is not in the group",
        ddiskId.ShortDebugString().c_str());
}

// Marks the entry that holds this DDisk removed and moves the group to the
// next DBG connections config generation. Returns the index of the entry.
[[nodiscard]] THostIndex MarkSlotRemoved(
    TDirectBlockGroupsConnections* connections,
    size_t dbgId,
    const NKikimrBlobStorage::NDDisk::TDDiskId& ddiskId,
    const NKikimrBlobStorage::NDDisk::TDDiskId& pbufferId)
{
    auto* dbgConnections =
        connections->MutableDirectBlockGroupConnections(dbgId);
    const THostIndex removeIndex =
        FindConnectionIndex(*dbgConnections, ddiskId);
    auto* removed = dbgConnections->MutableConnections(removeIndex);
    Y_ABORT_UNLESS(
        removed->GetPersistentBufferDDiskId().SerializeAsString() ==
            pbufferId.SerializeAsString(),
        "RemoveHost: the persistent buffer at removeIndex does not match the "
        "intent (dbgId=%lu)",
        dbgId);

    removed->SetRemovedFromBSC(true);
    dbgConnections->SetDBGConnectionsConfigGeneration(
        dbgConnections->GetDBGConnectionsConfigGeneration() + 1);
    return removeIndex;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareStartRemoveHost(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStartRemoveHost& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteStartRemoveHost(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStartRemoveHost& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreRemoveHostInProgress(args.Intent);
}

void TPartitionActor::CompleteStartRemoveHost(
    const TActorContext& ctx,
    TTxPartition::TStartRemoveHost& args)
{
    Y_UNUSED(args);

    SendRemoveHostRequest(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCommitRemoveHost(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCommitRemoveHost& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteCommitRemoveHost(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCommitRemoveHost& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    db.StoreDirectBlockGroupsConnections(args.DirectBlockGroupsConnections);
    db.ClearRemoveHostInProgress();
}

void TPartitionActor::CompleteCommitRemoveHost(
    const TActorContext& ctx,
    TTxPartition::TCommitRemoveHost& args)
{
    const size_t dbgId = args.DirectBlockGroupId;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s RemoveHost persisted dbgId=%lu removeIndex=%s, the slot is "
        "dead until the next tablet start compacts it away",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        PrintHostIndex(args.RemoveIndex).c_str());

    auto dbgPtr = FastPathService->GetDirectBlockGroup(dbgId);
    Y_ABORT_UNLESS(dbgPtr);
    auto executor = dbgPtr->GetExecutor();
    const ui32 dbgConnectionsConfigGeneration =
        args.DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetDBGConnectionsConfigGeneration();
    executor->ExecuteSimple(
        [dbgPtr,
         removeIndex = args.RemoveIndex,
         dbgConnectionsConfigGeneration]()
        {
            dbgPtr->OnRemoveHostSucceeded(
                removeIndex,
                dbgConnectionsConfigGeneration);
        });

    RemoveHostInFlight.reset();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::SendRemoveHostRequest(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(RemoveHostInFlight.has_value());

    const size_t dbgId = RemoveHostInFlight->DirectBlockGroupId;

    const auto pipe = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
    RemoveHostInFlight->BSPipeClient = pipe;

    auto request = MakeAllocateDDiskBlockGroupRequest();

    // The deletion commits atomically in BSC and is idempotent: a re-sent
    // request whose ids are already deleted answers NOT_FOUND.
    auto* op = request->Record.AddDirectBlockGroupOperations();
    op->SetDirectBlockGroupId(dbgId);
    op->AddDeleteDDisks()->MutableDDiskId()->CopyFrom(
        RemoveHostInFlight->DDiskId);
    op->AddDeletePersistentBuffers()->MutablePersistentBufferId()->CopyFrom(
        RemoveHostInFlight->PBufferId);

    NTabletPipe::SendData(ctx, pipe, request.release(), dbgId);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleRemoveHostAllocationResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const size_t dbgId = ev->Cookie;

    Y_ABORT_UNLESS(RemoveHostInFlight.has_value());
    if (RemoveHostInFlight->DirectBlockGroupId != dbgId) {
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s RemoveHost response for unexpected dbgId=%lu (stale)",
            LogTitle.GetWithTime().c_str(),
            dbgId);
        return;
    }

    NTabletPipe::CloseClient(ctx, RemoveHostInFlight->BSPipeClient);

    auto updated = DirectBlockGroupsConnections;
    const THostIndex removeIndex = MarkSlotRemoved(
        &updated,
        dbgId,
        RemoveHostInFlight->DDiskId,
        RemoveHostInFlight->PBufferId);
    const auto& updatedDbg = updated.GetDirectBlockGroupConnections(dbgId);

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::NOT_FOUND) {
        // The deletion already applied (a crash between the BSC mutation and
        // the local commit); there is no listing to check.
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s RemoveHost already applied (dbgId=%lu), persisting: %s",
            LogTitle.GetWithTime().c_str(),
            dbgId,
            msg->Record.GetErrorReason().c_str());
    } else {
        // A malformed or mismatching response keeps the intent, so the
        // remove is retried on the next recovery.
        const auto response =
            ValidateAllocationResponse(*msg, dbgId, LiveHostCount(updatedDbg));
        if (HasError(response.Error)) {
            LOG_WARN(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s RemoveHost (dbgId=%lu) not completed, kept for retry on "
                "recovery: %s",
                LogTitle.GetWithTime().c_str(),
                dbgId,
                FormatError(response.Error).c_str());
            return;
        }
    }

    DirectBlockGroupsConnections = updated;
    ExecuteTx(
        ctx,
        CreateTx<TCommitRemoveHost>(std::move(updated), dbgId, removeIndex));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleRemoveHostFromDBG(
    const TEvPartitionDirectPrivate::TEvRemoveHostFromDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto dbgId = msg->DirectBlockGroupId;
    const auto hostIndex = msg->HostIndex;
    const ui32 dbgConnectionsConfigGeneration =
        msg->DBGConnectionsConfigGeneration;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle RemoveHostFromDBG dbgId=%lu hostIndex=%s DBG connections "
        "config generation=%u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        PrintHostIndex(static_cast<THostIndex>(hostIndex)).c_str(),
        dbgConnectionsConfigGeneration);

    Y_ABORT_UNLESS(FastPathService);

    if (!ValidateRemoveHostFromDBGRequest(
            ctx,
            dbgId,
            hostIndex,
            dbgConnectionsConfigGeneration))
    {
        return;
    }

    const auto& connection =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnections(hostIndex);

    TTxPartition::TRemoveHostInProgress intent;
    intent.SetDirectBlockGroupId(static_cast<ui32>(dbgId));
    intent.MutableDDiskId()->CopyFrom(connection.GetDDiskId());
    intent.MutablePersistentBufferDDiskId()->CopyFrom(
        connection.GetPersistentBufferDDiskId());
    intent.SetDBGConnectionsConfigGeneration(dbgConnectionsConfigGeneration);

    // The intent is persisted before the BSC request: a crash in between
    // leaves it durable, and the replay finishes the removal.
    RemoveHostInFlight = TRemoveHostInFlight{
        .DirectBlockGroupId = dbgId,
        .DDiskId = intent.GetDDiskId(),
        .PBufferId = intent.GetPersistentBufferDDiskId(),
        .DBGConnectionsConfigGeneration = dbgConnectionsConfigGeneration,
    };

    ExecuteTx(ctx, CreateTx<TStartRemoveHost>(std::move(intent)));
}

bool TPartitionActor::ValidateRemoveHostFromDBGRequest(
    const TActorContext& ctx,
    size_t dbgId,
    size_t hostIndex,
    ui32 dbgConnectionsConfigGeneration)
{
    // dbgId comes from the DBG itself, so out-of-range is a bug.
    const auto dbgCount = static_cast<size_t>(
        DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize());
    Y_ABORT_UNLESS(
        dbgId < dbgCount,
        "RemoveHost for out-of-range dbgId=%lu (have %lu DBGs)",
        dbgId,
        dbgCount);

    if (AddHostInFlight.has_value()) {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            "An AddHost is already in progress");
        return false;
    }
    if (RemoveHostInFlight.has_value()) {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            "Another RemoveHost is already in progress");
        return false;
    }

    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);

    const auto currentSize =
        static_cast<size_t>(dbgConn.GetConnections().size());

    if (hostIndex >= currentSize) {
        // Raw index: the value may exceed the THostIndex range.
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            TStringBuilder()
                << "host index " << hostIndex << " is out of range (have "
                << currentSize << ")");
        return false;
    }
    if (dbgConn.GetConnections(hostIndex).GetRemovedFromBSC()) {
        RejectRemoveHost(ctx, dbgId, hostIndex, "The slot is already removed");
        return false;
    }
    if (dbgConnectionsConfigGeneration !=
        dbgConn.GetDBGConnectionsConfigGeneration())
    {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            TStringBuilder()
                << "RemoveHost was decided on DBG connections config "
                   "generation "
                << dbgConnectionsConfigGeneration << ", the group is at "
                << dbgConn.GetDBGConnectionsConfigGeneration());
        return false;
    }

    return true;
}

void TPartitionActor::RejectRemoveHost(
    const NActors::TActorContext& ctx,
    size_t dbgId,
    size_t hostIndex,
    const TString& message)
{
    auto error = MakeError(E_REJECTED, message);

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s RemoveHost failed (dbgId=%lu): %s",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        FormatError(error).c_str());

    auto dbgPtr = FastPathService->GetDirectBlockGroup(dbgId);
    Y_ABORT_UNLESS(dbgPtr);
    auto executor = dbgPtr->GetExecutor();
    // hostIndex is raw input, so it may not fit into THostIndex at all.
    const auto removeIndex = hostIndex < MaxHostCount
                                 ? static_cast<THostIndex>(hostIndex)
                                 : InvalidHostIndex;
    executor->ExecuteSimple(
        [dbgPtr, error, removeIndex]()
        { dbgPtr->OnRemoveHostFailed(removeIndex, error); });
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
