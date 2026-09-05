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
using TAllocatedDirectBlockGroup = NKikimrBlobStorage::
    TEvControllerAllocateDDiskBlockGroupResult::TDirectBlockGroup;

// Marks the slot dead in a copy of the connections and moves the group to the
// next connection config generation. The entry must still match the intent's
// ids: membership ops are serialized.
[[nodiscard]] TDirectBlockGroupsConnections MarkSlotRemoved(
    const TDirectBlockGroupsConnections& current,
    size_t dbgId,
    THostIndex removeIndex,
    const TString& ddiskIdBytes,
    const TString& pbufferIdBytes)
{
    TDirectBlockGroupsConnections result = current;
    auto* dbgConnections = result.MutableDirectBlockGroupConnections(dbgId);
    Y_ABORT_UNLESS(removeIndex < dbgConnections->ConnectionsSize());
    const auto& removed = dbgConnections->GetConnections(removeIndex);
    Y_ABORT_UNLESS(
        removed.GetDDiskId().SerializeAsString() == ddiskIdBytes &&
            removed.GetPersistentBufferDDiskId().SerializeAsString() ==
                pbufferIdBytes,
        "RemoveHost: the persisted connection at removeIndex no longer "
        "matches the intent's ids (dbgId=%lu)",
        dbgId);
    Y_ABORT_UNLESS(
        !removed.GetRemoved(),
        "RemoveHost: the slot is already removed (dbgId=%lu)",
        dbgId);
    dbgConnections->MutableConnections(removeIndex)->SetRemoved(true);
    dbgConnections->SetConnectionConfigGeneration(
        dbgConnections->GetConnectionConfigGeneration() + 1);
    return result;
}

// Whether BSC's post-remove listing equals our live entries, in order
// (both sides append at the end and compact on delete).
[[nodiscard]] bool MatchesGroup(
    const TDirectBlockGroupConnections& connections,
    const TAllocatedDirectBlockGroup& group)
{
    if (group.PersistentBufferDDiskIdSize() != group.DDiskIdSize()) {
        return false;
    }
    size_t bscIndex = 0;
    for (const auto& connection: connections.GetConnections()) {
        if (connection.GetRemoved()) {
            continue;
        }
        if (bscIndex >= group.DDiskIdSize() ||
            connection.GetDDiskId().SerializeAsString() !=
                group.GetDDiskId(bscIndex).SerializeAsString() ||
            connection.GetPersistentBufferDDiskId().SerializeAsString() !=
                group.GetPersistentBufferDDiskId(bscIndex).SerializeAsString())
        {
            return false;
        }
        ++bscIndex;
    }
    return bscIndex == group.DDiskIdSize();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// static
size_t TPartitionActor::LiveHostCount(
    const ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupConnections&
        connections)
{
    size_t liveCount = 0;
    for (const auto& connection: connections.GetConnections()) {
        if (!connection.GetRemoved()) {
            ++liveCount;
        }
    }
    return liveCount;
}

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
    // One tx: recovery never sees a half-applied remove.
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
    const ui32 connectionConfigGeneration =
        args.DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnectionConfigGeneration();
    executor->ExecuteSimple(
        [dbgPtr, removeIndex = args.RemoveIndex, connectionConfigGeneration]() {
            dbgPtr->OnRemoveHostSucceeded(
                removeIndex,
                connectionConfigGeneration);
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

    const auto removeIndex = RemoveHostInFlight->RemoveIndex;
    auto updated = MarkSlotRemoved(
        DirectBlockGroupsConnections,
        dbgId,
        removeIndex,
        RemoveHostInFlight->DDiskId.SerializeAsString(),
        RemoveHostInFlight->PBufferId.SerializeAsString());
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
        if (!MatchesGroup(updatedDbg, *response.Group)) {
            LOG_WARN(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s RemoveHost (dbgId=%lu) got a group that differs from the "
                "compacted connections, kept for retry on recovery",
                LogTitle.GetWithTime().c_str(),
                dbgId);
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
    const ui32 connectionConfigGeneration = msg->ConnectionConfigGeneration;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle RemoveHostFromDBG dbgId=%lu hostIndex=%s connection "
        "config generation=%u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        PrintHostIndex(static_cast<THostIndex>(hostIndex)).c_str(),
        connectionConfigGeneration);

    Y_ABORT_UNLESS(FastPathService);

    if (!ValidateRemoveHostFromDBGRequest(
            ctx,
            dbgId,
            hostIndex,
            connectionConfigGeneration))
    {
        return;
    }

    const auto& connection =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnections(hostIndex);

    TTxPartition::TRemoveHostInProgress intent;
    intent.SetDirectBlockGroupId(static_cast<ui32>(dbgId));
    intent.SetRemoveIndex(static_cast<ui32>(hostIndex));
    intent.MutableDDiskId()->CopyFrom(connection.GetDDiskId());
    intent.MutablePersistentBufferDDiskId()->CopyFrom(
        connection.GetPersistentBufferDDiskId());
    intent.SetConnectionConfigGeneration(connectionConfigGeneration);

    // The intent is persisted before the BSC request: a crash in between
    // leaves it durable, and the replay finishes the removal.
    RemoveHostInFlight = TRemoveHostInFlight{
        .DirectBlockGroupId = dbgId,
        .RemoveIndex = static_cast<THostIndex>(hostIndex),
        .DDiskId = intent.GetDDiskId(),
        .PBufferId = intent.GetPersistentBufferDDiskId(),
        .ConnectionConfigGeneration = connectionConfigGeneration,
    };

    ExecuteTx(ctx, CreateTx<TStartRemoveHost>(std::move(intent)));
}

bool TPartitionActor::ValidateRemoveHostFromDBGRequest(
    const TActorContext& ctx,
    size_t dbgId,
    size_t hostIndex,
    ui32 connectionConfigGeneration)
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
    if (dbgConn.GetConnections(hostIndex).GetRemoved()) {
        RejectRemoveHost(ctx, dbgId, hostIndex, "The slot is already removed");
        return false;
    }
    if (LiveHostCount(dbgConn) <= 1) {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            "RemoveHost from a single-host DBG is not supported");
        return false;
    }
    if (connectionConfigGeneration != dbgConn.GetConnectionConfigGeneration()) {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            TStringBuilder()
                << "RemoveHost was decided on connection config generation "
                << connectionConfigGeneration << ", the group is at "
                << dbgConn.GetConnectionConfigGeneration());
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
