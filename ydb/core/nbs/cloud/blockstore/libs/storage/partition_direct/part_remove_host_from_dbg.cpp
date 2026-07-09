#include "fast_path_service.h"
#include "part_database.h"
#include "partition_direct_actor.h"
#include "partition_direct_events_private.h"

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

// Whether `ids` holds the given serialized id; a successful deletion
// response must hold neither the removed ddisk nor its pbuffer.
bool ContainsId(
    const google::protobuf::RepeatedPtrField<
        NKikimrBlobStorage::NDDisk::TDDiskId>& ids,
    const TString& idBytes)
{
    for (const auto& id: ids) {
        if (id.SerializeAsString() == idBytes) {
            return true;
        }
    }
    return false;
}

// Rebuilds the DBG's connections from the post-remove BSC group (the
// authoritative, already-compacted state) and stamps the removal record:
// the persisted vchunk configs shift by it at the next tablet start.
[[nodiscard]] TDirectBlockGroupsConnections RebuildConnectionsFromGroup(
    const TDirectBlockGroupsConnections& current,
    const NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
        TDirectBlockGroup& group,
    size_t dbgId,
    THostIndex removeIndex)
{
    TDirectBlockGroupsConnections result = current;
    auto* dbgConnections = result.MutableDirectBlockGroupConnections(dbgId);
    dbgConnections->ClearConnections();
    for (size_t i = 0; i < group.DDiskIdSize(); ++i) {
        auto* connection = dbgConnections->AddConnections();
        connection->MutableDDiskId()->CopyFrom(group.GetDDiskId(i));
        connection->MutablePersistentBufferDDiskId()->CopyFrom(
            group.GetPersistentBufferDDiskId(i));
    }
    dbgConnections->MutableLastRemove()->SetRemoveIndex(removeIndex);
    return result;
}

// Rebuilds the connections for an already-applied deletion (NOT_FOUND on a
// replay): erases the removed entry locally - the deterministic mirror of
// BSController's compaction - and stamps the removal record. The entry at
// removeIndex must still carry the intent's ids: membership ops are
// serialized, so the connections cannot have changed since the intent was
// persisted.
[[nodiscard]] TDirectBlockGroupsConnections RebuildConnectionsFromIntent(
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
        "RemoveHost replay: the persisted connection at removeIndex no "
        "longer matches the intent's ids (dbgId=%lu)",
        dbgId);
    dbgConnections->MutableConnections()->DeleteSubrange(removeIndex, 1);
    dbgConnections->MutableLastRemove()->SetRemoveIndex(removeIndex);
    return result;
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
    // Store the compacted connections (with the removal record) and clear
    // the intent in one tx, so they commit together: recovery never sees a
    // half-applied remove.
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
        "%s RemoveHost persisted dbgId=%lu removeIndex=%s; the vchunk "
        "configs shift at the next tablet start",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        PrintHostIndex(args.RemoveIndex).c_str());

    auto dbgPtr = GetDirectBlockGroupChecked(dbgId);
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple([dbgPtr, removeIndex = args.RemoveIndex]()
                            { dbgPtr->OnRemoveHostResult({}, removeIndex); });

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

    // The deletion is atomic on the BSController side (both commands commit
    // in one tx) and idempotent to repeat: a re-sent request whose ids are
    // already deleted answers NOT_FOUND without changing anything, so a
    // retry (e.g. after a restart) is safe.
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
    const TString ddiskIdBytes =
        RemoveHostInFlight->DDiskId.SerializeAsString();
    const TString pbufferIdBytes =
        RemoveHostInFlight->PBufferId.SerializeAsString();

    // NOT_FOUND means the ids are no longer allocated and nothing changed:
    // the deletion already applied (a replay after a crash between the
    // BSController mutation and the local commit). The response carries no
    // group listing, so commit by erasing the removed entry locally.
    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::NOT_FOUND) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s RemoveHost already applied (dbgId=%lu), persisting: %s",
            LogTitle.GetWithTime().c_str(),
            dbgId,
            msg->Record.GetErrorReason().c_str());

        auto updated = RebuildConnectionsFromIntent(
            DirectBlockGroupsConnections,
            dbgId,
            removeIndex,
            ddiskIdBytes,
            pbufferIdBytes);
        DirectBlockGroupsConnections = updated;

        ExecuteTx(
            ctx,
            CreateTx<TCommitRemoveHost>(
                std::move(updated),
                dbgId,
                removeIndex));
        return;
    }

    // A successful deletion lists the compacted group: exactly one entry
    // shorter than the persisted connections.
    const size_t expectedHostCount =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .ConnectionsSize() -
        1;

    NProto::TError error;
    const auto* group = ValidateAllocationResponseEnvelope(
        *msg,
        dbgId,
        expectedHostCount,
        &error);
    if (group == nullptr) {
        // Not cancelled: the intent stays persisted, so the remove is retried
        // on the next recovery.
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s RemoveHost (dbgId=%lu) not completed, kept for retry on "
            "recovery: %s",
            LogTitle.GetWithTime().c_str(),
            dbgId,
            FormatError(error).c_str());
        return;
    }

    if (ContainsId(group->GetDDiskId(), ddiskIdBytes) ||
        ContainsId(group->GetPersistentBufferDDiskId(), pbufferIdBytes))
    {
        // A successful deletion is atomic, so its response can hold neither
        // id. Keep the intent and retry on recovery rather than persist a
        // state we do not understand.
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s RemoveHost (dbgId=%lu) got a group still holding the "
            "deleted ids, kept for retry on recovery",
            LogTitle.GetWithTime().c_str(),
            dbgId);
        return;
    }

    auto updated = RebuildConnectionsFromGroup(
        DirectBlockGroupsConnections,
        *group,
        dbgId,
        removeIndex);
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

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle RemoveHostFromDBG dbgId=%lu hostIndex=%s",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        PrintHostIndex(static_cast<THostIndex>(hostIndex)).c_str());

    // TEvRemoveHostFromDBG is only sent by a running FastPathService, so it
    // (and the allocated DBGs) is alive by the time we handle the request.
    Y_ABORT_UNLESS(FastPathService);

    if (!ValidateRemoveHostFromDBGRequest(ctx, dbgId, hostIndex)) {
        return;
    }

    const auto& connection =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnections(hostIndex);

    TTxPartition::TRemoveHostInProgress intent;
    intent.SetDirectBlockGroupId(static_cast<ui32>(dbgId));
    intent.SetRemoveIndex(static_cast<ui32>(hostIndex));
    intent.MutableDDiskId()->CopyFrom(connection.GetDDiskId());
    intent.MutablePersistentBufferId()->CopyFrom(
        connection.GetPersistentBufferDDiskId());

    // Persist the intent (with the removed host's resource ids) before the
    // BSController request, sent from the tx's completion. A crash after
    // BSController applied the deletion but before the connections are
    // persisted leaves a durable intent; the replay re-sends the deletion
    // and commits on its NOT_FOUND answer.
    RemoveHostInFlight = TRemoveHostInFlight{
        .DirectBlockGroupId = dbgId,
        .RemoveIndex = static_cast<THostIndex>(hostIndex),
        .DDiskId = intent.GetDDiskId(),
        .PBufferId = intent.GetPersistentBufferId(),
    };

    ExecuteTx(ctx, CreateTx<TStartRemoveHost>(std::move(intent)));
}

bool TPartitionActor::ValidateRemoveHostFromDBGRequest(
    const TActorContext& ctx,
    size_t dbgId,
    size_t hostIndex)
{
    // The request always carries a DBG's own index (QueryRemoveHost), so an
    // out-of-range dbgId is a bug, not a bad request.
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

    // The partition owns the persisted connection count; the semantic
    // preconditions (host disabled, its pbuffer drained, quorum) live in the
    // DBG's QueryRemoveHost, which sees the live configs.
    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);

    if (dbgConn.HasLastRemove()) {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            "A committed RemoveHost is pending a tablet restart");
        return false;
    }

    const auto currentSize =
        static_cast<size_t>(dbgConn.GetConnections().size());

    if (hostIndex >= currentSize) {
        // Raw index on purpose: the value may exceed the THostIndex range,
        // and the message must show what was actually rejected.
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            TStringBuilder()
                << "host index " << hostIndex << " is out of range (have "
                << currentSize << ")");
        return false;
    }
    if (currentSize <= 1) {
        RejectRemoveHost(
            ctx,
            dbgId,
            hostIndex,
            "RemoveHost from a single-host DBG is not supported");
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

    // Notify the DBG that asked for the removal.
    auto dbgPtr = GetDirectBlockGroupChecked(dbgId);
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple(
        [dbgPtr, error, removeIndex = static_cast<THostIndex>(hostIndex)]()
        { dbgPtr->OnRemoveHostResult(error, removeIndex); });
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
