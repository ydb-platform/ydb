#include "fast_path_service.h"
#include "part_database.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TDirectBlockGroupsConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Appends the newly granted DDisk/PBuffer (the last entry of a validated
// allocation response group) to `result` (a copy of `current`). Returns a
// retriable error - never aborts - when the grant duplicates an existing
// connection: the add-host intent stays persisted and is replayed on
// recovery, so a bad response must not crash-loop the tablet.
NProto::TError AddConnection(
    const TDirectBlockGroupsConnections& current,
    const NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
        TDirectBlockGroup& group,
    size_t dbgId,
    ui32 expectedCurrent,
    TDirectBlockGroupsConnections* result)
{
    const auto& newDDiskId = group.GetDDiskId(expectedCurrent);
    const auto& newPBufferId =
        group.GetPersistentBufferDDiskId(expectedCurrent);

    const TString newDDiskIdBytes = newDDiskId.SerializeAsString();
    const TString newPBufferIdBytes = newPBufferId.SerializeAsString();
    for (const auto& conn:
         current.GetDirectBlockGroupConnections(dbgId).GetConnections())
    {
        if (conn.GetDDiskId().SerializeAsString() == newDDiskIdBytes ||
            conn.GetPersistentBufferDDiskId().SerializeAsString() ==
                newPBufferIdBytes)
        {
            return MakeError(
                E_REJECTED,
                "BSController returned a DDisk/PBuffer already in this DBG");
        }
    }

    *result = current;
    auto* connection =
        result->MutableDirectBlockGroupConnections(dbgId)->AddConnections();
    connection->MutableDDiskId()->CopyFrom(newDDiskId);
    connection->MutablePersistentBufferDDiskId()->CopyFrom(newPBufferId);
    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareStartAddHost(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStartAddHost& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteStartAddHost(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStartAddHost& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    TTxPartition::TAddHostInProgress proto;
    proto.SetDirectBlockGroupId(args.DirectBlockGroupId);
    proto.SetNewHostIndex(args.NewHostIndex);
    db.StoreAddHostInProgress(proto);
}

void TPartitionActor::CompleteStartAddHost(
    const TActorContext& ctx,
    TTxPartition::TStartAddHost& args)
{
    SendAllocateDDiskForAddHost(
        ctx,
        args.DirectBlockGroupId,
        args.NewHostIndex);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareAddHostToDBG(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddHostToDBG& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteAddHostToDBG(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddHostToDBG& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);
    // Store the connection and clear the intent in one tx, so they commit
    // together: recovery never sees a half-applied add.
    db.StoreDirectBlockGroupsConnections(args.DirectBlockGroupsConnections);
    db.ClearAddHostInProgress();
}

void TPartitionActor::CompleteAddHostToDBG(
    const TActorContext& ctx,
    TTxPartition::TAddHostToDBG& args)
{
    const size_t dbgId = args.DirectBlockGroupId;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s AddHost persisted dbgId=%lu newHostIndex=%s",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        PrintHostIndex(args.NewHostIndex).c_str());

    // The new connection was persisted at NewHostIndex; read its DDisk/PBuffer
    // back out to hand to the DBG.
    const auto& newConnection =
        args.DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnections(args.NewHostIndex);

    auto dbgPtr = GetDirectBlockGroupChecked(dbgId);
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple(
        [dbgPtr,
         newHostIndex = args.NewHostIndex,
         newDDiskId = newConnection.GetDDiskId(),
         newPBufferId = newConnection.GetPersistentBufferDDiskId()]() mutable
        {
            dbgPtr->OnAddHostResult(
                {},
                newHostIndex,
                std::move(newDDiskId),
                std::move(newPBufferId));
        });

    AddHostInFlight.reset();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddHostAllocationResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const size_t dbgId = ev->Cookie;

    if (!AddHostInFlight.has_value() ||
        AddHostInFlight->DirectBlockGroupId != dbgId)
    {
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s AddHost response for unexpected dbgId=%lu (stale)",
            LogTitle.GetWithTime().c_str(),
            dbgId);
        return;
    }

    const ui32 expectedCurrent = AddHostInFlight->NewHostIndex;
    const auto newHostIndex = AddHostInFlight->NewHostIndex;
    NTabletPipe::CloseClient(ctx, AddHostInFlight->BSPipeClient);

    NProto::TError error;
    const auto* group = ValidateAllocationResponseEnvelope(
        *msg,
        dbgId,
        expectedCurrent + 1,
        &error);

    TDirectBlockGroupsConnections updated;
    if (group != nullptr) {
        error = AddConnection(
            DirectBlockGroupsConnections,
            *group,
            dbgId,
            expectedCurrent,
            &updated);
    }
    if (HasError(error)) {
        // Not cancelled: the intent stays persisted, so it is retried on the
        // next recovery until BSController grants the DDisk.
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s AddHost (dbgId=%lu) not completed, kept for retry on "
            "recovery: %s",
            LogTitle.GetWithTime().c_str(),
            dbgId,
            FormatError(error).c_str());
        return;
    }

    DirectBlockGroupsConnections = updated;

    ExecuteTx(
        ctx,
        CreateTx<TAddHostToDBG>(std::move(updated), dbgId, newHostIndex));
}

void TPartitionActor::HandleAddHostToDBG(
    const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(FastPathService);

    const auto* msg = ev->Get();
    const size_t dbgId = msg->DirectBlockGroupId;
    THostIndex newHostIndex = msg->NewHostIndex;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle AddHost %s to dbgId=%lu",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(newHostIndex).c_str(),
        dbgId);

    // The request always carries a DBG's own index so an out-of-range dbgId is
    // a bug, not a bad request.
    const size_t dbgCount =
        DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize();
    Y_ABORT_UNLESS(
        dbgId < dbgCount,
        "AddHost for out-of-range dbgId=%lu (have %lu DBGs)",
        dbgId,
        dbgCount);

    if (newHostIndex == 0) {
        // When a request comes from mon-page, need to find the newHostIndex.
        const auto& dbgConn =
            DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
        newHostIndex = static_cast<ui32>(dbgConn.GetConnections().size());
    }

    if (!ValidateAddHostToDBGRequest(ctx, dbgId, newHostIndex)) {
        return;
    }

    // Persist the intent before the BSController request (sent from the tx's
    // completion). A crash after the DDisk is allocated but before the
    // connection is persisted then leaves a durable intent, replayed on
    // restart.
    AddHostInFlight = TAddHostInFlight{
        .DirectBlockGroupId = dbgId,
        .NewHostIndex = newHostIndex,
    };

    ExecuteTx(ctx, CreateTx<TStartAddHost>(dbgId, newHostIndex));
}

bool TPartitionActor::ValidateAddHostToDBGRequest(
    const TActorContext& ctx,
    size_t dbgId,
    THostIndex newHostIndex)
{
    if (AddHostInFlight.has_value()) {
        RejectAddHost(ctx, dbgId, "Another AddHost is already in progress");
        return false;
    }
    if (RemoveHostInFlight.has_value()) {
        RejectAddHost(ctx, dbgId, "A RemoveHost is already in progress");
        return false;
    }
    if (DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .HasLastRemove())
    {
        RejectAddHost(
            ctx,
            dbgId,
            "A committed RemoveHost is pending a tablet restart");
        return false;
    }

    // Authoritative AddHost gate: reads the persisted connection count under
    // the single-in-flight guard above, so it cannot overshoot MaxHostCount or
    // race a concurrent add. The DBG's own DDiskConnections lags, so it cannot
    // gate.
    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
    const size_t currentSize = dbgConn.GetConnections().size();

    if (currentSize >= MaxHostCount) {
        RejectAddHost(
            ctx,
            dbgId,
            TStringBuilder() << "MaxHostCount=" << MaxHostCount << " reached");
        return false;
    }
    if (currentSize == 0) {
        RejectAddHost(ctx, dbgId, "AddHost on an empty DBG is not supported");
        return false;
    }

    if (newHostIndex != currentSize) {
        RejectAddHost(
            ctx,
            dbgId,
            TStringBuilder()
                << "AddHost " << PrintHostIndex(newHostIndex)
                << " must append at the end. Current size=" << currentSize);
        return false;
    }

    return true;
}

void TPartitionActor::RejectAddHost(
    const NActors::TActorContext& ctx,
    size_t dbgId,
    const TString& message)
{
    auto error = MakeError(E_REJECTED, message);

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s AddHost failed (dbgId=%lu): %s",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        FormatError(error).c_str());

    // Notify the DBG that asked for the host.
    auto dbgPtr = GetDirectBlockGroupChecked(dbgId);
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple([dbgPtr, error]()
                            { dbgPtr->OnAddHostResult(error, 0, {}, {}); });
}

void TPartitionActor::SendAllocateDDiskForAddHost(
    const TActorContext& ctx,
    size_t dbgId,
    THostIndex newHostIndex)
{
    Y_ABORT_UNLESS(AddHostInFlight.has_value());

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionCount =
        CalcRegionCount(blockCount, VolumeConfig.GetBlockSize());

    const auto pipe = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
    AddHostInFlight->BSPipeClient = pipe;

    // Idempotent: NumDDisks=N+1 is the desired final state, not "add one"; a
    // re-sent request returns the same DDisk from BSController's persisted
    // allocation, so a retry (e.g. after a restart) is safe.
    const ui32 numDDisks = newHostIndex + 1;
    auto request = MakeAllocateDDiskBlockGroupRequest();

    auto* op = request->Record.AddDirectBlockGroupOperations();
    op->SetDirectBlockGroupId(dbgId);
    auto* define = op->MutableDefineDirectBlockGroup();
    define->SetNumDDisks(numDDisks);
    define->SetNumChunksPerDDisk(regionCount);
    define->SetNumPersistentBuffers(numDDisks);

    NTabletPipe::SendData(ctx, pipe, request.release(), dbgId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
