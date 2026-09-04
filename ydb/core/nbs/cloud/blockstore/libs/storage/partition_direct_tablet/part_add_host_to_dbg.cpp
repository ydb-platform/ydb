#include "part_database.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>

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

// Builds in `updated` the connections the add will commit: `connections` with
// the granted host appended. BSController answers with the whole group, so the
// granted host is its last entry. A response that does not fit is answered
// with an error and not with an abort: the intent stays on disk, so the add is
// retried at the next start.
NProto::TError AddConnection(
    const TDirectBlockGroupsConnections& connections,
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& msg,
    size_t dbgId,
    ui32 expectedCurrent,
    TDirectBlockGroupsConnections* updated)
{
    const auto response = ValidateAllocationResponse(
        msg,
        dbgId,
        static_cast<size_t>(expectedCurrent) + 1);
    if (HasError(response.Error)) {
        return response.Error;
    }
    const auto& group = *response.Group;

    const auto& newDDiskId = group.GetDDiskId(expectedCurrent);
    const auto& newPBufferId =
        group.GetPersistentBufferDDiskId(expectedCurrent);

    const TString newDDiskIdBytes = newDDiskId.SerializeAsString();
    const TString newPBufferIdBytes = newPBufferId.SerializeAsString();
    for (const auto& conn:
         connections.GetDirectBlockGroupConnections(dbgId).GetConnections())
    {
        // A dead slot's ids are freed in BSC and may be granted to the
        // new host.
        if (conn.GetRemoved()) {
            continue;
        }
        if (conn.GetDDiskId().SerializeAsString() == newDDiskIdBytes ||
            conn.GetPersistentBufferDDiskId().SerializeAsString() ==
                newPBufferIdBytes)
        {
            return MakeError(
                E_REJECTED,
                "BSController returned a DDisk/PBuffer already in this DBG");
        }
    }

    *updated = connections;
    auto* dbgConnections = updated->MutableDirectBlockGroupConnections(dbgId);
    auto* connection = dbgConnections->AddConnections();
    connection->MutableDDiskId()->CopyFrom(newDDiskId);
    connection->MutablePersistentBufferDDiskId()->CopyFrom(newPBufferId);
    dbgConnections->SetConnectionConfigGeneration(
        dbgConnections->GetConnectionConfigGeneration() + 1);
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
    proto.SetConnectionConfigGeneration(args.ConnectionConfigGeneration);
    db.StoreAddHostInProgress(proto);
}

void TPartitionActor::CompleteStartAddHost(
    const TActorContext& ctx,
    TTxPartition::TStartAddHost& args)
{
    SendAllocateDDiskForAddHost(ctx, args.DirectBlockGroupId);
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

    Y_ABORT_UNLESS(FastPathService);

    const auto& dbgConnections =
        args.DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);

    // AddConnection appends, so the plan must name the entry it appended.
    // Nothing may renumber the slots while a plan is pending.
    Y_ABORT_UNLESS(
        static_cast<size_t>(args.NewHostIndex) + 1 ==
            dbgConnections.ConnectionsSize(),
        "AddHost plan points at %lu, the connection landed at %lu",
        static_cast<size_t>(args.NewHostIndex),
        dbgConnections.ConnectionsSize() - 1);

    const auto& newConnection =
        dbgConnections.GetConnections(args.NewHostIndex);

    auto dbgPtr = FastPathService->GetDirectBlockGroup(dbgId);
    Y_ABORT_UNLESS(dbgPtr);
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple(
        [dbgPtr,
         newHostIndex = args.NewHostIndex,
         connectionConfigGeneration =
             dbgConnections.GetConnectionConfigGeneration(),
         newDDiskId = newConnection.GetDDiskId(),
         newPBufferId = newConnection.GetPersistentBufferDDiskId()]() mutable
        {
            dbgPtr->OnAddHostSucceeded(
                newHostIndex,
                std::move(newDDiskId),
                std::move(newPBufferId),
                connectionConfigGeneration);
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

    // BSC lists live hosts only, dead slots have no resources there. The
    // slot the plan names may be further along than that count.
    const auto newHostIndex = AddHostInFlight->NewHostIndex;
    const ui32 expectedCurrent = static_cast<ui32>(LiveHostCount(
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)));
    NTabletPipe::CloseClient(ctx, AddHostInFlight->BSPipeClient);

    TDirectBlockGroupsConnections updated;
    if (auto error = AddConnection(
            DirectBlockGroupsConnections,
            *msg,
            dbgId,
            expectedCurrent,
            &updated);
        HasError(error))
    {
        // Not cancelled: the intent stays persisted, so it is retried on the
        // next recovery until BSController grants the DDisk.
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s AddHost (dbgId=%lu) not completed, kept for retry on "
            "recovery: %s",
            LogTitle.GetWithTime().c_str(),
            dbgId,
            FormatError(error).Quote().c_str());
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
    const ui32 connectionConfigGeneration = msg->ConnectionConfigGeneration;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle AddHost to dbgId=%lu, connection config generation %u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        connectionConfigGeneration);

    // The request always carries a DBG's own index so an out-of-range dbgId is
    // a bug, not a bad request.
    const size_t dbgCount =
        DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize();
    Y_ABORT_UNLESS(
        dbgId < dbgCount,
        "AddHost for out-of-range dbgId=%lu (have %lu DBGs)",
        dbgId,
        dbgCount);

    if (!ValidateAddHostToDBGRequest(ctx, dbgId, connectionConfigGeneration)) {
        return;
    }

    // The plan: a new host is always added at the end of the validated
    // state.
    const auto newHostIndex = static_cast<THostIndex>(
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnections()
            .size());

    // Persist the intent before the BSController request (sent from the tx's
    // completion). A crash after the DDisk is allocated but before the
    // connection is persisted then leaves a durable intent, replayed on
    // restart.
    AddHostInFlight = TAddHostInFlight{
        .DirectBlockGroupId = dbgId,
        .NewHostIndex = newHostIndex,
        .ConnectionConfigGeneration = connectionConfigGeneration,
    };

    ExecuteTx(
        ctx,
        CreateTx<TStartAddHost>(
            dbgId,
            newHostIndex,
            connectionConfigGeneration));
}

bool TPartitionActor::ValidateAddHostToDBGRequest(
    const TActorContext& ctx,
    size_t dbgId,
    ui32 connectionConfigGeneration)
{
    if (AddHostInFlight.has_value()) {
        RejectAddHost(ctx, dbgId, "Another AddHost is already in progress");
        return false;
    }
    if (RemoveHostInFlight.has_value()) {
        RejectAddHost(ctx, dbgId, "A RemoveHost is already in progress");
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
            TStringBuilder()
                << "slot budget exhausted (" << MaxHostCount
                << "), restart the tablet to compact the dead slots");
        return false;
    }
    if (currentSize == 0) {
        RejectAddHost(ctx, dbgId, "AddHost on an empty DBG is not supported");
        return false;
    }

    if (connectionConfigGeneration != dbgConn.GetConnectionConfigGeneration()) {
        RejectAddHost(
            ctx,
            dbgId,
            TStringBuilder()
                << "AddHost was decided on connection config generation "
                << connectionConfigGeneration << ", the group is at "
                << dbgConn.GetConnectionConfigGeneration());
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
        FormatError(error).Quote().c_str());

    auto dbgPtr = FastPathService->GetDirectBlockGroup(dbgId);
    Y_ABORT_UNLESS(dbgPtr);
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple([dbgPtr, error]()
                            { dbgPtr->OnAddHostFailed(error); });
}

void TPartitionActor::SendAllocateDDiskForAddHost(
    const TActorContext& ctx,
    size_t dbgId)
{
    Y_ABORT_UNLESS(AddHostInFlight.has_value());

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionCount =
        CalcRegionCount(blockCount, VolumeConfig.GetBlockSize());

    const auto pipe = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
    AddHostInFlight->BSPipeClient = pipe;

    // NumDDisks is the desired final state in live hosts (dead slots have no
    // resources in BSC), so a re-sent request is idempotent.
    const ui32 numDDisks = static_cast<ui32>(
        LiveHostCount(
            DirectBlockGroupsConnections.GetDirectBlockGroupConnections(
                dbgId)) +
        1);
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
