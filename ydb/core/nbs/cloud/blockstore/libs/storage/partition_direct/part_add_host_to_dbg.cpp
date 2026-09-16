#include "fast_path_service.h"
#include "part_database.h"
#include "partition_direct_actor.h"
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
#include "partition_direct_events_private.h"
=======

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TDirectBlockGroupsConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
=======
namespace {

////////////////////////////////////////////////////////////////////////////////

// Builds in `updated` the connections the add will store in the local db.
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
        if (conn.GetRemovedFromBSC()) {
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
    dbgConnections->SetDBGConnectionsConfigGeneration(
        dbgConnections->GetDBGConnectionsConfigGeneration() + 1);
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
    proto.SetLiveHostCount(args.LiveHostCount);
    proto.SetDBGConnectionsConfigGeneration(
        args.DBGConnectionsConfigGeneration);
    db.StoreAddHostInProgress(proto);
}

void TPartitionActor::CompleteStartAddHost(
    const TActorContext& ctx,
    TTxPartition::TStartAddHost& args)
{
    SendAllocateDDiskForAddHost(ctx, args.DirectBlockGroupId);
}

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp
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

    const auto& directBlockGroups = FastPathService->GetDirectBlockGroups();
    Y_ABORT_UNLESS(dbgId < directBlockGroups.size());

    // The new connection was persisted at NewHostIndex; read its DDisk/PBuffer
    // back out to hand to the DBG.
    const auto& newConnection =
        args.DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
            .GetConnections(args.NewHostIndex);

    auto dbgPtr = directBlockGroups[dbgId];
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
    proto.SetDirectBlockGroupId(static_cast<ui32>(args.DirectBlockGroupId));
    proto.SetNewHostIndex(static_cast<ui32>(args.NewHostIndex));
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

namespace {

// Validates a BSController add-host allocation response and, on success,
// appends the newly granted DDisk/PBuffer to `result` (a copy of `current`).
// Returns a retriable error - never aborts - for a failed or malformed
// response: the add-host intent stays persisted and is replayed on recovery, so
// a bad response must not crash-loop the tablet.
NProto::TError AddConnection(
    const TDirectBlockGroupsConnections& current,
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& msg,
    size_t dbgId,
    ui32 expectedCurrent,
    TDirectBlockGroupsConnections* result)
{
    const auto& record = msg.Record;

    if (record.GetStatus() != NKikimrProto::EReplyStatus::OK) {
        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "BSController error: " << record.GetErrorReason());
    }

    if (record.DirectBlockGroupsSize() != 1) {
        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "BSController returned " << record.DirectBlockGroupsSize()
                << " DirectBlockGroups, expected 1");
    }

    const auto& group = record.GetDirectBlockGroups(0);
    if (group.GetDirectBlockGroupId() != dbgId) {
        return MakeError(
            E_REJECTED,
            "BSController response is for a different DirectBlockGroup");
    }

    if (group.GetError()) {
        return MakeError(
            E_REJECTED,
            "BSController reported an error for this DirectBlockGroup");
    }

    if (static_cast<ui32>(group.DDiskIdSize()) != expectedCurrent + 1 ||
        static_cast<ui32>(group.PersistentBufferDDiskIdSize()) !=
            expectedCurrent + 1)
    {
        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "BSController returned " << group.DDiskIdSize()
                << " ddisks / " << group.PersistentBufferDDiskIdSize()
                << " pbuffers, expected " << (expectedCurrent + 1));
    }

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

    const auto& dbgConnections =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
    // BSC lists live hosts only, dead slots have no resources there.
    const ui32 expectedCurrent =
        static_cast<ui32>(LiveHostCount(dbgConnections));
    Y_ABORT_UNLESS(
        expectedCurrent == AddHostInFlight->LiveHostCount,
        "AddHost plan was decided on %u live hosts, the group has %u",
        AddHostInFlight->LiveHostCount,
        expectedCurrent);
    // Host indices are positions in Connections, dead entries included, so
    // the new host takes the position after the last entry.
    const auto newHostIndex =
        static_cast<THostIndex>(dbgConnections.ConnectionsSize());

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
            FormatError(error).c_str());
        return;
    }

    DirectBlockGroupsConnections = updated;

    ExecuteTx(
        ctx,
        CreateTx<TAddHostToDBG>(std::move(updated), dbgId, newHostIndex));
}

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
=======
void TPartitionActor::HandleAddHostToDBG(
    const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(FastPathService);

    const auto* msg = ev->Get();
    const size_t dbgId = msg->DirectBlockGroupId;
    const ui32 dbgConnectionsConfigGeneration =
        msg->DBGConnectionsConfigGeneration;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle AddHost to dbgId=%lu, DBG connections config generation %u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        dbgConnectionsConfigGeneration);

    // The request always carries a DBG's own index so an out-of-range dbgId is
    // a bug, not a bad request.
    const size_t dbgCount =
        DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize();
    Y_ABORT_UNLESS(
        dbgId < dbgCount,
        "AddHost for out-of-range dbgId=%lu (have %lu DBGs)",
        dbgId,
        dbgCount);

    if (!ValidateAddHostToDBGRequest(
            ctx,
            dbgId,
            dbgConnectionsConfigGeneration))
    {
        return;
    }

    const auto liveHostCount = static_cast<ui32>(LiveHostCount(
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)));

    // Persist the intent before the BSController request (sent from the tx's
    // completion). A crash after the DDisk is allocated but before the
    // connection is persisted then leaves a durable intent, replayed on
    // restart.
    AddHostInFlight = TAddHostInFlight{
        .DirectBlockGroupId = dbgId,
        .LiveHostCount = liveHostCount,
        .DBGConnectionsConfigGeneration = dbgConnectionsConfigGeneration,
    };

    ExecuteTx(
        ctx,
        CreateTx<TStartAddHost>(
            dbgId,
            liveHostCount,
            dbgConnectionsConfigGeneration));
}

bool TPartitionActor::ValidateAddHostToDBGRequest(
    const TActorContext& ctx,
    size_t dbgId,
    ui32 dbgConnectionsConfigGeneration)
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

    if (dbgConnectionsConfigGeneration !=
        dbgConn.GetDBGConnectionsConfigGeneration())
    {
        RejectAddHost(
            ctx,
            dbgId,
            TStringBuilder()
                << "AddHost was decided on DBG connections config generation "
                << dbgConnectionsConfigGeneration << ", the group is at "
                << dbgConn.GetDBGConnectionsConfigGeneration());
        return false;
    }

    return true;
}

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp
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

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
    // Notify the DBG that asked for the host. dbgId is always a valid request
    // index (see ValidateAddHostToDBGRequest), so the DBG exists.
    const auto& directBlockGroups = FastPathService->GetDirectBlockGroups();
    Y_ABORT_UNLESS(
        dbgId < directBlockGroups.size(),
        "RejectAddHost for a non-existent DBG (dbgId=%lu)",
        dbgId);
    auto dbgPtr = directBlockGroups[dbgId];
=======
    auto dbgPtr = FastPathService->GetDirectBlockGroup(dbgId);
    Y_ABORT_UNLESS(dbgPtr);
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple([dbgPtr, error]()
                            { dbgPtr->OnAddHostResult(error, 0, {}, {}); });
}

bool TPartitionActor::ValidateAddHostToDBGRequest(
    const TActorContext& ctx,
    size_t dbgId)
{
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
    // The request always carries a DBG's own index (QueryAddHost ->
    // RequestAddHost), so an out-of-range dbgId is a bug, not a bad request.
    const auto dbgCount = static_cast<size_t>(
        DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize());
    Y_ABORT_UNLESS(
        dbgId < dbgCount,
        "AddHost for out-of-range dbgId=%lu (have %lu DBGs)",
        dbgId,
        dbgCount);
=======
    if (CurrentStateFunc() == &TThis::StateDelete) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Skip AddHost BSC send during delete dbgId=%lu",
            LogTitle.GetWithTime().c_str(),
            dbgId);
        return;
    }

    Y_ABORT_UNLESS(AddHostInFlight.has_value());
>>>>>>> e44405fda45 ([NBS-7661] Add BSC proxy actor (#53116)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
    if (AddHostInFlight.has_value()) {
        RejectAddHost(ctx, dbgId, "Another AddHost is already in progress");
        return false;
    }
=======
    const ui64 regionCount = GetRegionCount(
        VolumeConfig.GetPartitions(0).GetBlockCount(),
        VolumeConfig.GetBlockSize(),
        StorageConfig->GetVChunkSize());
    const ui32 vChunkPerDbgCount = GetVChunkCountPerDirectBlockGroup(
        regionCount,
        DefaultVolumeDirectBlockGroupCount);
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
    // Authoritative AddHost gate: reads the persisted connection count under
    // the single-in-flight guard above, so it cannot overshoot MaxHostCount or
    // race a concurrent add. The DBG's own DDiskConnections lags, so it cannot
    // gate.
    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
    const auto currentSize = static_cast<ui32>(dbgConn.GetConnections().size());

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
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
=======
=======
>>>>>>> e44405fda45 ([NBS-7661] Add BSC proxy actor (#53116)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp
    // NumDDisks is the desired final state in live hosts (dead slots have no
    // resources in BSC), so a re-sent request is idempotent.
    const ui32 numDDisks = static_cast<ui32>(
        LiveHostCount(
            DirectBlockGroupsConnections.GetDirectBlockGroupConnections(
                dbgId)) +
        1);
    auto request = MakeAllocateDDiskBlockGroupRequest();
>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
    return true;
=======
    auto* op = request->Record.AddDirectBlockGroupOperations();
    op->SetDirectBlockGroupId(dbgId);
    auto* define = op->MutableDefineDirectBlockGroup();
    define->SetNumDDisks(numDDisks);
    define->SetNumChunksPerDDisk(vChunkPerDbgCount);
    define->SetNumPersistentBuffers(numDDisks);

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_add_host_to_dbg.cpp
    NTabletPipe::SendData(ctx, pipe, request.release(), dbgId);
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp
=======
    SendToBsc(ctx, THolder<IEventBase>(request.release()), dbgId);
>>>>>>> e44405fda45 ([NBS-7661] Add BSC proxy actor (#53116)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/part_add_host_to_dbg.cpp
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
