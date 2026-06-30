#include "fast_path_service.h"
#include "partition_direct_actor.h"
#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

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
        "%s AddHost persisted dbgId=%lu newHostIndex=%u",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        static_cast<ui32>(args.NewHostIndex));

    Y_ABORT_UNLESS(FastPathService);

    const auto& directBlockGroups = FastPathService->GetDirectBlockGroups();
    Y_ABORT_UNLESS(dbgId < directBlockGroups.size());

    auto dbgPtr = directBlockGroups[dbgId];
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple(
        [dbgPtr,
         newHostIndex = args.NewHostIndex,
         newDDiskId = std::move(args.NewDDiskId),
         newPBufferId = std::move(args.NewPBufferId)]() mutable
        {
            dbgPtr->AddHost(
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

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
