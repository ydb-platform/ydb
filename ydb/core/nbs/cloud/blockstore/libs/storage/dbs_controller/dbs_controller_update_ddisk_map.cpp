#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

template <typename TField, typename TPred>
    requires requires(const TField& f, TPred pred)
{
    {
        pred(f)
    } -> std::same_as<bool>;
}

static size_t RemoveIf(
    ::google::protobuf::RepeatedPtrField<TField>& repeatedField,
    TPred predicate)
{
    size_t removesCount = 0;
    for (int i = 0; i < repeatedField.size(); ++i) {
        if (predicate(repeatedField.Get(i))) {
            repeatedField.SwapElements(i, repeatedField.size() - 1);
            repeatedField.RemoveLast();
            --i;
            ++removesCount;
        }
    }
    return removesCount;
}

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleUpdateDDiskMapRequest(
    const TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle UpdateDDiskMap request" << ", tabletId: "
                                        << ev->Get()->Record.GetTabletId());

    ExecuteTx(
        ctx,
        CreateTx<TUpdateDDiskMap>(
            NBS::NStorage::CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>()),
            ev->Get()->Record.GetTabletId(),
            ev->Get()->Record.GetPartitionDDisks()));
}

void TDbsControllerActor::HandleRemoveTabletDDiskMapRequest(
    const TEvDbsControllerPrivate::TEvRemoveTabletDDiskMapRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle RemoveTabletDDiskMap request"
            << ", tabletId: " << ev->Get()->Record.GetTabletId());

    ExecuteTx(
        ctx,
        CreateTx<TRemoveTabletDDiskMap>(
            NBS::NStorage::CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>()),
            ev->Get()->Record.GetTabletId()));
}

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);

    for (ui64 dbgIndex = 0;
         dbgIndex < args.DDisks.DirectBlockGroupsDDisksSize();
         ++dbgIndex)
    {
        NProto::TDirectBlockGroupDDisks directRecord;
        if (!db.LoadDirectRecord(
                {args.PartitionTabletId, dbgIndex},
                directRecord))
        {
            return false;
        }

        THashSet<TDbsControllerDatabase::TInverseKey> oldRelations;

        for (const auto& ids: directRecord.GetDDiskIds()) {
            oldRelations.insert(
                {ids.GetDDisk().GetNodeId(),
                 ids.GetDDisk().GetPDiskId(),
                 ids.GetDDisk().GetDDiskSlotId()});
            oldRelations.insert(
                {ids.GetPersistentBuffer().GetNodeId(),
                 ids.GetPersistentBuffer().GetPDiskId(),
                 ids.GetPersistentBuffer().GetDDiskSlotId()});
        }

        THashSet<TDbsControllerDatabase::TInverseKey> newRelations;
        const auto& dbgInfo = args.DDisks.GetDirectBlockGroupsDDisks(dbgIndex);

        for (const auto& ids: dbgInfo.GetDDiskIds()) {
            newRelations.insert(
                {ids.GetDDisk().GetNodeId(),
                 ids.GetDDisk().GetPDiskId(),
                 ids.GetDDisk().GetDDiskSlotId()});
            newRelations.insert(
                {ids.GetPersistentBuffer().GetNodeId(),
                 ids.GetPersistentBuffer().GetPDiskId(),
                 ids.GetPersistentBuffer().GetDDiskSlotId()});
        }

        TVector<TDbsControllerDatabase::TInverseKey> common;
        for (const auto& key: newRelations) {
            if (oldRelations.contains(key)) {
                common.push_back(key);
            }
        }

        for (const auto& key: common) {
            oldRelations.erase(key);
            newRelations.erase(key);
        }

        for (const auto& key: oldRelations) {
            args.RelationsToRemove[key].insert(
                {args.PartitionTabletId, dbgIndex});
        }

        for (const auto& key: newRelations) {
            args.RelationsToAdd[key].push_back(
                {args.PartitionTabletId, dbgIndex});
        }
    }

    // Preload inverse records
    for (const auto& key: args.RelationsToAdd | std::views::keys) {
        NProto::TDDiskDirectBlockGroups record;
        if (!db.LoadInverseRecord(key, record)) {
            return false;
        }
        args.InverseRecordsPreloaded[key] = std::move(record);
    }

    for (const auto& key: args.RelationsToRemove | std::views::keys) {
        if (args.InverseRecordsPreloaded.contains(key)) {
            continue;
        }
        NProto::TDDiskDirectBlockGroups record;
        if (!db.LoadInverseRecord(key, record)) {
            return false;
        }
        args.InverseRecordsPreloaded[key] = std::move(record);
    }

    return true;
}

void TDbsControllerActor::ExecuteUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);

    for (ui64 dbgIndex = 0;
         dbgIndex < args.DDisks.DirectBlockGroupsDDisksSize();
         ++dbgIndex)
    {
        db.StoreDirectRecord(
            {args.PartitionTabletId, dbgIndex},
            args.DDisks.GetDirectBlockGroupsDDisks(dbgIndex));
    }

    for (const auto& [key, dbgs]: args.RelationsToRemove) {
        auto& rec = args.InverseRecordsPreloaded.at(key);
        for (auto& perPartitionRec: *rec.MutablePartitionDirectBlockGroups()) {
            if (perPartitionRec.GetPartitionTabletId() != std::get<0>(key)) {
                continue;
            }
            TVector<ui64> filteredDbgIndexes;
            for (const auto& dbg: dbgs) {
                if (!dbgs.contains(dbg)) {
                    filteredDbgIndexes.push_back(std::get<1>(dbg));
                }
            }
            perPartitionRec.MutableDirectBlockGroupIndex()->Assign(
                filteredDbgIndexes.begin(),
                filteredDbgIndexes.end());
        }
    }

    for (const auto& [key, dbgs]: args.RelationsToAdd) {
        auto& rec = args.InverseRecordsPreloaded.at(key);

        bool recordFound = false;
        for (auto& perPartitionRec: *rec.MutablePartitionDirectBlockGroups()) {
            if (perPartitionRec.GetPartitionTabletId() != std::get<0>(key)) {
                continue;
            }
            recordFound = true;
            for (const auto& dbg: dbgs) {
                perPartitionRec.MutableDirectBlockGroupIndex()->Add(
                    std::get<1>(dbg));
            }
        }
        if (!recordFound) {
            auto* perPartitionRec = rec.AddPartitionDirectBlockGroups();
            perPartitionRec->SetPartitionTabletId(std::get<0>(key));
            for (const auto& dbg: dbgs) {
                perPartitionRec->MutableDirectBlockGroupIndex()->Add(
                    std::get<1>(dbg));
            }
        }
    }

    for (const auto& [key, record]: args.InverseRecordsPreloaded) {
        db.StoreInverseRecord(key, record);
    }
}

void TDbsControllerActor::CompleteUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "UpdateDDiskMap persisted data for tablet %" PRIu64,
        args.PartitionTabletId);

    auto response =
        std::make_unique<TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>(
            MakeError(S_OK));

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TDbsControllerActor::PrepareRemoveTabletDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TRemoveTabletDDiskMap& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);

    if (!db.GetRecordKeysForTablet(
            args.PartitionTabletId,
            args.DirectKeys,
            args.InverseKeys))
    {
        return false;
    }

    // Preload inverse records
    for (const auto& key: args.InverseKeys) {
        NProto::TDDiskDirectBlockGroups record;
        if (!db.LoadInverseRecord(key, record)) {
            return false;
        }
        args.InverseRecordsPreloaded[key] = std::move(record);
    }

    return true;
}

void TDbsControllerActor::ExecuteRemoveTabletDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TRemoveTabletDDiskMap& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    TDbsControllerDatabase db(tx.DB);

    for (const auto& key: args.DirectKeys) {
        db.RemoveRecord(key);
    }

    for (auto& [key, record]: args.InverseRecordsPreloaded) {
        const size_t removed = RemoveIf(
            *record.MutablePartitionDirectBlockGroups(),
            [id = args.PartitionTabletId](const auto& x)
            { return x.GetPartitionTabletId() == id; });
        if (removed != 1) {
            LOG_ERROR_S(
                ctx,
                NKikimrServices::DBS_CONTROLLER,
                "Database integrity failure: expected 1 records for tablet, "
                "got "
                    << removed << "; partition tablet id = "
                    << args.PartitionTabletId << ", DDiskId = " << key);
        }
    }
}

void TDbsControllerActor::CompleteRemoveTabletDDiskMap(
    const NActors::TActorContext& ctx,
    TTxDbsController::TRemoveTabletDDiskMap& args)
{
    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "RemoveTabletDDiskMap cleared data for tablet %" PRIu64,
        args.PartitionTabletId);

    auto response = std::make_unique<
        TEvDbsControllerPrivate::TEvRemoveTabletDDiskMapResponse>(
        MakeError(S_OK));

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
