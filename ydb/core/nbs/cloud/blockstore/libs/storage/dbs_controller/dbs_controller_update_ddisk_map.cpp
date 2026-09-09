#include "dbs_controller_actor.h"
#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

template <typename TRepeatedField, typename TPred>
    requires requires(TRepeatedField& f, TPred pred)
{
    {
        f.SwapElements(0, 1)
    };
    {
        f.RemoveLast()
    };
    {
        pred(f.Get(0))
    } -> std::same_as<bool>;
}

static size_t RemoveIf(TRepeatedField& repeatedField, TPred predicate)
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

template <typename TDDisks>
static TVector<TDbsControllerDatabase::TInverseKey> ListSortedDDiskIds(
    const TDDisks& ddisks)
{
    TVector<TDbsControllerDatabase::TInverseKey> result;
    for (const auto& ids: ddisks) {
        result.push_back(
            {ids.GetDDisk().GetNodeId(),
             ids.GetDDisk().GetPDiskId(),
             ids.GetDDisk().GetDDiskSlotId()});
        result.push_back(
            {ids.GetPersistentBuffer().GetNodeId(),
             ids.GetPersistentBuffer().GetPDiskId(),
             ids.GetPersistentBuffer().GetDDiskSlotId()});
    }
    std::ranges::sort(result);
    return result;
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

    TDbsControllerDatabase db(tx.DB);

    TVector<TDbsControllerDatabase::TInverseKey> relationsToModify;

    struct TDiff
    {
        THashSet<ui64> Added;
        THashSet<ui64> Removed;
    };

    THashMap<TDbsControllerDatabase::TInverseKey, TDiff> diffs;

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

        auto oldDDiskIds = ListSortedDDiskIds(directRecord.GetDDiskIds());
        auto newDDiskIds = ListSortedDDiskIds(
            args.DDisks.GetDirectBlockGroupsDDisks(dbgIndex).GetDDiskIds());

        TVector<TDbsControllerDatabase::TInverseKey> relationsToRemove;
        std::ranges::set_difference(
            oldDDiskIds,
            newDDiskIds,
            std::back_inserter(relationsToRemove));

        TVector<TDbsControllerDatabase::TInverseKey> relationsToAdd;
        std::ranges::set_difference(
            newDDiskIds,
            oldDDiskIds,
            std::back_inserter(relationsToAdd));

        std::ranges::set_union(
            newDDiskIds,
            oldDDiskIds,
            std::back_inserter(relationsToModify));

        for (const auto& key: relationsToRemove) {
            diffs[key].Removed.insert(dbgIndex);
        }

        for (const auto& key: relationsToAdd) {
            diffs[key].Added.insert(dbgIndex);
        }
    }

    for (const auto& key: relationsToModify) {
        auto& record = args.ModifiedInverseRecords[key];
        if (!db.LoadInverseRecord(key, record)) {
            return false;
        }

        const auto& diff = diffs.at(key);

        decltype(record.MutablePartitionDirectBlockGroups(0)
                     ->MutableDirectBlockGroupIndex()) dbgListPtr = nullptr;

        for (auto& partitionRec: *record.MutablePartitionDirectBlockGroups()) {
            if (partitionRec.GetPartitionTabletId() == args.PartitionTabletId) {
                dbgListPtr = partitionRec.MutableDirectBlockGroupIndex();
                break;
            }
        }
        if (dbgListPtr == nullptr) {
            auto* newRec = record.AddPartitionDirectBlockGroups();
            newRec->SetPartitionTabletId(args.PartitionTabletId);
            dbgListPtr = newRec->MutableDirectBlockGroupIndex();
        }

        RemoveIf(
            *dbgListPtr,
            [&diff](const ui64 index) { return diff.Removed.contains(index); });
        dbgListPtr->Add(diff.Added.begin(), diff.Added.end());
    }

    return true;
}

void TDbsControllerActor::ExecuteUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    Y_UNUSED(ctx);

    TDbsControllerDatabase db(tx.DB);

    for (ui64 dbgIndex = 0;
         dbgIndex < args.DDisks.DirectBlockGroupsDDisksSize();
         ++dbgIndex)
    {
        db.StoreDirectRecord(
            {args.PartitionTabletId, dbgIndex},
            args.DDisks.GetDirectBlockGroupsDDisks(dbgIndex));
    }

    for (const auto& [key, record]: args.ModifiedInverseRecords) {
        db.StoreInverseRecord(key, record);
    }
}

void TDbsControllerActor::CompleteUpdateDDiskMap(
    const NActors::TActorContext& ctx,
    TTxDbsController::TUpdateDDiskMap& args)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "UpdateDDiskMap persisted data for tablet "
            << args.PartitionTabletId << ": "
            << args.ModifiedInverseRecords.size()
            << " inverse records updated");

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
        auto& record = args.ModifiedInverseRecords[key];
        if (!db.LoadInverseRecord(key, record)) {
            return false;
        }

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

    return true;
}

void TDbsControllerActor::ExecuteRemoveTabletDDiskMap(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxDbsController::TRemoveTabletDDiskMap& args)
{
    Y_UNUSED(ctx);

    TDbsControllerDatabase db(tx.DB);

    for (const auto& key: args.DirectKeys) {
        db.RemoveRecord(key);
    }

    for (const auto& [key, record]: args.ModifiedInverseRecords) {
        db.StoreInverseRecord(key, record);
    }
}

void TDbsControllerActor::CompleteRemoveTabletDDiskMap(
    const NActors::TActorContext& ctx,
    TTxDbsController::TRemoveTabletDDiskMap& args)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "RemoveTabletDDiskMap cleared data for tablet "
            << args.PartitionTabletId << ": "
            << args.ModifiedInverseRecords.size()
            << " inverse records updated");

    auto response = std::make_unique<
        TEvDbsControllerPrivate::TEvRemoveTabletDDiskMapResponse>(
        MakeError(S_OK));

    Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
