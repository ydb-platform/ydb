#include "leaked_blobs.h"

#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_accessor.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NOlap {

class TRemoveLeakedBlobsActor: public TActorBootstrapped<TRemoveLeakedBlobsActor> {
private:
    TVector<TTabletChannelInfo> Channels;
    THashSet<TLogoBlobID> CSBlobIds;
    TActorId CSActorId;
    ui64 CSTabletId;
    TVector<TTabletChannelInfo>::iterator CurrentChannel;
    TVector<TTabletChannelInfo::THistoryEntry>::iterator CurrentGroup;

public:
    static constexpr NKikimrServices::TActivity::EType
    ActorActivityType() {
        return NKikimrServices::TActivity::ASYNC_DESTROYER;
    }

    TRemoveLeakedBlobsActor(TVector<TTabletChannelInfo>&& channels, THashSet<TLogoBlobID>&& csBlobIDs, TActorId csActorId, ui64 csTabletId)
        : Channels(std::move(channels))
        , CSBlobIds(std::move(csBlobIDs))
        , CSActorId(csActorId)
        , CSTabletId(csTabletId) {
    }

    void Bootstrap(const TActorContext& ctx) {
        CurrentChannel = Channels.begin();

        ProcessNextChannel(ctx);

        Become(&TThis::StateWait);
    }

    void ProcessNextGroup(const TActorContext& ctx) {
        for (; CurrentGroup != CurrentChannel->History.end(); ++CurrentGroup)
            ;

        THashSet<TLogoBlobID> AliveBlobs;

        if (CurrentGroup == CurrentChannel->History.end()) {
            ProcessNextChannel(ctx);
            return;
        }

        TLogoBlobID from(CSTabletId, 0, 0, CurrentChannel->Channel, 0, 0);
        TLogoBlobID to(CSTabletId, Max<ui32>(), Max<ui32>(), CurrentChannel->Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
        auto request = MakeHolder<TEvBlobStorage::TEvRange>(CSTabletId, from, to, false, TInstant::Max(), true);
        SendToBSProxy(ctx, CurrentGroup->GroupID, request.Release());
    }

    void ProcessNextChannel(const TActorContext& ctx) {
        for (; CurrentChannel != Channels.end() && CurrentChannel->Channel < NKeyValue::BLOB_CHANNEL; ++CurrentChannel)
            ;

        if (CurrentChannel == Channels.end()) {
            // TODO: notify completion
            PassAway();
            return;
        }

        CurrentGroup = CurrentChannel->History.begin();

        ProcessNextGroup(ctx);
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr& ev, const TActorContext& ctx) {
        TEvBlobStorage::TEvRangeResult* msg = ev->Get();
        AFL_VERIFY(msg->Status == NKikimrProto::OK);

        TVector<TLogoBlobID> leakedBlobs;

        for (auto& resp : msg->Responses) {
            if (!CSBlobIds.contains(resp.Id)) {
                leakedBlobs.push_back(resp.Id);
            }
        }

        if (leakedBlobs.empty()) {
            ProcessNextGroup(ctx);
            return;
        }

        auto request = MakeHolder<TEvBlobStorage::TEvCollectGarbage>(CSTabletId, RecordGeneration,
            CurrentChannel->Channel, collect, collectGeneration,
            collectStep, nullptr, leakedBlobs, TInstant::Max());

        SendToBSProxy(ctx, CurrentGroup->GroupID, request.Release());
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx) {
        ProcessNextGroup(ctx);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvRangeResult, Handle);
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            default:
                break;
        }
    }
};

TLeakedBlobsNormalizer::TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info)
    : Channels(info.GetStorageInfo()->Channels)
    , TabletId(info.GetStorageInfo()->TabletID)
    , ActorId(info.GetActorId())
    , DsGroupSelector(info.GetStorageInfo()) {
}

TConclusion<std::vector<INormalizerTask::TPtr>> TLeakedBlobsNormalizer::DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;

    NIceDb::TNiceDb db(txc.DB);
    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    NColumnShard::TTablesManager tablesManager(controller.GetStoragesManager(), std::make_shared<NDataAccessorControl::TLocalManager>(nullptr), 0);

    if (!tablesManager.InitFromDB(db)) {
        ACFL_TRACE("normalizer", "TPortionsNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    if (!tablesManager.HasPrimaryIndex()) {
        return std::vector<INormalizerTask::TPtr>{};
    }

    THashSet<TLogoBlobID> csBlobIDs;
    auto conclusion = LoadPortionBlobIds(tablesManager, db, csBlobIDs);
    if (conclusion.IsFail()) {
        return conclusion;
    }

    NActors::TActivationContext::Register(new TRemoveLeakedBlobsActor(std::move(Channels), std::move(csBlobIDs), ActorId, TabletId));

    // TODO: create waiter task
    std::vector<INormalizerTask::TPtr> result;

    return result;
}

TConclusionStatus TLeakedBlobsNormalizer::LoadPortionBlobIds(
    const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashSet<TLogoBlobID>& result) {
    TDbWrapper wrapper(db.GetDatabase(), nullptr);
    if (!wrapper.LoadPortions({}, [&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
            const TIndexInfo& indexInfo =
                portion.GetSchema(tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex())->GetIndexInfo();
            AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, DsGroupSelector));
            TPortionAccessorConstructor constructor(std::move(portion));
            TPortionDataAccessor accessor = constructor.Build(false);

            THashMap<TString, THashSet<TUnifiedBlobId>> blobIds;
            accessor.FillBlobIdsByStorage(blobIds, indexInfo);

            for (auto& id : blobIds[NBlobOperations::TGlobal::DefaultStorageId]) {
                result.insert(id.GetLogoBlobId());
            }
        })) {
        return TConclusionStatus::Fail("repeated read db");
    }
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap
