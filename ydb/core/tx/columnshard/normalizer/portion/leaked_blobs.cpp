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
    THashSet<TLogoBlobID> AliveBlobs;
    ui64 RepliesInFlight = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ASYNC_DESTROYER;
    }

    TRemoveLeakedBlobsActor(TVector<TTabletChannelInfo>&& channels, THashSet<TLogoBlobID>&& csBlobIDs, TActorId csActorId, ui64 csTabletId)
        : Channels(std::move(channels))
        , CSBlobIds(std::move(csBlobIDs))
        , CSActorId(csActorId)
        , CSTabletId(csTabletId) {
    }

    void Bootstrap(const TActorContext& ctx) {
        for (const auto& channel : Channels) {
            if (channel.Channel >= NKeyValue::BLOB_CHANNEL) {
                for (const auto& history : channel.History) {
                    TLogoBlobID from(CSTabletId, 0, 0, channel.Channel, 0, 0);
                    TLogoBlobID to(CSTabletId, Max<ui32>(), Max<ui32>(), channel.Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
                    auto request = MakeHolder<TEvBlobStorage::TEvRange>(CSTabletId, from, to, false, TInstant::Max(), true);
                    SendToBSProxy(ctx, history.GroupID, request.Release());
                    RepliesInFlight++;
                }
            }
        }
        Become(&TThis::StateWait);
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr& ev, const TActorContext& /*ctx*/) {
        TEvBlobStorage::TEvRangeResult* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("reason", "request to blob storage returned not ok");
            return;
        }

        for (const auto& resp : msg->Responses) {
            AliveBlobs.insert(resp.Id);
        }

        RepliesInFlight--;

        if (RepliesInFlight == 0) {
            SendLeakedBlobs();
        }
    }

    void SendLeakedBlobs() {
        std::vector<TLogoBlobID> leakedBlobs;
        std::vector<TLogoBlobID> curruptedBlobs;

        for (auto& id : AliveBlobs) {
            if (!CSBlobIds.contains(id)) {
                leakedBlobs.push_back(id);
            }
        }

        // Send EvNormalizerResult with leakedBlobs

        for (auto& id : CSBlobIds) {
            if (!AliveBlobs.contains(id)) {
                curruptedBlobs.push_back(id);
            }
        }

        AFL_VERIFY(curruptedBlobs.empty());
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvRangeResult, Handle);
            default:
                break;
        }
    }
};

class TRemoveLeakedBlobsTask: public INormalizerTask {
private:
    using TBase = NOlap::NBlobOperations::NRead::ITask;
    TVector<TTabletChannelInfo> Channels;
    std::vector<TPortionDataAccessor> Portions;
    ui64 TabletId;
    TActorId ActorId;

public:
    TRemoveLeakedBlobsTask(TVector<TTabletChannelInfo>&& channels, std::vector<TPortionDataAccessor>&& portions, ui64 tabletId, TActorId actorId)
        : Channels(std::move(channels))
        , Portions(std::move(portions))
        , TabletId(tabletId)
        , ActorId(actorId) {
    }

public:
    virtual void Start(const TNormalizationController& /*controller*/, const TNormalizationContext& /*nCtx*/) {
        TlsActivationContext->ExecutorThread.RegisterActor(new TRemoveLeakedBlobsActor(std::move(Channels), std::move(Portions), ActorId, TabletId));
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
    auto conclusion = InitPortions(tablesManager, db, csBlobIDs);
    if (conclusion.IsFail()) {
        return conclusion;
    }

    TlsActivationContext->ExecutorThread.RegisterActor(new TRemoveLeakedBlobsActor(std::move(Channels), std::move(csBlobIDs), ActorId, TabletId));
}

TConclusionStatus TLeakedBlobsNormalizer::InitPortions(
    const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashSet<TLogoBlobID>& blobIds) {
    // найти все TLogoBlobID которые достижимы из ColumnShard

    // TDbWrapper wrapper(db.GetDatabase(), nullptr);
    // if (!wrapper.LoadPortions({}, [&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
    //         const TIndexInfo& indexInfo =
    //             portion.GetSchema(tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex())->GetIndexInfo();
    //         AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, DsGroupSelector));
    //         const ui64 portionId = portion.GetPortionIdVerified();
    //         AFL_VERIFY(constructors.push_back(TPortionAccessorConstructor(std::move(portion)).RestoreBlobRange(const TBlobRangeLink16 &linkRange)).second);
    //     })) {
    //     return TConclusionStatus::Fail("repeated read db");
    // }
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap
