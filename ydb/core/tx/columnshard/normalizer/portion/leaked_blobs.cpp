#include "leaked_blobs.h"

#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_accessor.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <ydb/library/actors/core/actor.h>

#include <util/string/vector.h>

namespace NKikimr::NOlap {

class TLeakedBlobsNormalizerChanges: public INormalizerChanges {
private:
    THashSet<TLogoBlobID> Leaks;
    const ui64 TabletId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
    ui64 LeakeadBlobsSize;

public:
    TLeakedBlobsNormalizerChanges(THashSet<TLogoBlobID>&& leaks, const ui64 tabletId, NColumnShard::TBlobGroupSelector dsGroupSelector)
        : Leaks(std::move(leaks))
        , TabletId(tabletId)
        , DsGroupSelector(dsGroupSelector) {
        LeakeadBlobsSize = 0;
        for (const auto& blob : Leaks) {
            LeakeadBlobsSize += blob.BlobSize();
        }
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /*normController*/) const override {
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : Leaks) {
            TUnifiedBlobId blobId(DsGroupSelector.GetGroup(i), i);
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("normalizer", "leaked_blobs")("blob_id", blobId.ToStringLegacy());
            db.Table<NColumnShard::Schema::BlobsToDeleteWT>().Key(blobId.ToStringLegacy(), TabletId).Update();
        }
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("normalizer", "leaked_blobs")("removed_blobs", Leaks.size());

        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
    }

    ui64 GetSize() const override {
        return Leaks.size();
    }

    TString DebugString() const override {
        TStringBuilder sb;
        sb << "tablet=" << TabletId;
        sb << ";leaked_blob_count=" << Leaks.size();
        sb << ";leaked_blobs_size=" << LeakeadBlobsSize;
        auto blobSampleEnd = Leaks.begin();
        for (ui64 i = 0; i < 10 && blobSampleEnd != Leaks.end(); ++i, ++blobSampleEnd) {
        }
        sb << ";leaked_blobs=[" << JoinStrings(Leaks.begin(), blobSampleEnd, ",") << "]";
        return sb;
    }
};

class TRemoveLeakedBlobsActor: public TActorBootstrapped<TRemoveLeakedBlobsActor> {
private:
    TVector<TTabletChannelInfo> Channels;
    THashSet<TLogoBlobID> CSBlobIds;
    THashSet<TLogoBlobID> BSBlobIds;
    TActorId CSActorId;
    ui64 CSTabletId;
    i32 WaitingCount = 0;
    THashSet<ui32> WaitingRequests;
    NColumnShard::TBlobGroupSelector DsGroupSelector;

    void CheckFinish() {
        if (WaitingCount) {
            return;
        }
        AFL_VERIFY(CSBlobIds.size() <= BSBlobIds.size())("cs", CSBlobIds.size())("bs", BSBlobIds.size())(
                                         "error", "have to use broken blobs repair");
        for (auto&& i : CSBlobIds) {
            AFL_VERIFY(BSBlobIds.erase(i))("error", "have to use broken blobs repair")("blob_id", i);
        }
        TActorContext::AsActorContext().Send(
            CSActorId, std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(
                           std::make_shared<TLeakedBlobsNormalizerChanges>(std::move(BSBlobIds), CSTabletId, DsGroupSelector)));
        PassAway();
    }

public:
    TRemoveLeakedBlobsActor(TVector<TTabletChannelInfo>&& channels, THashSet<TLogoBlobID>&& csBlobIDs, TActorId csActorId, ui64 csTabletId,
        const NColumnShard::TBlobGroupSelector& dsGroupSelector)
        : Channels(std::move(channels))
        , CSBlobIds(std::move(csBlobIDs))
        , CSActorId(csActorId)
        , CSTabletId(csTabletId)
        , DsGroupSelector(dsGroupSelector) {
    }

    void Bootstrap(const TActorContext& /* ctx */) {
        WaitingCount = 0;

        for (auto it = Channels.begin(); it != Channels.end(); ++it) {
            if (it->Channel < 2) {
                continue;
            }
            for (auto&& i : it->History) {
                TLogoBlobID from(CSTabletId, 0, 0, it->Channel, 0, 0);
                TLogoBlobID to(CSTabletId, Max<ui32>(), Max<ui32>(), it->Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
                auto request = MakeHolder<TEvBlobStorage::TEvRange>(CSTabletId, from, to, false, TInstant::Max(), true);
                SendToBSProxy(SelfId(), i.GroupID, request.Release(), ++WaitingCount);
                WaitingRequests.emplace(WaitingCount);
            }
        }
        CheckFinish();

        Become(&TThis::StateWait);
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr& ev, const TActorContext& /*ctx*/) {
        TEvBlobStorage::TEvRangeResult* msg = ev->Get();
        AFL_VERIFY(msg->Status == NKikimrProto::OK)("status", msg->Status)("error", msg->ErrorReason);
        AFL_VERIFY(--WaitingCount >= 0);
        AFL_VERIFY(WaitingRequests.erase(ev->Cookie));
        for (auto& resp : msg->Responses) {
            AFL_VERIFY(!resp.Buffer);
            BSBlobIds.emplace(resp.Id);
        }
        CheckFinish();
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvRangeResult, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

TLeakedBlobsNormalizer::TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info)
    : TBase(info)
    , Channels(info.GetStorageInfo()->Channels)
    , DsGroupSelector(info.GetStorageInfo()) {
}

class TRemoveLeakedBlobsTask: public INormalizerTask {
    TVector<TTabletChannelInfo> Channels;
    THashSet<TLogoBlobID> CSBlobIDs;
    ui64 TabletId;
    TActorId ActorId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;

public:
    TRemoveLeakedBlobsTask(TVector<TTabletChannelInfo>&& channels, THashSet<TLogoBlobID>&& csBlobIDs, ui64 tabletId, TActorId actorId,
        const NColumnShard::TBlobGroupSelector& dsGroupSelector)
        : Channels(std::move(channels))
        , CSBlobIDs(std::move(csBlobIDs))
        , TabletId(tabletId)
        , ActorId(actorId)
        , DsGroupSelector(dsGroupSelector) {
    }
    void Start(const TNormalizationController& /*controller*/, const TNormalizationContext& /*nCtx*/) override {
        NActors::TActivationContext::Register(
            new TRemoveLeakedBlobsActor(std::move(Channels), std::move(CSBlobIDs), ActorId, TabletId, DsGroupSelector));
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TLeakedBlobsNormalizer::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    AFL_VERIFY(AppDataVerified().FeatureFlags.GetEnableWritePortionsOnInsert());
    NIceDb::TNiceDb db(txc.DB);
    const bool ready = (int)Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme()) &
                       (int)Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme()) &
                       (int)Schema::Precharge<Schema::IndexIndexes>(db, txc.DB.GetScheme()) &
                       (int)Schema::Precharge<Schema::BlobsToDeleteWT>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    NColumnShard::TTablesManager tablesManager(controller.GetStoragesManager(), controller.GetDataAccessorsManager(),
        std::make_shared<TPortionIndexStats>(), TabletId);

    if (!tablesManager.InitFromDB(db, nullptr)) {
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

    return std::vector<INormalizerTask::TPtr>{ std::make_shared<TRemoveLeakedBlobsTask>(
        std::move(Channels), std::move(csBlobIDs), TabletId, TabletActorId, DsGroupSelector) };
}

TConclusionStatus TLeakedBlobsNormalizer::LoadPortionBlobIds(
    const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashSet<TLogoBlobID>& result) {
    TDbWrapper wrapper(db.GetDatabase(), nullptr);
    if (Portions.empty()) {
        THashMap<ui64, std::unique_ptr<TPortionInfoConstructor>> portionsLocal;
        if (!wrapper.LoadPortions(
                {}, [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                    const TIndexInfo& indexInfo =
                        portion->GetSchema(tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex())->GetIndexInfo();
                    AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, DsGroupSelector));
                    const ui64 portionId = portion->GetPortionIdVerified();
                    AFL_VERIFY(portionsLocal.emplace(portionId, std::move(portion)).second);
                })) {
            return TConclusionStatus::Fail("repeated read db");
        }
        Portions = std::move(portionsLocal);
    }
    if (Records.empty()) {
        THashMap<ui64, TColumnChunkLoadContextV2::TBuildInfo> recordsLocal;
        if (!wrapper.LoadColumns(std::nullopt, [&](TColumnChunkLoadContextV2&& chunk) {
                const ui64 portionId = chunk.GetPortionId();
                AFL_VERIFY(recordsLocal.emplace(portionId, chunk.CreateBuildInfo()).second);
            })) {
            return TConclusionStatus::Fail("repeated read db");
        }
        Records = std::move(recordsLocal);
    }
    if (Indexes.empty()) {
        THashMap<ui64, std::vector<TIndexChunkLoadContext>> indexesLocal;
        if (!wrapper.LoadIndexes(
                std::nullopt, [&](const TInternalPathId /*pathId*/, const ui64 /*portionId*/, TIndexChunkLoadContext&& indexChunk) {
                    const ui64 portionId = indexChunk.GetPortionId();
                    indexesLocal[portionId].emplace_back(std::move(indexChunk));
                })) {
            return TConclusionStatus::Fail("repeated read db");
        }
        Indexes = std::move(indexesLocal);
    }
    if (BlobsToDelete.empty()) {
        THashSet<TUnifiedBlobId> blobsToDelete;
        auto rowset = db.Table<NColumnShard::Schema::BlobsToDeleteWT>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready: BlobsToDeleteWT");
        }
        while (!rowset.EndOfSet()) {
            const TString& blobIdStr = rowset.GetValue<NColumnShard::Schema::BlobsToDeleteWT::BlobId>();
            TString error;
            TUnifiedBlobId blobId = TUnifiedBlobId::ParseFromString(blobIdStr, &DsGroupSelector, error);
            AFL_VERIFY(blobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);
            blobsToDelete.emplace(blobId);
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Local table is not loaded: BlobsToDeleteWT");
            }
        }
        BlobsToDelete = std::move(blobsToDelete);
    }
    AFL_VERIFY(Portions.size() == Records.size())("portions", Portions.size())("records", Records.size());
    THashSet<TLogoBlobID> resultLocal;
    for (auto&& i : Portions) {
        auto itRecords = Records.find(i.first);
        AFL_VERIFY(itRecords != Records.end());
        auto itIndexes = Indexes.find(i.first);
        std::vector<TIndexChunkLoadContext> indexes;
        if (itIndexes != Indexes.end()) {
            indexes = std::move(itIndexes->second);
        }
        TPortionDataAccessor accessor =
            TPortionAccessorConstructor::BuildForLoading(i.second->Build(), std::move(itRecords->second), std::move(indexes));
        THashMap<TString, THashSet<TUnifiedBlobId>> blobIdsByStorage;
        accessor.FillBlobIdsByStorage(blobIdsByStorage, tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex());
        auto it = blobIdsByStorage.find(NBlobOperations::TGlobal::DefaultStorageId);
        if (it == blobIdsByStorage.end()) {
            continue;
        }
        for (auto&& c : it->second) {
            resultLocal.emplace(c.GetLogoBlobId());
        }
        for (const auto& c : BlobsToDelete) {
            resultLocal.emplace(c.GetLogoBlobId());
        }
    }
    std::swap(resultLocal, result);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap
