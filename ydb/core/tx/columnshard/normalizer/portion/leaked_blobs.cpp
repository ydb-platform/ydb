#include "leaked_blobs.h"

#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_accessor.h>
#include <ydb/core/tablet_flat/flat_database.h>
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
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("fuck", "after applying normalizer result")("blobs_to_delete", Leaks.size());

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
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("fuck", "before sending normalizer result")("bs_blob_ids", BSBlobIds.size());
        // BSBlobIds.clear();
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

TLeakedBlobsNormalizer::TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info)
    : TBase(info)
    , Channels(info.GetStorageInfo()->Channels)
    , DsGroupSelector(info.GetStorageInfo()) {
}

TConclusion<std::vector<INormalizerTask::TPtr>> TLeakedBlobsNormalizer::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    AFL_VERIFY(AppDataVerified().FeatureFlags.GetEnableWritePortionsOnInsert());
    NIceDb::TNiceDb db(txc.DB);
    // If a table is big, precharging OOMs the node.
   // const bool ready = (int)Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme()) &
    //                    (int)Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme()) &
    //                    (int)Schema::Precharge<Schema::IndexIndexes>(db, txc.DB.GetScheme()) &
    //                    (int)Schema::Precharge<Schema::BlobsToDeleteWT>(db, txc.DB.GetScheme());
    // if (!ready) {
    //     return TConclusionStatus::Fail("Not ready");
    // }


    NColumnShard::TTablesManager tablesManager(
        controller.GetStoragesManager(), controller.GetDataAccessorsManager(), std::make_shared<TPortionIndexStats>(), TabletId);

    if (!tablesManager.InitFromDB(db, nullptr)) {
        ACFL_TRACE("normalizer", "TPortionsNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    if (!tablesManager.HasPrimaryIndex()) {
        return std::vector<INormalizerTask::TPtr>{};
    }

    if ((StoppedOnPortion + StoppedOnColumns + StoppedOnIndexes + StoppedOnBlobsToDelete + StoppedOnIndexColumnsV2) % 10000 == 0) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("leaked_blobs normalizer", "stopped on some steps")("stopped on portion", StoppedOnPortion)("stopped on columns", StoppedOnColumns)("stopped on indexes", StoppedOnIndexes)("stopped on blobs to delete", StoppedOnBlobsToDelete)("stopped on index columns v2", StoppedOnIndexColumnsV2);
    }

    auto conclusion = LoadPortionBlobIds(tablesManager, db);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    if (ComparePortionsAndColumns && !IndexColumnsV2CountFinished) {
        if (!FullScanIndexColumnsV2(txc)) {
            return TConclusionStatus::Fail("Not ready: IndexColumnsV2");
        }
        IndexColumnsV2CountFinished = true;
        AFL_VERIFY(PortionIdsInPortions.size() == PortionIdsInIndexColumnsV2.size())("portion_ids_in_portions", PortionIdsInPortions.size())("portion_ids_in_index_columns_v2", PortionIdsInIndexColumnsV2.size());
    }

    return std::vector<INormalizerTask::TPtr>{ std::make_shared<TRemoveLeakedBlobsTask>(
        std::move(Channels), std::move(Result), TabletId, TabletActorId, DsGroupSelector) };
}

TConclusionStatus TLeakedBlobsNormalizer::LoadPortionBlobIds(
    const NColumnShard::TTablesManager& tablesManager, 
    NIceDb::TNiceDb& db
) {
    TDbWrapper wrapper(db.GetDatabase(), &DsGroupSelector);
    if (!PortionsFinished) {
        bool processPortionOk = true;
        if (!wrapper.LoadPortions(
                [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                    processPortionOk = ProcessPortion(std::move(portion), metaProto, tablesManager, wrapper, db);
                    return processPortionOk;
                },
                PortionsNextKey.first, 
                PortionsNextKey.second
            )) {
            if (processPortionOk) {
                StoppedOnPortion++;
            }
            return TConclusionStatus::Fail("Portions are not ready yet");
        }
        PortionsFinished = true;
    }
    if (!DeleteBlobsFinished) {
        THashSet<TUnifiedBlobId> blobsToDelete;
        auto rowset = db.Table<NColumnShard::Schema::BlobsToDeleteWT>().Select();
        if (!rowset.IsReady()) {
            StoppedOnBlobsToDelete++;
            return TConclusionStatus::Fail("Not ready: BlobsToDeleteWT");
        }
        while (!rowset.EndOfSet()) {
            const TString& blobIdStr = rowset.GetValue<NColumnShard::Schema::BlobsToDeleteWT::BlobId>();
            TString error;
            TUnifiedBlobId blobId = TUnifiedBlobId::ParseFromString(blobIdStr, &DsGroupSelector, error);
            AFL_VERIFY(blobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);
            blobsToDelete.emplace(blobId);
            if (!rowset.Next()) {
                StoppedOnBlobsToDelete++;
                return TConclusionStatus::Fail("Local table is not loaded: BlobsToDeleteWT");
            }
        }
        for (const auto& c : blobsToDelete) {
            Result.emplace(c.GetLogoBlobId());
        }
        DeleteBlobsFinished = true;
    }
    return TConclusionStatus::Success();
}

bool TLeakedBlobsNormalizer::FullScanIndexColumnsV2(
    NTabletFlatExecutor::TTransactionContext& txc
) {
    using TIndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;

    const TVector<NTable::TTag> tags = {
        TIndexColumnsV2::PathId::ColumnId,
        TIndexColumnsV2::PortionId::ColumnId
    };

    TVector<TRawTypeValue> key;
    NTable::ELookup lookup = NTable::ELookup::GreaterOrEqualThan;
    ui64 pathId = 0;
    ui64 portionId = 0;
    if (IndexColumnsV2LastKey) {
        pathId = IndexColumnsV2LastKey->first;
        portionId = IndexColumnsV2LastKey->second;
        key.emplace_back(&pathId, sizeof(pathId), NScheme::NTypeIds::Uint64);
        key.emplace_back(&portionId, sizeof(portionId), NScheme::NTypeIds::Uint64);
        lookup = NTable::ELookup::GreaterThan;
    }

    if (PrechargeIndexColumnV2) {
        const auto prechargeResult = txc.DB.Precharge(
            TIndexColumnsV2::TableId, key, {}, tags, 0 /* readFlags */, 100 /* itemsLimit */, 5_MB /* bytesLimit */);
        if (!prechargeResult.Ready) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("leaked_blobs normalizer", "IndexColumnsV2 precharge is not ready")
                ("count", IndexColumnsV2RowCount)
                ("precharged_items", prechargeResult.ItemsPrecharged)
                ("precharged_bytes", prechargeResult.BytesPrecharged);
            StoppedOnIndexColumnsV2++;
            return false;
        }
    }

    auto it = txc.DB.Iterate(TIndexColumnsV2::TableId, key, tags, lookup);
    while (it->Next(NTable::ENext::Data) == NTable::EReady::Data) {
        const auto dbKey = it->GetKey();
        AFL_VERIFY(dbKey.ColumnCount == 2);

        const ui64 currentPathId = dbKey.Columns[0].AsValue<ui64>();
        const ui64 currentPortionId = dbKey.Columns[1].AsValue<ui64>();

        AFL_VERIFY(PortionIdsInIndexColumnsV2.emplace(std::make_pair(TInternalPathId::FromRawValue(currentPathId), currentPortionId)).second);
        IndexColumnsV2LastKey = std::make_pair(currentPathId, currentPortionId);
        ++IndexColumnsV2RowCount;
        if (IndexColumnsV2RowCount % 100000 == 0) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("leaked_blobs normalizer", "read something from IndexColumnsV2")("count", IndexColumnsV2RowCount);
        }
    }

    if (it->Last() == NTable::EReady::Page) {
        StoppedOnIndexColumnsV2++;
        return false;
    }
    AFL_VERIFY(it->Last() == NTable::EReady::Gone)("last", static_cast<ui32>(it->Last()));

    IndexColumnsV2CountFinished = true;
    return true;
}

bool TLeakedBlobsNormalizer::ProcessPortion(
    const std::unique_ptr<TPortionInfoConstructor>& portion, 
    const NKikimrTxColumnShard::TIndexPortionMeta& metaProto,
    const NColumnShard::TTablesManager& tablesManager, 
    TDbWrapper& wrapper,
    NIceDb::TNiceDb& db 
) {
    // portion
    const auto& versionedIndex = tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex();
    const TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
    AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, DsGroupSelector));
    const auto pathId = portion->GetPathId();
    const ui64 portionId = portion->GetPortionIdVerified();

    // columns
    auto columnsRowset = db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(pathId.GetRawValue(), portionId).Select();
    if (!columnsRowset.IsReady()) {
        StoppedOnColumns++;
        return false;
    }
    AFL_VERIFY(!columnsRowset.EndOfSet())("error", "not found blobs for the portion")("path_id", pathId)("portion_id", portionId);
    NOlap::TColumnChunkLoadContextV2 columnsContext(columnsRowset, DsGroupSelector);

    // indexes
    std::vector<TIndexChunkLoadContext> indices;
    if (!wrapper.LoadIndexes(
            [&](const TInternalPathId /*pathId*/, const ui64 /*portionId*/, TIndexChunkLoadContext&& indexChunk) {
                indices.emplace_back(std::move(indexChunk));
            },
            pathId,
            portionId
        )) {
        StoppedOnIndexes++;
        return false;
    }

    // add blob ids to result
    std::shared_ptr<TPortionDataAccessor> accessor = TPortionAccessorConstructor::BuildForLoading(portion->Build(), std::move(columnsContext.CreateBuildInfo()), std::move(indices));
    THashMap<TString, THashSet<TUnifiedBlobId>> blobIdsByStorage;
    accessor->FillBlobIdsByStorage(blobIdsByStorage, tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex());
    auto it = blobIdsByStorage.find(NBlobOperations::TGlobal::DefaultStorageId);
    if (it != blobIdsByStorage.end()) {
        for (auto&& c : it->second) {
            Result.emplace(c.GetLogoBlobId());
        }
    }
    if (ComparePortionsAndColumns) {
        AFL_VERIFY(PortionIdsInPortions.emplace(std::make_pair(pathId, portionId)).second);
    }
    TotalPortions++;
    PortionsNextKey = std::make_pair(portion->GetPathId(), portionId + 1);
    if (TotalPortions % 10000 == 0) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("leaked_blobs normalizer", "processed some portions")("processed portions", TotalPortions)("collected blobs", Result.size())("stopped on portion", StoppedOnPortion)("stopped on columns", StoppedOnColumns)("stopped on indexes", StoppedOnIndexes);
    }
    return true;
}

}   // namespace NKikimr::NOlap

