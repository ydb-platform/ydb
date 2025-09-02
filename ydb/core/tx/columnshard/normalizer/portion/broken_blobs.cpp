#include "broken_blobs.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <util/string/vector.h>

namespace NKikimr::NOlap::NNormalizer::NBrokenBlobs {

class TNormalizerResult: public INormalizerChanges {
    THashMap<ui64, TPortionDataAccessor> BrokenPortions;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;

public:
    TNormalizerResult(THashMap<ui64, TPortionDataAccessor>&& portions, const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>>& schemas)
        : BrokenPortions(std::move(portions))
        , Schemas(schemas) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normController) const override {
        NOlap::TBlobManagerDb blobManagerDb(txc.DB);

        TDbWrapper db(txc.DB, nullptr);
        for (auto&& [_, portionInfo] : BrokenPortions) {
            auto schema = Schemas->FindPtr(portionInfo.GetPortionInfo().GetPortionId());
            AFL_VERIFY(!!schema)("portion_id", portionInfo.GetPortionInfo().GetPortionId());
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "portion_removed_as_broken")(
                "portion_id", portionInfo.GetPortionInfo().GetAddress().DebugString());
            auto copy = portionInfo.GetPortionInfo().MakeCopy();
            copy->SetRemoveSnapshot(TSnapshot(1, 1));
            db.WritePortion({}, *copy);
        }
        if (BrokenPortions.size()) {
            NIceDb::TNiceDb db(txc.DB);
            normController.AddNormalizerEvent(db, "REMOVE_PORTIONS", DebugString());
        }
        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
    }

    ui64 GetSize() const override {
        return BrokenPortions.size();
    }

    TString DebugString() const override {
        TStringBuilder sb;
        ui64 recordsCount = 0;
        sb << "path_ids=[";
        for (auto&& [_, p] : BrokenPortions) {
            sb << p.GetPortionInfo().GetPathId() << ",";
            recordsCount += p.GetPortionInfo().GetRecordsCount();
        }
        sb << "]";
        sb << ";records_count=" << recordsCount;
        return sb;
    }
};

class TReadTask: public NOlap::NBlobOperations::NRead::ITask {
private:
    using TBase = NOlap::NBlobOperations::NRead::ITask;
    TNormalizationContext NormContext;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
    THashMap<TString, THashMap<TUnifiedBlobId, TPortionDataAccessor>> PortionsByBlobId;
    THashMap<ui64, TPortionDataAccessor> BrokenPortions;

public:
    TReadTask(const TNormalizationContext& nCtx, const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions,
        std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas,
        THashMap<TString, THashMap<TUnifiedBlobId, TPortionDataAccessor>>&& portionsByBlobId)
        : TBase(actions, "CS::NORMALIZER")
        , NormContext(nCtx)
        , Schemas(std::move(schemas))
        , PortionsByBlobId(portionsByBlobId) {
    }

protected:
    virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) override {
        NBlobOperations::NRead::TCompositeReadBlobs blobs = ExtractBlobsData();

        THashSet<ui64> readyPortions;
        for (auto&& i : BrokenPortions) {
            readyPortions.emplace(i.first);
        }
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("event", "broken_data_found");
        for (auto&& i : PortionsByBlobId) {
            for (auto&& [_, p] : i.second) {
                if (readyPortions.emplace(p.GetPortionInfo().GetPortionId()).second) {
                    auto it = Schemas->find(p.GetPortionInfo().GetPortionId());
                    AFL_VERIFY(it != Schemas->end());
                    auto restored = TReadPortionInfoWithBlobs::RestorePortion(p, blobs, it->second->GetIndexInfo());
                    auto restoredBatch = restored.RestoreBatch(*it->second, *it->second, {});
                    if (restoredBatch.IsFail()) {
                        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("portion", p.DebugString())("fail", restoredBatch.GetErrorMessage());
                        BrokenPortions.emplace(p.GetPortionInfo().GetPortionId(), p);
                    }
                }
            }
        }

        auto changes = std::make_shared<TNormalizerResult>(std::move(BrokenPortions), Schemas);
        TActorContext::AsActorContext().Send(
            NormContext.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(changes));
    }

    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("blob_id", range.GetBlobId().ToStringNew())(
            "error", status.GetErrorMessage())("status", status.GetStatus())("event", "broken_blob_found")("storage_id", storageId);
        AFL_VERIFY(status.GetStatus() == NKikimrProto::EReplyStatus::NODATA)("status", status.GetStatus());
        auto itStorage = PortionsByBlobId.find(storageId);
        AFL_VERIFY(itStorage != PortionsByBlobId.end());
        auto it = itStorage->second.find(range.GetBlobId());
        AFL_VERIFY(it != itStorage->second.end());
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("portion", it->second.GetPortionInfo().GetAddress().DebugString());
        BrokenPortions.emplace(it->second.GetPortionInfo().GetPortionId(), it->second);
        return true;
    }

public:
    using TBase::TBase;
};

class TBrokenBlobsTask: public INormalizerTask {
    THashMap<TString, THashSet<TBlobRange>> Blobs;
    THashMap<TString, THashMap<TUnifiedBlobId, TPortionDataAccessor>> PortionsByBlobId;
    const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;

public:
    TBrokenBlobsTask(THashMap<TString, THashSet<TBlobRange>>&& blobs,
        THashMap<TString, THashMap<TUnifiedBlobId, TPortionDataAccessor>>&& portionsByBlobId,
        const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>>& schemas)
        : Blobs(std::move(blobs))
        , PortionsByBlobId(portionsByBlobId)
        , Schemas(schemas) {
    }

    void Start(const TNormalizationController& controller, const TNormalizationContext& nCtx) override {
        ui64 memSize = 0;
        std::vector<std::shared_ptr<IBlobsReadingAction>> actions;
        for (auto&& [storageId, data] : Blobs) {
            auto op = controller.GetStoragesManager()->GetOperatorVerified(storageId);
            actions.emplace_back(op->StartReadingAction(NBlobOperations::EConsumer::NORMALIZER));
            for (auto&& b : data) {
                memSize += b.GetBlobSize();
                actions.back()->AddRange(b);
            }
        }
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            nCtx.GetResourceSubscribeActor(), std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                                                  std::make_shared<TReadTask>(nCtx, actions, Schemas, std::move(PortionsByBlobId)), 0, memSize,
                                                  "CS::NORMALIZER", controller.GetTaskSubscription()));
    }
};

bool TNormalizer::CheckPortion(const NColumnShard::TTablesManager& /*tablesManager*/, const TPortionDataAccessor& /*portionInfo*/) const {
    return false;
}

INormalizerTask::TPtr TNormalizer::BuildTask(
    std::vector<TPortionDataAccessor>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const {
    THashMap<TString, THashSet<TBlobRange>> blobIds;
    THashMap<TString, THashMap<TUnifiedBlobId, TPortionDataAccessor>> portionByBlobId;
    for (auto&& portion : portions) {
        auto schemaPtr = schemas->FindPtr(portion.GetPortionInfo().GetPortionId());
        THashMap<TString, THashSet<TBlobRange>> blobsByStorage;
        portion.FillBlobRangesByStorage(blobsByStorage, schemaPtr->get()->GetIndexInfo());
        if (blobsByStorage.size() > 1 || !blobsByStorage.contains(NBlobOperations::TGlobal::DefaultStorageId)) {
            continue;
        }
        for (auto&& i : blobsByStorage) {
            AFL_VERIFY(i.first == NBlobOperations::TGlobal::DefaultStorageId)("details", "Invalid storage for normalizer")(
                "storage_id", i.first);
            for (auto&& b : i.second) {
                portionByBlobId[i.first].emplace(b.BlobId, portion);
                AFL_VERIFY(blobIds[i.first].emplace(b).second);
            }
        }
    }
    if (blobIds.empty()) {
        return nullptr;
    }
    return std::make_shared<TBrokenBlobsTask>(std::move(blobIds), std::move(portionByBlobId), schemas);
}

TConclusion<bool> TNormalizer::DoInitImpl(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext&) {
    return true;
}

}   // namespace NKikimr::NOlap::NNormalizer::NBrokenBlobs
