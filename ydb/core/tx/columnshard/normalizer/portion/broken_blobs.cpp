#include "broken_blobs.h"

#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>


namespace NKikimr::NOlap::NNormalizer::NBrokenBlobs {

class TNormalizerResult : public INormalizerChanges {
    THashMap<ui64, std::shared_ptr<TPortionInfo>> BrokenPortions;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
public:
    TNormalizerResult(THashMap<ui64, std::shared_ptr<TPortionInfo>>&& portions, const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>>& schemas)
        : BrokenPortions(std::move(portions))
        , Schemas(schemas)
    {}

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normController) const override {
        NOlap::TBlobManagerDb blobManagerDb(txc.DB);
        
        TDbWrapper db(txc.DB, nullptr);
        for (auto&& [_, portionInfo] : BrokenPortions) {
            auto schema = Schemas->FindPtr(portionInfo->GetPortionId());
            AFL_VERIFY(!!schema)("portion_id", portionInfo->GetPortionId());
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "portion_removed_as_broken")("portion_id", portionInfo->GetAddress().DebugString());
            portionInfo->SetRemoveSnapshot(TSnapshot(1, 1));
            portionInfo->SaveToDatabase(db, (*schema)->GetIndexInfo().GetPKFirstColumnId(), false);
        }
        if (BrokenPortions.size()) {
            TStringBuilder sb;
            ui64 recordsCount = 0;
            sb << "path_ids:[";
            for (auto&& [_, p] : BrokenPortions) {
                sb << p->GetPathId() << ",";
                recordsCount += p->GetRecordsCount();
            }
            sb << "];";
            sb << "records_count:" << recordsCount;
            NIceDb::TNiceDb db(txc.DB);
            normController.AddNormalizerEvent(db, "REMOVE_PORTIONS", sb);
        }
        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
    }

    ui64 GetSize() const override {
        return BrokenPortions.size();
    }
};

class TReadTask: public NOlap::NBlobOperations::NRead::ITask {
private:
    using TBase = NOlap::NBlobOperations::NRead::ITask;
    TNormalizationContext NormContext;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
    THashMap<TString, THashMap<TUnifiedBlobId, std::shared_ptr<TPortionInfo>>> PortionsByBlobId;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> BrokenPortions;
public:
    TReadTask(const TNormalizationContext& nCtx, const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions,
        std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas, THashMap < TString, THashMap<TUnifiedBlobId, std::shared_ptr<TPortionInfo>>>&& portionsByBlobId)
        : TBase(actions, "CS::NORMALIZER")
        , NormContext(nCtx)
        , Schemas(std::move(schemas))
        , PortionsByBlobId(portionsByBlobId)
    {
    }

protected:
    virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) override {
        auto changes = std::make_shared<TNormalizerResult>(std::move(BrokenPortions), Schemas);
        TActorContext::AsActorContext().Send(NormContext.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(changes));
    }

    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("blob_id", range.GetBlobId().ToStringNew())
            ("error", status.GetErrorMessage())("status", status.GetStatus())("event", "broken_blob_found")("storage_id", storageId);
        AFL_VERIFY(status.GetStatus() == NKikimrProto::EReplyStatus::NODATA)("status", status.GetStatus());
        auto itStorage = PortionsByBlobId.find(storageId);
        AFL_VERIFY(itStorage != PortionsByBlobId.end());
        auto it = itStorage->second.find(range.GetBlobId());
        AFL_VERIFY(it != itStorage->second.end());
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("portion", it->second->GetAddress().DebugString());
        BrokenPortions.emplace(it->second->GetPortionId(), it->second);
        return true;
    }

public:
    using TBase::TBase;
};

class TBrokenBlobsTask: public INormalizerTask {
    THashMap<TString, THashSet<TUnifiedBlobId>> Blobs;
    THashMap<TString, THashMap<TUnifiedBlobId, std::shared_ptr<TPortionInfo>>> PortionsByBlobId;
    const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
public:
    TBrokenBlobsTask(THashMap<TString, THashSet<TUnifiedBlobId>>&& blobs, THashMap<TString, THashMap<TUnifiedBlobId, std::shared_ptr<TPortionInfo>>>&& portionsByBlobId,
        const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>>& schemas)
        : Blobs(std::move(blobs))
        , PortionsByBlobId(portionsByBlobId)
        , Schemas(schemas)
    {}

    void Start(const TNormalizationController& controller, const TNormalizationContext& nCtx) override {
        ui64 memSize = 0;
        std::vector<std::shared_ptr<IBlobsReadingAction>> actions;
        for (auto&& [storageId, data] : Blobs) {
            auto op = controller.GetStoragesManager()->GetOperatorVerified(storageId);
            actions.emplace_back(op->StartReadingAction(NBlobOperations::EConsumer::NORMALIZER));
            for (auto&& b : data) {
                memSize += b.BlobSize();
                actions.back()->AddRange(TBlobRange::FromBlobId(b));
            }
        }
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            nCtx.GetResourceSubscribeActor(), std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                std::make_shared<TReadTask>(nCtx, actions, Schemas, std::move(PortionsByBlobId)), 0, memSize, "CS::NORMALIZER", controller.GetTaskSubscription()));
    }
};


bool TNormalizer::CheckPortion(const NColumnShard::TTablesManager& /*tablesManager*/, const TPortionInfo& /*portionInfo*/) const {
    return false;
}

INormalizerTask::TPtr TNormalizer::BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const {
    THashMap<TString, THashSet<TUnifiedBlobId>> blobIds;
    THashMap<TString, THashMap<TUnifiedBlobId, std::shared_ptr<TPortionInfo>>> portionByBlobId;
    for (auto&& portion : portions) {
        auto schemaPtr = schemas->FindPtr(portion->GetPortionId());
        THashMap<TString, THashSet<TUnifiedBlobId>> blobsByStorage;
        portion->FillBlobIdsByStorage(blobsByStorage, schemaPtr->get()->GetIndexInfo());
        if (blobsByStorage.size() > 1 || !blobsByStorage.contains(NBlobOperations::TGlobal::DefaultStorageId)) {
            continue;
        }
        for (auto&& i: blobsByStorage) {
            for (auto&& b : i.second) {
                AFL_VERIFY(portionByBlobId[i.first].emplace(b, portion).second);
            }
        }
    }
    for (auto&& [storageId, blobs] : portionByBlobId) {
        AFL_VERIFY(storageId == NBlobOperations::TGlobal::DefaultStorageId)("details", "Invalid storage for normalizer")("storage_id", storageId);
        for (auto&& [blobId, _] : blobs) {
            AFL_VERIFY(blobIds[storageId].emplace(blobId).second);
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


}
