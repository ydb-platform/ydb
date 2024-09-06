#include "clean.h"

#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>


namespace NKikimr::NOlap {

class TBlobsRemovingResult : public INormalizerChanges {
    std::shared_ptr<IBlobsDeclareRemovingAction> RemovingAction;
    std::vector<std::shared_ptr<TPortionInfo>> Portions;
public:
    TBlobsRemovingResult(std::shared_ptr<IBlobsDeclareRemovingAction> removingAction, std::vector<std::shared_ptr<TPortionInfo>>&& portions)
        : RemovingAction(removingAction)
        , Portions(std::move(portions))
    {}

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        NOlap::TBlobManagerDb blobManagerDb(txc.DB);
        RemovingAction->OnExecuteTxAfterRemoving(blobManagerDb, true);

        TDbWrapper db(txc.DB, nullptr);
        for (auto&& portion : Portions) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("message", "remove lost portion")("path_id", portion->GetPathId())("portion_id", portion->GetPortionId());
            portion->RemoveFromDatabase(db);
        }
        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
        RemovingAction->OnCompleteTxAfterRemoving(true);
    }

    ui64 GetSize() const override {
        return Portions.size();
    }
};

class TBlobsRemovingTask : public INormalizerTask {
    std::vector<TUnifiedBlobId> Blobs;
    std::vector<std::shared_ptr<TPortionInfo>> Portions;
public:
    TBlobsRemovingTask(std::vector<TUnifiedBlobId>&& blobs, std::vector<std::shared_ptr<TPortionInfo>>&& portions)
        : Blobs(std::move(blobs))
        , Portions(std::move(portions))
    {}

    void Start(const TNormalizationController& controller, const TNormalizationContext& nCtx) override {
        controller.GetCounters().CountObjects(Blobs.size());
        auto removeAction = controller.GetStoragesManager()->GetDefaultOperator()->StartDeclareRemovingAction(NBlobOperations::EConsumer::NORMALIZER);
        for (auto&& blobId : Blobs) {
            removeAction->DeclareSelfRemove(blobId);
        }
        TActorContext::AsActorContext().Send(nCtx.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(std::make_shared<TBlobsRemovingResult>(removeAction, std::move(Portions))));
    }
};


bool TCleanPortionsNormalizer::CheckPortion(const NColumnShard::TTablesManager& tablesManager, const TPortionInfo& portionInfo) const {
    return tablesManager.HasTable(portionInfo.GetAddress().GetPathId(), true);
}

INormalizerTask::TPtr TCleanPortionsNormalizer::BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const {
    std::vector<TUnifiedBlobId> blobIds;
    THashMap<TString, THashSet<TUnifiedBlobId>> blobsByStorage;
    for (auto&& portion : portions) {
        auto schemaPtr = schemas->FindPtr(portion->GetPortionId());
        portion->FillBlobIdsByStorage(blobsByStorage, schemaPtr->get()->GetIndexInfo());
    }
    for (auto&& [storageId, blobs] : blobsByStorage) {
        if (storageId == NBlobOperations::TGlobal::DefaultStorageId) {
            for (auto&& blobId : blobs) {
                blobIds.emplace_back(blobId);
            }
        } else if (storageId == NBlobOperations::TGlobal::LocalMetadataStorageId) {
        } else {
            AFL_VERIFY(false)("details", "Invalid storage for normalizer");
        }
    }
    return std::make_shared<TBlobsRemovingTask>(std::move(blobIds), std::move(portions));
}

 TConclusion<bool> TCleanPortionsNormalizer::DoInitImpl(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext&) {
    return true;
}


}
