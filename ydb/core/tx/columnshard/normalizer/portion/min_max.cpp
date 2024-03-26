#include "min_max.h"
#include "normalizer.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>


namespace NKikimr::NOlap {

class TMinMaxSnapshotChangesTask: public NConveyor::ITask {
public:
    using TDataContainer = std::vector<std::shared_ptr<TPortionInfo>>;
private:
    NBlobOperations::NRead::TCompositeReadBlobs Blobs;
    TDataContainer Portions;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
    TNormalizationContext NormContext;
protected:
    virtual bool DoExecute() override {
        Y_ABORT_UNLESS(!Schemas->empty());
        auto pkColumnIds = Schemas->begin()->second->GetPkColumnsIds();
        pkColumnIds.insert(TIndexInfo::GetSpecialColumnIds().begin(), TIndexInfo::GetSpecialColumnIds().end());

        for (auto&& portionInfo : Portions) {
            auto blobSchema = Schemas->FindPtr(portionInfo->GetPortionId());
            THashMap<TChunkAddress, TPortionInfo::TAssembleBlobInfo> blobsDataAssemble;
            for (auto&& i : portionInfo->Records) {
                auto blobData = Blobs.Extract((*blobSchema)->GetIndexInfo().GetColumnStorageId(i.GetColumnId(), portionInfo->GetMeta().GetTierName()), portionInfo->RestoreBlobRange(i.BlobRange));
                blobsDataAssemble.emplace(i.GetAddress(), blobData);
            }

            AFL_VERIFY(!!blobSchema)("details", portionInfo->DebugString());
            auto filteredSchema = std::make_shared<TFilteredSnapshotSchema>(*blobSchema, pkColumnIds);
            auto preparedBatch = portionInfo->PrepareForAssemble(**blobSchema, *filteredSchema, blobsDataAssemble);
            auto batch = preparedBatch.Assemble();
            Y_ABORT_UNLESS(!!batch);
            portionInfo->AddMetadata(**blobSchema, batch, portionInfo->GetMeta().GetTierName());
        }

        auto changes = std::make_shared<TPortionsNormalizer::TNormalizerResult>(std::move(Portions));
        TActorContext::AsActorContext().Send(NormContext.GetColumnshardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(changes));
        return true;
    }

public:
    TMinMaxSnapshotChangesTask(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TNormalizationContext& nCtx, TDataContainer&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas)
        : Blobs(std::move(blobs))
        , Portions(std::move(portions))
        , Schemas(schemas)
        , NormContext(nCtx)
    {}

    virtual TString GetTaskClassIdentifier() const override {
        const static TString name = "TMinMaxSnapshotChangesTask";
        return name;
    }

    static void FillBlobRanges(std::shared_ptr<IBlobsReadingAction> readAction, const std::shared_ptr<TPortionInfo>& portion) {
        for (auto&& chunk : portion->Records) {
            readAction->AddRange(portion->RestoreBlobRange(chunk.BlobRange));
        }
    }

    static ui64 GetMemSize(const std::shared_ptr<TPortionInfo>& portion) {
        return portion->GetRawBytes();
    }

    static bool CheckPortion(const TPortionInfo& portionInfo) {
        if (!portionInfo.GetMeta().HasPrimaryKeyBorders() || !portionInfo.GetMeta().HasSnapshotMinMax()) {
            return false;
        }
        return true;
    }

    static std::set<ui32> GetColumnsFilter(const ISnapshotSchema::TPtr& schema) {
        return schema->GetPkColumnsIds();
    }
};


class TPortionsNormalizer::TNormalizerResult : public INormalizerChanges {
    TMinMaxSnapshotChangesTask::TDataContainer Portions;
public:
    TNormalizerResult(TMinMaxSnapshotChangesTask::TDataContainer&& portions)
        : Portions(std::move(portions))
    {}

    bool Apply(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        for (auto&& portionInfo : Portions) {
            for (auto&& chunk : portionInfo->Records) {
                auto proto = portionInfo->GetMeta().SerializeToProto(chunk.ColumnId, chunk.Chunk);
                if (!proto) {
                    continue;
                }
                auto rowProto = chunk.GetMeta().SerializeToProto();
                *rowProto.MutablePortionMeta() = std::move(*proto);

                db.Table<Schema::IndexColumns>().Key(0, portionInfo->GetDeprecatedGranuleId(), chunk.ColumnId,
                    portionInfo->GetMinSnapshot().GetPlanStep(), portionInfo->GetMinSnapshot().GetTxId(), portionInfo->GetPortion(), chunk.Chunk).Update(
                    NIceDb::TUpdate<Schema::IndexColumns::Metadata>(rowProto.SerializeAsString())
                );
            }
        }
        return true;
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TPortionsNormalizer::Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    std::vector<INormalizerTask::TPtr> tasks;

    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    TTablesManager tablesManager(controller.GetStoragesManager(), 0);
    if (!tablesManager.InitFromDB(db)) {
        ACFL_ERROR("normalizer", "TPortionsNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    if (!tablesManager.HasPrimaryIndex()) {
        return tasks;
    }

    THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
    auto schemas = std::make_shared<THashMap<ui64, ISnapshotSchema::TPtr>>();
    auto pkColumnIds = TMinMaxSnapshotChangesTask::GetColumnsFilter(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema());

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        TSnapshot lastSnapshot(0, 0);
        ISnapshotSchema::TPtr currentSchema;
        auto initPortionCB = [&](const TPortionInfo& portion, const TColumnChunkLoadContext& loadContext) {
            if (!currentSchema || lastSnapshot != portion.GetMinSnapshot()) {
                currentSchema = tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetSchema(portion.GetMinSnapshot());
                lastSnapshot = portion.GetMinSnapshot();
            }

            AFL_VERIFY(portion.ValidSnapshotInfo())("details", portion.DebugString());
            if (!pkColumnIds.contains(loadContext.GetAddress().GetColumnId())) {
                return;
            }
            auto it = portions.find(portion.GetPortion());
            auto portionMeta = loadContext.GetPortionMeta();
            if (it == portions.end()) {
                Y_ABORT_UNLESS(portion.Records.empty());
                (*schemas)[portion.GetPortionId()] = currentSchema;
                it = portions.emplace(portion.GetPortion(), std::make_shared<TPortionInfo>(portion)).first;
            }
            TColumnRecord rec(it->second->RegisterBlobId(loadContext.GetBlobRange().GetBlobId()), loadContext, currentSchema->GetIndexInfo().GetColumnFeaturesVerified(loadContext.GetAddress().GetColumnId()));
            AFL_VERIFY(it->second->IsEqualWithSnapshots(portion))("self", it->second->DebugString())("item", portion.DebugString());
            it->second->AddRecord(currentSchema->GetIndexInfo(), rec, portionMeta);
        };

        while (!rowset.EndOfSet()) {
            TPortionInfo portion = TPortionInfo::BuildEmpty();
            auto index = rowset.GetValue<Schema::IndexColumns::Index>();
            Y_ABORT_UNLESS(index == 0);

            portion.SetPathId(rowset.GetValue<Schema::IndexColumns::PathId>());

            portion.SetMinSnapshot(rowset.GetValue<Schema::IndexColumns::PlanStep>(), rowset.GetValue<Schema::IndexColumns::TxId>());
            portion.SetPortion(rowset.GetValue<Schema::IndexColumns::Portion>());
            portion.SetDeprecatedGranuleId(rowset.GetValue<Schema::IndexColumns::Granule>());
            portion.SetRemoveSnapshot(rowset.GetValue<Schema::IndexColumns::XPlanStep>(), rowset.GetValue<Schema::IndexColumns::XTxId>());

            NOlap::TColumnChunkLoadContext chunkLoadContext(rowset, &DsGroupSelector);
            initPortionCB(portion, chunkLoadContext);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<std::shared_ptr<TPortionInfo>> package;
    package.reserve(100);

    ui64 brokenPortioncCount = 0;
    for (auto&& portion : portions) {
        if (TMinMaxSnapshotChangesTask::CheckPortion(*portion.second)) {
            continue;
        }
        ++brokenPortioncCount;
        package.emplace_back(portion.second);
        if (package.size() == 1000) {
            std::vector<std::shared_ptr<TPortionInfo>> local;
            local.swap(package);
            tasks.emplace_back(std::make_shared<TPortionsNormalizerTask<TMinMaxSnapshotChangesTask>>(std::move(local), schemas));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(std::make_shared<TPortionsNormalizerTask<TMinMaxSnapshotChangesTask>>(std::move(package), schemas));
    }

    AtomicSet(ActiveTasksCount, tasks.size());
    ACFL_INFO("normalizer", "TPortionsNormalizer")("message", TStringBuilder() << brokenPortioncCount << " portions found");
    return tasks;
}

}
