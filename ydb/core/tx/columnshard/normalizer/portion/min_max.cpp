#include "min_max.h"
#include "normalizer.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

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

        THashMap<ui64, ui32> firstPKColumnIdByPathId;
        for (auto&& portionInfo : Portions) {
            auto blobSchema = Schemas->FindPtr(portionInfo->GetPortionId());
            AFL_VERIFY(!!blobSchema)("details", portionInfo->DebugString());
            {
                auto it = firstPKColumnIdByPathId.find(portionInfo->GetPathId());
                if (it == firstPKColumnIdByPathId.end()) {
                    firstPKColumnIdByPathId.emplace(portionInfo->GetPathId(), (*blobSchema)->GetIndexInfo().GetPKFirstColumnId());
                } else {
                    AFL_VERIFY(it->second == (*blobSchema)->GetIndexInfo().GetPKFirstColumnId());
                }
            }
            THashMap<TChunkAddress, TPortionInfo::TAssembleBlobInfo> blobsDataAssemble;
            for (auto&& i : portionInfo->GetRecords()) {
                auto blobData = Blobs.Extract((*blobSchema)->GetIndexInfo().GetColumnStorageId(i.GetColumnId(), portionInfo->GetMeta().GetTierName()), portionInfo->RestoreBlobRange(i.BlobRange));
                blobsDataAssemble.emplace(i.GetAddress(), blobData);
            }

            auto filteredSchema = std::make_shared<TFilteredSnapshotSchema>(*blobSchema, pkColumnIds);
            auto preparedBatch = portionInfo->PrepareForAssemble(**blobSchema, *filteredSchema, blobsDataAssemble);
            auto batch = preparedBatch.Assemble();
            Y_ABORT_UNLESS(!!batch);
//            portionInfo->AddMetadata(**blobSchema, batch, portionInfo->GetMeta().GetTierName());
        }

        auto changes = std::make_shared<TPortionsNormalizer::TNormalizerResult>(std::move(Portions), std::move(firstPKColumnIdByPathId));
        TActorContext::AsActorContext().Send(NormContext.GetColumnshardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(changes));
        return true;
    }

public:
    TMinMaxSnapshotChangesTask(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TNormalizationContext& nCtx, TDataContainer&& portions, 
        std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas)
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
        return portion->GetTotalRawBytes();
    }

    static bool CheckPortion(const TPortionInfo& /*portionInfo*/) {
        return true;
    }

    static std::set<ui32> GetColumnsFilter(const ISnapshotSchema::TPtr& schema) {
        return schema->GetPkColumnsIds();
    }
};


class TPortionsNormalizer::TNormalizerResult : public INormalizerChanges {
    TMinMaxSnapshotChangesTask::TDataContainer Portions;
    THashMap<ui64, ui32> FirstPKColumnIdByPathId;
public:
    TNormalizerResult(TMinMaxSnapshotChangesTask::TDataContainer&& portions, THashMap<ui64, ui32>&& firstPKColumnIdByPathId)
        : Portions(std::move(portions))
        , FirstPKColumnIdByPathId(std::move(firstPKColumnIdByPathId))
    {}

    bool Apply(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        for (auto&& portionInfo : Portions) {
            auto it = FirstPKColumnIdByPathId.find(portionInfo->GetPathId());
            AFL_VERIFY(it != FirstPKColumnIdByPathId.end());
            for (auto&& chunk : portionInfo->Records) {
                if (chunk.GetChunkIdx() || chunk.GetColumnId() != it->second) {
                    continue;
                }
                auto rowProto = chunk.GetMeta().SerializeToProto();
                *rowProto.MutablePortionMeta() = portionInfo->GetMeta().SerializeToProto();

                db.Table<Schema::IndexColumns>().Key(0, portionInfo->GetPathId(), chunk.ColumnId,
                    portionInfo->GetMinSnapshotDeprecated().GetPlanStep(), portionInfo->GetMinSnapshotDeprecated().GetTxId(), portionInfo->GetPortion(), chunk.Chunk).Update(
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

    THashMap<ui64, TPortionInfoConstructor> portions;
    auto schemas = std::make_shared<THashMap<ui64, ISnapshotSchema::TPtr>>();
    auto pkColumnIds = TMinMaxSnapshotChangesTask::GetColumnsFilter(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema());

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        TPortionInfo::TSchemaCursor schema(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex());
        auto initPortionCB = [&](TPortionInfoConstructor&& portion, const TColumnChunkLoadContext& loadContext) {
            auto currentSchema = schema.GetSchema(portion);

            if (!pkColumnIds.contains(loadContext.GetAddress().GetColumnId())) {
                return;
            }
            auto it = portions.find(portion.GetPortionIdVerified());
            if (it == portions.end()) {
                (*schemas)[portion.GetPortionIdVerified()] = currentSchema;
                const ui64 portionId = portion.GetPortionIdVerified();
                it = portions.emplace(portionId, std::move(portion)).first;
            } else {
                it->second.Merge(std::move(portion));
            }
            it->second.LoadRecord(currentSchema->GetIndexInfo(), loadContext);
        };

        while (!rowset.EndOfSet()) {
            TPortionInfoConstructor portion(rowset.GetValue<Schema::IndexColumns::PathId>(), rowset.GetValue<Schema::IndexColumns::Portion>());
            Y_ABORT_UNLESS(rowset.GetValue<Schema::IndexColumns::Index>() == 0);

            portion.SetMinSnapshotDeprecated(NOlap::TSnapshot(rowset.GetValue<Schema::IndexColumns::PlanStep>(), rowset.GetValue<Schema::IndexColumns::TxId>()));
            portion.SetRemoveSnapshot(rowset.GetValue<Schema::IndexColumns::XPlanStep>(), rowset.GetValue<Schema::IndexColumns::XTxId>());

            NOlap::TColumnChunkLoadContext chunkLoadContext(rowset, &DsGroupSelector);
            initPortionCB(std::move(portion), chunkLoadContext);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<std::shared_ptr<TPortionInfo>> package;
    package.reserve(100);

    ui64 brokenPortioncCount = 0;
    for (auto&& portionConstructor : portions) {
        auto portionInfo = std::make_shared<TPortionInfo>(portionConstructor.second.Build(false));
        if (TMinMaxSnapshotChangesTask::CheckPortion(*portionInfo)) {
            continue;
        }
        ++brokenPortioncCount;
        package.emplace_back(portionInfo);
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
