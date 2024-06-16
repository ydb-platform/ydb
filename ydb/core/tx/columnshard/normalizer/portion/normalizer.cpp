#include "normalizer.h"

#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TPortionsNormalizerBase::DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    auto initRes = DoInitImpl(controller,txc);

    if (initRes.IsFail()) {
        return initRes;
    }

    using namespace NColumnShard;

    NIceDb::TNiceDb db(txc.DB);
    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    NColumnShard::TTablesManager tablesManager(controller.GetStoragesManager(), 0);
    if (!tablesManager.InitFromDB(db)) {
        ACFL_ERROR("normalizer", "TPortionsNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (!tablesManager.HasPrimaryIndex()) {
        return tasks;
    }

    auto columnsFilter = GetColumnsFilter(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema());

    THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
    auto schemas = std::make_shared<THashMap<ui64, ISnapshotSchema::TPtr>>();

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        TPortionInfo::TSchemaCursor schema(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex());
        auto initPortionCB = [&](const TPortionInfo& portion, const TColumnChunkLoadContext& loadContext) {
            auto currentSchema = schema.GetSchema(portion);
            AFL_VERIFY(portion.ValidSnapshotInfo())("details", portion.DebugString());

            if (!columnsFilter.empty() && !columnsFilter.contains(loadContext.GetAddress().GetColumnId())) {
                return;
            }
            auto it = portions.find(portion.GetPortionId());
            if (it == portions.end()) {
                Y_ABORT_UNLESS(portion.Records.empty());
                (*schemas)[portion.GetPortionId()] = currentSchema;
                it = portions.emplace(portion.GetPortionId(), std::make_shared<TPortionInfo>(portion)).first;
            }
            TColumnRecord rec(it->second->RegisterBlobId(loadContext.GetBlobRange().GetBlobId()), loadContext, currentSchema->GetIndexInfo().GetColumnFeaturesVerified(loadContext.GetAddress().GetColumnId()));
            AFL_VERIFY(it->second->IsEqualWithSnapshots(portion))("self", it->second->DebugString())("item", portion.DebugString());
            auto portionMeta = loadContext.GetPortionMeta();
            it->second->AddRecord(currentSchema->GetIndexInfo(), rec, portionMeta);
        };

        while (!rowset.EndOfSet()) {
            TPortionInfo portion = TPortionInfo::BuildEmpty();
            auto index = rowset.GetValue<Schema::IndexColumns::Index>();
            Y_ABORT_UNLESS(index == 0);

            portion.SetPathId(rowset.GetValue<Schema::IndexColumns::PathId>());
            portion.SetMinSnapshotDeprecated(NOlap::TSnapshot(rowset.GetValue<Schema::IndexColumns::PlanStep>(), rowset.GetValue<Schema::IndexColumns::TxId>()));
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
    for (auto&& [_, portionInfo] : portions) {
        if (CheckPortion(tablesManager,  *portionInfo)) {
            continue;
        }
        ++brokenPortioncCount;
        package.emplace_back(portionInfo);
        if (package.size() == 1000) {
            std::vector<std::shared_ptr<TPortionInfo>> local;
            local.swap(package);
            tasks.emplace_back(BuildTask(std::move(local), schemas));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(BuildTask(std::move(package), schemas));
    }
    ACFL_INFO("normalizer", "TPortionsNormalizer")("message", TStringBuilder() << brokenPortioncCount << " portions found");
    return tasks;
}

}
