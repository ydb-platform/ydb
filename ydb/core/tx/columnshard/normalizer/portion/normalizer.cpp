#include "normalizer.h"

#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>
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
        ACFL_TRACE("normalizer", "TPortionsNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (!tablesManager.HasPrimaryIndex()) {
        return tasks;
    }

    auto columnsFilter = GetColumnsFilter(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema());

    THashMap<ui64, TPortionInfoConstructor> portions;
    auto schemas = std::make_shared<THashMap<ui64, ISnapshotSchema::TPtr>>();

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        TPortionInfo::TSchemaCursor schema(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex());
        auto initPortion = [&](TPortionInfoConstructor&& portion, const TColumnChunkLoadContext& loadContext) {
            auto currentSchema = schema.GetSchema(portion);
            portion.SetSchemaVersion(currentSchema->GetVersion());

            if (!columnsFilter.empty() && !columnsFilter.contains(loadContext.GetAddress().GetColumnId())) {
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
            initPortion(std::move(portion), chunkLoadContext);

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
