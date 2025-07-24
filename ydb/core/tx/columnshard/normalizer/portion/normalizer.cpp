#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_accessor.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TPortionsNormalizerBase::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    auto initRes = DoInitImpl(controller, txc);

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

    NColumnShard::TTablesManager tablesManager(
        controller.GetStoragesManager(), controller.GetDataAccessorsManager(), std::make_shared<TPortionIndexStats>(), 0);
    if (!tablesManager.InitFromDB(db, nullptr)) {
        ACFL_TRACE("normalizer", "TPortionsNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (!tablesManager.HasPrimaryIndex()) {
        return tasks;
    }

    THashMap<ui64, TPortionAccessorConstructor> portions;
    auto schemas = std::make_shared<THashMap<ui64, ISnapshotSchema::TPtr>>();
    {
        auto conclusion = InitPortions(tablesManager, db, portions);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    {
        auto conclusion = InitColumns(tablesManager, db, portions);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    {
        auto conclusion = InitIndexes(db, portions);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    for (auto&& [_, p] : portions) {
        (*schemas)[p.GetPortionConstructor().GetPortionIdVerified()] =
            tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetSchemaVerified(p.GetPortionConstructor().GetSchemaVersionVerified());
    }

    std::vector<TPortionDataAccessor> package;

    ui64 brokenPortioncCount = 0;
    for (auto&& portionConstructor : portions) {
        auto portionInfo = portionConstructor.second.Build(false);
        if (CheckPortion(tablesManager, portionInfo)) {
            continue;
        }
        ++brokenPortioncCount;
        package.emplace_back(portionInfo);
        if (package.size() == 1000) {
            std::vector<TPortionDataAccessor> local;
            local.swap(package);
            auto task = BuildTask(std::move(local), schemas);
            if (!!task) {
                tasks.emplace_back(task);
            }
        }
    }

    if (package.size() > 0) {
        auto task = BuildTask(std::move(package), schemas);
        if (!!task) {
            tasks.emplace_back(task);
        }
    }
    ACFL_INFO("normalizer", "TPortionsNormalizer")("message", TStringBuilder() << brokenPortioncCount << " portions found");
    return tasks;
}

TConclusionStatus TPortionsNormalizerBase::InitPortions(
    const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashMap<ui64, TPortionAccessorConstructor>& constructors) {
    TDbWrapper wrapper(db.GetDatabase(), nullptr);
    if (!wrapper.LoadPortions(
            {}, [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const TIndexInfo& indexInfo =
                    portion->GetSchema(tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex())->GetIndexInfo();
                AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, DsGroupSelector));
                const ui64 portionId = portion->GetPortionIdVerified();
                AFL_VERIFY(constructors.emplace(portionId, TPortionAccessorConstructor(std::move(portion))).second);
            })) {
        return TConclusionStatus::Fail("repeated read db");
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TPortionsNormalizerBase::InitColumns(
    const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashMap<ui64, TPortionAccessorConstructor>& portions) {
    using namespace NColumnShard;
    auto columnsFilter = GetColumnsFilter(tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetLastSchema());
    auto rowset = db.Table<Schema::IndexColumnsV2>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("Not ready");
    }

    while (!rowset.EndOfSet()) {
        NOlap::TColumnChunkLoadContextV2 chunkLoadContext(rowset, DsGroupSelector);
        auto it = portions.find(chunkLoadContext.GetPortionId());
        AFL_VERIFY(it != portions.end());
        it->second.AddBuildInfo(chunkLoadContext.CreateBuildInfo());

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("Not ready");
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TPortionsNormalizerBase::InitIndexes(NIceDb::TNiceDb& db, THashMap<ui64, TPortionAccessorConstructor>& portions) {
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    auto rowset = db.Table<IndexIndexes>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("Not ready");
    }

    while (!rowset.EndOfSet()) {
        NOlap::TIndexChunkLoadContext chunkLoadContext(rowset, &DsGroupSelector);

        auto it = portions.find(rowset.GetValue<IndexIndexes::PortionId>());
        AFL_VERIFY(it != portions.end());
        it->second.LoadIndex(std::move(chunkLoadContext));

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("Not ready");
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap
