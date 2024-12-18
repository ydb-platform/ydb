#include "granule.h"
#include "stages.h"
#include "storage.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_accessor/in_mem/manager.h>
#include <ydb/core/tx/columnshard/data_accessor/local_db/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/tx_reader/composite.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void TGranuleMeta::AppendPortion(const TPortionDataAccessor& info, const bool addAsAccessor) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "upsert_portion")("portion", info.GetPortionInfo().DebugString())(
        "path_id", GetPathId());
    auto it = Portions.find(info.GetPortionInfo().GetPortionId());
    AFL_VERIFY(info.GetPortionInfo().GetPathId() == GetPathId())("event", "incompatible_granule")(
        "portion", info.GetPortionInfo().DebugString())("path_id", GetPathId());

    AFL_VERIFY(info.GetPortionInfo().ValidSnapshotInfo())("event", "incorrect_portion_snapshots")(
        "portion", info.GetPortionInfo().DebugString());

    AFL_VERIFY(it == Portions.end());
    OnBeforeChangePortion(nullptr);
    it = Portions.emplace(info.GetPortionInfo().GetPortionId(), info.MutablePortionInfoPtr()).first;
    if (addAsAccessor) {
        DataAccessorsManager->AddPortion(info);
    }
    OnAfterChangePortion(it->second, nullptr);
}

bool TGranuleMeta::ErasePortion(const ui64 portion) {
    auto it = Portions.find(portion);
    if (it == Portions.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "portion_erased_already")("portion_id", portion)("pathId", PathId);
        return false;
    } else {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "portion_erased")("portion_info", it->second->DebugString())("pathId", PathId);
    }
    DataAccessorsManager->RemovePortion(it->second);
    OnBeforeChangePortion(it->second);
    Portions.erase(it);
    OnAfterChangePortion(nullptr, nullptr);
    return true;
}

void TGranuleMeta::OnAfterChangePortion(const std::shared_ptr<TPortionInfo> portionAfter,
    NStorageOptimizer::IOptimizerPlanner::TModificationGuard* modificationGuard, const bool onLoad) {
    if (portionAfter) {
        PortionInfoGuard.OnNewPortion(portionAfter);
        if (!portionAfter->HasRemoveSnapshot()) {
            PortionsIndex.AddPortion(portionAfter);
            if (modificationGuard) {
                modificationGuard->AddPortion(portionAfter);
            } else {
                OptimizerPlanner->StartModificationGuard().AddPortion(portionAfter);
            }
            NActualizer::TAddExternalContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now(), Portions);
            if (!onLoad) {
                ActualizationIndex->AddPortion(portionAfter, context);
            }
        }
        Stats->OnAddPortion(*portionAfter);
    }
    if (!!AdditiveSummaryCache) {
        if (portionAfter && !portionAfter->HasRemoveSnapshot()) {
            auto g = AdditiveSummaryCache->StartEdit(Counters);
            g.AddPortion(*portionAfter);
        }
    }

    ModificationLastTime = TMonotonic::Now();
    Stats->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnBeforeChangePortion(const std::shared_ptr<TPortionInfo> portionBefore) {
    if (portionBefore) {
        PortionInfoGuard.OnDropPortion(portionBefore);
        if (!portionBefore->HasRemoveSnapshot()) {
            PortionsIndex.RemovePortion(portionBefore);
            OptimizerPlanner->StartModificationGuard().RemovePortion(portionBefore);
            ActualizationIndex->RemovePortion(portionBefore);
        }
        Stats->OnRemovePortion(*portionBefore);
    }
    if (!!AdditiveSummaryCache) {
        if (portionBefore && !portionBefore->HasRemoveSnapshot()) {
            auto g = AdditiveSummaryCache->StartEdit(Counters);
            g.RemovePortion(*portionBefore);
        }
    }
}

void TGranuleMeta::OnCompactionFinished() {
    AllowInsertionFlag = false;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFinished")("info", DebugString());
    Stats->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionFailed(const TString& reason) {
    AllowInsertionFlag = false;
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFailed")("reason", reason)("info", DebugString());
    Stats->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionStarted() {
    AllowInsertionFlag = false;
}

void TGranuleMeta::RebuildAdditiveMetrics() const {
    TGranuleAdditiveSummary result;
    {
        auto g = result.StartEdit(Counters);
        for (auto&& i : Portions) {
            if (i.second->HasRemoveSnapshot()) {
                continue;
            }
            g.AddPortion(*i.second);
        }
    }
    AdditiveSummaryCache = result;
}

const NKikimr::NOlap::TGranuleAdditiveSummary& TGranuleMeta::GetAdditiveSummary() const {
    if (!AdditiveSummaryCache) {
        RebuildAdditiveMetrics();
    }
    return *AdditiveSummaryCache;
}

TGranuleMeta::TGranuleMeta(
    const ui64 pathId, const TGranulesStorage& owner, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , DataAccessorsManager(owner.GetDataAccessorsManager())
    , Counters(counters)
    , PortionInfoGuard(owner.GetCounters().BuildPortionBlobsGuard())
    , Stats(owner.GetStats())
    , StoragesManager(owner.GetStoragesManager())
    , PortionsIndex(*this, Counters.GetPortionsIndexCounters()) {
    NStorageOptimizer::IOptimizerPlannerConstructor::TBuildContext context(
        PathId, owner.GetStoragesManager(), versionedIndex.GetLastSchema()->GetIndexInfo().GetPrimaryKey());
    OptimizerPlanner = versionedIndex.GetLastSchema()->GetIndexInfo().GetCompactionPlannerConstructor()->BuildPlanner(context).DetachResult();
    NDataAccessorControl::TManagerConstructionContext mmContext(DataAccessorsManager->GetTabletActorId(), false);
    ResetAccessorsManager(versionedIndex.GetLastSchema()->GetIndexInfo().GetMetadataManagerConstructor(), mmContext);
    AFL_VERIFY(!!OptimizerPlanner);
    ActualizationIndex = std::make_unique<NActualizer::TGranuleActualizationIndex>(PathId, versionedIndex, StoragesManager);
}

void TGranuleMeta::UpsertPortionOnLoad(const std::shared_ptr<TPortionInfo>& portion) {
    if (portion->HasInsertWriteId() && !portion->HasCommitSnapshot()) {
        const TInsertWriteId insertWriteId = portion->GetInsertWriteIdVerified();
        AFL_VERIFY(InsertedPortions.emplace(insertWriteId, portion).second);
        AFL_VERIFY(!Portions.contains(portion->GetPortionId()));
    } else {
        auto portionId = portion->GetPortionId();
        AFL_VERIFY(Portions.emplace(portionId, portion).second);
    }
}

void TGranuleMeta::BuildActualizationTasks(NActualizer::TTieringProcessContext& context, const TDuration actualizationLag) const {
    if (context.GetActualInstant() < NextActualizations) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_actualization")("waiting", NextActualizations - context.GetActualInstant());
        return;
    }
    NActualizer::TExternalTasksContext extTasks(Portions);
    ActualizationIndex->ExtractActualizationTasks(context, extTasks);
    NextActualizations = context.GetActualInstant() + actualizationLag;
}

void TGranuleMeta::ResetAccessorsManager(const std::shared_ptr<NDataAccessorControl::IManagerConstructor>& constructor,
    const NDataAccessorControl::TManagerConstructionContext& context) {
    MetadataMemoryManager = constructor->Build(context).DetachResult();
    DataAccessorsManager->RegisterController(MetadataMemoryManager->BuildCollector(PathId), context.IsUpdate());
}

void TGranuleMeta::ResetOptimizer(const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& constructor,
    std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if (constructor->ApplyToCurrentObject(OptimizerPlanner)) {
        return;
    }
    NStorageOptimizer::IOptimizerPlannerConstructor::TBuildContext context(PathId, storages, pkSchema);
    OptimizerPlanner = constructor->BuildPlanner(context).DetachResult();
    AFL_VERIFY(!!OptimizerPlanner);
    THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
    for (auto&& i : Portions) {
        if (i.second->HasRemoveSnapshot()) {
            continue;
        }
        portions.emplace(i.first, i.second);
    }
    OptimizerPlanner->ModifyPortions(portions, {});
}
/*

void TGranuleMeta::ResetMetadataManager(const std::shared_ptr<NDataAccessorControl::IManagerConstructor>& constructor,
    std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if (constructor->ApplyToCurrentObject(MetadataMemoryManager)) {
        return;
    }
    NStorageOptimizer::IManagerConstructor::TBuildContext context(PathId, storages, pkSchema);
    MetadataMemoryManager = constructor->Build(context).DetachResult();
    AFL_VERIFY(!!OptimizerPlanner);
    THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
    for (auto&& i : Portions) {
        if (i.second->HasRemoveSnapshot()) {
            continue;
        }
        portions.emplace(i.first, i.second);
    }
    OptimizerPlanner->ModifyPortions(portions, {});
}
*/

std::shared_ptr<NKikimr::ITxReader> TGranuleMeta::BuildLoader(
    const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector, const TVersionedIndex& vIndex) {
    auto portionsLoader = std::make_shared<NLoading::TGranuleOnlyPortionsReader>("portions", &vIndex, this, dsGroupSelector);
    auto metadataLoader = MetadataMemoryManager->BuildLoader(vIndex, this, dsGroupSelector);
    auto commonFinish = std::make_shared<NLoading::TGranuleFinishCommonLoading>("granule_finished_common", this);

    auto result = std::make_shared<TTxCompositeReader>("granule");
    result->AddChildren(portionsLoader);
    if (metadataLoader) {
        result->AddChildren(metadataLoader);
    }
    result->AddChildren(commonFinish);
    return result;
}

bool TGranuleMeta::TestingLoad(IDbWrapper& db, const TVersionedIndex& versionedIndex) {
    TInGranuleConstructors constructors;
    {
        if (!db.LoadPortions(PathId, [&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const TIndexInfo& indexInfo = portion.GetSchema(versionedIndex)->GetIndexInfo();
                AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, db.GetDsGroupSelectorVerified()));
                AFL_VERIFY(constructors.AddConstructorVerified(std::move(portion)));
            })) {
            return false;
        }
    }

    {
        TPortionInfo::TSchemaCursor schema(versionedIndex);
        if (!db.LoadColumns(PathId, [&](TColumnChunkLoadContextV2&& loadContext) {
                auto* constructor = constructors.GetConstructorVerified(loadContext.GetPortionId());
                for (auto&& i : loadContext.BuildRecordsV1()) {
                    constructor->LoadRecord(std::move(i));
                }
            })) {
            return false;
        }
    }

    {
        if (!db.LoadIndexes(PathId, [&](const ui64 /*pathId*/, const ui64 portionId, TIndexChunkLoadContext&& loadContext) {
                auto* constructor = constructors.GetConstructorVerified(portionId);
                constructor->LoadIndex(std::move(loadContext));
            })) {
            return false;
        };
    }
    for (auto&& [portionId, constructor] : constructors) {
        auto accessor = constructor.Build(false);
        DataAccessorsManager->AddPortion(accessor);
        UpsertPortionOnLoad(accessor.MutablePortionInfoPtr());
    }
    return true;
}

void TGranuleMeta::InsertPortionOnComplete(const TPortionDataAccessor& portion, IColumnEngine& /*engine*/) {
    AFL_VERIFY(InsertedPortions.emplace(portion.GetPortionInfo().GetInsertWriteIdVerified(), portion.MutablePortionInfoPtr()).second);
    AFL_VERIFY(InsertedAccessors.emplace(portion.GetPortionInfo().GetInsertWriteIdVerified(), portion).second);
    DataAccessorsManager->AddPortion(portion);
}

void TGranuleMeta::InsertPortionOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TPortionDataAccessor& portion) const {
    AFL_VERIFY(!InsertedPortions.contains(portion.GetPortionInfo().GetInsertWriteIdVerified()));
    TDbWrapper wrapper(txc.DB, nullptr);
    portion.SaveToDatabase(wrapper, 0, false);
}

void TGranuleMeta::CommitPortionOnExecute(
    NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId, const TSnapshot& snapshot) const {
    auto it = InsertedPortions.find(insertWriteId);
    AFL_VERIFY(it != InsertedPortions.end());
    it->second->SetCommitSnapshot(snapshot);
    TDbWrapper wrapper(txc.DB, nullptr);
    it->second->SaveMetaToDatabase(wrapper);
}

void TGranuleMeta::CommitPortionOnComplete(const TInsertWriteId insertWriteId, IColumnEngine& engine) {
    auto it = InsertedPortions.find(insertWriteId);
    AFL_VERIFY(it != InsertedPortions.end());
    InsertedPortions.erase(it);
    {
        auto it = InsertedAccessors.find(insertWriteId);
        if (it != InsertedAccessors.end()) {
            (static_cast<TColumnEngineForLogs*>(&engine))->AppendPortion(it->second, false);
            InsertedAccessors.erase(it);
        }
    }
}

void TGranuleMeta::CommitImmediateOnExecute(
    NTabletFlatExecutor::TTransactionContext& txc, const TSnapshot& snapshot, const TPortionDataAccessor& portion) const {
    AFL_VERIFY(!InsertedPortions.contains(portion.GetPortionInfo().GetInsertWriteIdVerified()));
    portion.MutablePortionInfo().SetCommitSnapshot(snapshot);
    TDbWrapper wrapper(txc.DB, nullptr);
    portion.SaveToDatabase(wrapper, 0, false);
}

void TGranuleMeta::CommitImmediateOnComplete(const std::shared_ptr<TPortionInfo> /*portion*/, IColumnEngine& /*engine*/) {
    AFL_VERIFY(false);
    //    (static_cast<TColumnEngineForLogs&>(engine)).AppendPortion(portion);
}

}   // namespace NKikimr::NOlap
