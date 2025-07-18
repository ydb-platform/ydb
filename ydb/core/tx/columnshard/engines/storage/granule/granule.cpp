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

void TGranuleMeta::AppendPortion(const std::shared_ptr<TPortionInfo>& info) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "upsert_portion")("portion", info->DebugString())("path_id", GetPathId());
    AFL_VERIFY(!Portions.contains(info->GetPortionId()));
    AFL_VERIFY(info->GetPathId() == GetPathId())("event", "incompatible_granule")("portion", info->DebugString())("path_id", GetPathId());

    AFL_VERIFY(info->ValidSnapshotInfo())("event", "incorrect_portion_snapshots")("portion", info->DebugString());

    OnBeforeChangePortion(nullptr);
    Portions.emplace(info->GetPortionId(), info);
    OnAfterChangePortion(info, nullptr);
}

void TGranuleMeta::AppendPortion(const TPortionDataAccessor& info) {
    AppendPortion(info.MutablePortionInfoPtr());
    DataAccessorsManager->AddPortion(info);
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

TGranuleMeta::TGranuleMeta(const TInternalPathId pathId, const TGranulesStorage& owner, const NColumnShard::TGranuleDataCounters& counters,
    const TVersionedIndex& versionedIndex)
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
    if (!portion->IsCommitted()) {
        const std::shared_ptr<TWrittenPortionInfo> portionImpl = std::static_pointer_cast<TWrittenPortionInfo>(portion);
        const TInsertWriteId insertWriteId = portionImpl->GetInsertWriteId();
        if (AtomicGet(LastInsertWriteId) < (i64)portionImpl->GetInsertWriteId()) {
            AtomicSet(LastInsertWriteId, (i64)portionImpl->GetInsertWriteId());
        }
        AFL_VERIFY(InsertedPortions.emplace(insertWriteId, portionImpl).second);
        AFL_VERIFY(InsertedPortionsById.emplace(portionImpl->GetPortionId(), portionImpl).second);
        AFL_VERIFY(!Portions.contains(portionImpl->GetPortionId()));
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
}

void TGranuleMeta::ResetOptimizer(const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& constructor,
    std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if (constructor->ApplyToCurrentObject(OptimizerPlanner)) {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "applied_optimizer")("constructor", constructor->GetClassName());
        return;
    }
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "reset_optimizer")("constructor", constructor->GetClassName());
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
        if (!db.LoadPortions(
                PathId, [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                    const TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
                    AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, db.GetDsGroupSelectorVerified()));
                    AFL_VERIFY(constructors.AddConstructorVerified(std::move(portion)));
                })) {
            return false;
        }
    }

    {
        if (!db.LoadColumns(PathId, [&](TColumnChunkLoadContextV2&& loadContext) {
                auto* constructor = constructors.GetConstructorVerified(loadContext.GetPortionId());
                constructor->AddBuildInfo(loadContext.CreateBuildInfo());
            })) {
            return false;
        }
    }

    {
        if (!db.LoadIndexes(PathId, [&](const TInternalPathId /*pathId*/, const ui64 portionId, TIndexChunkLoadContext&& loadContext) {
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

void TGranuleMeta::InsertPortionOnExecute(
    NTabletFlatExecutor::TTransactionContext& txc, const TPortionDataAccessor& portion, const ui64 firstPKColumnId) const {
    auto portionImpl = portion.MutablePortionInfoPtr();
    if (portionImpl->GetPortionType() == EPortionType::Written) {
        auto writtenPortion = std::static_pointer_cast<TWrittenPortionInfo>(portionImpl);
        AFL_VERIFY(!InsertedPortions.contains(writtenPortion->GetInsertWriteId()));
    } else {
        AFL_VERIFY(!InsertedPortions.contains((TInsertWriteId)0));
    }
    TDbWrapper wrapper(txc.DB, nullptr);
    portion.SaveToDatabase(wrapper, firstPKColumnId, false);
}

void TGranuleMeta::InsertPortionOnComplete(const TPortionDataAccessor& portion, IColumnEngine& /*engine*/) {
    auto portionImpl = portion.MutablePortionInfoPtr();
    AFL_VERIFY(portionImpl->GetPortionType() == EPortionType::Written);
    auto writtenPortion = std::static_pointer_cast<TWrittenPortionInfo>(portionImpl);
    AFL_VERIFY(InsertedPortions.emplace(writtenPortion->GetInsertWriteId(), writtenPortion).second);
    AFL_VERIFY(InsertedPortionsById.emplace(portionImpl->GetPortionId(), writtenPortion).second);
    AFL_VERIFY(InsertedAccessors.emplace(writtenPortion->GetInsertWriteId(), portion).second);
    DataAccessorsManager->AddPortion(portion);
}

void TGranuleMeta::CommitPortionOnExecute(
    NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId, const TSnapshot& snapshot) const {
    auto it = InsertedPortions.find(insertWriteId);
    AFL_VERIFY(it != InsertedPortions.end());
    it->second->SetCommitSnapshot(snapshot);
    TDbWrapper wrapper(txc.DB, nullptr);
    it->second->CommitToDatabase(wrapper);
}

void TGranuleMeta::CommitPortionOnComplete(const TInsertWriteId insertWriteId, IColumnEngine& engine) {
    auto it = InsertedPortions.find(insertWriteId);
    AFL_VERIFY(it != InsertedPortions.end());
    AFL_VERIFY(InsertedPortionsById.erase(it->second->GetPortionId()));
    (static_cast<TColumnEngineForLogs*>(&engine))->AppendPortion(it->second);
    InsertedPortions.erase(it);
    {
        auto it = InsertedAccessors.find(insertWriteId);
        if (it != InsertedAccessors.end()) {
            InsertedAccessors.erase(it);
        }
    }
}

void TGranuleMeta::CommitImmediateOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TSnapshot& snapshot,
    const TPortionDataAccessor& portion, const ui64 firstPKColumnId) const {
    auto portionImpl = portion.MutablePortionInfoPtr();
    AFL_VERIFY(portionImpl->GetPortionType() == EPortionType::Written);
    auto writtenPortion = std::static_pointer_cast<TWrittenPortionInfo>(portionImpl);

    AFL_VERIFY(!InsertedPortions.contains(writtenPortion->GetInsertWriteId()));
    writtenPortion->SetCommitSnapshot(snapshot);
    TDbWrapper wrapper(txc.DB, nullptr);
    portion.SaveToDatabase(wrapper, firstPKColumnId, false);
}

void TGranuleMeta::CommitImmediateOnComplete(const std::shared_ptr<TPortionInfo> /*portion*/, IColumnEngine& /*engine*/) {
    AFL_VERIFY(false);
    //    (static_cast<TColumnEngineForLogs&>(engine)).AppendPortion(portion);
}

}   // namespace NKikimr::NOlap
