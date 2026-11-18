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

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap {

namespace {

NColumnShard::EPortionGroup GetCommittedPortionGroup(const TPortionInfo& portion) {
    AFL_VERIFY(portion.IsCommitted())("portion", portion.DebugString());
    if (portion.HasRemoveSnapshot()) {
        return NColumnShard::EPortionGroup::Removed;
    }
    return portion.GetPortionType() == EPortionType::Written ? NColumnShard::EPortionGroup::Written : NColumnShard::EPortionGroup::Compacted;
}

}   // namespace

void TGranuleMeta::AppendPortion(const std::shared_ptr<TPortionInfo>& info) {
    YDB_LOG_TRACE("",
        {"event", "upsert_portion"},
        {"portion", info->DebugString()},
        {"pathId", GetPathId()});
    YDB_LOG_VERIFY(!Portions.contains(info->GetPortionId()));
    YDB_LOG_VERIFY(info->GetPathId() == GetPathId(),
        {"event", "incompatible_granule"},
        {"portion", info->DebugString()},
        {"pathId", GetPathId()});

    YDB_LOG_VERIFY(info->ValidSnapshotInfo(),
        {"event", "incorrect_portion_snapshots"},
        {"portion", info->DebugString()});

    Portions.emplace(info->GetPortionId(), info);
    OnAddCommittedPortion(info);

    if (IntervalTree) {
        IntervalTree->AddRange(
            PortionIntervalTree::TPortionIntervalTree::TOwnedRange(PortionIntervalTree::TPositionView::FromPortionInfoIndexStart(info), true,
                PortionIntervalTree::TPositionView::FromPortionInfoIndexEnd(info), true), info);
    }
}

void TGranuleMeta::AppendPortion(const std::shared_ptr<TPortionDataAccessor>& info) {
    AppendPortion(info->MutablePortionInfoPtr());
    DataAccessorsManager->AddPortion(info);
}

bool TGranuleMeta::ErasePortion(const ui64 portion) {
    auto it = Portions.find(portion);
    if (it == Portions.end()) {
        YDB_LOG_WARN("",
            {"event", "portion_erased_already"},
            {"portionId", portion},
            {"pathId", PathId});
        return false;
    } else {
        YDB_LOG_TRACE("",
            {"event", "portion_erased"},
            {"portionInfo", it->second->DebugString()},
            {"pathId", PathId});
    }
    if (IntervalTree) {
        IntervalTree->RemoveRanges(it->second);
    }
    DataAccessorsManager->RemovePortion(it->second);
    OnRemoveCommittedPortion(it->second);
    Portions.erase(it);
    return true;
}

void TGranuleMeta::OnAddCommittedPortion(const std::shared_ptr<TPortionInfo>& portion, const bool refreshCounters, const bool onLoad) {
    DataStats->AddPortion(portion, GetCommittedPortionGroup(*portion));
    if (!portion->HasRemoveSnapshot()) {
        PortionsIndex.AddPortion(portion);
        // a loaded portion goes to the optimizer together with the rest of its batch
        if (!onLoad) {
            OptimizerPlanner->StartModificationGuard().AddPortion(portion);
        }
        if (ActualizationIndex->IsStarted()) {
            NActualizer::TAddExternalContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now(), Portions);
            ActualizationIndex->AddPortion(portion, context);
        }
    }
    if (refreshCounters) {
        RefreshDataCounters();
    }
}

void TGranuleMeta::OnRemoveCommittedPortion(const std::shared_ptr<TPortionInfo>& portion, const bool refreshCounters) {
    DataStats->RemovePortion(portion, GetCommittedPortionGroup(*portion));
    if (!portion->HasRemoveSnapshot()) {
        PortionsIndex.RemovePortion(portion);
        OptimizerPlanner->StartModificationGuard().RemovePortion(portion);
        ActualizationIndex->RemovePortion(portion);
    }
    if (refreshCounters) {
        RefreshDataCounters();
    }
}

void TGranuleMeta::RefreshDataCounters() const {
    const NColumnShard::TDataStatsByStorage& stats = GetDataStats();
    Counters.OnPortionsDataRefresh(stats.GetWritten(), stats.GetCompacted());
}

void TGranuleMeta::AddInsertedPortion(const std::shared_ptr<TWrittenPortionInfo>& portion) {
    AFL_VERIFY(!portion->IsCommitted())("portion", portion->DebugString());
    AFL_VERIFY(InsertedPortions.emplace(portion->GetInsertWriteId(), portion).second);
    AFL_VERIFY(InsertedPortionsById.emplace(portion->GetPortionId(), portion).second);
    DataStats->AddPortion(portion, NColumnShard::EPortionGroup::Uncommitted);
}

std::shared_ptr<TWrittenPortionInfo> TGranuleMeta::ExtractInsertedPortion(const TInsertWriteId insertWriteId) {
    auto it = InsertedPortions.find(insertWriteId);
    AFL_VERIFY(it != InsertedPortions.end());
    std::shared_ptr<TWrittenPortionInfo> portion = it->second;
    InsertedPortions.erase(it);
    AFL_VERIFY(InsertedPortionsById.erase(portion->GetPortionId()));
    InsertedAccessors.erase(insertWriteId);
    DataStats->RemovePortion(portion, NColumnShard::EPortionGroup::Uncommitted);
    return portion;
}

const NColumnShard::TDataStatsByStorage& TGranuleMeta::GetDataStats() const {
    return DataStats->GetGranuleStats(PathId);
}

void TGranuleMeta::OnCompactionFinished() {
    AllowInsertionFlag = false;
    YDB_LOG_DEBUG("",
        {"event", "OnCompactionFinished"},
        {"info", DebugString()});
}

void TGranuleMeta::OnCompactionFailed(const TString& reason) {
    AllowInsertionFlag = false;
    YDB_LOG_WARN("",
        {"event", "OnCompactionFailed"},
        {"reason", reason},
        {"info", DebugString()});
}

void TGranuleMeta::OnCompactionStarted() {
    AllowInsertionFlag = false;
}

TGranuleMeta::TGranuleMeta(const TInternalPathId pathId, const TGranulesStorage& owner, const NColumnShard::TGranuleDataCounters& counters,
    const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , DataAccessorsManager(owner.GetDataAccessorsManager())
    , Counters(counters)
    , DataStats(owner.GetDataStats())
    , StoragesManager(owner.GetStoragesManager())
    , PortionsIndex(*this, Counters.GetPortionsIndexCounters())
{
    NStorageOptimizer::IOptimizerPlannerConstructor::TBuildContext context(
        PathId, owner.GetStoragesManager(), versionedIndex.GetLastSchema()->GetIndexInfo().GetPrimaryKey(), owner.GetOptimizerRuntimeSettings());
    OptimizerPlanner = versionedIndex.GetLastSchema()->GetIndexInfo().GetCompactionPlannerConstructor()->BuildPlanner(context).DetachResult();
    NDataAccessorControl::TManagerConstructionContext mmContext(DataAccessorsManager->GetTabletActorId(), false);
    ResetAccessorsManager(versionedIndex.GetLastSchema()->GetIndexInfo().GetMetadataManagerConstructor(), mmContext);
    YDB_LOG_VERIFY(!!OptimizerPlanner);
    ActualizationIndex = std::make_unique<NActualizer::TGranuleActualizationIndex>(PathId, versionedIndex, StoragesManager);
    if (HasAppData() && AppData()->ColumnShardConfig.GetEnableIntervalTreeForMetadataSelect()) {
        IntervalTree = std::make_unique<PortionIntervalTree::TPortionIntervalTree>();
    }
}

void TGranuleMeta::UpsertPortionOnLoad(const std::shared_ptr<TPortionInfo>& portion) {
    if (!portion->IsCommitted()) {
        YDB_LOG_VERIFY(!Portions.contains(portion->GetPortionId()));
        AddInsertedPortion(std::static_pointer_cast<TWrittenPortionInfo>(portion));
    } else {
        auto portionId = portion->GetPortionId();
        YDB_LOG_VERIFY(Portions.emplace(portionId, portion).second);
        if (IntervalTree) {
            IntervalTree->AddRange(
                PortionIntervalTree::TPortionIntervalTree::TOwnedRange(PortionIntervalTree::TPositionView::FromPortionInfoIndexStart(portion),
                    true, PortionIntervalTree::TPositionView::FromPortionInfoIndexEnd(portion), true), portion);
        }
        OnAddCommittedPortion(portion, /*refreshCounters=*/false, /*onLoad=*/true);
    }
}

void TGranuleMeta::AddPortionsToOptimizer(const std::vector<std::shared_ptr<TPortionInfo>>& activePortions) {
    OptimizerPlanner->AddLoadedPortions(activePortions);
}

std::vector<std::shared_ptr<TPortionInfo>> TGranuleMeta::AddPortionsLoadedInBatch(
    const std::vector<std::shared_ptr<TPortionInfo>>& portions, TLoadedKeyRanges&& loadedKeyRanges) {
    std::vector<std::shared_ptr<TPortionInfo>> loaded;
    std::vector<std::shared_ptr<TPortionInfo>> active;
    for (const auto& portion : portions) {
        if (!portion->IsCommitted()) {
            // uncommitted portions are all in memory: loaded by TTxInit or added by their write
            YDB_LOG_VERIFY(InsertedPortionsById.contains(portion->GetPortionId()),
                {"portionId", portion->GetPortionId()});
            continue;
        }
        // compaction output below the loaded keys is already in memory.
        // if a portion gets deleted concurrently with the loading, its transaction Complete will be executed
        // after this one and delete the portion just fine.
        if (Portions.contains(portion->GetPortionId())) {
            continue;
        }
        UpsertPortionOnLoad(portion);
        loaded.emplace_back(portion);
        if (!portion->HasRemoveSnapshot()) {
            active.emplace_back(portion);
        }
    }
    AddPortionsToOptimizer(active);
    LoadedKeyRanges = std::move(loadedKeyRanges);
    RefreshDataCounters();
    return loaded;
}

std::vector<std::shared_ptr<TPortionInfo>> TGranuleMeta::GetActivePortions() const {
    std::vector<std::shared_ptr<TPortionInfo>> result;
    for (const auto& [_, portion] : Portions) {
        if (!portion->HasRemoveSnapshot()) {
            result.emplace_back(portion);
        }
    }
    return result;
}

void TGranuleMeta::BuildActualizationTasks(NActualizer::TTieringProcessContext& context, const TDuration actualizationLag) const {
    if (context.GetActualInstant() < NextActualizations) {
        YDB_LOG_DEBUG("",
            {"event", "skip_actualization"},
            {"waiting", NextActualizations - context.GetActualInstant()});
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
        YDB_LOG_NOTICE("",
            {"event", "applied_optimizer"},
            {"constructor", constructor->GetClassName()});
        return;
    }
    YDB_LOG_NOTICE("",
        {"event", "reset_optimizer"},
        {"constructor", constructor->GetClassName()});
    NStorageOptimizer::IOptimizerPlannerConstructor::TBuildContext context(PathId, storages, pkSchema, OptimizerPlanner->GetRuntimeSettings());
    OptimizerPlanner = constructor->BuildPlanner(context).DetachResult();
    YDB_LOG_VERIFY(!!OptimizerPlanner);
    AddPortionsToOptimizer(GetActivePortions());
}

/*

void TGranuleMeta::ResetMetadataManager(const std::shared_ptr<NDataAccessorControl::IManagerConstructor>& constructor,
    std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if (constructor->ApplyToCurrentObject(MetadataMemoryManager)) {
        return;
    }
    NStorageOptimizer::IManagerConstructor::TBuildContext context(PathId, storages, pkSchema);
    MetadataMemoryManager = constructor->Build(context).DetachResult();
    YDB_LOG_VERIFY(!!OptimizerPlanner);
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
    const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector, TColumnEngineForLogs& engine) {
    auto portionsLoader = std::make_shared<NLoading::TGranuleOnlyPortionsReader>("portions", this, &engine, dsGroupSelector);
    auto metadataLoader = MetadataMemoryManager->BuildLoader(engine.GetVersionedIndex(), this, dsGroupSelector);
    auto optimizerLoader = std::make_shared<NLoading::TGranuleOptimizerLoading>("optimizer", this);

    auto result = std::make_shared<TTxCompositeReader>("granule");
    result->AddNameSuffix(":" + ::ToString(GetPathId()));
    result->AddChildren(portionsLoader);
    if (metadataLoader) {
        result->AddChildren(metadataLoader);
    }
    result->AddChildren(optimizerLoader);
    return result;
}

bool TGranuleMeta::TestingLoad(IDbWrapper& db, TColumnEngineForLogs& engine) {
    TInGranuleConstructors constructors;
    const TVersionedIndex& versionedIndex = engine.GetVersionedIndex();
    {
        if (!db.LoadPortions([&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
                YDB_LOG_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, db.GetDsGroupSelectorVerified()));
                YDB_LOG_VERIFY(constructors.AddConstructorVerified(std::move(portion)));
                return true;
            }, PathId)) {
            return false;
        }
    }

    {
        if (!db.LoadColumns([&](TColumnChunkLoadContextV2&& loadContext) {
                auto* constructor = constructors.GetConstructorVerified(loadContext.GetPortionId());
                constructor->AddBuildInfo(loadContext.CreateBuildInfo());
            }, PathId)) {
            return false;
        }
    }

    {
        if (!db.LoadIndexes([&](const TInternalPathId /*pathId*/, const ui64 portionId, TIndexChunkLoadContext&& loadContext) {
                auto* constructor = constructors.GetConstructorVerified(portionId);
                constructor->LoadIndex(std::move(loadContext));
                return true;
            }, PathId)) {
            return false;
        };
    }
    for (auto&& [portionId, constructor] : constructors) {
        auto accessor = constructor.Build(false);
        DataAccessorsManager->AddPortion(accessor);
        engine.AddPortionLoadedOnStart(*this, accessor->MutablePortionInfoPtr());
    }
    return true;
}

void TGranuleMeta::InsertPortionOnExecute(
    NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TPortionDataAccessor>& portion, const ui64 firstPKColumnId) const {
    auto portionImpl = portion->MutablePortionInfoPtr();
    if (portionImpl->GetPortionType() == EPortionType::Written) {
        auto writtenPortion = std::static_pointer_cast<TWrittenPortionInfo>(portionImpl);
        YDB_LOG_VERIFY(!InsertedPortions.contains(writtenPortion->GetInsertWriteId()));
    } else {
        YDB_LOG_VERIFY(!InsertedPortions.contains((TInsertWriteId)0));
    }
    TDbWrapper wrapper(txc.DB, nullptr);
    portion->SaveToDatabase(wrapper, firstPKColumnId, false);
}

void TGranuleMeta::InsertPortionOnComplete(const std::shared_ptr<TPortionDataAccessor>& portion, IColumnEngine& /*engine*/) {
    auto portionImpl = portion->MutablePortionInfoPtr();
    YDB_LOG_VERIFY(portionImpl->GetPortionType() == EPortionType::Written);
    auto writtenPortion = std::static_pointer_cast<TWrittenPortionInfo>(portionImpl);
    AddInsertedPortion(writtenPortion);
    YDB_LOG_VERIFY(InsertedAccessors.emplace(writtenPortion->GetInsertWriteId(), portion).second);
    DataAccessorsManager->AddPortion(portion);
}

void TGranuleMeta::CommitPortionOnExecute(
    NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId, const TSnapshot& snapshot) const {
    auto it = InsertedPortions.find(insertWriteId);
    YDB_LOG_VERIFY(it != InsertedPortions.end());
    it->second->SetCommitSnapshot(snapshot);
    TDbWrapper wrapper(txc.DB, nullptr);
    it->second->CommitToDatabase(wrapper);
}

void TGranuleMeta::CommitPortionOnComplete(const TInsertWriteId insertWriteId, IColumnEngine& engine) {
    static_cast<TColumnEngineForLogs&>(engine).AppendPortion(ExtractInsertedPortion(insertWriteId));
}

void TGranuleMeta::CommitImmediateOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TSnapshot& snapshot,
    const TPortionDataAccessor& portion, const ui64 firstPKColumnId) const {
    auto portionImpl = portion.MutablePortionInfoPtr();
    YDB_LOG_VERIFY(portionImpl->GetPortionType() == EPortionType::Written);
    auto writtenPortion = std::static_pointer_cast<TWrittenPortionInfo>(portionImpl);

    YDB_LOG_VERIFY(!InsertedPortions.contains(writtenPortion->GetInsertWriteId()));
    writtenPortion->SetCommitSnapshot(snapshot);
    TDbWrapper wrapper(txc.DB, nullptr);
    portion.SaveToDatabase(wrapper, firstPKColumnId, false);
}

}   // namespace NKikimr::NOlap
