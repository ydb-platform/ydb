#include "granule.h"
#include "storage.h"
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/optimizer/optimizer.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>

namespace NKikimr::NOlap {

void TGranuleMeta::UpsertPortion(const TPortionInfo& info) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "upsert_portion")("portion", info.DebugString())("path_id", GetPathId());
    auto it = Portions.find(info.GetPortion());
    AFL_VERIFY(info.GetPathId() == GetPathId())("event", "incompatible_granule")("portion", info.DebugString())("path_id", GetPathId());

    AFL_VERIFY(info.Valid())("event", "invalid_portion")("portion", info.DebugString());
    AFL_VERIFY(info.ValidSnapshotInfo())("event", "incorrect_portion_snapshots")("portion", info.DebugString());
    for (auto& record : info.Records) {
        AFL_VERIFY(record.Valid())("event", "incorrect_record")("record", record.DebugString())("portion", info.DebugString());
    }

    if (it == Portions.end()) {
        OnBeforeChangePortion(nullptr);
        auto portionNew = std::make_shared<TPortionInfo>(info);
        it = Portions.emplace(portionNew->GetPortion(), portionNew).first;
    } else {
        OnBeforeChangePortion(it->second);
        it->second = std::make_shared<TPortionInfo>(info);
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
    OnBeforeChangePortion(it->second);
    Portions.erase(it);
    OnAfterChangePortion(nullptr, nullptr);
    return true;
}

void TGranuleMeta::OnAfterChangePortion(const std::shared_ptr<TPortionInfo> portionAfter, NStorageOptimizer::IOptimizerPlanner::TModificationGuard* modificationGuard) {
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
            ActualizationIndex->AddPortion(portionAfter, context);
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
    Y_ABORT_UNLESS(Activity.erase(EActivity::GeneralCompaction));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFinished")("info", DebugString());
    Stats->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionFailed(const TString& reason) {
    AllowInsertionFlag = false;
    Y_ABORT_UNLESS(Activity.erase(EActivity::GeneralCompaction));
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFailed")("reason", reason)("info", DebugString());
    Stats->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionStarted() {
    AllowInsertionFlag = false;
    Y_ABORT_UNLESS(Activity.empty());
    Activity.emplace(EActivity::GeneralCompaction);
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

TGranuleMeta::TGranuleMeta(const ui64 pathId, const TGranulesStorage& owner, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , Counters(counters)
    , PortionInfoGuard(owner.GetCounters().BuildPortionBlobsGuard())
    , Stats(owner.GetStats())
    , StoragesManager(owner.GetStoragesManager())
    , PortionsIndex(*this, Counters.GetPortionsIndexCounters()) {
    NStorageOptimizer::IOptimizerPlannerConstructor::TBuildContext context(PathId, owner.GetStoragesManager(), versionedIndex.GetLastSchema()->GetIndexInfo().GetPrimaryKey());
    OptimizerPlanner = versionedIndex.GetLastSchema()->GetIndexInfo().GetCompactionPlannerConstructor()->BuildPlanner(context).DetachResult();
    AFL_VERIFY(!!OptimizerPlanner);
    ActualizationIndex = std::make_shared<NActualizer::TGranuleActualizationIndex>(PathId, versionedIndex);

}

std::shared_ptr<TPortionInfo> TGranuleMeta::UpsertPortionOnLoad(TPortionInfo&& portion) {
    auto portionId = portion.GetPortionId();
    auto emplaceInfo = Portions.emplace(portionId, std::make_shared<TPortionInfo>(std::move(portion)));
    AFL_VERIFY(emplaceInfo.second);
    return emplaceInfo.first->second;
}

void TGranuleMeta::BuildActualizationTasks(NActualizer::TTieringProcessContext& context, const TDuration actualizationLag) const {
    if (context.GetActualInstant() < NextActualizations) {
        return;
    }
    NActualizer::TExternalTasksContext extTasks(Portions);
    ActualizationIndex->ExtractActualizationTasks(context, extTasks);
    NextActualizations = context.GetActualInstant() + actualizationLag;
}

void TGranuleMeta::ResetOptimizer(const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& constructor, std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema) {
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

} // namespace NKikimr::NOlap
