#include "granule.h"
#include "storage.h"
#include "optimizer/lbuckets/optimizer.h"

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
        AFL_VERIFY(PortionsByPK[portionAfter->IndexKeyStart()].emplace(portionAfter->GetPortion(), portionAfter).second);

        PortionInfoGuard.OnNewPortion(portionAfter);
        if (!portionAfter->HasRemoveSnapshot()) {
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
        {
            auto itByKey = PortionsByPK.find(portionBefore->IndexKeyStart());
            Y_ABORT_UNLESS(itByKey != PortionsByPK.end());
            auto itPortion = itByKey->second.find(portionBefore->GetPortion());
            Y_ABORT_UNLESS(itPortion != itByKey->second.end());
            itByKey->second.erase(itPortion);
            if (itByKey->second.empty()) {
                PortionsByPK.erase(itByKey);
            }
        }

        PortionInfoGuard.OnDropPortion(portionBefore);
        if (!portionBefore->HasRemoveSnapshot()) {
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
{
    OptimizerPlanner = std::make_shared<NStorageOptimizer::NBuckets::TOptimizerPlanner>(PathId, owner.GetStoragesManager(), versionedIndex.GetLastSchema()->GetIndexInfo().GetReplaceKey());
    ActualizationIndex = std::make_shared<NActualizer::TGranuleActualizationIndex>(PathId, versionedIndex);

}

std::shared_ptr<TPortionInfo> TGranuleMeta::UpsertPortionOnLoad(TPortionInfo&& portion) {
    auto portionId = portion.GetPortionId();
    auto emplaceInfo = Portions.emplace(portionId, std::make_shared<TPortionInfo>(std::move(portion)));
    AFL_VERIFY(emplaceInfo.second);
    return emplaceInfo.first->second;
}

void TGranuleMeta::BuildActualizationTasks(NActualizer::TTieringProcessContext& context, const TDuration actualizationLag) const {
    if (context.GetActualInstant() - LastActualizations < actualizationLag) {
        return;
    }
    NActualizer::TExternalTasksContext extTasks(Portions);
    ActualizationIndex->ExtractActualizationTasks(context, extTasks);
    LastActualizations = context.GetActualInstant();
}

} // namespace NKikimr::NOlap
