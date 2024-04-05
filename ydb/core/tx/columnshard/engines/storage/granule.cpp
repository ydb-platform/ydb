#include "granule.h"
#include "storage.h"
#include "optimizer/lbuckets/optimizer.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TGranuleAdditiveSummary::ECompactionClass TGranuleMeta::GetCompactionType(const TCompactionLimits& limits) const {
    return GetAdditiveSummary().GetCompactionClass(
        limits, ModificationLastTime, TMonotonic::Now());
}

ui64 TGranuleMeta::Size() const {
    return GetAdditiveSummary().GetGranuleSize();
}

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

void TGranuleMeta::AddColumnRecordOnLoad(const TIndexInfo& indexInfo, const TPortionInfo& portion, const TColumnChunkLoadContext& rec, const NKikimrTxColumnShard::TIndexPortionMeta* portionMeta) {
    std::shared_ptr<TPortionInfo> pInfo = UpsertPortionOnLoad(portion);
    TColumnRecord cRecord(pInfo->RegisterBlobId(rec.GetBlobRange().GetBlobId()), rec, indexInfo.GetColumnFeaturesVerified(rec.GetAddress().GetColumnId()));
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "AddColumnRecordOnLoad")("portion_info", portion.DebugString())("record", cRecord.DebugString());
    pInfo->AddRecord(indexInfo, cRecord, portionMeta);
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
    }
    if (!!AdditiveSummaryCache) {
        auto g = AdditiveSummaryCache->StartEdit(Counters);
        if (portionAfter && !portionAfter->HasRemoveSnapshot()) {
            g.AddPortion(*portionAfter);
        }
    }

    ModificationLastTime = TMonotonic::Now();
    Owner->UpdateGranuleInfo(*this);
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
    }
    if (!!AdditiveSummaryCache) {
        auto g = AdditiveSummaryCache->StartEdit(Counters);
        if (portionBefore && !portionBefore->HasRemoveSnapshot()) {
            g.RemovePortion(*portionBefore);
        }
    }
}

void TGranuleMeta::OnCompactionFinished() {
    AllowInsertionFlag = false;
    Y_ABORT_UNLESS(Activity.erase(EActivity::GeneralCompaction));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFinished")("info", DebugString());
    Owner->UpdateGranuleInfo(*this);
}

void TGranuleMeta::OnCompactionFailed(const TString& reason) {
    AllowInsertionFlag = false;
    Y_ABORT_UNLESS(Activity.erase(EActivity::GeneralCompaction));
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompactionFailed")("reason", reason)("info", DebugString());
    Owner->UpdateGranuleInfo(*this);
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

TGranuleMeta::TGranuleMeta(const ui64 pathId, std::shared_ptr<TGranulesStorage> owner, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , Owner(owner)
    , Counters(counters)
    , PortionInfoGuard(Owner->GetCounters().BuildPortionBlobsGuard())
{
    Y_ABORT_UNLESS(Owner);
    OptimizerPlanner = std::make_shared<NStorageOptimizer::NBuckets::TOptimizerPlanner>(PathId, owner->GetStoragesManager(), versionedIndex.GetLastSchema()->GetIndexInfo().GetReplaceKey());
    ActualizationIndex = std::make_shared<NActualizer::TGranuleActualizationIndex>(PathId, versionedIndex);

}

bool TGranuleMeta::InCompaction() const {
    return Activity.contains(EActivity::GeneralCompaction);
}

std::shared_ptr<TPortionInfo> TGranuleMeta::UpsertPortionOnLoad(const TPortionInfo& portion) {
    auto it = Portions.find(portion.GetPortion());
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "UpsertPortionOnLoad")("portion_info", portion.DebugString());
    if (it == Portions.end()) {
        Y_ABORT_UNLESS(portion.Records.empty());
        auto portionNew = std::make_shared<TPortionInfo>(portion);
        it = Portions.emplace(portion.GetPortion(), portionNew).first;
    } else {
        AFL_VERIFY(it->second->IsEqualWithSnapshots(portion))("self", it->second->DebugString())("item", portion.DebugString());
    }
    return it->second;
}

} // namespace NKikimr::NOlap
