#include "index.h"
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

void TGranuleActualizationIndex::BuildActualizationTasks(NActualizer::TTieringProcessContext& context, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portionInfos) const {
    THashSet<ui64> usedPortions;
    for (auto&& [address, addressPortions] : PortionIdByWaitDuration) {
        if (!context.IsRWAddressAvailable(address)) {
            break;
        }
        for (auto&& [duration, portions] : addressPortions) {
            if (duration - (context.Now - StartInstant) > TDuration::Zero()) {
                break;
            }
            bool limitEnriched = false;
            for (auto&& p : portions) {
                auto itPortionInfo = portionInfos.find(p);
                AFL_VERIFY(itPortionInfo != portionInfos.end());
                if (context.DataLocksManager->IsLocked(*itPortionInfo->second)) {
                    continue;
                }
                auto portion = itPortionInfo->second;

                auto info = BuildActualizationInfo(portion, context.Now);
                auto portionScheme = VersionedIndex.GetSchema(portion->GetMinSnapshot());
                TPortionEvictionFeatures features(portionScheme, info.GetScheme() ? info.GetScheme()->GetTargetScheme() : portionScheme, portion->GetTierNameDef(IStoragesManager::DefaultStorageId));

                std::optional<TDuration> lateness;
                if (info.GetEviction()) {
                    features.SetTargetTierName(info.GetEviction()->GetTargetTierName());
                    lateness = info.GetEviction()->GetLateness();
                }

                if (!context.AddPortion(*portion, std::move(features), lateness, context.Now)) {
                    limitEnriched = true;
                    break;
                }
            }
            if (limitEnriched) {
                break;
            }
        }
    }
    for (auto&& [address, portions] : PortionsToActualizeScheme) {
        if (!context.IsRWAddressAvailable(address)) {
            break;
        }
        for (auto&& portionId : portions) {
            if (!usedPortions.emplace(portionId).second) {
                continue;
            }
            auto itPortionInfo = portionInfos.find(portionId);
            AFL_VERIFY(itPortionInfo != portionInfos.end());
            auto portion = itPortionInfo->second;
            if (context.DataLocksManager->IsLocked(*portion)) {
                continue;
            }

            auto info = BuildActualizationInfo(portion, context.Now);
            auto portionScheme = VersionedIndex.GetSchema(portion->GetMinSnapshot());
            TPortionEvictionFeatures features(portionScheme, info.GetScheme() ? info.GetScheme()->GetTargetScheme() : portionScheme, portion->GetTierNameDef(IStoragesManager::DefaultStorageId));

            if (!context.AddPortion(*portion, std::move(features), {}, context.Now)) {
                break;
            }
        }
    }
}

TGranuleActualizationIndex::TActualizationInfo TGranuleActualizationIndex::BuildActualizationInfo(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) const {
    const auto portionSchema = VersionedIndex.GetSchema(portion->GetMinSnapshot());
    auto targetSchema = ActualCriticalScheme ? ActualCriticalScheme : portionSchema;
    const auto currentTierName = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
    TActualizationInfo result;
    if (Tiering) {
        AFL_VERIFY(TieringColumnId);
        auto statOperator = portionSchema->GetIndexInfo().GetStatistics(NStatistics::TIdentifier(NStatistics::EType::Max, {*TieringColumnId}));
        std::shared_ptr<arrow::Scalar> max;
        if (!statOperator) {
            max = portion->MaxValue(*TieringColumnId);
            if (!max) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_not_max");
                return result;
            }
        } else {
            NYDBTest::TControllers::GetColumnShardController()->OnStatisticsUsage(statOperator);
            max = statOperator.GetScalarVerified(portion->GetMeta().GetStatisticsStorage());
        }
        auto tieringInfo = Tiering->GetTierToMove(max, now);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("tiering_info", tieringInfo.DebugString());
        std::optional<i64> d;
        std::set<TString> storagesWrite;
        TString targetTierName;
        if (portion->GetTierNameDef(IStoragesManager::DefaultStorageId) != tieringInfo.GetCurrentTierName()) {
            d = -1 * tieringInfo.GetCurrentTierLag().GetValue();
            targetTierName = tieringInfo.GetCurrentTierName();
        } else if (tieringInfo.GetNextTierName()) {
            d = tieringInfo.GetNextTierWaitingVerified().GetValue();
            targetTierName = tieringInfo.GetNextTierNameVerified();
        }
        if (d) {
//            if (currentTierName == "deploy_logs_s3" && targetTierName == IStoragesManager::DefaultStorageId) {
//                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("tiering_info", tieringInfo.DebugString())("max", max->ToString())("now", now.ToString())("d", *d)("tiering", Tiering->GetDebugString())("pathId", PathId);
//                AFL_VERIFY(false)("tiering_info", tieringInfo.DebugString())("max", max->ToString())("now", now.ToString())("d", *d)("tiering", Tiering->GetDebugString())("pathId", PathId);
//            }
            auto storagesWrite = targetSchema->GetIndexInfo().GetUsedStorageIds(targetTierName);
            auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
            TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
            result.SetEviction(std::move(address), targetTierName, *d);
            if (*d < (i64)TDuration::Minutes(1).Seconds()) {
                return result;
            }
        }
    } else if (currentTierName != IStoragesManager::DefaultStorageId) {
//        if (currentTierName == "deploy_logs_s3") {
//            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("pathId", PathId);
//            AFL_VERIFY(false)("pathId", PathId);
//        }
        auto storagesWrite = targetSchema->GetIndexInfo().GetUsedStorageIds(IStoragesManager::DefaultStorageId);
        auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
        result.SetEviction(std::move(address), IStoragesManager::DefaultStorageId, 0);
        return result;
    }
    if (portionSchema->GetVersion() < targetSchema->GetVersion()) {
        auto storagesWrite = targetSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
        result.SetScheme(std::move(address), targetSchema);
    }
    return result;
}

void TGranuleActualizationIndex::AddPortion(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) {
    if (!Started) {
        return;
    }
    if (portion->HasRemoveSnapshot()) {
        return;
    }
    TMemoryProfileGuard mGuard("ActualizationIndex");
    auto actualizationInfo = BuildActualizationInfo(portion, now);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("actualization_info", actualizationInfo.DebugString());
    TPortionIndexInfo result;
    if (actualizationInfo.GetEviction()) {
        auto& eviction = *actualizationInfo.GetEviction();
        PortionIdByWaitDuration[eviction.GetAddress()][eviction.GetWaitDuration() + (now - StartInstant)].emplace(portion->GetPortionId());
        auto address = eviction.GetAddress();
        result.SetEviction(std::move(address), eviction.GetWaitDuration() + (now - StartInstant));
    }
    if (actualizationInfo.GetScheme()) {
        auto& scheme = *actualizationInfo.GetScheme();
        PortionsToActualizeScheme[scheme.GetAddress()].emplace(portion->GetPortionId());
        auto address = scheme.GetAddress();
        result.SetScheme(std::move(address));
    }
    result.AddCounters(Counters, portion);
    if (!result.IsEmpty()) {
        AFL_VERIFY(PortionsInfo.emplace(portion->GetPortionId(), std::move(result)).second);
    }
}

void TGranuleActualizationIndex::RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
    auto it = PortionsInfo.find(portion->GetPortionId());
    if (it == PortionsInfo.end()) {
        return;
    }
    if (portion->HasRemoveSnapshot()) {
        return;
    }
    if (it->second.GetEviction()) {
        auto itAddress = PortionIdByWaitDuration.find(it->second.GetEviction()->GetRWAddress());
        AFL_VERIFY(itAddress != PortionIdByWaitDuration.end());
        auto itDuration = itAddress->second.find(it->second.GetEviction()->GetWaitDuration());
        AFL_VERIFY(itDuration != itAddress->second.end());
        AFL_VERIFY(itDuration->second.erase(portion->GetPortionId()));
        if (itDuration->second.empty()) {
            itAddress->second.erase(itDuration);
        }
        if (itAddress->second.empty()) {
            PortionIdByWaitDuration.erase(itAddress);
        }
    }

    if (it->second.GetScheme()) {
        auto itAddress = PortionsToActualizeScheme.find(it->second.GetScheme()->GetRWAddress());
        AFL_VERIFY(itAddress != PortionsToActualizeScheme.end());
        AFL_VERIFY(itAddress->second.erase(portion->GetPortionId()));
        if (itAddress->second.empty()) {
            PortionsToActualizeScheme.erase(itAddress);
        }
    }
    it->second.RemoveCounters(Counters, portion);
    PortionsInfo.erase(it);
}

void TGranuleActualizationIndex::Rebuild(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions) {
    if (!Started) {
        return;
    }
    StartInstant = HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
    PortionIdByWaitDuration.clear();
    PortionsToActualizeScheme.clear();
    PortionsInfo.clear();
    for (auto&& [_, portion] : portions) {
        if (portion->HasRemoveSnapshot()) {
            continue;
        }
        AddPortion(portion, StartInstant);
    }
}

bool TGranuleActualizationIndex::RefreshTiering(const std::optional<TTiering>& info) {
    AFL_VERIFY(Started);
    Tiering = info;
    if (Tiering) {
        TieringColumnId = VersionedIndex.GetLastSchema()->GetColumnId(Tiering->GetTtlColumn());
    } else {
        TieringColumnId = {};
    }
    return true;
}

bool TGranuleActualizationIndex::RefreshScheme() {
    AFL_VERIFY(Started);
    if (!ActualCriticalScheme) {
        if (!VersionedIndex.GetLastCriticalSchema()) {
            return false;
        }
    } else if (ActualCriticalScheme->GetVersion() == VersionedIndex.GetLastCriticalSchemaDef(ActualCriticalScheme)->GetVersion()) {
        return false;
    }
    ActualCriticalScheme = VersionedIndex.GetLastCriticalSchema();
    return true;
}

TGranuleActualizationIndex::TGranuleActualizationIndex(const ui64 pathId, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , VersionedIndex(versionedIndex)
    , StartInstant(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now())
{
    Y_UNUSED(PathId);
}

}
