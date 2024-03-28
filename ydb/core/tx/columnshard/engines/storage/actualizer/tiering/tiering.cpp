#include "tiering.h"
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

std::shared_ptr<NKikimr::NOlap::ISnapshotSchema> TTieringActualizer::GetTargetSchema(const std::shared_ptr<ISnapshotSchema>& portionSchema) const {
    if (!TargetCriticalSchema) {
        return portionSchema;
    }
    if (portionSchema->GetVersion() < TargetCriticalSchema->GetVersion()) {
        return TargetCriticalSchema;
    }
    return portionSchema;
}

std::optional<TTieringActualizer::TFullActualizationInfo> TTieringActualizer::BuildActualizationInfo(const TPortionInfo& portion, const TInstant now) const {
    std::shared_ptr<ISnapshotSchema> portionSchema = VersionedIndex.GetSchema(portion.GetMinSnapshot());
    std::shared_ptr<ISnapshotSchema> targetSchema = GetTargetSchema(portionSchema);
    const TString& currentTierName = portion.GetTierNameDef(IStoragesManager::DefaultStorageId);

    if (Tiering) {
        AFL_VERIFY(TieringColumnId);
        auto statOperator = portionSchema->GetIndexInfo().GetStatistics(NStatistics::TIdentifier(NStatistics::EType::Max, {*TieringColumnId}));
        std::shared_ptr<arrow::Scalar> max;
        if (!statOperator) {
            max = portion.MaxValue(*TieringColumnId);
            if (!max) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_not_max");
                return {};
            }
        } else {
            NYDBTest::TControllers::GetColumnShardController()->OnStatisticsUsage(statOperator);
            max = statOperator.GetScalarVerified(portion.GetMeta().GetStatisticsStorage());
        }
        auto tieringInfo = Tiering->GetTierToMove(max, now);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("tiering_info", tieringInfo.DebugString());
        std::optional<i64> d;
        std::set<TString> storagesWrite;
        TString targetTierName;
        if (portion.GetTierNameDef(IStoragesManager::DefaultStorageId) != tieringInfo.GetCurrentTierName()) {
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
            return TFullActualizationInfo(TRWAddress(std::move(storagesRead), std::move(storagesWrite)), targetTierName, *d, targetSchema);
        }
    } else if (currentTierName != IStoragesManager::DefaultStorageId) {
        //        if (currentTierName == "deploy_logs_s3") {
        //            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("pathId", PathId);
        //            AFL_VERIFY(false)("pathId", PathId);
        //        }
        auto storagesWrite = targetSchema->GetIndexInfo().GetUsedStorageIds(IStoragesManager::DefaultStorageId);
        auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
        return TFullActualizationInfo(std::move(address), IStoragesManager::DefaultStorageId, 0, targetSchema);
    }
    return {};
}

void TTieringActualizer::DoAddPortion(const TPortionInfo& portion, const TAddExternalContext& addContext) {
    if (!addContext.GetPortionExclusiveGuarantee()) {
        if (PortionsInfo.contains(portion.GetPortionId())) {
            return;
        }
    } else {
        AFL_VERIFY(!PortionsInfo.contains(portion.GetPortionId()));
    }
    auto info = BuildActualizationInfo(portion, addContext.GetNow());
    if (!info) {
        return;
    }
    AFL_VERIFY(PortionIdByWaitDuration[info->GetAddress()].AddPortion(*info, portion.GetPortionId(), addContext.GetNow() - StartInstant));
    auto address = info->GetAddress();
    TFindActualizationInfo findId(std::move(address), info->GetWaitDuration() + (addContext.GetNow() - StartInstant));
    AFL_VERIFY(PortionsInfo.emplace(portion.GetPortionId(), std::move(findId)).second);
}

void TTieringActualizer::DoRemovePortion(const ui64 portionId) {
    auto it = PortionsInfo.find(portionId);
    if (it == PortionsInfo.end()) {
        return;
    }
    auto itAddress = PortionIdByWaitDuration.find(it->second.GetRWAddress());
    AFL_VERIFY(itAddress != PortionIdByWaitDuration.end());
    if (itAddress->second.RemovePortion(it->second, portionId)) {
        PortionIdByWaitDuration.erase(itAddress);
    }
    PortionsInfo.erase(it);
}

void TTieringActualizer::DoExtractTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& /*internalContext*/) {
    THashSet<ui64> portionIds;
    for (auto&& [address, addressPortions] : PortionIdByWaitDuration) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            Counters.SkipEvictionForLimit->Add(1);
            continue;
        }
        for (auto&& [duration, portions] : addressPortions.GetPortions()) {
            if (duration - (tasksContext.Now - StartInstant) > TDuration::Zero()) {
                break;
            }
            bool limitEnriched = false;
            for (auto&& p : portions) {
                auto portion = externalContext.GetPortionVerified(p);
                if (!address.WriteIs(NBlobOperations::TGlobal::DefaultStorageId) && !address.WriteIs(NTiering::NCommon::DeleteTierName)) {
                    if (!portion->HasRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized)) {
                        Counters.SkipEvictionForCompaction->Add(1);
                        continue;
                    }
                }
                auto info = BuildActualizationInfo(*portion, tasksContext.Now);
                AFL_VERIFY(info);
                auto portionScheme = VersionedIndex.GetSchema(portion->GetMinSnapshot());
                TPortionEvictionFeatures features(portionScheme, info->GetTargetScheme(), portion->GetTierNameDef(IStoragesManager::DefaultStorageId));
                features.SetTargetTierName(info->GetTargetTierName());

                if (!tasksContext.AddPortion(*portion, std::move(features), info->GetLateness())) {
                    limitEnriched = true;
                    break;
                } else {
                    portionIds.emplace(portion->GetPortionId());
                }
            }
            if (limitEnriched) {
                break;
            }
        }
    }
    for (auto&& i : portionIds) {
        RemovePortion(i);
    }

    ui64 waitDurationEvict = 0;
    ui64 waitQueueEvict = 0;
    ui64 waitDurationDelete = 0;
    ui64 waitQueueDelete = 0;
    for (auto&& i : PortionIdByWaitDuration) {
        std::shared_ptr<NColumnShard::TValueAggregationClient> waitDurationSignal;
        std::shared_ptr<NColumnShard::TValueAggregationClient> queueSizeSignal;
        if (i.first.WriteIs(NTiering::NCommon::DeleteTierName)) {
            i.second.CorrectSignals(waitQueueDelete, waitDurationDelete, tasksContext.Now - StartInstant);
        } else {
            i.second.CorrectSignals(waitQueueEvict, waitDurationEvict, tasksContext.Now - StartInstant);
        }
    }
    Counters.DifferenceWaitToDelete->SetValue(waitDurationDelete);
    Counters.DifferenceWaitToEvict->SetValue(waitDurationEvict);
    Counters.QueueSizeToDelete->SetValue(waitQueueDelete);
    Counters.QueueSizeToEvict->SetValue(waitQueueEvict);

}

void TTieringActualizer::Refresh(const std::optional<TTiering>& info, const TAddExternalContext& externalContext) {
    StartInstant = externalContext.GetNow();
    Tiering = info;
    if (Tiering) {
        TieringColumnId = VersionedIndex.GetLastSchema()->GetColumnId(Tiering->GetTtlColumn());
    } else {
        TieringColumnId = {};
    }
    TargetCriticalSchema = VersionedIndex.GetLastCriticalSchema();
    PortionsInfo.clear();
    PortionIdByWaitDuration.clear();

    for (auto&& i : externalContext.GetPortions()) {
        AddPortion(i.second, externalContext);
    }
}

}
