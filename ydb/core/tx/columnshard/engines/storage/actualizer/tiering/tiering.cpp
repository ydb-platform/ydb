#include "tiering.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

std::shared_ptr<NKikimr::NOlap::ISnapshotSchema> TTieringActualizer::GetTargetSchema(
    const std::shared_ptr<ISnapshotSchema>& portionSchema) const {
    if (!TargetCriticalSchema) {
        return portionSchema;
    }
    if (portionSchema->GetVersion() < TargetCriticalSchema->GetVersion()) {
        return TargetCriticalSchema;
    }
    return portionSchema;
}

std::optional<TTieringActualizer::TFullActualizationInfo> TTieringActualizer::BuildActualizationInfo(
    const TPortionInfo& portion, const TInstant now) const {
    std::shared_ptr<ISnapshotSchema> portionSchema = portion.GetSchema(VersionedIndex);
    std::shared_ptr<ISnapshotSchema> targetSchema = GetTargetSchema(portionSchema);
    const TString& currentTierName = portion.GetTierNameDef(IStoragesManager::DefaultStorageId);

    if (Tiering) {
        AFL_VERIFY(TieringColumnId);
        std::shared_ptr<arrow::Scalar> max;
        {
            auto it = MaxByPortionId.find(portion.GetPortionId());
            if (it == MaxByPortionId.end()) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "skip_add_portion")("reason", "data not ready");
                return {};
            } else if (!it->second) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "skip_add_portion")(
                    "reason", "no data for ttl usage (need to create index or use first pk column)");
                return {};
            } else {
                max = it->second;
            }
        }
        const bool skipEviction = !NYDBTest::TControllers::GetColumnShardController()->CheckPortionForEvict(portion);
        auto tieringInfo = Tiering->GetTierToMove(max, now, skipEviction);
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
        } else {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "skip_add_portion")("reason", "no_eviction")(
                "portion", portion.GetPortionId())("skip_eviction", skipEviction)("optimized",
                portion.HasRuntimeFeature(NOlap::TPortionInfo::ERuntimeFeature::Optimized))("has_insert_write_id", portion.HasInsertWriteId());
            return {};
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
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "skip_add_portion")("reason", "no_tiering");
    return {};
}

void TTieringActualizer::AddPortionImpl(const TPortionInfo& portion, const TInstant now) {
    auto info = BuildActualizationInfo(portion, now);
    if (!info) {
        return;
    }
    AFL_VERIFY(PortionIdByWaitDuration[info->GetAddress()].AddPortion(*info, portion.GetPortionId(), now));
    auto address = info->GetAddress();
    TFindActualizationInfo findId(std::move(address), info->GetWaitInstant(now));
    AFL_VERIFY(PortionsInfo.emplace(portion.GetPortionId(), std::move(findId)).second);
}

void TTieringActualizer::DoAddPortion(const TPortionInfo& portion, const TAddExternalContext& addContext) {
    AFL_VERIFY(PathId == portion.GetPathId());
    if (!addContext.GetPortionExclusiveGuarantee()) {
        if (PortionsInfo.contains(portion.GetPortionId())) {
            return;
        }
    } else {
        AFL_VERIFY(!PortionsInfo.contains(portion.GetPortionId()))("id", portion.GetPortionId())("path_id", portion.GetPathId());
        AFL_VERIFY(!NewPortionIds.contains(portion.GetPortionId()))("id", portion.GetPortionId())("path_id", portion.GetPathId());
    }
    if (!Tiering || MaxByPortionId.contains(portion.GetPortionId())) {
        AddPortionImpl(portion, addContext.GetNow());
    } else {
        auto schema = portion.GetSchema(VersionedIndex);
        if (*TValidator::CheckNotNull(TieringColumnId) == schema->GetIndexInfo().GetPKColumnIds().front()) {
            NYDBTest::TControllers::GetColumnShardController()->OnMaxValueUsage();
            const auto lastPk = portion.GetMeta().GetFirstLastPK().GetLast();
            const auto max = NArrow::TStatusValidator::GetValid(lastPk.Column(0).GetScalar(lastPk.GetPosition()));
            AFL_VERIFY(MaxByPortionId.emplace(portion.GetPortionId(), max).second);
            AddPortionImpl(portion, addContext.GetNow());
        } else {
            NewPortionIds.emplace(portion.GetPortionId());
        }
    }
}

void TTieringActualizer::ActualizePortionInfo(const TPortionDataAccessor& accessor, const TActualizationContext& context) {
    if (!NewPortionIds.erase(accessor.GetPortionInfo().GetPortionId())) {
        return;
    }
    if (NewPortionIds.empty()) {
        NYDBTest::TControllers::GetColumnShardController()->OnTieringMetadataActualized();
    }
    auto& portion = accessor.GetPortionInfo();
    if (Tiering) {
        std::shared_ptr<ISnapshotSchema> portionSchema = portion.GetSchema(VersionedIndex);
        std::shared_ptr<arrow::Scalar> max;
        AFL_VERIFY(*TieringColumnId != portionSchema->GetIndexInfo().GetPKColumnIds().front());
        if (auto indexMeta = portionSchema->GetIndexInfo().GetIndexMetaMax(*TieringColumnId)) {
            NYDBTest::TControllers::GetColumnShardController()->OnStatisticsUsage(NIndexes::TIndexMetaContainer(indexMeta));
            const std::vector<TString> data = accessor.GetIndexInplaceDataVerified(indexMeta->GetIndexId());
            max = indexMeta->GetMaxScalarVerified(data, portionSchema->GetIndexInfo().GetColumnFieldVerified(*TieringColumnId)->type());
        }
        AFL_VERIFY(MaxByPortionId.emplace(portion.GetPortionId(), max).second);
    }
    AddPortionImpl(portion, context.GetNow());
}

void TTieringActualizer::DoRemovePortion(const ui64 portionId) {
    MaxByPortionId.erase(portionId);
    NewPortionIds.erase(portionId);
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

void TTieringActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& /*internalContext*/) {
    THashSet<ui64> portionIds;
    THashSet<ui64> rejectedPortions;
    for (auto&& [address, addressPortions] : PortionIdByWaitDuration) {
        if (addressPortions.GetPortions().size() && tasksContext.GetActualInstant() < addressPortions.GetPortions().begin()->first) {
            Counters.SkipEvictionForTooEarly->Add(1);
            continue;
        }
        if (!tasksContext.IsRWAddressAvailable(address)) {
            Counters.SkipEvictionForLimit->Add(1);
            continue;
        }
        for (auto&& [wInstant, portions] : addressPortions.GetPortions()) {
            if (tasksContext.GetActualInstant() < wInstant) {
                break;
            }
            bool limitEnriched = false;
            for (auto&& p : portions) {
                const auto& portion = externalContext.GetPortionVerified(p);
                auto info = BuildActualizationInfo(*portion, tasksContext.GetActualInstant());
                if (!info) {
                    Counters.SkipEvictionForNoLongerNeeded->Add(1);
                    rejectedPortions.insert(p);
                    continue;
                }
                auto portionScheme = portion->GetSchema(VersionedIndex);
                TPortionEvictionFeatures features(
                    portionScheme, info->GetTargetScheme(), portion->GetTierNameDef(IStoragesManager::DefaultStorageId));
                features.SetTargetTierName(info->GetTargetTierName());

                switch (tasksContext.AddPortion(portion, std::move(features), info->GetLateness())) {
                    case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                        limitEnriched = true;
                        break;
                    case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                        break;
                    case TTieringProcessContext::EAddPortionResult::SUCCESS:
                        AFL_VERIFY(portionIds.emplace(portion->GetPortionId()).second);
                        break;
                }
                if (limitEnriched) {
                    break;
                }
            }
            if (limitEnriched) {
                break;
            }
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "ExtractTtlTasks")("total_portions", PortionsInfo.size())(
        "tasks", portionIds.size());
    if (portionIds.size()) {
        ui64 waitDurationEvict = 0;
        ui64 waitQueueEvict = 0;
        ui64 waitDurationDelete = 0;
        ui64 waitQueueDelete = 0;
        for (auto&& i : PortionIdByWaitDuration) {
            std::shared_ptr<NColumnShard::TValueAggregationClient> waitDurationSignal;
            std::shared_ptr<NColumnShard::TValueAggregationClient> queueSizeSignal;
            if (i.first.WriteIs(NTiering::NCommon::DeleteTierName)) {
                i.second.CorrectSignals(waitQueueDelete, waitDurationDelete, tasksContext.GetActualInstant());
            } else {
                i.second.CorrectSignals(waitQueueEvict, waitDurationEvict, tasksContext.GetActualInstant());
            }
        }
        Counters.DifferenceWaitToDelete->SetValue(waitDurationDelete);
        Counters.DifferenceWaitToEvict->SetValue(waitDurationEvict);
        Counters.QueueSizeToDelete->SetValue(waitQueueDelete);
        Counters.QueueSizeToEvict->SetValue(waitQueueEvict);
    }
    for (auto&& i : portionIds) {
        RemovePortion(i);
    }
    for (auto&& i : rejectedPortions) {
        RemovePortion(i);
    }
}

void TTieringActualizer::Refresh(const std::optional<TTiering>& info, const TAddExternalContext& externalContext) {
    AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "refresh_tiering")("has_tiering", !!info)(
        "tiers", info ? info->GetOrderedTiers().size() : 0)("had_tiering_before", !!Tiering);
    Tiering = info;
    std::optional<ui32> newTieringColumnId;
    if (Tiering) {
        newTieringColumnId = VersionedIndex.GetLastSchema()->GetColumnId(Tiering->GetEvictColumnName());
    }
    TargetCriticalSchema = VersionedIndex.GetLastCriticalSchema();
    PortionsInfo.clear();
    NewPortionIds.clear();
    PortionIdByWaitDuration.clear();
    if (newTieringColumnId != TieringColumnId) {
        MaxByPortionId.clear();
    }
    TieringColumnId = newTieringColumnId;

    for (auto&& i : externalContext.GetPortions()) {
        AddPortion(i.second, externalContext);
    }
}

namespace {
class TActualizationReply: public IMetadataAccessorResultProcessor {
private:
    std::weak_ptr<TTieringActualizer> TieringActualizer;
    virtual void DoApplyResult(NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs& /*engine*/) override {
        auto locked = TieringActualizer.lock();
        if (!locked) {
            return;
        }
        TActualizationContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now());
        for (auto&& [_, portion] : result.GetValue().GetPortions()) {
            locked->ActualizePortionInfo(portion, context);
        }
    }

public:
    TActualizationReply(const std::shared_ptr<TTieringActualizer>& tieringActualizer)
        : TieringActualizer(tieringActualizer) {
        AFL_VERIFY(tieringActualizer);
    }
};

}   // namespace

std::vector<TCSMetadataRequest> TTieringActualizer::BuildMetadataRequests(
    const ui64 /*pathId*/, const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TTieringActualizer>& index) {
    if (NewPortionIds.empty()) {
        NYDBTest::TControllers::GetColumnShardController()->OnTieringMetadataActualized();
        return {};
    }

    const ui64 batchMemorySoftLimit = NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit();
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;
    for (auto&& i : NewPortionIds) {
        if (!currentRequest) {
            currentRequest = std::make_shared<TDataAccessorsRequest>("TIERING_ACTUALIZER");
        }
        auto it = portions.find(i);
        AFL_VERIFY(it != portions.end());
        currentRequest->AddPortion(it->second);
        if (currentRequest->PredictAccessorsMemory(it->second->GetSchema(VersionedIndex)) >= batchMemorySoftLimit) {
            requests.emplace_back(currentRequest, std::make_shared<TActualizationReply>(index));
            currentRequest.reset();
        }
    }
    if (currentRequest) {
        requests.emplace_back(std::move(currentRequest), std::make_shared<TActualizationReply>(index));
    }
    return requests;
}

}   // namespace NKikimr::NOlap::NActualizer
