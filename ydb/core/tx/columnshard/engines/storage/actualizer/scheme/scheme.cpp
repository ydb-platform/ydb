#include "scheme.h"
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

std::optional<NKikimr::NOlap::NActualizer::TSchemeActualizer::TFullActualizationInfo> TSchemeActualizer::BuildActualizationInfo(const TPortionInfo& portion) const {
    AFL_VERIFY(TargetSchema);
    const TString& currentTierName = portion.GetTierNameDef(IStoragesManager::DefaultStorageId);
    auto portionSchema = portion.GetSchema(VersionedIndex);
    if (portionSchema->GetVersion() < TargetSchema->GetVersion()) {
        auto storagesWrite = TargetSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
        return TFullActualizationInfo(std::move(address), TargetSchema);
    }
    return {};
}

void TSchemeActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& addContext) {
    if (!TargetSchema) {
        TSchemeGlobalCounters::OnEmptyTargetSchema();
        return;
    }
    if (!addContext.GetPortionExclusiveGuarantee()) {
        if (PortionsInfo.contains(info.GetPortionId())) {
            return;
        }
    } else {
        AFL_VERIFY(!PortionsInfo.contains(info.GetPortionId()));
    }
    auto actualizationInfo = BuildActualizationInfo(info);
    if (!actualizationInfo) {
        TSchemeGlobalCounters::OnSkipPortionNotActualizable();
        return;
    }
    TSchemeGlobalCounters::OnAddPortion();
    NYDBTest::TControllers::GetColumnShardController()->AddPortionForActualizer(1);
    AFL_VERIFY(PortionsToActualizeScheme[actualizationInfo->GetAddress()].emplace(info.GetPortionId()).second);
    AFL_VERIFY(PortionsInfo.emplace(info.GetPortionId(), actualizationInfo->ExtractFindId()).second);
}

void TSchemeActualizer::DoRemovePortion(const ui64 portionId) {
    auto it = PortionsInfo.find(portionId);
    if (it == PortionsInfo.end()) {
        TSchemeGlobalCounters::OnSkipPortionToRemove();
        return;
    }
    auto itAddress = PortionsToActualizeScheme.find(it->second.GetRWAddress());
    AFL_VERIFY(itAddress != PortionsToActualizeScheme.end());
    AFL_VERIFY(itAddress->second.erase(portionId));
    NYDBTest::TControllers::GetColumnShardController()->AddPortionForActualizer(-1);
    if (itAddress->second.empty()) {
        PortionsToActualizeScheme.erase(itAddress);
    }
    TSchemeGlobalCounters::OnRemovePortion();
    PortionsInfo.erase(it);
}

void TSchemeActualizer::DoExtractTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& /*internalContext*/) {
    THashSet<ui64> portionsToRemove;
    TSchemeGlobalCounters::OnExtract();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("rw_count", PortionsToActualizeScheme.size());
    for (auto&& [address, portions] : PortionsToActualizeScheme) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "skip_not_ready_for_write");
            TSchemeGlobalCounters::OnSkipNotReadyWrite();
            continue;
        }
        for (auto&& portionId : portions) {
            auto portion = externalContext.GetPortionVerified(portionId);
            if (!address.WriteIs(NBlobOperations::TGlobal::DefaultStorageId) && !address.WriteIs(NTiering::NCommon::DeleteTierName)) {
                if (!portion->HasRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized)) {
                    TSchemeGlobalCounters::OnSkipNotOptimized();
                    continue;
                }
            }
            auto info = BuildActualizationInfo(*portion);
            if (!info) { // its possible through chains with equivalent schemas collapsed
                portionsToRemove.emplace(portion->GetPortionId());
            }
            auto portionScheme = portion->GetSchema(VersionedIndex);
            TPortionEvictionFeatures features(portionScheme, info->GetTargetScheme(), portion->GetTierNameDef(IStoragesManager::DefaultStorageId));
            features.SetTargetTierName(portion->GetTierNameDef(IStoragesManager::DefaultStorageId));

            bool limitExceeded = false;
            switch (tasksContext.AddPortion(portion, std::move(features), {})) {
                case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "cannot_add_portion")("reason", "limit_exceeded")(
                        "context", tasksContext.DebugString());
                    limitExceeded = true;
                    break;
                case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                    break;
                case TTieringProcessContext::EAddPortionResult::SUCCESS:
                    portionsToRemove.emplace(portion->GetPortionId());
                    break;
            }
            if (limitExceeded) {
                break;
            }
        }
    }
    for (auto&& i : portionsToRemove) {
        RemovePortion(i);
    }

    ui64 waitQueueExternal = 0;
    ui64 waitQueueInternal = 0;
    for (auto&& i : PortionsToActualizeScheme) {
        if (i.first.WriteIs(IStoragesManager::DefaultStorageId)) {
            waitQueueInternal += i.second.size();
        } else {
            waitQueueExternal += i.second.size();
        }
    }
    Counters.QueueSizeInternalWrite->SetValue(waitQueueInternal);
    Counters.QueueSizeExternalWrite->SetValue(waitQueueExternal);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("internal_queue", waitQueueInternal)("external_queue", waitQueueExternal);
}

void TSchemeActualizer::Refresh(const TAddExternalContext& externalContext) {
    TargetSchema = VersionedIndex.GetLastCriticalSchema();
    if (!TargetSchema) {
        TSchemeGlobalCounters::OnRefreshEmpty();
        AFL_VERIFY(PortionsInfo.empty());
    } else {
        TSchemeGlobalCounters::OnRefreshValue();
        NYDBTest::TControllers::GetColumnShardController()->AddPortionForActualizer(-1 * PortionsInfo.size());
        PortionsInfo.clear();
        PortionsToActualizeScheme.clear();
        for (auto&& i : externalContext.GetPortions()) {
            AddPortion(i.second, externalContext);
        }
    }
}

TSchemeActualizer::TSchemeActualizer(const TInternalPathId pathId, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , VersionedIndex(versionedIndex) {
    Y_UNUSED(PathId);
}

}
