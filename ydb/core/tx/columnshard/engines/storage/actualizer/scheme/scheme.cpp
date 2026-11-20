#include "scheme.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION

namespace NKikimr::NOlap::NActualizer {

std::optional<NKikimr::NOlap::NActualizer::TSchemeActualizer::TFullActualizationInfo> TSchemeActualizer::BuildActualizationInfo(
    const TPortionInfo& portion) const {
    if (!portion.NeedSchemeActualization(VersionedIndex)) {
        return {};
    }
    const TString& currentTierName = portion.GetTierNameDef(IStoragesManager::DefaultStorageId);
    const auto writeSchema = portion.GetRewriteSchema(VersionedIndex);   // the latest schema at the time of the rewrite
    auto storagesWrite = writeSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
    auto storagesRead = portion.GetSchema(VersionedIndex)->GetIndexInfo().GetUsedStorageIds(currentTierName);
    return TFullActualizationInfo(TRWAddress(std::move(storagesRead), std::move(storagesWrite)), writeSchema);
}

void TSchemeActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& addContext) {
    PortionsInFlight.erase(info.GetPortionId());
    if (!VersionedIndex.GetSchemeActualizationThreshold()) {
        TSchemeGlobalCounters::OnEmptyTargetSchema();
        return;
    }
    if (!addContext.GetPortionExclusiveGuarantee()) {
        if (PortionsInfo.contains(info.GetPortionId())) {
            return;
        }
    } else {
        YDB_LOG_VERIFY(!PortionsInfo.contains(info.GetPortionId()));
    }
    auto actualizationInfo = BuildActualizationInfo(info);
    if (!actualizationInfo) {
        TSchemeGlobalCounters::OnSkipPortionNotActualizable();
        return;
    }
    TSchemeGlobalCounters::OnAddPortion();
    NYDBTest::TControllers::GetColumnShardController()->AddPortionForActualizer(1);
    YDB_LOG_VERIFY(PortionsToActualizeScheme[actualizationInfo->GetAddress()].emplace(info.GetPortionId()).second);
    YDB_LOG_VERIFY(PortionsInfo.emplace(info.GetPortionId(), actualizationInfo->ExtractFindId()).second);
}

void TSchemeActualizer::DoRemovePortion(const ui64 portionId) {
    auto it = PortionsInfo.find(portionId);
    if (it == PortionsInfo.end()) {
        // not queued: a portion handed to a task, removed from the granule now that its rewrite is committed
        PortionsInFlight.erase(portionId);
        TSchemeGlobalCounters::OnSkipPortionToRemove();
        return;
    }
    auto itAddress = PortionsToActualizeScheme.find(it->second.GetRWAddress());
    YDB_LOG_VERIFY(itAddress != PortionsToActualizeScheme.end());
    YDB_LOG_VERIFY(itAddress->second.erase(portionId));
    NYDBTest::TControllers::GetColumnShardController()->AddPortionForActualizer(-1);
    if (itAddress->second.empty()) {
        PortionsToActualizeScheme.erase(itAddress);
    }
    TSchemeGlobalCounters::OnRemovePortion();
    PortionsInfo.erase(it);
}

void TSchemeActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& /*internalContext*/) {
    THashSet<ui64> portionsToRemove;
    TSchemeGlobalCounters::OnExtract();
    YDB_LOG_DEBUG("",
        {"rwCount", PortionsToActualizeScheme.size()});
    for (auto&& [address, portions] : PortionsToActualizeScheme) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            YDB_LOG_DEBUG("",
                {"event", "skip_not_ready_for_write"});
            TSchemeGlobalCounters::OnSkipNotReadyWrite();
            continue;
        }
        for (auto&& portionId : portions) {
            auto portion = externalContext.GetPortionVerified(portionId);
            if (!address.WriteIs(NBlobOperations::TGlobal::DefaultStorageId) && !address.WriteIs(NTiering::NCommon::DeleteTierName)) {
                const bool sameTierRewrite = portion->GetTierNameDef(IStoragesManager::DefaultStorageId) == IStoragesManager::DefaultStorageId;
                if (!sameTierRewrite && !portion->HasRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized)) {
                    TSchemeGlobalCounters::OnSkipNotOptimized();
                    continue;
                }
            }
            auto info = BuildActualizationInfo(*portion);
            if (!info) {   // not expected: queued by the same test, the queue is rebuilt whenever the threshold changes
                portionsToRemove.emplace(portion->GetPortionId());
                continue;
            }
            auto portionScheme = portion->GetSchema(VersionedIndex);
            TPortionEvictionFeatures features(portion->GetSchemaVersionVerified(), portionScheme, info->GetTargetScheme(),
                portion->GetTierNameDef(IStoragesManager::DefaultStorageId));
            features.SetTargetTierName(portion->GetTierNameDef(IStoragesManager::DefaultStorageId));

            bool limitExceeded = false;
            switch (tasksContext.AddPortion(portion, std::move(features), {})) {
                case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                    YDB_LOG_DEBUG("",
                        {"event", "cannot_add_portion"},
                        {"reason", "limit_exceeded"},
                        {"context", tasksContext.DebugString()});
                    TSchemeGlobalCounters::OnSkipNotReadyWrite();
                    limitExceeded = true;
                    break;
                case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                    break;
                case TTieringProcessContext::EAddPortionResult::SUCCESS:
                    PortionsInFlight.emplace(portion->GetPortionId());
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
    YDB_LOG_DEBUG("",
        {"internalQueue", waitQueueInternal},
        {"externalQueue", waitQueueExternal});
}

void TSchemeActualizer::Refresh(const TAddExternalContext& externalContext) {
    NYDBTest::TControllers::GetColumnShardController()->AddPortionForActualizer(-1 * PortionsInfo.size());
    PortionsInfo.clear();
    PortionsToActualizeScheme.clear();
    PortionsInFlight.clear();
    if (!VersionedIndex.GetSchemeActualizationThreshold()) {
        // SchemeNeedActualization set back to false: the actualization stops
        TSchemeGlobalCounters::OnRefreshEmpty();
    } else {
        TSchemeGlobalCounters::OnRefreshValue();
        for (auto&& i : externalContext.GetPortions()) {
            AddPortion(i.second, externalContext);
        }
    }
}

TSchemeActualizer::TSchemeActualizer(const TInternalPathId pathId, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , VersionedIndex(versionedIndex)
{
    Y_UNUSED(PathId);
}

}   // namespace NKikimr::NOlap::NActualizer
