#include "scheme.h"
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap::NActualizer {

std::optional<NKikimr::NOlap::NActualizer::TSchemeActualizer::TFullActualizationInfo> TSchemeActualizer::BuildActualizationInfo(const TPortionInfo& portion) const {
    AFL_VERIFY(TargetSchema);
    const TString& currentTierName = portion.GetTierNameDef(IStoragesManager::DefaultStorageId);
    auto portionSchema = VersionedIndex.GetSchema(portion.GetMinSnapshot());
    if (portionSchema->GetVersion() < TargetSchema->GetVersion()) {
        auto storagesWrite = TargetSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(currentTierName);
        TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
        return TFullActualizationInfo(std::move(address), TargetSchema);
    }
    return {};
}

void TSchemeActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& /*context*/) {
    if (!TargetSchema) {
        return;
    }
    auto actualizationInfo = BuildActualizationInfo(info);
    if (!actualizationInfo) {
        return;
    }
    PortionsToActualizeScheme[actualizationInfo->GetAddress()].emplace(info.GetPortionId());
    PortionsInfo.emplace(info.GetPortionId(), actualizationInfo->ExtractFindId());
}

void TSchemeActualizer::DoRemovePortion(const TPortionInfo& info) {
    auto it = PortionsInfo.find(info.GetPortionId());
    if (it == PortionsInfo.end()) {
        return;
    }

    auto itAddress = PortionsToActualizeScheme.find(it->second.GetRWAddress());
    AFL_VERIFY(itAddress != PortionsToActualizeScheme.end());
    AFL_VERIFY(itAddress->second.erase(info.GetPortionId()));
    if (itAddress->second.empty()) {
        PortionsToActualizeScheme.erase(itAddress);
    }
}

void TSchemeActualizer::DoBuildTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& /*internalContext*/) const {
    for (auto&& [address, portions] : PortionsToActualizeScheme) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            break;
        }
        for (auto&& portionId : portions) {
            auto portion = externalContext.GetPortionVerified(portionId);
            auto info = BuildActualizationInfo(*portion);
            AFL_VERIFY(info);
            auto portionScheme = VersionedIndex.GetSchema(portion->GetMinSnapshot());
            TPortionEvictionFeatures features(portionScheme, info->GetTargetScheme(), portion->GetTierNameDef(IStoragesManager::DefaultStorageId));
            features.SetTargetTierName(portion->GetTierNameDef(IStoragesManager::DefaultStorageId));

            if (!tasksContext.AddPortion(*portion, std::move(features), {})) {
                break;
            }
        }
    }
}

void TSchemeActualizer::Refresh(const TAddExternalContext& externalContext) {
    TargetSchema = VersionedIndex.GetLastCriticalSchema();
    if (!TargetSchema) {
        AFL_VERIFY(PortionsInfo.empty());
    } else {
        PortionsInfo.clear();
        PortionsToActualizeScheme.clear();
        for (auto&& i : externalContext.GetPortions()) {
            AddPortion(i.second, externalContext);
        }
    }
}

}
