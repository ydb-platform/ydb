#include "move.h"

#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

void TMoveDataActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& /*context*/) {
    if (!InitialPortionIds.contains(info.GetPortionId())) {
        return;
    }
    if (PortionAddress.contains(info.GetPortionId())) {
        return;
    }
    const TString tierName = info.GetTierNameDef(IStoragesManager::DefaultStorageId);
    auto portionSchema = info.GetSchema(VersionedIndex);
    auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(tierName);
    auto storagesWrite = portionSchema->GetIndexInfo().GetUsedStorageIds(tierName);
    TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
    AFL_VERIFY(PortionsToMove[address].emplace(info.GetPortionId()).second);
    AFL_VERIFY(PortionAddress.emplace(info.GetPortionId(), address).second);
}

void TMoveDataActualizer::DoRemovePortion(const ui64 portionId) {
    InitialPortionIds.erase(portionId);
    auto it = PortionAddress.find(portionId);
    if (it == PortionAddress.end()) {
        return;
    }
    auto itAddr = PortionsToMove.find(it->second);
    AFL_VERIFY(itAddr != PortionsToMove.end());
    AFL_VERIFY(itAddr->second.erase(portionId));
    if (itAddr->second.empty()) {
        PortionsToMove.erase(itAddr);
    }
    PortionAddress.erase(it);
}

void TMoveDataActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& /*internalContext*/) {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::MoveData)) {
        return;
    }
    THashSet<ui64> portionsToRemove;
    for (auto& [address, portions] : PortionsToMove) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            continue;
        }
        for (auto& portionId : portions) {
            auto portion = externalContext.GetPortionVerified(portionId);
            auto portionSchema = portion->GetSchema(VersionedIndex);
            const TString tierName = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
            TPortionEvictionFeatures features(portionSchema, portionSchema, tierName);
            features.SetTargetTierName(tierName);
            features.SetForcedMove();

            bool limitExceeded = false;
            switch (tasksContext.AddPortion(portion, std::move(features), TDuration::Zero())) {
                case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                    limitExceeded = true;
                    break;
                case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                    break;
                case TTieringProcessContext::EAddPortionResult::SUCCESS:
                    portionsToRemove.emplace(portionId);
                    break;
            }
            if (limitExceeded) {
                break;
            }
        }
    }
    for (auto& i : portionsToRemove) {
        RemovePortion(i);
    }
}

void TMoveDataActualizer::Refresh(const TAddExternalContext& externalContext) {
    InitialPortionIds.clear();
    PortionsToMove.clear();
    PortionAddress.clear();
    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        if (portion->HasRemoveSnapshot()) {
            continue;
        }
        if (portion->GetTierNameDef(IStoragesManager::DefaultStorageId) != IStoragesManager::DefaultStorageId) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
    }
    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        if (!InitialPortionIds.contains(portionId)) {
            continue;
        }
        AddPortion(portion, externalContext);
    }
}

}   // namespace NKikimr::NOlap::NActualizer
