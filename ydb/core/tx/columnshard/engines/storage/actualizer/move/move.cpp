#include "move.h"

#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NOlap::NActualizer {

namespace {

class TMoveDataActualizationReply: public IMetadataAccessorResultProcessor {
private:
    std::weak_ptr<TMoveDataActualizer> MoveDataActualizer;

    void DoApplyResult(NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs&) override {
        auto locked = MoveDataActualizer.lock();
        if (!locked) {
            return;
        }
        if (result.GetValue().HasErrors()) {
            // Affected portions stay in PendingPortionIds and are re-requested on the
            // next BuildMoveDataMetadataRequests cycle; surface the failure for operators.
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"error", "move data accessor result with errors " + result.GetValue().GetErrorMessage()});
        }
        if (result.GetValue().HasRemovedData()) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "",
                {"event", TStringBuilder{} << "move data accessor result with removed data, " << result.GetValue().GetRemovedData().size()});
        }
        for (auto&& [_, accessor] : result.GetValue().GetPortions()) {
            locked->ActualizePortionInfo(*accessor);
        }
    }

public:
    explicit TMoveDataActualizationReply(const std::shared_ptr<TMoveDataActualizer>& actualizer)
        : MoveDataActualizer(actualizer)
    {
        AFL_VERIFY(!!actualizer);
    }
};

}   // anonymous namespace

void TMoveDataActualizer::RemoveFromActiveQueue(ui64 portionId) {
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

void TMoveDataActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext&) {
    const ui64 portionId = info.GetPortionId();
    if (!InitialPortionIds.contains(portionId)) {
        return;
    }
    if (PortionAddress.contains(portionId) || PendingPortionIds.contains(portionId)) {
        return;
    }
    if (info.GetTierNameDef(IStoragesManager::DefaultStorageId) != IStoragesManager::DefaultStorageId) {
        return;
    }
    InFlightPortionIds.erase(portionId);
    PendingPortionIds.emplace(portionId);
}

void TMoveDataActualizer::DoRemovePortion(const ui64 portionId) {
    InitialPortionIds.erase(portionId);
    PendingPortionIds.erase(portionId);
    InFlightPortionIds.erase(portionId);
    RemoveFromActiveQueue(portionId);
}

void TMoveDataActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext&) {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::MoveData)) {
        return;
    }
    THashSet<ui64> submitted;
    for (auto& [address, portions] : PortionsToMove) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            continue;
        }
        bool limitExceeded = false;
        for (auto& portionId : portions) {
            auto portion = externalContext.GetPortionVerified(portionId);
            auto portionSchema = portion->GetSchema(VersionedIndex);
            const TString tierName = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
            TPortionEvictionFeatures features(portionSchema, portionSchema, tierName);
            features.SetTargetTierName(tierName);
            features.SetForcedMove();

            switch (tasksContext.AddPortion(portion, std::move(features), TDuration::Zero())) {
                case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                    limitExceeded = true;
                    break;
                case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                    break;
                case TTieringProcessContext::EAddPortionResult::SUCCESS:
                    submitted.emplace(portionId);
                    break;
            }
            if (limitExceeded) {
                break;
            }
        }
        if (limitExceeded) {
            break;
        }
    }
    for (auto portionId : submitted) {
        RemoveFromActiveQueue(portionId);
        InFlightPortionIds.emplace(portionId);
    }
}

bool TMoveDataActualizer::HasBlobInGroups(const std::vector<TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups) {
    return AnyOf(blobIds, [&groups](const TUnifiedBlobId& blobId) {
        return groups.contains(blobId.GetDsGroup());
    });
}

void TMoveDataActualizer::ActualizePortionInfo(const TPortionDataAccessor& accessor) {
    const ui64 portionId = accessor.GetPortionInfo().GetPortionId();
    if (!PendingPortionIds.erase(portionId)) {
        return;
    }
    if (!HasBlobInGroups(accessor.GetBlobIds(), TargetGroups)) {
        return;
    }
    auto portionSchema = accessor.GetPortionInfo().GetSchema(VersionedIndex);
    const TString tierName = accessor.GetPortionInfo().GetTierNameDef(IStoragesManager::DefaultStorageId);
    auto readStorages = portionSchema->GetIndexInfo().GetUsedStorageIds(tierName);
    auto writeStorages = readStorages;
    TRWAddress address(std::move(readStorages), std::move(writeStorages));
    AFL_VERIFY(PortionsToMove[address].emplace(portionId).second);
    AFL_VERIFY(PortionAddress.emplace(portionId, std::move(address)).second);
}

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TMoveDataActualizer>& self) const {
    if (PendingPortionIds.empty()) {
        return {};
    }
    const ui64 batchMemorySoftLimit = NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit();
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;

    for (auto portionId : PendingPortionIds) {
        auto it = portions.find(portionId);
        if (it == portions.end()) {
            continue;
        }
        if (!currentRequest) {
            currentRequest = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::MOVE_DATA);
        }
        currentRequest->AddPortion(it->second);
        if (currentRequest->PredictAccessorsMemory(it->second->GetSchema(VersionedIndex)) >= batchMemorySoftLimit) {
            requests.emplace_back(currentRequest, std::make_shared<TMoveDataActualizationReply>(self));
            currentRequest.reset();
        }
    }
    if (currentRequest) {
        requests.emplace_back(std::move(currentRequest), std::make_shared<TMoveDataActualizationReply>(self));
    }
    return requests;
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    TMoveDataQueueSizes result;
    result.Pending = PendingPortionIds.size();
    result.InFlight = InFlightPortionIds.size();
    for (auto& [addr, portions] : PortionsToMove) {
        result.ConfirmedToMove += portions.size();
    }
    return result;
}

void TMoveDataActualizer::Refresh(const TAddExternalContext& externalContext) {
    InitialPortionIds.clear();
    PendingPortionIds.clear();
    PortionsToMove.clear();
    PortionAddress.clear();
    InFlightPortionIds.clear();

    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        if (portion->GetTierNameDef(IStoragesManager::DefaultStorageId) != IStoragesManager::DefaultStorageId) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
        AddPortion(portion, externalContext);
    }
}

}   // namespace NKikimr::NOlap::NActualizer
