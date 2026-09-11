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
    const std::vector<ui64> PortionIds;

    void DoApplyResult(NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs&) override {
        auto locked = MoveDataActualizer.lock();
        if (!locked) {
            return;
        }
        locked->OnMetadataRequestAnswered(PortionIds);
        if (result.GetValue().HasErrors()) {
            // Affected portions stay in PendingPortionIds and are re-requested next cycle.
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
    TMoveDataActualizationReply(const std::shared_ptr<TMoveDataActualizer>& actualizer, std::vector<ui64>&& portionIds)
        : MoveDataActualizer(actualizer)
        , PortionIds(std::move(portionIds))
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

void TMoveDataActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) {
    const ui64 portionId = info.GetPortionId();
    if (!InitialPortionIds.contains(portionId)) {
        if (context.GetNow() >= AdmissionDeadline) {
            return;
        }
        InitialPortionIds.emplace(portionId);
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
    RequestedAt.erase(portionId);
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

namespace {
// Groups the blobs resolve into, so the rejection log says what the portion was dropped for.
std::vector<ui32> GetBlobGroupsForLog(const std::vector<TUnifiedBlobId>& blobIds) {
    std::vector<ui32> result;
    for (const auto& blobId : blobIds) {
        result.emplace_back(blobId.GetDsGroup());
    }
    SortUnique(result);
    return result;
}
}   // namespace

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
        ++RejectedPortions;
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, "",
            {"event", "move_data_portion_rejected"},
            {"portionId", portionId},
            {"blobs", JoinSeq(",", GetBlobGroupsForLog(accessor.GetBlobIds()))},
            {"targets", JoinSeq(",", TargetGroups)});
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

void TMoveDataActualizer::OnMetadataRequestAnswered(const std::vector<ui64>& portionIds) {
    for (const ui64 portionId : portionIds) {
        RequestedAt.erase(portionId);
    }
}

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TMoveDataActualizer>& self, const TInstant now) {
    if (PendingPortionIds.empty()) {
        return {};
    }
    const ui64 batchMemorySoftLimit = NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit();
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;
    std::vector<ui64> currentPortionIds;

    for (auto portionId : PendingPortionIds) {
        // Every background pass and every reply lands here, so skip portions whose request is still unanswered.
        if (const auto* requestedAt = RequestedAt.FindPtr(portionId); requestedAt && now < *requestedAt + MetadataRequestExpiry) {
            continue;
        }
        auto it = portions.find(portionId);
        if (it == portions.end()) {
            continue;
        }
        if (!currentRequest) {
            currentRequest = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::MOVE_DATA);
        }
        currentRequest->AddPortion(it->second);
        currentPortionIds.emplace_back(portionId);
        RequestedAt[portionId] = now;
        if (currentRequest->PredictAccessorsMemory(it->second->GetSchema(VersionedIndex)) >= batchMemorySoftLimit) {
            requests.emplace_back(currentRequest, std::make_shared<TMoveDataActualizationReply>(self, std::move(currentPortionIds)));
            currentRequest.reset();
            currentPortionIds.clear();
        }
    }
    if (currentRequest) {
        requests.emplace_back(std::move(currentRequest), std::make_shared<TMoveDataActualizationReply>(self, std::move(currentPortionIds)));
    }
    return requests;
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    TMoveDataQueueSizes result{ .Pending = PendingPortionIds.size(), .ConfirmedToMove = 0, .InFlight = InFlightPortionIds.size(),
        .Rejected = RejectedPortions };
    for (auto& [addr, portions] : PortionsToMove) {
        result.ConfirmedToMove += portions.size();
    }
    return result;
}

void TMoveDataActualizer::Refresh(const TAddExternalContext& externalContext) {
    AdmissionDeadline =
        externalContext.GetNow() + NYDBTest::TControllers::GetColumnShardController()->GetMoveDataAdmissionWindow(AdmissionWindow);
    InitialPortionIds.clear();
    PendingPortionIds.clear();
    PortionsToMove.clear();
    PortionAddress.clear();
    InFlightPortionIds.clear();
    RequestedAt.clear();
    RejectedPortions = 0;

    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        if (portion->GetTierNameDef(IStoragesManager::DefaultStorageId) != IStoragesManager::DefaultStorageId) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
        AddPortion(portion, externalContext);
    }
}

}   // namespace NKikimr::NOlap::NActualizer
