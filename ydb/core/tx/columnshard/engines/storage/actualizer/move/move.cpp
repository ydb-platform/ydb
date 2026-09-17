#include "move.h"

#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NOlap::NActualizer {

namespace {

class TMoveDataActualizationReply: public IMetadataAccessorResultProcessor {
private:
    std::weak_ptr<TMoveDataActualizer> MoveDataActualizer;
    const std::vector<ui64> PortionIds;
    const TInstant RequestedAt;

    void DoApplyResult(NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs&) override {
        auto locked = MoveDataActualizer.lock();
        if (!locked) {
            return;
        }
        locked->OnMetadataRequestAnswered(PortionIds, RequestedAt);
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
    TMoveDataActualizationReply(
        const std::shared_ptr<TMoveDataActualizer>& actualizer, std::vector<ui64>&& portionIds, const TInstant requestedAt)
        : MoveDataActualizer(actualizer)
        , PortionIds(std::move(portionIds))
        , RequestedAt(requestedAt)
    {
        AFL_VERIFY(!!actualizer);
    }
};

// True when at least one entity (column or index) of this portion lives in default storage for the given tier.
bool HasEntityInDefaultStorage(const TPortionInfo& info, const TVersionedIndex& versionedIndex) {
    const TString tier = info.GetTierNameDef(IStoragesManager::DefaultStorageId);
    if (tier == IStoragesManager::DefaultStorageId) {
        return true;
    }
    const auto schema = info.GetSchema(versionedIndex);
    for (const auto entityId : schema->GetIndexInfo().GetEntityIds()) {
        if (schema->GetIndexInfo().GetEntityStorageId(entityId, tier) == IStoragesManager::DefaultStorageId) {
            return true;
        }
    }
    return false;
}

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
    // A seeded uncommitted portion has committed, so from here on it moves like any other.
    UncommittedPortionIds.erase(portionId);
    UncommittedOnTarget.erase(portionId);
    if (!InitialPortionIds.contains(portionId)) {
        if (context.GetNow() >= AdmissionDeadline) {
            return;
        }
        InitialPortionIds.emplace(portionId);
    }
    if (PortionAddress.contains(portionId) || PendingPortionIds.contains(portionId)) {
        return;
    }
    if (!HasEntityInDefaultStorage(info, VersionedIndex)) {
        return;
    }
    InFlightPortionIds.erase(portionId);
    PendingPortionIds.emplace(portionId);
}

void TMoveDataActualizer::DoRemovePortion(const ui64 portionId) {
    // InitialPortionIds is kept: a level move removes and re-adds the same portion, which must still pass admission.
    PendingPortionIds.erase(portionId);
    RequestedAt.erase(portionId);
    InFlightPortionIds.erase(portionId);
    UncommittedPortionIds.erase(portionId);
    UncommittedOnTarget.erase(portionId);
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
    // An uncommitted write cannot be rewritten, so the gate holds until it commits or aborts.
    if (UncommittedPortionIds.contains(portionId)) {
        UncommittedOnTarget.emplace(portionId);
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

void TMoveDataActualizer::OnMetadataRequestAnswered(const std::vector<ui64>& portionIds, const TInstant requestedAt) {
    for (const ui64 portionId : portionIds) {
        // A late answer to an expired request must leave the request that replaced it outstanding.
        if (const auto* at = RequestedAt.FindPtr(portionId); at && *at == requestedAt) {
            RequestedAt.erase(portionId);
        }
    }
}

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
    const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self,
    const TInstant now) {
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
        TPortionInfo::TPtr portion;
        if (const auto it = portions.find(portionId); it != portions.end()) {
            portion = it->second;
        } else if (const auto itUncommitted = uncommitted.find(portionId); itUncommitted != uncommitted.end()) {
            portion = itUncommitted->second;
        } else {
            continue;
        }
        if (!currentRequest) {
            currentRequest = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::MOVE_DATA);
        }
        currentRequest->AddPortion(portion);
        currentPortionIds.emplace_back(portionId);
        RequestedAt[portionId] = now;
        if (currentRequest->PredictAccessorsMemory(portion->GetSchema(VersionedIndex)) >= batchMemorySoftLimit) {
            requests.emplace_back(currentRequest, std::make_shared<TMoveDataActualizationReply>(self, std::move(currentPortionIds), now));
            currentRequest.reset();
            currentPortionIds.clear();
        }
    }
    if (currentRequest) {
        requests.emplace_back(std::move(currentRequest), std::make_shared<TMoveDataActualizationReply>(self, std::move(currentPortionIds), now));
    }
    return requests;
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    return TMoveDataQueueSizes{ .Pending = PendingPortionIds.size(), .ConfirmedToMove = PortionAddress.size(),
        .InFlight = InFlightPortionIds.size(),
        .Uncommitted = UncommittedOnTarget.size(),
        .Rejected = RejectedPortions };
}

void TMoveDataActualizer::Refresh(
    const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    AdmissionDeadline =
        externalContext.GetNow() + NYDBTest::TControllers::GetColumnShardController()->GetMoveDataAdmissionWindow(AdmissionWindow);
    InitialPortionIds.clear();
    PendingPortionIds.clear();
    PortionsToMove.clear();
    PortionAddress.clear();
    InFlightPortionIds.clear();
    RequestedAt.clear();
    UncommittedPortionIds.clear();
    UncommittedOnTarget.clear();
    RejectedPortions = 0;

    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        if (!HasEntityInDefaultStorage(*portion, VersionedIndex)) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
        AddPortion(portion, externalContext);
    }
    // Initial membership lets a write that commits after the admission window still move.
    for (const auto& [portionId, portion] : uncommitted) {
        if (portion->HasRemoveSnapshot() || !HasEntityInDefaultStorage(*portion, VersionedIndex)) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
        UncommittedPortionIds.emplace(portionId);
        PendingPortionIds.emplace(portionId);
    }
}

}   // namespace NKikimr::NOlap::NActualizer
