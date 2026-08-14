#include "move.h"

#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

namespace {

// Receives loaded accessor results and forwards each portion to the actualizer for
// DsGroup-based filtering (moves confirmed portions from Pending → PortionsToMove).
class TMoveDataActualizationReply: public IMetadataAccessorResultProcessor {
private:
    std::weak_ptr<TMoveDataActualizer> MoveDataActualizer;

    void DoApplyResult(
        NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs& /*engine*/) override {
        auto locked = MoveDataActualizer.lock();
        if (!locked) {
            return;
        }
        for (auto&& [_, accessor] : result.GetValue().GetPortions()) {
            locked->ActualizePortionInfo(*accessor);
        }
    }

public:
    explicit TMoveDataActualizationReply(const std::shared_ptr<TMoveDataActualizer>& actualizer)
        : MoveDataActualizer(actualizer)
    {
        AFL_VERIFY(actualizer);
    }
};

}   // namespace

// ─── IActualizer interface ───────────────────────────────────────────────────

void TMoveDataActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& /*context*/) {
    const ui64 portionId = info.GetPortionId();
    if (!InitialPortionIds.contains(portionId)) {
        return;
    }
    if (PortionAddress.contains(portionId) || PendingPortionIds.contains(portionId)) {
        return;
    }
    // Only consider default-tier portions — tiered data lives on external storage.
    if (info.GetTierNameDef(IStoragesManager::DefaultStorageId) != IStoragesManager::DefaultStorageId) {
        return;
    }
    PendingPortionIds.emplace(portionId);
}

void TMoveDataActualizer::DoRemovePortion(const ui64 portionId) {
    InitialPortionIds.erase(portionId);
    PendingPortionIds.erase(portionId);

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
                    portionsToRemove.emplace(portionId);
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
    for (auto& i : portionsToRemove) {
        RemovePortion(i);
    }
}

// ─── Public interface ────────────────────────────────────────────────────────

// Called by TMoveDataActualizationReply once blob metadata is available.
// Checks whether the portion has any blob in the target groups; if yes, the
// portion is promoted from PendingPortionIds to PortionsToMove.
void TMoveDataActualizer::ActualizePortionInfo(const TPortionDataAccessor& accessor) {
    const ui64 portionId = accessor.GetPortionInfo().GetPortionId();
    if (!PendingPortionIds.erase(portionId)) {
        // Already removed (e.g. portion was deleted between request and reply).
        return;
    }
    bool hasTargetBlob = false;
    for (auto& blobId : accessor.GetBlobIds()) {
        if (TargetGroups.contains(blobId.GetDsGroup())) {
            hasTargetBlob = true;
            break;
        }
    }
    if (!hasTargetBlob) {
        // No blobs in the target groups — this portion is already fully migrated.
        return;
    }
    // The portion has blobs in the old groups: queue it for rewriting.
    auto portionSchema = accessor.GetPortionInfo().GetSchema(VersionedIndex);
    const TString tierName = accessor.GetPortionInfo().GetTierNameDef(IStoragesManager::DefaultStorageId);
    auto storagesRead = portionSchema->GetIndexInfo().GetUsedStorageIds(tierName);
    auto storagesWrite = portionSchema->GetIndexInfo().GetUsedStorageIds(tierName);
    TRWAddress address(std::move(storagesRead), std::move(storagesWrite));
    AFL_VERIFY(PortionsToMove[address].emplace(portionId).second);
    AFL_VERIFY(PortionAddress.emplace(portionId, std::move(address)).second);
}

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const std::shared_ptr<TMoveDataActualizer>& self) {
    if (PendingPortionIds.empty()) {
        return {};
    }

    const ui64 batchMemorySoftLimit = NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit();
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;

    for (auto& portionId : PendingPortionIds) {
        auto it = portions.find(portionId);
        if (it == portions.end()) {
            // Portion was removed; will be cleaned up via DoRemovePortion.
            continue;
        }
        if (!currentRequest) {
            currentRequest = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::TTL);
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

ui64 TMoveDataActualizer::GetPortionsToMoveCount() const {
    ui64 total = PendingPortionIds.size();
    for (auto& [addr, portions] : PortionsToMove) {
        total += portions.size();
    }
    return total;
}

void TMoveDataActualizer::Refresh(const TAddExternalContext& externalContext) {
    // Capture a fresh snapshot; only portions present NOW can be rewritten.
    // Any portion added after this point is a newly written one and must be excluded
    // to prevent infinite rewrite loops.
    InitialPortionIds.clear();
    PendingPortionIds.clear();
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
    // Re-add all captured portions; they go into PendingPortionIds pending
    // accessor load (group validation).
    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        AddPortion(portion, externalContext);
    }
}

}   // namespace NKikimr::NOlap::NActualizer
