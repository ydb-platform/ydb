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
#include <util/generic/mapfindptr.h>

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
    TMoveDataActualizationReply(const std::shared_ptr<TMoveDataActualizer>& actualizer)
        : MoveDataActualizer(actualizer)
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

TTieringProcessContext::EAddPortionResult AddPortionToMove(
    TTieringProcessContext& tasks, const TPortionInfo::TConstPtr& portion, const TVersionedIndex& index) {
    const auto schema = portion->GetSchema(index);
    const auto tier = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
    TPortionEvictionFeatures features(schema, schema, tier);
    features.SetTargetTierName(tier);
    features.SetForcedMove();
    return tasks.AddPortion(portion, std::move(features), TDuration::Zero());
}

}   // anonymous namespace

void TMoveDataActualizer::RemoveFromActiveQueue(ui64 portionId) {
    const auto* address = MapFindPtr(PortionAddress, portionId);
    if (address == nullptr) {
        return;
    }
    auto* portions = MapFindPtr(PortionsToMove, *address);
    AFL_VERIFY(portions != nullptr);
    AFL_VERIFY(portions->erase(portionId));
    if (portions->empty()) {
        PortionsToMove.erase(*address);
    }
    PortionAddress.erase(portionId);
}

void TMoveDataActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& /*context*/) {
    // Bookkeeping only: a seeded portion coming back after a commit or an aborted move re-enters the queues.
    const ui64 portionId = info.GetPortionId();
    // A seeded uncommitted portion has committed, so from here on it moves like any other.
    UncommittedPortionIds.erase(portionId);
    UncommittedOnTarget.erase(portionId);
    // An aborted task returns the portion here; leaving it in flight past any check below freezes the gate.
    InFlightPortionIds.erase(portionId);
    // An aborted move re-adds the portion, so it is live again rather than awaiting cleanup.
    RetiredPortionIds.erase(portionId);
    if (!InitialPortionIds.contains(portionId) || RejectedPortionIds.contains(portionId)) {
        // New portions are outside the fixed session set; rejected portions stay outside the target groups.
        return;
    }
    if (PortionAddress.contains(portionId) || PendingPortionIds.contains(portionId)) {
        return;
    }
    if (!HasEntityInDefaultStorage(info, VersionedIndex)) {
        return;
    }
    PendingPortionIds.emplace(portionId);
}

void TMoveDataActualizer::DoRemovePortion(const ui64 portionId) {
    // Retirement drops rewrite work, but an unclassified candidate still needs its accessor before cleanup can be ignored.
    // InitialPortionIds is kept: a level move removes and re-adds the same portion, which stays ours.
    if (InitialPortionIds.contains(portionId) && !RejectedPortionIds.contains(portionId)) {
        RetiredPortionIds.emplace(portionId);
        if (!TargetPortionIds.contains(portionId)) {
            // Retirement can race an accessor request. Classify unknown candidates before dropping the request.
            PendingPortionIds.emplace(portionId);
        } else {
            PendingPortionIds.erase(portionId);
        }
    } else {
        PendingPortionIds.erase(portionId);
    }
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
            switch (AddPortionToMove(tasksContext, portion, VersionedIndex)) {
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
        RejectedPortionIds.emplace(portionId);
        RetiredPortionIds.erase(portionId);
        ++RejectedPortions;
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, "",
            {"event", "move_data_portion_rejected"},
            {"portionId", portionId},
            {"blobs", JoinSeq(",", GetBlobGroupsForLog(accessor.GetBlobIds()))},
            {"targets", JoinSeq(",", TargetGroups)});
        return;
    }
    TargetPortionIds.emplace(portionId);
    if (RetiredPortionIds.contains(portionId)) {
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

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
    const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self) {
    if (PendingPortionIds.empty()) {
        return {};
    }
    const ui64 batchMemorySoftLimit = NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit();
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;
    ui64 currentRequestMemory = 0;

    for (auto portionId : PendingPortionIds) {
        TPortionInfo::TPtr portion;
        if (const auto* found = MapFindPtr(portions, portionId)) {
            portion = *found;
        } else if (const auto* found = MapFindPtr(uncommitted, portionId)) {
            portion = *found;
        } else {
            continue;
        }
        if (!currentRequest) {
            currentRequest = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::MOVE_DATA);
        }
        currentRequest->AddPortion(portion);
        currentRequestMemory += portion->PredictAccessorsMemory(portion->GetSchema(VersionedIndex));
        if (currentRequestMemory >= batchMemorySoftLimit) {
            requests.emplace_back(currentRequest, std::make_shared<TMoveDataActualizationReply>(self));
            currentRequest.reset();
            currentRequestMemory = 0;
        }
    }
    if (currentRequest) {
        requests.emplace_back(std::move(currentRequest), std::make_shared<TMoveDataActualizationReply>(self));
    }
    return requests;
}

void TMoveDataActualizer::PruneRetiredPortions(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    // Cleanup does not notify actualizers again for an already-retired portion.
    // An empty THashSet can retain buckets; avoid walking them on every driver turn.
    if (!RetiredPortionIds.empty()) {
        EraseNodesIf(RetiredPortionIds, [&](const ui64 portionId) {
            if (portions.contains(portionId) || uncommitted.contains(portionId)) {
                return false;
            }
            PendingPortionIds.erase(portionId);
            return true;
        });
    }
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    return TMoveDataQueueSizes{ .Pending = PendingPortionIds.size(), .ConfirmedToMove = PortionAddress.size(),
        .InFlight = InFlightPortionIds.size(),
        .Uncommitted = UncommittedOnTarget.size(),
        .Retired = RetiredPortionIds.size(),
        .Rejected = RejectedPortions };
}

void TMoveDataActualizer::Seed(
    const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    AFL_VERIFY(InitialPortionIds.empty() && PendingPortionIds.empty());
    for (auto& [portionId, portion] : externalContext.GetPortions()) {
        if (!HasEntityInDefaultStorage(*portion, VersionedIndex)) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
        PendingPortionIds.emplace(portionId);
        // Retired candidates still need their blob groups classified; only target data blocks cleanup.
        if (portion->HasRemoveSnapshot()) {
            RetiredPortionIds.emplace(portionId);
            continue;
        }
    }
    // Initial membership lets a write that commits after the session started still move.
    for (const auto& [portionId, portion] : uncommitted) {
        if (!HasEntityInDefaultStorage(*portion, VersionedIndex)) {
            continue;
        }
        InitialPortionIds.emplace(portionId);
        PendingPortionIds.emplace(portionId);
        // An aborted write is classified too: only target-group data must wait for its cleanup.
        if (portion->HasRemoveSnapshot()) {
            RetiredPortionIds.emplace(portionId);
            continue;
        }
        UncommittedPortionIds.emplace(portionId);
    }
}

}   // namespace NKikimr::NOlap::NActualizer
