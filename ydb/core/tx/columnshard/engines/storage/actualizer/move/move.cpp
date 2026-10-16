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
            // Affected portions stay pending and are re-requested next cycle.
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

TMoveDataActualizer::TPortions::iterator TMoveDataActualizer::NextPortion(TPortions::iterator& position) {
    if (position == Portions.end()) {
        position = Portions.begin();
    }
    return position++;
}

void TMoveDataActualizer::SetState(EPortionState& state, EPortionState next) {
    AFL_VERIFY(StateCounts[static_cast<size_t>(state)]);
    --StateCounts[static_cast<size_t>(state)];
    ++StateCounts[static_cast<size_t>(next)];
    state = next;
}

void TMoveDataActualizer::ErasePortion(TPortions::iterator it) {
    if (RefreshPosition == it) {
        ++RefreshPosition;
    }
    if (RewritePosition == it) {
        ++RewritePosition;
    }
    AFL_VERIFY(StateCounts[static_cast<size_t>(it->second)]);
    --StateCounts[static_cast<size_t>(it->second)];
    Portions.erase(it);
}

void TMoveDataActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext&) {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::MoveData)) {
        return;
    }
    const size_t count = Min(PortionsPerPass, Portions.size());
    for (size_t i = 0; i < count; ++i) {
        auto it = NextPortion(RewritePosition);
        if (it->second != EPortionState::Ready && it->second != EPortionState::InFlight) {
            continue;
        }
        const auto* portion = MapFindPtr(externalContext.GetPortions(), it->first);
        // A commit may still be completing, or another background task may have retired the portion since the last pass.
        if (!portion || (*portion)->HasRemoveSnapshot() || !(*portion)->IsCommitted()) {
            continue;
        }
        switch (AddPortionToMove(tasksContext, *portion, VersionedIndex)) {
            case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                return;
            case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                break;
            case TTieringProcessContext::EAddPortionResult::SUCCESS:
                // Retain the candidate. Its task lock prevents duplicates; an abort releases the lock for the next pass.
                SetState(it->second, EPortionState::InFlight);
                break;
        }
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
    const auto& info = accessor.GetPortionInfo();
    const ui64 portionId = info.GetPortionId();
    auto it = Portions.find(portionId);
    if (it == Portions.end() || it->second != EPortionState::Pending) {
        return;
    }
    if (!HasBlobInGroups(accessor.GetBlobIds(), TargetGroups)) {
        ErasePortion(it);
        ++RejectedPortions;
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, "",
            {"event", "move_data_portion_rejected"},
            {"portionId", portionId},
            {"blobs", JoinSeq(",", GetBlobGroupsForLog(accessor.GetBlobIds()))},
            {"targets", JoinSeq(",", TargetGroups)});
        return;
    }
    SetState(it->second,
        info.HasRemoveSnapshot() ? EPortionState::Retired : (info.IsCommitted() ? EPortionState::Ready : EPortionState::Uncommitted));
}

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
    const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self) {
    if (!StateCounts[static_cast<size_t>(EPortionState::Pending)]) {
        return {};
    }
    const ui64 batchMemorySoftLimit = NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit();
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;
    ui64 currentRequestMemory = 0;

    for (const auto& [portionId, state] : Portions) {
        if (state != EPortionState::Pending) {
            continue;
        }
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

void TMoveDataActualizer::RefreshPortions(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    const size_t count = Min(PortionsPerPass, Portions.size());
    for (size_t i = 0; i < count; ++i) {
        auto it = NextPortion(RefreshPosition);
        TPortionInfo::TPtr portion;
        bool committed = false;
        if (const auto* found = MapFindPtr(portions, it->first)) {
            portion = *found;
            committed = true;
        } else if (const auto* found = MapFindPtr(uncommitted, it->first)) {
            portion = *found;
        } else {
            // Cleanup has removed the portion; durable keep/delete checks now cover its blobs.
            ErasePortion(it);
            continue;
        }
        if (it->second == EPortionState::Pending) {
            // Retired candidates still need classification: unrelated groups must not hold the gate.
            continue;
        }
        if (portion->HasRemoveSnapshot()) {
            SetState(it->second, EPortionState::Retired);
        } else if (!committed) {
            SetState(it->second, EPortionState::Uncommitted);
        } else if (it->second != EPortionState::InFlight) {
            SetState(it->second, EPortionState::Ready);
        }
    }
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    const auto count = [this](EPortionState state) {
        return StateCounts[static_cast<size_t>(state)];
    };
    return TMoveDataQueueSizes{ .Pending = count(EPortionState::Pending), .ConfirmedToMove = count(EPortionState::Ready),
        .InFlight = count(EPortionState::InFlight),
        .Uncommitted = count(EPortionState::Uncommitted),
        .Retired = count(EPortionState::Retired),
        .Rejected = RejectedPortions };
}

void TMoveDataActualizer::Seed(
    const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    AFL_VERIFY(Portions.empty());
    const auto add = [this](const auto& source) {
        for (const auto& [portionId, portion] : source) {
            if (HasEntityInDefaultStorage(*portion, VersionedIndex)) {
                AFL_VERIFY(Portions.emplace(portionId, EPortionState::Pending).second);
                ++StateCounts[static_cast<size_t>(EPortionState::Pending)];
            }
        }
    };
    add(externalContext.GetPortions());
    // A persisted uncommitted portion can survive both restart and acknowledgement of its Keep flag.
    add(uncommitted);
    RefreshPosition = RewritePosition = Portions.begin();
}

}   // namespace NKikimr::NOlap::NActualizer
