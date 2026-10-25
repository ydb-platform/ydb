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
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "MoveData accessor request failed",
                {"error", result.GetValue().GetErrorMessage()});
        }
        if (result.GetValue().HasRemovedData()) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "MoveData accessor result contains removed portions",
                {"removedCount", result.GetValue().GetRemovedData().size()});
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

TTieringProcessContext::EAddPortionResult AddPortionToMove(
    TTieringProcessContext& tasks, const TPortionInfo::TConstPtr& portion, const TVersionedIndex& index) {
    auto currentSchema = portion->GetSchema(index);
    auto targetSchema = currentSchema;
    auto tier = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
    TPortionEvictionFeatures features(std::move(currentSchema), std::move(targetSchema), tier);
    features.SetTargetTierName(std::move(tier));
    features.SetForcedMove();
    return tasks.AddPortion(portion, std::move(features), TDuration::Zero());
}

}   // anonymous namespace

void TMoveDataActualizer::SetState(EPortionState& state, EPortionState next) {
    AFL_VERIFY(StateCounts[static_cast<size_t>(state)]);
    --StateCounts[static_cast<size_t>(state)];
    ++StateCounts[static_cast<size_t>(next)];
    state = next;
}

void TMoveDataActualizer::ErasePortion(TPortions::iterator portion) {
    AFL_VERIFY(portion->second == EPortionState::Finished || portion->second == EPortionState::Rejected);
    const auto next = std::next(portion);
    for (auto* cursor : { &StateScanPosition, &RewriteScanPosition, &MetadataScanPosition }) {
        if (*cursor == portion) {
            *cursor = next;
        }
    }
    PortionsById.erase(portion->first);
    Portions.erase(portion);
    // Terminal counters are cumulative; only active states count towards the completion gate.
}

void TMoveDataActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext&) {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::MoveData)) {
        return;
    }
    ScanPortions(RewriteScanPosition, PortionsPerPass, [&](ui64 portionId, EPortionState& state) {
        if (state != EPortionState::Ready && state != EPortionState::RewriteStarted) {
            return true;
        }
        const auto* portion = MapFindPtr(externalContext.GetPortions(), portionId);
        // A commit may still be completing, or another background task may have retired the portion since the last pass.
        if (!portion || (*portion)->HasRemoveSnapshot()) {
            return true;
        }
        AFL_VERIFY((*portion)->IsCommitted());
        switch (AddPortionToMove(tasksContext, *portion, VersionedIndex)) {
            case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                return false;
            case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                break;
            case TTieringProcessContext::EAddPortionResult::SUCCESS:
                // Retain the candidate. Its task lock prevents duplicates; an abort releases the lock for the next pass.
                SetState(state, EPortionState::RewriteStarted);
                break;
        }
        return true;
    });
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
    const auto found = PortionsById.find(portionId);
    if (found == PortionsById.end()) {
        return;
    }
    const auto it = found->second;
    if (it->second != EPortionState::Pending) {
        return;
    }
    if (!HasBlobInGroups(accessor.GetBlobIds(), TargetGroups)) {
        SetState(it->second, EPortionState::Rejected);
        ErasePortion(it);
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, "MoveData portion has no blobs in target groups",
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
    const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self,
    TMoveDataMetadataBudget& budget) {
    if (!StateCounts[static_cast<size_t>(EPortionState::Pending)] || budget.IsExhausted()) {
        return {};
    }
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;
    ScanPortions(MetadataScanPosition, budget.PortionsLeft, [&](ui64 portionId, EPortionState state) {
        --budget.PortionsLeft;
        if (state == EPortionState::Pending) {
            TPortionInfo::TPtr portion;
            if (const auto* found = MapFindPtr(portions, portionId)) {
                portion = *found;
            } else if (const auto* found = MapFindPtr(uncommitted, portionId)) {
                portion = *found;
            }
            if (portion) {
                if (!currentRequest) {
                    currentRequest = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::MOVE_DATA);
                }
                currentRequest->AddPortion(portion);
                budget.Memory += portion->PredictAccessorsMemory(portion->GetSchema(VersionedIndex));
                ++budget.RequestedPortions;
            }
        }
        return !budget.IsExhausted();
    });
    if (currentRequest) {
        requests.emplace_back(std::move(currentRequest), std::make_shared<TMoveDataActualizationReply>(self));
    }
    return requests;
}

void TMoveDataActualizer::UpdatePortionStates(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    ScanPortions(StateScanPosition, PortionsPerPass, [&](ui64 portionId, EPortionState& state) {
        TPortionInfo::TPtr portion;
        bool committed = false;
        if (const auto* found = MapFindPtr(portions, portionId)) {
            portion = *found;
            committed = true;
        } else if (const auto* found = MapFindPtr(uncommitted, portionId)) {
            portion = *found;
        } else {
            // Cleanup has committed removal of the portion.
            SetState(state, EPortionState::Finished);
            return true;
        }
        if (state == EPortionState::Pending) {
            // Retired candidates still need classification: unrelated groups must not hold the gate.
            return true;
        }
        if (portion->HasRemoveSnapshot()) {
            SetState(state, EPortionState::Retired);
        } else if (!committed) {
            SetState(state, EPortionState::Uncommitted);
        } else if (state != EPortionState::RewriteStarted) {
            SetState(state, EPortionState::Ready);
        }
        return true;
    });
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    const auto count = [this](EPortionState state) {
        return StateCounts[static_cast<size_t>(state)];
    };
    return TMoveDataQueueSizes{ .Pending = count(EPortionState::Pending), .ConfirmedToMove = count(EPortionState::Ready),
        .RewriteStarted = count(EPortionState::RewriteStarted),
        .Uncommitted = count(EPortionState::Uncommitted),
        .Retired = count(EPortionState::Retired),
        .Rejected = count(EPortionState::Rejected) };
}

void TMoveDataActualizer::Seed(
    const TAddExternalContext& externalContext, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    AFL_VERIFY(!Seeded);
    Seeded = true;
    const auto add = [this](const auto& source) {
        for (const auto& [portionId, portion] : source) {
            if (portion->HasBlobsInBlobStorage(portion->GetSchema(VersionedIndex)->GetIndexInfo())) {
                const auto entry = Portions.emplace(Portions.end(), portionId, EPortionState::Pending);
                AFL_VERIFY(PortionsById.emplace(portionId, entry).second);
                ++StateCounts[static_cast<size_t>(EPortionState::Pending)];
            }
        }
    };
    add(externalContext.GetPortions());
    // A persisted uncommitted portion can survive both restart and acknowledgement of its Keep flag.
    add(uncommitted);
}

}   // namespace NKikimr::NOlap::NActualizer
