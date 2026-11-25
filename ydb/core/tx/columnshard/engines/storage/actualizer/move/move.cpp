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
    TMoveDataActualizationReply(std::weak_ptr<TMoveDataActualizer> actualizer)
        : MoveDataActualizer(std::move(actualizer))
    {
        AFL_VERIFY(!MoveDataActualizer.expired());
    }
};

TTieringProcessContext::EAddPortionResult AddPortionToMove(
    TTieringProcessContext& tasks, const TPortionInfo::TConstPtr& portion, const TVersionedIndex& index) {
    const auto schema = portion->GetSchema(index);
    auto tier = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
    TPortionEvictionFeatures features(portion->GetSchemaVersionVerified(), schema, schema, tier);
    features.SetTargetTierName(std::move(tier));
    features.SetForcedMove();
    return tasks.AddPortion(portion, std::move(features), TDuration::Zero());
}

}   // anonymous namespace

ui64& TMoveDataActualizer::TPortionQueue::Count(EPortionState state) {
    return const_cast<ui64&>(std::as_const(*this).Count(state));
}

const ui64& TMoveDataActualizer::TPortionQueue::Count(EPortionState state) const {
    return StateCounts[static_cast<size_t>(state)];
}

void TMoveDataActualizer::TPortionQueue::SetState(EPortionState& state, EPortionState next) {
    AFL_VERIFY(Count(state));
    --Count(state);
    ++Count(next);
    state = next;
}

void TMoveDataActualizer::TPortionQueue::ErasePortion(TPortions::iterator portion) {
    AFL_VERIFY(portion->second == EPortionState::Finished || portion->second == EPortionState::Rejected);
    const auto next = std::next(portion);
    for (auto* cursor : { &StateUpdateCursor, &RewriteCursor, &MetadataCursor }) {
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
    Queue.ScanForRewrite(PortionsPerPass, [&](ui64 portionId, EPortionState& state) {
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
                Queue.SetState(state, EPortionState::RewriteStarted);
                break;
        }
        return true;
    });
}

namespace {
std::vector<ui32> GetDistinctBlobGroups(const std::vector<TUnifiedBlobId>& blobIds) {
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
    Queue.ClassifyPending(portionId, [&] {
        if (!HasBlobInGroups(accessor.GetBlobIds(), TargetGroups)) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, "MoveData portion has no blobs in target groups",
                {"event", "move_data_portion_rejected"},
                {"portionId", portionId},
                {"blobs", JoinSeq(",", GetDistinctBlobGroups(accessor.GetBlobIds()))},
                {"targets", JoinSeq(",", TargetGroups)});
            return EPortionState::Rejected;
        }
        if (info.HasRemoveSnapshot()) {
            return EPortionState::Retired;
        }
        return info.IsCommitted() ? EPortionState::Ready : EPortionState::Uncommitted;
    });
}

std::vector<TCSMetadataRequest> TMoveDataActualizer::BuildMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
    const TWrittenPortionsById& uncommitted, const std::shared_ptr<TMoveDataActualizer>& self,
    TMoveDataMetadataBudget& budget) {
    if (!Queue.HasPending() || budget.IsExhausted()) {
        return {};
    }
    std::vector<TCSMetadataRequest> requests;
    std::shared_ptr<TDataAccessorsRequest> currentRequest;
    Queue.ScanForMetadata(budget.PortionsLeft, [&](ui64 portionId, EPortionState state) {
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

size_t TMoveDataActualizer::UpdatePortionStates(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const TWrittenPortionsById& uncommitted, size_t limit) {
    return Queue.ScanForStateUpdate(limit, [&](ui64 portionId, EPortionState& state) {
        TPortionInfo::TPtr portion;
        bool committed = false;
        if (const auto* found = MapFindPtr(portions, portionId)) {
            portion = *found;
            committed = true;
        } else if (const auto* found = MapFindPtr(uncommitted, portionId)) {
            portion = *found;
        } else {
            // Cleanup has committed removal of the portion.
            Queue.SetState(state, EPortionState::Finished);
            return true;
        }
        if (state == EPortionState::Pending) {
            // Retired candidates still need classification: unrelated groups must not hold the gate.
            return true;
        }
        if (portion->HasRemoveSnapshot()) {
            Queue.SetState(state, EPortionState::Retired);
        } else if (!committed) {
            Queue.SetState(state, EPortionState::Uncommitted);
        } else if (state != EPortionState::RewriteStarted) {
            Queue.SetState(state, EPortionState::Ready);
        }
        return true;
    });
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes() const {
    return Queue.GetSizes();
}

TMoveDataQueueSizes TMoveDataActualizer::TPortionQueue::GetSizes() const {
    return TMoveDataQueueSizes{
        .Pending = Count(EPortionState::Pending),
        .ConfirmedToMove = Count(EPortionState::Ready),
        .RewriteStarted = Count(EPortionState::RewriteStarted),
        .Uncommitted = Count(EPortionState::Uncommitted),
        .Retired = Count(EPortionState::Retired),
        .Rejected = Count(EPortionState::Rejected),
    };
}

void TMoveDataActualizer::TPortionQueue::Add(ui64 portionId) {
    const auto entry = Portions.emplace(Portions.end(), portionId, EPortionState::Pending);
    AFL_VERIFY(PortionsById.emplace(portionId, entry).second);
    ++Count(EPortionState::Pending);
}

void TMoveDataActualizer::Seed(
    const TAddExternalContext& externalContext, const TWrittenPortionsById& uncommitted) {
    AFL_VERIFY(!Seeded);
    Seeded = true;
    const auto add = [this](const auto& source) {
        for (const auto& [portionId, portion] : source) {
            if (portion->HasBlobsInBlobStorage(portion->GetSchema(VersionedIndex)->GetIndexInfo())) {
                Queue.Add(portionId);
            }
        }
    };
    add(externalContext.GetPortions());
    add(uncommitted);
}

}   // namespace NKikimr::NOlap::NActualizer
