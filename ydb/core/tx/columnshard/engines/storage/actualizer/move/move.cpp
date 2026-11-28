#include "move.h"

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

using EPortionState = TPortionQueue::EPortionState;

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

bool TMoveDataActualizer::HasBlobInGroups(const std::vector<TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups) {
    return AnyOf(blobIds, [&groups](const TUnifiedBlobId& blobId) {
        return groups.contains(blobId.GetDsGroup());
    });
}

void TMoveDataActualizer::AddCandidate(ui64 portionId, bool committed) {
    Queue.Add(portionId, committed ? EPortionState::Ready : EPortionState::Uncommitted);
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
        if (portion->HasRemoveSnapshot()) {
            Queue.SetState(state, EPortionState::Finished);
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

}   // namespace NKikimr::NOlap::NActualizer
