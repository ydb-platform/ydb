#include "optimizer.h"

#include "level/common_level.h"
#include "level/zero_level.h"
#include "selector/snapshot.h"
#include "selector/transparent.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/constructor/constructor.h>

#include <util/string/join.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TOptimizerPlanner::TOptimizerPlanner(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager,
    const std::shared_ptr<arrow::Schema>& primaryKeysSchema, std::shared_ptr<TCounters> counters, std::shared_ptr<TSimplePortionsGroupInfo> portionsGroupInfo, std::vector<std::shared_ptr<IPortionsLevel>>&& levels,
    std::vector<std::shared_ptr<IPortionsSelector>>&& selectors, const std::optional<ui32>& nodePortionsCountLimit)
    : TBase(pathId, nodePortionsCountLimit)
    , Counters(counters)
    , PortionsInfo(portionsGroupInfo)
    , Selectors(std::move(selectors))
    , Levels(std::move(levels))
    , StoragesManager(storagesManager)
    , PrimaryKeysSchema(primaryKeysSchema) {
    RefreshWeights();
}

std::shared_ptr<TColumnEngineChanges> TOptimizerPlanner::DoGetOptimizationTask(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
    AFL_VERIFY(LevelsByWeight.size());
    auto level = LevelsByWeight.begin()->second;
    auto data = level->GetOptimizationTask();
    TSaverContext saverContext(StoragesManager);
    std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges> result;
    //    if (level->GetLevelId() == 0) {
    result =
        std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, data.GetRepackPortions(level->GetLevelId()), saverContext);
    //    } else {
    //        result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(
    //            granule, data.GetRepackPortions(level->GetLevelId()), saverContext);
    //        result->AddMovePortions(data.GetMovePortions());
    //    }
    result->SetTargetCompactionLevel(data.GetTargetCompactionLevel());
    result->SetPortionExpectedSize(Levels[data.GetTargetCompactionLevel()]->GetExpectedPortionSize());
    auto positions = data.GetCheckPositions(PrimaryKeysSchema, level->GetLevelId() > 1);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("task_id", result->GetTaskIdentifier())("positions", positions.DebugString())(
        "level", level->GetLevelId())("target", data.GetTargetCompactionLevel())("data", data.DebugString());
    result->SetCheckPoints(std::move(positions));
    for (auto&& i : result->GetSwitchedPortions()) {
        AFL_VERIFY(!locksManager->IsLocked(i, NDataLocks::ELockCategory::Compaction));
    }
    return result;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
