#include "accumulation_level.h"
#include "common_level.h"
#include "optimizer.h"
#include "zero_level.h"

#include <util/string/join.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TOptimizerPlanner::TOptimizerPlanner(
    const ui64 pathId, const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema)
    : TBase(pathId)
    , Counters(std::make_shared<TCounters>())
    , StoragesManager(storagesManager)
    , PrimaryKeysSchema(primaryKeysSchema) {
    std::vector<ui64> levelLimits = { (ui64)1 << 13, (ui64)1 << 16, (ui64)1 << 19, (ui64)1 << 22, (ui64)1 << 25, (ui64)1 << 28, (ui64)1 << 31,
        (ui64)1 << 34, (ui64)1 << 40 };
    std::shared_ptr<IPortionsLevel> nextLevel;
    for (i32 i = levelLimits.size() - 1; i >= 2; --i) {
        nextLevel = std::make_shared<TLevelPortions>(i, levelLimits[i], 0, nextLevel, Counters->Levels[i]);
        Levels.insert(Levels.begin(), nextLevel);
    }
    Levels.insert(Levels.begin(), std::make_shared<TAccumulationLevelPortions>(1, 0, 0, Levels.front(), Counters->Levels[1]));
    Levels.insert(Levels.begin(), std::make_shared<TZeroLevelPortions>(0, 0, Levels.front(), Counters->Levels[0]));
    RefreshWeights();
}

std::shared_ptr<NKikimr::NOlap::TColumnEngineChanges> TOptimizerPlanner::DoGetOptimizationTask(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
    AFL_VERIFY(LevelsByWeight.size());
    auto level = LevelsByWeight.begin()->second;
    auto data = level->GetOptimizationTask();
    TSaverContext saverContext(StoragesManager);
    auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, data.GetPortions(), saverContext);
    result->SetTargetCompactionLevel(data.GetTargetCompactionLevel());
    auto positions = data.GetCheckPositions(PrimaryKeysSchema);
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("task_id", result->GetTaskIdentifier())("positions", positions.DebugString())(
        "level", level->GetLevelId())("portions", JoinSeq(",", data.GetPortionIds()))("target", data.GetTargetCompactionLevel())(
        "data", data.DebugString());
    result->SetCheckPoints(std::move(positions));
    for (auto&& i : result->SwitchedPortions) {
        AFL_VERIFY(!locksManager->IsLocked(i));
    }
    return result;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
