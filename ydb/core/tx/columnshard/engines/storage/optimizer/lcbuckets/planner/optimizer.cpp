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
    std::shared_ptr<IPortionsLevel> nextLevel;
/*
    const ui64 maxPortionBlobBytes = (ui64)1 << 20;
    Levels.emplace_back(
        std::make_shared<TLevelPortions>(2, 0.9, maxPortionBlobBytes, nullptr, PortionsInfo, Counters->GetLevelCounters(2)));
    Levels.emplace_back(
        std::make_shared<TLevelPortions>(1, 0.1, maxPortionBlobBytes, Levels.back(), PortionsInfo, Counters->GetLevelCounters(1)));
*/
    Levels.emplace_back(std::make_shared<TZeroLevelPortions>(1, nullptr, Counters->GetLevelCounters(2)));
    Levels.emplace_back(std::make_shared<TZeroLevelPortions>(0, Levels.back(), Counters->GetLevelCounters(0)));
    std::reverse(Levels.begin(), Levels.end());
    RefreshWeights();
}

std::shared_ptr<NKikimr::NOlap::TColumnEngineChanges> TOptimizerPlanner::DoGetOptimizationTask(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
    AFL_VERIFY(LevelsByWeight.size());
    auto level = LevelsByWeight.begin()->second;
    auto data = level->GetOptimizationTask();
    TSaverContext saverContext(StoragesManager);
    std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges> result;
    if (level->GetLevelId() == 0) {
        result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(
            granule, data.GetRepackPortions(level->GetLevelId()), saverContext);
    } else {
        result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(
            granule, data.GetRepackPortions(level->GetLevelId()), saverContext);
        result->AddMovePortions(data.GetMovePortions());
    }
    result->SetTargetCompactionLevel(data.GetTargetCompactionLevel());
    auto levelPortions = std::dynamic_pointer_cast<TLevelPortions>(Levels[data.GetTargetCompactionLevel()]);
    if (levelPortions) {
        result->SetPortionExpectedSize(levelPortions->GetExpectedPortionSize());
    }
    auto positions = data.GetCheckPositions(PrimaryKeysSchema, level->GetLevelId() > 1);
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("task_id", result->GetTaskIdentifier())("positions", positions.DebugString())(
        "level", level->GetLevelId())("target", data.GetTargetCompactionLevel())("data", data.DebugString());
    result->SetCheckPoints(std::move(positions));
    for (auto&& i : result->SwitchedPortions) {
        AFL_VERIFY(!locksManager->IsLocked(i));
    }
    return result;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
