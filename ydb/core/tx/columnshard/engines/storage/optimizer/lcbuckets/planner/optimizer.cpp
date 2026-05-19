#include "optimizer.h"

#include "level/one_layer.h"
#include "level/zero_level.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/constructor/constructor.h>

#include <util/string/join.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

namespace {

std::shared_ptr<TColumnEngineChanges> BuildCompactionChange(const std::shared_ptr<TGranuleMeta>& granule,
    const std::shared_ptr<IPortionsLevel>& level, TCompactionTaskData& data, const TSaverContext& saverContext,
    const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const std::vector<std::shared_ptr<IPortionsLevel>>& levels) {
    auto result =
        std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, data.GetRepackPortions(level->GetLevelId()), saverContext);
    result->SetTargetCompactionLevel(data.GetTargetCompactionLevel());
    result->SetPortionExpectedSize(levels[data.GetTargetCompactionLevel()]->GetExpectedPortionSize());
    auto positions = data.GetCheckPositions(primaryKeysSchema, level->GetLevelId() > 1);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("task_id", result->GetTaskIdentifier())("positions", positions.DebugString())(
        "level", level->GetLevelId())("target", data.GetTargetCompactionLevel())("data", data.DebugString());
    result->SetCheckPoints(std::move(positions));
    return result;
}

}   // namespace

TOptimizerPlanner::TOptimizerPlanner(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager,
    const std::shared_ptr<arrow::Schema>& primaryKeysSchema, std::shared_ptr<TCounters> counters,
    std::shared_ptr<TSimplePortionsGroupInfo> portionsGroupInfo, std::vector<std::shared_ptr<IPortionsLevel>>&& levels,
    std::vector<std::shared_ptr<IPortionsSelector>>&& selectors, const std::optional<ui64>& nodePortionsCountLimit)
    : TBase(pathId, nodePortionsCountLimit)
    , Counters(counters)
    , PortionsInfo(portionsGroupInfo)
    , Selectors(std::move(selectors))
    , Levels(std::move(levels))
    , StoragesManager(storagesManager)
    , PrimaryKeysSchema(primaryKeysSchema)
{
    RefreshWeights();
}

std::vector<std::shared_ptr<TColumnEngineChanges>> TOptimizerPlanner::DoGetOptimizationTasks(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
    AFL_VERIFY(LevelsByWeight.size());

    TSaverContext saverContext(StoragesManager);
    std::vector<std::shared_ptr<TColumnEngineChanges>> results;
    bool hasOneLayer = false;
    const auto mayUsePortion = [&](const TPortionInfo::TConstPtr& p) {
        return !locksManager->IsLocked(p, NDataLocks::ELockCategory::Compaction);
    };
    for (const auto& [weight, level] : LevelsByWeight) {
        if (weight == 0) {
            break;
        }
        auto tasks = level->GetOptimizationTasks(mayUsePortion);
        for (auto& data : tasks) {
            if (data.IsEmpty()) {
                continue;
            }
            hasOneLayer |= dynamic_pointer_cast<TOneLayerPortions>(level) != nullptr;
            if (hasOneLayer && !results.empty()) {
                return results;
            }

            results.push_back(BuildCompactionChange(granule, level, data, saverContext, PrimaryKeysSchema, Levels));
            if (!AppDataVerified().ColumnShardConfig.GetEnableParallelCompaction()) {
                return results;
            }
        }
    }
    return results;
}

// Reserved for future pull scheduling; tablet still uses batch StartCompaction for lc-buckets.
std::shared_ptr<TColumnEngineChanges> TOptimizerPlanner::DoGetNextOptimizationTask(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
    AFL_VERIFY(LevelsByWeight.size());

    TSaverContext saverContext(StoragesManager);
    const auto mayUsePortion = [&](const TPortionInfo::TConstPtr& p) {
        return !locksManager->IsLocked(p, NDataLocks::ELockCategory::Compaction);
    };
    for (const auto& [weight, level] : LevelsByWeight) {
        if (weight == 0) {
            break;
        }
        if (dynamic_pointer_cast<TOneLayerPortions>(level) != nullptr) {
            auto tasks = level->GetOptimizationTasks(mayUsePortion);
            for (auto& data : tasks) {
                if (!data.IsEmpty()) {
                    return BuildCompactionChange(granule, level, data, saverContext, PrimaryKeysSchema, Levels);
                }
            }
            continue;
        }
        auto task = level->GetNextOptimizationTask(mayUsePortion);
        if (task && !task->IsEmpty()) {
            return BuildCompactionChange(granule, level, *task, saverContext, PrimaryKeysSchema, Levels);
        }
    }
    return nullptr;
}

ui32 TOptimizerPlanner::DoGetMaxCompactionInflight() const {
    if (!AppDataVerified().ColumnShardConfig.GetEnableParallelCompaction()) {
        return 1;
    }
    ui32 maxConcurrency = 1;
    for (const auto& level : Levels) {
        if (const auto zeroLevel = std::dynamic_pointer_cast<TZeroLevelPortions>(level)) {
            maxConcurrency = Max(maxConcurrency, static_cast<ui32>(zeroLevel->GetMaxConcurrency()));
        }
    }
    return maxConcurrency;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
