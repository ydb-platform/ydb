#pragma once
#include "level/abstract.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TLevelConstructorContainer;
class TSelectorConstructorContainer;

class TOptimizerPlanner: public IOptimizerPlanner {
private:
    using TBase = IOptimizerPlanner;
    std::shared_ptr<TCounters> Counters;
    std::shared_ptr<TSimplePortionsGroupInfo> PortionsInfo;

    std::vector<std::shared_ptr<IPortionsSelector>> Selectors;
    std::vector<std::shared_ptr<IPortionsLevel>> Levels;
    std::map<ui64, std::shared_ptr<IPortionsLevel>, std::greater<ui64>> LevelsByWeight;
    const std::shared_ptr<IStoragesManager> StoragesManager;
    const std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    virtual ui32 GetAppropriateLevel(const ui32 baseLevel, const TPortionInfoForCompaction& info) const override {
        ui32 result = baseLevel;
        for (ui32 i = baseLevel; i + 1 < Levels.size(); ++i) {
            if (Levels[i]->IsAppropriatePortionToMove(info) && Levels[i + 1]->IsAppropriatePortionToStore(info)) {
                result = i + 1;
            } else {
                break;
            }
        }
        return result;
    }

    virtual bool DoIsOverloaded() const override {
        for (auto&& i : Levels) {
            if (i->IsOverloaded()) {
                return true;
            }
        }
        return false;
    }

    virtual std::vector<TTaskDescription> DoGetTasksDescription() const override {
        std::vector<TTaskDescription> result;
        for (auto&& i : Levels) {
            result.emplace_back(i->GetTaskDescription());
        }
        return result;
    }

    void RefreshWeights() {
        LevelsByWeight.clear();
        for (ui32 i = 0; i < Levels.size(); ++i) {
            LevelsByWeight.emplace(Levels[i]->GetWeight(), Levels[i]);
        }
    }

protected:
    virtual bool DoIsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        for (auto&& i : Levels) {
            if (i->IsLocked(dataLocksManager)) {
                return true;
            }
        }
        return false;
    }

    virtual void DoModifyPortions(const THashMap<ui64, TPortionInfo::TPtr>& add, const THashMap<ui64, TPortionInfo::TPtr>& remove) override {
        std::vector<std::vector<TPortionInfo::TPtr>> removePortionsByLevel;
        removePortionsByLevel.resize(Levels.size());
        std::vector<std::vector<TPortionInfo::TPtr>> addPortionsByLevels;
        addPortionsByLevels.resize(Levels.size());
        for (auto&& [_, i] : remove) {
            if (i->GetProduced() == NPortion::EProduced::EVICTED) {
                continue;
            }
            PortionsInfo->RemovePortion(i);
            AFL_VERIFY(i->GetCompactionLevel() < Levels.size());
            removePortionsByLevel[i->GetCompactionLevel()].emplace_back(i);
        }
        std::vector<TPortionInfo::TPtr> problemPortions;
        for (auto&& [_, i] : add) {
            if (i->GetProduced() == NPortion::EProduced::EVICTED) {
                continue;
            }
            PortionsInfo->AddPortion(i);
            if (i->GetCompactionLevel() >= Levels.size()) {
                problemPortions.emplace_back(i);
            } else {
                addPortionsByLevels[i->GetMeta().GetCompactionLevel()].emplace_back(i);
            }
        }
        for (ui32 i = 0; i < Levels.size(); ++i) {
            auto problems = Levels[i]->ModifyPortions(addPortionsByLevels[i], removePortionsByLevel[i]);
            problemPortions.insert(problemPortions.end(), problems.begin(), problems.end());
        }
        for (auto&& i : problemPortions) {
            i->MutableMeta().ResetCompactionLevel(0);
            if (!i->GetCompactionLevel() && i->GetPortionType() != EPortionType::Written) {
                i->MutableMeta().ResetCompactionLevel(GetAppropriateLevel(0, i->GetCompactionInfo()));
            }
            AFL_VERIFY(i->GetCompactionLevel() < Levels.size());
            AFL_VERIFY(Levels[i->GetMeta().GetCompactionLevel()]->ModifyPortions({ i }, {}).empty());
        }
        RefreshWeights();
    }
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override;

    virtual void DoActualize(const TInstant currentInstant) override {
        for (const auto& level : Levels) {
            if (currentInstant >= level->GetWeightExpirationInstant()) {
                return RefreshWeights();
            }
        }
    }

    virtual TOptimizationPriority DoGetUsefulMetric() const override {
        AFL_VERIFY(LevelsByWeight.size());
        const ui64 levelPriority = LevelsByWeight.begin()->first;
        if (levelPriority) {
            return TOptimizationPriority::Critical(levelPriority);
        } else {
            return TOptimizationPriority::Zero();
        }
    }

    virtual TString DoDebugString() const override {
        TStringBuilder sb;
        sb << "[";
        for (auto&& i : Levels) {
            sb << "{" << i->GetLevelId() << ":" << i->DebugString() << "},";
        }
        sb << "]";
        return sb;
    }

    virtual NJson::TJsonValue DoSerializeToJsonVisual() const override {
        NJson::TJsonValue arr = NJson::JSON_MAP;
        NJson::TJsonValue& arrLevels = arr.InsertValue("levels", NJson::JSON_ARRAY);
        for (auto&& i : Levels) {
            arrLevels.AppendValue(i->SerializeToJson());
        }
        return arr;
    }

public:
    virtual NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        NArrow::NMerger::TIntervalPositions result = Levels.back()->GetBucketPositions(PrimaryKeysSchema);
        return result;
    }

    ~TOptimizerPlanner() = default;

    TOptimizerPlanner(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<arrow::Schema>& primaryKeysSchema, std::shared_ptr<TCounters> counters, std::shared_ptr<TSimplePortionsGroupInfo> portionsGroupInfo,
        std::vector<std::shared_ptr<IPortionsLevel>>&& levels, std::vector<std::shared_ptr<IPortionsSelector>>&& selectors, const std::optional<ui32>& nodePortionsCountLimit);
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
