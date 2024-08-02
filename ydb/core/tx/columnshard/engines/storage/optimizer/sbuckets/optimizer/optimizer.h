#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/counters/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/index/index.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TOptimizerPlanner: public IOptimizerPlanner {
private:
    using TBase = IOptimizerPlanner;
    std::shared_ptr<TCounters> Counters;
    const std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
    TPortionBuckets Buckets;
    const std::shared_ptr<IStoragesManager> StoragesManager;

    virtual std::vector<TTaskDescription> DoGetTasksDescription() const override {
        return Buckets.GetTasksDescription();
    }

protected:
    virtual bool DoIsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        return Buckets.IsLocked(dataLocksManager);
    }

    virtual void DoModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) override {
        for (auto&& [_, i] : remove) {
            if (i->GetMeta().GetTierName() != IStoragesManager::DefaultStorageId && i->GetMeta().GetTierName() != "") {
                continue;
            }
            Buckets.RemovePortion(i);
            if (Buckets.IsEmpty()) {
                Counters->OptimizersCount->Sub(1);
            }
        }
        for (auto&& [_, i] : add) {
            if (i->GetMeta().GetTierName() != IStoragesManager::DefaultStorageId && i->GetMeta().GetTierName() != "") {
                continue;
            }
            if (Buckets.IsEmpty()) {
                Counters->OptimizersCount->Add(1);
            }
            Buckets.AddPortion(i);
        }
    }
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        return Buckets.BuildOptimizationTask(granule, locksManager);
    }
    virtual void DoActualize(const TInstant currentInstant) override {
        Buckets.Actualize(currentInstant);
    }

    virtual TOptimizationPriority DoGetUsefulMetric() const override {
        if (Buckets.GetWeight()) {
            return TOptimizationPriority::Critical(Buckets.GetWeight());
        } else {
            return TOptimizationPriority::Zero();
        }
    }
    virtual TString DoDebugString() const override {
        return Buckets.DebugString();
    }
    virtual NJson::TJsonValue DoSerializeToJsonVisual() const override {
        return Buckets.SerializeToJson();
    }

public:
    virtual NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        return Buckets.GetBucketPositions();
    }

    void ResetLogic(const std::shared_ptr<IOptimizationLogic>& logic) {
        Buckets.ResetLogic(logic);
    }

    TOptimizerPlanner(const ui64 pathId, const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const std::shared_ptr<IOptimizationLogic>& logic);
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
