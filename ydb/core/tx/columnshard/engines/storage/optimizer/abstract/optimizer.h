#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <library/cpp/object_factory/object_factory.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
class TColumnEngineChanges;
namespace NDataLocks {
class TManager;
}
}

namespace NKikimr::NOlap::NStorageOptimizer {

class TOptimizationPriority {
private:
    YDB_READONLY(i64, Level, 0);
    YDB_READONLY(i64, InternalLevelWeight, 0);
    TOptimizationPriority(const i64 level, const i64 levelWeight)
        : Level(level)
        , InternalLevelWeight(levelWeight) {

    }

public:
    bool operator<(const TOptimizationPriority& item) const {
        return std::tie(Level, InternalLevelWeight) < std::tie(item.Level, item.InternalLevelWeight);
    }

    bool IsZero() const {
        return !Level && !InternalLevelWeight;
    }

    TString DebugString() const {
        return TStringBuilder() << "(" << Level << "," << InternalLevelWeight << ")";
    }

    static TOptimizationPriority Critical(const i64 weight) {
        return TOptimizationPriority(10, weight);
    }

    static TOptimizationPriority Optimization(const i64 weight) {
        return TOptimizationPriority(0, weight);
    }

    static TOptimizationPriority Zero() {
        return TOptimizationPriority(0, 0);
    }

};

class TTaskDescription {
private:
    YDB_READONLY(ui64, TaskId, 0);
    YDB_ACCESSOR_DEF(TString, Start);
    YDB_ACCESSOR_DEF(TString, Finish);
    YDB_ACCESSOR_DEF(TString, Details);
    YDB_ACCESSOR_DEF(ui64, WeightCategory);
    YDB_ACCESSOR_DEF(i64, Weight);
public:
    TTaskDescription(const ui64 taskId)
        : TaskId(taskId)
    {

    }

    bool operator<(const TTaskDescription& item) const {
        return TaskId < item.TaskId;
    }
};

class IOptimizerPlanner {
private:
    const ui64 PathId;
    YDB_READONLY(TInstant, ActualizationInstant, TInstant::Zero());
protected:
    virtual void DoModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const = 0;
    virtual TOptimizationPriority DoGetUsefulMetric() const = 0;
    virtual void DoActualize(const TInstant currentInstant) = 0;
    virtual TString DoDebugString() const {
        return "";
    }
    virtual NJson::TJsonValue DoSerializeToJsonVisual() const {
        return NJson::JSON_NULL;
    }
    virtual bool DoIsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const = 0;
    virtual std::vector<TTaskDescription> DoGetTasksDescription() const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<IOptimizerPlanner, TString>;
    IOptimizerPlanner(const ui64 pathId)
        : PathId(pathId)
    {

    }

    std::vector<TTaskDescription> GetTasksDescription() const {
        return DoGetTasksDescription();
    }

    class TModificationGuard: TNonCopyable {
    private:
        IOptimizerPlanner& Owner;
        THashMap<ui64, std::shared_ptr<TPortionInfo>> AddPortions;
        THashMap<ui64, std::shared_ptr<TPortionInfo>> RemovePortions;
    public:
        TModificationGuard& AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
            AFL_VERIFY(AddPortions.emplace(portion->GetPortionId(), portion).second);
            return*this;
        }

        TModificationGuard& RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
            AFL_VERIFY(RemovePortions.emplace(portion->GetPortionId(), portion).second);
            return*this;
        }

        TModificationGuard(IOptimizerPlanner& owner)
            : Owner(owner)
        {
        }
        ~TModificationGuard() {
            Owner.ModifyPortions(AddPortions, RemovePortions);
        }
    };

    TModificationGuard StartModificationGuard() {
        return TModificationGuard(*this);
    }

    virtual ~IOptimizerPlanner() = default;
    TString DebugString() const {
        return DoDebugString();
    }

    virtual std::vector<NArrow::NMerger::TSortableBatchPosition> GetBucketPositions() const = 0;
    bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
        return DoIsLocked(dataLocksManager);
    }

    NJson::TJsonValue SerializeToJsonVisual() const {
        return DoSerializeToJsonVisual();
    }

    void ModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) {
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("path_id", PathId));
        DoModifyPortions(add, remove);
    }

    std::shared_ptr<TColumnEngineChanges> GetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const;
    TOptimizationPriority GetUsefulMetric() const {
        return DoGetUsefulMetric();
    }
    void Actualize(const TInstant currentInstant) {
        ActualizationInstant = currentInstant;
        return DoActualize(currentInstant);
    }
};

} // namespace NKikimr::NOlap
