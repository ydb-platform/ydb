#pragma once
#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/reader/position.h>

#include <ydb/library/conclusion/result.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {
class TColumnEngineChanges;
class IStoragesManager;
class TGranuleMeta;
class TPortionInfo;
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
        TModificationGuard& AddPortion(const std::shared_ptr<TPortionInfo>& portion);
        TModificationGuard& RemovePortion(const std::shared_ptr<TPortionInfo>& portion);

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

    virtual NArrow::NMerger::TIntervalPositions GetBucketPositions() const = 0;
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

class IOptimizerPlannerConstructor {
public:
    class TBuildContext {
    private:
        YDB_READONLY(ui64, PathId, 0);
        YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, Storages);
        YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PKSchema);
    public:
        TBuildContext(const ui64 pathId, const std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema)
            : PathId(pathId)
            , Storages(storages)
            , PKSchema(pkSchema) {

        }
    };

    using TFactory = NObjectFactory::TObjectFactory<IOptimizerPlannerConstructor, TString>;
    using TProto = NKikimrSchemeOp::TCompactionPlannerConstructorContainer;
private:
    virtual TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const = 0;
    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual bool DoDeserializeFromProto(const TProto& proto) = 0;
    virtual bool DoIsEqualTo(const IOptimizerPlannerConstructor& item) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual bool DoApplyToCurrentObject(IOptimizerPlanner& current) const = 0;

public:

    static std::shared_ptr<IOptimizerPlannerConstructor> BuildDefault() {
        auto result = TFactory::MakeHolder("l-buckets");
        AFL_VERIFY(!!result);
        return std::shared_ptr<IOptimizerPlannerConstructor>(result.Release());
    }

    virtual ~IOptimizerPlannerConstructor() = default;

    bool ApplyToCurrentObject(const std::shared_ptr<IOptimizerPlanner>& current) const {
        if (!current) {
            return false;
        }
        return DoApplyToCurrentObject(*current);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        return DoDeserializeFromJson(jsonInfo);
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> BuildPlanner(const TBuildContext& context) const {
        return DoBuildPlanner(context);
    }

    virtual TString GetClassName() const = 0;
    void SerializeToProto(TProto& proto) const {
        DoSerializeToProto(proto);
    }

    bool IsEqualTo(const std::shared_ptr<IOptimizerPlannerConstructor>& item) const {
        AFL_VERIFY(!!item);
        if (GetClassName() != item->GetClassName()) {
            return false;
        }
        return DoIsEqualTo(*item);
    }

    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }

};

class TOptimizerPlannerConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IOptimizerPlannerConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IOptimizerPlannerConstructor>;
public:
    using TBase::TBase;

    static TConclusion<TOptimizerPlannerConstructorContainer> BuildFromProto(const IOptimizerPlannerConstructor::TProto& data) {
        TOptimizerPlannerConstructorContainer result;
        if (!result.DeserializeFromProto(data)) {
            return TConclusionStatus::Fail("cannot parse interface from proto: " + data.DebugString());
        }
        return result;
    }

};

} // namespace NKikimr::NOlap
