#pragma once
#include "counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/portion.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/object_factory/object_factory.h>

#include <utility>

namespace NKikimr::NOlap {
class TColumnEngineChanges;
class IStoragesManager;
class TGranuleMeta;
class TPortionInfo;
class TPortionAccessorConstructor;
namespace NDataLocks {
class TManager;
}
}   // namespace NKikimr::NOlap

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
    void Mul(const ui32 kff) {
        InternalLevelWeight *= kff;
    }

    ui64 GetGeneralPriority() const {
        return ((ui64)Level << 56) + InternalLevelWeight;
    }

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
        : TaskId(taskId) {
    }

    bool operator<(const TTaskDescription& item) const {
        return TaskId < item.TaskId;
    }
};

using TPortionInfoForCompaction = NPortion::TPortionInfoForCompaction;

class IOptimizerPlanner {
private:
    friend class IOptimizerPlannerConstructor;
    const TInternalPathId PathId;
    YDB_READONLY(TInstant, ActualizationInstant, TInstant::Zero());

    virtual bool DoIsOverloaded() const {
        return false;
    }
    const std::optional<ui64> NodePortionsCountLimit{};
    double WeightKff = 1;
    static inline TAtomicCounter NodePortionsCounter = 0;
    static inline std::atomic<ui64> DynamicPortionsCountLimit = 1000000;
    TPositiveControlInteger LocalPortionsCount;
    std::shared_ptr<TCounters> Counters = std::make_shared<TCounters>();

protected:
    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) = 0;
    virtual std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const = 0;
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
    virtual TConclusionStatus DoCheckWriteData() const {
        return TConclusionStatus::Success();
    }

public:
    static ui64 GetNodePortionsConsumption() {
        return NodePortionsCounter.Val() * NKikimr::NOlap::TGlobalLimits::AveragePortionSizeLimit;
    }

    static void SetPortionsCacheLimit(const ui64 portionsCacheLimitBytes) {
        DynamicPortionsCountLimit.store(portionsCacheLimitBytes / NKikimr::NOlap::TGlobalLimits::AveragePortionSizeLimit);
    }

    static ui64 GetPortionsCacheLimit() {
        return DynamicPortionsCountLimit.load() * NKikimr::NOlap::TGlobalLimits::AveragePortionSizeLimit;
    }

    virtual ui32 GetAppropriateLevel(const ui32 baseLevel, const TPortionInfoForCompaction& /*info*/) const {
        return baseLevel;
    }

    IOptimizerPlanner(const TInternalPathId pathId, const std::optional<ui64>& nodePortionsCountLimit)
        : PathId(pathId)
        , NodePortionsCountLimit(nodePortionsCountLimit) {
        Counters->NodePortionsCountLimit->Set(NodePortionsCountLimit ? *NodePortionsCountLimit : DynamicPortionsCountLimit.load());
    }

    bool IsOverloaded(const NMonitoring::TDynamicCounters::TCounterPtr& badPortions) const {
        if (!AppDataVerified().FeatureFlags.GetEnableCompactionOverloadDetection()) {
            return false;
        }

        if (std::cmp_less_equal(GetBadPortionsLimit(), badPortions->Val())) {
           AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_WRITE)
                   ("error", "overload: bad portions")("value", badPortions->Val())("limit", GetBadPortionsLimit());
            return true;
        }

        if (std::cmp_less_equal(GetNodePortionsCountLimit(), NodePortionsCounter.Val())) {
           AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_WRITE)
                   ("error", "overload: node portions count limit")("value", NodePortionsCounter.Val())("limit", GetNodePortionsCountLimit());
            return true;
        }

        return DoIsOverloaded();
    }

    ui64 GetBadPortionsLimit() const {
        if (AppDataVerified().ColumnShardConfig.GetBadPortionsLimit()) {
            return AppDataVerified().ColumnShardConfig.GetBadPortionsLimit();
        }
        return 2 * GetNodePortionsCountLimit();
    }

    ui64 GetNodePortionsCountLimit() const {
        return NodePortionsCountLimit.value_or(DynamicPortionsCountLimit.load());
    }

    bool IsHighPriority() const {
        if (!AppDataVerified().FeatureFlags.GetEnableCompactionOverloadDetection()) {
            return false;
        }
        if (NodePortionsCountLimit) {
            if (std::cmp_less_equal(std::max(static_cast<ui64>(0.7 * *NodePortionsCountLimit), ui64(1)), NodePortionsCounter.Val())) {
                return true;
            }
        } else if (std::cmp_less_equal(std::max(static_cast<ui64>(0.7 * DynamicPortionsCountLimit.load()), ui64(1)), NodePortionsCounter.Val())) {
            return true;
        }
        return DoIsOverloaded();
    }

    TConclusionStatus CheckWriteData() const {
        return DoCheckWriteData();
    }

    std::vector<TTaskDescription> GetTasksDescription() const {
        return DoGetTasksDescription();
    }

    class TModificationGuard: TNonCopyable {
    private:
        IOptimizerPlanner& Owner;
        std::vector<std::shared_ptr<TPortionInfo>> AddPortions;
        std::vector<std::shared_ptr<TPortionInfo>> RemovePortions;

    public:
        TModificationGuard& AddPortion(const std::shared_ptr<TPortionInfo>& portion);
        TModificationGuard& RemovePortion(const std::shared_ptr<TPortionInfo>& portion);

        TModificationGuard(IOptimizerPlanner& owner)
            : Owner(owner) {
        }
        ~TModificationGuard() {
            Owner.ModifyPortions(AddPortions, RemovePortions);
        }
    };

    TModificationGuard StartModificationGuard() {
        return TModificationGuard(*this);
    }

    virtual ~IOptimizerPlanner() {
        NodePortionsCounter.Sub(LocalPortionsCount.Val());
        Counters->NodePortionsCount->Set(NodePortionsCounter.Val());
    }
    TString DebugString() const {
        return DoDebugString();
    }

    virtual NArrow::NMerger::TIntervalPositions GetBucketPositions() const = 0;

    NJson::TJsonValue SerializeToJsonVisual() const {
        return DoSerializeToJsonVisual();
    }

    void ModifyPortions(const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) {
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("path_id", PathId));
        LocalPortionsCount.Add(add.size());
        LocalPortionsCount.Sub(remove.size());
        NodePortionsCounter.Add(add.size());
        NodePortionsCounter.Sub(remove.size());
        Counters->NodePortionsCount->Set(NodePortionsCounter.Val());
        DoModifyPortions(add, remove);
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> GetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const;
    TOptimizationPriority GetUsefulMetric() const {
        auto result = DoGetUsefulMetric();
        result.Mul(WeightKff);
        return result;
    }
    void Actualize(const TInstant currentInstant) {
        ActualizationInstant = currentInstant;
        return DoActualize(currentInstant);
    }
};

class IOptimizerPlannerConstructor {
public:
    enum class EOptimizerStrategy {
        Default,   //use One Layer levels to avoid portion intersections
        Logs,   // use Zero Levels only for performance
        LogsInStore
    };
    class TBuildContext {
    private:
        YDB_READONLY_DEF(TInternalPathId, PathId);
        YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, Storages);
        YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PKSchema);
        YDB_READONLY_DEF(EOptimizerStrategy, DefaultStrategy);

    public:
        TBuildContext(
            const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema)
            : PathId(pathId)
            , Storages(storages)
            , PKSchema(pkSchema)
            , DefaultStrategy(EOptimizerStrategy::Default) {   //TODO configure me via DDL
        }
    };

    using TFactory = NObjectFactory::TObjectFactory<IOptimizerPlannerConstructor, TString>;
    using TProto = NKikimrSchemeOp::TCompactionPlannerConstructorContainer;

private:
    std::optional<ui64> NodePortionsCountLimit{};
    double WeightKff = 1.0;

    virtual TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const = 0;
    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual bool DoDeserializeFromProto(const TProto& proto) = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual bool DoApplyToCurrentObject(IOptimizerPlanner& current) const = 0;

public:
    std::optional<ui64> GetNodePortionsCountLimit() const {
        return NodePortionsCountLimit;
    }

    static std::shared_ptr<IOptimizerPlannerConstructor> BuildDefault() {
        auto default_compaction = HasAppData() && AppDataVerified().ColumnShardConfig.HasDefaultCompaction()
            ? AppDataVerified().ColumnShardConfig.GetDefaultCompaction() : "tiling";
        auto result = TFactory::MakeHolder(default_compaction);
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
        if (jsonInfo.Has("node_portions_count_limit")) {
            const auto& jsonValue = jsonInfo["node_portions_count_limit"];
            if (!jsonValue.IsUInteger()) {
                return TConclusionStatus::Fail("incorrect node_portions_count_limit value have to be unsigned int");
            }
            NodePortionsCountLimit = jsonValue.GetUInteger();
        }
        if (jsonInfo.Has("weight_kff")) {
            const auto& jsonValue = jsonInfo["weight_kff"];
            if (!jsonValue.IsDouble()) {
                return TConclusionStatus::Fail("incorrect weight_kff value have to be double");
            }
            WeightKff = jsonValue.GetDouble();
        }
        return DoDeserializeFromJson(jsonInfo);
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> BuildPlanner(const TBuildContext& context) const {
        auto result = DoBuildPlanner(context);
        if (result.IsFail()) {
            return result;
        }
        (*result)->WeightKff = WeightKff;
        return result;
    }

    virtual TString GetClassName() const = 0;
    void SerializeToProto(TProto& proto) const {
        if (NodePortionsCountLimit) {
            proto.SetNodePortionsCountLimit(*NodePortionsCountLimit);
        }
        proto.SetWeightKff(WeightKff);
        DoSerializeToProto(proto);
    }

    bool IsEqualTo(const std::shared_ptr<IOptimizerPlannerConstructor>& item) const {
        AFL_VERIFY(!!item);
        if (GetClassName() != item->GetClassName()) {
            return false;
        }
        TProto selfProto;
        TProto itemProto;
        SerializeToProto(selfProto);
        item->SerializeToProto(itemProto);
        return selfProto.SerializeAsString() == itemProto.SerializeAsString();
    }

    bool DeserializeFromProto(const TProto& proto) {
        if (proto.HasNodePortionsCountLimit()) {
            NodePortionsCountLimit = proto.GetNodePortionsCountLimit();
        }
        if (proto.HasWeightKff()) {
            WeightKff = proto.GetWeightKff();
        }
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

}   // namespace NKikimr::NOlap::NStorageOptimizer
