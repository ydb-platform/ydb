#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/object_factory/object_factory.h>

#include <memory>
#include <optional>

namespace arrow {
class Schema;
}

namespace NKikimr::NOlap {
class IStoragesManager;
}

namespace NKikimr::NOlap::NStorageOptimizer {

class IOptimizerPlanner;
class TOptimizerRuntimeSettings;

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
        YDB_READONLY_DEF(std::shared_ptr<TOptimizerRuntimeSettings>, RuntimeSettings);

    public:
        TBuildContext(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storages,
            const std::shared_ptr<arrow::Schema>& pkSchema, const std::shared_ptr<TOptimizerRuntimeSettings>& runtimeSettings = nullptr)
            : PathId(pathId)
            , Storages(storages)
            , PKSchema(pkSchema)
            , DefaultStrategy(EOptimizerStrategy::Default)
            , RuntimeSettings(runtimeSettings)
        {   //TODO configure me via DDL
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

    static std::shared_ptr<IOptimizerPlannerConstructor> BuildDefault(const TString& defaultCompactionName) {
        auto result = TFactory::MakeHolder(defaultCompactionName);
        AFL_VERIFY(!!result)("name", defaultCompactionName);
        return std::shared_ptr<IOptimizerPlannerConstructor>(result.Release());
    }

    static std::shared_ptr<IOptimizerPlannerConstructor> BuildDefault() {
        return BuildDefault(NKikimrConfig::TColumnShardConfig::default_instance().GetDefaultCompactionPreset());
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

    TConclusion<std::shared_ptr<IOptimizerPlanner>> BuildPlanner(const TBuildContext& context) const;

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
