#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/abstract.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class ILevelConstructor {
private:
    virtual std::shared_ptr<IPortionsLevel> DoBuildLevel(
        const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel, const TLevelCounters& counters) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const = 0;
    virtual bool IsEqualToSameClass(const ILevelConstructor& item) const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<ILevelConstructor, TString>;
    using TProto = NKikimrSchemeOp::TCompactionLevelConstructorContainer;

    virtual ~ILevelConstructor() = default;

    bool IsEqualTo(const ILevelConstructor& item) const {
        if (GetClassName() != item.GetClassName()) {
            return false;
        }
        return IsEqualToSameClass(item);
    }

    std::shared_ptr<IPortionsLevel> BuildLevel(
        const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel, const TLevelCounters& counters) const {
        return DoBuildLevel(nextLevel, indexLevel, counters);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& json) {
        return DoDeserializeFromJson(json);
    }

    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }
    void SerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const {
        return DoSerializeToProto(proto);
    }
    virtual TString GetClassName() const = 0;
};

class TLevelConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<ILevelConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ILevelConstructor>;

public:
    using TBase::TBase;
};

class TOptimizerPlannerConstructor: public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "lc-buckets";
    }

private:
    std::vector<TLevelConstructorContainer> Levels;

    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    virtual void DoSerializeToProto(TProto& proto) const override;

    virtual bool DoDeserializeFromProto(const TProto& proto) override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;
    virtual bool DoApplyToCurrentObject(IOptimizerPlanner& current) const override;

    virtual TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override;
    virtual bool DoIsEqualTo(const IOptimizerPlannerConstructor& item) const override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
