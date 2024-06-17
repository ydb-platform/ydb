#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets {

class TOptimizerPlannerConstructor: public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "l-buckets";
    }
private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator = TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    virtual void DoSerializeToProto(TProto& proto) const override;

    virtual bool DoDeserializeFromProto(const TProto& proto) override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& /*jsonInfo*/) override {
        return TConclusionStatus::Success();
    }
    virtual bool DoApplyToCurrentObject(IOptimizerPlanner& current) const override;

    virtual TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override;
    virtual bool DoIsEqualTo(const IOptimizerPlannerConstructor& item) const override;
public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

};

} // namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets
