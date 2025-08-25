#pragma once
#include "level/constructor.h"
#include "selector/constructor.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TOptimizerPlannerConstructor: public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "lc-buckets";
    }

private:
    std::vector<TLevelConstructorContainer> LevelConstructors;
    std::vector<TSelectorConstructorContainer> SelectorConstructors;

    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    virtual void DoSerializeToProto(TProto& proto) const override;

    virtual bool DoDeserializeFromProto(const TProto& proto) override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;
    virtual bool DoApplyToCurrentObject(IOptimizerPlanner& current) const override;

    virtual TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
