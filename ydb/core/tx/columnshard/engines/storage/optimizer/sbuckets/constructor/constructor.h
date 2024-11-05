#pragma once
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract/logic.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TOptimizerPlannerConstructor: public IOptimizerPlannerConstructor {
private:
    YDB_READONLY_DEF(TString, LogicName);
    YDB_READONLY(TDuration, FreshnessCheckDuration, NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration());

public:
    static TString GetClassNameStatic() {
        return "s-buckets";
    }
private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator = TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    std::shared_ptr<IOptimizationLogic> BuildLogic() const;
    virtual bool DoApplyToCurrentObject(IOptimizerPlanner & current) const override;

    virtual void DoSerializeToProto(TProto& proto) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    virtual bool DoDeserializeFromProto(const TProto& proto) override;

    virtual TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override;
    virtual bool DoIsEqualTo(const IOptimizerPlannerConstructor& item) const override;
public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

};

} // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
