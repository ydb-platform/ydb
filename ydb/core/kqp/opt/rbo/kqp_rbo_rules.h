#pragma once

#include "kqp_rbo.h"


namespace NKikimr {
namespace NKqp {

class TPushFilterRule : public TSimplifiedRule {
    public:
    TPushFilterRule() : TSimplifiedRule("Push filter") {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) override;
};

class TAssignStagesRule : public IRule {
    public:
    TAssignStagesRule() : IRule("Assign stages") {}

    virtual bool TestAndApply(std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) override;
};

extern TRuleBasedStage RuleStage1;
extern TRuleBasedStage RuleStage2;

}
}