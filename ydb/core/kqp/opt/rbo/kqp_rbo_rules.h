#pragma once

#include "kqp_rbo.h"


namespace NKikimr {
namespace NKqp {

class TExtractJoinExpressionsRule : public IRule {
    public:
    TExtractJoinExpressionsRule() : IRule("Extract join expressions") {}

    virtual bool TestAndApply(std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) override;
};

class TPushMapRule : public TSimplifiedRule {
    public:
    TPushMapRule() : TSimplifiedRule("Push map operator") {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) override;
};

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

class TRenameStage : public ISinglePassStage {
    public:
    virtual void RunStage(TRuleBasedOptimizer* optimizer, TOpRoot & root, TExprContext& ctx) override;
};

}
}