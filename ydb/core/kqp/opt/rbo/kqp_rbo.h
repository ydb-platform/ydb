#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;


class IRule {
    public:
    IRule(TString name, bool parentRecompute = true) : RuleName(name), RequiresParentRecompute(parentRecompute) {}

    virtual bool TestAndApply(std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) = 0;

    virtual ~IRule() = default;

    TString RuleName;
    bool RequiresParentRecompute = true;
};

class TSimplifiedRule : public IRule {
    public:
    TSimplifiedRule(TString name, bool parentRecompute = true) : IRule(name, parentRecompute) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) = 0;

    virtual bool TestAndApply(std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) override;
};

class TRuleBasedOptimizer;

class IRBOStage {
    public:
    virtual void RunStage(TRuleBasedOptimizer* optimizer, TOpRoot & root, TExprContext& ctx) = 0;
    virtual ~IRBOStage() = default;
};

class TRuleBasedStage : public IRBOStage {
    public:
    TRuleBasedStage(TVector<std::shared_ptr<IRule>> rules, bool requiresRebuild) : Rules(rules), RequiresRebuild(requiresRebuild) {}
    virtual void RunStage(TRuleBasedOptimizer* optimizer, TOpRoot & root, TExprContext& ctx) override;

    TVector<std::shared_ptr<IRule>> Rules;
    bool RequiresRebuild;
};

class ISinglePassStage : public IRBOStage {
    public:
    virtual void RunStage(TRuleBasedOptimizer* optimizer, TOpRoot & root, TExprContext& ctx) override = 0;
};

class TRuleBasedOptimizer {
    friend class IRBOStage;
    
    public:
    TRuleBasedOptimizer(TVector<std::shared_ptr<IRBOStage>> stages, 
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TAutoPtr<IGraphTransformer> typeAnnTransformer,
        TAutoPtr<IGraphTransformer> peephole) : Stages(stages),
        KqpCtx(kqpCtx),
        TypeCtx(typeCtx),
        Config(config),
        TypeAnnTransformer(typeAnnTransformer),
        PeepholeTransformer(peephole) {}
    
    TExprNode::TPtr Optimize(TOpRoot & root,  TExprContext& ctx);

    TVector<std::shared_ptr<IRBOStage>> Stages;
    const TIntrusivePtr<TKqpOptimizeContext>& KqpCtx;
    TTypeAnnotationContext& TypeCtx;
    const TKikimrConfiguration::TPtr& Config;
    TAutoPtr<IGraphTransformer> TypeAnnTransformer;
    TAutoPtr<IGraphTransformer> PeepholeTransformer;
};

TExprNode::TPtr ConvertToPhysical(TOpRoot & root,  TExprContext& ctx, TTypeAnnotationContext& types, TAutoPtr<IGraphTransformer> typeAnnTransformer, TAutoPtr<IGraphTransformer> peepholeTransformer, TKikimrConfiguration::TPtr config);

}
}