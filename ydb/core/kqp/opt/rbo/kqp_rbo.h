#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;


class IRule {
    public:
    IRule(TString name) : RuleName(name) {}

    virtual bool TestAndApply(std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config,
        TPlanProps& props) = 0;

    virtual ~IRule() = default;

    TString RuleName;
};

class TSimplifiedRule : public IRule {
    public:
    TSimplifiedRule(TString name) : IRule(name) {}

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

struct TRuleBasedStage {
    TRuleBasedStage(TVector<std::shared_ptr<IRule>> rules, bool requiresRebuild) : Rules(rules), RequiresRebuild(requiresRebuild) {}
    TVector<std::shared_ptr<IRule>> Rules;
    bool RequiresRebuild;
};

class TRuleBasedOptimizer {
    public:
    TRuleBasedOptimizer(TVector<TRuleBasedStage> stages, 
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

    TVector<TRuleBasedStage> Stages;
    const TIntrusivePtr<TKqpOptimizeContext>& KqpCtx;
    TTypeAnnotationContext& TypeCtx;
    const TKikimrConfiguration::TPtr& Config;
    TAutoPtr<IGraphTransformer> TypeAnnTransformer;
    TAutoPtr<IGraphTransformer> PeepholeTransformer;
};

TExprNode::TPtr ConvertToPhysical(TOpRoot & root,  TExprContext& ctx, TTypeAnnotationContext& types, TAutoPtr<IGraphTransformer> typeAnnTransformer, TAutoPtr<IGraphTransformer> peepholeTransformer, TKikimrConfiguration::TPtr config);

}
}