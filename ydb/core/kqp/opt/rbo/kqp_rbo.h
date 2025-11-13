#pragma once

#include "kqp_rbo_context.h"
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;

enum ERuleProperties: ui32 {
    RequireParents = 0x01,
    RequireTypes = 0x02,
    RequireTableMeta = 0x04,
    RequireCosts = 0x08,
};

/**
 * Interface for transformation rule:
 *
 * The rule may contain various metadata such as its name and a list of properties it requires to be computed
 * And it currently has a TestAndApply method that checks if the rule can be applied and makes in-place modifications
 * to the plan.
 */
class IRule {
  public:
    IRule(TString name) : RuleName(name) {}
    IRule(TString name, ui32 props) : RuleName(name), Props(props) {}

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) = 0;

    virtual ~IRule() = default;

    TString RuleName;
    ui32 Props;
};

/**
 * A Simplified rule does not alter the original subplan that it matched, but instead returns a new
 * subplan that replaces the old one
 */
class ISimplifiedRule : public IRule {
  public:
    ISimplifiedRule(TString name) : IRule(name) {}
    ISimplifiedRule(TString name, ui32 props) : IRule(name, props) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) = 0;

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Stage Interface
 *
 * A Stage in a rule-based optimizer either applies a collection of rules, until there are no more matches
 * Or instead it runs a global stage
 */
class IRBOStage {
  public:
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) = 0;
    virtual ~IRBOStage() = default;
    ui32 Props;
};

/**
 * Rule based stage is just a collection of rules
 */
class TRuleBasedStage : public IRBOStage {
  public:
    TRuleBasedStage(TVector<std::shared_ptr<IRule>> rules);
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;

    TVector<std::shared_ptr<IRule>> Rules;
};

/**
 * A rule based optimizer is a collection of rule-based and global stages
 */
class TRuleBasedOptimizer {
  public:
    TRuleBasedOptimizer(TVector<std::shared_ptr<IRBOStage>> stages, 
                      const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                      TTypeAnnotationContext &typeCtx, 
                      TAutoPtr<IGraphTransformer> rboTypeAnnTransformer, 
                      TAutoPtr<IGraphTransformer> typeAnnTransformer, 
                      TAutoPtr<IGraphTransformer> peephole,
                      const NMiniKQL::IFunctionRegistry& funcRegistry) : 
        Stages(stages), 
        KqpCtx(*kqpCtx), 
        TypeCtx(typeCtx), 
        RBOTypeAnnTransformer(rboTypeAnnTransformer), 
        TypeAnnTransformer(typeAnnTransformer),
        PeepholeTransformer(peephole),
        FuncRegistry(funcRegistry) {}

    TExprNode::TPtr Optimize(TOpRoot &root, TExprContext &ctx);

    TVector<std::shared_ptr<IRBOStage>> Stages;
    const TKqpOptimizeContext &KqpCtx;
    TTypeAnnotationContext &TypeCtx;
    TAutoPtr<IGraphTransformer> RBOTypeAnnTransformer;
    TAutoPtr<IGraphTransformer> TypeAnnTransformer;
    TAutoPtr<IGraphTransformer> PeepholeTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
};

/**
 * After the rule-based optimizer generates a final plan (logical plan with detailed physical properties)
 * we convert it into a final physical representation that directly correpsonds to the exection plan
 */
TExprNode::TPtr ConvertToPhysical(TOpRoot &root, TRBOContext& ctx, TAutoPtr<IGraphTransformer> typeAnnTransformer, 
                                  TAutoPtr<IGraphTransformer> peepholeTransformer);

} // namespace NKqp
} // namespace NKikimr