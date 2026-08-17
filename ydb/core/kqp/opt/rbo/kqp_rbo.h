#pragma once

#include "kqp_rbo_context.h"
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;

enum ERuleProperties: ui32 {
    RequireParents         = 0x01,
    RequireOutputIUs       = 0x02,
    RequireTypes           = 0x04 | RequireOutputIUs,
    RequireMetadata        = 0x08 | RequireOutputIUs,
    RequireStatistics      = 0x10 | RequireTypes | RequireMetadata,
    RequireLiveness        = 0x20 | RequireOutputIUs,
    RequireNameConstraints = 0x40 | RequireOutputIUs,
    RequireAliases         = 0x80 | RequireOutputIUs
  };

/**
 * Interface for transformation rule:
 *
 * The rule may contain various metadata such as its name and a list of properties it requires to be computed
 * And it currently has a MatchAndApply method that checks if the rule can be applied and makes in-place modifications
 * to the plan. It can also modify the input operator, in this case the optimizer will replace the old version with the new
 * in the updated plan.
 */
class IRule {
  public:
    IRule(TString name) : RuleName(name) {}
    IRule(TString name, ui32 props, bool logRule = true) : RuleName(name), Props(props), LogRule(logRule) {}

    virtual bool QuickMatch(const TIntrusivePtr<IOperator>&) const {
        return true;
    }
    virtual bool MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) = 0;

    virtual ~IRule() = default;

    TString RuleName;
    ui32 Props{0x00};
    bool LogRule = false;
};

/**
 * A Simplified rule does not alter the original subplan that it matched, but instead returns a new
 * subplan that replaces the old one.
 */
class ISimplifiedRule : public IRule {
  public:
    ISimplifiedRule(TString name) : IRule(name) {}
    ISimplifiedRule(TString name, ui32 props, bool logRule = true) : IRule(name, props, logRule) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) = 0;

    virtual bool MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Stage Interface
 *
 * A Stage in a rule-based optimizer either applies a collection of rules, until there are no more matches
 * Or instead it runs a global stage.
 */
class IRBOStage : public NNonCopyable::TNonCopyable {
  public:
    IRBOStage(TString&& stageName) : StageName(std::move(stageName)) {}

    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) = 0;

    // If you return true here, then runtime will make sure that all the properties
    // you set in "Props" are up to date when your stage runs.

    // Some stages might want to control this manually instead. For example,
    // TRuleBasedStage recomputes properties lazily, only when a particular rule
    // actually expects them, so it opts out of this system and handles it internally.
    virtual bool NeedsInitialProps() const {
        return true;
    }

    virtual ~IRBOStage() = default;
    ui32 Props = 0x00;

    TString StageName;
};

/**
 * Rule based stage is just a collection of rules.
 */
class TRuleBasedStage : public IRBOStage {
  public:
    TRuleBasedStage(TString&& stageName, TVector<std::unique_ptr<IRule>>&& rules);
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;
    virtual bool NeedsInitialProps() const override {
        return false;
    }

    TVector<std::unique_ptr<IRule>> Rules;
};

/**
 * A rule based optimizer is a collection of rule-based and global stages.
 */
class TRuleBasedOptimizer : public NNonCopyable::TNonCopyable {
public:
    TRuleBasedOptimizer() = default;
    ~TRuleBasedOptimizer() = default;

    // This function applies RBO optimizations, translates given `root` to physical yql `callables`, applies lightweight (stage based) physical optimizations
    // and returns a root of the physical program.
    TExprNode::TPtr Optimize(TOpRoot& root, TRBOContext& rboCtx);

    // Adds a RBO stage to the RBO pipeline.
    void AddStage(std::unique_ptr<IRBOStage>&& stage) {
        Stages.push_back(std::move(stage));
    }

    TVector<std::unique_ptr<IRBOStage>> Stages;
};

/**
 * After the rule-based optimizer generates a final plan (logical plan with detailed physical properties)
 * we convert it into a final physical representation that directly correpsonds to the execution plan.
 */
TExprNode::TPtr ConvertToPhysical(TOpRoot& root, TRBOContext& ctx);
void ComputeRequiredProps(TOpRoot& root, ui32 props, TRBOContext& ctx, TString stageName);
void ComputePlanLiveness(TOpRoot& root);
const TInfoUnitSet& GetLiveIn(IOperator* op, ui32 childIndex);
const TInfoUnitSet& GetLiveOut(IOperator* op);
void ComputePlanAliases(TOpRoot& root);
const TPlanAliases::TCandidates* GetAliases(IOperator* op, const TInfoUnit& iu);

TString SerializeRBOExplainPlan(NJson::TJsonValue txPlan);
TString SerializeRBOAnalyzePlan(const TVector<const TString>& txPlans, const NKqpProto::TKqpStatsQuery& queryStats, const TString& poolId = "");

} // namespace NKqp
} // namespace NKikimr
