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
    RequireMetadata = 0x04,
    RequireStatistics = 0x08
  };

/**
 * Interface for transformation rule:
 *
 * The rule may contain various metadata such as its name and a list of properties it requires to be computed
 * And it currently has a MatchAndApply method that checks if the rule can be applied and makes in-place modifications
 * to the plan.
 */
class IRule {
  public:
    IRule(TString name) : RuleName(name) {}
    IRule(TString name, ui32 props, bool logRule = false) : RuleName(name), Props(props), LogRule(logRule) {}

    virtual bool MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) = 0;

    virtual ~IRule() = default;

    TString RuleName;
    ui32 Props{0x00};
    bool LogRule = false;
};

/**
 * A Simplified rule does not alter the original subplan that it matched, but instead returns a new
 * subplan that replaces the old one
 */
class ISimplifiedRule : public IRule {
  public:
    ISimplifiedRule(TString name) : IRule(name) {}
    ISimplifiedRule(TString name, ui32 props, bool logRule = false) : IRule(name, props, logRule) {}

    virtual std::shared_ptr<IOperator> SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) = 0;

    virtual bool MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Stage Interface
 *
 * A Stage in a rule-based optimizer either applies a collection of rules, until there are no more matches
 * Or instead it runs a global stage
 */
class IRBOStage {
  public:
    IRBOStage(TString&& stageName) : StageName(std::move(stageName)) {}
    
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) = 0;
    virtual ~IRBOStage() = default;
    ui32 Props = 0x00;

    TString StageName;
};

/**
 * Rule based stage is just a collection of rules
 */
class TRuleBasedStage : public IRBOStage {
  public:
    TRuleBasedStage(TString&& stageName, TVector<std::shared_ptr<IRule>>&& rules);
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;

    TVector<std::shared_ptr<IRule>> Rules;
};

/**
 * A rule based optimizer is a collection of rule-based and global stages
 */
class TRuleBasedOptimizer {
public:
    TRuleBasedOptimizer() = default;
    ~TRuleBasedOptimizer() = default;

    // This function applies RBO optimizations, translates given `root` to physical yql `callables`, applies lightweight (stage based) physical optimizations
    // and returns a root of the physical program.
    TExprNode::TPtr Optimize(TOpRoot& root, TRBOContext& rboCtx);

    // Adds a RBO stage to the RBO pipeline.
    void AddStage(std::shared_ptr<IRBOStage>&& stage) {
        Stages.push_back(std::move(stage));
    }

    TVector<std::shared_ptr<IRBOStage>> Stages;
};

/**
 * After the rule-based optimizer generates a final plan (logical plan with detailed physical properties)
 * we convert it into a final physical representation that directly correpsonds to the exection plan
 */
TExprNode::TPtr ConvertToPhysical(TOpRoot& root, TRBOContext& ctx);

} // namespace NKqp
} // namespace NKikimr