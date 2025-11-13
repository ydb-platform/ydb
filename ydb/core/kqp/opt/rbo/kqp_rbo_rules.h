#pragma once

#include "kqp_rbo.h"
#include "kqp_rbo_context.h"

/**
 * Collection of transformation rules
 */

namespace NKikimr {
namespace NKqp {

/**
 * Analyzes filter expressions, finds potential join conditions and if they are in the form of
 * expressions (i.e. not just equalities of columns) - creates expressions to generate new columns,
 * rewrites the filter to use these columns and create a map operator below filter that generates these columns
 */
class TExtractJoinExpressionsRule : public IRule {
  public:
    TExtractJoinExpressionsRule() : IRule("Extract join expressions") { }

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Rewrites scalar subplans into cross join plans
 */
class TInlineScalarSubplanRule : public IRule {
  public:
    TInlineScalarSubplanRule() : IRule("Inline scalar subplan") {
      Props.RequireTypes = true;
    }

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down a non-projecting map operator
 * Currently only pushes below joins that are immediately below
 */
class TPushMapRule : public ISimplifiedRule {
  public:
    TPushMapRule() : ISimplifiedRule("Push map operator") {
      Props.RequireParents = true;
    }

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down filter through joins, adding join conditions to the join operator and potentially
 * converting left join into inner join
 */
class TPushFilterRule : public ISimplifiedRule {
  public:
    TPushFilterRule() : ISimplifiedRule("Push filter") {
      Props.RequireParents = true;
    }

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Generate a stage graph for the plan and assign stage ids to operators
 */
class TAssignStagesRule : public IRule {
  public:
    TAssignStagesRule() : IRule("Assign stages") {
      Props.RequireParents = true;
    }

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

extern TRuleBasedStage RuleStage1;
extern TRuleBasedStage RuleStage2;

/**
 * Separate global stage to remove extra renames and project out unneeded columns
 */
class TRenameStage : public IRBOStage {
  public:
    TRenameStage();
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;
};

/**
 * Separate global constant folding stage
 */
class TConstantFoldingStage : public IRBOStage {
  public:
    TConstantFoldingStage();
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;
};

} // namespace NKqp
} // namespace NKikimr