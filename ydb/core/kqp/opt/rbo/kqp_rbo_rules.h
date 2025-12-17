#pragma once

#include "kqp_rbo.h"
#include "kqp_rbo_context.h"

/**
 * Collection of transformation rules
 */

namespace NKikimr {
namespace NKqp {

/**
 * Removed identity map
 */

 class TRemoveIdenityMapRule : public ISimplifiedRule {
  public:
    TRemoveIdenityMapRule() : ISimplifiedRule("Remove identity map", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Analyzes filter expressions, finds potential join conditions and if they are in the form of
 * expressions (i.e. not just equalities of columns) - creates expressions to generate new columns,
 * rewrites the filter to use these columns and create a map operator below filter that generates these columns
 */
class TExtractJoinExpressionsRule : public IRule {
  public:
    TExtractJoinExpressionsRule() : IRule("Extract join expressions") {}

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Rewrites scalar subplans into cross join plans
 */
class TInlineScalarSubplanRule : public IRule {
  public:
    TInlineScalarSubplanRule() : IRule("Inline scalar subplan", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down a non-projecting map operator
 * Currently only pushes below joins that are immediately below
 */
class TPushMapRule : public ISimplifiedRule {
  public:
    TPushMapRule() : ISimplifiedRule("Push map operator", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push limit into sort operator
 */
class TPushLimitIntoSortRule : public ISimplifiedRule {
  public:
    TPushLimitIntoSortRule() : ISimplifiedRule("Push limit into sort operator", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down filter through joins, adding join conditions to the join operator and potentially
 * converting left join into inner join
 */
class TPushFilterRule : public ISimplifiedRule {
  public:
    TPushFilterRule() : ISimplifiedRule("Push filter", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down filter to olap read.
 */
class TPushOlapFilterRule : public ISimplifiedRule {
  public:
      TPushOlapFilterRule() : ISimplifiedRule("Push olap filter", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

      virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Create inital CBO Tree
 */
class TBuildInitialCBOTreeRule : public ISimplifiedRule {
  public:
    TBuildInitialCBOTreeRule() : ISimplifiedRule("Building initial CBO tree", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Expand CBO Tree
 */
class TExpandCBOTreeRule : public ISimplifiedRule {
  public:
    TExpandCBOTreeRule() : ISimplifiedRule("Expand CBO tree", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Optimize CBO Tree
 */
class TOptimizeCBOTreeRule : public ISimplifiedRule {
  public:
    TOptimizeCBOTreeRule() : ISimplifiedRule("Optimize CBO tree", ERuleProperties::RequireParents | ERuleProperties::RequireStatistics) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Inline unoptimized CBO tree back into the plan
 */
class TInlineCBOTreeRule : public ISimplifiedRule {
  public:
    TInlineCBOTreeRule() : ISimplifiedRule("Inline unoptimized CBO tree", ERuleProperties::RequireParents) {}

    virtual std::shared_ptr<IOperator> SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Generate a stage graph for the plan and assign stage ids to operators
 */
class TAssignStagesRule : public IRule {
  public:
    TAssignStagesRule() : IRule("Assign stages", ERuleProperties::RequireParents) {}

    virtual bool TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

extern TRuleBasedStage RuleStage1;
extern TRuleBasedStage RuleStage2;
extern TRuleBasedStage RuleStage3;
extern TRuleBasedStage RuleStage4;
extern TRuleBasedStage RuleStage5;
extern TRuleBasedStage RuleStage6;
extern TRuleBasedStage RuleStage7;

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

/**
 * Prune unnecessary columns stage
 */
class TPruneColumnsStage : public IRBOStage {
  public:
    TPruneColumnsStage();
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;
};

} // namespace NKqp
} // namespace NKikimr