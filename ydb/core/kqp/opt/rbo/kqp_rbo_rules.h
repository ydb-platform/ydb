#pragma once

#include "kqp_rbo.h"
#include "kqp_rbo_context.h"

/**
 * Collection of transformation rules
 */

namespace NKikimr {
namespace NKqp {

/**
 * Remove identity map
 */

 class TRemoveIdenityMapRule : public ISimplifiedRule {
  public:
    TRemoveIdenityMapRule() : ISimplifiedRule("Remove identity map", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Analyzes filter expressions, finds potential join conditions and if they are in the form of
 * expressions (i.e. not just equalities of columns) - creates expressions to generate new columns,
 * rewrites the filter to use these columns and create a map operator below filter that generates these columns
 */
class TExtractJoinExpressionsRule : public IRule {
  public:
    TExtractJoinExpressionsRule() : IRule("Extract join expressions") {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Filter pull-up rule for correlated subqueries. Currently handles only basic form of correlated subqueries
 * Matches a filter on top of add dependencies operator. Extracts join conditions from this filter that are
 * dependent on outer columns. Pushes the filter up the plan though map and aggregate operators.
 */

 class TPullUpCorrelatedFilterRule : public IRule {
  public:
    TPullUpCorrelatedFilterRule() : IRule("Pull up correlated filter", ERuleProperties::RequireParents) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
 };

/**
 * Rewrites scalar subplans into cross join plans
 */
class TInlineScalarSubplanRule : public IRule {
  public:
    TInlineScalarSubplanRule() : IRule("Inline scalar subplan", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

class TInlineSimpleInExistsSubplanRule : public ISimplifiedRule {
  public:
    TInlineSimpleInExistsSubplanRule() : ISimplifiedRule("Inline simple in or exists subplan", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down a non-projecting map operator
 * Currently only pushes below joins that are immediately below
 */
class TPushMapRule : public ISimplifiedRule {
  public:
    TPushMapRule() : ISimplifiedRule("Push map operator", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push limit into sort operator
 */
class TPushLimitIntoSortRule : public ISimplifiedRule {
  public:
    TPushLimitIntoSortRule() : ISimplifiedRule("Push limit into sort operator", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push filter though non-projecting map
 */
class TPushFilterUnderMapRule : public ISimplifiedRule {
  public:
    TPushFilterUnderMapRule() : ISimplifiedRule("Push filter under map", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push down filter through joins, adding join conditions to the join operator and potentially
 * converting left join into inner join
 */
class TPushFilterIntoJoinRule : public ISimplifiedRule {
  public:
    TPushFilterIntoJoinRule() : ISimplifiedRule("Push filter into join", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Peephole predicate.
 */
class TPeepholePredicate : public ISimplifiedRule {
  public:
      TPeepholePredicate() : ISimplifiedRule("Peephole predicate", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

      virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Push down filter to olap read.
 */
class TPushOlapFilterRule : public ISimplifiedRule {
  public:
      TPushOlapFilterRule() : ISimplifiedRule("Push olap filter", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

      virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Create inital CBO Tree
 */
class TBuildInitialCBOTreeRule : public ISimplifiedRule {
  public:
    TBuildInitialCBOTreeRule() : ISimplifiedRule("Building initial CBO tree", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Expand CBO Tree
 */
class TExpandCBOTreeRule : public ISimplifiedRule {
  public:
    TExpandCBOTreeRule() : ISimplifiedRule("Expand CBO tree", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Optimize CBO Tree
 */
class TOptimizeCBOTreeRule : public ISimplifiedRule {
  public:
    TOptimizeCBOTreeRule() : ISimplifiedRule("Optimize CBO tree", ERuleProperties::RequireParents | ERuleProperties::RequireStatistics) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Inline unoptimized CBO tree back into the plan
 */
class TInlineCBOTreeRule : public ISimplifiedRule {
  public:
    TInlineCBOTreeRule() : ISimplifiedRule("Inline unoptimized CBO tree", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Generate a stage graph for the plan and assign stage ids to operators
 */
class TAssignStagesRule : public IRule {
  public:
    TAssignStagesRule() : IRule("Assign stages", ERuleProperties::RequireParents) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

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