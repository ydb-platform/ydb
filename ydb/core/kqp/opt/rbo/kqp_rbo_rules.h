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

class TInlineGenericInExistsSubplanRule : public ISimplifiedRule {
  public:
    TInlineGenericInExistsSubplanRule() : ISimplifiedRule("Inline generic in or exists subplan", ERuleProperties::RequireParents | ERuleProperties::RequireTypes | ERuleProperties::RequireMetadata) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Inline join filters
 */
class TInlineJoinFiltersRule : public ISimplifiedRule {
  public:
    TInlineJoinFiltersRule() : ISimplifiedRule("Inline join filters", ERuleProperties::RequireParents | ERuleProperties::RequireTypes | ERuleProperties::RequireMetadata) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};


/***
 * Fuse two consequtive filters
 */
class TFuseFiltersRule : public ISimplifiedRule {
  public:
    TFuseFiltersRule() : ISimplifiedRule("Fuse filters", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Push append map elements closer to sources one topology at a time.
 * If only part of a map can move safely, leave the rest above.
 */
class TPushAppendIntoMapRule : public ISimplifiedRule {
  public:
    TPushAppendIntoMapRule()
        : ISimplifiedRule("Push append into map", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

class TPushAppendThroughUnaryRule : public ISimplifiedRule {
  public:
    explicit TPushAppendThroughUnaryRule(bool pushUnderFilter = true)
        : ISimplifiedRule("Push append through unary", ERuleProperties::RequireParents)
        , PushUnderFilter(pushUnderFilter) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;

  private:
    bool PushUnderFilter;
};

class TPushAppendThroughAggregateRule : public ISimplifiedRule {
  public:
    TPushAppendThroughAggregateRule()
        : ISimplifiedRule("Push append through aggregate", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

class TPushAppendThroughJoinRule : public ISimplifiedRule {
  public:
    TPushAppendThroughJoinRule()
        : ISimplifiedRule("Push append through join", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Compatibility wrapper for focused tests: applies one alias-append topology per rule firing.
 */
class TPushAppendRule : public ISimplifiedRule {
  public:
    explicit TPushAppendRule(bool pushUnderFilter = true)
        : ISimplifiedRule("Push append map elements", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness)
        , PushUnderFilter(pushUnderFilter) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;

  private:
    bool PushUnderFilter;
};

/**
 * Compatibility wrapper for focused tests: applies one expression-append topology per rule firing.
 */
class TPushAppendExpressionRule : public ISimplifiedRule {
  public:
    explicit TPushAppendExpressionRule(bool pushUnderFilter = true)
        : ISimplifiedRule("Push append expressions", ERuleProperties::RequireParents)
        , PushUnderFilter(pushUnderFilter) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;

  private:
    bool PushUnderFilter;
};

/**
 * Convert semantic renames to append aliases when the original name may stay visible.
 */
class TRenameToAppendRule : public IRule {
  public:
    TRenameToAppendRule()
        : IRule("Convert safe renames to appends", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Push semantic renames one topology at a time.
 */
class TPushRenameThroughTransparentUnaryRule : public IRule {
  public:
    explicit TPushRenameThroughTransparentUnaryRule(bool pushAppendAliasesUnderFilter = true)
        : IRule("Push semantic rename through unary", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints)
        , PushAppendAliasesUnderFilter(pushAppendAliasesUnderFilter) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;

  private:
    bool PushAppendAliasesUnderFilter;
};

class TPushRenameThroughPassThroughMapRule : public IRule {
  public:
    TPushRenameThroughPassThroughMapRule()
        : IRule("Push semantic rename through map", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

class TPushRenameThroughAggregateKeyRule : public IRule {
  public:
    TPushRenameThroughAggregateKeyRule()
        : IRule("Push semantic rename through aggregate key", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

class TPushRenameThroughJoinSideRule : public IRule {
  public:
    TPushRenameThroughJoinSideRule()
        : IRule("Push semantic rename through join side", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

class TPushRenameIntoReadRule : public IRule {
  public:
    TPushRenameIntoReadRule()
        : IRule("Push semantic rename into read", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

class TPushRenameIntoMapProducerRule : public IRule {
  public:
    TPushRenameIntoMapProducerRule()
        : IRule("Push semantic rename into map producer", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

class TPushRenameIntoAggregateResultRule : public IRule {
  public:
    TPushRenameIntoAggregateResultRule()
        : IRule("Push semantic rename into aggregate result", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Compatibility wrapper for focused tests: applies one rename topology per rule firing.
 */
class TPushRenameRule : public IRule {
  public:
    explicit TPushRenameRule(bool pushAppendAliasesUnderFilter = true)
        : IRule("Push semantic rename", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints)
        , PushAppendAliasesUnderFilter(pushAppendAliasesUnderFilter) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;

  private:
    bool PushAppendAliasesUnderFilter;
};

/**
 * Rewrites local expressions to the alias already preferred by liveness.
 */
class TRewriteExpressionsToPreferredAliasesRule : public IRule {
  public:
    TRewriteExpressionsToPreferredAliasesRule()
        : IRule("Rewrite expressions to preferred aliases", ERuleProperties::RequireLiveness | ERuleProperties::RequireAliases) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
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
 * Push ranges to read.
 */
class TPushRangesRule : public ISimplifiedRule {
  public:
      TPushRangesRule() : ISimplifiedRule("Push ranges", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

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
 * Push down projection to olap read.
 */
class TPushOlapProjectionRule : public ISimplifiedRule {
  public:
      TPushOlapProjectionRule() : ISimplifiedRule("Push olap projection", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

      virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Disable blocks on columns limit.
 */
class TDisableBlocksOnColumnsLimitRule : public ISimplifiedRule {
  public:
      TDisableBlocksOnColumnsLimitRule() : ISimplifiedRule("Disable blocks on columns limit", ERuleProperties::RequireParents) {}

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
 * Separate global constant folding stage
 */
class TConstantFoldingStage : public IRBOStage {
  public:
    TConstantFoldingStage();
    virtual void RunStage(TOpRoot &root, TRBOContext &ctx) override;
};

/**
 * Remove append-only map elements whose outputs are not live.
 */
class TPruneDeadMapElementsRule : public IRule {
  public:
    TPruneDeadMapElementsRule()
        : IRule("Prune dead map elements", ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Remove read columns whose output IUs are not live.
 */
class TPruneDeadReadColumnsRule : public IRule {
  public:
    TPruneDeadReadColumnsRule()
        : IRule("Prune dead read columns", ERuleProperties::RequireLiveness) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Remove aggregate result traits whose output IUs are not live.
 */
class TPruneDeadAggregateTraitsRule : public IRule {
  public:
    TPruneDeadAggregateTraitsRule()
        : IRule("Prune dead aggregate traits", ERuleProperties::RequireLiveness) {}

    virtual bool MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Prune dead logical outputs using final liveness.
 */
class TLogicalOutputPruningStage : public IRBOStage {
  public:
    TLogicalOutputPruningStage();
    virtual void RunStage(TOpRoot& root, TRBOContext& ctx) override;
};

/**
 * Propagate and assign hash functions on StageGraph connections.
 */
class TPropagateHashFuncStage : public IRBOStage {
  public:
    TPropagateHashFuncStage();
    virtual void RunStage(TOpRoot& root, TRBOContext& ctx) override;
};

/**
 * Propagate aggregate operator.
 */
class TPropagateAggregateThroughStageRule: public ISimplifiedRule {
public:
    TPropagateAggregateThroughStageRule()
        : ISimplifiedRule("Propagate aggregate operator through stages", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {
    }

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) override;
};

/**
 * Propagate topsort operator.
 */
class TPropagateTopSortThroughStageRule : public ISimplifiedRule {
  public:
    TPropagateTopSortThroughStageRule() : ISimplifiedRule("Propagate topsort operator through stages", ERuleProperties::RequireParents | ERuleProperties::RequireTypes) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

/**
 * Propagate limit operator.
 */
class TPropagateLimitThroughStageRule : public ISimplifiedRule {
  public:
    TPropagateLimitThroughStageRule() : ISimplifiedRule("Propagate limit operator through stages", ERuleProperties::RequireParents) {}

    virtual TIntrusivePtr<IOperator> SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) override;
};

} // namespace NKqp
} // namespace NKikimr
