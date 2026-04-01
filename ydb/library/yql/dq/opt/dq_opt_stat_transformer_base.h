#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NYql::NDq {

class TDqStatisticsTransformerBase : public TSyncTransformerBase {
public:
    // Full constructor: for subclasses that use the default stat inference with TypeCtx and IProviderContext.
    TDqStatisticsTransformerBase(
        TTypeAnnotationContext* typeCtx,
        const IProviderContext& ctx,
        const TOptimizerHints& hints = {},
        TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr,
        const bool useFSMForSortElimination = false
    );

    // Minimal constructor: for subclasses that override all stat inference methods themselves.
    explicit TDqStatisticsTransformerBase(
        TTypeAnnotationContext* typeCtx,
        const bool useFSMForSortElimination = false
    );

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override;
    void Rewind() override;

protected:
    virtual bool BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) = 0;
    virtual bool AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) = 0;

    virtual bool BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx);
    virtual bool BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx);
    virtual bool AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx);

    // Called from DoTransform for each callable node (before-lambdas phase).
    // Default: PropagateStatisticsToLambdaArgument(input, TypeCtx).
    virtual void OnPropagateToLambdaArgument(const TExprNode::TPtr& input);

    // Called from DoTransform for each node (after-lambdas phase) when UseFSMForSortElimination is true.
    // Default: PropogateTableAliasesFromChildren(input, TypeCtx).
    virtual void OnPropagateTableAliases(const TExprNode::TPtr& input);

    TTypeAnnotationContext* TypeCtx;
    const IProviderContext* Pctx;  // May be null for subclasses that override BeforeLambdas.
    TOptimizerHints Hints;
    TShufflingOrderingsByJoinLabels* ShufflingOrderingsByJoinLabels;
    const bool UseFSMForSortElimination;
};

} // namespace NYql::NDq
