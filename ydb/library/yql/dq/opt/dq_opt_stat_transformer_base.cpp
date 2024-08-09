#include "dq_opt_stat_transformer_base.h"

#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NYql::NDq {

using namespace NNodes;

TDqStatisticsTransformerBase::TDqStatisticsTransformerBase(TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints)
    : TypeCtx(typeCtx), Pctx(ctx), CardinalityHints(hints)
{ }

IGraphTransformer::TStatus TDqStatisticsTransformerBase::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    VisitExprLambdasLast(
        input, [&](const TExprNode::TPtr& input) {
            BeforeLambdas(input, ctx) || BeforeLambdasSpecific(input, ctx) || BeforeLambdasUnmatched(input, ctx);

            // We have a separate rule for all callables that may use a lambda
            // we need to take each generic callable and see if it includes a lambda
            // if so - we will map the input to the callable to the argument of the lambda
            if (input->IsCallable()) {
                PropagateStatisticsToLambdaArgument(input, TypeCtx);
            }

            return true;
        },
        [&](const TExprNode::TPtr& input) {
            return AfterLambdas(input, ctx) || AfterLambdasSpecific(input, ctx) || true;
        });
    return IGraphTransformer::TStatus::Ok;
}

bool TDqStatisticsTransformerBase::BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    bool matched = true;
    // Generic matchers
    if (TCoFilterBase::Match(input.Get())){
        InferStatisticsForFilter(input, TypeCtx);
    }
    else if(TCoSkipNullMembers::Match(input.Get())){
        InferStatisticsForSkipNullMembers(input, TypeCtx);
    }
    else if(TCoExtractMembers::Match(input.Get())){
        InferStatisticsForExtractMembers(input, TypeCtx);
    }
    else if(TCoAggregateCombine::Match(input.Get())){
        InferStatisticsForAggregateCombine(input, TypeCtx);
    }
    else if(TCoAggregateMergeFinalize::Match(input.Get())){
        InferStatisticsForAggregateMergeFinalize(input, TypeCtx);
    }
    else if (TCoAsList::Match(input.Get())){
        InferStatisticsForAsList(input, TypeCtx);
    }
    else if (TCoParameter::Match(input.Get())) {
        InferStatisticsForListParam(input, TypeCtx);
    }

    // Join matchers
    else if(TCoMapJoinCore::Match(input.Get())) {
        InferStatisticsForMapJoin(input, TypeCtx, Pctx, CardinalityHints);
    }
    else if(TCoGraceJoinCore::Match(input.Get())) {
        InferStatisticsForGraceJoin(input, TypeCtx, Pctx, CardinalityHints);
    }

    // Do nothing in case of EquiJoin, otherwise the EquiJoin rule won't fire
    else if(TCoEquiJoin::Match(input.Get())){
    }

    // In case of DqSource, propagate the statistics from the correct argument
    else if (TDqSource::Match(input.Get())) {
        InferStatisticsForDqSource(input, TypeCtx);
    }
    else {
        matched = false;
    }

    return matched;
}

bool TDqStatisticsTransformerBase::BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    if (input->ChildrenSize() >= 1) {
        auto stats = TypeCtx->GetStats(input->ChildRef(0).Get());
        if (stats) {
            TypeCtx->SetStats(input.Get(), stats);
        }
    }
    return true;
}

bool TDqStatisticsTransformerBase::AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    if (TDqStageBase::Match(input.Get())) {
        InferStatisticsForStage(input, TypeCtx);
    } else if (TCoFlatMapBase::Match(input.Get())) {
        InferStatisticsForFlatMap(input, TypeCtx);
    } else {
        matched = false;
    }
    return matched;
}

void TDqStatisticsTransformerBase::Rewind() { }

} // namespace NYql::NDq

