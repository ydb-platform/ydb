#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

namespace NYql::NDq {

class TDqStatisticsTransformerBase : public TSyncTransformerBase {
public:
    TDqStatisticsTransformerBase(
        TTypeAnnotationContext* typeCtx,
        const NKikimr::NKqp::IProviderContext& ctx,
        const NKikimr::NKqp::TOptimizerHints& hints = {},
        TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr,
        const bool useFSMForSortElimination = false
    );

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override;
    void Rewind() override;

protected:
    virtual bool BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) = 0;
    virtual bool AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) = 0;

    bool BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx);
    bool BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx);
    bool AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx);

    TTypeAnnotationContext* TypeCtx;
    const NKikimr::NKqp::IProviderContext& Pctx;
    NKikimr::NKqp::TOptimizerHints Hints;
    TShufflingOrderingsByJoinLabels* ShufflingOrderingsByJoinLabels;
    const bool UseFSMForSortElimination;
};

} // namespace NYql::NDq
