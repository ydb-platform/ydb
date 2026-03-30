#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>

namespace NKikimr::NKqp {

class TDqStatisticsTransformerBase : public NYql::TSyncTransformerBase {
public:
    TDqStatisticsTransformerBase(
        NYql::TTypeAnnotationContext* typeCtx,
        const IProviderContext& ctx,
        const TOptimizerHints& hints = {},
        TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr,
        const bool useFSMForSortElimination = false,
        TKqpStatsStore* kqpStats = nullptr
    );

    NYql::IGraphTransformer::TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) override;
    void Rewind() override;

protected:
    virtual bool BeforeLambdasSpecific(const NYql::TExprNode::TPtr& input, NYql::TExprContext& ctx) = 0;
    virtual bool AfterLambdasSpecific(const NYql::TExprNode::TPtr& input, NYql::TExprContext& ctx) = 0;

    bool BeforeLambdasUnmatched(const NYql::TExprNode::TPtr& input, NYql::TExprContext& ctx);
    bool BeforeLambdas(const NYql::TExprNode::TPtr& input, NYql::TExprContext& ctx);
    bool AfterLambdas(const NYql::TExprNode::TPtr& input, NYql::TExprContext& ctx);

    NYql::TTypeAnnotationContext* TypeCtx;  // kept: FSM fields, type annotation ops
    TKqpStatsStore* KqpStats;               // all GetStats/SetStats (may be null if using TypeCtx)
    const IProviderContext& Pctx;
    TOptimizerHints Hints;
    TShufflingOrderingsByJoinLabels* ShufflingOrderingsByJoinLabels;
    const bool UseFSMForSortElimination;
};

} // namespace NKikimr::NKqp
