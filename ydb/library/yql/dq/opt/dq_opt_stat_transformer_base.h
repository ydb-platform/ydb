#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

namespace NYql::NDq {

class TDqStatisticsTransformerBase : public TSyncTransformerBase {
public:
    TDqStatisticsTransformerBase(TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints = {});

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override;
    void Rewind() override;

protected:
    virtual bool BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) = 0;
    virtual bool AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) = 0;

    bool BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx);
    bool BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx);
    bool AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx);

    TTypeAnnotationContext* TypeCtx;
    const IProviderContext& Pctx;
    TCardinalityHints CardinalityHints = {};
};

} // namespace NYql::NDq
