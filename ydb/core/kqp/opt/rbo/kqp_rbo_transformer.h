#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

class TKqpRewriteSelectTransformer : public TSyncTransformerBase {
  public:
    TKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx, TTypeAnnotationContext &typeCtx)
        : TypeCtx(typeCtx), KqpCtx(*kqpCtx), UniqueSourceIdCounter(0) {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) final;
    void Rewind() override;

  private:
    TTypeAnnotationContext &TypeCtx;
    const TKqpOptimizeContext &KqpCtx;
    ui64 UniqueSourceIdCounter = 0;
};

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                                                             TTypeAnnotationContext &typeCtx);

class TKqpNewRBOTransformer: public TGraphTransformerBase {
public:
    TKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                          TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer, const NMiniKQL::IFunctionRegistry& funcRegistry);
    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final;
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    TStatus RequestColumnStatistics();

    void Rewind() override;

private:
    TTypeAnnotationContext& TypeCtx;
    TKqpOptimizeContext& KqpCtx;
    TRuleBasedOptimizer RBO;
    std::shared_ptr<TOpRoot> OpRoot;
    NThreading::TPromise<void> ColumnStatisticsReadiness;
};

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                                       TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer,
                                                       const NMiniKQL::IFunctionRegistry& funcRegistry);

class TKqpRBOCleanupTransformer: public TSyncTransformerBase {
public:
    TKqpRBOCleanupTransformer(TTypeAnnotationContext& typeCtx)
        : TypeCtx(typeCtx) {
    }

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    void Rewind() override;

private:
    TTypeAnnotationContext& TypeCtx;
};

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext& typeCtx);

TExprNode::TPtr RewriteSelect(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& typeCtx, const TKqpOptimizeContext& kqpCtx,
                              ui64& uniqueSourceIdCounter, bool pgSyntax = false);

} // namespace NKqp
} // namespace NKikimr