#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/kqp_column_statistics_utils.h>
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
    TTypeAnnotationContext& TypeCtx;
    const TKqpOptimizeContext& KqpCtx;
    ui64 UniqueSourceIdCounter = 0;
};

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                                                             TTypeAnnotationContext &typeCtx);

class TKqpNewRBOTransformer: public TGraphTransformerBase {
public:
    TKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                          TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer, TKikimrTablesData& tables, const TString& cluster, const TString& database,
                          TActorSystem* actorSystem, const NMiniKQL::IFunctionRegistry& funcRegistry);
    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final;
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() override;

private:
    TStatus RequestColumnStatistics(TExprContext& ctx);
    TStatus ContinueOptimizations(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx);
    bool IsSuitableToRequestStatistics();
    void CollectTablesAndColumnsNames(TExprContext& ctx);
    void CollectTablesAndColumnsNames(const TIntrusivePtr<IOperator>& op);
    void CollectTablesAndColumnsNames(const TExpression& expr, const TPhysicalOpProps& props);
    bool IsSuitableToCollectStatistics(const TIntrusivePtr<IOperator>& op) const;
    void ApplyColumnStatistics();
    void InitializeRBOOptimizationStages();

    TTypeAnnotationContext& TypeCtx;
    TKqpOptimizeContext& KqpCtx;
    TAutoPtr<IGraphTransformer> RBOTypeAnnTransformer;
    TAutoPtr<IGraphTransformer> PeepholeTypeAnnTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;

    // Special fields to request column statistics.
    TKikimrTablesData& Tables;
    TString Cluster;
    TString Database;
    TActorSystem* ActorSystem;
    std::optional<TColumnStatisticsResponse> ColumnStatisticsResponse;
    NThreading::TFuture<void> ColumnStatisticsReadiness;
    THashMap<TString, THashSet<TString>> CMColumnsByTableName;
    THashMap<TString, THashSet<TString>> HistColumnsByTableName;

    TIntrusivePtr<TOpRoot> OpRoot;
    TRuleBasedOptimizer RBO;
};

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                                       TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer, TKikimrTablesData& tables,
                                                       const TString& cluster, const TString& database, TActorSystem* actorSystem,
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