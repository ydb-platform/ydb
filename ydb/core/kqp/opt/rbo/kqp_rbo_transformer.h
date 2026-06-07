#pragma once

#include "kqp_rbo.h"

#include <ydb/core/kqp/opt/kqp_column_statistics_utils.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

struct TTypeAnnotationContext;
struct TExprContext;
class TKikimrTablesData;

} // namespace NYql

namespace NMiniKQL {

class IFunctionRegistry;

} // namespace NMiniKQL

namespace NKikimr::NKqp {

struct TKqlTransformContext;
class IOperator;
class TExpression;
class TOpRoot;
struct TPhysicalOpProps;

namespace NOpt {

struct TKqpOptimizeContext;

} // namespace NOpt

class TKqpRewriteSelectTransformer : public NYql::TSyncTransformerBase {
public:
    TKqpRewriteSelectTransformer(const TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typeCtx)
        : TypeCtx(typeCtx), KqpCtx(*kqpCtx), UniqueSourceIdCounter(0) {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final;
    void Rewind() override;

private:
    NYql::TTypeAnnotationContext& TypeCtx;
    const NOpt::TKqpOptimizeContext& KqpCtx;
    ui64 UniqueSourceIdCounter = 0;
};

TAutoPtr<NYql::IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx,
                                                             NYql::TTypeAnnotationContext& typeCtx);

class TKqpNewRBOTransformer : public NYql::TGraphTransformerBase {
public:
    TKqpNewRBOTransformer(TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typeCtx, TAutoPtr<NYql::IGraphTransformer>&& rboTypeAnnTransformer,
                          NYql::TKikimrTablesData& tables, const TString& cluster, const TString& database,
                          NActors::TActorSystem* actorSystem, const NMiniKQL::IFunctionRegistry& funcRegistry, TIntrusivePtr<TKqlTransformContext> transformCtx);
    // Main method of the transformer
    NYql::IGraphTransformer::TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final;
    NThreading::TFuture<void> DoGetAsyncFuture(const NYql::TExprNode& input) final;
    TStatus DoApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final;
    void AddPlans(std::optional<NJson::TJsonValue> execPlan, std::optional<NJson::TJsonValue> explainPlan);

    void Rewind() override;

private:
    TStatus RequestColumnStatistics(NYql::TExprContext& ctx);
    TStatus ContinueOptimizations(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx);
    bool IsSuitableToRequestStatistics();
    void CollectTablesAndColumnsNames(NYql::TExprContext& ctx);
    void CollectTablesAndColumnsNames(const TIntrusivePtr<IOperator>& op);
    void CollectTablesAndColumnsNames(const TExpression& expr, const TPhysicalOpProps& props);
    bool IsSuitableToCollectStatistics(const TIntrusivePtr<IOperator>& op) const;
    void ApplyColumnStatistics();
    void InitializeRBOOptimizationStages();

    NYql::TTypeAnnotationContext& TypeCtx;
    NOpt::TKqpOptimizeContext& KqpCtx;
    TAutoPtr<NYql::IGraphTransformer> RBOTypeAnnTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;

    // Explain plan holder
    std::optional<NJson::TJsonValue> PlanJson;

    // Special fields to request column statistics.
    NYql::TKikimrTablesData& Tables;
    TString Cluster;
    TString Database;
    NActors::TActorSystem* ActorSystem;
    std::optional<TColumnStatisticsResponse> ColumnStatisticsResponse;
    NThreading::TFuture<void> ColumnStatisticsReadiness;
    THashMap<TString, THashSet<TString>> CMColumnsByTableName;
    THashMap<TString, THashSet<TString>> HistColumnsByTableName;

    TIntrusivePtr<TOpRoot> OpRoot;
    TRuleBasedOptimizer RBO;
};

TAutoPtr<NYql::IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typeCtx,
                                                       TAutoPtr<NYql::IGraphTransformer>&& rboTypeAnnTransformer, NYql::TKikimrTablesData& tables,
                                                       const TString& cluster, const TString& database, NActors::TActorSystem* actorSystem,
                                                       const NMiniKQL::IFunctionRegistry& funcRegistry, TIntrusivePtr<TKqlTransformContext> transformCtx);

class TKqpRBOCleanupTransformer : public NYql::TSyncTransformerBase {
public:
    TKqpRBOCleanupTransformer(NYql::TTypeAnnotationContext& typeCtx)
        : TypeCtx(typeCtx) {
    }

    // Main method of the transformer
    NYql::IGraphTransformer::TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final;
    void Rewind() override;

private:
    NYql::TTypeAnnotationContext& TypeCtx;
};

TAutoPtr<NYql::IGraphTransformer> CreateKqpRBOCleanupTransformer(NYql::TTypeAnnotationContext& typeCtx);

TExprNode::TPtr RewriteSelect(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& typeCtx, const TKqpOptimizeContext& kqpCtx,
                              ui64& uniqueSourceIdCounter, THashMap<const TExprNode*, TExprNode::TPtr>& translated, bool generateRoot = false);

} // namespace NKikimr::NKqp
