#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext : public TSimpleRefCount<TKqpOptimizeContext> {
    TKqpOptimizeContext(const TString& cluster, const NYql::TKikimrConfiguration::TPtr& config,
        const TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx, const TIntrusivePtr<NYql::TKikimrTablesData>& tables)
        : Cluster(cluster)
        , Config(config)
        , QueryCtx(queryCtx)
        , Tables(tables)
    {
        YQL_ENSURE(QueryCtx);
        YQL_ENSURE(Tables);
    }

    TString Cluster;
    const NYql::TKikimrConfiguration::TPtr Config;
    const TIntrusivePtr<NYql::TKikimrQueryContext> QueryCtx;
    const TIntrusivePtr<NYql::TKikimrTablesData> Tables;
    int JoinsCount{};
    int EquiJoinsCount{};

    bool IsDataQuery() const {
        return QueryCtx->Type == NYql::EKikimrQueryType::Dml;
    }

    bool IsScanQuery() const {
        return QueryCtx->Type == NYql::EKikimrQueryType::Scan;
    }

    bool IsGenericQuery() const {
        return QueryCtx->Type == NYql::EKikimrQueryType::Query || QueryCtx->Type == NYql::EKikimrQueryType::Script;
    }
};

struct TKqpBuildQueryContext : TThrRefBase {
    TKqpBuildQueryContext() {}

    TVector<NYql::NNodes::TKqpPhysicalTx> PhysicalTxs;
    TVector<NYql::NNodes::TExprBase> QueryResults;

    void Reset() {
        PhysicalTxs.clear();
        QueryResults.clear();
    }
};

bool IsKqpEffectsStage(const NYql::NNodes::TDqStageBase& stage);
bool NeedSinks(const NYql::TKikimrTableDescription& table, const TKqpOptimizeContext& kqpCtx);

TMaybe<NYql::NNodes::TKqlQueryList> BuildKqlQuery(NYql::NNodes::TKiDataQueryBlocks queryBlocks,
    const NYql::TKikimrTablesData& tablesData, NYql::TExprContext& ctx, bool withSystemColumns,
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpFinalizingOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpQueryPhasesTransformer();
TAutoPtr<NYql::IGraphTransformer> CreateKqpQueryEffectsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpCheckPhysicalQueryTransformer();

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildTxsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx, TAutoPtr<NYql::IGraphTransformer>&& typeAnnTransformer,
    NYql::TTypeAnnotationContext& typesCtx, NYql::TKikimrConfiguration::TPtr& config);

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildPhysicalQueryTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpQueryBlocksTransformer(TAutoPtr<NYql::IGraphTransformer> queryBlockTransformer);

} // namespace NKikimr::NKqp::NOpt
