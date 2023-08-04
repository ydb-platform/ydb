#include "kqp_host_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/query_compiler/kqp_query_compiler.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/yql_opt_proposed_by_data.h>

#include <util/generic/is_in.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;
using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NThreading;

namespace {

class TPhysicalAsyncRunResult : public TKqpAsyncResultBase<IKikimrQueryExecutor::TQueryResult, false> {
public:
    using TResult = IKikimrQueryExecutor::TQueryResult;

    TPhysicalAsyncRunResult(const TExprNode::TPtr& queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        const TKqlTransformContext& transformCtx)
        : TKqpAsyncResultBase(queryRoot, exprCtx, transformer)
        , TransformCtx(transformCtx) {}

    void FillResult(TResult& queryResult) const override {
        TVector<NKikimrMiniKQL::TResult*> results;
        for (auto& phyResult : TransformCtx.PhysicalQueryResults) {
            auto result = google::protobuf::Arena::CreateMessage<NKikimrMiniKQL::TResult>(
                queryResult.ProtobufArenaPtr.get());

            result->CopyFrom(phyResult);
            results.push_back(result);
        }

        queryResult.QueryStats.CopyFrom(TransformCtx.QueryStats);
        queryResult.Results = std::move(results);
    }

private:
    const TKqlTransformContext& TransformCtx;
};

class TKqpRunner : public IKqpRunner {
public:
    TKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        const TIntrusivePtr<TTypeAnnotationContext>& typesCtx, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const NMiniKQL::IFunctionRegistry& funcRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
        : Gateway(gateway)
        , Cluster(cluster)
        , TypesCtx(*typesCtx)
        , FuncRegistry(funcRegistry)
        , Config(sessionCtx->ConfigPtr())
        , TransformCtx(MakeIntrusive<TKqlTransformContext>(Config, sessionCtx->QueryPtr(), sessionCtx->TablesPtr()))
        , OptimizeCtx(MakeIntrusive<TKqpOptimizeContext>(cluster, Config, sessionCtx->QueryPtr(),
            sessionCtx->TablesPtr()))
        , BuildQueryCtx(MakeIntrusive<TKqpBuildQueryContext>())
    {
        auto logLevel = NYql::NLog::ELevel::TRACE;
        auto logComp = NYql::NLog::EComponent::ProviderKqp;

        PreparedExplainTransformer = TTransformationPipeline(typesCtx)
            .Add(CreateKqpExplainPreparedTransformer(
                Gateway, Cluster, TransformCtx, &funcRegistry, timeProvider, randomProvider), "ExplainQuery")
            .Build(false);

        PhysicalOptimizeTransformer = CreateKqpQueryBlocksTransformer(TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("PhysicalOptimizeTransformer", logComp, logLevel), "LogPhysicalOptimize")
            .AddExpressionEvaluation(FuncRegistry)
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(),
                *typesCtx, Config))
            .Add(CreateKqpCheckQueryTransformer(), "CheckKqlQuery")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .AddCommonOptimization()
            .Add(CreateKqpLogOptTransformer(OptimizeCtx, *typesCtx, Config), "LogicalOptimize")
            .Add(CreateLogicalDataProposalsInspector(*typesCtx), "ProvidersLogicalOptimize")
            .Add(CreateKqpPhyOptTransformer(OptimizeCtx, *typesCtx), "KqpPhysicalOptimize")
            .Add(CreatePhysicalDataProposalsInspector(*typesCtx), "ProvidersPhysicalOptimize")
            .Add(CreateKqpFinalizingOptTransformer(OptimizeCtx), "FinalizingOptimize")
            .Add(CreateKqpQueryPhasesTransformer(), "QueryPhases")
            .Add(CreateKqpQueryEffectsTransformer(OptimizeCtx), "QueryEffects")
            .Add(CreateKqpCheckPhysicalQueryTransformer(), "CheckKqlPhysicalQuery")
            .Build(false));

        PhysicalBuildTxsTransformer = CreateKqpQueryBlocksTransformer(TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("PhysicalBuildTxsTransformer", logComp, logLevel), "LogPhysicalBuildTxs")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config))
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(
                CreateKqpBuildTxsTransformer(
                    OptimizeCtx,
                    BuildQueryCtx,
                    CreateTypeAnnotationTransformer(
                        CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config),
                        *typesCtx),
                    *typesCtx,
                    Config),
                "BuildPhysicalTxs")
            .Build(false));

        PhysicalBuildQueryTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("PhysicalBuildQueryTransformer", logComp, logLevel), "LogPhysicalBuildQuery")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config))
            .AddPostTypeAnnotation()
            .Add(CreateKqpBuildPhysicalQueryTransformer(OptimizeCtx, BuildQueryCtx), "BuildPhysicalQuery")
            .Build(false);

        PhysicalPeepholeTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("PhysicalPeepholeTransformer", logComp, logLevel), "LogPhysicalPeephole")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config))
            .AddPostTypeAnnotation()
            .Add(
                CreateKqpTxsPeepholeTransformer(
                    CreateTypeAnnotationTransformer(
                        CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config),
                    *typesCtx), *typesCtx, Config), "Peephole")
            .Build(false);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareDataQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(TransformCtx->QueryCtx->Type == EKikimrQueryType::Dml);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);
        YQL_ENSURE(TransformCtx->QueryCtx->PreparingQuery);
        YQL_ENSURE(TMaybeNode<TKiDataQueryBlocks>(query));

        return PrepareQueryInternal(cluster, TKiDataQueryBlocks(query), ctx, settings);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareScanQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(TransformCtx->QueryCtx->Type == EKikimrQueryType::Scan);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);
        YQL_ENSURE(TransformCtx->QueryCtx->PreparingQuery);
        YQL_ENSURE(TMaybeNode<TKiDataQueryBlocks>(query));

        TKiDataQueryBlocks dataQueryBlocks(query);

        if (dataQueryBlocks.ArgCount() != 1) {
            ctx.AddError(YqlIssue(ctx.GetPosition(dataQueryBlocks.Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED,
               "Scan query should have single query block."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        const auto& queryBlock = dataQueryBlocks.Arg(0);
        if (queryBlock.Results().Size() != 1) {
            ctx.AddError(YqlIssue(ctx.GetPosition(dataQueryBlocks.Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                "Scan query should have a single result set."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }
        if (queryBlock.Effects().ArgCount() > 0) {
            ctx.AddError(YqlIssue(ctx.GetPosition(dataQueryBlocks.Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                "Scan query cannot have data modifications."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        IKikimrQueryExecutor::TExecuteSettings scanSettings(settings);
        return PrepareQueryInternal(cluster, dataQueryBlocks, ctx, scanSettings);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, TransformCtx->QueryCtx->Type));
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);
        YQL_ENSURE(TransformCtx->QueryCtx->PreparingQuery);
        YQL_ENSURE(TMaybeNode<TKiDataQueryBlocks>(query));

        TKiDataQueryBlocks dataQueryBlocks(query);
        return PrepareQueryInternal(cluster, dataQueryBlocks, ctx, settings);
    }

private:
    TIntrusivePtr<TAsyncQueryResult> PrepareQueryInternal(const TString& cluster,
        const TKiDataQueryBlocks& dataQueryBlocks, TExprContext& ctx,
        const IKikimrQueryExecutor::TExecuteSettings& settings)
    {
        YQL_ENSURE(cluster == Cluster);

        auto* queryCtx = TransformCtx->QueryCtx.Get();

        if (queryCtx->Type == EKikimrQueryType::Dml) {
            ui32 resultsCount = 0;
            for (const auto& block : dataQueryBlocks) {
                for (ui32 i = 0; i < block.Results().Size(); ++i, ++resultsCount) {
                    auto& result = *queryCtx->PreparingQuery->AddResults();
                    result.SetKqlIndex(0);
                    result.SetResultIndex(resultsCount);
                    for (const auto& column : block.Results().Item(i).Columns()) {
                        *result.AddColumnHints() = column.Value();
                    }
                    result.SetRowsLimit(FromString<ui64>(block.Results().Item(i).RowsLimit()));
                }
            }
        } else {
            // scan query
        }

        bool sysColumnsEnabled = TransformCtx->Config->SystemColumnsEnabled();
        return PrepareQueryNewEngine(cluster, dataQueryBlocks, ctx, settings, sysColumnsEnabled);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareQueryNewEngine(const TString& cluster,
        const TKiDataQueryBlocks& dataQueryBlocks, TExprContext& ctx,
        const IKikimrQueryExecutor::TExecuteSettings& settings, bool sysColumnsEnabled)
    {
        YQL_ENSURE(cluster == Cluster);
        YQL_ENSURE(!settings.CommitTx);
        YQL_ENSURE(!settings.RollbackTx);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);

        EKikimrQueryType queryType = TransformCtx->QueryCtx->Type;
        switch (queryType) {
            case EKikimrQueryType::Dml:
            case EKikimrQueryType::Scan:
            case EKikimrQueryType::Query:
            case EKikimrQueryType::Script:
                break;
            default:
                YQL_ENSURE(false, "PrepareQueryNewEngine, unexpected query type: " << queryType);
        }

        auto kqlQueryBlocks = BuildKqlQuery(dataQueryBlocks, *TransformCtx->Tables, ctx, sysColumnsEnabled, OptimizeCtx, TypesCtx);
        if (!kqlQueryBlocks) {
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        auto query = kqlQueryBlocks->Ptr();
        YQL_CLOG(DEBUG, ProviderKqp) << "Initial KQL query: " << KqpExprToPrettyString(*query, ctx);

        TransformCtx->Reset();

        PhysicalOptimizeTransformer->Rewind();
        auto optimizedQuery = query;
        auto status = InstantTransform(*PhysicalOptimizeTransformer, optimizedQuery, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(query->Pos()), "Failed to optimize query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        YQL_CLOG(TRACE, ProviderKqp) << "PhysicalOptimizeTransformer: "
            << TransformerStatsToYson(PhysicalOptimizeTransformer->GetStatistics());
        YQL_CLOG(DEBUG, ProviderKqp) << "Optimized KQL query: " << KqpExprToPrettyString(*optimizedQuery, ctx);

        BuildQueryCtx->Reset();
        PhysicalBuildTxsTransformer->Rewind();
        auto builtTxsQuery = optimizedQuery;
        status = InstantTransform(*PhysicalBuildTxsTransformer, builtTxsQuery, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(query->Pos()), "Failed to build physical txs."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        YQL_CLOG(TRACE, ProviderKqp) << "PhysicalBuildTxsTransformer: "
            << TransformerStatsToYson(PhysicalOptimizeTransformer->GetStatistics());

        PhysicalBuildQueryTransformer->Rewind();
        auto builtQuery = builtTxsQuery;
        status = InstantTransform(*PhysicalBuildQueryTransformer, builtQuery, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(query->Pos()), "Failed to build physical query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        YQL_CLOG(TRACE, ProviderKqp) << "PhysicalBuildQueryTransformer: "
            << TransformerStatsToYson(PhysicalBuildQueryTransformer->GetStatistics());

        PhysicalPeepholeTransformer->Rewind();
        auto transformedQuery = builtQuery;
        status = InstantTransform(*PhysicalPeepholeTransformer, transformedQuery, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(query->Pos()), "Failed peephole."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(
                ctx.IssueManager.GetIssues()));
        }

        YQL_CLOG(TRACE, ProviderKqp) << "PhysicalPeepholeTransformer: "
            << TransformerStatsToYson(PhysicalPeepholeTransformer->GetStatistics());
        YQL_CLOG(DEBUG, ProviderKqp) << "Physical KQL query: " << KqpExprToPrettyString(*builtQuery, ctx);
        YQL_CLOG(DEBUG, ProviderKqp) << "Physical KQL query after peephole: " << KqpExprToPrettyString(*transformedQuery, ctx);

        auto& preparedQuery = *TransformCtx->QueryCtx->PreparingQuery;
        TKqpPhysicalQuery physicalQuery(transformedQuery);

        auto compiler = CreateKqpQueryCompiler(Cluster, OptimizeCtx->Tables, FuncRegistry, TypesCtx);
        auto ret = compiler->CompilePhysicalQuery(physicalQuery, dataQueryBlocks, *preparedQuery.MutablePhysicalQuery(), ctx);
        if (!ret) {
            ctx.AddError(TIssue(ctx.GetPosition(query->Pos()), "Failed to compile physical query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        preparedQuery.SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
        // TODO(sk): only on stats mode or if explain-only
        PreparedExplainTransformer->Rewind();
        return MakeIntrusive<TPhysicalAsyncRunResult>(builtQuery, ctx, *PreparedExplainTransformer, *TransformCtx);
    }

    static bool MergeFlagValue(const TMaybe<bool>& configFlag, const TMaybe<bool>& flag) {
        if (flag) {
            return *flag;
        }

        if (configFlag) {
            return *configFlag;
        }

        return false;
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TTypeAnnotationContext& TypesCtx;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    TKikimrConfiguration::TPtr Config;

    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TIntrusivePtr<TKqpOptimizeContext> OptimizeCtx;
    TIntrusivePtr<TKqpBuildQueryContext> BuildQueryCtx;

    TAutoPtr<IGraphTransformer> PreparedExplainTransformer;

    TAutoPtr<IGraphTransformer> PhysicalOptimizeTransformer;
    TAutoPtr<IGraphTransformer> PhysicalBuildTxsTransformer;
    TAutoPtr<IGraphTransformer> PhysicalBuildQueryTransformer;
    TAutoPtr<IGraphTransformer> PhysicalPeepholeTransformer;
};

} // namespace

TIntrusivePtr<IKqpRunner> CreateKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    const TIntrusivePtr<TTypeAnnotationContext>& typesCtx, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    const NMiniKQL::IFunctionRegistry& funcRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
{
    return new TKqpRunner(gateway, cluster, typesCtx, sessionCtx, funcRegistry, timeProvider, randomProvider);
}

} // namespace NKqp
} // namespace NKikimr
