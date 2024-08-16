#include "kqp_host_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/query_compiler/kqp_query_compiler.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log.h>
#include <ydb/core/kqp/opt/kqp_statistics_transformer.h>
#include <ydb/core/kqp/opt/kqp_column_statistics_requester.h>
#include <ydb/core/kqp/opt/kqp_constant_folding_transformer.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>


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

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <util/generic/is_in.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;
using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NThreading;

namespace {

TAutoPtr<IGraphTransformer> Log(const TStringBuf& transformerName, NYql::NLog::ELevel level = NYql::NLog::ELevel::TRACE) {
    return TLogExprTransformer::Sync(TStringBuilder() << transformerName << "Transformer",
        NYql::NLog::EComponent::ProviderKqp,
        level);
}

TTransformStage LogStage(const TStringBuf& transformerName, NYql::NLog::ELevel level = NYql::NLog::ELevel::TRACE) {
    return TTransformStage{ Log(transformerName, level), TStringBuilder() << "Log" << transformerName, TIssuesIds::DEFAULT_ERROR };
}

class TCompilePhysicalQueryTransformer : public TSyncTransformerBase {
public:
    TCompilePhysicalQueryTransformer(
        const TString& cluster,
        TKqlTransformContext& transformCtx,
        TKqpOptimizeContext& optimizeCtx,
        TTypeAnnotationContext& typesCtx,
        const NMiniKQL::IFunctionRegistry& funcRegistry,
        const TKikimrConfiguration::TPtr& config
    )
        : Cluster(cluster)
        , TransformCtx(transformCtx)
        , OptimizeCtx(optimizeCtx)
        , TypesCtx(typesCtx)
        , FuncRegistry(funcRegistry)
        , Config(config)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        if (!TransformerFinished) {
            TransformerFinished = true;
            auto& preparedQuery = *TransformCtx.QueryCtx->PreparingQuery;
            TKqpPhysicalQuery physicalQuery(input);

            YQL_ENSURE(TransformCtx.DataQueryBlocks);
            auto compiler = CreateKqpQueryCompiler(Cluster, OptimizeCtx.Tables, FuncRegistry, TypesCtx, Config);
            auto ret = compiler->CompilePhysicalQuery(physicalQuery, *TransformCtx.DataQueryBlocks, *preparedQuery.MutablePhysicalQuery(), ctx);
            if (!ret) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Failed to compile physical query."));
                return TStatus::Error;
            }

            preparedQuery.SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
        }

        return TStatus::Ok;
    }

    void Rewind() override {
        TransformerFinished = false;
    }

private:
    const TString Cluster;
    TKqlTransformContext& TransformCtx;
    TKqpOptimizeContext& OptimizeCtx;
    TTypeAnnotationContext& TypesCtx;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    TKikimrConfiguration::TPtr Config;
    bool TransformerFinished = false;
};

class TPrepareQueryAsyncResult : public TKqpAsyncResultBase<IKikimrQueryExecutor::TQueryResult, false> {
public:
    using TResult = IKikimrQueryExecutor::TQueryResult;

    TPrepareQueryAsyncResult(
        const TExprNode::TPtr& queryRoot,
        IGraphTransformer& transformer,
        TExprContext& ctx,
        TKqlTransformContext& transformCtx)
        : TKqpAsyncResultBase(queryRoot, ctx, transformer, nullptr)
        , TransformCtx(transformCtx)
    {
    }

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
    TKqlTransformContext& TransformCtx;
};

class TKqpRunner : public IKqpRunner {
public:
    TKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        const TIntrusivePtr<TTypeAnnotationContext>& typesCtx, const TIntrusivePtr<TKikimrSessionContext>& sessionCtx,
        const TIntrusivePtr<TKqlTransformContext>& transformCtx, const NMiniKQL::IFunctionRegistry& funcRegistry,
        TActorSystem* actorSystem)
        : Gateway(gateway)
        , Cluster(cluster)
        , TypesCtx(*typesCtx)
        , SessionCtx(sessionCtx)
        , FunctionRegistry(funcRegistry)
        , Config(sessionCtx->ConfigPtr())
        , TransformCtx(transformCtx)
        , OptimizeCtx(MakeIntrusive<TKqpOptimizeContext>(cluster, Config, sessionCtx->QueryPtr(),
            sessionCtx->TablesPtr(), sessionCtx->GetUserRequestContext()))
        , BuildQueryCtx(MakeIntrusive<TKqpBuildQueryContext>())
        , Pctx(TKqpProviderContext(*OptimizeCtx, Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->DefaultCostBasedOptimizationLevel)))
        , ActorSystem(actorSystem)
    {
        CreateGraphTransformer(typesCtx, sessionCtx, funcRegistry);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareDataQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(TransformCtx->QueryCtx->Type == EKikimrQueryType::Dml);
        YQL_ENSURE(TMaybeNode<TKiDataQueryBlocks>(query));

        return PrepareQueryInternal(cluster, TKiDataQueryBlocks(query), ctx, settings);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareScanQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(TransformCtx->QueryCtx->Type == EKikimrQueryType::Scan);
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
        YQL_ENSURE(TMaybeNode<TKiDataQueryBlocks>(query));

        const auto dataQueryBlocks = TKiDataQueryBlocks(query);

        if (IsOlapQuery(dataQueryBlocks)) {
            switch (TransformCtx->Config->BlockChannelsMode) {
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR:
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
                    TypesCtx.BlockEngineMode = NYql::EBlockEngineMode::Auto;
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE:
                    TypesCtx.BlockEngineMode = NYql::EBlockEngineMode::Force;
                    break;
                default:
                    YQL_ENSURE(false);
            }
        }

        return PrepareQueryInternal(cluster, dataQueryBlocks, ctx, settings);
    }

private:
    bool IsOlapQuery(const TKiDataQueryBlocks& dataQueryBlocks) {
        if (dataQueryBlocks.ArgCount() != 1) {
            return false;
        }
        const auto& operations = dataQueryBlocks.Arg(0).Operations();
        return std::any_of(
                std::begin(operations),
                std::end(operations),
                [this](const auto& operation) {
                    const auto& tableData = SessionCtx->Tables().ExistingTable(operation.Cluster(), operation.Table());
                    return tableData.Metadata->IsOlap();
                });
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareQueryInternal(const TString& cluster,
        const TKiDataQueryBlocks& dataQueryBlocks, TExprContext& ctx,
        const IKikimrQueryExecutor::TExecuteSettings& settings)
    {
        CreateGraphTransformer(&TypesCtx, SessionCtx, FunctionRegistry);

        YQL_ENSURE(cluster == Cluster);
        YQL_ENSURE(!settings.CommitTx);
        YQL_ENSURE(!settings.RollbackTx);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);
        YQL_ENSURE(TransformCtx->QueryCtx->PreparingQuery);

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
        }

        const bool sysColumnsEnabled = TransformCtx->Config->SystemColumnsEnabled();
        auto kqlQueryBlocks = BuildKqlQuery(dataQueryBlocks, *TransformCtx->Tables, ctx, sysColumnsEnabled, OptimizeCtx, TypesCtx);
        if (!kqlQueryBlocks) {
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        TExprNode::TPtr query = kqlQueryBlocks->Ptr();
        YQL_CLOG(DEBUG, ProviderKqp) << "Initial KQL query: " << KqpExprToPrettyString(*query, ctx);

        TransformCtx->Reset();
        BuildQueryCtx->Reset();
        Transformer->Rewind();

        TransformCtx->DataQueryBlocks = dataQueryBlocks;

        return MakeIntrusive<TPrepareQueryAsyncResult>(query, *Transformer, ctx, *TransformCtx);
    }

    void CreateGraphTransformer(const TIntrusivePtr<TTypeAnnotationContext>& typesCtx, const TIntrusivePtr<TKikimrSessionContext>& sessionCtx,
        const NMiniKQL::IFunctionRegistry& funcRegistry)
    {
        auto preparedExplainTransformer = CreateKqpExplainPreparedTransformer(
            Gateway, Cluster, TransformCtx, &funcRegistry, *typesCtx, OptimizeCtx);

        auto physicalOptimizeTransformer = CreateKqpQueryBlocksTransformer(TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(Log("PhysicalOptimize"), "LogPhysicalOptimize")
            .AddPreTypeAnnotation()
            .AddExpressionEvaluation(funcRegistry)
            .AddIOAnnotation()
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(),
                *typesCtx, Config))
            .Add(CreateKqpCheckQueryTransformer(), "CheckKqlQuery")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .AddCommonOptimization()
            .Add(CreateKqpConstantFoldingTransformer(OptimizeCtx, *typesCtx, Config), "ConstantFolding")
            .Add(CreateKqpColumnStatisticsRequester(Config, *typesCtx, SessionCtx->Tables(), Cluster, ActorSystem), "ColumnGetter")
            .Add(CreateKqpStatisticsTransformer(OptimizeCtx, *typesCtx, Config, Pctx), "Statistics")
            .Add(CreateKqpLogOptTransformer(OptimizeCtx, *typesCtx, Config), "LogicalOptimize")
            .Add(CreateLogicalDataProposalsInspector(*typesCtx), "ProvidersLogicalOptimize")
            .Add(CreateKqpPhyOptTransformer(OptimizeCtx, *typesCtx, Config), "KqpPhysicalOptimize")
            .Add(CreatePhysicalDataProposalsInspector(*typesCtx), "ProvidersPhysicalOptimize")
            .Add(CreateKqpFinalizingOptTransformer(OptimizeCtx), "FinalizingOptimize")
            .Add(CreateKqpQueryPhasesTransformer(), "QueryPhases")
            .Add(CreateKqpQueryEffectsTransformer(OptimizeCtx), "QueryEffects")
            .Add(CreateKqpCheckPhysicalQueryTransformer(), "CheckKqlPhysicalQuery")
            .Build(false));

        auto physicalBuildTxsTransformer = CreateKqpQueryBlocksTransformer(TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(Log("PhysicalBuildTxs"), "LogPhysicalBuildTxs")
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
        
        auto physicalBuildQueryTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(Log("PhysicalBuildQuery"), "LogPhysicalBuildQuery")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config))
            .AddPostTypeAnnotation()
            .Add(CreateKqpBuildPhysicalQueryTransformer(OptimizeCtx, BuildQueryCtx), "BuildPhysicalQuery")
            .Add(CreateKqpStatisticsTransformer(OptimizeCtx, *typesCtx, Config, Pctx), "Statistics")
            .Build(false);

        auto physicalPeepholeTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(Log("PhysicalPeephole"), "LogPhysicalPeephole")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config))
            .AddPostTypeAnnotation()
            .Add(GetDqIntegrationPeepholeTransformer(false, typesCtx), "DqIntegrationPeephole")
            .Add(
                CreateKqpTxsPeepholeTransformer(
                    CreateTypeAnnotationTransformer(
                        CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config),
                    *typesCtx), *typesCtx, Config), "Peephole")
            .Build(false);

        TAutoPtr<IGraphTransformer> compilePhysicalQuery(new TCompilePhysicalQueryTransformer(Cluster,
            *TransformCtx,
            *OptimizeCtx,
            *typesCtx,
            funcRegistry,
            Config));

        Transformer = CreateCompositeGraphTransformer(
            {
                TTransformStage{ physicalOptimizeTransformer, "PhysicalOptimize", TIssuesIds::DEFAULT_ERROR },
                LogStage("PhysicalOptimize"),
                TTransformStage{ physicalBuildTxsTransformer, "PhysicalBuildTxs", TIssuesIds::DEFAULT_ERROR },
                LogStage("PhysicalBuildTxs"),
                TTransformStage{ physicalBuildQueryTransformer, "PhysicalBuildQuery", TIssuesIds::DEFAULT_ERROR },
                LogStage("PhysicalBuildQuery"),
                TTransformStage{ CreateSaveExplainTransformerInput(*TransformCtx), "SaveExplainTransformerInput", TIssuesIds::DEFAULT_ERROR },
                TTransformStage{ physicalPeepholeTransformer, "PhysicalPeephole", TIssuesIds::DEFAULT_ERROR },
                LogStage("PhysicalPeephole"),
                TTransformStage{ compilePhysicalQuery, "CompilePhysicalQuery", TIssuesIds::DEFAULT_ERROR },
                TTransformStage{ preparedExplainTransformer, "ExplainQuery", TIssuesIds::DEFAULT_ERROR }, // TODO(sk): only on stats mode or if explain-only
            },
            false
        );
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
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    const NMiniKQL::IFunctionRegistry& FunctionRegistry;
    TKikimrConfiguration::TPtr Config;

    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TIntrusivePtr<TKqpOptimizeContext> OptimizeCtx;
    TIntrusivePtr<TKqpBuildQueryContext> BuildQueryCtx;

    TKqpProviderContext Pctx;

    TAutoPtr<IGraphTransformer> Transformer;
    
    TActorSystem* ActorSystem; 
};

} // namespace

TIntrusivePtr<IKqpRunner> CreateKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    const TIntrusivePtr<TTypeAnnotationContext>& typesCtx, const TIntrusivePtr<TKikimrSessionContext>& sessionCtx,
    const TIntrusivePtr<TKqlTransformContext>& transformCtx, const NMiniKQL::IFunctionRegistry& funcRegistry, TActorSystem* actorSystem)
{
    return new TKqpRunner(gateway, cluster, typesCtx, sessionCtx, transformCtx, funcRegistry, actorSystem);
}

} // namespace NKqp
} // namespace NKikimr
