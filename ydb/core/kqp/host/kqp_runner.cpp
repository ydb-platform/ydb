#include "kqp_host_impl.h"
#include "kqp_ne_helper.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compile/kqp_compile.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/prepare/kqp_query_plan.h>
#include <ydb/core/kqp/prepare/kqp_prepare.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;
using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NThreading;

namespace {

void FillAstAndPlan(NKikimrKqp::TPreparedKql& kql, const TExprNode::TPtr& queryExpr, TExprContext& ctx) {
    TStringStream astStream;
    auto ast = ConvertToAst(*queryExpr, ctx, TExprAnnotationFlags::None, true);
    ast.Root->PrettyPrintTo(astStream, TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine);
    kql.SetAst(astStream.Str());

    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);

    WriteKqlPlan(writer, queryExpr);
    kql.SetPlan(writer.Str());
}

class TAsyncRunResult : public TKqpAsyncResultBase<IKikimrQueryExecutor::TQueryResult, false> {
public:
    using TResult = IKikimrQueryExecutor::TQueryResult;

    TAsyncRunResult(const TExprNode::TPtr& queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        const TKqlTransformContext& transformCtx)
        : TKqpAsyncResultBase(queryRoot, exprCtx, transformer)
        , TransformCtx(transformCtx) {}

    void FillResult(TResult& queryResult) const override {
        if (TransformCtx.QueryCtx->PrepareOnly) {
            return;
        }

        if (!TransformCtx.MkqlResults.empty() && TransformCtx.MkqlResults.back()) {
            queryResult.Results = UnpackKikimrRunResult(*TransformCtx.MkqlResults.back(),
                queryResult.ProtobufArenaPtr.get());
        }

        queryResult.QueryTraits = TransformCtx.QueryCtx->QueryTraits;
        queryResult.QueryStats.CopyFrom(TransformCtx.QueryStats);
    }

private:
    const TKqlTransformContext& TransformCtx;
};

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

        queryResult.QueryTraits = TransformCtx.QueryCtx->QueryTraits;
        queryResult.QueryStats.CopyFrom(TransformCtx.QueryStats);
        queryResult.Results = std::move(results);
    }

private:
    const TKqlTransformContext& TransformCtx;
};

class TScanAsyncRunResult : public TKqpAsyncResultBase<IKikimrQueryExecutor::TQueryResult, false> {
public:
    using TResult = IKikimrQueryExecutor::TQueryResult;

    TScanAsyncRunResult(const TExprNode::TPtr& queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        const TKqlTransformContext& transformCtx)
        : TKqpAsyncResultBase(queryRoot, exprCtx, transformer)
        , TransformCtx(transformCtx) {}

    void FillResult(TResult& queryResult) const override {
        queryResult.QueryStats.CopyFrom(TransformCtx.QueryStats);
    }

private:
    const TKqlTransformContext& TransformCtx;
};

class TKqpIterationGuardTransformer : public TSyncTransformerBase {
public:
    const ui32 MaxTransformIterations = 100;

public:
    TKqpIterationGuardTransformer()
        : Iterations(0) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (Iterations > MaxTransformIterations) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << "Exceeded maximum allowed KQP iterations: " << MaxTransformIterations));
            return TStatus::Error;
        }

        ++Iterations;
        YQL_CLOG(DEBUG, ProviderKqp) << "Iteration #" << Iterations << ":" << Endl
            << KqpExprToPrettyString(*input, ctx);

        return TStatus::Ok;
    }

    void Rewind() override {
        Iterations = 0;
    }

private:
    ui32 Iterations;
};

class TKqpRunner : public IKqpRunner {
public:
    TKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
        TIntrusivePtr<TTypeAnnotationContext> typesCtx, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const NMiniKQL::IFunctionRegistry& funcRegistry)
        : Gateway(gateway)
        , Cluster(cluster)
        , TypesCtx(*typesCtx)
        , FuncRegistry(funcRegistry)
        , Config(sessionCtx->ConfigPtr())
        , TxState(MakeIntrusive<TKqpTransactionState>(sessionCtx))
        , TransformCtx(MakeIntrusive<TKqlTransformContext>(Config, sessionCtx->QueryPtr(), sessionCtx->TablesPtr()))
        , OptimizeCtx(MakeIntrusive<TKqpOptimizeContext>(cluster, Config, sessionCtx->QueryPtr(),
            sessionCtx->TablesPtr()))
        , BuildQueryCtx(MakeIntrusive<TKqpBuildQueryContext>())
    {
        KqlTypeAnnTransformer = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(*typesCtx),
            *typesCtx);

        auto logLevel = NLog::ELevel::TRACE;
        auto logComp = NLog::EComponent::ProviderKqp;

        KqlOptimizeTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("KqlOptimizeTransformer", logComp, logLevel), "LogKqlOptimize")
            .AddTypeAnnotationTransformer()
            .AddPostTypeAnnotation()
            .AddOptimization(false)
            .Build(false);

        KqlPrepareTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("KqlPrepareTransformer", logComp, logLevel), "LogKqlRun")
            .Add(new TKqpIterationGuardTransformer(), "IterationGuard")
            .AddTypeAnnotationTransformer()
            .Add(CreateKqpCheckKiProgramTransformer(), "CheckQuery")
            .Add(CreateKqpSimplifyTransformer(), "Simplify")
            .Add(CreateKqpAnalyzeTransformer(TransformCtx), "Analyze")
            .Add(CreateKqpRewriteTransformer(TransformCtx), "Rewrite")
            .Add(CreateKqpExecTransformer(Gateway, Cluster, TxState, TransformCtx), "Prepare")
            .Add(CreateKqpSubstituteTransformer(TxState, TransformCtx), "Substitute")
            .Add(CreateKqpFinalizeTransformer(Gateway, Cluster, TxState, TransformCtx), "Finalize")
            .Build(false);

        PreparedRunTransformer = TTransformationPipeline(typesCtx)
            .Add(TLogExprTransformer::Sync("PreparedRun iteration", logComp, logLevel), "KqlPreparedRun")
            .Add(CreateKqpAcquireMvccSnapshotTransformer(Gateway, TxState, TransformCtx), "AcquireMvccSnapshot")
            .Add(CreateKqpExecutePreparedTransformer(Gateway, Cluster, TxState, TransformCtx), "ExecutePrepared")
            .Add(CreateKqpFinalizeTransformer(Gateway, Cluster, TxState, TransformCtx), "Finalize")
            .Build(false);

        PreparedExplainTransformer = TTransformationPipeline(typesCtx)
            .Add(CreateKqpExplainPreparedTransformer(Gateway, Cluster, TransformCtx), "ExplainQuery")
            .Build(false);

        PhysicalOptimizeTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("PhysicalOptimizeTransformer", logComp, logLevel), "LogPhysicalOptimize")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(),
                *typesCtx, Config))
            .Add(CreateKqpCheckQueryTransformer(), "CheckKqlQuery")
            .AddPostTypeAnnotation()
            .AddCommonOptimization()
            .Add(CreateKqpLogOptTransformer(OptimizeCtx, *typesCtx, Config), "LogicalOptimize")
            .Add(CreateKqpPhyOptTransformer(OptimizeCtx, *typesCtx), "PhysicalOptimize")
            .Add(CreateKqpFinalizingOptTransformer(OptimizeCtx), "FinalizingOptimize")
            .Add(CreateKqpQueryPhasesTransformer(), "QueryPhases")
            .Add(CreateKqpQueryEffectsTransformer(OptimizeCtx), "QueryEffects")
            .Add(CreateKqpCheckPhysicalQueryTransformer(), "CheckKqlPhysicalQuery")
            .Build(false);

        PhysicalBuildQueryTransformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("PhysicalBuildQueryTransformer", logComp, logLevel), "LogPhysicalBuildQuery")
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(Cluster, sessionCtx->TablesPtr(), *typesCtx, Config))
            .AddPostTypeAnnotation()
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

        PhysicalRunQueryTransformer = TTransformationPipeline(typesCtx)
            .Add(CreateKqpAcquireMvccSnapshotTransformer(Gateway, TxState, TransformCtx, true), "AcquireMvccSnapshot")
            .Add(CreateKqpExecutePhysicalDataTransformer(Gateway, Cluster, TxState, TransformCtx), "ExecutePhysical")
            .Build(false);

        ScanRunQueryTransformer = TTransformationPipeline(typesCtx)
            .Add(CreateKqpCreateSnapshotTransformer(Gateway, TransformCtx, TxState), "CreateSnapshot")
            .Add(CreateKqpExecuteScanTransformer(Gateway, Cluster, TxState, TransformCtx), "ExecuteScan")
            .Add(CreateKqpReleaseSnapshotTransformer(Gateway, TxState), "ReleaseSnapshot")
            .Build(false);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareDataQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(TransformCtx->QueryCtx->Type == EKikimrQueryType::Dml);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);
        YQL_ENSURE(TransformCtx->QueryCtx->PreparingQuery);
        YQL_ENSURE(TMaybeNode<TKiDataQuery>(query));

        return PrepareQueryInternal(cluster, TKiDataQuery(query), ctx, settings);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareScanQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(TransformCtx->QueryCtx->Type == EKikimrQueryType::Scan);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);
        YQL_ENSURE(TransformCtx->QueryCtx->PreparingQuery);
        YQL_ENSURE(TMaybeNode<TKiDataQuery>(query));

        TKiDataQuery dataQuery(query);

        if (dataQuery.Results().Size() != 1) {
            ctx.AddError(YqlIssue(ctx.GetPosition(dataQuery.Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                "Scan query should have a single result set."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }
        if (dataQuery.Effects().ArgCount() > 0) {
            ctx.AddError(YqlIssue(ctx.GetPosition(dataQuery.Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                "Scan query cannot have data modifications."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        IKikimrQueryExecutor::TExecuteSettings scanSettings(settings);
        return PrepareQueryInternal(cluster, dataQuery, ctx, scanSettings);
    }

    TIntrusivePtr<TAsyncQueryResult> ExecutePreparedDataQuery(const TString& cluster, TExprNode* queryExpr,
        const NKikimrKqp::TPreparedKql& kql, TExprContext& ctx,
        const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(queryExpr->Type() == TExprNode::World);
        YQL_ENSURE(cluster == Cluster);

        PreparedRunTransformer->Rewind();

        TransformCtx->Reset();
        TransformCtx->Settings.CopyFrom(kql.GetSettings());
        TransformCtx->Settings.SetCommitTx(settings.CommitTx);
        TransformCtx->Settings.SetRollbackTx(settings.RollbackTx);
        TransformCtx->PreparedKql = &kql;

        YQL_ENSURE(TxState->Tx().EffectiveIsolationLevel);
        YQL_ENSURE(TransformCtx->Settings.GetIsolationLevel() == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);

        bool strictDml = MergeFlagValue(Config->StrictDml.Get(Cluster), settings.StrictDml);
        if (!ApplyTableOperations(kql, strictDml, ctx)) {
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        return MakeIntrusive<TAsyncRunResult>(queryExpr, ctx, *PreparedRunTransformer, *TransformCtx);
    }

    TIntrusivePtr<TAsyncQueryResult> ExecutePreparedQueryNewEngine(const TString& cluster,
        const NYql::TExprNode::TPtr& world, std::shared_ptr<const NKqpProto::TKqpPhyQuery>&& phyQuery, TExprContext& ctx,
        const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        YQL_ENSURE(cluster == Cluster);
        YQL_ENSURE(phyQuery->GetType() == NKqpProto::TKqpPhyQuery::TYPE_DATA);

        if (!Config->HasAllowKqpNewEngine()) {
            ctx.AddError(TIssue(TPosition(), "NewEngine execution is not allowed on this cluster."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(
                ctx.IssueManager.GetIssues()));
        }

        return ExecutePhysicalDataQuery(world, std::move(phyQuery), ctx, settings);
    }

    TIntrusivePtr<TAsyncQueryResult> ExecutePreparedScanQuery(const TString& cluster,
        const NYql::TExprNode::TPtr& world, std::shared_ptr<const NKqpProto::TKqpPhyQuery>&& phyQuery, TExprContext& ctx,
        const NActors::TActorId& target) override
    {
        YQL_ENSURE(cluster == Cluster);
        YQL_ENSURE(phyQuery->GetType() == NKqpProto::TKqpPhyQuery::TYPE_SCAN);

        return ExecutePhysicalScanQuery(world, std::move(phyQuery), ctx, target);
    }

private:
    bool ApplyTableOperations(const TVector<NKqpProto::TKqpTableOp>& operations,
        const TVector<NKqpProto::TKqpTableInfo>& tableInfos, bool strictDml, TExprContext& ctx)
    {
        auto isolationLevel = *TxState->Tx().EffectiveIsolationLevel;
        if (!TxState->Tx().ApplyTableOperations(operations, tableInfos, isolationLevel, strictDml, EKikimrQueryType::Dml, ctx)) {
            return false;
        }

        return true;
    }

    bool ApplyTableOperations(const NKikimrKqp::TPreparedKql& kql, bool strictDml, TExprContext& ctx) {
        TVector<NKqpProto::TKqpTableOp> operations(kql.GetOperations().begin(), kql.GetOperations().end());
        TVector<NKqpProto::TKqpTableInfo> tableInfos(kql.GetTableInfo().begin(), kql.GetTableInfo().end());
        return ApplyTableOperations(operations, tableInfos, strictDml, ctx);
    }

    bool ApplyTableOperations(const NKqpProto::TKqpPhyQuery& query, bool strictDml, TExprContext& ctx) {
        TVector<NKqpProto::TKqpTableOp> operations(query.GetTableOps().begin(), query.GetTableOps().end());
        TVector<NKqpProto::TKqpTableInfo> tableInfos(query.GetTableInfos().begin(), query.GetTableInfos().end());
        return ApplyTableOperations(operations, tableInfos, strictDml, ctx);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareQueryInternal(const TString& cluster, const TKiDataQuery& dataQuery,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings)
    {
        YQL_ENSURE(cluster == Cluster);

        auto* queryCtx = TransformCtx->QueryCtx.Get();

        if (queryCtx->Type == EKikimrQueryType::Dml) {
            ui32 resultsCount = dataQuery.Results().Size();
            for (ui32 i = 0; i < resultsCount; ++i) {
                auto& result = *queryCtx->PreparingQuery->AddResults();
                result.SetKqlIndex(0);
                result.SetResultIndex(i);
                for (const auto& column : dataQuery.Results().Item(i).Columns()) {
                    *result.AddColumnHints() = column.Value();
                }
                result.SetRowsLimit(FromString<ui64>(dataQuery.Results().Item(i).RowsLimit()));
            }
        } else {
            // scan query
        }

        bool sysColumnsEnabled = TransformCtx->Config->SystemColumnsEnabled();

        std::optional<TKqpTransactionInfo::EEngine> engine;
        if (settings.UseNewEngine.Defined()) {
            engine = *settings.UseNewEngine
                ? TKqpTransactionInfo::EEngine::NewEngine
                : TKqpTransactionInfo::EEngine::OldEngine;
        }
        if (!engine.has_value() && Config->UseNewEngine.Get().Defined()) {
            engine = Config->UseNewEngine.Get().Get()
                ? TKqpTransactionInfo::EEngine::NewEngine
                : TKqpTransactionInfo::EEngine::OldEngine;
        }
        if (!engine.has_value() && Config->HasKqpForceNewEngine()) {
            engine = TKqpTransactionInfo::EEngine::NewEngine;
        }

        if ((queryCtx->Type == EKikimrQueryType::Scan) ||
            (engine.has_value() && *engine == TKqpTransactionInfo::EEngine::NewEngine))
        {
            return PrepareQueryNewEngine(cluster, dataQuery, ctx, settings, sysColumnsEnabled);
        }

        // OldEngine only
        YQL_ENSURE(!engine.has_value() || *engine == TKqpTransactionInfo::EEngine::OldEngine);

        for (const auto& [name, table] : TransformCtx->Tables->GetTables()) {
            if (!table.Metadata->SysView.empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(dataQuery.Pos()), TStringBuilder()
                    << "Table " << table.Metadata->Name << " is a system view. "
                    << "System views are not supported by data queries."
                ));
                return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
            }
        }

        auto program = BuildKiProgram(dataQuery, *TransformCtx->Tables, ctx, sysColumnsEnabled);

        KqlOptimizeTransformer->Rewind();

        TExprNode::TPtr optimizedProgram = program.Ptr();
        auto status = InstantTransform(*KqlOptimizeTransformer, optimizedProgram, ctx);
        if (status != IGraphTransformer::TStatus::Ok || !TMaybeNode<TKiProgram>(optimizedProgram)) {
            ctx.AddError(TIssue(ctx.GetPosition(dataQuery.Pos()), "Failed to optimize KQL query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        YQL_ENSURE(optimizedProgram->GetTypeAnn());

        queryCtx->QueryTraits = CollectQueryTraits(TKiProgram(optimizedProgram), ctx);

        KqlTypeAnnTransformer->Rewind();

        TExprNode::TPtr finalProgram;
        bool hasNonDeterministicFunctions;
        TPeepholeSettings peepholeSettings;
        peepholeSettings.WithNonDeterministicRules = false;
        status = PeepHoleOptimizeNode<false>(optimizedProgram, finalProgram, ctx, TypesCtx, KqlTypeAnnTransformer.Get(),
            hasNonDeterministicFunctions, peepholeSettings);
        if (status != IGraphTransformer::TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(dataQuery.Pos()), "Failed to peephole optimize KQL query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        status = ReplaceNonDetFunctionsWithParams(finalProgram, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(dataQuery.Pos()),
                "Failed to replace non deterministic functions with params for KQL query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        KqlPrepareTransformer->Rewind();

        NKikimrKqp::TKqlSettings kqlSettings;
        kqlSettings.SetCommitTx(settings.CommitTx);
        kqlSettings.SetRollbackTx(settings.RollbackTx);

        TransformCtx->Reset();
        TransformCtx->Settings = kqlSettings;

        if (!TxState->Tx().EffectiveIsolationLevel) {
            TxState->Tx().EffectiveIsolationLevel = kqlSettings.GetIsolationLevel();
        }

        TransformCtx->QueryCtx->PreparingQuery->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_V1);
        auto kql = TransformCtx->QueryCtx->PreparingQuery->AddKqls();
        kql->MutableSettings()->CopyFrom(TransformCtx->Settings);

        FillAstAndPlan(*kql, finalProgram, ctx);

        auto operations = TableOperationsToProto(dataQuery.Operations(), ctx);
        for (auto& op : operations) {
            const auto tableName = op.GetTable();
            auto operation = static_cast<TYdbOperation>(op.GetOperation());

            *kql->AddOperations() = std::move(op);

            const auto& desc = TransformCtx->Tables->GetTable(cluster, tableName);
            TableDescriptionToTableInfo(desc, operation, *kql->MutableTableInfo());
        }

        TransformCtx->PreparingKql = kql;

        return MakeIntrusive<TAsyncRunResult>(finalProgram, ctx, *KqlPrepareTransformer, *TransformCtx);
    }

    TIntrusivePtr<TAsyncQueryResult> PrepareQueryNewEngine(const TString& cluster, const TKiDataQuery& dataQuery,
        TExprContext& ctx, const IKikimrQueryExecutor::TExecuteSettings& settings, bool sysColumnsEnabled)
    {
        YQL_ENSURE(cluster == Cluster);
        YQL_ENSURE(!settings.CommitTx);
        YQL_ENSURE(!settings.RollbackTx);
        YQL_ENSURE(TransformCtx->QueryCtx->PrepareOnly);

        EKikimrQueryType queryType = TransformCtx->QueryCtx->Type;
        switch (queryType) {
            case EKikimrQueryType::Dml:
            case EKikimrQueryType::Scan:
                break;
            default:
                YQL_ENSURE(false, "PrepareQueryNewEngine, unexpected query type: " << queryType);
        }

        if (!Config->HasAllowKqpNewEngine() && queryType == EKikimrQueryType::Dml) {
            ctx.AddError(TIssue(ctx.GetPosition(dataQuery.Pos()),
                "NewEngine execution is not allowed on this cluster."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        auto kqlQuery = BuildKqlQuery(dataQuery, *TransformCtx->Tables, ctx, sysColumnsEnabled, OptimizeCtx);
        if (!kqlQuery) {
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }

        auto query = kqlQuery->Ptr();
        YQL_CLOG(DEBUG, ProviderKqp) << "Initial KQL query: " << KqpExprToPrettyString(*query, ctx);

        TransformCtx->Reset();
        TransformCtx->Settings = NKikimrKqp::TKqlSettings();

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
        PhysicalBuildQueryTransformer->Rewind();
        auto builtQuery = optimizedQuery;
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

        auto& preparedQuery = *TransformCtx->QueryCtx->PreparingQuery;
        TKqpPhysicalQuery physicalQuery(transformedQuery);
        auto compiler = CreateKqpQueryCompiler(Cluster, OptimizeCtx->Tables, FuncRegistry);
        auto ret = compiler->CompilePhysicalQuery(physicalQuery, dataQuery.Operations(),
            *preparedQuery.MutablePhysicalQuery(), ctx);
        if (!ret) {
            ctx.AddError(TIssue(ctx.GetPosition(query->Pos()), "Failed to compile physical query."));
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }
        preparedQuery.SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
        // TODO(sk): only on stats mode or if explain-only
        PreparedExplainTransformer->Rewind();
        return MakeIntrusive<TPhysicalAsyncRunResult>(builtQuery, ctx, *PreparedExplainTransformer, *TransformCtx);
    }

    TIntrusivePtr<TAsyncQueryResult> ExecutePhysicalDataQuery(const TExprNode::TPtr& world,
        std::shared_ptr<const NKqpProto::TKqpPhyQuery>&& phyQuery, TExprContext& ctx,
        const IKikimrQueryExecutor::TExecuteSettings& settings)
    {
        PhysicalRunQueryTransformer->Rewind();

        TransformCtx->Reset();
        TransformCtx->PhysicalQuery = phyQuery;

        YQL_ENSURE(TxState->Tx().EffectiveIsolationLevel);
        YQL_ENSURE(TransformCtx->Settings.GetIsolationLevel() == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);

        // TODO: Avoid using KQL settings
        TransformCtx->Settings.SetCommitTx(settings.CommitTx);
        TransformCtx->Settings.SetRollbackTx(settings.RollbackTx);

        bool strictDml = MergeFlagValue(Config->StrictDml.Get(Cluster), settings.StrictDml);
        if (!ApplyTableOperations(*phyQuery, strictDml, ctx)) {
            return MakeKikimrResultHolder(ResultFromErrors<IKqpHost::TQueryResult>(ctx.IssueManager.GetIssues()));
        }
        return MakeIntrusive<TPhysicalAsyncRunResult>(world, ctx, *PhysicalRunQueryTransformer, *TransformCtx);
    }

    TIntrusivePtr<TAsyncQueryResult> ExecutePhysicalScanQuery(const TExprNode::TPtr& world,
        std::shared_ptr<const NKqpProto::TKqpPhyQuery>&& phyQuery, TExprContext& ctx, const NActors::TActorId& target)
    {
        ScanRunQueryTransformer->Rewind();

        TransformCtx->Reset();
        TransformCtx->PhysicalQuery = phyQuery;
        TransformCtx->ReplyTarget = target;

        Y_ASSERT(!TxState->Tx().GetSnapshot().IsValid());

        return MakeIntrusive<TScanAsyncRunResult>(world, ctx, *ScanRunQueryTransformer, *TransformCtx);
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

    TIntrusivePtr<TKqpTransactionState> TxState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TIntrusivePtr<TKqpOptimizeContext> OptimizeCtx;
    TIntrusivePtr<TKqpBuildQueryContext> BuildQueryCtx;

    TAutoPtr<IGraphTransformer> KqlTypeAnnTransformer;
    TAutoPtr<IGraphTransformer> KqlOptimizeTransformer;
    TAutoPtr<IGraphTransformer> KqlPrepareTransformer;
    TAutoPtr<IGraphTransformer> PreparedRunTransformer;
    TAutoPtr<IGraphTransformer> PreparedExplainTransformer;

    TAutoPtr<IGraphTransformer> PhysicalOptimizeTransformer;
    TAutoPtr<IGraphTransformer> PhysicalBuildQueryTransformer;
    TAutoPtr<IGraphTransformer> PhysicalPeepholeTransformer;
    TAutoPtr<IGraphTransformer> PhysicalRunQueryTransformer;
    TAutoPtr<IGraphTransformer> ScanRunQueryTransformer;
};

class TKqpAcquireMvccSnapshotTransformer : public TGraphTransformerBase {
public:
    TKqpAcquireMvccSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKqlTransformContext> transformCtx,
        TIntrusivePtr<TKqpTransactionState> txState, bool newEngine)
        : Gateway(std::move(gateway))
        , TransformCtx(std::move(transformCtx))
        , NewEngine(newEngine)
        , TxState(std::move(txState))
    {}

    TStatus DoTransform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext&) override {
        output = input;

        if (!NeedSnapshot(TxState->Tx(), *TransformCtx->Config, TransformCtx->Settings.GetRollbackTx(),
                TransformCtx->Settings.GetCommitTx(), NewEngine ? TransformCtx->PhysicalQuery.get() : nullptr,
                NewEngine ? nullptr : TransformCtx->PreparedKql)) {
            return TStatus::Ok;
        }

        auto timeout = TransformCtx->QueryCtx->Deadlines.TimeoutAt - Gateway->GetCurrentTime();
        if (!timeout) {
            // TODO: Just cancel request.
            timeout = TDuration::MilliSeconds(1);
        }
        SnapshotFuture = Gateway->AcquireMvccSnapshot(timeout);

        Promise = NewPromise();

        SnapshotFuture.Apply([promise = Promise](const TFuture<IKqpGateway::TKqpSnapshotHandle> future) mutable {
            YQL_ENSURE(future.HasValue());
            promise.SetValue();
        });

        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const NYql::TExprNode&) override {
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) override {
        output = input;

        auto handle = SnapshotFuture.ExtractValue();

        if (handle.Snapshot.IsValid()) {
            TxState->Tx().SnapshotHandle = handle;

            return TStatus::Ok;
        }

        TIssue issue("Failed to acquire snapshot");
        switch (handle.Status) {
            case NKikimrIssues::TStatusIds::SCHEME_ERROR:
                issue.SetCode(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR, NYql::TSeverityIds::S_ERROR);
                break;
            case NKikimrIssues::TStatusIds::TIMEOUT:
                issue.SetCode(NYql::TIssuesIds::KIKIMR_TIMEOUT, NYql::TSeverityIds::S_ERROR);
                break;
            case NKikimrIssues::TStatusIds::OVERLOADED:
                issue.SetCode(NYql::TIssuesIds::KIKIMR_OVERLOADED, NYql::TSeverityIds::S_ERROR);
                break;
            default:
                // Snapshot is acquired before reads or writes, so we can return UNAVAILABLE here
                issue.SetCode(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, NYql::TSeverityIds::S_ERROR);
                break;
        }

        for (const auto& subIssue: handle.Issues()) {
            issue.AddSubIssue(MakeIntrusive<TIssue>(subIssue));
        }
        ctx.AddError(issue);
        return TStatus::Error;
    }

private:

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;

    bool NewEngine;

    NThreading::TFuture<IKqpGateway::TKqpSnapshotHandle> SnapshotFuture;
    NThreading::TPromise<void> Promise;
    TIntrusivePtr<TKqpTransactionState> TxState;
};

} // namespace

TAutoPtr<NYql::IGraphTransformer> CreateKqpAcquireMvccSnapshotTransformer(TIntrusivePtr<IKqpGateway> gateway,
    TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx, bool newEngine) {
    return new TKqpAcquireMvccSnapshotTransformer(gateway, transformCtx, txState, newEngine);
}

TIntrusivePtr<IKqpRunner> CreateKqpRunner(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    TIntrusivePtr<TTypeAnnotationContext> typesCtx, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    const NMiniKQL::IFunctionRegistry& funcRegistry)
{
    return new TKqpRunner(gateway, cluster, typesCtx, sessionCtx, funcRegistry);
}

} // namespace NKqp
} // namespace NKikimr
