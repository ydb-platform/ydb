#include <ydb/core/kqp/opt/rbo/kqp_native_query.h>
#include <ydb/library/yql/providers/ydb_external/common/provider_names.h>
#include "kqp_host_impl.h"
#include "lazy_provider/kqp_lazy_provider.h"
#include "new_parser/kqp_new_parser.h"
#include "new_parser/kqp_name_resolver.h"
#include "new_parser/kqp_type_annotation.h"
#include "new_parser/rbo/kqp_rbo_lowering.h"
#include <ydb/core/kqp/tracing/kqp_compile_trace_transformer.h>
#include "kqp_statement_rewrite.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join_cbo_factory.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>
#include <ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>
#include <ydb/library/yql/providers/dq/helper/yql_dq_helper_impl.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_dq_integration.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_provider.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_message_stream.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_settings.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_dq_integration.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_provider.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_state.h>
#include <ydb/library/yql/providers/ydb_external/provider/yql_ydb_external_provider.h>

#include <yql/essentials/core/yql_opt_proposed_by_data.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/services/yql_plan.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/providers/common/arrow_resolve/yql_simple_arrow_resolver.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/providers/config/yql_config_provider.h>
#include <yql/essentials/providers/pg/provider/yql_pg_provider_impl.h>
#include <yql/essentials/providers/result/provider/yql_result_provider.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NThreading;

namespace {

void FillColumnMeta(const NKqpProto::TKqpPhyQuery& phyQuery, IKqpHost::TQueryResult& queryResult) {
    const auto& bindings = phyQuery.GetResultBindings();
    for (const auto& binding: bindings) {
        auto meta = queryResult.ResultSetsMeta.Add();
        meta->CopyFrom(binding.GetResultSetMeta());
    }
}

void AddQueryStats(NKqpProto::TKqpStatsQuery& total, NKqpProto::TKqpStatsQuery&& stats) {
    // NOTE: Do not add duration & compilation stats as they are computed for the
    // whole query in KQP worker.

    for (auto& execution : *stats.MutableExecutions()) {
        total.AddExecutions()->Swap(&execution);
    }

    total.SetWorkerCpuTimeUs(total.GetWorkerCpuTimeUs() + stats.GetWorkerCpuTimeUs());
}

bool CheckIsBatch(const TExprNode::TPtr& root, TExprContext& exprCtx) {
    ui64 writeCount = 0;
    ui64 readCount = 0;
    bool isBatch = false;

    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->ChildrenSize() == 2
            && node->Child(0)->Content() == "is_batch"
            && node->Child(1)->Content() == "true") {
            isBatch = true;
            return true;
        }

        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            writeCount++;
        } else if (NYql::NNodes::TCoRead::Match(node.Get())) {
            readCount++;
        }

        return true;
    });

    if (isBatch && (writeCount > 1 || readCount != 0)) {
        exprCtx.AddError(NYql::TIssue(
            exprCtx.GetPosition(NYql::NNodes::TExprBase(root).Pos()),
            "BATCH can't be used with multiple writes or reads."));
        return false;
    }

    return true;
}

bool HasSamplingRead(const TExprNode::TPtr& root) {
    return FindNode(root, [](const TExprNode::TPtr& node) {
        if (!TCoRead::Match(node.Get()) || node->ChildrenSize() <= TKiReadTable::idx_Settings ||
            !TMaybeNode<TKiDataSource>(node->ChildPtr(TCoRead::idx_DataSource)))
        {
            return false;
        }
        const auto& settings = *node->Child(TKiReadTable::idx_Settings);
        return HasSetting(settings, "samplingrate") || HasSetting(settings, "samplingseed") ||
            HasSetting(settings, "samplingmemtablestride");
    }) != nullptr;
}

class TKqpResultWriter : public IResultWriter {
public:
    TKqpResultWriter() {}

    bool IsDiscard() const override {
        return Discard;
    }

    void Init(bool discard, const TString& label, TMaybe<TPosition> pos, bool unordered) override {
        Discard = discard;
        Y_UNUSED(label);
        Y_UNUSED(pos);
        Y_UNUSED(unordered);
    }

    void Write(const TStringBuf& resultData) override {
        if (!Discard) {
            YQL_ENSURE(Result.empty());
            Result = resultData;
        }
    }

    void Commit(bool overflow) override {
        YQL_ENSURE(!overflow);
    }

    TStringBuf Str() override {
        return Result;
    }

    ui64 Size() override {
        return Result.size();
    }

private:
    bool Discard = false;
    TString Result;
};

struct TExecuteContext : TThrRefBase {
    TVector<IKqpHost::TQueryResult> QueryResults;
    IKikimrQueryExecutor::TExecuteSettings Settings;
    NActors::TActorId ReplyTarget;

    void Reset(const IKikimrQueryExecutor::TExecuteSettings& settings) {
        Settings = settings;
        QueryResults.clear();
    }
};

void FillAstAndPlan(IKqpHost::TQueryResult& queryResult, TExprNode* queryRoot, TExprContext& ctx, IPlanBuilder& planBuilder) {
    TStringStream astStream;
    auto ast = ConvertToAst(*queryRoot, ctx, TExprAnnotationFlags::None, true);
    ast.Root->PrettyPrintTo(astStream, TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine);
    queryResult.QueryAst = astStream.Str();

    TStringStream planStream;
    NYson::TYsonWriter writer(&planStream, NYson::EYsonFormat::Binary);
    planBuilder.Clear();
    planBuilder.WritePlan(writer, queryRoot);
    queryResult.QueryPlan = planStream.Str();
}

template<typename TResult>
class TKqpFutureResult : public IKikimrAsyncResult<TResult> {
public:
    TKqpFutureResult(const NThreading::TFuture<TResult>& future, TExprContext& ctx)
        : Future(future)
        , ExprCtx(ctx)
        , Completed(false) {}

    bool HasResult() const override {
        if (Completed) {
            YQL_ENSURE(ExtractedResult.has_value());
        }
        return Completed;
    }

    TResult GetResult() override {
        YQL_ENSURE(Completed);
        if (ExtractedResult) {
            return std::move(*ExtractedResult);
        }
        return std::move(Future.ExtractValue());
    }

    NThreading::TFuture<bool> Continue() override {
        if (Completed) {
            return NThreading::MakeFuture(true);
        }

        if (Future.HasValue()) {
            ExtractedResult.emplace(std::move(Future.ExtractValue()));
            ExtractedResult->ReportIssues(ExprCtx.IssueManager);

            Completed = true;
            return NThreading::MakeFuture(true);
        }

        return Future.Apply([](const NThreading::TFuture<TResult>& future) {
            YQL_ENSURE(future.HasValue());
            return false;
        });
    }

private:
    NThreading::TFuture<TResult> Future;
    std::optional<TResult> ExtractedResult;
    TExprContext& ExprCtx;
    bool Completed;
};

/*
 * Validate YqlScript.
 */
class TAsyncValidateYqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncValidateYqlResult(TExprNode* queryRoot, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TExprContext& exprCtx, TAutoPtr<IGraphTransformer> transformer, TMaybe<TSqlVersion> sqlVersion,
        bool keepInCache, const TMaybe<TString>& commandTagName, TAsyncTransformStatusCallback asyncTransformCallback)
        : TKqpAsyncResultBase(queryRoot, exprCtx, *transformer.Get(), asyncTransformCallback)
        , SessionCtx(sessionCtx)
        , Transformer(transformer)
        , SqlVersion(sqlVersion)
        , KeepInCache(keepInCache)
        , CommandTagName(commandTagName) {}

    void FillResult(TResult& validateResult) const override {
        if (!validateResult.Success()) {
            return;
        }

        YQL_ENSURE(SessionCtx->Query().PrepareOnly);
        validateResult.PreparedQuery.reset(SessionCtx->Query().PreparingQuery.release());
        validateResult.SqlVersion = SqlVersion;
        validateResult.AllowCache = KeepInCache;
        validateResult.CommandTagName = CommandTagName;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TAutoPtr<IGraphTransformer> Transformer;
    TMaybe<TSqlVersion> SqlVersion;
    bool KeepInCache;
    TMaybe<TString> CommandTagName;
};

/*
 * Explain Yql/YqlScript.
 */
class TAsyncExplainYqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExplainYqlResult(TExprNode* queryRoot, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TExprContext& exprCtx, TAutoPtr<IGraphTransformer> transformer, IPlanBuilder& planBuilder,
        TMaybe<TSqlVersion> sqlVersion, bool keepInCache, const TMaybe<TString>& commandTagName,
        TAsyncTransformStatusCallback asyncTransformCallback, bool useDqExplain = false)
        : TKqpAsyncResultBase(queryRoot, exprCtx, *transformer.Get(), asyncTransformCallback)
        , SessionCtx(sessionCtx)
        , Transformer(transformer)
        , PlanBuilder(planBuilder)
        , SqlVersion(sqlVersion)
        , KeepInCache(keepInCache)
        , CommandTagName(commandTagName)
        , UseDqExplain(useDqExplain) {}

    void FillResult(TResult& queryResult) const override {
        if (!queryResult.Success()) {
            return;
        }

        if (UseDqExplain) {
            TVector<const TString> plans;
            for (auto id : SessionCtx->Query().ExecutionOrder) {
                auto result = SessionCtx->Query().Results.FindPtr(id);
                if (result) {
                    plans.push_back(result->QueryPlan);
                }
            }
            queryResult.QueryPlan = SerializeScriptPlan(plans);
        } else {
            FillAstAndPlan(queryResult, GetExprRoot().Get(), GetExprContext(), PlanBuilder);
        }
        queryResult.SqlVersion = SqlVersion;
        queryResult.AllowCache = KeepInCache;
        queryResult.CommandTagName = CommandTagName;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TAutoPtr<IGraphTransformer> Transformer;
    IPlanBuilder& PlanBuilder;
    TMaybe<TSqlVersion> SqlVersion;
    bool KeepInCache;
    TMaybe<TString> CommandTagName;
    bool UseDqExplain;
};

/*
 * Execute Yql/SchemeQuery/YqlScript.
 */
class TAsyncExecuteYqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExecuteYqlResult(TExprNode* queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        const TString& cluster, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const TResultProviderConfig& resultProviderConfig, IPlanBuilder& planBuilder,
        TMaybe<TSqlVersion> sqlVersion, bool keepInCache, const TMaybe<TString>& commandTagName, TAsyncTransformStatusCallback asyncTransformCallback)
        : TKqpAsyncResultBase(queryRoot, exprCtx, transformer, asyncTransformCallback)
        , Cluster(cluster)
        , SessionCtx(sessionCtx)
        , ResultProviderConfig(resultProviderConfig)
        , PlanBuilder(planBuilder)
        , SqlVersion(sqlVersion)
        , KeepInCache(keepInCache)
        , CommandTagName(commandTagName) {}

    void FillResult(TResult& queryResult) const override {
        if (!queryResult.Success()) {
            return;
        }

        for (auto& resultStr : ResultProviderConfig.CommittedResults) {
            queryResult.Results.emplace_back(
                queryResult.ProtobufArenaPtr->Allocate<Ydb::ResultSet>());
            Ydb::ResultSet* result = queryResult.Results.back();

            if (!result->ParseFromArray(resultStr.data(), resultStr.size())) {
                queryResult = ResultFromError<TResult>("Failed to parse run result.");
                return;
            }
        }

        TVector<const TString> queryPlans;
        for (auto id : SessionCtx->Query().ExecutionOrder) {
            auto result = SessionCtx->Query().Results.FindPtr(id);
            if (result) {
                queryPlans.push_back(SerializeAnalyzePlan(result->QueryStats, SessionCtx->Config().GetEnableNewRBO()));
                AddQueryStats(queryResult.QueryStats, std::move(result->QueryStats));
            }
        }

        FillAstAndPlan(queryResult, GetExprRoot().Get(), GetExprContext(), PlanBuilder);
        queryResult.SqlVersion = SqlVersion;
        queryResult.QueryPlan = SerializeScriptPlan(queryPlans);
        queryResult.AllowCache = KeepInCache;
        queryResult.CommandTagName = CommandTagName;
    }

private:
    TString Cluster;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    const TResultProviderConfig& ResultProviderConfig;
    IPlanBuilder& PlanBuilder;
    TMaybe<TSqlVersion> SqlVersion;
    bool KeepInCache;
    TMaybe<TString> CommandTagName;
};

/*
 * Prepare ScanQuery/DataQuery by AST (when called through scripting).
 */
class TAsyncExecuteKqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExecuteKqlResult(TExprNode* queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx, TExecuteContext& executeCtx, TAsyncTransformStatusCallback asyncTransformCallback)
        : TKqpAsyncResultBase(queryRoot, exprCtx, transformer, asyncTransformCallback)
        , SessionCtx(sessionCtx)
        , ExecuteCtx(executeCtx) {}

    void FillResult(TResult& queryResult) const override {
        if (!queryResult.Success()) {
            return;
        }

        YQL_ENSURE(ExecuteCtx.QueryResults.size() == 1);
        queryResult = std::move(ExecuteCtx.QueryResults[0]);
        queryResult.QueryPlan = queryResult.PreparingQuery->GetPhysicalQuery().GetQueryPlan();

        FillColumnMeta(queryResult.PreparingQuery->GetPhysicalQuery(), queryResult);
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TExecuteContext& ExecuteCtx;
};

/*
 * Prepare ScanQuery/DataQuery.
 */
class TAsyncPrepareYqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncPrepareYqlResult(TExprNode::TPtr queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        TIntrusivePtr<TKikimrQueryContext> queryCtx, const TKqpQueryRef& query, TMaybe<TSqlVersion> sqlVersion,
        TIntrusivePtr<TKqlTransformContext> transformCtx, bool keepInCache, const TMaybe<TString>& commandTagName, TAsyncTransformStatusCallback asyncTransformCallback)
        : TKqpAsyncResultBase(std::move(queryRoot), exprCtx, transformer, asyncTransformCallback)
        , QueryCtx(queryCtx)
        , ExprCtx(exprCtx)
        , TransformCtx(transformCtx)
        , QueryText(query.Text)
        , SqlVersion(sqlVersion)
        , KeepInCache(keepInCache)
        , CommandTagName(commandTagName) {}

    void FillResult(TResult& prepareResult) const override {
        if (!prepareResult.Success()) {
            auto exprRoot = GetExprRoot();
            if (TransformCtx && TransformCtx->ExplainTransformerInput) {
                exprRoot = TransformCtx->ExplainTransformerInput;
            }
            if (exprRoot) {
                prepareResult.PreparingQuery = std::move(QueryCtx->PreparingQuery);
                prepareResult.PreparingQuery->MutablePhysicalQuery()->SetQueryAst(KqpExprToPrettyString(*exprRoot, ExprCtx));
            }
            return;
        }

        YQL_ENSURE(QueryCtx->PrepareOnly);
        YQL_ENSURE(QueryCtx->PreparingQuery);

        FillColumnMeta(QueryCtx->PreparingQuery->GetPhysicalQuery(), prepareResult);

        // TODO: it's a const function, why do we move from class members?
        prepareResult.PreparingQuery = std::move(QueryCtx->PreparingQuery);
        prepareResult.PreparingQuery->SetText(std::move(QueryText));
        prepareResult.SqlVersion = SqlVersion;

        YQL_ENSURE(prepareResult.PreparingQuery->GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
        prepareResult.QueryPlan = prepareResult.PreparingQuery->GetPhysicalQuery().GetQueryPlan();
        prepareResult.QueryAst = prepareResult.PreparingQuery->GetPhysicalQuery().GetQueryAst();

        prepareResult.NeedToSplit = false;
        prepareResult.AllowCache = TransformCtx->FrontendKeepInCache.GetOrElse(KeepInCache);
        prepareResult.CommandTagName = TransformCtx->FrontendKeepInCache ? TransformCtx->FrontendCommandTag : CommandTagName;
    }

private:
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    NYql::TExprContext& ExprCtx;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TString QueryText;
    TMaybe<TSqlVersion> SqlVersion;
    bool KeepInCache;
    TMaybe<TString> CommandTagName;
};

class TAsyncPrepareNeedToSplitYqlResult : public IKikimrAsyncResult<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    bool HasResult() const override {
        return true;
    }

    TResult GetResult() override {
        TResult result;
        result.NeedToSplit = true;
        return result;
    }

    NThreading::TFuture<bool> Continue() override {
        return NThreading::MakeFuture<bool>(true);
    }
};

class TAsyncSplitQueryResult : public TKqpAsyncResultBase<IKqpHost::TSplitResult> {
public:
    using TResult = IKqpHost::TSplitResult;

    TAsyncSplitQueryResult(
        NYql::TExprNode::TPtr expr,
        std::shared_ptr<TExprContext> exprCtxStorage,
        TExprNode::TPtr fakeWorld,
        NYql::TExprNode::TPtr inputExpr,
        TAutoPtr<NYql::IGraphTransformer> transformer,
        TIntrusivePtr<TTypeAnnotationContext> typesCtx,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const TString& cluster,
        TAsyncExecuteKqlResult::TAsyncTransformStatusCallback asyncTransformCallback)
            : TKqpAsyncResultBase(inputExpr, *exprCtxStorage, *transformer, asyncTransformCallback)
            , ExprCtxStorage(std::move(exprCtxStorage))
            , Expr(expr)
            , FakeWorld(fakeWorld)
            , Transformer(transformer)
            , TypesCtx(std::move(typesCtx))
            , SessionCtx(std::move(sessionCtx))
            , Cluster(cluster) {
        YQL_ENSURE(ExprCtxStorage && Expr && FakeWorld);
        ExprCtxStorage->Step.Done(NYql::TExprStep::ExprEval);
    }

    void FillResult(TResult& result) const override {
        if (!result.Success()) {
            return;
        }

        auto rewriteResults = RewriteExpression(Expr, *ExprCtxStorage, SessionCtx, GetExprRoot());
        if (rewriteResults.empty()) {
            result = ResultFromErrors<TResult>(ExprCtxStorage->IssueManager.GetIssues());
            return;
        }
        for (const auto& resultPart : rewriteResults) {
            YQL_CLOG(DEBUG, ProviderKqp) << "Splitted query part: " << KqpExprToPrettyString(*resultPart, *ExprCtxStorage);
        }

        result.Ctx = ExprCtxStorage;
        result.Exprs = rewriteResults;
        result.World = FakeWorld;
    }

private:
    std::shared_ptr<TExprContext> ExprCtxStorage;
    NYql::TExprNode::TPtr Expr;
    TExprNode::TPtr FakeWorld;
    TAutoPtr<NYql::IGraphTransformer> Transformer;
    TIntrusivePtr<TTypeAnnotationContext> TypesCtx;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TString Cluster;
};

class TFailExpressionEvaluation : public TSyncTransformerBase {
public:
    TFailExpressionEvaluation(EKikimrQueryType queryType)
        : QueryType(queryType)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        if (QueryType == EKikimrQueryType::Query || QueryType == EKikimrQueryType::Script) {
            return TStatus::Ok;
        }

        auto evaluateNode = FindNode(input, [](const TExprNode::TPtr& node) {
            return node->IsCallable({"EvaluateIf!", "EvaluateFor!", "EvaluateAtom"});
        });

        if (!evaluateNode)
            return TStatus::Ok;

        TStringBuilder builder;

        if (evaluateNode->Content() == "EvaluateAtom"sv)
            builder << "ATOM evaluation";
        else if (evaluateNode->Content() == "EvaluateIf!"sv)
            builder << "EVALUATE IF";
        else
            builder << "EVALUATE";

        builder << " is not supported in YDB queries.";

        ctx.AddError(
            YqlIssue(
                ctx.GetPosition(evaluateNode->Pos()),
                TIssuesIds::KIKIMR_UNSUPPORTED,
                builder
            )
        );

        return TStatus::Error;
    }
    void Rewind() final {
    }

    const EKikimrQueryType QueryType;
};

class TPrepareDataQueryAstTransformer : public TGraphTransformerBase {
public:
    TPrepareDataQueryAstTransformer(const TString& cluster, const TIntrusivePtr<TExecuteContext>& executeCtx,
        const TIntrusivePtr<TKikimrQueryContext>& queryCtx, const TIntrusivePtr<IKqpRunner>& kqpRunner)
        : Cluster(cluster)
        , ExecuteCtx(executeCtx)
        , QueryCtx(queryCtx)
        , KqpRunner(kqpRunner) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (!AsyncResult) {
            YQL_ENSURE(QueryCtx->PrepareOnly);
            YQL_ENSURE(!ExecuteCtx->Settings.CommitTx);
            YQL_ENSURE(!ExecuteCtx->Settings.RollbackTx);

            if (QueryCtx->Type == EKikimrQueryType::Scan) {
                AsyncResult = KqpRunner->PrepareScanQuery(Cluster, input.Get(), ctx, ExecuteCtx->Settings);
            } else {
                AsyncResult = KqpRunner->PrepareDataQuery(Cluster, input.Get(), ctx, ExecuteCtx->Settings);
            }
        }

        Promise = NewPromise();

        auto promise = Promise;
        AsyncResult->Continue().Apply([promise](const TFuture<bool>& future) mutable {
            YQL_ENSURE(future.HasValue());
            promise.SetValue();
        });

        return TStatus::Async;
    }

    TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return Promise.GetFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        Y_UNUSED(ctx);

        output = input;

        if (!AsyncResult->HasResult()) {
            return TStatus::Repeat;
        }

        auto queryResult = AsyncResult->GetResult();
        if (!queryResult.Success()) {
            return TStatus::Error;
        }

        IKqpHost::TQueryResult prepareResult;
        prepareResult.SetSuccess();
        prepareResult.PreparingQuery = std::move(QueryCtx->PreparingQuery);

        ExecuteCtx->QueryResults.emplace_back(std::move(prepareResult));
        return TStatus::Ok;
    }

    void Rewind() override {
        AsyncResult.Reset();
    }

private:
    TString Cluster;
    TIntrusivePtr<TExecuteContext> ExecuteCtx;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<IKqpRunner> KqpRunner;
    TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult> AsyncResult;
    TPromise<void> Promise;
};

// A resolver requests catalog IO without constructing provider Read! nodes.
struct TNativeMetadataPending {};

// Select a metadata round, the native runner, or deferred legacy compilation.
// Retry only after a metadata round completes; the selected transformer owns all
// asynchronous work until its completion.
class TNativeFrontendTransformer : public TGraphTransformerBase {
public:
    using TSelect = std::function<std::pair<IGraphTransformer*, bool>(TExprNode::TPtr&, TExprContext&)>;
    explicit TNativeFrontendTransformer(TSelect select) : Select(std::move(select)) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        if (!Selected) {
            std::tie(Selected, Retry) = Select(output, ctx);
            if (!Selected) { return TStatus::Error; }
            Selected->Rewind();
        }
        auto current = output;
        return Finish(Selected->Transform(current, output, ctx));
    }
    TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Selected->GetAsyncFuture(input);
    }
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Finish(Selected->ApplyAsyncChanges(input, output, ctx));
    }
    void Rewind() override {
        if (Selected) { Selected->Rewind(); }
        Selected = nullptr;
    }
private:
    TStatus Finish(TStatus status) {
        if (status == TStatus::Ok && Retry) {
            Selected = nullptr;
            return TStatus::Repeat;
        }
        return status;
    }
    TSelect Select;
    IGraphTransformer* Selected = nullptr;
    bool Retry = false;
};

class TPrepareNativeQueryTransformer : public TGraphTransformerBase {
public:
    TPrepareNativeQueryTransformer(TIntrusivePtr<IKqpRunner> runner, TIntrusivePtr<TKqlTransformContext> transformCtx)
        : Runner(std::move(runner)), TransformCtx(std::move(transformCtx)) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        if (!Result) { Result = Runner->PrepareNativeQuery(TransformCtx->NativeQuery, ctx); }
        Promise = NewPromise();
        Result->Continue().Apply([promise = Promise](const TFuture<bool>& future) mutable {
            try {
                future.GetValue();
                promise.SetValue();
            } catch (...) {
                promise.SetException(std::current_exception());
            }
        });
        return TStatus::Async;
    }
    TFuture<void> DoGetAsyncFuture(const TExprNode&) override { return Promise.GetFuture(); }
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) override {
        output = input;
        if (!Result->HasResult()) { return TStatus::Repeat; }
        return Result->GetResult().Success() ? TStatus::Ok : TStatus::Error;
    }
    void Rewind() override { Result.Reset(); }
private:
    TIntrusivePtr<IKqpRunner> Runner;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TIntrusivePtr<IKqpRunner::TAsyncQueryResult> Result;
    TPromise<void> Promise;
};

const TTypedUnboxedValue* ValidateParameter(const TString& name, const TTypeAnnotationNode& type,
    const TPositionHandle& posHandle, TQueryData& parameters, TExprContext& ctx)
{
    auto parameter = parameters.GetParameterUnboxedValuePtr(name);
    auto pos = ctx.GetPosition(posHandle);
    if (!parameter) {
        if (type.GetKind() == ETypeAnnotationKind::Optional) {
            NKikimrMiniKQL::TParams param;
            auto typeBuilder = NKikimr::NMiniKQL::TTypeBuilder(parameters.TypeEnv());
            NKikimr::NMiniKQL::TType* result = NYql::NCommon::BuildType(posHandle, type, typeBuilder);
            auto guard = parameters.TypeEnv().BindAllocator();
            parameters.AddUVParam(name, result, NYql::NUdf::TUnboxedValuePod());
            return parameters.GetParameterUnboxedValuePtr(name);
        }

        ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_REQUEST,
            TStringBuilder() << "Missing value for parameter: " << name));
        return nullptr;
    }

    const TTypeAnnotationNode* actualType;
    {
        TIssueScopeGuard issueScope(ctx.IssueManager, [pos, name]() {
            return MakeIntrusive<TIssue>(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
                << "Failed to parse parameter type: " << name));
        });

        actualType = ConvertMiniKQLType(pos, parameter->first, ctx);
        if (!actualType) {
            return nullptr;
        }
    }

    if (!IsSameAnnotation(*actualType, type)) {
        ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Parameter " << name
            << " type mismatch, expected: " << type << ", actual: " << *actualType));
        return nullptr;
    }

    return parameter;
}

// EvaluateExpr nodes containing parameter references cannot be evaluated at compile time
// because parameter values are not available in the evaluation context for DDL queries.
// However, secret values can be set in SQL statements with parameters.
// This transformer unwraps the first EvaluateExpr node, if it's presented, before expression evaluation
// so the existing parameter-at-execution-time flow could handle them.
class TSecretValueExprTransformer {
public:
    TSecretValueExprTransformer(TIntrusivePtr<TKikimrQueryContext> queryCtx)
        : QueryCtx(queryCtx) {}

    IGraphTransformer::TStatus operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = false;

        auto& queryCtx = QueryCtx;
        return OptimizeExpr(input, output,
            [&queryCtx](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                if (!IsSecretStatement(*node)) {
                    return node;
                }

                const auto& settings = *node->Child(WriteSettingsChildIndex);
                const auto [evaluateExprNode, evaluateExprNodeIdx] = FindFirstEvaluateExprInValueExpr(settings);
                if (!evaluateExprNode) {
                    return node;
                }

                if (!evaluateExprNode->Head().IsCallable("Parameter")) {
                    ctx.AddError(YqlIssue(ctx.GetPosition(evaluateExprNode->Pos()),
                        TIssuesIds::KIKIMR_BAD_REQUEST,
                        "Only string or single string parameter is supported as secret value"));
                    return {};
                }

                const TString paramName(evaluateExprNode->Head().Child(0)->Content());
                RegisterParameterIfNeeded(queryCtx, paramName);

                return UnwrapEvaluateExpr(*node, settings, *evaluateExprNode, evaluateExprNodeIdx, ctx);
            }, ctx, optSettings);
    }

    static TAutoPtr<IGraphTransformer> Sync(TIntrusivePtr<TKikimrQueryContext> queryCtx) {
        return CreateFunctorTransformer(TSecretValueExprTransformer(queryCtx));
    }

private:
    static bool IsSecretStatement(const TExprNode& node) {
        if (!node.IsCallable("Write!") || node.ChildrenSize() <= WriteSettingsChildIndex) {
            return false;
        }
        const auto& key = *node.Child(2);
        return key.ChildrenSize() > 0 &&
               key.Child(0)->ChildrenSize() > 0 &&
               key.Child(0)->Child(0)->Content() == "secret";
    }

    static std::pair<const TExprNode*, ui32> FindFirstEvaluateExprInValueExpr(const TExprNode& settings) {
        for (ui32 i = 0; i < settings.ChildrenSize(); ++i) {
            const auto& node = *settings.Child(i);
            if (node.ChildrenSize() < 2 || node.Child(0)->Content() != "value_expr") {
                continue;
            }
            const auto* expr = node.Child(1);
            if (expr->IsCallable("EvaluateExpr") && expr->ChildrenSize() == 1) {
                return {expr, i};
            }

            break; // We need only the first EvaluateExpr
        }
        return {nullptr, 0};
    }

    static void RegisterParameterIfNeeded(
        TIntrusivePtr<TKikimrQueryContext>& queryCtx,
        const TString& paramName)
    {
        if (!queryCtx->PrepareOnly || !queryCtx->PreparingQuery) {
            return;
        }
        for (const auto& param : queryCtx->PreparingQuery->GetParameters()) {
            if (param.GetName() == paramName) {
                return;
            }
        }
        auto& paramDesc = *queryCtx->PreparingQuery->AddParameters();
        paramDesc.SetName(paramName);
        auto guard = queryCtx->QueryData->TypeEnv().BindAllocator();
        auto* utf8Type = NKikimr::NMiniKQL::TDataType::Create(
            NYql::NUdf::TDataType<NYql::NUdf::TUtf8>::Id,
            queryCtx->QueryData->TypeEnv());
        NKikimr::NMiniKQL::ExportTypeToProto(utf8Type, *paramDesc.MutableType());
    }

    static TExprNode::TPtr UnwrapEvaluateExpr(
        const TExprNode& writeNode, const TExprNode& settings, const TExprNode& evaluateExprNode,
        const ui32 evaluateExprNodeIdx, TExprContext& ctx)
    {
        auto newTuple = ctx.ChangeChild(*settings.Child(evaluateExprNodeIdx), 1, evaluateExprNode.HeadPtr());
        auto newSettings = ctx.ChangeChild(settings, evaluateExprNodeIdx, std::move(newTuple));
        return ctx.ChangeChild(writeNode, WriteSettingsChildIndex, std::move(newSettings));
    }

private:
    static constexpr ui32 WriteSettingsChildIndex = 4;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
};

class TCollectParametersTransformer {
public:
    TCollectParametersTransformer(TIntrusivePtr<TKikimrQueryContext> queryCtx)
        : QueryCtx(queryCtx) {}

    IGraphTransformer::TStatus operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (QueryCtx->PrepareOnly && QueryCtx->PreparingQuery->ParametersSize() > 0) {
            return IGraphTransformer::TStatus::Ok;
        }

        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = false;

        auto& queryCtx = QueryCtx;
        auto status = OptimizeExpr(input, output,
            [&queryCtx](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
                auto ret = input;
                TExprBase node(input);

                if (auto maybeParameter = node.Maybe<TCoParameter>()) {
                    auto parameter = maybeParameter.Cast();
                    auto name = parameter.Name().Value();
                    auto expectedType = parameter.Ref().GetTypeAnn();

                    if (queryCtx->PrepareOnly) {
                        auto& paramDesc = *queryCtx->PreparingQuery->AddParameters();
                        paramDesc.SetName(TString(name));

                        auto typeBuilder = NKikimr::NMiniKQL::TTypeBuilder(queryCtx->QueryData->TypeEnv());
                        NKikimr::NMiniKQL::TType* result = NYql::NCommon::BuildType(parameter.Pos(), *expectedType, typeBuilder);

                        if (!result) {
                            ctx.AddError(TIssue(ctx.GetPosition(parameter.Pos()), TStringBuilder()
                                << "Failed to export parameter type: " << name));
                            return nullptr;
                        }

                        ExportTypeToProto(result, *paramDesc.MutableType());

                        return ret;
                    }

                    auto parameterValue = ValidateParameter(TString(name), *expectedType, parameter.Pos(),
                        *(queryCtx->QueryData), ctx);
                    if (!parameterValue) {
                        return nullptr;
                    }

                    if (queryCtx->Type == EKikimrQueryType::YqlScript ||
                        queryCtx->Type == EKikimrQueryType::YqlScriptStreaming)
                    {
                        return ret;
                    }

                    TExprNode::TPtr valueExpr;
                    {
                        TIssueScopeGuard issueScope(ctx.IssueManager, [parameter, name, &ctx]() {
                            return MakeIntrusive<TIssue>(YqlIssue(ctx.GetPosition(parameter.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST,
                                TStringBuilder() << "Failed to parse parameter value: " << name));
                        });

                        valueExpr = NYql::NCommon::ValueToExprLiteral(expectedType, parameterValue->second, ctx, parameter.Pos());
                    }

                    if (!valueExpr) {
                        return nullptr;
                    }

                    ret = valueExpr;
                }

                return ret;
            }, ctx, optSettings);

        return status;
    }

    static TAutoPtr<IGraphTransformer> Sync(TIntrusivePtr<TKikimrQueryContext> queryCtx) {
        return CreateFunctorTransformer(TCollectParametersTransformer(queryCtx));
    }

private:
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
};


template <typename TResult>
TResult SyncProcess(TIntrusivePtr<IKikimrAsyncResult<TResult>> asyncResult) {
    if (asyncResult->HasResult()) {
        return asyncResult->GetResult();
    }

    for (;;) {
        auto future = asyncResult->Continue();
        future.Wait();
        bool finished = future.GetValue();
        if (finished) {
            return asyncResult->GetResult();
        }
    }
}

template<typename TResult, typename TLambda>
TIntrusivePtr<IKikimrAsyncResult<TResult>> CheckedProcess(TExprContext& ctx, TLambda&& getResultFunc) {
    try {
        auto asyncResult = getResultFunc(ctx);
        return asyncResult
            ? asyncResult
            : MakeKikimrResultHolder(ResultFromErrors<TResult>(ctx.IssueManager.GetIssues()));
    }
    catch (const std::exception& e) {
        return MakeKikimrResultHolder(ResultFromException<TResult>(e));
    }
}

template<typename TResult, typename TLambda>
TResult CheckedSyncProcess(TLambda&& getResultFunc) {
    try {
        auto asyncResult = getResultFunc();
        return SyncProcess(asyncResult);
    }
    catch (const std::exception& e) {
        return ResultFromException<TResult>(e);
    }
}

template<typename TLambda>
IKqpHost::IAsyncQueryResultPtr CheckedProcessQuery(TExprContext& ctx, TLambda&& getResultFunc) {
    return CheckedProcess<IKqpHost::TQueryResult>(ctx, getResultFunc);
}

template<typename TLambda>
IKqpHost::TQueryResult CheckedSyncProcessQuery(TLambda&& getResultFunc) {
    return CheckedSyncProcess<IKqpHost::TQueryResult>(getResultFunc);
}

class TKqpQueryExecutor : public IKikimrQueryExecutor {
public:
    TKqpQueryExecutor(const TIntrusivePtr<IKqpGateway>& gateway, const TString& cluster,
        const TIntrusivePtr<TKikimrSessionContext>& sessionCtx, const TIntrusivePtr<IKqpRunner>& kqpRunner)
        : Gateway(gateway)
        , Cluster(cluster)
        , SessionCtx(sessionCtx)
        , KqpRunner(kqpRunner) {}

    TIntrusivePtr<TAsyncQueryResult> ExecuteDataQuery(const TString& cluster, const TExprNode::TPtr& query,
        TExprContext& ctx, const TExecuteSettings& settings) override
    {
        auto queryType = SessionCtx->Query().Type;

        YQL_ENSURE(!settings.UseScanQuery ||
                   queryType == EKikimrQueryType::YqlScript ||
                   queryType == EKikimrQueryType::YqlScriptStreaming);

        if (SessionCtx->Query().PrepareOnly) {
            switch (queryType) {
                case EKikimrQueryType::Dml:
                    return KqpRunner->PrepareDataQuery(cluster, query, ctx, settings);
                case EKikimrQueryType::Scan:
                    return KqpRunner->PrepareScanQuery(cluster, query, ctx, settings);
                case EKikimrQueryType::Query:
                case EKikimrQueryType::Script:
                    return KqpRunner->PrepareQuery(cluster, query, ctx, settings);
                case EKikimrQueryType::YqlScript:
                case EKikimrQueryType::YqlScriptStreaming:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected query type for prepare action: " << queryType);
                    return nullptr;
            }
        }

        switch (queryType) {
            case EKikimrQueryType::YqlScript:
            case EKikimrQueryType::YqlScriptStreaming:
            {
                YQL_ENSURE(TMaybeNode<TKiDataQueryBlocks>(query));
                TKiDataQueryBlocks dataQueryBlocks(query);

                auto queryAstStr = SerializeExpr(ctx, *query);
                TMaybe<TString> traceId = SessionCtx->GetUserRequestContext() ? SessionCtx->GetUserRequestContext()->TraceId : TMaybe<TString>{};

                bool useGenericQuery = ShouldUseGenericQuery(dataQueryBlocks);
                bool useScanQuery = ShouldUseScanQuery(dataQueryBlocks, settings);

                IKqpGateway::TAstQuerySettings querySettings;
                querySettings.IsInternalCall = SessionCtx->Query().IsInternalCall;
                querySettings.CollectStats = GetStatsMode(settings.StatsMode);

                TFuture<TQueryResult> future;
                switch (queryType) {
                case EKikimrQueryType::YqlScript:
                    if (useGenericQuery) {
                        Ydb::Table::TransactionSettings txSettings;
                        txSettings.mutable_serializable_read_write();
                        if (SessionCtx->Query().PrepareOnly) {
                            future = Gateway->ExplainGenericQuery(Cluster, SessionCtx->Query().PreparingQuery->GetText());
                        } else {
                            future = Gateway->ExecGenericQuery(Cluster, SessionCtx->Query().PreparingQuery->GetText(), CollectParameters(query),
                                querySettings, txSettings, traceId);
                        }
                    } else if (useScanQuery) {
                        ui64 rowsLimit = 0;
                        if (dataQueryBlocks.ArgCount() && !dataQueryBlocks.Arg(0).Results().Empty()) {
                            const auto& queryBlock = dataQueryBlocks.Arg(0);
                            rowsLimit = FromString<ui64>(queryBlock.Results().Item(0).RowsLimit());
                        }

                        if (SessionCtx->Query().PrepareOnly) {
                            future = Gateway->ExplainScanQueryAst(Cluster, queryAstStr);
                        } else {
                            future = Gateway->ExecScanQueryAst(Cluster, queryAstStr, CollectParameters(query),
                                querySettings, rowsLimit);
                        }
                    } else {
                        Ydb::Table::TransactionSettings txSettings;
                        txSettings.mutable_serializable_read_write();
                        if (SessionCtx->Query().PrepareOnly) {
                            future = Gateway->ExplainDataQueryAst(Cluster, queryAstStr);
                        } else {
                            future = Gateway->ExecDataQueryAst(Cluster, queryAstStr, CollectParameters(query),
                                querySettings, txSettings, traceId);
                        }
                    }
                    break;
                case EKikimrQueryType::YqlScriptStreaming:
                    if (useGenericQuery) {
                        Ydb::Table::TransactionSettings txSettings;
                        txSettings.mutable_serializable_read_write();

                        future = Gateway->StreamExecGenericQuery(Cluster, SessionCtx->Query().PreparingQuery->GetText(), CollectParameters(query),
                                querySettings, txSettings, SessionCtx->Query().ReplyTarget, traceId);
                    } else if (useScanQuery) {
                        future = Gateway->StreamExecScanQueryAst(Cluster, queryAstStr, CollectParameters(query),
                            querySettings, SessionCtx->Query().ReplyTarget, SessionCtx->Query().RpcCtx);
                    } else {
                        Ydb::Table::TransactionSettings txSettings;
                        txSettings.mutable_serializable_read_write();

                        future = Gateway->StreamExecDataQueryAst(Cluster, queryAstStr, CollectParameters(query),
                            querySettings, txSettings, SessionCtx->Query().ReplyTarget, traceId);
                    }
                    break;

                default:
                    YQL_ENSURE(false, "Unexpected query type for execute action: " << queryType);
                    return nullptr;
                }

                return MakeIntrusive<TKqpFutureResult<TQueryResult>>(future, ctx);
            }

            default:
                YQL_ENSURE(false, "Unexpected query type for execute script action: " << queryType);
                return nullptr;
        }
    }

    TIntrusivePtr<TAsyncQueryResult> ExplainDataQuery(const TString&, const TExprNode::TPtr&, TExprContext&) override {
        YQL_ENSURE(false, "Not implemented.");
        return nullptr;
    }

private:
    TQueryData::TPtr CollectParameters(const TExprNode::TPtr& query) {
        TQueryData::TPtr result = std::make_shared<TQueryData>(
            AppData()->FunctionRegistry, AppData()->TimeProvider, AppData()->RandomProvider);

        auto queryCtx = SessionCtx->QueryPtr();
        VisitExpr(query, [queryCtx, &result] (const TExprNode::TPtr& exprNode) {
            if (auto parameter = TMaybeNode<TCoParameter>(exprNode)) {
                TString name(parameter.Cast().Name().Value());
                auto paramValue = queryCtx->QueryData->GetParameterTypedValue(name);
                YQL_ENSURE(paramValue);
                result->AddTypedValueParam(name, *paramValue);
            }

            return true;
        });

        return result;
    }

    bool ShouldUseGenericQuery(const TKiDataQueryBlocks& queryBlocks) {
        const auto& queryBlock = queryBlocks.Arg(0);

        bool hasFederatedSorcesOrSinks = false;
        VisitExpr(queryBlock.Ptr(), [&hasFederatedSorcesOrSinks](const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            hasFederatedSorcesOrSinks = hasFederatedSorcesOrSinks
                || node.Maybe<TS3DataSource>()
                || node.Maybe<TS3DataSink>()
                || node.Maybe<TYtDSource>()
                || node.Maybe<TYtDSink>()
                || node.Maybe<TGenDataSource>()
                || (node.Maybe<TCoDataSource>() && node.Cast<TCoDataSource>().Category().Value() == YdbExternalProviderName);

            return !hasFederatedSorcesOrSinks;
        });

        return hasFederatedSorcesOrSinks;
    }

    bool ShouldUseScanQuery(const TKiDataQueryBlocks& queryBlocks, const TExecuteSettings& settings) {
        if (settings.UseScanQuery) {
            return *settings.UseScanQuery;
        }

        if (queryBlocks.ArgCount() != 1) {
            // Don't use ScanQuery for muiltiple blocks query
            return false;
        }

        const auto& queryBlock = queryBlocks.Arg(0);

        if (queryBlock.Effects().ArgCount() > 0) {
            // Do not use ScanQuery for queries with effects.
            return false;
        }

        if (queryBlock.Results().Size() != 1) {
            // Do not use ScanQuery for queries with multiple result sets.
            return false;
        }

        if (queryBlock.Operations().Empty()) {
            // Do not use ScanQuery for pure queries.
            return false;
        }

        for (const auto& operation : queryBlock.Operations()) {
            auto& tableData = SessionCtx->Tables().ExistingTable(operation.Cluster(), operation.Table());
            if (tableData.Metadata->IsOlap() || tableData.Metadata->Kind == EKikimrTableKind::SysView) {
                // Always use ScanQuery for queries with OLAP and system tables.
                return true;
            }
        }

        bool hasIndexReads = false;
        bool hasJoins = false;
        VisitExpr(queryBlock.Results().Ptr(), [&hasIndexReads, &hasJoins] (const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            if (auto read = node.Maybe<TKiReadTable>()) {
                if (const auto& tableKey = read.TableKey().Maybe<TVarArgCallable<TExprBase>>()) {
                    if (tableKey.Cast().ArgCount() > 1) {
                        if (auto list = tableKey.Arg(1).Maybe<TExprList>()) {
                            bool hasViews = std::any_of(list.Cast().begin(), list.Cast().end(),
                                [](const TExprBase& item) {
                                    return item.Maybe<TCoAtom>() && item.Cast<TCoAtom>().Value() == "view";
                                });

                            hasIndexReads = hasIndexReads || hasViews;
                        }
                    }
                }

                return false;
            }

            if (node.Maybe<TCoEquiJoin>()) {
                hasJoins = true;
            }

            return true;
        });

        if (hasJoins) {
            // Temporarily disable implicit ScanQuery usage for queries with joins. (KIKIMR-13343)
            return false;
        }

        if (hasIndexReads) {
            // Temporarily disable implicit ScanQuery usage for queries with index reads. (KIKIMR-13295)
            return false;
        }

        return true;
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TIntrusivePtr<IKqpRunner> KqpRunner;
};

class TKqpHost : public IKqpHost {
public:
    struct TCompileExprResult {
        TExprNode::TPtr QueryExpr;
        bool KeepInCache = false;
        bool NeedToSplit = false;
        TMaybe<TString> CommandTagName;
    };

    TKqpHost(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster, const TString& database, const TGUCSettings::TPtr& gUCSettings,
        const TMaybe<TString>& applicationName, TKikimrConfiguration::TPtr config, IModuleResolver::TPtr moduleResolver,
        std::optional<TKqpFederatedQuerySetup> federatedQuerySetup, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry, bool keepConfigChanges, bool isInternalCall,
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr, NActors::TActorSystem* actorSystem = nullptr,
        NYql::TExprContext* ctx = nullptr, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig = NKikimrConfig::TQueryServiceConfig(),
        const TIntrusivePtr<TUserRequestContext>& userRequestContext = nullptr,
        bool usePessimisticLocks = false)
        : Gateway(gateway)
        , Cluster(cluster)
        , GUCSettings(gUCSettings)
        , ApplicationName(applicationName)
        , ExprCtxStorage(ctx ? nullptr : new TExprContext())
        , ExprCtx(ctx ? ctx : ExprCtxStorage.get())
        , ModuleResolver(moduleResolver)
        , KeepConfigChanges(keepConfigChanges)
        , IsInternalCall(isInternalCall)
        , FederatedQuerySetup(federatedQuerySetup)
        , Config(config)
        , TypesCtx(MakeIntrusive<TTypeAnnotationContext>())
        , PlanBuilder(CreatePlanBuilder(*TypesCtx))
        , FakeWorld(ctx ? nullptr : ExprCtx->NewWorld(TPosition()))
        , ExecuteCtx(MakeIntrusive<TExecuteContext>())
        , ActorSystem(actorSystem ? actorSystem : NActors::TActivationContext::ActorSystem())
        , QueryServiceConfig(queryServiceConfig)
        , UsePessimisticLocks(usePessimisticLocks)
    {
        if (funcRegistry) {
            FuncRegistry = funcRegistry;
        } else {
            FuncRegistryHolder = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
            FuncRegistry = FuncRegistryHolder.Get();
        }

        SessionCtx = MakeIntrusive<TKikimrSessionContext>(FuncRegistry, config, TAppData::TimeProvider, TAppData::RandomProvider, userToken, nullptr, userRequestContext);

        TypesCtx->LangVer = config->GetDefaultLangVer();
        TypesCtx->BackportMode = config->GetYqlBackportMode();
        SessionCtx->SetDatabase(database);
        SessionCtx->SetDatabaseId(Gateway->GetDatabaseId());
        SessionCtx->SetCluster(cluster);
        if (tempTablesState) {
            SessionCtx->SetTempTables(std::move(tempTablesState));
        }

        if (FederatedQuerySetup) {
            const auto& hostnamePatterns = QueryServiceConfig.GetHostnamePatterns();
            const auto& availableExternalDataSources = QueryServiceConfig.GetAvailableExternalDataSources();
            std::set<NYql::EDatabaseType> availableTypes;
            for (const auto& type : availableExternalDataSources) {
                // YdbTopics is a legacy configuration alias, not a valid EDS type.
                if (type == "YdbTopics") {
                    availableTypes.insert(NYql::EDatabaseType::Ydb);
                } else if (const auto databaseType = NYql::DatabaseTypeFromString(type)) {
                    availableTypes.insert(*databaseType);
                } else {
                    YDB_LOG_WARN_COMP(NKikimrServices::KQP_GATEWAY, "Unknown external data source type, ignoring it",
                        {"sourceType", type});
                }
            }
            ExternalSourceFactory = NExternalSource::CreateExternalSourceFactory(std::vector<TString>(hostnamePatterns.begin(), hostnamePatterns.end()),
                                                                                 ActorSystem,
                                                                                 FederatedQuerySetup->S3GatewayConfig.GetGeneratorPathsLimit(),
                                                                                 FederatedQuerySetup ? FederatedQuerySetup->CredentialsFactory : nullptr,
                                                                                 Config->FeatureFlags.GetEnableExternalSourceSchemaInference(),
                                                                                 FederatedQuerySetup->S3GatewayConfig.GetAllowLocalFiles(),
                                                                                 QueryServiceConfig.GetAllExternalDataSourcesAreAvailable(),
                                                                                 availableTypes);
        }
    }

    IAsyncQueryResultPtr ExecuteSchemeQuery(const TKqpQueryRef& query, bool isSql, const TExecSettings& settings) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql, settings] (TExprContext& ctx) {
                return ExecuteSchemeQueryInternal(query, isSql, settings, ctx);
            });
    }

    TQueryResult SyncExecuteSchemeQuery(const TKqpQueryRef& query, bool isSql, const TExecSettings& settings) override {
        return CheckedSyncProcessQuery(
            [this, &query, isSql, settings] () {
                return ExecuteSchemeQuery(query, isSql, settings);
            });
    }

    IAsyncQueryResultPtr ExplainDataQuery(const TKqpQueryRef& query, bool isSql) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql] (TExprContext& ctx) {
                return ExplainDataQueryInternal(query, isSql, ctx);
            });
    }

    IAsyncQueryResultPtr ExplainScanQuery(const TKqpQueryRef& query, bool isSql) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql] (TExprContext& ctx) {
                return ExplainScanQueryInternal(query, isSql, ctx);
            });
    }

    TQueryResult SyncExplainDataQuery(const TKqpQueryRef& query, bool isSql) override {
        return CheckedSyncProcessQuery(
            [this, &query, isSql] () {
                return ExplainDataQuery(query, isSql);
            });
    }

    IAsyncQueryResultPtr PrepareDataQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, settings] (TExprContext& ctx) mutable {
                return PrepareDataQueryInternal(query, settings, ctx);
            });
    }

    IAsyncQueryResultPtr PrepareDataQueryAst(const TKqpQueryRef& query, const TPrepareSettings& settings) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, settings] (TExprContext& ctx) mutable {
                return PrepareDataQueryAstInternal(query, settings, ctx);
            });
    }

    TQueryResult SyncPrepareDataQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) override {
        return CheckedSyncProcessQuery(
            [this, &query, settings] () mutable {
                return PrepareDataQuery(query, settings);
            });
    }

    IAsyncQueryResultPtr PrepareGenericQuery(const TKqpQueryRef& query, const TPrepareSettings& settings, NYql::TExprNode::TPtr expr = nullptr) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, settings, expr] (TExprContext& ctx) mutable {
                return PrepareQueryInternal(query, expr, EKikimrQueryType::Query, settings, ctx);
            });
    }

    IAsyncQueryResultPtr PrepareGenericScript(const TKqpQueryRef& query, const TPrepareSettings& settings, NYql::TExprNode::TPtr expr = nullptr) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, settings, expr] (TExprContext& ctx) mutable {
                return PrepareQueryInternal(query, expr, EKikimrQueryType::Script, settings, ctx);
            });
    }

    IAsyncQueryResultPtr ExecuteYqlScript(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const TExecScriptSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script, parameters, settings] (TExprContext& ctx) mutable {
                return ExecuteYqlScriptInternal(script, parameters, settings, ctx);
            });
    }

    TQueryResult SyncExecuteYqlScript(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const TExecScriptSettings& settings) override
    {
        return CheckedSyncProcessQuery(
            [this, &script, parameters, settings] () mutable {
                return ExecuteYqlScript(script, parameters, settings);
            });
    }

    IAsyncQueryResultPtr StreamExecuteYqlScript(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const NActors::TActorId& target, const TExecScriptSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script, parameters, target, settings](TExprContext& ctx) mutable {
            return StreamExecuteYqlScriptInternal(script, parameters, target, settings, ctx);
        });
    }

    IAsyncQueryResultPtr ValidateYqlScript(const TKqpQueryRef& script) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script](TExprContext& ctx) mutable {
            return ValidateYqlScriptInternal(script, ctx);
        });
    }

    TQueryResult SyncValidateYqlScript(const TKqpQueryRef& script) override {
        return CheckedSyncProcessQuery(
            [this, &script]() mutable {
            return ValidateYqlScript(script);
        });
    }

    IAsyncQueryResultPtr ExplainYqlScript(const TKqpQueryRef& script) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script] (TExprContext& ctx) mutable {
                return ExplainYqlScriptInternal(script, ctx);
            });
    }

    TQueryResult SyncExplainYqlScript(const TKqpQueryRef& script) override {
        return CheckedSyncProcessQuery(
            [this, &script] () mutable {
                return ExplainYqlScript(script);
            });
    }

    IAsyncQueryResultPtr PrepareScanQuery(const TKqpQueryRef& query, bool isSql, const TPrepareSettings& settings) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql, settings] (TExprContext& ctx) mutable {
                return PrepareScanQueryInternal(query, isSql, ctx, settings);
            });
    }

    TQueryResult SyncPrepareScanQuery(const TKqpQueryRef& query, bool isSql, const TPrepareSettings& settings) override {
        return CheckedSyncProcessQuery(
            [this, &query, isSql, settings] () mutable {
                return PrepareScanQuery(query, isSql, settings);
            });
    }

    IAsyncSplitcResultPtr SplitQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) override {
        return CheckedProcess<IKqpHost::TSplitResult>(*ExprCtx,
            [this, &query, settings] (TExprContext& ctx) mutable {
                return SplitQueryInternal(query, settings, ctx);
            });
    }

private:
    TCompileExprResult CompileQuery(const TKqpQueryRef& query, bool isSql, TExprContext& ctx, TMaybe<TSqlVersion>& sqlVersion,
        TKqpTranslationSettingsBuilder& settingsBuilder) const
    {
        TCompileExprResult result;
        std::shared_ptr<NYql::TAstParseResult> queryAst;
        // Native parsing and deferred fallback share effective translation
        // settings, including parameters and table path prefixes.
        if (!query.AstResult || (isSql && SessionCtx->Config().GetEnableNewYqlParser())) {
            settingsBuilder.SetKqpTablePathPrefix(SessionCtx->Config()._KqpTablePathPrefix.Get().GetRef())
                .SetIsEnableExternalDataSources(SessionCtx->Config().FeatureFlags.GetEnableExternalDataSources())
                .SetQueryParameters(query.ParameterTypes)
                .SetApplicationName(ApplicationName)
                .SetFromConfig(SessionCtx->Config());
        }

        // Build the owning syntax AST early, before asynchronous catalog loading.
        // A syntax error is final; only explicit unsupported-feature exceptions
        // permit fallback. Semantic passes below run once metadata is available.
        // A supplied AST can describe one statement of a larger SQL script.
        // Preserve that caller-selected statement instead of reparsing the full text.
        if (isSql && !query.AstResult && !LegacyFrontendRequested && SessionCtx->Config().GetEnableNewYqlParser()) {
            TCompileTraceScope traceScope(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Phase,
                "Parse query with new YQL frontend");
            try {
                const auto translationSettings = settingsBuilder.Build(ctx);
                auto parsed = ParseQueryWithNewParser(query.Text, translationSettings,
                    settingsBuilder.GetIsAmbiguityError(), SessionCtx->Query().CompileTrace.Get());
                traceScope.SetSuccess(parsed.IsOk());
                if (!parsed.IsOk()) {
                    YQL_CLOG(INFO, ProviderKqp) << "New YQL parser reported syntax errors";
                    ctx.IssueManager.AddIssues(parsed.Issues);
                    return result;
                }
                NewParserSettings = {translationSettings.DefaultCluster, translationSettings.PathPrefix,
                    translationSettings.ClusterPathPrefixes};
                NewParserTypeSettings = {};
                // Forward the expression settings supported by the new scalar path.
                // Pragmas with unsupported semantics are rejected by subsequent passes,
                // so their effects cannot be silently lost by using default settings.
                NewParserTypeSettings.Expressions.UnicodeLiterals = translationSettings.UnicodeLiterals;
                NewParserTypeSettings.Expressions.FlexibleTypes = !translationSettings.Flags.contains("DisableFlexibleTypes");
                if (translationSettings.Flags.contains("AnsiInForEmptyOrNullableItemsCollections")) {
                    NewParserTypeSettings.Expressions.AnsiIn = true;
                } else if (translationSettings.Flags.contains("DisableAnsiInForEmptyOrNullableItemsCollections")) {
                    NewParserTypeSettings.Expressions.AnsiIn = false;
                }
                if (query.ParameterTypes) {
                    for (const auto& [name, proto] : *query.ParameterTypes) {
                        const auto* type = ParseTypeFromYdbType(proto, ctx);
                        if (!type) {
                            return result;
                        }
                        // SDK parameter keys may omit the SQL sigil; scalar bindings use
                        // the exact $-prefixed convention. Types belong to this compile context.
                        NewParserTypeSettings.Parameters[name.StartsWith('$') ? name : "$" + name] = type;
                    }
                }
                NewParserAst = std::move(parsed.Ast);
                SessionCtx->Query().TranslationSettings = translationSettings;
                YQL_CLOG(INFO, ProviderKqp) << "New YQL AST construction succeeded";
                if (SessionCtx->Config().GetEnableNewRBO() && SessionCtx->Query().PrepareOnly
                    && IsIn({EKikimrQueryType::Dml, EKikimrQueryType::Query, EKikimrQueryType::Scan}, SessionCtx->Query().Type)) {
                    NativeFrontendActive = true;
                    // Own text and settings across asynchronous metadata requests.
                    LegacyCompile = [this, text = TString(query.Text), parameters = query.ParameterTypes,
                        ast = query.AstResult, builder = settingsBuilder, version = sqlVersion](TExprContext& context) mutable {
                        LegacyFrontendRequested = true;
                        NativeFrontendActive = false;
                        auto result = CompileYqlQuery(TKqpQueryRef(text, parameters, ast), true, context, version, builder);
                        TransformCtx->FrontendKeepInCache = result.KeepInCache;
                        TransformCtx->FrontendCommandTag = result.CommandTagName;
                        return result.QueryExpr;
                    };
                    result.QueryExpr = ctx.NewWorld(TPositionHandle());
                    result.KeepInCache = true;
                    result.CommandTagName = "SELECT";
                    for (const auto& statement : NewParserAst->Statements) {
                        if (const auto* write = std::get_if<NNewYql::TWriteStatement>(&statement.Value)) {
                            switch (write->Kind) {
                                case NNewYql::EWriteKind::Insert: result.CommandTagName = "INSERT"; break;
                                case NNewYql::EWriteKind::Upsert: result.CommandTagName = "UPSERT"; break;
                                case NNewYql::EWriteKind::Replace: result.CommandTagName = "REPLACE"; break;
                                case NNewYql::EWriteKind::Update: result.CommandTagName = "UPDATE"; break;
                                case NNewYql::EWriteKind::Delete: result.CommandTagName = "DELETE"; break;
                            }
                        }
                    }
                    sqlVersion = 1;
                    return result;
                }
            } catch (const TNewYqlParserFallback& e) {
                // No expression lowering has run yet. Resume the original pipeline
                // with its existing AST, settings and optimizer selection intact.
                traceScope.SetSuccess(true);
                TraceNativeFallback(e);
                YQL_CLOG(INFO, ProviderKqp) << "New YQL parser fallback: " << e.what();
            }
        }

        YQL_CLOG(INFO, ProviderKqp) << "Legacy YQL graph compilation started";
        if (!query.AstResult) {
            TCompileTraceScope traceScope(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Phase,
                "Parse query");
            NSQLTranslation::TTranslationSettings effectiveSettings;
            auto astRes = ParseQuery(
                query.Text,
                isSql,
                sqlVersion,
                TypesCtx->DeprecatedSQL,
                ctx,
                settingsBuilder,
                result.KeepInCache,
                result.CommandTagName,
                &effectiveSettings
            );
            if (!astRes.IsOk() && isSql && SessionCtx->Config().GetEnableNewRBO()) {
                // Forced YqlSelect translation rejects table hints before we can
                // inspect their Read! nodes. Accept legacy translation only for
                // sampling; other queries retain the original parser diagnostics.
                TExprContext samplingCtx;
                auto samplingSqlVersion = sqlVersion;
                bool samplingDeprecatedSQL = TypesCtx->DeprecatedSQL;
                bool samplingKeepInCache = false;
                TMaybe<TString> samplingCommandTag;
                NSQLTranslation::TTranslationSettings samplingSettings;
                settingsBuilder.SetYqlSelect(NSQLTranslation::EYqlSelect::Disable);
                auto samplingAst = ParseQuery(query.Text, isSql, samplingSqlVersion, samplingDeprecatedSQL,
                    samplingCtx, settingsBuilder, samplingKeepInCache, samplingCommandTag, &samplingSettings);
                TExprNode::TPtr samplingExpr;
                if (samplingAst.IsOk() &&
                    CompileExpr(*samplingAst.Root, samplingExpr, samplingCtx, ModuleResolver.get(), nullptr) &&
                    HasSamplingRead(samplingExpr))
                {
                    astRes = std::move(samplingAst);
                    sqlVersion = samplingSqlVersion;
                    TypesCtx->DeprecatedSQL = samplingDeprecatedSQL;
                    result.KeepInCache = samplingKeepInCache;
                    result.CommandTagName = std::move(samplingCommandTag);
                    effectiveSettings = std::move(samplingSettings);
                }
            }
            traceScope.SetSuccess(astRes.IsOk() && astRes.Root);
            SessionCtx->Query().TranslationSettings = std::move(effectiveSettings);
            queryAst = std::make_shared<NYql::TAstParseResult>(std::move(astRes));
        } else {
            queryAst = query.AstResult->Ast;
            result.KeepInCache = query.AstResult->KeepInCache;
            result.CommandTagName = query.AstResult->CommandTagName;
            sqlVersion = query.AstResult->SqlVersion;
            if (query.AstResult->DeprecatedSQL) {
               TypesCtx->DeprecatedSQL = *query.AstResult->DeprecatedSQL;
            }
        }

        YQL_ENSURE(queryAst);
        ctx.IssueManager.AddIssues(queryAst->Issues);
        if (!queryAst->IsOk()) {
            return result;
        }

        if (SessionCtx->Config().GetEnableDiscardSelect()) {
            bool hasDiscardWarning = false;
            for (const auto& issue : ctx.IssueManager.GetIssues()) {
                if (issue.GetCode() == TIssuesIds::YQL_DISCARD_IN_INVALID_PLACE) {
                    hasDiscardWarning = true;
                    break;
                }
            }

            // only query will throw error to save the backward compatibility
            if (SessionCtx->Query().Type == EKikimrQueryType::Query && hasDiscardWarning) {
                ctx.AddError(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_OPERATION, TStringBuilder()
                    << "DISCARD can only be used at the top level, not inside subqueries"));
                return result;
            }
        }

        YQL_ENSURE(queryAst->Root);
        TExprNode::TPtr queryExpr;
        YQL_CLOG(INFO, CoreDq) << "Good place to weld in";

        if (!CompileExpr(*queryAst->Root, queryExpr, ctx, ModuleResolver.get(), nullptr)) {
            return result;
        }
        YQL_CLOG(INFO, CoreDq) << "Compiled query:\n" << KqpExprToPrettyString(*queryExpr, ctx);

        if (!CheckIsBatch(queryExpr, ctx)) {
            return result;
        }

        if (HasSamplingRead(queryExpr)) {
            // Sampling requires the read ranges source even for scan queries.
            SessionCtx->ConfigPtr()->SetEnableKqpScanQuerySourceRead(true);
            // RBO read operators do not preserve sampling settings. Select the
            // legacy pipeline before optimization, independently of error fallback.
            SessionCtx->ConfigPtr()->SetEnableNewRBO(false);
            TypesCtx->IgnoreExpandPg = false;
        }

        YQL_CLOG(INFO, ProviderKqp) << "Compiled query:\n" << KqpExprToPrettyString(*queryExpr, ctx);

        result.NeedToSplit = Config->GetEnableCreateTableAs() && NeedToSplit(queryExpr, ctx);
        result.QueryExpr = queryExpr;
        return result;
    }

    TCompileExprResult CompileYqlQuery(const TKqpQueryRef& query, bool isSql, TExprContext& ctx, TMaybe<TSqlVersion>& sqlVersion,
        TKqpTranslationSettingsBuilder& settingsBuilder) const
    {
        auto compileResult = CompileQuery(query, isSql, ctx, sqlVersion, settingsBuilder);
        if (!compileResult.QueryExpr || compileResult.NeedToSplit || NativeFrontendActive) {
            return compileResult;
        }

        if (!isSql) {
            return compileResult;
        }

        if (TMaybeNode<TCoCommit>(compileResult.QueryExpr) && TCoCommit(compileResult.QueryExpr).DataSink().Maybe<TKiDataSink>()) {
            return compileResult;
        }

        compileResult.QueryExpr = {Build<TCoCommit>(ctx, compileResult.QueryExpr->Pos())
            .World(compileResult.QueryExpr)
            .DataSink<TKiDataSink>()
                .Category().Build(KikimrProviderName)
                .Cluster().Build(Cluster)
                .Build()
            .Settings()
                .Add()
                    .Name().Build("mode")
                    .Value<TCoAtom>().Build(KikimrCommitModeFlush())
                    .Build()
                .Build()
            .Done()
            .Ptr()};
        return compileResult;
    }

private:

    IAsyncQueryResultPtr ExecuteSchemeQueryInternal(const TKqpQueryRef& query, bool isSql, const TExecSettings& settings, TExprContext& ctx) {
        SetupYqlTransformer(EKikimrQueryType::Ddl);

        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, query.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false)
            .SetFromConfig(SessionCtx->Config());

        auto compileResult = CompileYqlQuery(query, isSql, ctx, sqlVersion, settingsBuilder);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncExecuteYqlResult>(compileResult.QueryExpr.Get(), ctx, GetYqlTransformer(), Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr ExplainDataQueryInternal(const TKqpQueryRef& query, bool isSql, TExprContext& ctx) {
        if (isSql) {
            return PrepareDataQueryInternal(query, {}, ctx);
        }

        auto prepareResult = PrepareDataQueryAstInternal(query, {}, ctx);
        if (!prepareResult) {
            return nullptr;
        }

        return AsyncApplyResult<TQueryResult, TQueryResult>(prepareResult, []
            (TQueryResult&& prepared) -> IAsyncQueryResultPtr {
                if (!prepared.Success()) {
                    return MakeKikimrResultHolder(std::move(prepared));
                }

                TQueryResult explainResult;
                explainResult.SetSuccess();
                YQL_ENSURE(prepared.PreparingQuery->GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);

                FillColumnMeta(prepared.PreparingQuery->GetPhysicalQuery(), explainResult);

                explainResult.QueryPlan = std::move(prepared.QueryPlan);
                explainResult.QueryAst = std::move(*prepared.PreparingQuery->MutablePhysicalQuery()->MutableQueryAst());
                explainResult.SqlVersion = prepared.SqlVersion;
                return MakeKikimrResultHolder(std::move(explainResult));
            });
    }

    IAsyncQueryResultPtr ExplainScanQueryInternal(const TKqpQueryRef& query, bool isSql, TExprContext& ctx) {
        return PrepareScanQueryInternal(query, isSql, ctx, {});
    }

    IAsyncQueryResultPtr PrepareDataQueryInternal(const TKqpQueryRef& query, const TPrepareSettings& settings,
        TExprContext& ctx)
    {
        SetupYqlTransformer(EKikimrQueryType::Dml, settings.CompileTrace);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }
        if (settings.IsInternalCall) {
            SessionCtx->Query().IsInternalCall = *settings.IsInternalCall;
        }
        if (settings.RuntimeParameterSizeLimitSatisfied) {
            SessionCtx->Query().RuntimeParameterSizeLimitSatisfied = settings.RuntimeParameterSizeLimitSatisfied;
        }

        if (settings.RuntimeParameterSizeLimit) {
            SessionCtx->Query().RuntimeParameterSizeLimit = settings.RuntimeParameterSizeLimit;
        }

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, query.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false)
            .SetFromConfig(SessionCtx->Config())
            .SetYqlSelect(settings.YqlSelect);
        auto compileResult = CompileYqlQuery(query, /* isSql */ true, ctx, sqlVersion, settingsBuilder);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        auto& yqlTransformer = NativeFrontendActive ? GetNativeFrontend()
            : GetYqlTransformer(SessionCtx->Config().GetEnableNewRBO());
        return MakeIntrusive<TAsyncPrepareYqlResult>(compileResult.QueryExpr.Get(), ctx, yqlTransformer, SessionCtx->QueryPtr(),
            query.Text, sqlVersion, TransformCtx, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr PrepareDataQueryAstInternal(const TKqpQueryRef& queryAst, const TPrepareSettings& settings,
        TExprContext& ctx)
    {
        IKikimrQueryExecutor::TExecuteSettings execSettings;
        SetupDataQueryAstTransformer(execSettings, EKikimrQueryType::Dml, settings.CompileTrace);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }
        if (settings.IsInternalCall) {
            SessionCtx->Query().IsInternalCall = *settings.IsInternalCall;
        }
        if (settings.RuntimeParameterSizeLimitSatisfied) {
            SessionCtx->Query().RuntimeParameterSizeLimitSatisfied = settings.RuntimeParameterSizeLimitSatisfied;
        }
        if (settings.RuntimeParameterSizeLimit) {
            SessionCtx->Query().RuntimeParameterSizeLimit = settings.RuntimeParameterSizeLimit;
        }

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, queryAst.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false)
            .SetFromConfig(SessionCtx->Config());
        auto compileResult = CompileYqlQuery(queryAst, false, ctx, sqlVersion, settingsBuilder);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }
        YQL_ENSURE(!sqlVersion);

        return MakeIntrusive<TAsyncExecuteKqlResult>(compileResult.QueryExpr.Get(), ctx, GetDataQueryAstTransformer(),
            SessionCtx, *ExecuteCtx, DataProvidersFinalizer);
    }

    TMaybe<TSqlVersion> SetupQueryParameters(const TPrepareSettings& settings, EKikimrQueryType queryType) {
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);

        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }
        if (settings.IsInternalCall) {
            SessionCtx->Query().IsInternalCall = *settings.IsInternalCall;
        }
        if (settings.ConcurrentResults) {
            YQL_ENSURE(*settings.ConcurrentResults || queryType == EKikimrQueryType::Query);
            SessionCtx->Query().ConcurrentResults = *settings.ConcurrentResults;
        }
        SessionCtx->Query().IsolateEffects = settings.UsePessimisticLocks;
        if (settings.RuntimeParameterSizeLimitSatisfied) {
            SessionCtx->Query().RuntimeParameterSizeLimitSatisfied = settings.RuntimeParameterSizeLimitSatisfied;
        }

        if (settings.RuntimeParameterSizeLimit) {
            SessionCtx->Query().RuntimeParameterSizeLimit = settings.RuntimeParameterSizeLimit;
        }

        TMaybe<TSqlVersion> sqlVersion = settings.SyntaxVersion;
        if (!sqlVersion) {
            sqlVersion = 1;
        }

        return sqlVersion;
    }

    IAsyncQueryResultPtr PrepareQueryInternal(const TKqpQueryRef& query, NYql::TExprNode::TPtr expr, EKikimrQueryType queryType,
        const TPrepareSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(queryType, settings.CompileTrace);
        auto sqlVersion = SetupQueryParameters(settings, queryType);

        if (!expr) {
            TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, query.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
            settingsBuilder.SetSqlAutoCommit(false)
                .SetFromConfig(SessionCtx->Config())
                .SetValidateViewStatement(!settings.IsInternalCall.GetOrElse(false))
                .SetYqlSelect(settings.YqlSelect);
            auto compileResult = CompileYqlQuery(query, /* isSql */ true, ctx, sqlVersion, settingsBuilder);

            if (compileResult.NeedToSplit) {
                return MakeIntrusive<TAsyncPrepareNeedToSplitYqlResult>();
            } else {
                if (!compileResult.QueryExpr) {
                    return nullptr;
                }
                auto& yqlTransformer = NativeFrontendActive ? GetNativeFrontend()
            : GetYqlTransformer(SessionCtx->Config().GetEnableNewRBO());
                return MakeIntrusive<TAsyncPrepareYqlResult>(compileResult.QueryExpr.Get(), ctx, yqlTransformer, SessionCtx->QueryPtr(),
                    query.Text, sqlVersion, TransformCtx, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
            }
        } else {
            return MakeIntrusive<TAsyncPrepareYqlResult>(expr, ctx, GetYqlTransformer(), SessionCtx->QueryPtr(),
                query.Text, sqlVersion, TransformCtx, false, Nothing(), DataProvidersFinalizer);
        }
    }

    IAsyncSplitcResultPtr SplitQueryInternal(const TKqpQueryRef& query, const TPrepareSettings& settings, TExprContext& ctx) {
        SetupYqlTransformer(EKikimrQueryType::Query, settings.CompileTrace);
        auto sqlVersion = SetupQueryParameters(settings, EKikimrQueryType::Query);

        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, query.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder
            .SetSqlAutoCommit(false)
            .SetFromConfig(SessionCtx->Config());
        auto compileResult = CompileQuery(query, /* isSql */ true, ctx, sqlVersion, settingsBuilder);
        YQL_ENSURE(compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        if (!CheckRewrite(compileResult.QueryExpr, Config->GetEnableDataShardCreateTableAs(), ctx)) {
            return nullptr;
        }

        YQL_ENSURE(ExprCtxStorage);

        auto prepareData = PrepareRewrite(
            compileResult.QueryExpr,
            *ExprCtxStorage,
            *TypesCtx,
            SessionCtx,
            *FuncRegistry,
            Cluster);

        return MakeIntrusive<TAsyncSplitQueryResult>(
            compileResult.QueryExpr,
            ExprCtxStorage,
            std::move(FakeWorld),
            prepareData.InputExpr,
            prepareData.Transformer,
            TypesCtx,
            SessionCtx,
            Cluster,
            DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr PrepareScanQueryInternal(const TKqpQueryRef& query, bool isSql, TExprContext& ctx, const TPrepareSettings& prepareSettings,
                                                  EKikimrStatsMode statsMode = EKikimrStatsMode::None) {
        return isSql
            ? PrepareScanQueryInternal(query, ctx, prepareSettings, statsMode)
            : PrepareScanQueryAstInternal(query, ctx, prepareSettings);
    }

    IAsyncQueryResultPtr PrepareScanQueryInternal(const TKqpQueryRef& query, TExprContext& ctx, const TPrepareSettings& prepareSettings,
        EKikimrStatsMode statsMode = EKikimrStatsMode::None)
    {
        SetupYqlTransformer(EKikimrQueryType::Scan, prepareSettings.CompileTrace);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().StatsMode = statsMode;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion = 1;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, query.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false)
            .SetFromConfig(SessionCtx->Config())
            .SetYqlSelect(prepareSettings.YqlSelect);
        auto compileResult = CompileYqlQuery(query, true, ctx, sqlVersion, settingsBuilder);
        YQL_ENSURE(!compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        auto& yqlTransformer = NativeFrontendActive ? GetNativeFrontend()
            : GetYqlTransformer(SessionCtx->Config().GetEnableNewRBO());
        return MakeIntrusive<TAsyncPrepareYqlResult>(compileResult.QueryExpr.Get(), ctx, yqlTransformer, SessionCtx->QueryPtr(),
            query.Text, sqlVersion, TransformCtx, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr PrepareScanQueryAstInternal(const TKqpQueryRef& queryAst, TExprContext& ctx, const TPrepareSettings& prepareSettings) {
        IKikimrQueryExecutor::TExecuteSettings settings;
        SetupDataQueryAstTransformer(settings, EKikimrQueryType::Scan, prepareSettings.CompileTrace);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, queryAst.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false)
            .SetFromConfig(SessionCtx->Config())
            .SetYqlSelect(prepareSettings.YqlSelect);
        auto compileResult = CompileYqlQuery(queryAst, false, ctx, sqlVersion, settingsBuilder);

        YQL_ENSURE(!sqlVersion);
        YQL_ENSURE(!compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncExecuteKqlResult>(compileResult.QueryExpr.Get(), ctx, GetDataQueryAstTransformer(),
            SessionCtx, *ExecuteCtx, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr ExecuteYqlScriptInternal(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const TExecScriptSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(EKikimrQueryType::YqlScript);

        SessionCtx->Query().IsInternalCall = settings.IsInternalCall;
        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, script.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true)
            .SetFromConfig(SessionCtx->Config());
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder);
        YQL_ENSURE(!compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        (SessionCtx->Query().QueryData)->ParseParameters(parameters);

        return MakeIntrusive<TAsyncExecuteYqlResult>(compileResult.QueryExpr.Get(), ctx, GetYqlTransformer(), Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr StreamExecuteYqlScriptInternal(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const NActors::TActorId& target,const TExecScriptSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(EKikimrQueryType::YqlScriptStreaming);

        SessionCtx->Query().IsInternalCall = settings.IsInternalCall;
        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().RpcCtx = settings.RpcCtx;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().ReplyTarget = target;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, script.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true)
            .SetFromConfig(SessionCtx->Config());
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder);
        YQL_ENSURE(!compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        (SessionCtx->Query().QueryData)->ParseParameters(parameters);

        return MakeIntrusive<TAsyncExecuteYqlResult>(compileResult.QueryExpr.Get(), ctx, GetYqlTransformer(), Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr ValidateYqlScriptInternal(const TKqpQueryRef& script, TExprContext& ctx) {
        SetupSession(EKikimrQueryType::YqlScript);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().SuppressDdlChecks = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, script.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true)
            .SetFromConfig(SessionCtx->Config());
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder);
        YQL_ENSURE(!compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        auto transformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .Add(TSecretValueExprTransformer::Sync(SessionCtx->QueryPtr()), "SecretValueExpr")
            .AddIOAnnotation()
            .AddTypeAnnotation()
            .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
            .Build(false);

        return MakeIntrusive<TAsyncValidateYqlResult>(compileResult.QueryExpr.Get(), SessionCtx, ctx, transformer, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr ExplainYqlScriptInternal(const TKqpQueryRef& script, TExprContext& ctx) {
        SetupYqlTransformer(EKikimrQueryType::YqlScript);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().SuppressDdlChecks = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, Cluster, script.Text, SessionCtx->Config().GetYqlBindingsMode(), GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true)
            .SetFromConfig(SessionCtx->Config());
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder);
        YQL_ENSURE(!compileResult.NeedToSplit);
        if (!compileResult.QueryExpr) {
            return nullptr;
        }

        GetYqlTransformer();
        return MakeIntrusive<TAsyncExplainYqlResult>(compileResult.QueryExpr.Get(), SessionCtx, ctx, YqlTransformer,
            *PlanBuilder, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer, true /* UseDqExplain */);
    }

    static TDataProviderInfo MakeProviders(TIntrusivePtr<IDataProvider> source, TIntrusivePtr<IDataProvider> sink) {
        TDataProviderInfo providers;
        providers.Source = std::move(source);
        providers.Sink = std::move(sink);
        return providers;
    }

    void RegisterLazyProvider(TStringBuf name, TStringBuf callablePrefix,
        std::function<TDataProviderInfo()> factory, bool hasLayers = false)
    {
        auto providers = CreateLazyProvider(name, callablePrefix, [this, name = TString(name), factory = std::move(factory)] {
            TCompileTraceScope trace(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Step,
                TStringBuilder() << "Initialize " << name << " provider");
            auto providers = factory();
            YQL_ENSURE(providers.Source->Initialize(*ExprCtx));
            YQL_ENSURE(providers.Sink->Initialize(*ExprCtx));
            return providers;
        }, hasLayers);
        TypesCtx->AddDataSource(name, std::move(providers.Source));
        TypesCtx->AddDataSink(name, std::move(providers.Sink));
    }

    TDataProviderInfo CreateS3Provider(EKikimrQueryType queryType) {
        auto state = MakeIntrusive<NYql::TS3State>(TypesCtx->StrictConfigValidation);

        auto& configuration = *state->Configuration;
        if (const auto requestContext = SessionCtx->GetUserRequestContext(); requestContext && requestContext->IsStreamingQuery) {
            configuration.DisablePragma(configuration.UseRuntimeListing, false, "Runtime listing is not supported for streaming queries, pragma value was ignored");
            configuration.DisablePragma(configuration.AtomicUploadCommit, false, "Atomic upload commit is not supported for streaming queries, pragma value was ignored");
            configuration.DefaultOutputKeyFlushTimeout = TDuration::Minutes(1);
        } else {
            if (queryType != EKikimrQueryType::Script) {
                configuration.DisablePragma(configuration.AtomicUploadCommit, false, "");
            }

            configuration.ChangeDefaultPragmaValue(configuration.UseRuntimeListing, true);
        }
        configuration.WriteThroughDqIntegration = true;
        configuration.Init(FederatedQuerySetup->S3GatewayConfig, TypesCtx);

        state->Types = TypesCtx.Get();
        state->FunctionRegistry = FuncRegistry;
        state->CredentialsFactory = FederatedQuerySetup->CredentialsFactory;
        state->Gateway = FederatedQuerySetup->HttpGateway;
        state->GatewayRetryPolicy = NYql::GetFqHTTPRetryPolicy();
        state->ExecutorPoolId = AppData()->UserPoolId;
        state->ActorSystem = ActorSystem;
        state->EnableS3ConstraintsTransformer = Config->_KqpYqlConstraintsTransformerEnabled.Get().GetOrElse(false);

        auto dataSource = NYql::CreateS3DataSource(state);
        auto dataSink = NYql::CreateS3DataSink(state);

        return MakeProviders(std::move(dataSource), std::move(dataSink));
    }

    TDataProviderInfo CreateYdbExternalProvider() {
        const auto& resources = FederatedQuerySetup->YdbExternalResources;
        YQL_ENSURE(resources, "Missing YdbExternal resources");
        auto provider = NYql::CreateYdbExternalDataProviders(
            TypesCtx.Get(), [resources] { return resources->GetMetadataClientCache(); },
            FederatedQuerySetup->CredentialsFactory);
        return MakeProviders(std::move(provider.Source), std::move(provider.Sink));
    }

    TDataProviderInfo CreateGenericProvider() {
        auto state = MakeIntrusive<NYql::TGenericState>(
            TypesCtx.Get(),
            FuncRegistry,
            FederatedQuerySetup->DatabaseAsyncResolver,
            FederatedQuerySetup->CredentialsFactory,
            FederatedQuerySetup->ConnectorClient,
            FederatedQuerySetup->GenericGatewayConfig
        );

        return MakeProviders(NYql::CreateGenericDataSource(state), NYql::CreateGenericDataSink(state));
    }

    TDataProviderInfo CreateYtProvider(TVector<std::function<TFuture<void>()>>& finalizers) {
        TString userName = CreateGuidAsString();
        if (SessionCtx->GetUserToken() && SessionCtx->GetUserToken()->GetUserSID()) {
            userName = SessionCtx->GetUserToken()->GetUserSID();
        }

        TString sessionId = CreateGuidAsString();
        auto [ytState, statWriter] = CreateYtNativeState(FederatedQuerySetup->YtGateway, userName, sessionId, &FederatedQuerySetup->YtGatewayConfig, TypesCtx, NYql::NDq::MakeCBOOptimizerFactory(), MakeDqHelper());

        ytState->PassiveExecution = true;
        ytState->Gateway->OpenSession(
            IYtGateway::TOpenSessionOptions(sessionId)
                .UserName(userName)
                .RandomProvider(TAppData::RandomProvider)
                .TimeProvider(TAppData::TimeProvider)
                .StatWriter(statWriter)
                .CreateOperationTracker(false)
        );

        auto ytSource = CreateYtDataSource(ytState);
        if (Config->FeatureFlags.GetEnableQYT()) {
            ytSource = NYql::WrapYtDataSourceWithMessageStreams(std::move(ytSource),
                NYql::CreateYtMessageStreamIntegration(FederatedQuerySetup->CredentialsFactory));
        }

        finalizers.emplace_back([ytGateway = FederatedQuerySetup->YtGateway, sessionId]() {
            return ytGateway->CloseSession(NYql::IYtGateway::TCloseSessionOptions(sessionId));
        });
        return MakeProviders(std::move(ytSource), CreateYtDataSink(ytState));
    }

    TDataProviderInfo CreatePgProvider() {
        auto state = MakeIntrusive<NYql::TPgState>();
        state->Types = TypesCtx.Get();

        return MakeProviders(NYql::CreatePgDataSource(state), NYql::CreatePgDataSink(state));
    }

    TDataProviderInfo CreateSolomonProvider() {
        auto solomonState = MakeIntrusive<TSolomonState>(TypesCtx->StrictConfigValidation);

        solomonState->WriteThroughDqIntegration = true;
        solomonState->Types = TypesCtx.Get();
        solomonState->Gateway = NYql::CreateSolomonGateway(FederatedQuerySetup->SolomonGatewayConfig);
        solomonState->CredentialsFactory = FederatedQuerySetup->CredentialsFactory;
        solomonState->DqIntegration = NYql::CreateSolomonDqIntegration(solomonState);
        solomonState->Configuration->Init(FederatedQuerySetup->SolomonGatewayConfig, TypesCtx);
        solomonState->ExecutorPoolId = AppData()->UserPoolId;

        return MakeProviders(NYql::CreateSolomonDataSource(solomonState), NYql::CreateSolomonDataSink(solomonState));
    }

    TDataProviderInfo CreatePqProvider(TVector<std::function<TFuture<void>()>>& finalizers) {
        TString sessionId = CreateGuidAsString();
        auto state = MakeIntrusive<TPqState>(sessionId, TypesCtx->StrictConfigValidation);
        state->SupportRtmrMode = false;
        state->AddTransparentPrefixToTransparentSystemColumns = false;
        state->EnableSettingsValidation = true;
        state->EnableUserAttributesInTopicQuery = Config->FeatureFlags.GetEnableUserAttributesInTopicQuery();
        state->StreamingTopicsReadByDefault = false;
        state->EnableTopicsPredicatePushdown = Config->FeatureFlags.GetEnableTopicsPredicatePushdown();
        state->ForbidYqlSysColumnsAndSystemMetadata = QueryServiceConfig.GetStreamingQueries().GetForbidYqlSysColumnsAndSystemMetadata();
        state->EnablePqConstraintsTransformer = Config->_KqpYqlConstraintsTransformerEnabled.Get().GetOrElse(false);
        state->EnableWatermarks = Config->GetEnableWatermarks();
        state->EnableWatermarksAdvanced = Config->GetEnableWatermarksAdvanced();
        state->EnableStreamingPartitionBalancing = Config->GetEnableStreamingPartitionBalancing();
        state->EnableExactlyOnceDeliveryGuaranty = Config->FeatureFlags.GetEnableExactlyOnceTopicsWriting();
        state->EnableConsumerRewindForDisposition = Config->FeatureFlags.GetEnableStreamingQueryReadFrom();
        state->Types = TypesCtx.Get();
        state->DbResolver = FederatedQuerySetup->DatabaseAsyncResolver;
        state->FunctionRegistry = FuncRegistry;
        state->Configuration->Init(FederatedQuerySetup->PqGatewayConfig, TypesCtx, state->DbResolver, state->DatabaseIds);
        state->Gateway = FederatedQuerySetup->PqGatewayFactory->CreatePqGateway();
        state->DqIntegration = NYql::CreatePqDqIntegration(state);
        state->Gateway->OpenSession(sessionId, "username");

        if (const auto requestContext = SessionCtx->GetUserRequestContext()) {
            if (requestContext->IsStreamingQuery) {
                state->StreamingTopicsReadByDefault = true;

                if (Config->FeatureFlags.GetEnableExactlyOnceTopicsWriting()) {
                    state->DeferredPublicationExtIdPrefix = TStringBuilder() << "__ydb_streaming:" << requestContext->StreamingQueryPath << ":" << requestContext->CurrentExecutionId;
                }
            }

            if (const auto disposition = requestContext->StreamingDisposition) {
                state->Disposition = *disposition;
            }
            state->WatermarkLateEventsPolicy = requestContext->WatermarkLateEventsPolicy;
        }

        finalizers.emplace_back([pqGateway = state->Gateway, sessionId]() {
            return pqGateway->CloseSession(sessionId);
        });
        return MakeProviders(NYql::CreatePqDataSource(state, state->Gateway), NYql::CreatePqDataSink(state, state->Gateway));
    }

    void Init(EKikimrQueryType queryType) {
        auto* trace = SessionCtx->Query().CompileTrace.Get();
        TCompileTraceScope providersTrace(trace, TCompileTraceLevels::Step,
            "Initialize compilation providers");
        // A host can prepare more than one query. Retire the previous factories
        // and registrations before creating this compilation's provider pairs.
        TypesCtx->DataSources.clear();
        TypesCtx->DataSinks.clear();
        TypesCtx->DataSourceMap.clear();
        TypesCtx->DataSinkMap.clear();
        TypesCtx->LayersRegistry.Reset();
        TypesCtx->InitializeResult.reset();
        TransformCtx = MakeIntrusive<TKqlTransformContext>(Config, SessionCtx->QueryPtr(), SessionCtx->TablesPtr());
        KqpRunner = CreateKqpRunner(Gateway, Cluster, TypesCtx, SessionCtx, TransformCtx, *FuncRegistry, ActorSystem, UsePessimisticLocks);

        ExprCtx->NodesAllocationLimit = SessionCtx->Config()._KqpExprNodesAllocationLimit.Get().GetRef();
        ExprCtx->StringsAllocationLimit = SessionCtx->Config()._KqpExprStringsAllocationLimit.Get().GetRef();

        THashSet<TString> providerNames {
            TString(KikimrProviderName),
            TString(YdbProviderName),
        };

        // Kikimr provider
        auto gatewayProxy = CreateKqpGatewayProxy(Gateway, SessionCtx, ActorSystem);

        auto queryExecutor = MakeIntrusive<TKqpQueryExecutor>(Gateway, Cluster, SessionCtx, KqpRunner);
        auto kikimrDataSource = CreateKikimrDataSource(*FuncRegistry, *TypesCtx, gatewayProxy, SessionCtx,
            ExternalSourceFactory, IsInternalCall, GUCSettings);
        auto kikimrDataSink = CreateKikimrDataSink(*FuncRegistry, *TypesCtx, gatewayProxy, SessionCtx, ExternalSourceFactory, queryExecutor);

        FillSettings.AllResultsBytesLimit = Nothing();
        FillSettings.RowsLimitPerWrite = SessionCtx->Config()._ResultRowsLimit.Get();
        FillSettings.Format = IDataProvider::EResultFormat::Custom;
        FillSettings.FormatDetails = TString(KikimrMkqlProtoFormat);

        TypesCtx->AddDataSource(providerNames, kikimrDataSource);
        TypesCtx->AddDataSink(providerNames, kikimrDataSink);
        TypesCtx->FilterPushdownOverJoinOptionalSide = SessionCtx->ConfigPtr()->GetFilterPushdownOverJoinOptionalSide();
        if (SessionCtx->ConfigPtr()->GetFilterPushdownOverJoinOptionalSide()) {
            TypesCtx->OptimizerFlags.insert("fuseequijoinsinputmultilabels");
            TypesCtx->OptimizerFlags.insert("pullupflatmapoverjoinmultiplelabels");
            TypesCtx->OptimizerFlags.insert("sqlinwithnothingornull");
            TypesCtx->OptimizerFlags.insert("filterpushdownoverjoinoptionalsideignoreonlykeys");
        }
        TypesCtx->OptimizerFlags.insert("disablenormalizeequalityfilteroverjoin");
        TypesCtx->OptimizerFlags.insert("rewriteswitchoverextractmembers");

        TypesCtx->IgnoreExpandPg = SessionCtx->ConfigPtr()->GetEnableNewRBO();

        bool isSupportedQueryType = queryType == EKikimrQueryType::Script || queryType == EKikimrQueryType::Query
            || queryType == EKikimrQueryType::YqlScript || queryType == EKikimrQueryType::YqlScriptStreaming;
        if (isSupportedQueryType && FederatedQuerySetup) {
            // Metadata may activate a provider after the async result has copied
            // its finalizer. Share the list so every opened session is closed.
            auto finalizers = std::make_shared<TVector<std::function<TFuture<void>()>>>();
            const auto available = [this](TStringBuf name) {
                return ExternalSourceFactory->IsAvailableProvider(TString(name));
            };
            if (AppData()->FeatureFlags.GetEnableExternalDataSources()) {
                if (available(S3ProviderName)) {
                    RegisterLazyProvider(S3ProviderName, "S3", [this, queryType] { return CreateS3Provider(queryType); });
                }
                if (available(GenericProviderName) && FederatedQuerySetup->ConnectorClient) {
                    RegisterLazyProvider(GenericProviderName, "Generic", [this] { return CreateGenericProvider(); });
                }
                if (available(YdbExternalProviderName)) {
                    RegisterLazyProvider(YdbExternalProviderName, "YdbExternal", [this] { return CreateYdbExternalProvider(); });
                }
                if (available(SolomonProviderName)) {
                    RegisterLazyProvider(SolomonProviderName, "So", [this] { return CreateSolomonProvider(); });
                }
                if (available(YtProviderName) && FederatedQuerySetup->YtGateway) {
                    RegisterLazyProvider(YtProviderName, "Yt", [this, finalizers] { return CreateYtProvider(*finalizers); }, true);
                }
            }
            const bool topicsEnabled = AppData()->FeatureFlags.GetEnableTopicsSqlIoOperations();
            if (FederatedQuerySetup->PqGatewayFactory
                && (AppData()->FeatureFlags.GetEnableExternalDataSources() || topicsEnabled)
                && (available(PqProviderName) || topicsEnabled)) {
                RegisterLazyProvider(PqProviderName, "Pq", [this, finalizers] { return CreatePqProvider(*finalizers); });
            }
            DataProvidersFinalizer = [finalizers](const NYql::IGraphTransformer::TStatus&) {
                TVector<TFuture<void>> futures;
                for (const auto& finalize : *finalizers) {
                    futures.push_back(finalize());
                }
                finalizers->clear();
                return WaitAll(futures);
            };
            TypesCtx->StreamLookupJoin = Config->GetEnableDqSourceStreamLookupJoin();
        }

        RegisterLazyProvider(PgProviderName, "Pg", [this] { return CreatePgProvider(); });

        TypesCtx->UdfResolver = CreateSimpleUdfResolver(FuncRegistry);
        TypesCtx->TimeProvider = TAppData::TimeProvider;
        TypesCtx->RandomProvider = TAppData::RandomProvider;
        TypesCtx->Modules = ModuleResolver;
        TypesCtx->UserDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, TUserDataTable(), nullptr, nullptr);
        TypesCtx->JsonQueryReturnsJsonDocument = true;
        TypesCtx->ArrowResolver = MakeSimpleArrowResolver(*FuncRegistry);

        // Result provider
        auto writerFactory = [] () { return MakeIntrusive<TKqpResultWriter>(); };
        ResultProviderConfig = MakeIntrusive<TResultProviderConfig>(*TypesCtx, *FuncRegistry, FillSettings.Format,
            FillSettings.FormatDetails, writerFactory);
        auto resultProvider = CreateResultProvider(ResultProviderConfig);
        TypesCtx->AddDataSink(ResultProviderName, resultProvider);
        TypesCtx->AvailablePureResultDataSources = TVector<TString>(1, TString(KikimrProviderName));

        // Config provider
        const TGatewaysConfig* gatewaysConfig = nullptr; // TODO: can we get real gatewaysConfig here?
        auto allowSettings = [](TStringBuf settingName) {
            return settingName == "OrderedColumns"
                || settingName == "DeriveColumnOrder"
                || settingName == "DisableOrderedColumns"
                || settingName == "Warning"
                || settingName == "UseBlocks"
                || settingName == "BlockEngine"
                || settingName == "DecimalCommonTypeConversionMode"
                || settingName == "FilterPushdownOverJoinOptionalSide"
                || settingName == "DisableFilterPushdownOverJoinOptionalSide"
                || settingName == "RotateJoinTree"
                || settingName == "TimeOrderRecoverDelay"
                || settingName == "TimeOrderRecoverAhead"
                || settingName == "TimeOrderRecoverRowLimit"
                || settingName == "MatchRecognizeStream"
                || settingName == "OptimizerFlags"
                || settingName == "FuseEquiJoinsInputMultiLabels"
                || settingName == "PullUpFlatMapOverJoinMultipleLabels"
                ;
        };
        auto configProvider = CreateConfigProvider(*TypesCtx, gatewaysConfig, {}, allowSettings);
        TypesCtx->AddDataSource(ConfigProviderName, configProvider);
        TypesCtx->MatchRecognize = QueryServiceConfig.GetEnableMatchRecognize();

        YQL_ENSURE(TypesCtx->Initialize(*ExprCtx));
    }

    IGraphTransformer& GetYqlTransformer(bool newRbo = false) {
        auto& transformer = newRbo ? YqlTransformerNewRBO : YqlTransformer;
        if (transformer) {
            return *transformer;
        }
        auto* trace = SessionCtx->Query().CompileTrace.Get();
        TCompileTraceScope pipelinesTrace(trace, TCompileTraceLevels::Step,
            newRbo ? "Create new RBO frontend pipeline" : "Create legacy frontend pipeline");
        const auto queryType = SessionCtx->Query().Type;

        // Compilation spans of the pipeline phases. The logical phases run in a loop
        // that restarts until the query stops changing: a phase span covers the loop,
        // and the passes of its phases are steps in it.
        static const char phases = 0;
        static const char passes = 0;
        const auto queryCtx = SessionCtx->QueryPtr();
        const auto getTrace = [queryCtx] { return queryCtx->CompileTrace.Get(); };
        const auto beginTrace = [&](const TString& name) {
            return CreateCompileTraceBegin(getTrace, TCompileTraceLevels::Phase, name, &phases);
        };
        const auto endTrace = [&](const TString& name) {
            return CreateCompileTraceEnd(getTrace, name, &phases);
        };
        const auto beginPass = [&](const TString& name) {
            return CreateCompileTraceBegin(getTrace, TCompileTraceLevels::Step, name, &passes);
        };
        const auto endPass = [&](const TString& name) {
            return CreateCompileTraceEnd(getTrace, name, &passes);
        };
        const auto providerPeephole = [this] {
            TCompileTraceScope trace(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Step,
                "Create provider peephole pipeline");
            return GetDqIntegrationPeepholeTransformer(true, TypesCtx);
        };

        if (!newRbo) {
            YqlTransformer = TTransformationPipeline(TypesCtx)
                .AddServiceTransformers()
                .Add(TLogExprTransformer::Sync("YqlTransformer", NYql::NLog::EComponent::ProviderKqp,
                    NYql::NLog::ELevel::TRACE), "LogYqlTransform")
                .Add(beginTrace("Prepare logical query"), "TraceBeginPrepareLogicalQuery")
                .Add(beginPass("Annotate IO"), "TraceBeginIOAnnotation")
                .AddPreTypeAnnotation()
                .Add(TSecretValueExprTransformer::Sync(SessionCtx->QueryPtr()), "SecretValueExpr")
                .AddExpressionEvaluation(*FuncRegistry)
                .Add(new TFailExpressionEvaluation(queryType), "FailExpressionEvaluation")
                .AddIOAnnotation(false)
                .Add(endPass("Annotate IO"), "TraceEndIOAnnotation")
                .Add(beginPass("Annotate types"), "TraceBeginTypeAnnotation")
                .AddTypeAnnotation()
                .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
                .AddPostTypeAnnotation()
                .Add(endPass("Annotate types"), "TraceEndTypeAnnotation")
                .Add(beginPass("Apply provider optimizations"), "TraceBeginProviderOptimization")
                .AddOptimization(true, false)
                .Add(CreateLazyTransformer(providerPeephole), "DqIntegrationPeephole")
                .Add(endPass("Apply provider optimizations"), "TraceEndProviderOptimization")
                .Add(TLogExprTransformer::Sync("Optimized expr"), "LogExpr")
                .Add(beginTrace("Prepare physical query"), "TraceBeginPreparePhysicalQuery")
                .AddRun(&NullProgressWriter)
                .Add(endTrace("Prepare physical query"), "TraceEndPreparePhysicalQuery")
                .Build();

            return *YqlTransformer;
        }

        auto typesCtx = TypesCtx;
        EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION;
        auto RBOOptimizationTransformer = CreateCompositeGraphTransformer(
            {
                TTransformStage(
                    CreateChoiceGraphTransformer(
                    [typesCtx](const TExprNode::TPtr&, TExprContext&) {
                        return typesCtx->EngineType == EEngineType::Ytflow;
                    },
                    TTransformStage(
                        CreateRecaptureDataProposalsInspector(*typesCtx, TString{YtflowProviderName}),
                        "RecaptureDataProposalsYtflow",
                        issueCode),
                    TTransformStage(
                        CreateRecaptureDataProposalsInspector(*typesCtx, TString{DqProviderName}),
                        "RecaptureDataProposalsDq",
                        issueCode)),
                    "RecaptureDataProposals",
                    issueCode),
                TTransformStage(
                    CreateStatisticsProposalsInspector(*typesCtx, TString{DqProviderName}),
                    "StatisticsProposals",
                    issueCode
                ),
                TTransformStage(
                    CreateLogicalDataProposalsInspector(*typesCtx),
                    "LogicalDataProposals",
                    issueCode),
                TTransformStage(
                    CreatePhysicalDataProposalsInspector(*typesCtx),
                    "PhysicalDataProposals",
                    issueCode),
                TTransformStage(
                    CreatePhysicalFinalizers(*typesCtx),
                    "PhysicalFinalizers",
                    issueCode),
                TTransformStage(
                    CreateCheckExecutionTransformer(*typesCtx, true),
                    "CheckExecution",
                    issueCode)
                },
                false
            );

        YqlTransformerNewRBO = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("YqlTransformerNewRBO", NYql::NLog::EComponent::ProviderKqp,
                NYql::NLog::ELevel::TRACE), "LogYqlTransformNewRBO")
            .Add(beginTrace("Prepare logical query"), "TraceBeginPrepareLogicalQuery")
            .Add(beginPass("Annotate IO"), "TraceBeginIOAnnotation")
            .AddPreTypeAnnotation()
            .Add(TSecretValueExprTransformer::Sync(SessionCtx->QueryPtr()), "SecretValueExpr")
            .AddExpressionEvaluation(*FuncRegistry)
            .Add(new TFailExpressionEvaluation(queryType), "FailExpressionEvaluation")
            .AddIOAnnotation(false)
            .Add(endPass("Annotate IO"), "TraceEndIOAnnotation")
            .Add(beginPass("Annotate types"), "TraceBeginTypeAnnotation")
            .AddTypeAnnotation()
            .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
            .AddPostTypeAnnotation()
            .Add(endPass("Annotate types"), "TraceEndTypeAnnotation")
            .Add(beginPass("Apply provider optimizations"), "TraceBeginProviderOptimization")
            //.AddOptimization(true, false, TIssuesIds::CORE_OPTIMIZATION)
            .Add(RBOOptimizationTransformer.Release(), "RBOOptimizationTransformer")
            .Add(CreateLazyTransformer(providerPeephole), "DqIntegrationPeephole")
            .Add(endPass("Apply provider optimizations"), "TraceEndProviderOptimization")
            .Add(TLogExprTransformer::Sync("Optimized expr"), "LogExpr")
            .Add(beginTrace("Prepare physical query"), "TraceBeginPreparePhysicalQuery")
            .AddRun(&NullProgressWriter)
            .Add(endTrace("Prepare physical query"), "TraceEndPreparePhysicalQuery")
            .Build();

        return *YqlTransformerNewRBO;
    }

    IGraphTransformer& GetDataQueryAstTransformer() {
        if (DataQueryAstTransformer) {
            return *DataQueryAstTransformer;
        }
        TCompileTraceScope trace(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Step,
            "Create data query AST pipeline");
        DataQueryAstTransformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .AddIntentDeterminationTransformer()
            .AddTableMetadataLoaderTransformer()
            .AddTypeAnnotationTransformer()
            .Add(new TPrepareDataQueryAstTransformer(Cluster, ExecuteCtx, SessionCtx->QueryPtr(), KqpRunner),
                "PrepareDataQueryAst")
            .Build();
        return *DataQueryAstTransformer;
    }

    void SetupSession(EKikimrQueryType queryType, TCompileTrace::TPtr trace = {}) {
        if (TransformCtx) {
            // Release native plans before their annotation and AST owners.
            TransformCtx->NativeQuery.reset();
            TransformCtx->FrontendKeepInCache.Clear();
            TransformCtx->FrontendCommandTag.Clear();
        }
        YqlTransformer.Destroy();
        YqlTransformerNewRBO.Destroy();
        DataQueryAstTransformer.Destroy();
        NativeFrontendTransformer.Destroy();
        NativeMetadataTransformer.Destroy();
        NativePrepareTransformer.Destroy();
        LegacyCompile = {};
        NativeFrontendActive = false;
        LegacyFrontendRequested = false;
        NewParserTypes.reset();
        NewParserTypeSettings = {};
        NewParserNames.reset();
        NewParserTablesCollected = false;
        NewParserAst.reset();
        SessionCtx->Reset(KeepConfigChanges);
        SessionCtx->Query().Type = queryType;
        SessionCtx->Query().CompileTrace = std::move(trace);
        TCompileTraceScope traceScope(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Phase,
            "Set up compilation");

        DataProvidersFinalizer = nullptr;

        Init(queryType);

        ExprCtx->Reset();
        if (queryType != EKikimrQueryType::Query && queryType != EKikimrQueryType::Script) {
            ExprCtx->Step.Done(TExprStep::ExprEval); // KIKIMR-8067
        }

        TypesCtx->DeprecatedSQL = false;
        TypesCtx->CachedNow.reset();
        std::get<0>(TypesCtx->CachedRandom).reset();
        std::get<1>(TypesCtx->CachedRandom).reset();
        std::get<2>(TypesCtx->CachedRandom).reset();
    }

    bool BuildNativeQuery(TExprContext& ctx) {
        auto* trace = SessionCtx->Query().CompileTrace.Get();
        size_t executableStatements = 0;
        bool hasWrites = false;
        for (const auto& statement : NewParserAst->Statements) {
            if (statement.Explain != NNewYql::EExplainMode::None) {
                ythrow TNewYqlParserFallback() << "EXPLAIN statement execution";
            }
            executableStatements += std::holds_alternative<NNewYql::TQueryStatement>(statement.Value)
                || std::holds_alternative<NNewYql::TWriteStatement>(statement.Value);
            hasWrites |= std::holds_alternative<NNewYql::TWriteStatement>(statement.Value);
        }
        if (hasWrites && (executableStatements != 1 || SessionCtx->Config().GetEnableFallbackOnDML())) {
            ythrow TNewYqlParserFallback() << "Native multi-statement writes or configured DML fallback";
        }
        if (!NewParserTablesCollected) {
            bool pending = false;
            {
                TCompileTraceScope tablesTrace(trace, TCompileTraceLevels::Step, "Collect native tables");
                auto references = NNewYql::CollectTableReferences(*NewParserAst, NewParserSettings);
                tablesTrace.SetSuccess(references.IsOk());
                if (!references.IsOk()) {
                    ctx.IssueManager.AddIssues(references.Issues);
                    return false;
                }
                for (const auto& [cluster, path] : references.Tables) {
                    const auto* description = SessionCtx->Tables().GetTables().FindPtr(std::make_pair(cluster, path));
                    if (description && description->Metadata) { continue; }
                    if (cluster != Cluster) {
                        ythrow TNewYqlParserFallback() << "Cross-cluster catalog lookup";
                    }
                    auto& table = SessionCtx->Tables().GetOrAddTable(cluster, SessionCtx->GetDatabase(), path);
                    if (table.Metadata) {
                        ythrow TNewYqlParserFallback() << "Catalog path was rewritten: " << path;
                    }
                    table.RequireStats();
                    pending = true;
                }
                NewParserTablesCollected = true;
            }
            if (pending) { throw TNativeMetadataPending{}; }
        }
        if (!NewParserNames) {
            TCompileTraceScope namesTrace(trace, TCompileTraceLevels::Step, "Resolve native names");
            THashMap<std::pair<TString, TString>, NNewYql::TTableMetadataPtr> snapshots;
            const auto lookup = [&](const TString& cluster, const TString& path) -> NNewYql::TTableMetadataPtr {
                const auto key = std::make_pair(cluster, path);
                if (const auto* snapshot = snapshots.FindPtr(key)) {
                    return *snapshot;
                }
                const auto* description = SessionCtx->Tables().GetTables().FindPtr(key);
                // A missing catalog entry can mean a rewritten path, a view
                // or a different provider, not necessarily a missing table.
                if (!description || !description->Metadata) {
                    if (cluster != Cluster) {
                        ythrow TNewYqlParserFallback() << "Cross-cluster catalog lookup";
                    }
                    auto& table = SessionCtx->Tables().GetOrAddTable(cluster, SessionCtx->GetDatabase(), path);
                    if (table.Metadata) {
                        ythrow TNewYqlParserFallback() << "Catalog path was rewritten: " << path;
                    }
                    table.RequireStats();
                    throw TNativeMetadataPending{};
                }
                const auto& metadata = *description->Metadata;
                if (!metadata.DoesExist) {
                    return {};
                }
                if (metadata.Temporary || metadata.Kind == EKikimrTableKind::External
                    || metadata.Kind == EKikimrTableKind::View || metadata.Kind == EKikimrTableKind::SysView
                    || SessionCtx->Config().SystemColumnsEnabled()) {
                    ythrow TNewYqlParserFallback() << "Unsupported catalog schema in new name resolution: " << path;
                }
                auto snapshot = std::make_shared<NNewYql::TTableMetadata>();
                snapshot->Cluster = metadata.Cluster;
                snapshot->Path = metadata.Name;
                snapshot->OwnerId = metadata.PathId.OwnerId();
                snapshot->TableId = metadata.PathId.TableId();
                snapshot->SchemaVersion = metadata.SchemaVersion;
                snapshot->KeyColumns = metadata.KeyColumnNames;
                for (const auto& index : metadata.Indexes) { snapshot->HasWriteIndexes |= index.ItUsedForWrite(); }
                const auto addColumn = [&](const TKikimrColumnMetadata& column) {
                    if (column.IsBuildInProgress || column.SetNotNullInProgress) {
                        ythrow TNewYqlParserFallback() << "Column schema change in progress: " << path;
                    }
                    const auto* type = description->GetColumnType(column.Name);
                    if (!type) {
                        ythrow TNewYqlParserFallback() << "Column type is not available: " << path << "." << column.Name;
                    }
                    const bool hasDefault = column.DefaultKind != NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_UNSPECIFIED
                        || bool(column.DefaultExpression);
                    snapshot->Columns.push_back({column.Name, column.Id, FormatType(type), column.NotNull, hasDefault});
                    snapshot->HasDefaults |= hasDefault;
                };
                // Catalog order determines native star expansion and result columns.
                if (metadata.ColumnOrder.empty()) {
                    for (const auto& [name, column] : metadata.Columns) {
                        Y_UNUSED(name);
                        addColumn(column);
                    }
                } else {
                    for (const auto& name : metadata.ColumnOrder) {
                        addColumn(metadata.Columns.at(name));
                    }
                }
                snapshots.emplace(key, snapshot);
                return snapshot;
            };
            auto resolved = NNewYql::ResolveNames(*NewParserAst, NewParserSettings, lookup);
            namesTrace.SetSuccess(resolved.IsOk());
            if (!resolved.IsOk()) {
                ctx.IssueManager.AddIssues(resolved.Issues);
                return false;
            }
            NewParserNames = std::move(resolved.Names);
            YQL_CLOG(INFO, ProviderKqp) << "New YQL name resolution succeeded";
        }
        // Each native result currently owns a separate importer/IU registry.
        // Named producers can be shared across results, including volatile
        // values. The legacy importer materializes them once across roots.
        if (executableStatements > 1 && !NewParserNames->NamedQueries.empty()) {
            ythrow TNewYqlParserFallback() << "Native multi-result named query sharing";
        }
        {
            TCompileTraceScope typesTrace(trace, TCompileTraceLevels::Step, "Annotate native AST");
            NewParserTypeSettings.CompileTrace = trace;
            NewParserTypes = NNewYql::AnnotateAst(*NewParserAst, *NewParserNames, ctx, *TypesCtx, NewParserTypeSettings);
            typesTrace.SetSuccess(bool(NewParserTypes));
        }
        if (!NewParserTypes) { return false; }
        YQL_CLOG(INFO, ProviderKqp) << "New YQL type annotation succeeded";
        std::optional<TCompileTraceScope> contractTrace(std::in_place, trace, TCompileTraceLevels::Step,
            "Validate native execution contract");
        auto native = std::make_shared<TNativeQuery>();
        const auto lookup = [&](const NNewYql::TTableMetadata& snapshot) -> NNewYql::TRboTable {
            if (snapshot.Cluster != Cluster) {
                ythrow TNewYqlParserFallback() << "Cross-cluster RBO reads are not supported";
            }
            const auto& description = SessionCtx->Tables().ExistingTable(snapshot.Cluster, snapshot.Path);
            const auto& metadata = *description.Metadata;
            if (metadata.PathId.OwnerId() != snapshot.OwnerId || metadata.PathId.TableId() != snapshot.TableId
                || metadata.SchemaVersion != snapshot.SchemaVersion
                || (metadata.Kind != EKikimrTableKind::Datashard && metadata.Kind != EKikimrTableKind::Olap)) {
                ythrow TNewYqlParserFallback() << "Table metadata changed before RBO lowering";
            }
            auto table = Build<TKqpTable>(ctx, TPositionHandle())
                .Path().Build(snapshot.Path)
                .PathId().Build(TStringBuilder() << snapshot.OwnerId << ':' << snapshot.TableId)
                .SysView().Build("").Version().Build(ToString(snapshot.SchemaVersion)).Done().Ptr();
            return {table, metadata.Kind == EKikimrTableKind::Olap ? NYql::EStorageType::ColumnStorage : NYql::EStorageType::RowStorage};
        };

        THashSet<const NNewYql::TSource*> writeOnlyTargets;
        for (const auto& statement : NewParserAst->Statements) {
            const auto* write = std::get_if<NNewYql::TWriteStatement>(&statement.Value);
            if (!write) { continue; }
            const auto& snapshot = *NewParserNames->Tables.at(write->Target.get());
            const auto& metadata = *SessionCtx->Tables().ExistingTable(snapshot.Cluster, snapshot.Path).Metadata;
            for (const auto& index : metadata.Indexes) {
                if (index.ItUsedForWrite() && !index.IsWrittenBySink(SessionCtx->Config().GetEnableIndexStreamWrite())) {
                    ythrow TNewYqlParserFallback() << "Native writes require sink-maintained indexes";
                }
            }
            const bool inputRows = write->Input || !write->Rows.empty();
            if (inputRows) { writeOnlyTargets.insert(write->Target.get()); }
            auto& op = native->Execution.TableOperations.emplace_back();
            op.SetTable(snapshot.Path);
            const auto operation = write->Kind == NNewYql::EWriteKind::Insert
                ? (write->OnConflict == "revert" ? TYdbOperation::InsertRevert : TYdbOperation::InsertAbort)
                : write->Kind == NNewYql::EWriteKind::Upsert ? TYdbOperation::Upsert
                : write->Kind == NNewYql::EWriteKind::Replace ? TYdbOperation::Replace
                : write->Kind == NNewYql::EWriteKind::Update ? (inputRows ? TYdbOperation::UpdateOn : TYdbOperation::Update)
                : inputRows ? TYdbOperation::DeleteOn : TYdbOperation::Delete;
            op.SetOperation(static_cast<ui32>(operation));
        }
        THashSet<TString> tables;
        for (const auto& [source, snapshot] : NewParserNames->Tables) {
            lookup(*snapshot); // Validate identity and provider even for unused definitions.
            if (writeOnlyTargets.contains(source)) { continue; }
            if (tables.insert(snapshot->Path).second) {
                auto& op = native->Execution.TableOperations.emplace_back();
                op.SetTable(snapshot->Path);
                op.SetOperation(static_cast<ui32>(TYdbOperation::Select));
            }
        }
        contractTrace.reset();
        const bool dataQuery = SessionCtx->Query().Type == EKikimrQueryType::Dml;
        const auto cap = dataQuery ? FillSettings.RowsLimitPerWrite : Nothing();
        for (const auto& statement : NewParserAst->Statements) {
            if (const auto* write = std::get_if<NNewYql::TWriteStatement>(&statement.Value)) {
                TCompileTraceScope lowerTrace(trace, TCompileTraceLevels::Step, "Lower native AST to RBO");
                auto root = NNewYql::LowerWriteToRbo(*write, *NewParserAst, *NewParserNames, *NewParserTypes, ctx, lookup,
                    TypesCtx->OrderedColumns, trace);
                root->PlanProps.PhysicalTxGroup = 0;
                root->PlanProps.EffectPriority = 0;
                if (root->PlanProps.WithReturning) {
                    root->PlanProps.ResultIndex = native->Execution.DiscardResults.size();
                    if (cap && *cap < Max<ui64>()) {
                        root->PlanProps.ReturningRowsLimit = Build<TCoUint64>(ctx, root->Pos).Literal().Build(ToString(*cap + 1)).Done().Ptr();
                    }
                    native->Execution.DiscardResults.push_back(false);
                }
                native->Roots.push_back(std::move(root));
                continue;
            }
            const auto* query = std::get_if<NNewYql::TQueryStatement>(&statement.Value);
            if (!query) { continue; }
            TCompileTraceScope lowerTrace(trace, TCompileTraceLevels::Step, "Lower native AST to RBO");
            auto lowered = NNewYql::LowerToRbo(*query->Query, *NewParserAst, *NewParserNames, *NewParserTypes,
                ctx, lookup, TypesCtx->OrderedColumns, trace);
            TVector<TString> columns;
            for (const auto& column : lowered->GetColumns().Items()) { columns.push_back(column.second); }
            auto input = lowered->GetInput();
            if (cap && *cap < Max<ui64>()) {
                auto count = Build<TCoUint64>(ctx, lowered->Pos).Literal().Build(ToString(*cap + 1)).Done().Ptr();
                input = MakeIntrusive<TOpLimit>(input, lowered->Pos, NKqp::TExpression(count, &ctx), EOpPhase::Undefined);
            }
            auto root = MakeIntrusive<TOpRoot>(input, lowered->Pos, lowered->GetColumns(), columns);
            root->PlanProps = std::move(lowered->PlanProps);
            root->PlanProps.ResultIndex = native->Roots.size();
            root->PlanProps.PhysicalTxGroup = 0;
            root->PlanProps.EffectPriority = 0;
            for (const auto& item : IterateSubtreeWithSubplans(root.Get(), root->PlanProps)) {
                item.Current->BindExpressionPlanProps(&root->PlanProps);
            }
            native->Roots.push_back(std::move(root));
            native->Execution.DiscardResults.push_back(false);
        }
        if (native->Roots.empty()) { ythrow TNewYqlParserFallback() << "No native query results"; }
        TCompileTraceScope finalizeTrace(trace, TCompileTraceLevels::Step, "Finalize native execution contract");
        finalizeTrace.SetSuccess(false);
        for (const auto& root : native->Roots) {
            if (root->PlanProps.WithEffects && !root->PlanProps.WithReturning) { continue; }
            native->Results.push_back({root->GetQueryColumns(), cap.GetOrElse(0)});
        }
        // Annotation can clone the same parameter into several expressions.
        // Export one declaration per name, independently of expression identity.
        TExprNode::TListType expressions;
        THashSet<TString> parameterNames;
        for (const auto& [expression, annotation] : NewParserTypes->Expressions) {
            Y_UNUSED(expression);
            VisitExpr(annotation.Lambda, [&](const TExprNode::TPtr& node) {
                if (auto parameter = TMaybeNode<TCoParameter>(node)) {
                    if (parameterNames.insert(TString(parameter.Cast().Name().Value())).second) {
                        expressions.push_back(node);
                    }
                }
                return true;
            });
        }
        auto parameters = ctx.NewList(TPositionHandle(), std::move(expressions));
        TExprNode::TPtr output;
        if (TCollectParametersTransformer(SessionCtx->QueryPtr())(parameters, output, ctx) == IGraphTransformer::TStatus::Error) {
            return false;
        }
        // Static KQP settings apply to the whole compilation, as in the legacy
        // configuration transformer. Defer mutation until all native fallback
        // points have passed, so a retry through legacy starts unmodified.
        for (const auto& statement : NewParserAst->Statements) {
            const auto* pragma = std::get_if<NNewYql::TPragmaStatement>(&statement.Value);
            if (!pragma) { continue; }
            const auto* value = NewParserNames->CostBasedOptimizationPragmas.FindPtr(pragma);
            if (!value) { continue; }
            const auto pos = ctx.AppendPosition(TPosition(pragma->Name.Range.Column + 1,
                pragma->Name.Range.Line, NewParserAst->File));
            if (!SessionCtx->Config().Dispatch(NCommon::ALL_CLUSTERS, "CostBasedOptimizationLevel", *value,
                NCommon::ISettingDispatcher::EStage::STATIC, NCommon::GetErrorCallback(pos, ctx))) {
                return false;
            }
        }
        TransformCtx->NativeQuery = std::move(native);
        YQL_CLOG(INFO, ProviderKqp) << "New YQL native execution contract built";
        for (size_t i = 0; i < TransformCtx->NativeQuery->Roots.size(); ++i) {
            YQL_CLOG(INFO, ProviderKqp) << "New YQL RBO lowering succeeded";
        }
        finalizeTrace.SetSuccess(true);
        return true;
    }

    void TraceNativeFallback(const TNewYqlParserFallback& reason) const {
        if (auto* trace = SessionCtx->Query().CompileTrace.Get()) {
            auto span = trace->StartSpan(TCompileTraceLevels::Step, "New YQL frontend fallback");
            span.Attribute("fallback.reason", TString(reason.what()));
            TCompileTrace::EndSpan(span);
        }
    }

    IGraphTransformer& GetNativeFrontend() {
        if (NativeFrontendTransformer) {
            return *NativeFrontendTransformer;
        }
        TCompileTraceScope trace(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Step,
            "Create native frontend pipeline");
        NativeMetadataTransformer = CreateCompileTraceTransformer(
            CreateKiSourceLoadTableMetadataTransformer(
                CreateKqpGatewayProxy(Gateway, SessionCtx, ActorSystem), SessionCtx, *TypesCtx, ExternalSourceFactory, IsInternalCall),
            [this] { return SessionCtx->Query().CompileTrace.Get(); }, TCompileTraceLevels::Phase, "Load native table metadata");
        NativePrepareTransformer = new TPrepareNativeQueryTransformer(KqpRunner, TransformCtx);
        NativeFrontendTransformer = new TNativeFrontendTransformer([this](TExprNode::TPtr& input, TExprContext& ctx)
            -> std::pair<IGraphTransformer*, bool> {
            TCompileTraceScope buildTrace(SessionCtx->Query().CompileTrace.Get(), TCompileTraceLevels::Phase, "Build native query");
            try {
                if (!BuildNativeQuery(ctx)) {
                    buildTrace.SetSuccess(false);
                    return {nullptr, false};
                }
                return {NativePrepareTransformer.Get(), false};
            } catch (const TNativeMetadataPending&) {
                return {NativeMetadataTransformer.Get(), true};
            } catch (const TNewYqlParserFallback& e) {
                TraceNativeFallback(e);
                YQL_CLOG(INFO, ProviderKqp) << "New YQL frontend fallback: " << e.what();
                TransformCtx->NativeQuery.reset();
                NewParserTypes.reset();
                NewParserNames.reset();
                input = LegacyCompile(ctx);
                if (!input) { return {nullptr, false}; }
                return {&GetYqlTransformer(SessionCtx->Config().GetEnableNewRBO()), false};
            }
        });
        return *NativeFrontendTransformer;
    }

    void SetupYqlTransformer(EKikimrQueryType queryType, TCompileTrace::TPtr trace = {}) {
        SetupSession(queryType, std::move(trace));

        ResultProviderConfig->FillSettings = FillSettings;
        ResultProviderConfig->CommittedResults.clear();
    }

    void SetupDataQueryAstTransformer(const IKikimrQueryExecutor::TExecuteSettings& settings, EKikimrQueryType queryType,
        TCompileTrace::TPtr trace = {}) {
        SetupSession(queryType, std::move(trace));

        ExecuteCtx->Reset(settings);
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TGUCSettings::TPtr GUCSettings;
    const TMaybe<TString> ApplicationName;
    std::shared_ptr<TExprContext> ExprCtxStorage;
    TExprContext* ExprCtx;
    IModuleResolver::TPtr ModuleResolver;
    bool KeepConfigChanges;
    bool IsInternalCall;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TKikimrConfiguration::TPtr Config;

    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistryHolder;
    const NKikimr::NMiniKQL::IFunctionRegistry* FuncRegistry;

    TIntrusivePtr<TTypeAnnotationContext> TypesCtx;
    TAutoPtr<IPlanBuilder> PlanBuilder;
    IDataProvider::TFillSettings FillSettings;
    TIntrusivePtr<TResultProviderConfig> ResultProviderConfig;
    TAutoPtr<IGraphTransformer> YqlTransformer;
    TAutoPtr<IGraphTransformer> YqlTransformerNewRBO;
    TAutoPtr<IGraphTransformer> DataQueryAstTransformer;
    // Names and annotations borrow AST addresses. SetupSession releases native
    // plans and async transformers before their semantic and syntax owners.
    mutable std::unique_ptr<NNewYql::TScript> NewParserAst;
    mutable NNewYql::TNameResolutionSettings NewParserSettings;
    std::unique_ptr<NNewYql::TResolvedNames> NewParserNames;
    mutable NNewYql::TAstTypeAnnotationSettings NewParserTypeSettings;
    std::unique_ptr<NNewYql::TAnnotatedAst> NewParserTypes;
    TAutoPtr<IGraphTransformer> NativeFrontendTransformer;
    TAutoPtr<IGraphTransformer> NativeMetadataTransformer;
    bool NewParserTablesCollected = false;
    TAutoPtr<IGraphTransformer> NativePrepareTransformer;
    mutable bool NativeFrontendActive = false;
    mutable bool LegacyFrontendRequested = false;
    mutable std::function<TExprNode::TPtr(TExprContext&)> LegacyCompile;
    TExprNode::TPtr FakeWorld;
    TKqpAsyncResultBase<IKqpHost::TQueryResult>::TAsyncTransformStatusCallback DataProvidersFinalizer;

    TIntrusivePtr<TExecuteContext> ExecuteCtx;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TIntrusivePtr<IKqpRunner> KqpRunner;
    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory;

    TKqpTempTablesState::TConstPtr TempTablesState;
    NActors::TActorSystem* ActorSystem = nullptr;
    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    bool UsePessimisticLocks = false;
};

} // anonymous namespace

Ydb::Table::QueryStatsCollection::Mode GetStatsMode(NYql::EKikimrStatsMode statsMode) {
    switch (statsMode) {
        case NYql::EKikimrStatsMode::Basic:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC;
        case NYql::EKikimrStatsMode::Full:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL;
        case NYql::EKikimrStatsMode::Profile:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE;
        default:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
    }
}

TIntrusivePtr<IKqpHost> CreateKqpHost(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster,
    const TString& database, TKikimrConfiguration::TPtr config, IModuleResolver::TPtr moduleResolver,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TGUCSettings::TPtr& gUCSettings,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, const TMaybe<TString>& applicationName, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry, bool keepConfigChanges,
    bool isInternalCall, TKqpTempTablesState::TConstPtr tempTablesState, NActors::TActorSystem* actorSystem, NYql::TExprContext* ctx, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    bool usePessimisticLocks)
{
    return MakeIntrusive<TKqpHost>(gateway, cluster, database, gUCSettings, applicationName, config, moduleResolver, federatedQuerySetup, userToken, funcRegistry,
                                   keepConfigChanges, isInternalCall, std::move(tempTablesState), actorSystem, ctx, queryServiceConfig, userRequestContext, usePessimisticLocks);
}

} // namespace NKikimr::NKqp
