#include "kqp_host_impl.h"
#include "kqp_statement_rewrite.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_proposed_by_data.h>
#include <ydb/library/yql/core/services/yql_plan.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/config/yql_config_provider.h>
#include <ydb/library/yql/providers/common/arrow_resolve/yql_simple_arrow_resolver.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_provider.h>
#include <ydb/library/yql/providers/pg/provider/yql_pg_provider_impl.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_state.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {
namespace NKqp {

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
                google::protobuf::Arena::CreateMessage<NKikimrMiniKQL::TResult>(queryResult.ProtobufArenaPtr.get()));
            NKikimrMiniKQL::TResult* result = queryResult.Results.back();

            if (!result->ParseFromArray(resultStr.data(), resultStr.size())) {
                queryResult = ResultFromError<TResult>("Failed to parse run result.");
                return;
            }
        }

        TVector<const TString> queryPlans;
        for (auto id : SessionCtx->Query().ExecutionOrder) {
            auto result = SessionCtx->Query().Results.FindPtr(id);
            if (result) {
                queryPlans.push_back(SerializeAnalyzePlan(result->QueryStats));
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
        prepareResult.AllowCache = KeepInCache;
        prepareResult.CommandTagName = CommandTagName;
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

const NKikimrMiniKQL::TParams* ValidateParameter(const TString& name, const TTypeAnnotationNode& type,
    const TPosition& pos, TQueryData& parameters, TExprContext& ctx)
{
    auto parameter = parameters.GetParameterMiniKqlValue(name);
    if (!parameter) {
        if (type.GetKind() == ETypeAnnotationKind::Optional) {
            NKikimrMiniKQL::TParams param;
            if (!ExportTypeToKikimrProto(type, *param.MutableType(), ctx)) {
                ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Failed to export parameter type: " << name));
                return nullptr;
            }

            parameters.AddMkqlParam(name, param.GetType(), param.GetValue());
            return parameters.GetParameterMiniKqlValue(name);
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

        actualType = ParseTypeFromKikimrProto(parameter->GetType(), ctx);
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
                        if (!ExportTypeToKikimrProto(*expectedType, *paramDesc.MutableType(), ctx)) {
                            ctx.AddError(TIssue(ctx.GetPosition(parameter.Pos()), TStringBuilder()
                                << "Failed to export parameter type: " << name));
                            return nullptr;
                        }

                        return ret;
                    }

                    auto parameterValue = ValidateParameter(TString(name), *expectedType, ctx.GetPosition(parameter.Pos()),
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

                        valueExpr = ParseKikimrProtoValue(parameterValue->GetType(), parameterValue->GetValue(),
                            parameter.Pos(), ctx);
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

class TValidatePreparedTransformer {
public:
    TValidatePreparedTransformer(TIntrusivePtr<TKikimrQueryContext> queryCtx)
        : QueryCtx(queryCtx) {}

    IGraphTransformer::TStatus operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        YQL_ENSURE(!QueryCtx->PrepareOnly);
        YQL_ENSURE(QueryCtx->PreparedQuery);
        YQL_ENSURE(input->Type() == TExprNode::World);
        output = input;

        for (const auto& paramDesc : QueryCtx->PreparedQuery->GetParameters()) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [input, &paramDesc, &ctx]() {
                return MakeIntrusive<TIssue>(YqlIssue(ctx.GetPosition(input->Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
                    << "Failed to parse parameter type: " << paramDesc.GetName()));
            });

            auto expectedType = ParseTypeFromKikimrProto(paramDesc.GetType(), ctx);
            if (!expectedType) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!ValidateParameter(paramDesc.GetName(), *expectedType, TPosition(), *(QueryCtx->QueryData), ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        return IGraphTransformer::TStatus::Ok;
    }

    static TAutoPtr<IGraphTransformer> Sync(TIntrusivePtr<TKikimrQueryContext> queryCtx) {
        return CreateFunctorTransformer(TValidatePreparedTransformer(queryCtx));
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

                bool useGenericQuery = ShouldUseGenericQuery(dataQueryBlocks);
                bool useScanQuery = ShouldUseScanQuery(dataQueryBlocks, settings);

                IKqpGateway::TAstQuerySettings querySettings;
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
                                querySettings, txSettings);
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
                                querySettings, txSettings);
                        }
                    }
                    break;
                case EKikimrQueryType::YqlScriptStreaming:
                    if (useGenericQuery) {
                        Ydb::Table::TransactionSettings txSettings;
                        txSettings.mutable_serializable_read_write();

                        future = Gateway->StreamExecGenericQuery(Cluster, SessionCtx->Query().PreparingQuery->GetText(), CollectParameters(query),
                                querySettings, txSettings, SessionCtx->Query().ReplyTarget);
                    } else if (useScanQuery) {
                        future = Gateway->StreamExecScanQueryAst(Cluster, queryAstStr, CollectParameters(query),
                            querySettings, SessionCtx->Query().ReplyTarget, SessionCtx->Query().RpcCtx);
                    } else {
                        Ydb::Table::TransactionSettings txSettings;
                        txSettings.mutable_serializable_read_write();

                        future = Gateway->StreamExecDataQueryAst(Cluster, queryAstStr, CollectParameters(query),
                            querySettings, txSettings, SessionCtx->Query().ReplyTarget);
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
                auto paramValue = queryCtx->QueryData->GetParameterMiniKqlValue(name);
                YQL_ENSURE(paramValue);
                result->AddMkqlParam(name, paramValue->GetType(), paramValue->GetValue());
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
                || node.Maybe<TGenDataSource>();

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
            if (tableData.Metadata->IsOlap() || !tableData.Metadata->SysView.empty()) {
                // Always use ScanQuery for queries with OLAP and system tables.
                return true;
            }
        }

        if (!SessionCtx->Config().FeatureFlags.GetEnableImplicitScanQueryInScripts()) {
            return false;
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
        TVector<TExprNode::TPtr> QueryExprs;
        bool KeepInCache;
        TMaybe<TString> CommandTagName;
    };

    TKqpHost(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster, const TString& database, const TGUCSettings::TPtr& gUCSettings,
        const TMaybe<TString>& applicationName, TKikimrConfiguration::TPtr config, IModuleResolver::TPtr moduleResolver,
        std::optional<TKqpFederatedQuerySetup> federatedQuerySetup, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry, bool keepConfigChanges, bool isInternalCall,
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr, NActors::TActorSystem* actorSystem = nullptr,
        NYql::TExprContext* ctx = nullptr, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig = NKikimrConfig::TQueryServiceConfig(),
        const TIntrusivePtr<TUserRequestContext>& userRequestContext = nullptr)
        : Gateway(gateway)
        , Cluster(cluster)
        , GUCSettings(gUCSettings)
        , ApplicationName(applicationName)
        , ExprCtxStorage(ctx ? nullptr : new TExprContext())
        , ExprCtx(ctx ? ctx : ExprCtxStorage.Get())
        , ModuleResolver(moduleResolver)
        , KeepConfigChanges(keepConfigChanges)
        , IsInternalCall(isInternalCall)
        , FederatedQuerySetup(federatedQuerySetup)
        , SessionCtx(new TKikimrSessionContext(funcRegistry, config, TAppData::TimeProvider, TAppData::RandomProvider, userToken, nullptr, userRequestContext))
        , Config(config)
        , TypesCtx(MakeIntrusive<TTypeAnnotationContext>())
        , PlanBuilder(CreatePlanBuilder(*TypesCtx))
        , FakeWorld(ctx ? nullptr : ExprCtx->NewWorld(TPosition()))
        , ExecuteCtx(MakeIntrusive<TExecuteContext>())
        , ActorSystem(actorSystem ? actorSystem : NActors::TActivationContext::ActorSystem())
        , QueryServiceConfig(queryServiceConfig)
    {
        if (funcRegistry) {
            FuncRegistry = funcRegistry;
        } else {
            FuncRegistryHolder = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
            FuncRegistry = FuncRegistryHolder.Get();
        }

        SessionCtx->SetDatabase(database);
        SessionCtx->SetCluster(cluster);
        if (tempTablesState) {
            SessionCtx->SetSessionId(tempTablesState->SessionId);
            SessionCtx->SetTempTables(std::move(tempTablesState));
        }

        if (FederatedQuerySetup) {
            ExternalSourceFactory = NExternalSource::CreateExternalSourceFactory({},
                                                                                 ActorSystem,
                                                                                 FederatedQuerySetup->S3GatewayConfig.GetGeneratorPathsLimit(),
                                                                                 FederatedQuerySetup ? FederatedQuerySetup->CredentialsFactory : nullptr,
                                                                                 Config->FeatureFlags.GetEnableExternalSourceSchemaInference(),
                                                                                 FederatedQuerySetup->S3GatewayConfig.GetAllowLocalFiles());
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

    IAsyncQueryResultPtr PrepareGenericScript(const TKqpQueryRef& query, const TPrepareSettings& settings) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, settings] (TExprContext& ctx) mutable {
                return PrepareQueryInternal(query, nullptr, EKikimrQueryType::Script, settings, ctx);
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

    IAsyncQueryResultPtr PrepareScanQuery(const TKqpQueryRef& query, bool isSql, const TPrepareSettings& /*settings*/) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql] (TExprContext& ctx) mutable {
                return PrepareScanQueryInternal(query, isSql, ctx);
            });
    }

    TQueryResult SyncPrepareScanQuery(const TKqpQueryRef& query, bool isSql, const TPrepareSettings& settings) override {
        return CheckedSyncProcessQuery(
            [this, &query, isSql, settings] () mutable {
                return PrepareScanQuery(query, isSql, settings);
            });
    }

private:
    TCompileExprResult CompileQuery(const TKqpQueryRef& query, bool isSql, TExprContext& ctx, TMaybe<TSqlVersion>& sqlVersion,
        TKqpTranslationSettingsBuilder& settingsBuilder) const
    {
        TCompileExprResult result;
        std::shared_ptr<NYql::TAstParseResult> queryAst;
        if (!query.AstResult) {
            settingsBuilder.SetKqpTablePathPrefix(SessionCtx->Config()._KqpTablePathPrefix.Get().GetRef())
                .SetIsEnableExternalDataSources(SessionCtx->Config().FeatureFlags.GetEnableExternalDataSources())
                .SetIsEnablePgConstsToParams(SessionCtx->Config().EnablePgConstsToParams)
                .SetQueryParameters(query.ParameterTypes)
                .SetApplicationName(ApplicationName);
            auto astRes = ParseQuery(query.Text, isSql, sqlVersion, TypesCtx->DeprecatedSQL, ctx, settingsBuilder, result.KeepInCache, result.CommandTagName);
            if (astRes.ActualSyntaxType == NYql::ESyntaxType::Pg) {
                SessionCtx->Config().IndexAutoChooserMode = NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode::TTableServiceConfig_EIndexAutoChooseMode_MAX_USED_PREFIX;
            }
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

        YQL_ENSURE(queryAst->Root);
        TExprNode::TPtr queryExpr;
        if (!CompileExpr(*queryAst->Root, queryExpr, ctx, ModuleResolver.get(), nullptr)) {
            return result;
        }

        YQL_CLOG(INFO, ProviderKqp) << "Compiled query:\n" << KqpExprToPrettyString(*queryExpr, ctx);

        if (Config->EnableCreateTableAs) {
            result.QueryExprs = RewriteExpression(queryExpr, ctx, *TypesCtx, SessionCtx, Cluster);

            for (const auto& resultPart : result.QueryExprs) {
                YQL_CLOG(INFO, ProviderKqp) << "Splitted Compiled query part:\n" << KqpExprToPrettyString(*resultPart, ctx);
            }

            return result;
        }

        result.QueryExprs = {queryExpr};
        return result;
    }

    TSplitResult SplitQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) override {
        SetupYqlTransformer(EKikimrQueryType::Query);
        auto sqlVersion = SetupQueryParameters(settings, EKikimrQueryType::Query);

        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, query.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder
            .SetSqlAutoCommit(false)
            .SetUsePgParser(settings.UsePgParser);
        auto compileResult = CompileYqlQuery(query, /* isSql */ true, *ExprCtx, sqlVersion, settingsBuilder, settings.PerStatementResult);

        return TSplitResult{
            .Ctx = std::move(ExprCtxStorage),
            .Exprs = std::move(compileResult.QueryExprs),
            .World = std::move(FakeWorld),
        };
    }

    TCompileExprResult CompileYqlQuery(const TKqpQueryRef& query, bool isSql, TExprContext& ctx, TMaybe<TSqlVersion>& sqlVersion,
        TKqpTranslationSettingsBuilder& settingsBuilder, bool perStatementResult) const
    {
        auto compileResult = CompileQuery(query, isSql, ctx, sqlVersion, settingsBuilder);
        if (!compileResult.QueryExprs) {
            return compileResult;
        }

        if (!isSql) {
            return compileResult;
        }

        // Currently used only for create table as
        if (!perStatementResult && compileResult.QueryExprs.size() > 1) {
            ctx.AddError(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_REQUEST,
                "Query can be executed only in per-statement mode (NoTx)"));
            compileResult.QueryExprs = {};
            return compileResult;
        } else if (compileResult.QueryExprs.size() > 1) {
            return compileResult;
        }

        if (TMaybeNode<TCoCommit>(compileResult.QueryExprs.front()) && TCoCommit(compileResult.QueryExprs.front()).DataSink().Maybe<TKiDataSink>()) {
            return compileResult;
        }

        compileResult.QueryExprs = {Build<TCoCommit>(ctx, compileResult.QueryExprs.front()->Pos())
            .World(compileResult.QueryExprs.front())
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
    static bool ParseParameters(NKikimrMiniKQL::TParams&& parameters, TQueryData& map,
        TExprContext& ctx)
    {
        if (!parameters.HasType()) {
            return true;
        }

        if (parameters.GetType().GetKind() != NKikimrMiniKQL::Struct) {
            ctx.AddError(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_REQUEST,
                "Expected struct as query parameters type"));
            return false;
        }

        auto& structType = *parameters.MutableType()->MutableStruct();
        for (ui32 i = 0; i < structType.MemberSize(); ++i) {
            auto memberName = structType.GetMember(i).GetName();

            if (parameters.GetValue().StructSize() <= i) {
                ctx.AddError(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Missing value for parameter: " << memberName));
                return false;
            }

            auto success = map.AddMkqlParam(
                memberName, structType.GetMember(i).GetType(), parameters.GetValue().GetStruct(i));
            if (!success) {
                ctx.AddError(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Duplicate parameter: " << memberName));
                return false;
            }
        }

        return true;
    }

    IAsyncQueryResultPtr ExecuteSchemeQueryInternal(const TKqpQueryRef& query, bool isSql, const TExecSettings& settings, TExprContext& ctx) {
        SetupYqlTransformer(EKikimrQueryType::Ddl);

        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, query.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false)
            .SetUsePgParser(settings.UsePgParser);
        auto compileResult = CompileYqlQuery(query, isSql, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        return MakeIntrusive<TAsyncExecuteYqlResult>(compileResult.QueryExprs.front().Get(), ctx, *YqlTransformer, Cluster, SessionCtx,
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
        return PrepareScanQueryInternal(query, isSql, ctx);
    }

    IAsyncQueryResultPtr PrepareDataQueryInternal(const TKqpQueryRef& query, const TPrepareSettings& settings,
        TExprContext& ctx)
    {
        SetupYqlTransformer(EKikimrQueryType::Dml);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }
        if (settings.IsInternalCall) {
            SessionCtx->Query().IsInternalCall = *settings.IsInternalCall;
        }

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, query.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false);
        auto compileResult = CompileYqlQuery(query, /* isSql */ true, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        return MakeIntrusive<TAsyncPrepareYqlResult>(compileResult.QueryExprs.front().Get(), ctx, *YqlTransformer, SessionCtx->QueryPtr(),
            query.Text, sqlVersion, TransformCtx, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr PrepareDataQueryAstInternal(const TKqpQueryRef& queryAst, const TPrepareSettings& settings,
        TExprContext& ctx)
    {
        IKikimrQueryExecutor::TExecuteSettings execSettings;
        SetupDataQueryAstTransformer(execSettings, EKikimrQueryType::Dml);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }
        if (settings.IsInternalCall) {
            SessionCtx->Query().IsInternalCall = *settings.IsInternalCall;
        }

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, queryAst.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false);
        auto compileResult = CompileYqlQuery(queryAst, false, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }

        YQL_ENSURE(compileResult.QueryExprs.size() == 1);
        YQL_ENSURE(!sqlVersion);

        return MakeIntrusive<TAsyncExecuteKqlResult>(compileResult.QueryExprs.front().Get(), ctx, *DataQueryAstTransformer,
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

        TMaybe<TSqlVersion> sqlVersion = settings.SyntaxVersion;
        if (!sqlVersion) {
            sqlVersion = 1;
        }

        return sqlVersion;
    }

    IAsyncQueryResultPtr PrepareQueryInternal(const TKqpQueryRef& query, NYql::TExprNode::TPtr expr, EKikimrQueryType queryType,
        const TPrepareSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(queryType);
        auto sqlVersion = SetupQueryParameters(settings, queryType);

        if (!expr) {
            TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, query.Text, SessionCtx->Config().BindingsMode, GUCSettings);
            settingsBuilder.SetSqlAutoCommit(false)
                .SetUsePgParser(settings.UsePgParser);
            auto compileResult = CompileYqlQuery(query, /* isSql */ true, ctx, sqlVersion, settingsBuilder, settings.PerStatementResult);
            if (compileResult.QueryExprs.empty()) {
                return nullptr;
            }

            if (compileResult.QueryExprs.size() > 1) {
                return MakeIntrusive<TAsyncPrepareNeedToSplitYqlResult>();
            } else {
                return MakeIntrusive<TAsyncPrepareYqlResult>(compileResult.QueryExprs.front().Get(), ctx, *YqlTransformer, SessionCtx->QueryPtr(),
                    query.Text, sqlVersion, TransformCtx, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
            }
        } else {
            return MakeIntrusive<TAsyncPrepareYqlResult>(expr, ctx, *YqlTransformer, SessionCtx->QueryPtr(),
                query.Text, sqlVersion, TransformCtx, false, Nothing(), DataProvidersFinalizer);
        }
    }

    IAsyncQueryResultPtr PrepareScanQueryInternal(const TKqpQueryRef& query, bool isSql, TExprContext& ctx,
        EKikimrStatsMode statsMode = EKikimrStatsMode::None)
    {
        return isSql
            ? PrepareScanQueryInternal(query, ctx, statsMode)
            : PrepareScanQueryAstInternal(query, ctx);
    }

    IAsyncQueryResultPtr PrepareScanQueryInternal(const TKqpQueryRef& query, TExprContext& ctx,
        EKikimrStatsMode statsMode = EKikimrStatsMode::None)
    {
        SetupYqlTransformer(EKikimrQueryType::Scan);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().StatsMode = statsMode;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion = 1;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, query.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false);
        auto compileResult = CompileYqlQuery(query, true, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);


        return MakeIntrusive<TAsyncPrepareYqlResult>(compileResult.QueryExprs.front().Get(), ctx, *YqlTransformer, SessionCtx->QueryPtr(),
            query.Text, sqlVersion, TransformCtx, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr PrepareScanQueryAstInternal(const TKqpQueryRef& queryAst, TExprContext& ctx) {
        IKikimrQueryExecutor::TExecuteSettings settings;
        SetupDataQueryAstTransformer(settings, EKikimrQueryType::Scan);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, queryAst.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(false);
        auto compileResult = CompileYqlQuery(queryAst, false, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }

        YQL_ENSURE(!sqlVersion);
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        return MakeIntrusive<TAsyncExecuteKqlResult>(compileResult.QueryExprs.front().Get(), ctx, *DataQueryAstTransformer,
            SessionCtx, *ExecuteCtx, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr ExecuteYqlScriptInternal(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const TExecScriptSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(EKikimrQueryType::YqlScript);

        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, script.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true)
            .SetUsePgParser(settings.UsePgParser);
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        (SessionCtx->Query().QueryData)->ParseParameters(parameters);

        return MakeIntrusive<TAsyncExecuteYqlResult>(compileResult.QueryExprs.front().Get(), ctx, *YqlTransformer, Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr StreamExecuteYqlScriptInternal(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const NActors::TActorId& target,const TExecScriptSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(EKikimrQueryType::YqlScriptStreaming);

        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().RpcCtx = settings.RpcCtx;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().ReplyTarget = target;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, script.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true)
            .SetUsePgParser(settings.UsePgParser);
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        (SessionCtx->Query().QueryData)->ParseParameters(parameters);

        return MakeIntrusive<TAsyncExecuteYqlResult>(compileResult.QueryExprs.front().Get(), ctx, *YqlTransformer, Cluster, SessionCtx,
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
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, script.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true);
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        auto transformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddIOAnnotation()
            .AddTypeAnnotation()
            .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
            .Build(false);

        return MakeIntrusive<TAsyncValidateYqlResult>(compileResult.QueryExprs.front().Get(), SessionCtx, ctx, transformer, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer);
    }

    IAsyncQueryResultPtr ExplainYqlScriptInternal(const TKqpQueryRef& script, TExprContext& ctx) {
        SetupYqlTransformer(EKikimrQueryType::YqlScript);

        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().SuppressDdlChecks = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparingQuery->SetText(script.Text);

        TMaybe<TSqlVersion> sqlVersion;
        TKqpTranslationSettingsBuilder settingsBuilder(SessionCtx->Query().Type, SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(), Cluster, script.Text, SessionCtx->Config().BindingsMode, GUCSettings);
        settingsBuilder.SetSqlAutoCommit(true);
        auto compileResult = CompileYqlQuery(script, true, ctx, sqlVersion, settingsBuilder, false);
        if (compileResult.QueryExprs.empty()) {
            return nullptr;
        }
        YQL_ENSURE(compileResult.QueryExprs.size() == 1);

        return MakeIntrusive<TAsyncExplainYqlResult>(compileResult.QueryExprs.front().Get(), SessionCtx, ctx, YqlTransformer,
            *PlanBuilder, sqlVersion, compileResult.KeepInCache, compileResult.CommandTagName, DataProvidersFinalizer, true /* UseDqExplain */);
    }

    void InitS3Provider(EKikimrQueryType queryType) {
        auto state = MakeIntrusive<NYql::TS3State>();
        state->Types = TypesCtx.Get();
        state->FunctionRegistry = FuncRegistry;
        state->CredentialsFactory = FederatedQuerySetup->CredentialsFactory;
        state->Configuration->WriteThroughDqIntegration = true;
        state->Configuration->AllowAtomicUploadCommit = queryType == EKikimrQueryType::Script;
        state->Configuration->Init(FederatedQuerySetup->S3GatewayConfig, TypesCtx);
        state->Gateway = FederatedQuerySetup->HttpGateway;
        state->GatewayRetryPolicy = NYql::GetHTTPDefaultRetryPolicy(NYql::THttpRetryPolicyOptions{.RetriedCurlCodes = NYql::FqRetriedCurlCodes()});
        state->ExecutorPoolId = AppData()->UserPoolId;
        state->ActorSystem = ActorSystem;

        auto dataSource = NYql::CreateS3DataSource(state);
        auto dataSink = NYql::CreateS3DataSink(state);

        TypesCtx->AddDataSource(NYql::S3ProviderName, std::move(dataSource));
        TypesCtx->AddDataSink(NYql::S3ProviderName, std::move(dataSink));
    }

    void InitGenericProvider() {
        if (!FederatedQuerySetup->ConnectorClient) {
            return;
        }

        auto state = MakeIntrusive<NYql::TGenericState>(
            TypesCtx.Get(),
            FuncRegistry,
            FederatedQuerySetup->DatabaseAsyncResolver,
            FederatedQuerySetup->CredentialsFactory,
            FederatedQuerySetup->ConnectorClient,
            FederatedQuerySetup->GenericGatewayConfig
        );

        TypesCtx->AddDataSource(NYql::GenericProviderName, NYql::CreateGenericDataSource(state));
        TypesCtx->AddDataSink(NYql::GenericProviderName, NYql::CreateGenericDataSink(state));
    }

    void InitYtProvider() {
        TString userName = CreateGuidAsString();
        if (SessionCtx->GetUserToken() && SessionCtx->GetUserToken()->GetUserSID()) {
            userName = SessionCtx->GetUserToken()->GetUserSID();
        }

        TString sessionId = CreateGuidAsString();
        auto [ytState, statWriter] = CreateYtNativeState(FederatedQuerySetup->YtGateway, userName, sessionId, &FederatedQuerySetup->YtGatewayConfig, TypesCtx);

        ytState->PassiveExecution = true;
        ytState->Gateway->OpenSession(
            IYtGateway::TOpenSessionOptions(sessionId)
                .UserName(userName)
                .RandomProvider(TAppData::RandomProvider)
                .TimeProvider(TAppData::TimeProvider)
                .StatWriter(statWriter)
                .CreateOperationTracker(false)
        );

        TypesCtx->AddDataSource(YtProviderName, CreateYtDataSource(ytState));
        TypesCtx->AddDataSink(YtProviderName, CreateYtDataSink(ytState));

        DataProvidersFinalizer = [ytGateway = FederatedQuerySetup->YtGateway, sessionId](const NYql::IGraphTransformer::TStatus&) {
            return ytGateway->CloseSession(NYql::IYtGateway::TCloseSessionOptions(sessionId));
        };
    }

    void InitPgProvider() {
        auto state = MakeIntrusive<NYql::TPgState>();
        state->Types = TypesCtx.Get();

        TypesCtx->AddDataSource(NYql::PgProviderName, NYql::CreatePgDataSource(state));
        TypesCtx->AddDataSink(NYql::PgProviderName, NYql::CreatePgDataSink(state));
    }

    void Init(EKikimrQueryType queryType) {
        TransformCtx = MakeIntrusive<TKqlTransformContext>(Config, SessionCtx->QueryPtr(), SessionCtx->TablesPtr());
        KqpRunner = CreateKqpRunner(Gateway, Cluster, TypesCtx, SessionCtx, TransformCtx, *FuncRegistry, ActorSystem);

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

        bool addExternalDataSources = queryType == EKikimrQueryType::Script || queryType == EKikimrQueryType::Query
            || (queryType == EKikimrQueryType::YqlScript || queryType == EKikimrQueryType::YqlScriptStreaming) && AppData()->FeatureFlags.GetEnableExternalDataSources();
        if (addExternalDataSources && FederatedQuerySetup) {
            InitS3Provider(queryType);
            InitGenericProvider();
            if (FederatedQuerySetup->YtGateway) {
                InitYtProvider();
            }
        }

        InitPgProvider();

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
                || settingName == "DisableOrderedColumns"
                || settingName == "Warning"
                || settingName == "UseBlocks"
                || settingName == "BlockEngine"
                || settingName == "FilterPushdownOverJoinOptionalSide"
                || settingName == "DisableFilterPushdownOverJoinOptionalSide"
                || settingName == "RotateJoinTree"
                || settingName == "TimeOrderRecoverDelay"
                || settingName == "TimeOrderRecoverAhead"
                || settingName == "TimeOrderRecoverRowLimit"
                || settingName == "MatchRecognizeStream"
                ;
        };
        auto configProvider = CreateConfigProvider(*TypesCtx, gatewaysConfig, {}, allowSettings);
        TypesCtx->AddDataSource(ConfigProviderName, configProvider);
        TypesCtx->MatchRecognize = QueryServiceConfig.GetEnableMatchRecognize();

        YQL_ENSURE(TypesCtx->Initialize(*ExprCtx));

        YqlTransformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("YqlTransformer", NYql::NLog::EComponent::ProviderKqp,
                NYql::NLog::ELevel::TRACE), "LogYqlTransform")
            .AddPreTypeAnnotation()
            .AddExpressionEvaluation(*FuncRegistry)
            .Add(new TFailExpressionEvaluation(queryType), "FailExpressionEvaluation")
            .AddIOAnnotation(false)
            .AddTypeAnnotation()
            .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
            .AddPostTypeAnnotation()
            .AddOptimization(true, false)
            .Add(GetDqIntegrationPeepholeTransformer(true, TypesCtx), "DqIntegrationPeephole")
            .Add(TLogExprTransformer::Sync("Optimized expr"), "LogExpr")
            .AddRun(&NullProgressWriter)
            .Build();

        DataQueryAstTransformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .AddIntentDeterminationTransformer()
            .AddTableMetadataLoaderTransformer()
            .AddTypeAnnotationTransformer()
            .Add(new TPrepareDataQueryAstTransformer(Cluster, ExecuteCtx, SessionCtx->QueryPtr(), KqpRunner),
                "PrepareDataQueryAst")
            .Build();
    }

    void SetupSession(EKikimrQueryType queryType) {
        SessionCtx->Reset(KeepConfigChanges);
        SessionCtx->Query().Type = queryType;

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

    void SetupYqlTransformer(EKikimrQueryType queryType) {
        SetupSession(queryType);

        YqlTransformer->Rewind();

        ResultProviderConfig->FillSettings = FillSettings;
        ResultProviderConfig->CommittedResults.clear();
    }

    void SetupDataQueryAstTransformer(const IKikimrQueryExecutor::TExecuteSettings& settings, EKikimrQueryType queryType) {
        SetupSession(queryType);

        DataQueryAstTransformer->Rewind();

        ExecuteCtx->Reset(settings);
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    TGUCSettings::TPtr GUCSettings;
    const TMaybe<TString> ApplicationName;
    THolder<TExprContext> ExprCtxStorage;
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
    TAutoPtr<IGraphTransformer> DataQueryAstTransformer;
    TExprNode::TPtr FakeWorld;
    TKqpAsyncResultBase<IKqpHost::TQueryResult>::TAsyncTransformStatusCallback DataProvidersFinalizer;

    TIntrusivePtr<TExecuteContext> ExecuteCtx;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
    TIntrusivePtr<IKqpRunner> KqpRunner;
    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory;

    TKqpTempTablesState::TConstPtr TempTablesState;
    NActors::TActorSystem* ActorSystem = nullptr;
    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
};

} // namespace

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
    bool isInternalCall, TKqpTempTablesState::TConstPtr tempTablesState, NActors::TActorSystem* actorSystem, NYql::TExprContext* ctx, const TIntrusivePtr<TUserRequestContext>& userRequestContext)
{
    return MakeIntrusive<TKqpHost>(gateway, cluster, database, gUCSettings, applicationName, config, moduleResolver, federatedQuerySetup, userToken, funcRegistry,
                                   keepConfigChanges, isInternalCall, std::move(tempTablesState), actorSystem, ctx, queryServiceConfig, userRequestContext);
}

} // namespace NKqp
} // namespace NKikimr
