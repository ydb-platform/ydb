#include "kqp_host_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/prepare/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_proposed_by_data.h>
#include <ydb/library/yql/core/services/yql_plan.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/config/yql_config_provider.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/sql/sql.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NNodes;
using namespace NThreading;

using TSqlVersion = ui16;

namespace {

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

    void Init(bool discard, const TString& label, TMaybe<TPosition> pos) override {
        Discard = discard;
        Y_UNUSED(label);
        Y_UNUSED(pos);
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

void FillAstAndPlan(IKqpHost::TQueryResult& queryResult, const NKikimrKqp::TPreparedQuery& query) {
    YQL_ENSURE(query.KqlsSize() == 1);

    queryResult.QueryAst = query.GetKqls(0).GetAst();
    queryResult.QueryPlan = query.GetKqls(0).GetPlan();

    if (queryResult.QueryPlan.empty()) {
        TStringStream planStream;
        NYson::TYsonWriter writer(&planStream, NYson::EYsonFormat::Binary);
        writer.OnEntity();
        queryResult.QueryPlan = planStream.Str();
    }
}

/*
 * Validate YqlScript.
 */
class TAsyncValidateYqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncValidateYqlResult(TExprNode* queryRoot, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TExprContext& exprCtx, TAutoPtr<IGraphTransformer> transformer, TMaybe<TSqlVersion> sqlVersion)
        : TKqpAsyncResultBase(queryRoot, exprCtx, *transformer.Get())
        , SessionCtx(sessionCtx)
        , Transformer(transformer)
        , SqlVersion(sqlVersion) {}

    void FillResult(TResult& validateResult) const override {
        YQL_ENSURE(SessionCtx->Query().PrepareOnly);
        validateResult.PreparedQuery.reset(SessionCtx->Query().PreparingQuery.release());
        validateResult.SqlVersion = SqlVersion;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TAutoPtr<IGraphTransformer> Transformer;
    TMaybe<TSqlVersion> SqlVersion;
};

/*
 * Explain Yql/YqlScript.
 */
class TAsyncExplainYqlResult : public TKqpAsyncResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExplainYqlResult(TExprNode* queryRoot, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TExprContext& exprCtx, TAutoPtr<IGraphTransformer> transformer,
        IPlanBuilder& planBuilder, TMaybe<TSqlVersion> sqlVersion, bool useDqExplain = false)
        : TKqpAsyncResultBase(queryRoot, exprCtx, *transformer.Get())
        , SessionCtx(sessionCtx)
        , Transformer(transformer)
        , PlanBuilder(planBuilder)
        , SqlVersion(sqlVersion)
        , UseDqExplain(useDqExplain) {}

    void FillResult(TResult& queryResult) const override {
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
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TAutoPtr<IGraphTransformer> Transformer;
    IPlanBuilder& PlanBuilder;
    TMaybe<TSqlVersion> SqlVersion;
    bool UseDqExplain;
};

/*
 * Execute Yql/SchemeQuery/YqlScript.
 */
class TAsyncExecuteYqlResult : public TKqpAsyncExecuteResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExecuteYqlResult(TExprNode* queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        const TString& cluster, TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const TResultProviderConfig& resultProviderConfig, IPlanBuilder& planBuilder,
        TMaybe<TSqlVersion> sqlVersion)
        : TKqpAsyncExecuteResultBase(queryRoot, exprCtx, transformer, sessionCtx->TxPtr())
        , Cluster(cluster)
        , SessionCtx(sessionCtx)
        , ResultProviderConfig(resultProviderConfig)
        , PlanBuilder(planBuilder)
        , SqlVersion(sqlVersion) {}

    void FillResult(TResult& queryResult) const override {
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
    }

private:
    TString Cluster;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    const TResultProviderConfig& ResultProviderConfig;
    IPlanBuilder& PlanBuilder;
    TMaybe<TSqlVersion> SqlVersion;
};

/*
 * Execute prepared Yql/ScanQuery/DataQuery.
 */
class TAsyncExecutePreparedResult : public TKqpAsyncExecuteResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExecutePreparedResult(TExprNode* queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx, const IDataProvider::TFillSettings& fillSettings,
        TIntrusivePtr<TExecuteContext> executeCtx, std::shared_ptr<const NKikimrKqp::TPreparedQuery> query,
        TMaybe<TSqlVersion> sqlVersion)
        : TKqpAsyncExecuteResultBase(queryRoot, exprCtx, transformer, sessionCtx->TxPtr())
        , SessionCtx(sessionCtx)
        , FillSettings(fillSettings)
        , ExecuteCtx(executeCtx)
        , Query(query)
        , SqlVersion(sqlVersion) {}

    void FillResult(TResult& queryResult) const override {
        for (const auto& result : SessionCtx->Query().PreparedQuery->GetResults()) {
            YQL_ENSURE(result.GetKqlIndex() < ExecuteCtx->QueryResults.size());
            const auto& execResult = ExecuteCtx->QueryResults[result.GetKqlIndex()];

            YQL_ENSURE(result.GetResultIndex() < execResult.Results.size());
            const auto& resultValue = execResult.Results[result.GetResultIndex()];

            auto fillSettings = FillSettings;
            if (result.GetRowsLimit()) {
                fillSettings.RowsLimitPerWrite = result.GetRowsLimit();
            }
            TVector<TString> columnHints(result.GetColumnHints().begin(), result.GetColumnHints().end());
            auto protoResult = KikimrResultToProto(*resultValue, columnHints, fillSettings,
                queryResult.ProtobufArenaPtr.get());

            queryResult.Results.push_back(protoResult);
        }

        if (!SessionCtx->Query().PrepareOnly && SessionCtx->Query().PreparedQuery->KqlsSize() == 1) {
            FillAstAndPlan(queryResult, *SessionCtx->Query().PreparedQuery);
        }
        queryResult.QueryAst = SessionCtx->Query().PreparedQuery->GetPhysicalQuery().GetQueryAst();

        if (Query) {
            queryResult.PreparedQuery = Query;
        }

        /*
         * Set stats and plan for DataQuery. In case of ScanQuery they will be set
         * later in TStreamExecuteScanQueryRPC.
         */
        if (ExecuteCtx->QueryResults.size() == 1) {
            auto& execResult = ExecuteCtx->QueryResults[0];
            queryResult.QueryStats.Swap(&execResult.QueryStats);
            queryResult.QueryPlan = SerializeAnalyzePlan(queryResult.QueryStats);
        }

        queryResult.SqlVersion = SqlVersion;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    IDataProvider::TFillSettings FillSettings;
    TIntrusivePtr<TExecuteContext> ExecuteCtx;
    mutable std::shared_ptr<const NKikimrKqp::TPreparedQuery> Query;
    TMaybe<TSqlVersion> SqlVersion;
};

/*
 * Prepare ScanQuery/DataQuery by AST (when called through scripting).
 */
class TAsyncExecuteKqlResult : public TKqpAsyncExecuteResultBase<IKqpHost::TQueryResult> {
public:
    using TResult = IKqpHost::TQueryResult;

    TAsyncExecuteKqlResult(TExprNode* queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx, TExecuteContext& executeCtx)
        : TKqpAsyncExecuteResultBase(queryRoot, exprCtx, transformer, sessionCtx->TxPtr())
        , SessionCtx(sessionCtx)
        , ExecuteCtx(executeCtx) {}

    void FillResult(TResult& queryResult) const override {
        YQL_ENSURE(ExecuteCtx.QueryResults.size() == 1);
        queryResult = std::move(ExecuteCtx.QueryResults[0]);
        queryResult.QueryPlan = SerializeExplainPlan(queryResult.PreparingQuery->GetPhysicalQuery());
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

    TAsyncPrepareYqlResult(TExprNode* queryRoot, TExprContext& exprCtx, IGraphTransformer& transformer,
        TIntrusivePtr<TKikimrQueryContext> queryCtx, const TString& queryText, TMaybe<TSqlVersion> sqlVersion)
        : TKqpAsyncResultBase(queryRoot, exprCtx, transformer)
        , QueryCtx(queryCtx)
        , QueryText(queryText)
        , SqlVersion(sqlVersion) {}

    void FillResult(TResult& prepareResult) const override {
        YQL_ENSURE(QueryCtx->PrepareOnly);
        YQL_ENSURE(QueryCtx->PreparingQuery);

        prepareResult.PreparingQuery = std::move(QueryCtx->PreparingQuery);
        prepareResult.PreparingQuery->SetText(std::move(QueryText));
        prepareResult.SqlVersion = SqlVersion;

        if (prepareResult.PreparingQuery->GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1) {
            prepareResult.QueryPlan = SerializeExplainPlan(prepareResult.PreparingQuery->GetPhysicalQuery());
            prepareResult.QueryAst = prepareResult.PreparingQuery->GetPhysicalQuery().GetQueryAst();
        } else {
            FillAstAndPlan(prepareResult, *prepareResult.PreparingQuery);
        }

        prepareResult.QueryTraits = QueryCtx->QueryTraits;
    }

private:
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TString QueryText;
    TMaybe<TSqlVersion> SqlVersion;
};

class TFailExpressionEvaluation : public TSyncTransformerBase {
public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

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
            YQL_ENSURE(!ExecuteCtx->Settings.IsolationLevel);

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

NKikimrMiniKQL::TParams* ValidateParameter(const TString& name, const TTypeAnnotationNode& type,
    const TPosition& pos, TKikimrParamsMap& parameters, TExprContext& ctx)
{
    auto parameter = parameters.FindPtr(name);
    if (!parameter) {
        if (type.GetKind() == ETypeAnnotationKind::Optional) {
            auto& newParameter = parameters[name];

            if (!ExportTypeToKikimrProto(type, *newParameter.MutableType(), ctx)) {
                ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Failed to export parameter type: " << name));
                return nullptr;
            }

            return &newParameter;
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
                        queryCtx->Parameters, ctx);
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

            if (!ValidateParameter(paramDesc.GetName(), *expectedType, TPosition(), QueryCtx->Parameters, ctx)) {
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

class TExecutePreparedTransformer : public TGraphTransformerBase {
public:
    TExecutePreparedTransformer(TIntrusivePtr<TKikimrQueryContext> queryCtx,
        TIntrusivePtr<TExecuteContext> executeCtx, TIntrusivePtr<IKqpRunner> kqpRunner, const TString& cluster)
        : QueryCtx(queryCtx)
        , ExecuteCtx(executeCtx)
        , KqpRunner(kqpRunner)
        , Cluster(cluster)
        , CurrentKqlIndex(0) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_ENSURE(!QueryCtx->PrepareOnly);
        YQL_ENSURE(QueryCtx->PreparedQuery);
        YQL_ENSURE(input->Type() == TExprNode::World);
        output = input;

        if (!AsyncResult) {
            const auto& query = *QueryCtx->PreparedQuery;
            YQL_ENSURE(QueryCtx->Type != EKikimrQueryType::Unspecified);

            if (query.GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1) {
                if (CurrentKqlIndex) {
                    return TStatus::Ok;
                }

                const auto& phyQuery = query.GetPhysicalQuery();

                if (QueryCtx->Type == EKikimrQueryType::Scan) {
                    AsyncResult = KqpRunner->ExecutePreparedScanQuery(Cluster, input.Get(), phyQuery, ctx,
                        ExecuteCtx->ReplyTarget);
                } else {
                    YQL_ENSURE(QueryCtx->Type == EKikimrQueryType::Dml);
                    AsyncResult = KqpRunner->ExecutePreparedQueryNewEngine(Cluster, input.Get(), phyQuery, ctx,
                        ExecuteCtx->Settings);
                }
            } else {
                if (CurrentKqlIndex >= query.KqlsSize()) {
                    return TStatus::Ok;
                }

                const auto& kql = query.GetKqls(CurrentKqlIndex);

                YQL_ENSURE(QueryCtx->Type == EKikimrQueryType::Dml);
                YQL_ENSURE(!kql.GetSettings().GetNewEngine());
                AsyncResult = KqpRunner->ExecutePreparedDataQuery(Cluster, input.Get(), kql, ctx, ExecuteCtx->Settings);
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

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
        output = input;

        if (!AsyncResult->HasResult()) {
            return TStatus::Repeat;
        }

        auto queryResult = std::move(AsyncResult->GetResult());
        if (!queryResult.Success()) {
            return TStatus::Error;
        }

        ExecuteCtx->QueryResults.emplace_back(std::move(queryResult));

        ++CurrentKqlIndex;
        AsyncResult.Reset();

        return TStatus::Repeat;
    }

    void Rewind() override {
        CurrentKqlIndex = 0;
        AsyncResult.Reset();
    }

private:
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TExecuteContext> ExecuteCtx;
    TIntrusivePtr<IKqpRunner> KqpRunner;
    TString Cluster;
    ui32 CurrentKqlIndex;
    TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult> AsyncResult;
    TPromise<void> Promise;
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

    TIntrusivePtr<TAsyncQueryResult> ExecuteKql(const TString&, const TExprNode::TPtr&, TExprContext&,
        const TExecuteSettings&) override
    {
        YQL_ENSURE(false, "Unexpected ExecuteKql call.");
        return nullptr;
    }

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
                YQL_ENSURE(TMaybeNode<TKiDataQuery>(query));
                TKiDataQuery dataQuery(query);

                auto queryAstStr = SerializeExpr(ctx, *query);

                bool useScanQuery = ShouldUseScanQuery(dataQuery, settings);

                IKqpGateway::TAstQuerySettings querySettings;
                querySettings.StatsMode = GetStatsMode(settings.StatsMode);

                TFuture<TQueryResult> future;
                switch (queryType) {
                case EKikimrQueryType::YqlScript:
                    if (useScanQuery) {
                        ui64 rowsLimit = 0;
                        if (!dataQuery.Results().Empty()) {
                            rowsLimit = FromString<ui64>(dataQuery.Results().Item(0).RowsLimit());
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
                    if (useScanQuery) {
                        future = Gateway->StreamExecScanQueryAst(Cluster, queryAstStr, CollectParameters(query),
                            querySettings, SessionCtx->Query().ReplyTarget);
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

                return MakeIntrusive<TKikimrFutureResult<TQueryResult>>(future, ctx);
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
    TKqpParamsMap CollectParameters(const TExprNode::TPtr& query) {
        TKqpParamsMap paramsMap;

        auto queryCtx = SessionCtx->QueryPtr();
        VisitExpr(query, [queryCtx, &paramsMap] (const TExprNode::TPtr& exprNode) {
            if (auto parameter = TMaybeNode<TCoParameter>(exprNode)) {
                TString name(parameter.Cast().Name().Value());
                auto paramValue = queryCtx->Parameters.FindPtr(name);
                YQL_ENSURE(paramValue);

                paramsMap.Values.emplace(name, *paramValue);
            }

            return true;
        });

        return paramsMap;
    }

    bool ShouldUseScanQuery(const TKiDataQuery& query, const TExecuteSettings& settings) {
        if (settings.UseScanQuery) {
            return *settings.UseScanQuery;
        }

        if (query.Effects().ArgCount() > 0) {
            // Do not use ScanQuery for queries with effects.
            return false;
        }

        if (query.Results().Size() != 1) {
            // Do not use ScanQuery for queries with multiple result sets.
            return false;
        }

        if (query.Operations().Empty()) {
            // Do not use ScanQuery for pure queries.
            return false;
        }

        for (const auto& operation : query.Operations()) {
            auto& tableData = SessionCtx->Tables().ExistingTable(operation.Cluster(), operation.Table());
            if (!tableData.Metadata->SysView.empty()) {
                // Always use ScanQuery for queries with system tables.
                return true;
            }
        }

        if (!SessionCtx->Config().FeatureFlags.GetEnableImplicitScanQueryInScripts()) {
            return false;
        }

        bool hasIndexReads = false;
        bool hasJoins = false;
        VisitExpr(query.Results().Ptr(), [&hasIndexReads, &hasJoins] (const TExprNode::TPtr& exprNode) {
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
    TKqpHost(TIntrusivePtr<IKqpGateway> gateway, const TString& cluster, const TString& database,
        TKikimrConfiguration::TPtr config, IModuleResolver::TPtr moduleResolver,
        const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry, bool keepConfigChanges)
        : Gateway(gateway)
        , Cluster(cluster)
        , ExprCtx(new TExprContext())
        , ModuleResolver(moduleResolver)
        , KeepConfigChanges(keepConfigChanges)
        , SessionCtx(new TKikimrSessionContext(config))
        , ClustersMap({{Cluster, TString(KikimrProviderName)}})
        , TypesCtx(MakeIntrusive<TTypeAnnotationContext>())
        , PlanBuilder(CreatePlanBuilder(*TypesCtx))
        , FakeWorld(ExprCtx->NewWorld(TPosition()))
        , ExecuteCtx(MakeIntrusive<TExecuteContext>())
        , ImplicitTransaction(MakeIntrusive<TKqpTransactionContext>(true))
        , ExplicitTransactions(MaxActiveTx())
    {
        if (funcRegistry) {
            FuncRegistry = funcRegistry;
        } else {
            FuncRegistryHolder = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
            FuncRegistry = FuncRegistryHolder.Get();
        }

        SessionCtx->SetDatabase(database);
        SessionCtx->QueryPtr()->TimeProvider = TAppData::TimeProvider;
        SessionCtx->QueryPtr()->RandomProvider = TAppData::RandomProvider;

        KqpRunner = CreateKqpRunner(Gateway, Cluster, TypesCtx, SessionCtx, *FuncRegistry);

        ExprCtx->NodesAllocationLimit = SessionCtx->Config()._KqpExprNodesAllocationLimit.Get().GetRef();
        ExprCtx->StringsAllocationLimit = SessionCtx->Config()._KqpExprStringsAllocationLimit.Get().GetRef();

        THashSet<TString> providerNames {
            TString(KikimrProviderName),
            TString(YdbProviderName),
        };

        // Kikimr provider
        auto queryExecutor = MakeIntrusive<TKqpQueryExecutor>(Gateway, Cluster, SessionCtx, KqpRunner);
        auto kikimrDataSource = CreateKikimrDataSource(*FuncRegistry, *TypesCtx, Gateway, SessionCtx);
        auto kikimrDataSink = CreateKikimrDataSink(*FuncRegistry, *TypesCtx, Gateway, SessionCtx, queryExecutor);

        FillSettings.AllResultsBytesLimit = Nothing();
        FillSettings.RowsLimitPerWrite = SessionCtx->Config()._ResultRowsLimit.Get().GetRef();
        FillSettings.Format = IDataProvider::EResultFormat::Custom;
        FillSettings.FormatDetails = TString(KikimrMkqlProtoFormat);

        TypesCtx->AddDataSource(providerNames, kikimrDataSource);
        TypesCtx->AddDataSink(providerNames, kikimrDataSink);
        TypesCtx->UdfResolver = CreateSimpleUdfResolver(FuncRegistry);
        TypesCtx->TimeProvider = TAppData::TimeProvider;
        TypesCtx->RandomProvider = TAppData::RandomProvider;
        TypesCtx->Modules = ModuleResolver;
        TypesCtx->UserDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, TUserDataTable(), nullptr, nullptr);
        TypesCtx->JsonQueryReturnsJsonDocument = true;

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
            return
                settingName == "OrderedColumns" ||
                settingName == "DisableOrderedColumns" ||
                settingName == "Warning";
        };
        auto configProvider = CreateConfigProvider(*TypesCtx, gatewaysConfig, allowSettings);
        TypesCtx->AddDataSource(ConfigProviderName, configProvider);

        YQL_ENSURE(TypesCtx->Initialize(*ExprCtx));

        YqlTransformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("YqlTransformer", NYql::NLog::EComponent::ProviderKqp,
                NYql::NLog::ELevel::TRACE), "LogYqlTransform")
            .AddPreTypeAnnotation()
            // TODO: .AddExpressionEvaluation(*FuncRegistry)
            .Add(new TFailExpressionEvaluation(), "FailExpressionEvaluation")
            .AddIOAnnotation()
            .AddTypeAnnotation()
            .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
            .AddPostTypeAnnotation()
            .AddOptimization(true, false)
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

        ExecutePreparedTransformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .Add(TValidatePreparedTransformer::Sync(SessionCtx->QueryPtr()), "ValidatePrepared")
            .Add(new TExecutePreparedTransformer(SessionCtx->QueryPtr(), ExecuteCtx, KqpRunner, Cluster),
                "ExecutePrepared", TIssuesIds::CORE_EXEC)
            .Build();
    }

    IAsyncQueryResultPtr ExecuteSchemeQuery(const TString& query, bool isSql) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql] (TExprContext& ctx) {
                return ExecuteSchemeQueryInternal(query, isSql, ctx);
            });
    }

    TQueryResult SyncExecuteSchemeQuery(const TString& query, bool isSql) override {
        return CheckedSyncProcessQuery(
            [this, &query, isSql] () {
                return ExecuteSchemeQuery(query, isSql);
            });
    }

    TBeginTxResult BeginTransaction(NKikimrKqp::EIsolationLevel isolationLevel, bool readonly) override {
        try {
            return BeginTransactionInternal(isolationLevel, readonly);
        }
        catch (const std::exception& e) {
            return ResultFromException<TBeginTxResult>(e);
        }
    }

    TGenericResult DeleteTransaction(const TString& txId) override {
        try {
            return DeleteTransactionInternal(txId);
        }
        catch (const std::exception& e) {
            return ResultFromException<TGenericResult>(e);
        }
    }

    IAsyncQueryResultPtr RollbackTransaction(const TString& txId,
        const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, txId, settings] (TExprContext& ctx) {
                return RollbackTransactionInternal(txId, settings, ctx);
            });
    }

    TQueryResult SyncRollbackTransaction(const TString& txId,
        const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedSyncProcessQuery(
            [this, txId, settings] () {
                return RollbackTransaction(txId, settings);
            });
    }

    IAsyncQueryResultPtr CommitTransaction(const TString& txId,
        const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, txId, settings] (TExprContext& ctx) {
                return CommitTransactionInternal(txId, settings, ctx);
            });
    }

    TQueryResult SyncCommitTransaction(const TString& txId,
        const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedSyncProcessQuery(
            [this, txId, settings] () {
                return CommitTransaction(txId, settings);
            });
    }

    TMaybe<TKqpTransactionInfo> GetTransactionInfo() override {
        return ImplicitTransaction->GetInfo();
    }

    void ForceTxOldEngine(const TString& txId) override {
        if (auto tx = FindTransaction(txId)) {
            (*tx)->ForceOldEngine();
        }
   }

    void ForceTxNewEngine(const TString& txId, ui32 percent, ui32 level) override {
        if (auto tx = FindTransaction(txId)) {
            (*tx)->ForceNewEngine(percent, level);
        }
    }

    TMaybe<TKqpTransactionInfo> GetTransactionInfo(const TString& txId) override {
        auto tx = FindTransaction(txId);
        if (!tx) {
            return TMaybe<TKqpTransactionInfo>();
        }

        auto txCtx = *tx;
        if (txCtx->IsClosed()) {
            RemoveTransaction(txId);
        }

        if (txCtx->IsInvalidated() && SessionCtx->Config()._KqpRollbackInvalidatedTx.Get().GetRef()) {
            AbortTransaction(txId);
        }

        return txCtx->GetInfo();
    }

    bool AbortTransaction(const TString& txId) override {
        auto it = ExplicitTransactions.FindWithoutPromote(txId);
        if (it != ExplicitTransactions.End()) {
            AbortTransaction(it);
            return true;
        }

        return false;
    }

    ui32 AbortAll() override {
        auto abortedCount = ExplicitTransactions.Size();

        for (auto it = ExplicitTransactions.Begin(); it != ExplicitTransactions.End(); ++it) {
            it.Value()->Invalidate();
            AbortedTransactions.emplace(std::make_pair(it.Key(), it.Value()));
        }

        ExplicitTransactions.Clear();
        return abortedCount;
    }

    IAsyncQueryResultPtr RollbackAborted() override {
        return CheckedProcessQuery(*ExprCtx,
            [this] (TExprContext& ctx) {
                return RollbackAbortedInternal(ctx);
            });
    }

    TQueryResult SyncRollbackAborted() override {
        return CheckedSyncProcessQuery(
            [this] () {
                return RollbackAborted();
            });
    }

    IAsyncQueryResultPtr ExplainDataQuery(const TString& query, bool isSql) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql] (TExprContext& ctx) {
                return ExplainDataQueryInternal(query, isSql, ctx);
            });
    }

    IAsyncQueryResultPtr ExplainScanQuery(const TString& query, bool isSql) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql] (TExprContext& ctx) {
                return ExplainScanQueryInternal(query, isSql, ctx);
            });
    }

    TQueryResult SyncExplainDataQuery(const TString& query, bool isSql) override {
        return CheckedSyncProcessQuery(
            [this, &query, isSql] () {
                return ExplainDataQuery(query, isSql);
            });
    }

    IAsyncQueryResultPtr PrepareDataQuery(const TString& query, const TPrepareSettings& settings) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, settings] (TExprContext& ctx) mutable {
                return PrepareDataQueryInternal(query, settings, ctx);
            });
    }

    TQueryResult SyncPrepareDataQuery(const TString& query, const TPrepareSettings& settings) override {
        return CheckedSyncProcessQuery(
            [this, &query, settings] () mutable {
                return PrepareDataQuery(query, settings);
            });
    }

    IAsyncQueryResultPtr ExecuteDataQuery(const TString& txId, std::shared_ptr<const NKikimrKqp::TPreparedQuery>& query,
        NKikimrMiniKQL::TParams&& parameters, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, txId, &query, parameters = std::move(parameters), settings] (TExprContext& ctx) mutable {
                return ExecuteDataQueryInternal(txId, query, std::move(parameters), settings, {}, {}, ctx);
            });
    }

    TQueryResult SyncExecuteDataQuery(const TString& txId, std::shared_ptr<const NKikimrKqp::TPreparedQuery>& query,
        NKikimrMiniKQL::TParams&& parameters, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedSyncProcessQuery(
            [this, txId, &query, parameters = std::move(parameters), settings] () mutable {
                return ExecuteDataQuery(txId, query, std::move(parameters), settings);
            });
    }

    IAsyncQueryResultPtr ExecuteDataQuery(const TString& txId, const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&& parameters, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, txId, &query, isSql, parameters = std::move(parameters), settings] (TExprContext& ctx) mutable {
                return ExecuteDataQueryInternal(txId, query, isSql, std::move(parameters), settings, ctx);
            });
    }

    TQueryResult SyncExecuteDataQuery(const TString& txId, const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&& parameters, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedSyncProcessQuery(
            [this, txId, &query, isSql, parameters = std::move(parameters), settings] () mutable {
                return ExecuteDataQuery(txId, query, isSql, std::move(parameters), settings);
            });
    }

    IAsyncQueryResultPtr ExecuteYqlScript(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const TExecScriptSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script, parameters = std::move(parameters), settings] (TExprContext& ctx) mutable {
                return ExecuteYqlScriptInternal(script, std::move(parameters), settings, ctx);
            });
    }

    TQueryResult SyncExecuteYqlScript(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const TExecScriptSettings& settings) override
    {
        return CheckedSyncProcessQuery(
            [this, &script, parameters = std::move(parameters), settings] () mutable {
                return ExecuteYqlScript(script, std::move(parameters), settings);
            });
    }

    IAsyncQueryResultPtr StreamExecuteYqlScript(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const NActors::TActorId& target, const TExecScriptSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script, parameters = std::move(parameters), target, settings](TExprContext& ctx) mutable {
            return StreamExecuteYqlScriptInternal(script, std::move(parameters), target, settings, ctx);
        });
    }

    IAsyncQueryResultPtr ValidateYqlScript(const TString& script) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script](TExprContext& ctx) mutable {
            return ValidateYqlScriptInternal(script, ctx);
        });
    }

    TQueryResult SyncValidateYqlScript(const TString& script) override {
        return CheckedSyncProcessQuery(
            [this, &script]() mutable {
            return ValidateYqlScript(script);
        });
    }

    IAsyncQueryResultPtr ExplainYqlScript(const TString& script) override {
        return CheckedProcessQuery(*ExprCtx,
            [this, &script] (TExprContext& ctx) mutable {
                return ExplainYqlScriptInternal(script, ctx);
            });
    }

    TQueryResult SyncExplainYqlScript(const TString& script) override {
        return CheckedSyncProcessQuery(
            [this, &script] () mutable {
                return ExplainYqlScript(script);
            });
    }

    IAsyncQueryResultPtr ExecuteScanQuery(const TString& query, bool isSql, NKikimrMiniKQL::TParams&& parameters,
        const NActors::TActorId& target, const IKikimrQueryExecutor::TExecuteSettings& settings) override
    {
        return CheckedProcessQuery(*ExprCtx,
            [this, &query, isSql, parameters = std::move(parameters), target, settings] (TExprContext& ctx) mutable {
                return ExecuteScanQueryInternal(query, isSql, std::move(parameters), target, settings, ctx);
            });
    }

private:
    ui32 MaxActiveTx() const {
        return SessionCtx->Config()._KqpMaxActiveTxPerSession.Get().GetRef();
    }

    TMaybe<TIntrusivePtr<TKqpTransactionContext>> FindTransaction(const TString& id) {
        auto it = ExplicitTransactions.Find(id);
        if (it != ExplicitTransactions.End()) {
            auto& value = it.Value();
            value->Touch();
            return value;
        }

        return {};
    }

    void RemoveTransaction(const TString& txId) {
        auto it = ExplicitTransactions.FindWithoutPromote(txId);
        if (it != ExplicitTransactions.End()) {
            ExplicitTransactions.Erase(it);
        }
    }

    void AbortTransaction(const TLRUCache<TString, TIntrusivePtr<TKqpTransactionContext>>::TIterator& it) {
        it.Value()->Invalidate();
        AbortedTransactions.emplace(std::make_pair(it.Key(), it.Value()));
        ExplicitTransactions.Erase(it);
    }

    TExprNode::TPtr CompileQuery(const TString& query, bool isSql, bool sqlAutoCommit, TExprContext& ctx,
        TMaybe<TSqlVersion>& sqlVersion) const
    {
        TAstParseResult astRes;
        if (isSql) {
            NSQLTranslation::TTranslationSettings settings;

            if (sqlVersion) {
                settings.SyntaxVersion = *sqlVersion;

                if (*sqlVersion > 0) {
                    // Restrict fallback to V0
                    settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
                }
            } else {
                settings.SyntaxVersion = SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef();
                settings.V0Behavior = NSQLTranslation::EV0Behavior::Silent;
            }

            settings.InferSyntaxVersion = true;
            settings.V0ForceDisable = false;
            settings.WarnOnV0 = false;
            settings.DefaultCluster = Cluster;
            settings.ClusterMapping = ClustersMap;
            auto tablePathPrefix = SessionCtx->Config()._KqpTablePathPrefix.Get().GetRef();
            if (!tablePathPrefix.empty()) {
                settings.PathPrefix = tablePathPrefix;
            }
            settings.EndOfQueryCommit = sqlAutoCommit;

            ui16 actualSyntaxVersion = 0;
            astRes = NSQLTranslation::SqlToYql(query, settings, nullptr, &actualSyntaxVersion);
            TypesCtx->DeprecatedSQL = (actualSyntaxVersion == 0);
            sqlVersion = actualSyntaxVersion;
        } else {
            sqlVersion = {};
            astRes = ParseAst(query);

            // Do not check SQL constraints on s-expressions input, as it may come from both V0/V1.
            // Constraints were already checked on type annotation of SQL query.
            TypesCtx->DeprecatedSQL = true;
        }

        ctx.IssueManager.AddIssues(astRes.Issues);
        if (!astRes.IsOk()) {
            return nullptr;
        }

        TExprNode::TPtr result;
        if (!CompileExpr(*astRes.Root, result, ctx, ModuleResolver.get())) {
            return nullptr;
        }

        YQL_CLOG(INFO, ProviderKqp) << "Compiled query:\n" << KqpExprToPrettyString(*result, ctx);

        return result;
    }

    TExprNode::TPtr CompileYqlQuery(const TString& query, bool isSql, bool sqlAutoCommit, TExprContext& ctx,
        TMaybe<TSqlVersion>& sqlVersion) const
    {
        auto queryExpr = CompileQuery(query, isSql, sqlAutoCommit, ctx, sqlVersion);
        if (!queryExpr) {
            return nullptr;
        }

        if (!isSql) {
            return queryExpr;
        }

        if (TMaybeNode<TCoCommit>(queryExpr) && TCoCommit(queryExpr).DataSink().Maybe<TKiDataSink>()) {
            return queryExpr;
        }

        return Build<TCoCommit>(ctx, queryExpr->Pos())
            .World(queryExpr)
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
            .Ptr();
    }

    static bool ParseParameters(NKikimrMiniKQL::TParams&& parameters, TKikimrParamsMap& map,
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

            NKikimrMiniKQL::TParams param;
            param.MutableType()->Swap(structType.MutableMember(i)->MutableType());
            param.MutableValue()->Swap(parameters.MutableValue()->MutableStruct(i));

            auto result = map.emplace(std::make_pair(memberName, std::move(param)));
            if (!result.second) {
                ctx.AddError(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Duplicate parameter: " << memberName));
                return false;
            }
        }

        return true;
    }

    TBeginTxResult BeginTransactionInternal(NKikimrKqp::EIsolationLevel isolationLevel, bool readonly) {
        YQL_ENSURE(ExplicitTransactions.Size() <= MaxActiveTx());

        if (isolationLevel == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED) {
            return ResultFromError<TBeginTxResult>(YqlIssue(TPosition(), TIssuesIds::KIKIMR_BAD_OPERATION,
                "Unknown transaction mode."));
        }

        TBeginTxResult result;
        if (ExplicitTransactions.Size() == MaxActiveTx()) {
            auto it = ExplicitTransactions.FindOldest();
            YQL_ENSURE(it != ExplicitTransactions.End());

            auto idleDuration = TInstant::Now() - it.Value()->LastAccessTime;
            if (idleDuration.Seconds() >= SessionCtx->Config()._KqpTxIdleTimeoutSec.Get().GetRef()) {
                AbortTransaction(it);
                ++result.EvictedTx;
            } else {
                result.AddIssue(YqlIssue(TPosition(), TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS));
                result.CurrentActiveTx = ExplicitTransactions.Size();
                result.CurrentAbortedTx = AbortedTransactions.size();
                return result;
            }
        }

        auto txCtx = MakeIntrusive<TKqpTransactionContext>(/* implicit */ false);
        txCtx->EffectiveIsolationLevel = isolationLevel;
        txCtx->Readonly = readonly;

        auto txId = CreateGuidAsString();
        YQL_ENSURE(ExplicitTransactions.Insert(std::make_pair(txId, txCtx)));

        result.SetSuccess();
        result.TxId = txId;
        result.CurrentActiveTx = ExplicitTransactions.Size();
        result.CurrentAbortedTx = AbortedTransactions.size();
        return result;
    }

    TGenericResult DeleteTransactionInternal(const TString& txId) {
        auto maybeTx = FindTransaction(txId);
        if (!maybeTx) {
            return ResultFromError<TGenericResult>(
                YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND, TStringBuilder()
                    << "Transaction not found: " << txId));
        }

        if ((*maybeTx)->QueriesCount > 0) {
            return ResultFromError<TGenericResult>(
                YqlIssue(TPosition(), TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                    << "Transaction is not empty: " << txId));
        }

        RemoveTransaction(txId);

        TGenericResult result;
        result.SetSuccess();
        return result;
    }

    IAsyncQueryResultPtr ExecuteSchemeQueryInternal(const TString& query, bool isSql, TExprContext& ctx) {
        SetupYqlTransformer(nullptr);

        SessionCtx->Query().Type = EKikimrQueryType::Ddl;

        TMaybe<TSqlVersion> sqlVersion;
        auto queryExpr = CompileYqlQuery(query, isSql, false, ctx, sqlVersion);
        if (!queryExpr) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncExecuteYqlResult>(queryExpr.Get(), ctx, *YqlTransformer, Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion);
    }

    IAsyncQueryResultPtr RollbackTransactionInternal(const TString& txId,
        const IKikimrQueryExecutor::TExecuteSettings& settings, TExprContext& ctx)
    {
        auto maybeTx = FindTransaction(txId);
        if (!maybeTx) {
            return MakeKikimrResultHolder(ResultFromError<TQueryResult>(
                YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND, TStringBuilder()
                    << "Transaction not found: " << txId)));
        }

        YQL_ENSURE(settings.RollbackTx);

        auto query = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto engine = maybeTx->Get()->DeferredEffects.GetEngine();
        if (engine.has_value() && *engine == TKqpTransactionInfo::EEngine::NewEngine) {
            YQL_ENSURE(settings.UseNewEngine.Defined() && *settings.UseNewEngine);
            query->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
            query->MutablePhysicalQuery()->SetType(NKqpProto::TKqpPhyQuery::TYPE_DATA);
        } else {
            YQL_ENSURE(!settings.UseNewEngine.Defined() || !*settings.UseNewEngine);
            query->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_V1);
            query->AddKqls();
        }

        YQL_CLOG(INFO, ProviderKqp) << "Rollback tx: " << txId << ", query: " << query->ShortUtf8DebugString();

        std::shared_ptr<const NKikimrKqp::TPreparedQuery> q(query.release());
        return ExecuteDataQueryInternal(txId, q, NKikimrMiniKQL::TParams(), settings, {}, {}, ctx);
    }

    IAsyncQueryResultPtr RollbackTransactionInternal(TIntrusivePtr<TKqpTransactionContext> tx,
        const IKikimrQueryExecutor::TExecuteSettings& settings, TExprContext& ctx)
    {
        YQL_ENSURE(settings.RollbackTx);

        auto query = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto settings1 = settings;

        auto engine = tx->DeferredEffects.GetEngine();
        if (engine.has_value() && *engine == TKqpTransactionInfo::EEngine::NewEngine) {
            YQL_ENSURE(!settings.UseNewEngine.Defined() || *settings.UseNewEngine == true);
            settings1.UseNewEngine = true;
            query->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
            query->MutablePhysicalQuery()->SetType(NKqpProto::TKqpPhyQuery::TYPE_DATA);
        } else {
            YQL_ENSURE(!settings.UseNewEngine.Defined() || *settings.UseNewEngine == false);
            settings1.UseNewEngine = false;
            query->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_V1);
            query->AddKqls();
        }

        std::shared_ptr<const NKikimrKqp::TPreparedQuery> q(query.release());
        return ExecuteDataQueryInternal(tx, q, {}, settings1, {}, {}, /* replyPrepared */ false, ctx);
    }

    IAsyncQueryResultPtr CommitTransactionInternal(const TString& txId,
        const IKikimrQueryExecutor::TExecuteSettings& settings, TExprContext& ctx)
    {
        auto maybeTx = FindTransaction(txId);
        if (!maybeTx) {
            return MakeKikimrResultHolder(ResultFromError<TQueryResult>(
                YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND, TStringBuilder()
                    << "Transaction not found: " << txId)));
        }

        YQL_ENSURE(settings.CommitTx);

        auto query = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto engine = maybeTx->Get()->DeferredEffects.GetEngine();
        if (engine.has_value() && *engine == TKqpTransactionInfo::EEngine::NewEngine) {
            YQL_ENSURE(settings.UseNewEngine.Defined() && *settings.UseNewEngine);
            query->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1);
            query->MutablePhysicalQuery()->SetType(NKqpProto::TKqpPhyQuery::TYPE_DATA);
        } else {
            YQL_ENSURE(!settings.UseNewEngine.Defined() || !*settings.UseNewEngine);
            query->SetVersion(NKikimrKqp::TPreparedQuery::VERSION_V1);
            query->AddKqls();
        }

        std::shared_ptr<const NKikimrKqp::TPreparedQuery> q(query.release());
        return ExecuteDataQueryInternal(txId, q, NKikimrMiniKQL::TParams(), settings, /* issues */ {},
            /* sqlVersion */ {}, ctx);
    }

    IAsyncQueryResultPtr RollbackAbortedInternal(TExprContext& ctx) {
        if (AbortedTransactions.empty()) {
            TQueryResult result;
            result.SetSuccess();
            return MakeKikimrResultHolder(std::move(result));
        }

        TVector<TIntrusivePtr<TKqpTransactionContext>> txs;
        txs.reserve(AbortedTransactions.size());
        for (auto& pair : AbortedTransactions) {
            txs.push_back(pair.second);
        }

        AbortedTransactions.clear();
        return MakeIntrusive<TKqpAsyncExecAllResult<TIntrusivePtr<TKqpTransactionContext>, TQueryResult>>(txs,
            [this, &ctx] (const TIntrusivePtr<TKqpTransactionContext>& tx) {
                IKikimrQueryExecutor::TExecuteSettings settings;
                settings.RollbackTx = true;
                settings.Deadlines.TimeoutAt = TInstant::Now() + TDuration::Minutes(1);

                auto engine = tx->DeferredEffects.GetEngine();
                settings.UseNewEngine = engine.has_value() && *engine == TKqpTransactionInfo::EEngine::NewEngine;

                return RollbackTransactionInternal(tx, settings, ctx);
            });
    }

    IAsyncQueryResultPtr ExplainDataQueryInternal(const TString& query, bool isSql, TExprContext& ctx) {
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
                if (prepared.PreparingQuery->GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1) {
                    explainResult.QueryPlan = std::move(prepared.QueryPlan);
                    explainResult.QueryAst = std::move(*prepared.PreparingQuery->MutablePhysicalQuery()->MutableQueryAst());
                } else {
                    FillAstAndPlan(explainResult, *prepared.PreparingQuery);
                }
                explainResult.SqlVersion = prepared.SqlVersion;
                return MakeKikimrResultHolder(std::move(explainResult));
            });
    }

    IAsyncQueryResultPtr ExplainScanQueryInternal(const TString& query, bool isSql, TExprContext& ctx) {
        auto prepareResult = isSql
            ? PrepareScanQueryInternal(query, ctx)
            : PrepareScanQueryAstInternal(query, ctx);
        return prepareResult;
    }

    IAsyncQueryResultPtr PrepareDataQueryInternal(const TString& query, const TPrepareSettings& settings,
        TExprContext& ctx)
    {
        // TODO: Use empty tx context
        auto tempTxCtx = MakeIntrusive<TKqpTransactionContext>(/* implicit */ true);
        SetupYqlTransformer(tempTxCtx);

        SessionCtx->Query().Type = EKikimrQueryType::Dml;
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        if (settings.UseNewEngine) {
            SessionCtx->Config().UseNewEngine = *settings.UseNewEngine;
        }
        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }

        TMaybe<TSqlVersion> sqlVersion;
        auto queryExpr = CompileYqlQuery(query, /* isSql */ true, /* sqlAutoCommit */ false, ctx, sqlVersion);
        if (!queryExpr) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncPrepareYqlResult>(queryExpr.Get(), ctx, *YqlTransformer, SessionCtx->QueryPtr(),
            query, sqlVersion);
    }

    IAsyncQueryResultPtr PrepareDataQueryAstInternal(const TString& queryAst, const TPrepareSettings& settings,
        TExprContext& ctx)
    {
        // TODO: Use empty tx context
        TIntrusivePtr<IKikimrTransactionContext> tempTxCtx = MakeIntrusive<TKqpTransactionContext>(true);
        IKikimrQueryExecutor::TExecuteSettings execSettings;
        execSettings.UseNewEngine = settings.UseNewEngine;
        SetupDataQueryAstTransformer(execSettings, tempTxCtx);

        SessionCtx->Query().Type = EKikimrQueryType::Dml;
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        if (settings.DocumentApiRestricted) {
            SessionCtx->Query().DocumentApiRestricted = *settings.DocumentApiRestricted;
        }

        TMaybe<TSqlVersion> sqlVersion;
        auto queryExpr = CompileYqlQuery(queryAst, false, false, ctx, sqlVersion);
        if (!queryExpr) {
            return nullptr;
        }

        YQL_ENSURE(!sqlVersion);

        return MakeIntrusive<TAsyncExecuteKqlResult>(queryExpr.Get(), ctx, *DataQueryAstTransformer,
            SessionCtx, *ExecuteCtx);
    }

    IAsyncQueryResultPtr PrepareScanQueryInternal(const TString& query, TExprContext& ctx, EKikimrStatsMode statsMode = EKikimrStatsMode::None) {
        SetupYqlTransformer(nullptr);

        SessionCtx->Query().Type = EKikimrQueryType::Scan;
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().StatsMode = statsMode;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion = 1;
        auto queryExpr = CompileYqlQuery(query, true, false, ctx, sqlVersion);
        if (!queryExpr) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncPrepareYqlResult>(queryExpr.Get(), ctx, *YqlTransformer, SessionCtx->QueryPtr(),
            query, sqlVersion);
    }

    IAsyncQueryResultPtr PrepareScanQueryAstInternal(const TString& queryAst, TExprContext& ctx) {
        IKikimrQueryExecutor::TExecuteSettings settings;
        SetupDataQueryAstTransformer(settings);

        SessionCtx->Query().Type = EKikimrQueryType::Scan;
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion;
        auto queryExpr = CompileYqlQuery(queryAst, false, false, ctx, sqlVersion);
        if (!queryExpr) {
            return nullptr;
        }

        YQL_ENSURE(!sqlVersion);

        return MakeIntrusive<TAsyncExecuteKqlResult>(queryExpr.Get(), ctx, *DataQueryAstTransformer,
            SessionCtx, *ExecuteCtx);
    }

    IAsyncQueryResultPtr ExecuteDataQueryInternal(TIntrusivePtr<TKqpTransactionContext> tx,
        std::shared_ptr<const NKikimrKqp::TPreparedQuery>& preparedQuery, NKikimrMiniKQL::TParams&& parameters,
        const IKikimrQueryExecutor::TExecuteSettings& settings, const TIssues& issues,
        const TMaybe<TSqlVersion>& sqlVersion, bool replyPrepared, TExprContext& ctx)
    {
        YQL_ENSURE(!settings.CommitTx || !settings.RollbackTx);

        switch (preparedQuery->GetVersion()) {
            case NKikimrKqp::TPreparedQuery::VERSION_V1: {
                YQL_ENSURE(preparedQuery->KqlsSize() == 1);
                auto& kql = preparedQuery->GetKqls(0);
                YQL_ENSURE(!kql.GetSettings().GetCommitTx());
                YQL_ENSURE(!kql.GetSettings().GetRollbackTx());
                YQL_ENSURE(kql.GetSettings().GetIsolationLevel() == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);
                break;
            }

            case NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1: {
                break;
            }

            default:
                return MakeKikimrResultHolder(ResultFromError<TQueryResult>(
                    TIssue(TPosition(), TStringBuilder()
                        << "Unexpected query version: " << preparedQuery->GetVersion())));
        }

        SetupExecutePreparedTransformer(settings, tx);

        if (issues) {
            ctx.IssueManager.AddIssues(issues);
        }

        if (!ParseParameters(std::move(parameters), SessionCtx->Query().Parameters, ctx)) {
            return nullptr;
        }

        SessionCtx->Query().Type = EKikimrQueryType::Dml;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().Limits = settings.Limits;
        SessionCtx->Query().PreparedQuery = preparedQuery;

        // moved to kqp_runner.cpp, ExecutePreparedQuery(...)
//        if (preparedQuery->GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_V1) {
//            SessionCtx->Query().PreparedQuery.MutableKqls(0)->MutableSettings()->SetCommitTx(settings.CommitTx);
//            SessionCtx->Query().PreparedQuery.MutableKqls(0)->MutableSettings()->SetRollbackTx(settings.RollbackTx);
//        }

        std::shared_ptr<const NKikimrKqp::TPreparedQuery> reply;
        if (replyPrepared) {
            reply = preparedQuery;
        }

        return MakeIntrusive<TAsyncExecutePreparedResult>(FakeWorld.Get(), ctx, *ExecutePreparedTransformer,
            SessionCtx, FillSettings, ExecuteCtx, reply, sqlVersion);
    }

    IAsyncQueryResultPtr ExecuteDataQueryInternal(const TString& txId, std::shared_ptr<const NKikimrKqp::TPreparedQuery>& query,
        NKikimrMiniKQL::TParams&& parameters, const IKikimrQueryExecutor::TExecuteSettings& settings,
        const TIssues& issues, const TMaybe<TSqlVersion>& sqlVersion, TExprContext& ctx)
    {
        auto maybeTx = FindTransaction(txId);
        if (!maybeTx) {
            return MakeKikimrResultHolder(ResultFromError<TQueryResult>(
                YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND, TStringBuilder()
                    << "Transaction not found: " << txId)));
        }

        (*maybeTx)->OnBeginQuery();

        return ExecuteDataQueryInternal(*maybeTx, query, std::move(parameters), settings, issues, sqlVersion,
            /* replyPrepared */ false, ctx);
    }

    IAsyncQueryResultPtr ExecuteDataQueryInternal(const TString& txId, const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&& parameters, const IKikimrQueryExecutor::TExecuteSettings& settings,
        TExprContext& ctx)
    {
        auto maybeTx = FindTransaction(txId);
        if (!maybeTx) {
            return MakeKikimrResultHolder(ResultFromError<TQueryResult>(
                YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND, TStringBuilder()
                    << "Transaction not found: " << txId)));
        }

        (*maybeTx)->OnBeginQuery();

        TPrepareSettings prepareSettings;
        prepareSettings.UseNewEngine = settings.UseNewEngine;
        prepareSettings.DocumentApiRestricted = settings.DocumentApiRestricted;

        auto prepareResult = isSql
            ? PrepareDataQueryInternal(query, prepareSettings, ctx)
            : PrepareDataQueryAstInternal(query, prepareSettings, ctx);

        if (!prepareResult) {
            return nullptr;
        }

        auto tx = *maybeTx;
        return AsyncApplyResult<TQueryResult, TQueryResult>(prepareResult,
            [this, tx, parameters = std::move(parameters), settings, &ctx]
            (TQueryResult&& prepared) mutable -> IAsyncQueryResultPtr {
                if (!prepared.Success()) {
                    tx->Invalidate();
                    return MakeKikimrResultHolder(std::move(prepared));
                }

                std::shared_ptr<const NKikimrKqp::TPreparedQuery> query;
                query.reset(prepared.PreparingQuery.release());

                return ExecuteDataQueryInternal(tx, query, std::move(parameters), settings, prepared.Issues(),
                    prepared.SqlVersion, /* replyPrepared */ true, ctx);
            });
    }

    IAsyncQueryResultPtr ExecuteYqlScriptInternal(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const TExecScriptSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(nullptr);

        SessionCtx->Query().Type = EKikimrQueryType::YqlScript;
        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        auto scriptExpr = CompileYqlQuery(script, true, true, ctx, sqlVersion);
        if (!scriptExpr) {
            return nullptr;
        }

        if (!ParseParameters(std::move(parameters), SessionCtx->Query().Parameters, ctx)) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncExecuteYqlResult>(scriptExpr.Get(), ctx, *YqlTransformer, Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion);
    }

    IAsyncQueryResultPtr StreamExecuteYqlScriptInternal(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const NActors::TActorId& target,const TExecScriptSettings& settings, TExprContext& ctx)
    {
        SetupYqlTransformer(nullptr);

        SessionCtx->Query().Type = EKikimrQueryType::YqlScriptStreaming;
        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().ReplyTarget = target;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        auto scriptExpr = CompileYqlQuery(script, true, true, ctx, sqlVersion);
        if (!scriptExpr) {
            return nullptr;
        }

        if (!ParseParameters(std::move(parameters), SessionCtx->Query().Parameters, ctx)) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncExecuteYqlResult>(scriptExpr.Get(), ctx, *YqlTransformer, Cluster, SessionCtx,
            *ResultProviderConfig, *PlanBuilder, sqlVersion);
    }

    IAsyncQueryResultPtr ValidateYqlScriptInternal(const TString& script, TExprContext& ctx) {
        SetupSession(nullptr);

        SessionCtx->Query().Type = EKikimrQueryType::YqlScript;
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().SuppressDdlChecks = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        SessionCtx->Query().PreparedQuery.reset();

        TMaybe<TSqlVersion> sqlVersion;
        auto scriptExpr = CompileYqlQuery(script, true, true, ctx, sqlVersion);
        if (!scriptExpr) {
            return nullptr;
        }

        auto transformer = TTransformationPipeline(TypesCtx)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddIOAnnotation()
            .AddTypeAnnotation()
            .Add(TCollectParametersTransformer::Sync(SessionCtx->QueryPtr()), "CollectParameters")
            .Build(false);

        return MakeIntrusive<TAsyncValidateYqlResult>(scriptExpr.Get(), SessionCtx, ctx, transformer, sqlVersion);
    }

    IAsyncQueryResultPtr ExplainYqlScriptInternal(const TString& script, TExprContext& ctx) {
        SetupYqlTransformer(nullptr);

        SessionCtx->Query().Type = EKikimrQueryType::YqlScript;
        SessionCtx->Query().PrepareOnly = true;
        SessionCtx->Query().SuppressDdlChecks = true;
        SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();

        TMaybe<TSqlVersion> sqlVersion;
        auto scriptExpr = CompileYqlQuery(script, true, true, ctx, sqlVersion);
        if (!scriptExpr) {
            return nullptr;
        }

        return MakeIntrusive<TAsyncExplainYqlResult>(scriptExpr.Get(), SessionCtx, ctx, YqlTransformer,
            *PlanBuilder, sqlVersion, true /* UseDqExplain */);
    }

    IAsyncQueryResultPtr ExecuteScanQueryInternal(const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&& parameters, const NActors::TActorId& target,
        const IKikimrQueryExecutor::TExecuteSettings& settings, TExprContext& ctx)
    {
        auto prepareResult = isSql
            ? PrepareScanQueryInternal(query, ctx, settings.StatsMode)
            : PrepareScanQueryAstInternal(query, ctx);

        if (!prepareResult) {
            return nullptr;
        }

        if (!isSql) {
            // Disable LLVM for scripting queries.
            // TODO: KIKIMR-13561
            SessionCtx->Config().EnableLlvm = EOptionalFlag::Disabled;
        }

        return AsyncApplyResult<TQueryResult, TQueryResult>(prepareResult,
            [this, parameters = std::move(parameters), target, settings, &ctx]
            (TQueryResult&& prepared) mutable -> IAsyncQueryResultPtr {
                if (!prepared.Success()) {
                    return MakeKikimrResultHolder(std::move(prepared));
                }

                std::shared_ptr<const NKikimrKqp::TPreparedQuery> query;
                query.reset(prepared.PreparingQuery.release());

                return ExecuteScanQueryInternal(query, std::move(parameters), target, settings, prepared.Issues(),
                    prepared.SqlVersion, ctx);
            });
    }

    IAsyncQueryResultPtr ExecuteScanQueryInternal(std::shared_ptr<const NKikimrKqp::TPreparedQuery>& query,
        NKikimrMiniKQL::TParams&& parameters, const NActors::TActorId& target,
        const IKikimrQueryExecutor::TExecuteSettings& settings, const TIssues& issues,
        const TMaybe<TSqlVersion>& sqlVersion, TExprContext& ctx)
    {
        auto txCtx = MakeIntrusive<TKqpTransactionContext>(true);
        txCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;

        SetupExecutePreparedTransformer(settings, txCtx);

        ExecuteCtx->ReplyTarget = target;

        if (issues) {
            ctx.IssueManager.AddIssues(issues);
        }

        if (!ParseParameters(std::move(parameters), SessionCtx->Query().Parameters, ctx)) {
            return nullptr;
        }

        SessionCtx->Query().Type = EKikimrQueryType::Scan;
        SessionCtx->Query().PreparedQuery = query;
        SessionCtx->Query().Deadlines = settings.Deadlines;
        SessionCtx->Query().Limits = settings.Limits;
        SessionCtx->Query().StatsMode = settings.StatsMode;
        SessionCtx->Query().RlPath = settings.RlPath;

        return MakeIntrusive<TAsyncExecutePreparedResult>(FakeWorld.Get(), ctx, *ExecutePreparedTransformer,
            SessionCtx, FillSettings, ExecuteCtx, nullptr, sqlVersion);
    }

    void SetupSession(TIntrusivePtr<IKikimrTransactionContext> txCtx) {
        ExprCtx->Reset();
        ExprCtx->Step.Done(TExprStep::ExprEval); // KIKIMR-8067

        TypesCtx->DeprecatedSQL = false;
        TypesCtx->CachedNow.reset();
        std::get<0>(TypesCtx->CachedRandom).reset();
        std::get<1>(TypesCtx->CachedRandom).reset();
        std::get<2>(TypesCtx->CachedRandom).reset();

        SessionCtx->Reset(KeepConfigChanges);
        if (txCtx) {
            SessionCtx->SetTx(txCtx);
        }
    }

    void SetupYqlTransformer(TIntrusivePtr<IKikimrTransactionContext> txCtx) {
        SetupSession(txCtx);

        YqlTransformer->Rewind();

        ResultProviderConfig->FillSettings = FillSettings;
        ResultProviderConfig->CommittedResults.clear();
    }

    void SetupDataQueryAstTransformer(const IKikimrQueryExecutor::TExecuteSettings& settings,
        TIntrusivePtr<IKikimrTransactionContext> txCtx = {})
    {
        SetupSession(txCtx);

        DataQueryAstTransformer->Rewind();

        ExecuteCtx->Reset(settings);
    }

    void SetupExecutePreparedTransformer(const IKikimrQueryExecutor::TExecuteSettings& settings,
        TIntrusivePtr<IKikimrTransactionContext> txCtx)
    {
        SetupSession(txCtx);

        ExecutePreparedTransformer->Rewind();

        ExecuteCtx->Reset(settings);
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TString Cluster;
    THolder<TExprContext> ExprCtx;
    IModuleResolver::TPtr ModuleResolver;
    bool KeepConfigChanges;

    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    THashMap<TString, TString> ClustersMap;

    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistryHolder;
    const NKikimr::NMiniKQL::IFunctionRegistry* FuncRegistry;

    TIntrusivePtr<TTypeAnnotationContext> TypesCtx;
    TAutoPtr<IPlanBuilder> PlanBuilder;
    IDataProvider::TFillSettings FillSettings;
    TIntrusivePtr<TResultProviderConfig> ResultProviderConfig;
    TAutoPtr<IGraphTransformer> YqlTransformer;
    TAutoPtr<IGraphTransformer> DataQueryAstTransformer;
    TAutoPtr<IGraphTransformer> ExecutePreparedTransformer;
    TExprNode::TPtr FakeWorld;

    TIntrusivePtr<TExecuteContext> ExecuteCtx;
    TIntrusivePtr<IKqpRunner> KqpRunner;

    TIntrusivePtr<TKqpTransactionContext> ImplicitTransaction;
    TLRUCache<TString, TIntrusivePtr<TKqpTransactionContext>> ExplicitTransactions;
    THashMap<TString, TIntrusivePtr<TKqpTransactionContext>> AbortedTransactions;
};

} // namespace

NDqProto::EDqStatsMode GetStatsMode(NYql::EKikimrStatsMode statsMode) {
    switch (statsMode) {
        case NYql::EKikimrStatsMode::Basic:
            return NYql::NDqProto::DQ_STATS_MODE_BASIC;
        case NYql::EKikimrStatsMode::Profile:
            return NYql::NDqProto::DQ_STATS_MODE_PROFILE;
        default:
            return NYql::NDqProto::DQ_STATS_MODE_NONE;
    }
}

TKqpTransactionInfo TKqpTransactionContext::GetInfo() const {
    TKqpTransactionInfo txInfo;

    // Status
    if (Invalidated) {
        txInfo.Status = TKqpTransactionInfo::EStatus::Aborted;
    } else if (Closed) {
        txInfo.Status = TKqpTransactionInfo::EStatus::Committed;
    } else {
        txInfo.Status = TKqpTransactionInfo::EStatus::Active;
    }

    // Kind
    bool hasReads = false;
    bool hasWrites = false;
    for (auto& pair : TableOperations) {
        hasReads = hasReads || (pair.second & KikimrReadOps());
        hasWrites = hasWrites || (pair.second & KikimrModifyOps());
    }

    if (hasReads) {
        txInfo.Kind = hasWrites
            ? TKqpTransactionInfo::EKind::ReadWrite
            : TKqpTransactionInfo::EKind::ReadOnly;
    } else {
        txInfo.Kind = hasWrites
            ? TKqpTransactionInfo::EKind::WriteOnly
            : TKqpTransactionInfo::EKind::Pure;
    }

    txInfo.TotalDuration = FinishTime - CreationTime;
    txInfo.ServerDuration = QueriesDuration;
    txInfo.QueriesCount = QueriesCount;

    txInfo.TxEngine = DeferredEffects.GetEngine();
    txInfo.ForceNewEngineState = ForceNewEngineSettings;

    return txInfo;
}

TIntrusivePtr<IKqpHost> CreateKqpHost(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, const TString& database, TKikimrConfiguration::TPtr config, IModuleResolver::TPtr moduleResolver,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry, bool keepConfigChanges)
{
    return MakeIntrusive<TKqpHost>(gateway, cluster, database, config, moduleResolver, funcRegistry,
        keepConfigChanges);
}

} // namespace NKqp
} // namespace NKikimr
