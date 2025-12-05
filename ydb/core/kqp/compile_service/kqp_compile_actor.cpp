#include "kqp_compile_service.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/host/kqp_translate.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/core/util/stlog.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/string/escape.h>

#include <ydb/core/base/cputime.h>

namespace NKikimr::NKqp {

static const TString YqlName = "CompileActor";

using namespace NKikimrConfig;
using namespace NThreading;
using namespace NYql;
using namespace NYql::NDq;


class TKqpCompileActor : public TActorBootstrapped<TKqpCompileActor> {
public:
    using TBase = TActorBootstrapped<TKqpCompileActor>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_ACTOR;
    }

    TKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
        const TTableServiceConfig& tableServiceConfig,
        const TQueryServiceConfig& queryServiceConfig,
        TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
        const TGUCSettings::TPtr& gUCSettings,
        const TMaybe<TString>& applicationName, const TString& uid, const TKqpQueryId& queryId,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& clientAddress,
        TKqpDbCountersPtr dbCounters, std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        NWilson::TTraceId traceId, TKqpTempTablesState::TConstPtr tempTablesState, bool collectFullDiagnostics,
        bool perStatementResult,
        ECompileActorAction compileAction, TMaybe<TQueryAst> queryAst,
        std::shared_ptr<NYql::TExprContext> splitCtx,
        NYql::TExprNode::TPtr splitExpr)
        : Owner(owner)
        , ModuleResolverState(moduleResolverState)
        , Counters(counters)
        , GUCSettings(gUCSettings)
        , ApplicationName(applicationName)
        , FederatedQuerySetup(federatedQuerySetup)
        , Uid(uid)
        , QueryId(queryId)
        , QueryRef(QueryId.Text, QueryId.QueryParameterTypes, queryAst)
        , UserToken(userToken)
        , ClientAddress(clientAddress)
        , DbCounters(dbCounters)
        , KqpSettings(kqpSettings)
        , TableServiceConfig(tableServiceConfig)
        , QueryServiceConfig(queryServiceConfig)
        , CompilationTimeout(TDuration::MilliSeconds(tableServiceConfig.GetCompileTimeoutMs()))
        , SplitCtx(std::move(splitCtx))
        , SplitExpr(std::move(splitExpr))
        , UserRequestContext(userRequestContext)
        , CompileActorSpan(TWilsonKqp::CompileActor, std::move(traceId), "CompileActor")
        , TempTablesState(std::move(tempTablesState))
        , CollectFullDiagnostics(collectFullDiagnostics)
        , CompileAction(compileAction)
        , QueryAst(std::move(queryAst))
        , EnforcedSqlVersion(tableServiceConfig.GetEnforceSqlVersionV1())
        , EnableNewRBO(tableServiceConfig.GetEnableNewRBO())
        , EnableFallbackToYqlOptimizer(tableServiceConfig.GetEnableFallbackToYqlOptimizer())
    {
        Config = BuildConfiguration(tableServiceConfig);
        PerStatementResult = perStatementResult && Config->EnablePerStatementQueryExecution;
    }

    TKikimrConfiguration::TPtr BuildConfiguration(const TTableServiceConfig& tableServiceConfig) {
        NYql::TKikimrConfiguration::TPtr config = MakeIntrusive<TKikimrConfiguration>();

        config->Init(KqpSettings->DefaultSettings.GetDefaultSettings(), QueryId.Cluster, KqpSettings->Settings, false);

        if (!QueryId.Database.empty()) {
            config->_KqpTablePathPrefix = QueryId.Database;
        }

        ApplyServiceConfig(*config, tableServiceConfig);

        if (!tableServiceConfig.HasSqlVersion() || tableServiceConfig.GetSqlVersion() != 0) {
            EnforcedSqlVersion = false;
        } else if (EnforcedSqlVersion) {
            STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC12, "Enforced SQL version 1",
                (SelfId, SelfId()),
                (TraceId, TraceId()),
                (CurrentSqlVersion, tableServiceConfig.GetSqlVersion()),
                (QueryText, EscapeC(QueryId.Text)));

            config->_KqpYqlSyntaxVersion = 1;
        } else {
            EnforcedSqlVersion = false;
        }

        // This is either the default setting or the explicit exclusion of a new RBO when compilation fails and recompilation is attempted.
        config->EnableNewRBO = EnableNewRBO;

        if (QueryId.Settings.QueryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT || QueryId.Settings.QueryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY) {
            ui32 scriptResultRowsLimit = QueryServiceConfig.GetScriptResultRowsLimit();
            if (scriptResultRowsLimit > 0) {
                config->_ResultRowsLimit = scriptResultRowsLimit;
            } else {
                config->_ResultRowsLimit.Clear();
            }
        }

        config->FreezeDefaults();

        return config;
    }

    void Bootstrap(const TActorContext& ctx) {
        switch(CompileAction) {
            case ECompileActorAction::PARSE:
                StartParsing(ctx);
                break;
            case ECompileActorAction::COMPILE:
                StartCompilation(ctx);
                break;
            case ECompileActorAction::SPLIT:
                StartSplitting(ctx);
                break;
        }
    }

    void Die(const NActors::TActorContext& ctx) override {
        if (TimeoutTimerActorId) {
            ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }

        TBase::Die(ctx);
    }

private:
    STFUNC(CompileState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvContinueProcess, HandleCompile);
                cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                UnexpectedEvent("CompileState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }

    STFUNC(SplitState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvContinueProcess, HandleSplit);
                cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                UnexpectedEvent("SplitState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }


private:
    TVector<TQueryAst> GetAstStatements(const TActorContext &ctx) {
        TString cluster = QueryId.Cluster;
        ui16 kqpYqlSyntaxVersion = Config->_KqpYqlSyntaxVersion.Get().GetRef();

        TKqpTranslationSettingsBuilder settingsBuilder(ConvertType(QueryId.Settings.QueryType), kqpYqlSyntaxVersion, cluster, QueryId.Text, Config->BindingsMode, GUCSettings);
        settingsBuilder.SetKqpTablePathPrefix(Config->_KqpTablePathPrefix.Get().GetRef())
            .SetIsEnableExternalDataSources(AppData(ctx)->FeatureFlags.GetEnableExternalDataSources())
            .SetIsEnablePgConstsToParams(Config->EnablePgConstsToParams)
            .SetApplicationName(ApplicationName)
            .SetQueryParameters(QueryId.QueryParameterTypes)
            .SetIsEnablePgSyntax(AppData(ctx)->FeatureFlags.GetEnablePgSyntax())
            .SetFromConfig(*Config);

        return ParseStatements(QueryId.Text, QueryId.Settings.Syntax, QueryId.IsSql(), settingsBuilder, PerStatementResult);
    }

    void ReplySplitResult(const TActorContext &ctx, IKqpHost::TSplitResult&& result) {
        Y_UNUSED(ctx);
        STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC01, "Send split result",
            (SelfId, SelfId()),
            (Owner, Owner),
            (TraceId, TraceId()),
            (Status, GetYdbStatus(result)),
            (Issues, result.Issues().ToOneLineString()));

        auto responseEv = MakeHolder<TEvKqp::TEvSplitResponse>(
            GetYdbStatus(result), result.Issues(),
            QueryId, std::move(result.Exprs), std::move(result.World), std::move(result.Ctx));
        Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        if (CompileActorSpan) {
            CompileActorSpan.End();
        }

        PassAway();
    }

    void StartSplitting(const TActorContext &ctx) {
        Become(&TKqpCompileActor::SplitState);
        TimeoutTimerActorId = CreateLongTimer(ctx, CompilationTimeout, new IEventHandle(SelfId(), SelfId(),
            new TEvents::TEvWakeup()));

        const auto prepareSettings = PrepareCompilationSettings(ctx);
        AsyncSplitResult = KqpHost->SplitQuery(QueryRef, prepareSettings);
        ContinueSplittig(ctx);
    }

    void ContinueSplittig(const TActorContext &ctx) {
        TActorSystem* actorSystem = ctx.ActorSystem();
        TActorId selfId = ctx.SelfID;

        auto callback = [actorSystem, selfId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(0, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        AsyncSplitResult->Continue().Apply(callback);
    }

    void HandleSplit(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_ENSURE(!ev->Get()->QueryId);

        if (!ev->Get()->Finished) {
            ContinueSplittig(ctx);
            return;
        }

        auto splitResult = std::move(AsyncSplitResult->GetResult());
        ReplySplitResult(ctx, std::move(splitResult));
    }

    void StartParsing(const TActorContext &ctx) {
        Become(&TKqpCompileActor::CompileState);
        ReplyParseResult(ctx, GetAstStatements(ctx));
    }

    void StartCompilation(const TActorContext &ctx) {
        StartTime = TInstant::Now();

        Counters->ReportCompileStart(DbCounters);

        if (CompileActorSpan) {
            STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC02, "TraceId info",
                (SelfId, SelfId()),
                (Verbosity, CompileActorSpan.GetTraceId().GetVerbosity()),
                (TraceId, TraceId()));
        }

        STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC03, "Start compilation",
            (SelfId, SelfId()),
            (Cluster, QueryId.Cluster),
            (Database, QueryId.Database),
            (TraceId, TraceId()),
            (Text, EscapeC(QueryId.Text)),
            (StartTime, StartTime));

        TimeoutTimerActorId = CreateLongTimer(ctx, CompilationTimeout, new IEventHandle(SelfId(), SelfId(),
            new TEvents::TEvWakeup()));

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, UserRequestContext->TraceId);

        auto prepareSettings = PrepareCompilationSettings(ctx);
        StartCompilationWithSettings(prepareSettings);
        Continue(ctx);
        Become(&TKqpCompileActor::CompileState);
    }

    void StartCompilationWithSettings(IKqpHost::TPrepareSettings& prepareSettings) {
        NCpuTime::TCpuTimer timer(CompileCpuTime);

        switch (QueryId.Settings.QueryType) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
                AsyncCompileResult = KqpHost->PrepareDataQuery(QueryRef, prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_AST_DML:
                AsyncCompileResult = KqpHost->PrepareDataQueryAst(QueryRef, prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
                AsyncCompileResult = KqpHost->PrepareScanQuery(QueryRef, QueryId.IsSql(), prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
                prepareSettings.ConcurrentResults = false;
                AsyncCompileResult = KqpHost->PrepareGenericQuery(QueryRef, prepareSettings, SplitExpr.get());
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
                AsyncCompileResult = KqpHost->PrepareGenericQuery(QueryRef, prepareSettings, SplitExpr.get());
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
                AsyncCompileResult = KqpHost->PrepareGenericScript(QueryRef, prepareSettings, SplitExpr.get());
                break;

            default:
                YQL_ENSURE(false, "Unexpected query type: " << QueryId.Settings.QueryType);
        }
    }

    void Continue(const TActorContext &ctx) {
        TActorSystem* actorSystem = ctx.ActorSystem();
        TActorId selfId = ctx.SelfID;

        auto callback = [actorSystem, selfId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(0, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        AsyncCompileResult->Continue().Apply(callback);
    }

    IKqpHost::TPrepareSettings PrepareCompilationSettings(const TActorContext &ctx) {
        // If CurrentSqlVersion differs from the frozen Config, create a new Config with updated SqlVersion
        TKqpRequestCounters::TPtr counters = new TKqpRequestCounters;
        counters->Counters = Counters;
        counters->DbCounters = DbCounters;
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(AppData(ctx)->Counters);
        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader =
            std::make_shared<TKqpTableMetadataLoader>(
                QueryId.Cluster, TlsActivationContext->ActorSystem(), Config, true, TempTablesState, FederatedQuerySetup);
        Gateway = CreateKikimrIcGateway(QueryId.Cluster, QueryId.Settings.QueryType, QueryId.Database, QueryId.DatabaseId, std::move(loader),
            ctx.ActorSystem(), ctx.SelfID.NodeId(), counters, QueryServiceConfig);
        Gateway->SetToken(QueryId.Cluster, UserToken);
        Gateway->SetClientAddress(ClientAddress);

        Config->FeatureFlags = AppData(ctx)->FeatureFlags;

        KqpHost = CreateKqpHost(Gateway, QueryId.Cluster, QueryId.Database, Config, ModuleResolverState->ModuleResolver,
            FederatedQuerySetup, UserToken, GUCSettings, QueryServiceConfig, ApplicationName, AppData(ctx)->FunctionRegistry,
            false, false, std::move(TempTablesState), nullptr, SplitCtx.get(), UserRequestContext);

        IKqpHost::TPrepareSettings prepareSettings;
        prepareSettings.DocumentApiRestricted = QueryId.Settings.DocumentApiRestricted;
        prepareSettings.IsInternalCall = QueryId.Settings.IsInternalCall;
        prepareSettings.RuntimeParameterSizeLimit = QueryId.Settings.RuntimeParameterSizeLimit;
        prepareSettings.RuntimeParameterSizeLimitSatisfied = QueryId.Settings.RuntimeParameterSizeLimitSatisfied;

        switch (QueryId.Settings.Syntax) {
            case Ydb::Query::Syntax::SYNTAX_YQL_V1:
                prepareSettings.UsePgParser = false;
                prepareSettings.SyntaxVersion = 1;
                break;

            case Ydb::Query::Syntax::SYNTAX_PG:
                prepareSettings.UsePgParser = true;
                break;

            default:
                break;
        }

        return prepareSettings;
    }

    void AddMessageToReplayLog(const TString& queryPlan) {
        NJson::TJsonValue replayMessage(NJson::JSON_MAP);

        NJson::TJsonValue tablesMeta(NJson::JSON_ARRAY);
        auto collectedSchemeData = Gateway->GetCollectedSchemeData();
        for (auto proto: collectedSchemeData) {
            tablesMeta.AppendValue(Base64Encode(proto.SerializeAsString()));
        }

        replayMessage.InsertValue("query_id", Uid);
        replayMessage.InsertValue("version", "1.0");
        NJson::TJsonValue queryParameterTypes(NJson::JSON_MAP);
        if (QueryId.QueryParameterTypes) {
            for (const auto& [paramName, paramType] : *QueryId.QueryParameterTypes) {
                queryParameterTypes[paramName] = Base64Encode(paramType.SerializeAsString());
            }
        }
        replayMessage.InsertValue("query_parameter_types", std::move(queryParameterTypes));
        replayMessage.InsertValue("created_at", ToString(TlsActivationContext->ActorSystem()->Timestamp().Seconds()));
        replayMessage.InsertValue("query_syntax", ToString(Config->_KqpYqlSyntaxVersion.Get().GetRef()));
        replayMessage.InsertValue("query_database", QueryId.Database);
        replayMessage.InsertValue("query_cluster", QueryId.Cluster);
        replayMessage.InsertValue("query_type", ToString(QueryId.Settings.QueryType));

        if (CollectFullDiagnostics) {
            NJson::TJsonValue tablesMetaUserView(NJson::JSON_ARRAY);
            for (auto proto: collectedSchemeData) {
                tablesMetaUserView.AppendValue(NProtobufJson::Proto2Json(proto));
            }

            replayMessage.InsertValue("table_metadata", tablesMetaUserView);
            replayMessage.InsertValue("table_meta_serialization_type", EMetaSerializationType::Json);

            ReplayMessageUserView = NJson::WriteJson(replayMessage, /*formatOutput*/ false);
        }

        replayMessage.InsertValue("query_plan", queryPlan);
        replayMessage.InsertValue("query_text", EscapeC(QueryId.Text));
        replayMessage.InsertValue("table_metadata", TString(NJson::WriteJson(tablesMeta, false)));
        replayMessage.InsertValue("table_meta_serialization_type", EMetaSerializationType::EncodedProto);

        GUCSettings->ExportToJson(replayMessage);

        TString message(NJson::WriteJson(replayMessage, /*formatOutput*/ false));
        STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC04, "Built the replay message: " << message,
            (SelfId, SelfId()),
            (TraceId, TraceId()));
        ReplayMessage = std::move(message);
    }

    void Reply() {
        Y_ENSURE(KqpCompileResult);
        STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC05, "Send response",
            (SelfId, SelfId()),
            (Owner, Owner),
            (TraceId, TraceId()),
            (Status, KqpCompileResult->Status),
            (Issues, KqpCompileResult->Issues.ToString()),
            (Uid, KqpCompileResult->Uid));

        if (ReplayMessageUserView) {
            KqpCompileResult->ReplayMessageUserView = std::move(*ReplayMessageUserView);
        }
        auto responseEv = MakeHolder<TEvKqp::TEvCompileResponse>(KqpCompileResult);

        responseEv->ReplayMessage = std::move(ReplayMessage);
        ReplayMessage = std::nullopt;
        ReplayMessageUserView = std::nullopt;
        auto& stats = responseEv->Stats;
        stats.FromCache = false;
        stats.DurationUs = (TInstant::Now() - StartTime).MicroSeconds();
        stats.CpuTimeUs = CompileCpuTime.MicroSeconds();
        Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        if (CompileActorSpan) {
            CompileActorSpan.End();
        }

        PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TIssues& issues) {
        auto meta = CollectMeta();
        if (!KqpCompileResult) {
            KqpCompileResult = TKqpCompileResult::Make(Uid, status, issues, ETableReadType::Other, CompileCpuTime, std::move(QueryId), std::move(QueryAst), meta);
        } else {
            KqpCompileResult = TKqpCompileResult::Make(Uid, status, issues, ETableReadType::Other, CompileCpuTime, std::move(KqpCompileResult->Query), std::move(KqpCompileResult->QueryAst), meta);
        }

        Reply();
    }

    void InternalError(const TString message) {
        STLOG(PRI_ERROR, NKikimrServices::KQP_COMPILE_ACTOR, KQPC06, "Internal error: " << message,
            (SelfId, SelfId()),
            (TraceId, TraceId()));

        NYql::TIssue issue(NYql::TPosition(), "Internal error while compiling query.");
        issue.AddSubIssue(MakeIntrusive<TIssue>(NYql::TPosition(), message));

        ReplyError(Ydb::StatusIds::INTERNAL_ERROR, {issue});
    }

    void UnexpectedEvent(const TString& state, ui32 eventType) {
        InternalError(TStringBuilder() << "TKqpCompileActor, unexpected event: " << eventType
            << ", at state:" << state);
    }

    void ReplyParseResult(const TActorContext &ctx, TVector<TQueryAst>&& astStatements) {
        Y_UNUSED(ctx);

        if (astStatements.empty()) {
            NYql::TIssue issue(NYql::TPosition(), "Parsing result of query is empty");
            ReplyError(Ydb::StatusIds::INTERNAL_ERROR, {issue});
            return;
        }

        for (size_t statementId = 0; statementId < astStatements.size(); ++statementId) {
            if (!astStatements[statementId].Ast || !astStatements[statementId].Ast->IsOk() || !astStatements[statementId].Ast->Root) {
                STLOG(PRI_ERROR, NKikimrServices::KQP_COMPILE_ACTOR, KQPC07, "Get parsing result with error",
                    (SelfId, SelfId()),
                    (TraceId, TraceId()),
                    (Owner, Owner),
                    (StatementId, statementId));

                auto status = GetYdbStatus(astStatements[statementId].Ast->Issues);

                NYql::TIssue issue(NYql::TPosition(), "Error while parsing query.");
                for (const auto& i : astStatements[statementId].Ast->Issues) {
                    issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
                }

                ReplyError(status, {issue});
                return;
            }
        }

        STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC08, "Send parsing result",
            (SelfId, SelfId()),
            (Owner, Owner),
            (TraceId, TraceId()),
            (StatementsSize, astStatements.size()));

        auto responseEv = MakeHolder<TEvKqp::TEvParseResponse>(QueryId, std::move(astStatements));
        Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        if (CompileActorSpan) {
            CompileActorSpan.End();
        }

        PassAway();
    }

    void FillCompileResult(std::unique_ptr<NKikimrKqp::TPreparedQuery> preparingQuery, NKikimrKqp::EQueryType queryType,
            bool allowCache, bool success) {
        auto preparedQueryHolder = std::make_shared<TPreparedQueryHolder>(
            preparingQuery.release(), AppData()->FunctionRegistry, !success);
        preparedQueryHolder->MutableLlvmSettings().Fill(Config, queryType);
        KqpCompileResult->PreparedQuery = preparedQueryHolder;
        KqpCompileResult->AllowCache = CanCacheQuery(KqpCompileResult->PreparedQuery->GetPhysicalQuery()) && allowCache;
    }

    void HandleCompile(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_ENSURE(!ev->Get()->QueryId);

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, UserRequestContext->TraceId);

        if (!ev->Get()->Finished) {
            NCpuTime::TCpuTimer timer(CompileCpuTime);
            Continue(ctx);
            return;
        }

        auto kqpResult = std::move(AsyncCompileResult->GetResult());
        auto status = GetYdbStatus(kqpResult);
        auto meta = CollectMeta();

        if (kqpResult.NeedToSplit) {
            KqpCompileResult = TKqpCompileResult::Make(
                Uid, status, kqpResult.Issues(), ETableReadType::Other, CompileCpuTime, std::move(QueryId), std::move(QueryAst), meta, true);
            Reply();
            return;
        }

        if (IsSuitableToFallbackToYqlOptimizer(status)) {
            Counters->ReportCompileNewRBOFailed(DbCounters);
            // Disable compilation with new RBO.
            EnableNewRBO = false;
            TString logMessage = "Compilation with new RBO failed, retrying with YQL optimizer";
            RebuildConfigAndStartCompilation(ctx, std::move(logMessage));
            return;
        } else if (IsSuitableToReportSuccessOnNewRBO(status)) {
            Counters->ReportCompileNewRBOSuccess(DbCounters);
        } else if (IsSuitableToReportFailOnNewRBO(status)) {
            Counters->ReportCompileNewRBOFailed(DbCounters);
        }

        // If compilation failed and we tried SqlVersion = 1, retry with SqlVersion = 0
        if (IsSuitableToFallbackToSqlV0(status)) {
            Counters->ReportCompileEnforceConfigFailed(DbCounters);
            STLOG(PRI_ERROR, NKikimrServices::KQP_COMPILE_ACTOR, KQPC13, "Compilation with SqlVersion = 1 failed, retrying with SqlVersion = 0",
                (SelfId, ctx.SelfID),
                (Database, QueryId.Database),
                (TraceId, TraceId()),
                (Text, EscapeC(QueryId.Text)));

            EnforcedSqlVersion = false;
            TString logMessage = "Compilation with SqlVersion = 1 failed, retrying with SqlVersion = 0";
            RebuildConfigAndStartCompilation(ctx, std::move(logMessage));
            return;
        } else if (IsSuitableToReportSuccessOnEnforcedSqlVersion(status)) {
            Counters->ReportCompileEnforceConfigSuccess(DbCounters);
        }

        auto database = QueryId.Database;
        if (kqpResult.SqlVersion) {
            Counters->ReportSqlVersion(DbCounters, *kqpResult.SqlVersion);
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            AddMessageToReplayLog(kqpResult.QueryPlan);
        }

        ETableReadType maxReadType = ExtractMostHeavyReadType(kqpResult.QueryPlan);

        auto queryType = QueryId.Settings.QueryType;

        KqpCompileResult = TKqpCompileResult::Make(Uid, status, kqpResult.Issues(), maxReadType, CompileCpuTime, std::move(QueryId), std::move(QueryAst), meta);
        KqpCompileResult->CommandTagName = kqpResult.CommandTagName;

        if (status == Ydb::StatusIds::SUCCESS) {
            YQL_ENSURE(kqpResult.PreparingQuery);
            FillCompileResult(std::move(kqpResult.PreparingQuery), queryType, kqpResult.AllowCache, true);

            auto now = TInstant::Now();
            auto duration = now - StartTime;
            Counters->ReportCompileDurations(DbCounters, duration, CompileCpuTime);

            STLOG(PRI_DEBUG, NKikimrServices::KQP_COMPILE_ACTOR, KQPC09, "Compilation successful",
                (SelfId, ctx.SelfID),
                (TraceId, TraceId()),
                (Duration, duration));
        } else {
            if (kqpResult.PreparingQuery) {
                FillCompileResult(std::move(kqpResult.PreparingQuery), queryType, kqpResult.AllowCache, false);
            }

            STLOG(PRI_ERROR, NKikimrServices::KQP_COMPILE_ACTOR, KQPC10, "Compilation failed",
                (SelfId, ctx.SelfID),
                (TraceId, TraceId()),
                (Status, Ydb::StatusIds_StatusCode_Name(status)),
                (Issues, kqpResult.Issues().ToString()));
            Counters->ReportCompileError(DbCounters);
        }

        Reply();
    }

    void HandleTimeout() {
        STLOG(PRI_NOTICE, NKikimrServices::KQP_COMPILE_ACTOR, KQPC11, "Compilation timeout",
            (SelfId, SelfId()),
            (Cluster, QueryId.Cluster),
            (Database, QueryId.Database),
            (TraceId, TraceId()),
            (Text, EscapeC(QueryId.Text)),
            (StartTime, StartTime));

        NYql::TIssue issue(NYql::TPosition(), "Query compilation timed out.");
        return ReplyError(Ydb::StatusIds::TIMEOUT, {issue});
    }

    NJson::TJsonValue CollectMeta() {
        NJson::TJsonValue meta;
        meta.AppendValue("metadata");
        NJson::TJsonValue parameters;
        if (QueryId.QueryParameterTypes) {
            for (const auto& [name, typedValue] : *QueryId.QueryParameterTypes) {
                parameters[name] = Base64Encode(typedValue.SerializeAsString());
            }
        }
        meta["parameters"] = parameters;
        return meta;
    }

    TString TraceId() const {
        return CompileActorSpan ? CompileActorSpan.GetTraceId().GetHexTraceId() : TString();
    }

private:
    void RebuildConfigAndStartCompilation(const TActorContext &ctx, TString&& logMessage) {
        LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, logMessage
                << ", self: " << ctx.SelfID
                << ", database: " << QueryId.Database
                << ", text: \"" << EscapeC(QueryId.Text) << "\"");

        // Explicitly drop a pointer to result, it holds pointer `TExprNode` allocated from `TExprContext` in KqpHost
        // and we want rebuild a KqpHost.
        AsyncCompileResult.Drop();
        Config = BuildConfiguration(TableServiceConfig);
        auto prepareSettings = PrepareCompilationSettings(ctx);

        StartCompilationWithSettings(prepareSettings);
        Continue(ctx);
    }

    bool IsSuitableToFallbackToYqlOptimizer(Ydb::StatusIds::StatusCode status) {
        return EnableNewRBO && EnableFallbackToYqlOptimizer && status != Ydb::StatusIds::SUCCESS;
    }

    bool IsSuitableToReportSuccessOnNewRBO(Ydb::StatusIds::StatusCode status) {
        return EnableNewRBO && status == Ydb::StatusIds::SUCCESS;
    }

    bool IsSuitableToReportFailOnNewRBO(Ydb::StatusIds::StatusCode status) {
        return EnableNewRBO && status != Ydb::StatusIds::SUCCESS;
    }

    bool IsSuitableToFallbackToSqlV0(Ydb::StatusIds::StatusCode status) {
        return !EnableNewRBO && EnforcedSqlVersion && status != Ydb::StatusIds::SUCCESS;
    }

    bool IsSuitableToReportSuccessOnEnforcedSqlVersion(Ydb::StatusIds::StatusCode status) {
        return !EnableNewRBO && EnforcedSqlVersion && status == Ydb::StatusIds::SUCCESS;
    }

    TActorId Owner;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TIntrusivePtr<TKqpCounters> Counters;
    TGUCSettings::TPtr GUCSettings;
    TMaybe<TString> ApplicationName;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    TString Uid;
    TKqpQueryId QueryId;
    TKqpQueryRef QueryRef;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString ClientAddress;
    TKqpDbCountersPtr DbCounters;
    TKikimrConfiguration::TPtr Config;
    TKqpSettings::TConstPtr KqpSettings;
    TTableServiceConfig TableServiceConfig;
    TQueryServiceConfig QueryServiceConfig;
    TDuration CompilationTimeout;
    TInstant StartTime;
    TDuration CompileCpuTime;
    TInstant RecompileStartTime;
    TActorId TimeoutTimerActorId;
    std::shared_ptr<NYql::TExprContext> SplitCtx;
    NYql::TExprNode::TPtr SplitExpr;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncCompileResult;
    TIntrusivePtr<IKqpHost::IAsyncSplitResult> AsyncSplitResult;
    std::shared_ptr<TKqpCompileResult> KqpCompileResult;
    std::optional<TString> ReplayMessage;  // here metadata is encoded protobuf - for logs
    std::optional<TString> ReplayMessageUserView;  // here metadata is part of json - full readable json for diagnostics

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NWilson::TSpan CompileActorSpan;

    TKqpTempTablesState::TConstPtr TempTablesState;
    bool CollectFullDiagnostics;

    bool PerStatementResult;
    ECompileActorAction CompileAction;
    TMaybe<TQueryAst> QueryAst;
    bool EnforcedSqlVersion;
    bool EnableNewRBO;
    bool EnableFallbackToYqlOptimizer;
};

void ApplyServiceConfig(TKikimrConfiguration& kqpConfig, const TTableServiceConfig& serviceConfig) {
    if (serviceConfig.HasSqlVersion()) {
        kqpConfig._KqpYqlSyntaxVersion = serviceConfig.GetSqlVersion();
    }
    if (serviceConfig.GetQueryLimits().HasResultRowsLimit()) {
        kqpConfig._ResultRowsLimit = serviceConfig.GetQueryLimits().GetResultRowsLimit();
    }

    kqpConfig.EnableKqpScanQuerySourceRead = serviceConfig.GetEnableKqpScanQuerySourceRead();
    kqpConfig.EnableKqpScanQueryStreamIdxLookupJoin = serviceConfig.GetEnableKqpScanQueryStreamIdxLookupJoin();
    kqpConfig.EnableKqpDataQueryStreamIdxLookupJoin = serviceConfig.GetEnableKqpDataQueryStreamIdxLookupJoin();
    kqpConfig.BindingsMode = RemapBindingsMode(serviceConfig.GetBindingsMode());
    kqpConfig.EnablePgConstsToParams = serviceConfig.GetEnablePgConstsToParams() && serviceConfig.GetEnableAstCache();
    kqpConfig.ExtractPredicateRangesLimit = serviceConfig.GetExtractPredicateRangesLimit();
    kqpConfig.EnablePerStatementQueryExecution = serviceConfig.GetEnablePerStatementQueryExecution();
    kqpConfig.EnableCreateTableAs = serviceConfig.GetEnableCreateTableAs();
    kqpConfig.EnableDataShardCreateTableAs = serviceConfig.GetEnableDataShardCreateTableAs();
    kqpConfig.AllowOlapDataQuery = serviceConfig.GetAllowOlapDataQuery();
    kqpConfig.EnableOlapSink = serviceConfig.GetEnableOlapSink();
    kqpConfig.EnableOltpSink = serviceConfig.GetEnableOltpSink();
    kqpConfig.EnableHtapTx = serviceConfig.GetEnableHtapTx();
    kqpConfig.EnableStreamWrite = serviceConfig.GetEnableStreamWrite();
    kqpConfig.BlockChannelsMode = serviceConfig.GetBlockChannelsMode();
    kqpConfig.IdxLookupJoinsPrefixPointLimit = serviceConfig.GetIdxLookupJoinPointsLimit();
    kqpConfig.DefaultCostBasedOptimizationLevel = serviceConfig.GetDefaultCostBasedOptimizationLevel();
    kqpConfig.DefaultEnableShuffleElimination = serviceConfig.GetDefaultEnableShuffleElimination();
    kqpConfig.EnableConstantFolding = serviceConfig.GetEnableConstantFolding();
    kqpConfig.EnableFoldUdfs = serviceConfig.GetEnableFoldUdfs();
    kqpConfig.SetDefaultEnabledSpillingNodes(serviceConfig.GetEnableSpillingNodes());
    kqpConfig.EnableSpilling = serviceConfig.GetEnableQueryServiceSpilling();
    kqpConfig.EnableSnapshotIsolationRW = serviceConfig.GetEnableSnapshotIsolationRW();
    kqpConfig.AllowMultiBroadcasts = serviceConfig.GetAllowMultiBroadcasts();
    kqpConfig.EnableNewRBO = serviceConfig.GetEnableNewRBO();
    kqpConfig.EnableSpillingInHashJoinShuffleConnections = serviceConfig.GetEnableSpillingInHashJoinShuffleConnections();
    kqpConfig.EnableOlapScalarApply = serviceConfig.GetEnableOlapScalarApply();
    kqpConfig.EnableOlapSubstringPushdown = serviceConfig.GetEnableOlapSubstringPushdown();
    kqpConfig.EnableIndexStreamWrite = serviceConfig.GetEnableIndexStreamWrite();
    kqpConfig.EnableOlapPushdownProjections = serviceConfig.GetEnableOlapPushdownProjections();
    kqpConfig.LangVer = serviceConfig.GetDefaultLangVer();
    kqpConfig.EnableParallelUnionAllConnectionsForExtend = serviceConfig.GetEnableParallelUnionAllConnectionsForExtend();
    kqpConfig.EnableTempTablesForUser = serviceConfig.GetEnableTempTablesForUser();
    kqpConfig.EnableSimpleProgramsSinglePartitionOptimization = serviceConfig.GetEnableSimpleProgramsSinglePartitionOptimization();
    kqpConfig.EnableSimpleProgramsSinglePartitionOptimizationBroadPrograms = serviceConfig.GetEnableSimpleProgramsSinglePartitionOptimizationBroadPrograms();

    kqpConfig.EnableOlapPushdownAggregate = serviceConfig.GetEnableOlapPushdownAggregate();
    kqpConfig.EnableOrderOptimizaionFSM = serviceConfig.GetEnableOrderOptimizaionFSM();
    kqpConfig.EnableTopSortSelectIndex = serviceConfig.GetEnableTopSortSelectIndex();
    kqpConfig.EnablePointPredicateSortAutoSelectIndex = serviceConfig.GetEnablePointPredicateSortAutoSelectIndex();
    kqpConfig.EnableDqHashCombineByDefault = serviceConfig.GetEnableDqHashCombineByDefault();
    kqpConfig.EnableWatermarks = serviceConfig.GetEnableWatermarks();
    kqpConfig.EnableBuildAggregationResultStages = serviceConfig.GetEnableBuildAggregationResultStages();
    kqpConfig.EnableFallbackToYqlOptimizer = serviceConfig.GetEnableFallbackToYqlOptimizer();

    if (const auto limit = serviceConfig.GetResourceManager().GetMkqlHeavyProgramMemoryLimit()) {
        kqpConfig._KqpYqlCombinerMemoryLimit = std::max(1_GB, limit - (limit >> 2U));
    }

    if (serviceConfig.GetFilterPushdownOverJoinOptionalSide()) {
        kqpConfig.FilterPushdownOverJoinOptionalSide = true;
        kqpConfig.YqlCoreOptimizerFlags.insert("fuseequijoinsinputmultilabels");
        kqpConfig.YqlCoreOptimizerFlags.insert("pullupflatmapoverjoinmultiplelabels");
        kqpConfig.YqlCoreOptimizerFlags.insert("sqlinwithnothingornull");
    }

    switch(serviceConfig.GetDefaultHashShuffleFuncType()) {
        case NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V1:
            kqpConfig.DefaultHashShuffleFuncType = NYql::NDq::EHashShuffleFuncType::HashV1;
            break;
        case NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2:
            kqpConfig.DefaultHashShuffleFuncType = NYql::NDq::EHashShuffleFuncType::HashV2;
            break;
    }

    switch(serviceConfig.GetBackportMode()) {
        case NKikimrConfig::TTableServiceConfig_EBackportMode_Released:
            kqpConfig.BackportMode = NYql::EBackportCompatibleFeaturesMode::Released;
            break;
        case NKikimrConfig::TTableServiceConfig_EBackportMode_All:
            kqpConfig.BackportMode = NYql::EBackportCompatibleFeaturesMode::All;
            break;
    }
}

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const TTableServiceConfig& tableServiceConfig,
    const TQueryServiceConfig& queryServiceConfig,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TString& uid, const TKqpQueryId& query, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& clientAddress,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup, TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings,
    const TMaybe<TString>& applicationName, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    NWilson::TTraceId traceId, TKqpTempTablesState::TConstPtr tempTablesState,
    ECompileActorAction compileAction, TMaybe<TQueryAst> queryAst, bool collectFullDiagnostics,
    bool perStatementResult, std::shared_ptr<NYql::TExprContext> splitCtx, NYql::TExprNode::TPtr splitExpr)
{
    return new TKqpCompileActor(owner, kqpSettings, tableServiceConfig, queryServiceConfig,
                                moduleResolverState, counters, gUCSettings, applicationName,
                                uid, query, userToken, clientAddress, dbCounters,
                                federatedQuerySetup, userRequestContext,
                                std::move(traceId), std::move(tempTablesState), collectFullDiagnostics,
                                perStatementResult, compileAction, std::move(queryAst),
                                std::move(splitCtx), std::move(splitExpr));
}

} // namespace NKikimr::NKqp
