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

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/string/escape.h>

#include <ydb/core/base/cputime.h>

namespace NKikimr {
namespace NKqp {

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
        const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
        TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
        const TGUCSettings::TPtr& gUCSettings,
        const TMaybe<TString>& applicationName, const TString& uid, const TKqpQueryId& queryId,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        TKqpDbCountersPtr dbCounters, std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        NWilson::TTraceId traceId, TKqpTempTablesState::TConstPtr tempTablesState, bool collectFullDiagnostics,
        bool perStatementResult,
        ECompileActorAction compileAction, TMaybe<TQueryAst> queryAst,
        NYql::TExprContext* splitCtx,
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
        , DbCounters(dbCounters)
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , QueryServiceConfig(queryServiceConfig)
        , MetadataProviderConfig(metadataProviderConfig)
        , CompilationTimeout(TDuration::MilliSeconds(tableServiceConfig.GetCompileTimeoutMs()))
        , UserRequestContext(userRequestContext)
        , CompileActorSpan(TWilsonKqp::CompileActor, std::move(traceId), "CompileActor")
        , TempTablesState(std::move(tempTablesState))
        , CollectFullDiagnostics(collectFullDiagnostics)
        , CompileAction(compileAction)
        , QueryAst(std::move(queryAst))
        , SplitCtx(splitCtx)
        , SplitExpr(splitExpr)
    {
        Config->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), QueryId.Cluster, kqpSettings->Settings, false);

        if (!QueryId.Database.empty()) {
            Config->_KqpTablePathPrefix = QueryId.Database;
        }

        ApplyServiceConfig(*Config, tableServiceConfig);

        if (QueryId.Settings.QueryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT || QueryId.Settings.QueryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY) {
            ui32 scriptResultRowsLimit = QueryServiceConfig.GetScriptResultRowsLimit();
            if (scriptResultRowsLimit > 0) {
                Config->_ResultRowsLimit = scriptResultRowsLimit;
            } else {
                Config->_ResultRowsLimit.Clear();
            }
        }
        PerStatementResult = perStatementResult && Config->EnablePerStatementQueryExecution;

        Config->FreezeDefaults();
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
                HFunc(TEvKqp::TEvContinueProcess, Handle);
                cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                UnexpectedEvent("CompileState", ev->GetTypeRewrite());
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
            .SetQueryParameters(QueryId.QueryParameterTypes);

        return ParseStatements(QueryId.Text, QueryId.Settings.Syntax, QueryId.IsSql(), settingsBuilder, PerStatementResult);
    }

    void ReplySplitResult(const TActorContext &ctx, IKqpHost::TSplitResult&& result) {
        Y_UNUSED(ctx);
        ALOG_DEBUG(NKikimrServices::KQP_COMPILE_ACTOR, "Send split result"
            << ", self: " << SelfId()
            << ", owner: " << Owner
            << (!result.Exprs.empty() ? ", split is successful" : ", split is not successful"));

        auto responseEv = MakeHolder<TEvKqp::TEvSplitResponse>(
            QueryId, std::move(result.Exprs), std::move(result.World), std::move(result.Ctx));
        Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        if (CompileActorSpan) {
            CompileActorSpan.End();
        }

        PassAway();
    }

    void StartSplitting(const TActorContext &ctx) {
        YQL_ENSURE(PerStatementResult);

        const auto prepareSettings = PrepareCompilationSettings(ctx);

        auto result = KqpHost->SplitQuery(QueryId.Text, prepareSettings);

        Become(&TKqpCompileActor::CompileState);
        ReplySplitResult(ctx, std::move(result));
    }

    void StartParsing(const TActorContext &ctx) {
        Become(&TKqpCompileActor::CompileState);
        ReplyParseResult(ctx, GetAstStatements(ctx));
    }

    void StartCompilation(const TActorContext &ctx) {
        StartTime = TInstant::Now();

        Counters->ReportCompileStart(DbCounters);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "traceId: verbosity = "
            << std::to_string(CompileActorSpan.GetTraceId().GetVerbosity()) << ", trace_id = "
            << std::to_string(CompileActorSpan.GetTraceId().GetTraceId()));

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Start compilation"
            << ", self: " << ctx.SelfID
            << ", cluster: " << QueryId.Cluster
            << ", database: " << QueryId.Database
            << ", text: \"" << EscapeC(QueryId.Text) << "\""
            << ", startTime: " << StartTime);

        TimeoutTimerActorId = CreateLongTimer(ctx, CompilationTimeout, new IEventHandle(SelfId(), SelfId(),
            new TEvents::TEvWakeup()));

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, UserRequestContext->TraceId);

        auto prepareSettings = PrepareCompilationSettings(ctx);

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
                AsyncCompileResult = KqpHost->PrepareGenericQuery(QueryRef, prepareSettings, SplitExpr);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
                AsyncCompileResult = KqpHost->PrepareGenericQuery(QueryRef, prepareSettings, SplitExpr);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
                AsyncCompileResult = KqpHost->PrepareGenericScript(QueryRef, prepareSettings);
                break;

            default:
                YQL_ENSURE(false, "Unexpected query type: " << QueryId.Settings.QueryType);
        }

        Continue(ctx);
        Become(&TKqpCompileActor::CompileState);
    }

    void Continue(const TActorContext &ctx) {
        TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        TActorId selfId = ctx.SelfID;

        auto callback = [actorSystem, selfId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(0, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        AsyncCompileResult->Continue().Apply(callback);
    }

    IKqpHost::TPrepareSettings PrepareCompilationSettings(const TActorContext &ctx) {
        TKqpRequestCounters::TPtr counters = new TKqpRequestCounters;
        counters->Counters = Counters;
        counters->DbCounters = DbCounters;
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(AppData(ctx)->Counters);
        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader =
            std::make_shared<TKqpTableMetadataLoader>(
                QueryId.Cluster, TlsActivationContext->ActorSystem(), Config, true, TempTablesState, 2 * TDuration::Seconds(MetadataProviderConfig.GetRefreshPeriodSeconds()));
        Gateway = CreateKikimrIcGateway(QueryId.Cluster, QueryId.Settings.QueryType, QueryId.Database, std::move(loader),
            ctx.ExecutorThread.ActorSystem, ctx.SelfID.NodeId(), counters, QueryServiceConfig);
        Gateway->SetToken(QueryId.Cluster, UserToken);

        Config->FeatureFlags = AppData(ctx)->FeatureFlags;

        KqpHost = CreateKqpHost(Gateway, QueryId.Cluster, QueryId.Database, Config, ModuleResolverState->ModuleResolver,
            FederatedQuerySetup, UserToken, GUCSettings, ApplicationName, AppData(ctx)->FunctionRegistry,
            false, false, std::move(TempTablesState), nullptr, SplitCtx);

        IKqpHost::TPrepareSettings prepareSettings;
        prepareSettings.DocumentApiRestricted = QueryId.Settings.DocumentApiRestricted;
        prepareSettings.IsInternalCall = QueryId.Settings.IsInternalCall;
        prepareSettings.PerStatementResult = PerStatementResult;

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
        replayMessage.InsertValue("query_text", EscapeC(QueryId.Text));
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
        replayMessage.InsertValue("query_plan", queryPlan);
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

        replayMessage.InsertValue("table_metadata", TString(NJson::WriteJson(tablesMeta, false)));
        replayMessage.InsertValue("table_meta_serialization_type", EMetaSerializationType::EncodedProto);

        GUCSettings->ExportToJson(replayMessage);

        TString message(NJson::WriteJson(replayMessage, /*formatOutput*/ false));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_ACTOR, "[" << SelfId() << "]: "
            << "Built the replay message " << message);

        ReplayMessage = std::move(message);
    }

    void Reply() {
        Y_ENSURE(KqpCompileResult);
        ALOG_DEBUG(NKikimrServices::KQP_COMPILE_ACTOR, "Send response"
            << ", self: " << SelfId()
            << ", owner: " << Owner
            << ", status: " << KqpCompileResult->Status
            << ", issues: " << KqpCompileResult->Issues.ToString()
            << ", uid: " << KqpCompileResult->Uid);

        auto responseEv = MakeHolder<TEvKqp::TEvCompileResponse>(KqpCompileResult);

        responseEv->ReplayMessage = std::move(ReplayMessage);
        responseEv->ReplayMessageUserView = std::move(ReplayMessageUserView);
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
        if (!KqpCompileResult) {
            KqpCompileResult = TKqpCompileResult::Make(Uid, status, issues, ETableReadType::Other, std::move(QueryId));
        } else {
            KqpCompileResult = TKqpCompileResult::Make(Uid, status, issues, ETableReadType::Other, std::move(KqpCompileResult->Query));
        }

        Reply();
    }

    void InternalError(const TString message) {
        ALOG_ERROR(NKikimrServices::KQP_COMPILE_ACTOR, "Internal error"
            << ", self: " << SelfId()
            << ", message: " << message);


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
                ALOG_ERROR(NKikimrServices::KQP_COMPILE_ACTOR, "Get parsing result with error"
                    << ", self: " << SelfId()
                    << ", owner: " << Owner
                    << ", statement id: " << statementId);

                auto status = GetYdbStatus(astStatements[statementId].Ast->Issues);

                NYql::TIssue issue(NYql::TPosition(), "Error while parsing query.");
                for (const auto& i : astStatements[statementId].Ast->Issues) {
                    issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
                }

                ReplyError(status, {issue});
                return;
            }
        }

        ALOG_DEBUG(NKikimrServices::KQP_COMPILE_ACTOR, "Send parsing result"
            << ", self: " << SelfId()
            << ", owner: " << Owner
            << ", statements size: " << astStatements.size());

        auto responseEv = MakeHolder<TEvKqp::TEvParseResponse>(QueryId, std::move(astStatements));
        Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        if (CompileActorSpan) {
            CompileActorSpan.End();
        }

        PassAway();
    }

    void FillCompileResult(std::unique_ptr<NKikimrKqp::TPreparedQuery> preparingQuery, NKikimrKqp::EQueryType queryType,
            bool allowCache) {
        auto preparedQueryHolder = std::make_shared<TPreparedQueryHolder>(
            preparingQuery.release(), AppData()->FunctionRegistry);
        preparedQueryHolder->MutableLlvmSettings().Fill(Config, queryType);
        KqpCompileResult->PreparedQuery = preparedQueryHolder;
        KqpCompileResult->AllowCache = CanCacheQuery(KqpCompileResult->PreparedQuery->GetPhysicalQuery()) && allowCache;

        if (QueryAst) {
            KqpCompileResult->Ast = QueryAst->Ast;
        }
    }

    void Handle(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_ENSURE(!ev->Get()->QueryId);

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, UserRequestContext->TraceId);

        if (!ev->Get()->Finished) {
            NCpuTime::TCpuTimer timer(CompileCpuTime);
            Continue(ctx);
            return;
        }

        auto kqpResult = std::move(AsyncCompileResult->GetResult());
        auto status = GetYdbStatus(kqpResult);

        if (kqpResult.NeedToSplit) {
            KqpCompileResult = TKqpCompileResult::Make(
                Uid, status, kqpResult.Issues(), ETableReadType::Other, std::move(QueryId), {}, true);
            Reply();
            return;
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

        KqpCompileResult = TKqpCompileResult::Make(Uid, status, kqpResult.Issues(), maxReadType, std::move(QueryId));
        KqpCompileResult->CommandTagName = kqpResult.CommandTagName;

        if (status == Ydb::StatusIds::SUCCESS) {
            YQL_ENSURE(kqpResult.PreparingQuery);
            FillCompileResult(std::move(kqpResult.PreparingQuery), queryType, kqpResult.AllowCache);

            auto now = TInstant::Now();
            auto duration = now - StartTime;
            Counters->ReportCompileDurations(DbCounters, duration, CompileCpuTime);

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation successful"
                << ", self: " << ctx.SelfID
                << ", duration: " << duration);
        } else {
            if (kqpResult.PreparingQuery) {
                FillCompileResult(std::move(kqpResult.PreparingQuery), queryType, kqpResult.AllowCache);
            }

            LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation failed"
                << ", self: " << ctx.SelfID
                << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
                << ", issues: " << kqpResult.Issues().ToString());
            Counters->ReportCompileError(DbCounters);
        }

        Reply();
    }

    void HandleTimeout() {
        ALOG_NOTICE(NKikimrServices::KQP_COMPILE_ACTOR, "Compilation timeout"
            << ", self: " << SelfId()
            << ", cluster: " << QueryId.Cluster
            << ", database: " << QueryId.Database
            << ", text: \"" << EscapeC(QueryId.Text) << "\""
            << ", startTime: " << StartTime);

        NYql::TIssue issue(NYql::TPosition(), "Query compilation timed out.");
        return ReplyError(Ydb::StatusIds::TIMEOUT, {issue});
    }

private:
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
    TKqpDbCountersPtr DbCounters;
    TKikimrConfiguration::TPtr Config;
    TQueryServiceConfig QueryServiceConfig;
    TMetadataProviderConfig MetadataProviderConfig;
    TDuration CompilationTimeout;
    TInstant StartTime;
    TDuration CompileCpuTime;
    TInstant RecompileStartTime;
    TActorId TimeoutTimerActorId;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncCompileResult;
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

    NYql::TExprContext* SplitCtx = nullptr;
    NYql::TExprNode::TPtr SplitExpr = nullptr;
};

void ApplyServiceConfig(TKikimrConfiguration& kqpConfig, const TTableServiceConfig& serviceConfig) {
    if (serviceConfig.HasSqlVersion()) {
        kqpConfig._KqpYqlSyntaxVersion = serviceConfig.GetSqlVersion();
    }
    if (serviceConfig.GetQueryLimits().HasResultRowsLimit()) {
        kqpConfig._ResultRowsLimit = serviceConfig.GetQueryLimits().GetResultRowsLimit();
    }

    kqpConfig.EnableKqpDataQuerySourceRead = serviceConfig.GetEnableKqpDataQuerySourceRead();
    kqpConfig.EnableKqpScanQuerySourceRead = serviceConfig.GetEnableKqpScanQuerySourceRead();
    kqpConfig.EnableKqpDataQueryStreamLookup = serviceConfig.GetEnableKqpDataQueryStreamLookup();
    kqpConfig.EnableKqpScanQueryStreamLookup = serviceConfig.GetEnableKqpScanQueryStreamLookup();
    kqpConfig.EnableKqpScanQueryStreamIdxLookupJoin = serviceConfig.GetEnableKqpScanQueryStreamIdxLookupJoin();
    kqpConfig.EnableKqpDataQueryStreamIdxLookupJoin = serviceConfig.GetEnableKqpDataQueryStreamIdxLookupJoin();
    kqpConfig.EnableKqpImmediateEffects = serviceConfig.GetEnableKqpImmediateEffects();
    kqpConfig.EnablePreparedDdl = serviceConfig.GetEnablePreparedDdl();
    kqpConfig.EnableSequences = serviceConfig.GetEnableSequences();
    kqpConfig.EnableColumnsWithDefault = serviceConfig.GetEnableColumnsWithDefault();
    kqpConfig.BindingsMode = RemapBindingsMode(serviceConfig.GetBindingsMode());
    kqpConfig.PredicateExtract20 = serviceConfig.GetPredicateExtract20();
    kqpConfig.IndexAutoChooserMode = serviceConfig.GetIndexAutoChooseMode();
    kqpConfig.EnablePgConstsToParams = serviceConfig.GetEnablePgConstsToParams() && serviceConfig.GetEnableAstCache();
    kqpConfig.ExtractPredicateRangesLimit = serviceConfig.GetExtractPredicateRangesLimit();
    kqpConfig.EnablePerStatementQueryExecution = serviceConfig.GetEnablePerStatementQueryExecution();
    kqpConfig.EnableCreateTableAs = serviceConfig.GetEnableCreateTableAs();
    kqpConfig.EnableOlapSink = serviceConfig.GetEnableOlapSink();
    kqpConfig.EnableOltpSink = serviceConfig.GetEnableOltpSink();
    kqpConfig.BlockChannelsMode = serviceConfig.GetBlockChannelsMode();
    kqpConfig.IdxLookupJoinsPrefixPointLimit = serviceConfig.GetIdxLookupJoinPointsLimit();
    kqpConfig.OldLookupJoinBehaviour = serviceConfig.GetOldLookupJoinBehaviour();

    if (const auto limit = serviceConfig.GetResourceManager().GetMkqlHeavyProgramMemoryLimit()) {
        kqpConfig._KqpYqlCombinerMemoryLimit = std::max(1_GB, limit - (limit >> 2U));
    }
}

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const TTableServiceConfig& tableServiceConfig,
    const TQueryServiceConfig& queryServiceConfig,
    const TMetadataProviderConfig& metadataProviderConfig,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TString& uid, const TKqpQueryId& query, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup, TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings,
    const TMaybe<TString>& applicationName, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    NWilson::TTraceId traceId, TKqpTempTablesState::TConstPtr tempTablesState,
    ECompileActorAction compileAction, TMaybe<TQueryAst> queryAst, bool collectFullDiagnostics,
    bool perStatementResult, NYql::TExprContext* splitCtx, NYql::TExprNode::TPtr splitExpr)
{
    return new TKqpCompileActor(owner, kqpSettings, tableServiceConfig, queryServiceConfig, metadataProviderConfig,
                                moduleResolverState, counters, gUCSettings, applicationName,
                                uid, query, userToken, dbCounters,
                                federatedQuerySetup, userRequestContext,
                                std::move(traceId), std::move(tempTablesState), collectFullDiagnostics,
                                perStatementResult, compileAction, std::move(queryAst),
                                splitCtx, splitExpr);
}

} // namespace NKqp
} // namespace NKikimr
