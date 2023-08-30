#include "kqp_compile_service.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>

#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/json/json_writer.h>
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

namespace {
NSQLTranslation::EBindingsMode RemapBindingsMode(NKikimrConfig::TTableServiceConfig::EBindingsMode mode) {
    switch (mode) {
    case NKikimrConfig::TTableServiceConfig::BM_ENABLED:
        return NSQLTranslation::EBindingsMode::ENABLED;
    case NKikimrConfig::TTableServiceConfig::BM_DISABLED:
        return NSQLTranslation::EBindingsMode::DISABLED;
    case NKikimrConfig::TTableServiceConfig::BM_DROP_WITH_WARNING:
        return NSQLTranslation::EBindingsMode::DROP_WITH_WARNING;
    case NKikimrConfig::TTableServiceConfig::BM_DROP:
        return NSQLTranslation::EBindingsMode::DROP;
    default:
        return NSQLTranslation::EBindingsMode::ENABLED;
    }
}
}

class TKqpCompileActor : public TActorBootstrapped<TKqpCompileActor> {
public:
    using TBase = TActorBootstrapped<TKqpCompileActor>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_ACTOR;
    }

    TKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
        const TTableServiceConfig& serviceConfig, NYql::IHTTPGateway::TPtr httpGateway,
        TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const TString& uid, const TKqpQueryId& queryId,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        TKqpDbCountersPtr dbCounters, NWilson::TTraceId traceId, TKqpTempTablesState::TConstPtr tempTablesState)
        : Owner(owner)
        , HttpGateway(std::move(httpGateway))
        , ModuleResolverState(moduleResolverState)
        , Counters(counters)
        , CredentialsFactory(std::move(credentialsFactory))
        , Uid(uid)
        , QueryId(queryId)
        , QueryRef(QueryId.Text, QueryId.QueryParameterTypes)
        , UserToken(userToken)
        , DbCounters(dbCounters)
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , CompilationTimeout(TDuration::MilliSeconds(serviceConfig.GetCompileTimeoutMs()))
        , CompileActorSpan(TWilsonKqp::CompileActor, std::move(traceId), "CompileActor")
        , TempTablesState(std::move(tempTablesState))
    {
        Config->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), QueryId.Cluster, kqpSettings->Settings, false);

        if (!QueryId.Database.empty()) {
            Config->_KqpTablePathPrefix = QueryId.Database;
        }

        ApplyServiceConfig(*Config, serviceConfig);

        Config->FreezeDefaults();
    }

    void Bootstrap(const TActorContext& ctx) {
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

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, "");

        TKqpRequestCounters::TPtr counters = new TKqpRequestCounters;
        counters->Counters = Counters;
        counters->DbCounters = DbCounters;
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(AppData(ctx)->Counters);
        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader =
            std::make_shared<TKqpTableMetadataLoader>(
                TlsActivationContext->ActorSystem(), Config, true, TempTablesState);
        Gateway = CreateKikimrIcGateway(QueryId.Cluster, QueryId.Database, std::move(loader),
            ctx.ExecutorThread.ActorSystem, ctx.SelfID.NodeId(), counters);
        Gateway->SetToken(QueryId.Cluster, UserToken);

        Config->FeatureFlags = AppData(ctx)->FeatureFlags;

        KqpHost = CreateKqpHost(Gateway, QueryId.Cluster, QueryId.Database, Config, ModuleResolverState->ModuleResolver,
            HttpGateway, AppData(ctx)->FunctionRegistry, false, false, CredentialsFactory, std::move(TempTablesState));

        IKqpHost::TPrepareSettings prepareSettings;
        prepareSettings.DocumentApiRestricted = QueryId.Settings.DocumentApiRestricted;
        prepareSettings.IsInternalCall = QueryId.Settings.IsInternalCall;

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
                AsyncCompileResult = KqpHost->PrepareGenericQuery(QueryRef, prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
                AsyncCompileResult = KqpHost->PrepareGenericQuery(QueryRef, prepareSettings);
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

    void AddMessageToReplayLog(const TString& queryPlan) {
        NJson::TJsonValue replayMessage(NJson::JSON_MAP);
        NJson::TJsonValue tablesMeta(NJson::JSON_ARRAY);
        for(auto data: Gateway->GetCollectedSchemeData()) {
            tablesMeta.AppendValue(Base64Encode(std::move(data)));
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
        replayMessage.InsertValue("table_metadata", TString(NJson::WriteJson(tablesMeta, false)));
        replayMessage.InsertValue("created_at", ToString(TlsActivationContext->ActorSystem()->Timestamp().Seconds()));
        replayMessage.InsertValue("query_syntax", ToString(Config->_KqpYqlSyntaxVersion.Get().GetRef()));
        replayMessage.InsertValue("query_database", QueryId.Database);
        replayMessage.InsertValue("query_cluster", QueryId.Cluster);
        replayMessage.InsertValue("query_plan", queryPlan);
        replayMessage.InsertValue("query_type", ToString(QueryId.Settings.QueryType));
        TString message(NJson::WriteJson(replayMessage, /*formatOutput*/ false));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_ACTOR, "[" << SelfId() << "]: "
            << "Built the replay message " << message);

        ReplayMessage = std::move(message);
    }

    void Reply(const TKqpCompileResult::TConstPtr& compileResult) {
        ALOG_DEBUG(NKikimrServices::KQP_COMPILE_ACTOR, "Send response"
            << ", self: " << SelfId()
            << ", owner: " << Owner
            << ", status: " << compileResult->Status
            << ", issues: " << compileResult->Issues.ToString()
            << ", uid: " << compileResult->Uid);

        auto responseEv = MakeHolder<TEvKqp::TEvCompileResponse>(compileResult);

        responseEv->ReplayMessage = std::move(ReplayMessage);
        ReplayMessage = std::nullopt;
        auto& stats = responseEv->Stats;
        stats.SetFromCache(false);
        stats.SetDurationUs((TInstant::Now() - StartTime).MicroSeconds());
        stats.SetCpuTimeUs(CompileCpuTime.MicroSeconds());
        Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        if (CompileActorSpan) {
            CompileActorSpan.End();
        }

        PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TIssues& issues) {
        Reply(TKqpCompileResult::Make(Uid, std::move(QueryId), status, issues, ETableReadType::Other));
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

    void Handle(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_ENSURE(!ev->Get()->QueryId);

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, "");

        if (!ev->Get()->Finished) {
            NCpuTime::TCpuTimer timer(CompileCpuTime);
            Continue(ctx);
            return;
        }

        auto kqpResult = std::move(AsyncCompileResult->GetResult());
        auto status = GetYdbStatus(kqpResult);

        auto database = QueryId.Database;
        if (kqpResult.SqlVersion) {
            Counters->ReportSqlVersion(DbCounters, *kqpResult.SqlVersion);
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            AddMessageToReplayLog(kqpResult.QueryPlan);
        }

        ETableReadType maxReadType = ExtractMostHeavyReadType(kqpResult.QueryPlan);

        auto queryType = QueryId.Settings.QueryType;

        KqpCompileResult = TKqpCompileResult::Make(Uid, std::move(QueryId), status, kqpResult.Issues(), maxReadType);

        if (status == Ydb::StatusIds::SUCCESS) {
            YQL_ENSURE(kqpResult.PreparingQuery);
            {
                auto preparedQueryHolder = std::make_shared<TPreparedQueryHolder>(
                    kqpResult.PreparingQuery.release(), AppData()->FunctionRegistry);
                preparedQueryHolder->MutableLlvmSettings().Fill(Config, queryType);
                KqpCompileResult->PreparedQuery = preparedQueryHolder;
                KqpCompileResult->AllowCache = CanCacheQuery(KqpCompileResult->PreparedQuery->GetPhysicalQuery());
            }

            auto now = TInstant::Now();
            auto duration = now - StartTime;
            Counters->ReportCompileDurations(DbCounters, duration, CompileCpuTime);

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation successful"
                << ", self: " << ctx.SelfID
                << ", duration: " << duration);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation failed"
                << ", self: " << ctx.SelfID
                << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
                << ", issues: " << kqpResult.Issues().ToString());
            Counters->ReportCompileError(DbCounters);
        }

        Reply(KqpCompileResult);
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
    NYql::IHTTPGateway::TPtr HttpGateway;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TIntrusivePtr<TKqpCounters> Counters;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    TString Uid;
    TKqpQueryId QueryId;
    TKqpQueryRef QueryRef;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TKqpDbCountersPtr DbCounters;
    TKikimrConfiguration::TPtr Config;
    TDuration CompilationTimeout;
    TInstant StartTime;
    TDuration CompileCpuTime;
    TInstant RecompileStartTime;
    TActorId TimeoutTimerActorId;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncCompileResult;
    std::shared_ptr<TKqpCompileResult> KqpCompileResult;
    std::optional<TString> ReplayMessage;

    NWilson::TSpan CompileActorSpan;

    TKqpTempTablesState::TConstPtr TempTablesState;
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
    kqpConfig.EnablePredicateExtractForDataQuery = serviceConfig.GetEnablePredicateExtractForDataQueries();
    kqpConfig.EnablePredicateExtractForScanQuery = serviceConfig.GetEnablePredicateExtractForScanQueries();
    kqpConfig.EnableSequentialReads = serviceConfig.GetEnableSequentialReads();
    kqpConfig.EnableKqpImmediateEffects = serviceConfig.GetEnableKqpImmediateEffects();
    kqpConfig.EnablePreparedDdl = serviceConfig.GetEnablePreparedDdl();
    kqpConfig.EnableSequences = serviceConfig.GetEnableSequences();
    kqpConfig.BindingsMode = RemapBindingsMode(serviceConfig.GetBindingsMode());
    kqpConfig.PredicateExtract20 = serviceConfig.GetPredicateExtract20();
}

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const TTableServiceConfig& serviceConfig, NYql::IHTTPGateway::TPtr httpGateway,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& uid, const TKqpQueryId& query, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    TKqpDbCountersPtr dbCounters, NWilson::TTraceId traceId, TKqpTempTablesState::TConstPtr tempTablesState)
{
    return new TKqpCompileActor(owner, kqpSettings, serviceConfig, std::move(httpGateway), moduleResolverState,
        counters, std::move(credentialsFactory),
        uid, query, userToken, dbCounters, std::move(traceId), std::move(tempTablesState));
}

} // namespace NKqp
} // namespace NKikimr
