#include "kqp_impl.h"
#include "kqp_metadata_loader.h"

#include "kqp_worker_common.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/host/kqp_host.h>

#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/string/escape.h>

#include <ydb/core/base/cputime.h>

namespace NKikimr {
namespace NKqp {

static std::atomic<bool> FailForcedNewEngineCompilationStatus = false;

void FailForcedNewEngineCompilationForTests(bool fail) {
    FailForcedNewEngineCompilationStatus = fail;
}

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
        const TTableServiceConfig& serviceConfig, TIntrusivePtr<TModuleResolverState> moduleResolverState,
        TIntrusivePtr<TKqpCounters> counters, const TString& uid, const TKqpQueryId& query, const TString& userToken,
        TKqpDbCountersPtr dbCounters, bool recompileWithNewEngine)
        : Owner(owner)
        , ModuleResolverState(moduleResolverState)
        , Counters(counters)
        , Uid(uid)
        , Query(query)
        , UserToken(userToken)
        , DbCounters(dbCounters)
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , CompilationTimeout(TDuration::MilliSeconds(serviceConfig.GetCompileTimeoutMs()))
        , RecompileWithNewEngine(recompileWithNewEngine)
    {
        Config->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), Query.Cluster, kqpSettings->Settings, false);

        if (!Query.Database.empty()) {
            Config->_KqpTablePathPrefix = Query.Database;
        }

        ApplyServiceConfig(*Config, serviceConfig);

        Config->FreezeDefaults();
    }

    void Bootstrap(const TActorContext& ctx) {
        StartTime = TInstant::Now();

        Counters->ReportCompileStart(DbCounters);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Start compilation"
            << ", self: " << ctx.SelfID
            << ", cluster: " << Query.Cluster
            << ", database: " << Query.Database
            << ", text: \"" << EscapeC(Query.Text) << "\""
            << ", startTime: " << StartTime);

        TimeoutTimerActorId = CreateLongTimer(ctx, CompilationTimeout, new IEventHandle(SelfId(), SelfId(), new TEvents::TEvWakeup()));

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, "");

        TKqpRequestCounters::TPtr counters = new TKqpRequestCounters;
        counters->Counters = Counters;
        counters->DbCounters = DbCounters;
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(AppData(ctx)->Counters);
        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = std::make_shared<TKqpTableMetadataLoader>(TlsActivationContext->ActorSystem(), true);
        Gateway = CreateKikimrIcGateway(Query.Cluster, Query.Database, std::move(loader), ctx.ExecutorThread.ActorSystem,
            ctx.SelfID.NodeId(), counters, MakeMiniKQLCompileServiceID());
        Gateway->SetToken(Query.Cluster, UserToken);

        Config->FeatureFlags = AppData(ctx)->FeatureFlags;

        KqpHost = CreateKqpHost(Gateway, Query.Cluster, Query.Database, Config, ModuleResolverState->ModuleResolver,
            AppData(ctx)->FunctionRegistry, false);

        IKqpHost::TPrepareSettings prepareSettings;
        // prepareSettings.UseNewEngine = use default settings
        prepareSettings.DocumentApiRestricted = Query.Settings.DocumentApiRestricted;

        NCpuTime::TCpuTimer timer(CompileCpuTime);

        AsyncCompileResult = Query.Scan
            ? KqpHost->PrepareScanQuery(Query.Text, true, prepareSettings)
            : KqpHost->PrepareDataQuery(Query.Text, prepareSettings);

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
                CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                UnexpectedEvent("CompileState", ev->GetTypeRewrite(), ctx);
            }
        } catch (const yexception& e) {
            InternalError(e.what(), ctx);
        }
    }

    STFUNC(RecompileNewEngineState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvContinueProcess, HandleRecompile);
                CFunc(TEvents::TSystem::Wakeup, HandleRecompileTimeout);
                default: {
                    LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Internal error in RecompileNewEngineState"
                        << ", self: " << ctx.SelfID
                        << ", unexpected event: " << ev->GetTypeRewrite());

                    Reply(KqpCompileResult, ctx);
                }
            }
        } catch (const yexception& e) {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Internal error in RecompileNewEngineState"
                << ", self: " << ctx.SelfID
                << ", message: " << e.what());

            Reply(KqpCompileResult, ctx);
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
        replayMessage.InsertValue("query_text", EscapeC(Query.Text));
        replayMessage.InsertValue("table_metadata", TString(NJson::WriteJson(tablesMeta, false)));
        replayMessage.InsertValue("created_at", ToString(TlsActivationContext->ActorSystem()->Timestamp().Seconds()));
        replayMessage.InsertValue("query_syntax", ToString(Config->_KqpYqlSyntaxVersion.Get().GetRef()));
        replayMessage.InsertValue("query_database", Query.Database);
        replayMessage.InsertValue("query_cluster", Query.Cluster);
        replayMessage.InsertValue("query_plan", queryPlan);
        TString message(NJson::WriteJson(replayMessage, /*formatOutput*/ false));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_ACTOR, "[" << SelfId() << "]: " << "Built the replay message "
            << message);

        ReplayMessage = std::move(message);
    }

    void Reply(const TKqpCompileResult::TConstPtr& compileResult, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Send response"
            << ", self: " << ctx.SelfID
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
        ctx.Send(Owner, responseEv.Release());

        Counters->ReportCompileFinish(DbCounters);

        Die(ctx);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TIssues& issues, const TActorContext& ctx) {
        Reply(TKqpCompileResult::Make(Uid, std::move(Query), status, issues, ETableReadType::Other), ctx);
    }

    void InternalError(const TString message, const TActorContext &ctx) {
        LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Internal error"
            << ", self: " << ctx.SelfID
            << ", message: " << message);


        NYql::TIssue issue(NYql::TPosition(), "Internal error while compiling query.");
        issue.AddSubIssue(MakeIntrusive<TIssue>(NYql::TPosition(), message));

        ReplyError(Ydb::StatusIds::INTERNAL_ERROR, {issue}, ctx);
    }

    void UnexpectedEvent(const TString& state, ui32 eventType, const TActorContext &ctx) {
        InternalError(TStringBuilder() << "TKqpCompileActor, unexpected event: " << eventType
            << ", at state:" << state, ctx);
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

        auto database = Query.Database;
        if (kqpResult.SqlVersion) {
            Counters->ReportSqlVersion(DbCounters, *kqpResult.SqlVersion);
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            AddMessageToReplayLog(kqpResult.QueryPlan);
        }

        ETableReadType maxReadType = ExtractMostHeavyReadType(kqpResult.QueryPlan);

        KqpCompileResult = TKqpCompileResult::Make(Uid, std::move(Query), status, kqpResult.Issues(), maxReadType);

        if (status == Ydb::StatusIds::SUCCESS) {
            YQL_ENSURE(kqpResult.PreparingQuery);
            if (Config->EnableLlvm.Get()) {
                kqpResult.PreparingQuery->SetEnableLlvm(*Config->EnableLlvm.Get());
            }
            KqpCompileResult->PreparedQuery.reset(kqpResult.PreparingQuery.release());
            KqpCompileResult->QueryTraits = kqpResult.QueryTraits;

            auto now = TInstant::Now();
            auto duration = now - StartTime;
            Counters->ReportCompileDurations(DbCounters, duration, CompileCpuTime);

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation successful"
                << ", self: " << ctx.SelfID
                << ", duration: " << duration
                << ", traits: " << (KqpCompileResult->QueryTraits ? KqpCompileResult->QueryTraits->ToString() : "<none>"));

            if (RecompileWithNewEngine &&
                KqpCompileResult->PreparedQuery->GetVersion() == NKikimrKqp::TPreparedQuery::VERSION_V1)
            {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "About to recompile with NewEngine"
                    << ", self: " << ctx.SelfID);

                RecompileStartTime = now;

                IKqpHost::TPrepareSettings prepareSettings;
                prepareSettings.UseNewEngine = true;
                prepareSettings.DocumentApiRestricted = KqpCompileResult->Query->Settings.DocumentApiRestricted;

                NCpuTime::TCpuTimer timer(CompileCpuTime);
                AsyncCompileResult = KqpHost->PrepareDataQuery(KqpCompileResult->Query->Text, prepareSettings);

                Continue(ctx);
                Become(&TKqpCompileActor::RecompileNewEngineState);

                return;
            }
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation failed"
                << ", self: " << ctx.SelfID
                << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
                << ", issues: " << kqpResult.Issues().ToString());
            Counters->ReportCompileError(DbCounters);
        }

        Reply(KqpCompileResult, ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "Compilation timeout"
            << ", self: " << ctx.SelfID
            << ", cluster: " << Query.Cluster
            << ", database: " << Query.Database
            << ", text: \"" << EscapeC(Query.Text) << "\""
            << ", startTime: " << StartTime);

        NYql::TIssue issue(NYql::TPosition(), "Query compilation timed out.");
        return ReplyError(Ydb::StatusIds::TIMEOUT, {issue}, ctx);
    }

    void HandleRecompile(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_ENSURE(!ev->Get()->QueryId);

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, YqlName, "");

        if (!ev->Get()->Finished) {
            NCpuTime::TCpuTimer timer(CompileCpuTime);
            Continue(ctx);
            return;
        }

        auto kqpResult = std::move(AsyncCompileResult->GetResult());
        auto status = GetYdbStatus(kqpResult);

        if (status == Ydb::StatusIds::SUCCESS && !FailForcedNewEngineCompilationStatus.load(std::memory_order_relaxed)) {
            YQL_ENSURE(kqpResult.PreparingQuery);
            if (Config->EnableLlvm.Get()) {
                kqpResult.PreparingQuery->SetEnableLlvm(*Config->EnableLlvm.Get());
            }
            KqpCompileResult->PreparedQueryNewEngine.reset(kqpResult.PreparingQuery.release());

            auto duration = TInstant::Now() - RecompileStartTime;
            Counters->ReportCompileDurations(DbCounters, duration, CompileCpuTime);

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "ReCompilation successful"
                << ", self: " << ctx.SelfID
                << ", duration: " << duration);
        } else {
            Counters->ReportCompileError(DbCounters);
            Counters->ForceNewEngineCompileErrors->Inc();

            LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "ReCompilation failed"
                << ", self: " << ctx.SelfID
                << ", query: " << KqpCompileResult->Query->Text
                << ", status: " << Ydb::StatusIds_StatusCode_Name(status)
                << ", issues: " << kqpResult.Issues().ToString());
        }

        Reply(KqpCompileResult, ctx);
    }

    void HandleRecompileTimeout(const TActorContext& ctx) {
        LOG_WARN_S(ctx, NKikimrServices::KQP_COMPILE_ACTOR, "ReCompilation timeout"
            << ", self: " << ctx.SelfID
            << ", cluster: " << Query.Cluster
            << ", database: " << Query.Database
            << ", text: \"" << EscapeC(Query.Text) << "\""
            << ", startTime: " << StartTime);

        return Reply(KqpCompileResult, ctx);
    }

private:
    TActorId Owner;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TIntrusivePtr<TKqpCounters> Counters;
    TString Uid;
    TKqpQueryId Query;
    TString UserToken;
    TKqpDbCountersPtr DbCounters;
    TKikimrConfiguration::TPtr Config;
    TDuration CompilationTimeout;
    bool RecompileWithNewEngine;
    TInstant StartTime;
    TDuration CompileCpuTime;
    TInstant RecompileStartTime;
    TActorId TimeoutTimerActorId;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncCompileResult;
    std::shared_ptr<TKqpCompileResult> KqpCompileResult;
    std::optional<TString> ReplayMessage;
};

void ApplyServiceConfig(TKikimrConfiguration& kqpConfig, const TTableServiceConfig& serviceConfig) {
    kqpConfig._KqpAllowUnsafeCommit = serviceConfig.GetAllowUnsafeCommit();
    if (serviceConfig.HasSqlVersion()) {
        kqpConfig._KqpYqlSyntaxVersion = serviceConfig.GetSqlVersion();
    }
    if (serviceConfig.GetQueryLimits().HasResultRowsLimit()) {
        kqpConfig._ResultRowsLimit = serviceConfig.GetQueryLimits().GetResultRowsLimit();
    }
}

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const TTableServiceConfig& serviceConfig, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, const TString& uid, const TKqpQueryId& query, const TString& userToken,
    TKqpDbCountersPtr dbCounters, bool recompileWithNewEngine)
{
    return new TKqpCompileActor(owner, kqpSettings, serviceConfig, moduleResolverState, counters, uid,
        std::move(query), userToken, dbCounters, recompileWithNewEngine);
}

} // namespace NKqp
} // namespace NKikimr
