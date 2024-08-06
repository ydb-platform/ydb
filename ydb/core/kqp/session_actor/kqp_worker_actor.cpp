#include "kqp_worker_common.h"
#include "kqp_query_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/ydb_issue/issue_helpers.h>

#include <ydb/library/yql/utils/actor_log/log.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr {
namespace NKqp {

using namespace NKikimrConfig;
using namespace NThreading;
using namespace NYql;
using namespace NYql::NDq;
using namespace NRuCalc;

namespace {

#define LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)
#define LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)
#define LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)
#define LOG_T(msg) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, LogPrefix() << msg)

using TQueryResult = IKqpHost::TQueryResult;

struct TKqpQueryState {
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    std::unique_ptr<TEvKqp::TEvQueryRequest> RequestEv;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncQueryResult;
    IKqpHost::TQueryResult QueryResult;
    TString Error;
    TInstant StartTime;
    TDuration CpuTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;
    ui32 ReplyFlags = 0;
    bool KeepSession = false;
};

struct TKqpCleanupState {
    bool Final = false;
    TInstant Start;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncResult;
};

EKikimrStatsMode GetStatsMode(const TEvKqp::TEvQueryRequest* queryRequest, EKikimrStatsMode minMode) {
    if (queryRequest->HasCollectStats()) {
        switch (queryRequest->GetCollectStats()) {
            case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
                return EKikimrStatsMode::None;
            case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
                return EKikimrStatsMode::Basic;
            case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
                return EKikimrStatsMode::Full;
            case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
                return EKikimrStatsMode::Profile;
            default:
                return EKikimrStatsMode::None;
        }
    }

    switch (queryRequest->Record.GetRequest().GetStatsMode()) {
        case NYql::NDqProto::DQ_STATS_MODE_BASIC:
            return EKikimrStatsMode::Basic;
        case NYql::NDqProto::DQ_STATS_MODE_PROFILE:
            return EKikimrStatsMode::Full;
        default:
            return std::max(EKikimrStatsMode::None, minMode);
    }
}

class TKqpWorkerActor : public TActorBootstrapped<TKqpWorkerActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_WORKER_ACTOR;
    }

    TKqpWorkerActor(const TActorId& owner, const TString& sessionId, const TKqpSettings::TConstPtr& kqpSettings,
        const TKqpWorkerSettings& workerSettings, std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
        TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
        const TQueryServiceConfig& queryServiceConfig
        )
        : Owner(owner)
        , SessionId(sessionId)
        , Settings(workerSettings)
        , FederatedQuerySetup(federatedQuerySetup)
        , ModuleResolverState(moduleResolverState)
        , Counters(counters)
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , QueryServiceConfig(queryServiceConfig)
        , CreationTime(TInstant::Now())
        , QueryId(0)
        , ShutdownState(std::nullopt)
    {
        Y_ABORT_UNLESS(ModuleResolverState);
        Y_ABORT_UNLESS(ModuleResolverState->ModuleResolver);

        Config->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), Settings.Cluster, kqpSettings->Settings, false);

        if (!Settings.Database.empty()) {
            Config->_KqpTablePathPrefix = Settings.Database;
        }

        ApplyServiceConfig(*Config, Settings.TableService);

        Config->FreezeDefaults();

        RequestCounters = MakeIntrusive<TKqpRequestCounters>();
        RequestCounters->Counters = Counters;
        RequestCounters->DbCounters = Settings.DbCounters;
        RequestCounters->TxProxyMon = MakeIntrusive<NTxProxy::TTxProxyMon>(AppData()->Counters);
    }

    void Bootstrap(const TActorContext&) {
        LOG_D("Worker bootstrapped");
        Counters->ReportWorkerCreated(Settings.DbCounters);
        Become(&TKqpWorkerActor::ReadyState);
    }

    TString LogPrefix() const {
        TStringBuilder result = TStringBuilder() << "SessionId: " << SessionId << ", " << "ActorId: " << SelfId();
        if (Y_LIKELY(QueryState)) {
            result << ", " << QueryState->RequestEv->GetTraceId();
        }

        return result;
    }

    void HandleReady(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        if (CheckRequest(ev->Get()->Record.GetRequest().GetSessionId(), ev->Sender, proxyRequestId, ctx)) {
            LOG_I("Session closed due to explicit close event");
            Counters->ReportWorkerClosedRequest(Settings.DbCounters);
            FinalCleanup(ctx);
        }
    }

    void HandleReady(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandleReady(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        if (!CheckRequest(ev->Get()->GetSessionId(), ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        LOG_D("Received request, proxyRequestId: " << proxyRequestId
            << " rpcCtx: " << (void*)(ev->Get()->GetRequestCtx().get()));

        Y_ABORT_UNLESS(!QueryState);

        MakeNewQueryState();

        auto now = TAppData::TimeProvider->Now();

        QueryState->Sender = ev->Sender;
        QueryState->RequestEv.reset(ev->Release().Release());

        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = std::make_shared<TKqpTableMetadataLoader>(
            Settings.Cluster, TlsActivationContext->ActorSystem(), Config, false, nullptr);
        Gateway = CreateKikimrIcGateway(Settings.Cluster, QueryState->RequestEv->GetType(), Settings.Database, std::move(loader),
            ctx.ExecutorThread.ActorSystem, ctx.SelfID.NodeId(), RequestCounters, QueryServiceConfig);

        Config->FeatureFlags = AppData(ctx)->FeatureFlags;

        KqpHost = CreateKqpHost(Gateway, Settings.Cluster, Settings.Database, Config, ModuleResolverState->ModuleResolver,
            FederatedQuerySetup, QueryState->RequestEv->GetUserToken(), QueryServiceConfig, AppData(ctx)->FunctionRegistry, !Settings.LongSession, false);

        auto& queryRequest = QueryState->RequestEv;
        QueryState->ProxyRequestId = proxyRequestId;
        QueryState->KeepSession = Settings.LongSession || queryRequest->GetKeepSession();
        QueryState->StartTime = now;
        QueryState->ReplyFlags = queryRequest->Record.GetRequest().GetReplyFlags();

        if (GetStatsMode(QueryState->RequestEv.get(), EKikimrStatsMode::None) > EKikimrStatsMode::Basic) {
            QueryState->ReplyFlags |= NKikimrKqp::QUERY_REPLY_FLAG_AST;
        }

        NCpuTime::TCpuTimer timer;

        if (QueryState->RequestEv->GetCancelAfter()) {
            QueryState->QueryDeadlines.CancelAt = now + QueryState->RequestEv->GetCancelAfter();
        }

        auto timeoutMs = GetQueryTimeout(QueryState->RequestEv->GetType(), QueryState->RequestEv->GetOperationTimeout().MilliSeconds(), Settings.TableService, Settings.QueryService);
        QueryState->QueryDeadlines.TimeoutAt = now + timeoutMs;

        auto onError = [this, &ctx] (Ydb::StatusIds::StatusCode status, const TString& message) {
            ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId, status, message);

            if (Settings.LongSession) {
                QueryState.Reset();
            } else {
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                FinalCleanup(ctx);
            }
        };

        auto onBadRequest = [onError] (const TString& message) {
            onError(Ydb::StatusIds::BAD_REQUEST, message);
        };

        if (QueryState->RequestEv->GetDatabase() != Settings.Database) {
            onBadRequest(TStringBuilder() << "Wrong database, expected: " << Settings.Database
                << ", got: " << QueryState->RequestEv->GetDatabase());
            return;
        }

        if (!CheckLegacyYql(queryRequest.get())) {
            onBadRequest(TStringBuilder() << "Legacy YQL requests are restricted in current database, action: "
                << (ui32)QueryState->RequestEv->GetAction() << ", type: " << (ui32)QueryState->RequestEv->GetType());
            return;
        }

        // Most of the queries should be executed directly via session_actor
        switch (QueryState->RequestEv->GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
            case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
            case NKikimrKqp::QUERY_ACTION_PARSE:
                break;

            default:
                onError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() <<
                    "Unexpected query action type in KQP worker: " << (ui32)QueryState->RequestEv->GetAction());
                return;
        }

        if (QueryState->RequestEv->GetAction() == NKikimrKqp::QUERY_ACTION_EXECUTE) {
            switch (QueryState->RequestEv->GetType()) {
                case NKikimrKqp::QUERY_TYPE_SQL_DDL:
                case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
                case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
                    break;

                default:
                    onError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() <<
                        "Unexpected execute query type in KQP worker: " << (ui32)QueryState->RequestEv->GetType());
                    return;
            }
        }

        switch (QueryState->RequestEv->GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            case NKikimrKqp::QUERY_ACTION_PARSE:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
            case NKikimrKqp::QUERY_ACTION_PREPARE:
                if (QueryState->KeepSession && !Settings.LongSession) {
                    onBadRequest("Expected KeepSession=false for non-execute requests");
                    return;
                }
                break;

            default:
                break;
        }

        HandleQueryRequest(timer, ctx);
    }

    void HandleQueryRequest(NCpuTime::TCpuTimer& timer, const TActorContext& ctx) {
        PerformQuery(ctx);
        if (QueryState) {
            QueryState->CpuTime += timer.GetTime();
        }
    }

    void HandlePerformQuery(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ReplyBusy(ev, ctx);
    }

    void HandlePerformQuery(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        LOG_D("Got TEvCloseSessionRequest during PerformQuery state");
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
        QueryState->KeepSession = false;

        QueryState->AsyncQueryResult.Reset();
        QueryCleanup(ctx);
    }

    void HandlePerformQuery(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->QueryId != QueryId) {
            return;
        }

        Y_ABORT_UNLESS(QueryState);
        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, SessionId, QueryState->RequestEv->GetTraceId());

        if (ev->Get()->Finished) {
            QueryState->QueryResult = QueryState->AsyncQueryResult->GetResult();
            QueryState->AsyncQueryResult.Reset();
            QueryCleanup(ctx);
        } else {
            NCpuTime::TCpuTimer timer(QueryState->CpuTime);
            ContinueQueryProcess(ctx);
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;

        if (!CheckRequest(ev->Get()->GetSessionId(), ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        Y_ABORT_UNLESS(CleanupState);
        if (CleanupState->Final) {
            ReplyProcessError(ev->Sender, proxyRequestId, Ydb::StatusIds::BAD_SESSION, "Session is being closed");
        } else {
            auto busyStatus = Settings.TableService.GetUseSessionBusyStatus()
                ? Ydb::StatusIds::SESSION_BUSY
                : Ydb::StatusIds::PRECONDITION_FAILED;

            ReplyProcessError(ev->Sender, proxyRequestId, busyStatus, "Pending previous query completion");
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);

        Y_ABORT_UNLESS(CleanupState);
        if (!CleanupState->Final) {
            Y_ABORT_UNLESS(QueryState);
            QueryState->KeepSession = false;
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->QueryId != QueryId) {
            return;
        }

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, SessionId);

        if (ev->Get()->Finished) {
            Y_ABORT_UNLESS(CleanupState);
            auto result = CleanupState->AsyncResult->GetResult();
            if (!result.Success()) {
                LOG_E("Failed to cleanup: " << result.Issues().ToString());
            }

            EndCleanup(ctx);
        } else {
            ContinueCleanup(ctx);
        }
    }

    STFUNC(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvQueryRequest, HandleReady);
                HFunc(TEvKqp::TEvCloseSessionRequest, HandleReady);
                HFunc(TEvKqp::TEvContinueProcess, HandleReady);
            default:
                UnexpectedEvent("ReadyState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STFUNC(PerformQueryState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvQueryRequest, HandlePerformQuery);
                HFunc(TEvKqp::TEvCloseSessionRequest, HandlePerformQuery);
                HFunc(TEvKqp::TEvContinueProcess, HandlePerformQuery);
            default:
                UnexpectedEvent("PerformQueryState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STFUNC(PerformCleanupState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvQueryRequest, HandlePerformCleanup);
                HFunc(TEvKqp::TEvCloseSessionRequest, HandlePerformCleanup);
                HFunc(TEvKqp::TEvContinueProcess, HandlePerformCleanup);
            default:
                UnexpectedEvent("PerformCleanupState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

private:
    bool CheckLegacyYql(TEvKqp::TEvQueryRequest* queryRequest) {
        switch (queryRequest->GetType()) {
            case NKikimrKqp::QUERY_TYPE_UNDEFINED:
                switch (queryRequest->GetAction()) {
                    case NKikimrKqp::QUERY_ACTION_EXECUTE:
                    case NKikimrKqp::QUERY_ACTION_EXPLAIN:
                    case NKikimrKqp::QUERY_ACTION_PARSE:
                    case NKikimrKqp::QUERY_ACTION_VALIDATE:
                    case NKikimrKqp::QUERY_ACTION_PREPARE:
                    case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                        break;

                    default:
                        return true;
                }
                break;

            default:
                return true;
        }

        LOG_N("Legacy YQL request"
            << ", action: " << (ui32)queryRequest->GetAction()
            << ", type: " << (ui32)queryRequest->GetType()
            << ", query: \"" << queryRequest->GetQuery().substr(0, 1000) << "\"");

        return false;
    }

    void PerformQuery(const TActorContext& ctx) {
        Y_ABORT_UNLESS(QueryState);
        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, SessionId, QueryState->RequestEv->GetTraceId());

        Gateway->SetToken(Settings.Cluster, QueryState->RequestEv->GetUserToken());

        auto onError = [this, &ctx]
            (Ydb::StatusIds::StatusCode status, const TString& message) {
                ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId, status, message);

                if (Settings.LongSession) {
                    QueryState.Reset();
                } else {
                    Counters->ReportWorkerClosedError(Settings.DbCounters);
                    FinalCleanup(ctx);
                }
            };

        auto onBadRequest = [onError] (const TString& message) {
            onError(Ydb::StatusIds::BAD_REQUEST, message);
        };

        YQL_ENSURE(!QueryState->RequestEv->HasTxControl());

        auto action = QueryState->RequestEv->GetAction();
        auto queryType = QueryState->RequestEv->GetType();

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE: {
                if (!ExecuteQuery(QueryState->RequestEv->GetQuery(), QueryState->RequestEv->GetYdbParameters(), queryType, QueryState->RequestEv->GetRequestActorId())) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_EXPLAIN: {
                // Force reply flags
                QueryState->ReplyFlags |= NKikimrKqp::QUERY_REPLY_FLAG_PLAN | NKikimrKqp::QUERY_REPLY_FLAG_AST;
                if (!ExplainQuery(ctx, QueryState->RequestEv->GetQuery(), queryType)) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_PARSE: {
                onBadRequest("Parse mode is not supported yet");
                return;
            }

            case NKikimrKqp::QUERY_ACTION_VALIDATE: {
                if (!ValidateQuery(ctx, QueryState->RequestEv->GetQuery(), queryType)) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            default: {
                onBadRequest(TStringBuilder() << "Unknown query action: " << (ui32)QueryState->RequestEv->GetAction());
                return;
            }
        }

        Become(&TKqpWorkerActor::PerformQueryState);
        ContinueQueryProcess(ctx);
    }

    void Cleanup(const TActorContext &ctx, bool isFinal) {
        Become(&TKqpWorkerActor::PerformCleanupState);

        CleanupState.Reset(MakeHolder<TKqpCleanupState>());
        CleanupState->Final = isFinal;
        CleanupState->Start = TInstant::Now();

        if (isFinal) {
            Counters->ReportQueriesPerWorker(Settings.DbCounters, QueryId);
            MakeNewQueryState();
        }

        if (!CleanupState->AsyncResult) {
            EndCleanup(ctx);
        } else {
            ContinueCleanup(ctx);
        }
    }

    void EndCleanup(const TActorContext &ctx) {
        Y_ABORT_UNLESS(CleanupState);

        if (CleanupState->AsyncResult) {
            auto cleanupTime = TInstant::Now() - CleanupState->Start;
            Counters->ReportWorkerCleanupLatency(Settings.DbCounters, cleanupTime);
        }

        bool isFinal = CleanupState->Final;
        CleanupState.Reset();

        if (isFinal) {
            auto lifeSpan = TInstant::Now() - CreationTime;
            Counters->ReportWorkerFinished(Settings.DbCounters, lifeSpan);

            auto closeEv = MakeHolder<TEvKqp::TEvCloseSessionResponse>();
            closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            closeEv->Record.MutableResponse()->SetSessionId(SessionId);
            closeEv->Record.MutableResponse()->SetClosed(true);
            ctx.Send(Owner, closeEv.Release());

            Die(ctx);
        } else {
            if (ReplyQueryResult(ctx)) {
                Become(&TKqpWorkerActor::ReadyState);
            } else {
                FinalCleanup(ctx);
            }
        }
    }

    void QueryCleanup(const TActorContext &ctx) {
        Cleanup(ctx, false);
    }

    void FinalCleanup(const TActorContext &ctx) {
        Cleanup(ctx, true);
    }

    IKqpHost::TExecScriptSettings ParseExecScriptSettings() {
        IKqpHost::TExecScriptSettings execSettings;
        execSettings.Deadlines = QueryState->QueryDeadlines;
        execSettings.RpcCtx = QueryState->RequestEv->GetRequestCtx();
        auto statsMode = GetStatsMode(QueryState->RequestEv.get(), EKikimrStatsMode::Basic);
        execSettings.StatsMode = statsMode;

        switch (QueryState->RequestEv->GetSyntax()) {
            case Ydb::Query::Syntax::SYNTAX_YQL_V1:
                execSettings.UsePgParser = false;
                execSettings.SyntaxVersion = 1;
                break;
            case Ydb::Query::Syntax::SYNTAX_PG:
                execSettings.UsePgParser = true;
                break;
            default:
                break;
        }

        return execSettings;
    }

    bool ExecuteQuery(const TString& query, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        NKikimrKqp::EQueryType type, const TActorId& requestActorId)
    {
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DDL: {
                IKqpHost::TExecSettings execSettings;
                switch (QueryState->RequestEv->GetSyntax()) {
                    case Ydb::Query::Syntax::SYNTAX_YQL_V1:
                        execSettings.UsePgParser = false;
                        execSettings.SyntaxVersion = 1;
                        break;

                    case Ydb::Query::Syntax::SYNTAX_PG:
                        execSettings.UsePgParser = true;
                        break;
                    default:
                        break;
                }
                execSettings.DocumentApiRestricted = IsDocumentApiRestricted(QueryState->RequestEv->GetRequestType());
                QueryState->AsyncQueryResult = KqpHost->ExecuteSchemeQuery(query, true, execSettings);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT: {
                IKqpHost::TExecScriptSettings execSettings = ParseExecScriptSettings();
                QueryState->AsyncQueryResult = KqpHost->ExecuteYqlScript(query, parameters, execSettings);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                IKqpHost::TExecScriptSettings execSettings = ParseExecScriptSettings();
                QueryState->AsyncQueryResult = KqpHost->StreamExecuteYqlScript(query, parameters,
                    requestActorId, execSettings);
                break;
            }

            default: {
                QueryState->Error = TStringBuilder() << "Unexpected query type: " << (ui32)type;
                return false;
            }
        }

        return true;
    }

    bool ExplainQuery(const TActorContext&, const TString& query, NKikimrKqp::EQueryType type) {
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_AST_DML: {
                bool isSql = (type == NKikimrKqp::QUERY_TYPE_SQL_DML);
                QueryState->AsyncQueryResult = KqpHost->ExplainDataQuery(query, isSql);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN: {
                bool isSql = (type == NKikimrKqp::QUERY_TYPE_SQL_SCAN);
                QueryState->AsyncQueryResult = KqpHost->ExplainScanQuery(query, isSql);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                QueryState->AsyncQueryResult = KqpHost->ExplainYqlScript(query);
                break;
            }

            default:
                QueryState->Error = "Unexpected query type.";
                return false;
        }

        return true;
    }

    bool ValidateQuery(const TActorContext&, const TString& query, NKikimrKqp::EQueryType type) {
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                QueryState->AsyncQueryResult = KqpHost->ValidateYqlScript(query);
                break;
            }

            default:
                QueryState->Error = "Unexpected query type.";
                return false;
        }

        return true;
    }

    bool PrepareQuery(const TActorContext&, const TString& query, NKikimrKqp::EQueryType type, bool sqlAutoCommit) {
        if (sqlAutoCommit) {
            QueryState->Error = "Expected SqlAutoCommit=false for query prepare.";
            return false;
        }

        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML: {
                IKqpHost::TPrepareSettings prepareSettings;
                prepareSettings.DocumentApiRestricted = IsDocumentApiRestricted(QueryState->RequestEv->GetRequestType());
                prepareSettings.IsInternalCall = QueryState->RequestEv->IsInternalCall();
                QueryState->AsyncQueryResult = KqpHost->PrepareDataQuery(query, prepareSettings);
                break;
            }

            default:
                QueryState->Error = "Unexpected query type.";
                return false;
        }

        return true;
    }

    void ContinueQueryProcess(const TActorContext &ctx) {
        Y_ABORT_UNLESS(QueryState);

        TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        TActorId selfId = ctx.SelfID;
        ui32 queryId = QueryId;

        auto callback = [actorSystem, selfId, queryId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(queryId, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        QueryState->AsyncQueryResult->Continue().Apply(callback);
    }

    void ContinueCleanup(const TActorContext &ctx) {
        Y_ABORT_UNLESS(CleanupState);

        TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        TActorId selfId = ctx.SelfID;
        ui32 queryId = QueryId;

        auto callback = [actorSystem, selfId, queryId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(queryId, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        CleanupState->AsyncResult->Continue().Apply(callback);
    }

    bool Reply(THolder<TEvKqp::TEvQueryResponse>&& responseEv, const TActorContext &ctx) {
        Y_ABORT_UNLESS(QueryState);

        auto& record = responseEv->Record.GetRef();
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        bool keepSession = QueryState->KeepSession;
        if (keepSession) {
            response.SetSessionId(SessionId);
        }

        ctx.Send(QueryState->Sender, responseEv.Release(), 0, QueryState->ProxyRequestId);
        LOG_D("Sent query response back to proxy, proxyRequestId: " << QueryState->ProxyRequestId
            << ", proxyId: " << QueryState->Sender.ToString());

        QueryState.Reset();

        if (Settings.LongSession) {
            if (status == Ydb::StatusIds::INTERNAL_ERROR) {
                LOG_D("Worker destroyed due to internal error");
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                return false;
            }
            if (status == Ydb::StatusIds::BAD_SESSION) {
                LOG_D("Worker destroyed due to session error");
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                return false;
            }
        } else {
            if (status != Ydb::StatusIds::SUCCESS) {
                LOG_D("Worker destroyed due to query error");
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                return false;
            }
        }

        if (!keepSession) {
            LOG_D("Worker destroyed due to negative keep session flag");
            Counters->ReportWorkerClosedRequest(Settings.DbCounters);
            return false;
        }

        return true;
    }

    bool ReplyQueryResult(const TActorContext& ctx) {
        Y_ABORT_UNLESS(QueryState);
        auto& queryResult = QueryState->QueryResult;

        auto responseEv = MakeHolder<TEvKqp::TEvQueryResponse>();
        FillResponse(responseEv->Record);

        auto& record = responseEv->Record.GetRef();
        auto status = record.GetYdbStatus();

        auto now = TAppData::TimeProvider->Now();
        auto queryDuration = now - QueryState->StartTime;

        if (status == Ydb::StatusIds::SUCCESS) {
            Counters->ReportQueryLatency(Settings.DbCounters, QueryState->RequestEv->GetAction(), queryDuration);

            auto maxReadType = ExtractMostHeavyReadType(queryResult.QueryPlan);
            if (maxReadType == ETableReadType::FullScan) {
                Counters->ReportQueryWithFullScan(Settings.DbCounters);
            } else if (maxReadType == ETableReadType::Scan) {
                Counters->ReportQueryWithRangeScan(Settings.DbCounters);
            }

            ui32 affectedShardsCount = 0;
            ui64 readBytesCount = 0;
            ui64 readRowsCount = 0;
            for (const auto& exec : queryResult.QueryStats.GetExecutions()) {
                for (const auto& table : exec.GetTables()) {
                    affectedShardsCount = std::max(affectedShardsCount, table.GetAffectedPartitions());
                    readBytesCount += table.GetReadBytes();
                    readRowsCount += table.GetReadRows();
                }
            }

            Counters->ReportQueryAffectedShards(Settings.DbCounters, affectedShardsCount);
            Counters->ReportQueryReadRows(Settings.DbCounters, readRowsCount);
            Counters->ReportQueryReadBytes(Settings.DbCounters, readBytesCount);
            Counters->ReportQueryReadSets(Settings.DbCounters, queryResult.QueryStats.GetReadSetsCount());
            Counters->ReportQueryMaxShardReplySize(Settings.DbCounters, queryResult.QueryStats.GetMaxShardReplySize());
            Counters->ReportQueryMaxShardProgramSize(Settings.DbCounters, queryResult.QueryStats.GetMaxShardProgramSize());
        }

        if (queryResult.SqlVersion) {
            Counters->ReportSqlVersion(Settings.DbCounters, *queryResult.SqlVersion);
        }

        auto& stats = queryResult.QueryStats;
        stats.SetDurationUs(queryDuration.MicroSeconds());
        stats.SetWorkerCpuTimeUs(QueryState->CpuTime.MicroSeconds());

        if (IsExecuteAction(QueryState->RequestEv->GetAction())) {
            auto ru = CalcRequestUnit(stats);
            record.SetConsumedRu(ru);
            CollectSystemViewQueryStats(ctx, &stats, queryDuration, QueryState->RequestEv->GetDatabase(), ru);
            auto requestInfo = TKqpRequestInfo(QueryState->RequestEv->GetTraceId(), SessionId);
            SlowLogQuery(ctx, Config.Get(), requestInfo, queryDuration, record.GetYdbStatus(), QueryState->RequestEv->GetUserToken(),
                QueryState->RequestEv->GetParametersSize(), &record, [this] () { return this->ExtractQueryText(); });
        }

        bool reportStats = (GetStatsMode(QueryState->RequestEv.get(), EKikimrStatsMode::None) != EKikimrStatsMode::None);
        if (reportStats) {
            record.MutableResponse()->MutableQueryStats()->Swap(&stats);
            record.MutableResponse()->SetQueryPlan(queryResult.QueryPlan);
        }

        AddTrailingInfo(responseEv->Record.GetRef());
        return Reply(std::move(responseEv), ctx);
    }

    template<class TEvRecord>
    void AddTrailingInfo(TEvRecord& record) {
        if (ShutdownState) {
            LOG_D("Session is closing, set trailing metadata to request session shutdown");
            record.SetWorkerIsClosing(true);
        }
    }

    bool ReplyProcessError(const TActorId& sender, ui64 proxyRequestId,
        Ydb::StatusIds::StatusCode ydbStatus, const TString& message)
    {
        LOG_W(message);
        auto response = std::make_unique<TEvKqp::TEvQueryResponse>();
        response->Record.GetRef().SetYdbStatus(ydbStatus);
        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message);
        NYql::TIssues issues;
        issues.AddIssue(issue);
        NYql::IssuesToMessage(issues, response->Record.GetRef().MutableResponse()->MutableQueryIssues());
        AddTrailingInfo(response->Record.GetRef());
        return Send(sender, response.release(), 0, proxyRequestId);
    }

    bool CheckRequest(const TString& eventSessionId, const TActorId& sender, ui64 proxyRequestId, const TActorContext&)
    {
        if (eventSessionId != SessionId) {
            TString error = TStringBuilder() << "Invalid session, got: " << eventSessionId
                << " expected: " << SessionId << ", request ignored";
            ReplyProcessError(sender, proxyRequestId, Ydb::StatusIds::BAD_SESSION, error);
            return false;
        }

        return true;
    }

    void ReplyBusy(TEvKqp::TEvQueryRequest::TPtr& ev, const TActorContext& ctx) {
        ui64 proxyRequestId = ev->Cookie;
        if (!CheckRequest(ev->Get()->Record.GetRequest().GetSessionId(), ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        auto busyStatus = Settings.TableService.GetUseSessionBusyStatus()
            ? Ydb::StatusIds::SESSION_BUSY
            : Ydb::StatusIds::PRECONDITION_FAILED;

        ReplyProcessError(ev->Sender, proxyRequestId, busyStatus,
            "Pending previous query completion");
    }

    void CollectSystemViewQueryStats(const TActorContext& ctx,
        const NKqpProto::TKqpStatsQuery* stats, TDuration queryDuration,
        const TString& database, ui64 requestUnits)
    {
        auto type = QueryState->RequestEv->GetType();
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                TString text = ExtractQueryText();
                if (IsQueryAllowedToLog(text)) {
                    auto userSID = QueryState->RequestEv->GetUserToken()->GetUserSID();
                    CollectQueryStats(ctx, stats, queryDuration, text,
                        userSID, QueryState->RequestEv->GetParametersSize(), database, type, requestUnits);
                }
                break;
            }
            default:
                break;
        }
    }

    TString ExtractQueryText() const {
        return QueryState->RequestEv->GetQuery();
    }

    void FillResponse(TEvKqp::TProtoArenaHolder<NKikimrKqp::TEvQueryResponse>& record) {
        Y_ABORT_UNLESS(QueryState);

        auto& queryResult = QueryState->QueryResult;
        auto arena = queryResult.ProtobufArenaPtr;
        if (arena) {
            record.Realloc(arena);
        }
        auto& ev = record.GetRef();

        bool replyResults = IsExecuteAction(QueryState->RequestEv->GetAction());
        bool replyPlan = true;
        bool replyAst = true;

        // TODO: Handle in KQP to avoid generation of redundant data
        replyResults = replyResults && (QueryState->ReplyFlags & NKikimrKqp::QUERY_REPLY_FLAG_RESULTS);
        replyPlan = replyPlan && (QueryState->ReplyFlags & NKikimrKqp::QUERY_REPLY_FLAG_PLAN);
        replyAst = replyAst && (QueryState->ReplyFlags & NKikimrKqp::QUERY_REPLY_FLAG_AST);

        auto ydbStatus = GetYdbStatus(queryResult);
        auto issues = queryResult.Issues();

        ev.SetYdbStatus(ydbStatus);
        AddQueryIssues(*ev.MutableResponse(), issues);

        if (replyResults) {
            auto resp = ev.MutableResponse();
            for (auto& result : queryResult.Results) {
                // If we have result it must be allocated on protobuf arena
                Y_ASSERT(result->GetArena());
                Y_ASSERT(resp->GetArena() == result->GetArena());
                resp->AddResults()->Swap(result);
            }
        } else {
            auto resp = ev.MutableResponse();
            for (auto& result : queryResult.ResultSetsMeta) {
                auto ydbRes = resp->AddYdbResults();
                ydbRes->mutable_columns()->Swap(result.mutable_columns());
            }
        }

        /*
         * TODO:
         * For Scan/Data plan will be set later on rpc_* level from stats and execution profiles, so
         * QUERY_REPLY_FLAG_PLAN doesn't matter much. However it's may be a good idea to move FillQueryStats here,
         * but for that we need to set QueryStats for scan query earlier in pipeline (now ExecutionProfiles are
         * handled in rpc_stream_execute_scan_query). Other option is to remove REPLY_FLAGs at all.
         */
        if (replyAst && !queryResult.QueryAst.empty()) {
            ev.MutableResponse()->SetQueryAst(queryResult.QueryAst);
        }
        if (replyPlan && !queryResult.QueryPlan.empty()) {
            ev.MutableResponse()->SetQueryPlan(queryResult.QueryPlan);
        }

        if (ydbStatus != Ydb::StatusIds::SUCCESS) {
            return;
        }

        bool replyQueryId = false;
        bool replyQueryParameters = false;
        switch (QueryState->RequestEv->GetAction()) {
        case NKikimrKqp::QUERY_ACTION_PREPARE:
            replyQueryId = true;
            replyQueryParameters = true;
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            replyQueryParameters = replyQueryId = QueryState->RequestEv->GetQueryKeepInCache();
            break;

        case NKikimrKqp::QUERY_ACTION_PARSE:
        case NKikimrKqp::QUERY_ACTION_VALIDATE:
            replyQueryParameters = true;
            break;

        default:
            break;
        }

        if (replyQueryParameters) {
            YQL_ENSURE(queryResult.PreparedQuery);
            ev.MutableResponse()->MutableQueryParameters()->CopyFrom(queryResult.PreparedQuery->GetParameters());
        }

        YQL_ENSURE(!replyQueryId);
    }

    void MakeNewQueryState() {
        ++QueryId;
        QueryState.Reset(MakeHolder<TKqpQueryState>());
    }

    IKikimrQueryExecutor::TExecuteSettings CreateRollbackSettings() {
        YQL_ENSURE(QueryState);

        IKikimrQueryExecutor::TExecuteSettings settings;
        settings.RollbackTx = true;
        settings.Deadlines.TimeoutAt = TInstant::Now() + TDuration::Minutes(1);

        return settings;
    }

    static TKikimrQueryLimits GetQueryLimits(const TKqpWorkerSettings& settings) {
        const auto& queryLimitsProto = settings.TableService.GetQueryLimits();
        const auto& phaseLimitsProto = queryLimitsProto.GetPhaseLimits();

        TKikimrQueryLimits queryLimits;
        auto& phaseLimits = queryLimits.PhaseLimits;
        phaseLimits.AffectedShardsLimit = phaseLimitsProto.GetAffectedShardsLimit();
        phaseLimits.ReadsetCountLimit = phaseLimitsProto.GetReadsetCountLimit();
        phaseLimits.ComputeNodeMemoryLimitBytes = phaseLimitsProto.GetComputeNodeMemoryLimitBytes();
        phaseLimits.TotalReadSizeLimitBytes = phaseLimitsProto.GetTotalReadSizeLimitBytes();

        return queryLimits;
    }

private:
    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        TString message = TStringBuilder() << "TKqpWorkerActor in state "
            << state << " received unexpected event "
            << ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite());

        InternalError(message);
    }

    void InternalError(const TString& message) {
        LOG_E("Internal error, message: " << message);
        if (QueryState) {
            ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId, Ydb::StatusIds::INTERNAL_ERROR, message);
        }

        auto lifeSpan = TInstant::Now() - CreationTime;
        Counters->ReportWorkerFinished(Settings.DbCounters, lifeSpan);

        auto closeEv = MakeHolder<TEvKqp::TEvCloseSessionResponse>();
        closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        closeEv->Record.MutableResponse()->SetSessionId(SessionId);
        closeEv->Record.MutableResponse()->SetClosed(true);
        Send(Owner, closeEv.Release());

        PassAway();
    }

private:
    TActorId Owner;
    TString SessionId;
    TKqpWorkerSettings Settings;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpRequestCounters> RequestCounters;
    TKikimrConfiguration::TPtr Config;
    TQueryServiceConfig QueryServiceConfig;
    TInstant CreationTime;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    ui32 QueryId;
    THolder<TKqpQueryState> QueryState;
    THolder<TKqpCleanupState> CleanupState;
    std::optional<TSessionShutdownState> ShutdownState;
};

} // namespace

IActor* CreateKqpWorkerActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TQueryServiceConfig& queryServiceConfig
    )
{
    return new TKqpWorkerActor(owner, sessionId, kqpSettings, workerSettings, federatedQuerySetup,
                               moduleResolverState, counters, queryServiceConfig);
}

} // namespace NKqp
} // namespace NKikimr
