#include "kqp_impl.h"
#include "kqp_metadata_loader.h"
#include <ydb/core/kqp/common/kqp_ru_calc.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/escape.h>

namespace NKikimr {
namespace NKqp {

using namespace NKikimrConfig;
using namespace NThreading;
using namespace NYql;
using namespace NYql::NDq;
using namespace NRuCalc;

static std::atomic<bool> FailForcedNewEngineExecution = false;
void FailForcedNewEngineExecutionForTests(bool fail) {
    FailForcedNewEngineExecution = fail;
}

namespace {

constexpr std::string_view DocumentApiRequestType = "_document_api_request"sv;

using TQueryResult = IKqpHost::TQueryResult;

struct TKqpQueryState {
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    NKikimrKqp::TQueryRequest Request;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncQueryResult;
    IKqpHost::TQueryResult QueryResult;
    TString Error;
    TString TraceId;
    TString RequestType;
    ui64 ParametersSize = 0;
    TString UserToken;
    TActorId RequestActorId;
    TInstant StartTime;
    TDuration CpuTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;
    TString TxId;
    TKqpCompileResult::TConstPtr QueryCompileResult;
    NKqpProto::TKqpStatsCompile CompileStats;
    ui32 ReplyFlags = 0;
    bool KeepSession = false;
    bool InteractiveTx = true;

    bool OldEngineFallback = false;
    bool NewEngineCompatibleQuery = false;

    TKqpForceNewEngineState ForceNewEngineState;
    std::optional<TQueryTraits> QueryTraits;

    TMaybe<NKikimrKqp::TRlPath> RlPath;
};


struct TSessionShutdownState {
    TSessionShutdownState(ui32 softTimeout, ui32 hardTimeout)
        : HardTimeout(hardTimeout)
        , SoftTimeout(softTimeout)
    {}

    ui32 Step = 0;
    ui32 HardTimeout;
    ui32 SoftTimeout;

    void MoveToNextState() {
        ++Step;
    }

    ui32 GetNextTickMs() const {
        if (Step == 0) {
            return std::min(HardTimeout, SoftTimeout);
        } else if (Step == 1) {
            return std::max(HardTimeout, SoftTimeout) - std::min(HardTimeout, SoftTimeout) + 1;
        } else {
            return 50;
        }
    }

    bool SoftTimeoutReached() const {
        return Step == 1;
    }

    bool HardTimeoutReached() const {
        return Step == 2;
    }
};

struct TKqpCleanupState {
    bool Final = false;
    TInstant Start;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncResult;
};

enum ETableReadType {
    Other = 0,
    Scan = 1,
    FullScan = 2,
};

EKikimrStatsMode GetStatsMode(const NKikimrKqp::TQueryRequest& queryRequest, EKikimrStatsMode minMode) {
    if (queryRequest.GetProfile()) {
        // TODO: Deprecate, StatsMode is the new way to enable stats.
        return EKikimrStatsMode::Profile;
    }

    switch (queryRequest.GetStatsMode()) {
        case NYql::NDqProto::DQ_STATS_MODE_BASIC:
            return EKikimrStatsMode::Basic;
        case NYql::NDqProto::DQ_STATS_MODE_PROFILE:
            return EKikimrStatsMode::Profile;
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
        const TKqpWorkerSettings& workerSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
        TIntrusivePtr<TKqpCounters> counters)
        : Owner(owner)
        , SessionId(sessionId)
        , Settings(workerSettings)
        , ModuleResolverState(moduleResolverState)
        , Counters(counters)
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , CreationTime(TInstant::Now())
        , QueryId(0)
        , IdleTimerId(0)
        , ShutdownState(std::nullopt)
    {
        Y_VERIFY(ModuleResolverState);
        Y_VERIFY(ModuleResolverState->ModuleResolver);

        Config->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), Settings.Cluster, kqpSettings->Settings, false);

        if (!Settings.Database.empty()) {
            Config->_KqpTablePathPrefix = Settings.Database;
        }

        ApplyServiceConfig(*Config, Settings.Service);

        Config->FreezeDefaults();

        RequestCounters = MakeIntrusive<TKqpRequestCounters>();
        RequestCounters->Counters = Counters;
        RequestCounters->DbCounters = Settings.DbCounters;
        RequestCounters->TxProxyMon = MakeIntrusive<NTxProxy::TTxProxyMon>(AppData()->Counters);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, "Worker bootstrapped, workerId: " << ctx.SelfID);
        Counters->ReportWorkerCreated(Settings.DbCounters);

        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = std::make_shared<TKqpTableMetadataLoader>(TlsActivationContext->ActorSystem(), false);
        Gateway = CreateKikimrIcGateway(Settings.Cluster, Settings.Database, std::move(loader),
            ctx.ExecutorThread.ActorSystem, ctx.SelfID.NodeId(), RequestCounters, MakeMiniKQLCompileServiceID());

        Config->FeatureFlags = AppData(ctx)->FeatureFlags;

        KqpHost = CreateKqpHost(Gateway, Settings.Cluster, Settings.Database, Config, ModuleResolverState->ModuleResolver,
            AppData(ctx)->FunctionRegistry, !Settings.LongSession);

        Become(&TKqpWorkerActor::ReadyState);
        StartIdleTimer(ctx);
    }

    void HandleReady(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());
        if (CheckRequest(requestInfo, ev->Sender, proxyRequestId, ctx)) {
            LOG_INFO_S(ctx, NKikimrServices::KQP_WORKER, requestInfo << "Session closed due to explicit close event");
            Counters->ReportWorkerClosedRequest(Settings.DbCounters);
            FinalCleanup(ctx);
        }
    }

    void HandleReady(TEvKqp::TEvPingSessionRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());
        if (!CheckRequest(requestInfo, ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        if (ShutdownState) {
            ReplyProcessError(ev->Sender, proxyRequestId, requestInfo, Ydb::StatusIds::BAD_SESSION,
                "Session is under shutdown.", ctx);
            FinalCleanup(ctx);
            return;
        }

        StartIdleTimer(ctx);

        ReplyPingStatus(ev->Sender, proxyRequestId, true, ctx);
    }

    void HandleReady(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandleReady(TEvKqp::TEvCompileResponse::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);

        LOG_ERROR_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo("", SessionId)
            << "Unexpected compile response while in Ready state.");
    }

    void HandleReady(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());

        if (!CheckRequest(requestInfo, ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        if (ShutdownState && ShutdownState->SoftTimeoutReached()) {
            // we reached the soft timeout, so at this point we don't allow to accept new
            // queries for session.
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo("", SessionId)
                << "System shutdown requested: soft timeout reached, no queries can be accepted. Closing session.");

            ReplyProcessError(ev->Sender, proxyRequestId, requestInfo,
                Ydb::StatusIds::BAD_SESSION, "Session is under shutdown.", ctx);
            FinalCleanup(ctx);
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, requestInfo << "Received request, proxyRequestId: "
            << proxyRequestId);

        Y_VERIFY(!QueryState);
        MakeNewQueryState();
        QueryState->Request.Swap(event.MutableRequest());
        auto& queryRequest = QueryState->Request;

        if (!queryRequest.HasAction()) {
            queryRequest.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        }

        auto now = TAppData::TimeProvider->Now();

        QueryState->ParametersSize = queryRequest.GetParameters().ByteSize();
        QueryState->Sender = ev->Sender;
        QueryState->ProxyRequestId = proxyRequestId;
        QueryState->KeepSession = Settings.LongSession || queryRequest.GetKeepSession();
        QueryState->TraceId = requestInfo.GetTraceId();
        QueryState->RequestType = event.GetRequestType();
        QueryState->StartTime = now;
        QueryState->ReplyFlags = queryRequest.GetReplyFlags();
        QueryState->UserToken = event.GetUserToken();
        QueryState->RequestActorId = ActorIdFromProto(event.GetRequestActorId());

        if (GetStatsMode(queryRequest, EKikimrStatsMode::None) > EKikimrStatsMode::Basic) {
            QueryState->ReplyFlags |= NKikimrKqp::QUERY_REPLY_FLAG_AST;
        }

        if (event.HasRlPath()) {
            QueryState->RlPath = event.GetRlPath();
        }

        NCpuTime::TCpuTimer timer;

        if (queryRequest.GetCancelAfterMs()) {
            QueryState->QueryDeadlines.CancelAt = now + TDuration::MilliSeconds(queryRequest.GetCancelAfterMs());
        }

        auto timeoutMs = GetQueryTimeout(queryRequest.GetType(), queryRequest.GetTimeoutMs(), Settings.Service);
        QueryState->QueryDeadlines.TimeoutAt = now + timeoutMs;

        auto onError = [this, &ctx, &requestInfo] (Ydb::StatusIds::StatusCode status, const TString& message) {
            ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId, requestInfo, status, message, ctx);

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

        if (queryRequest.GetDatabase() != Settings.Database) {
            onBadRequest(TStringBuilder() << "Wrong database, expected:" << Settings.Database
                << ", got: " << queryRequest.GetDatabase());
            return;
        }

        if (!CheckLegacyYql(requestInfo, queryRequest, ctx)) {
            onBadRequest(TStringBuilder() << "Legacy YQL requests are restricted in current database, action: "
                << (ui32)queryRequest.GetAction() << ", type: " << (ui32)queryRequest.GetType());
            return;
        }

        switch (queryRequest.GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
            case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
            case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
                break;
            default:
                if (!queryRequest.HasType()) {
                    onBadRequest("Query type not specified");
                    return;
                }
        }

        switch (queryRequest.GetAction()) {
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

        HandleQueryRequest(timer, false, ctx);
    }

    void HandleQueryRequest(NCpuTime::TCpuTimer& timer, bool fallbackToOldEngine, const TActorContext& ctx) {
        auto& queryRequest = QueryState->Request;

        if (fallbackToOldEngine) {
            QueryState->ForceNewEngineState.ForcedNewEngine = false;
            QueryState->OldEngineFallback = true;
        }

        auto replyError = [this, &ctx] (NYql::EYqlIssueCode status, const TString& info) {
            QueryState->AsyncQueryResult = MakeKikimrResultHolder(NCommon::ResultFromError<TQueryResult>(
                YqlIssue(TPosition(), status, info)));

            ContinueQueryProcess(ctx);
            Become(&TKqpWorkerActor::PerformQueryState);
        };

        if (queryRequest.HasTxControl()) {
            const auto& txControl = queryRequest.GetTxControl();

            switch (txControl.tx_selector_case()) {
                case Ydb::Table::TransactionControl::kTxId: {
                    QueryState->TxId = txControl.tx_id();

                    auto txInfo = KqpHost->GetTransactionInfo(QueryState->TxId);
                    if (!txInfo) {
                        replyError(TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND, TStringBuilder() << "Transaction not found: " << QueryState->TxId);
                        return;
                    }

                    YQL_ENSURE(!QueryState->OldEngineFallback);
                    QueryState->ForceNewEngineState = txInfo->ForceNewEngineState;
                    break;
                }

                case Ydb::Table::TransactionControl::kBeginTx: {
                    if (txControl.commit_tx()) {
                        QueryState->InteractiveTx = false;
                    }
                    if (QueryState->OldEngineFallback) {
                        YQL_ENSURE(!QueryState->InteractiveTx);
                    }
                    break;
                }

                case Ydb::Table::TransactionControl::TX_SELECTOR_NOT_SET: {
                    replyError(TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "wrong TxControl: tx_selector must be set");
                    return;
                }
            }
        } else {
            // some kind of internal query? or verify here?
        }

        StopIdleTimer(ctx);

        if (CompileQuery(ctx)) {
            if (QueryState) {
                QueryState->CpuTime += timer.GetTime();
            }
            return;
        }

        PerformQuery(ctx);
        if (QueryState) {
            QueryState->CpuTime += timer.GetTime();
        }
    }

    void HandleContinueShutdown(TEvKqp::TEvContinueShutdown::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        CheckContinueShutdown(ctx);
    }

    void HandleInitiateShutdown(TEvKqp::TEvInitiateSessionShutdown::TPtr &ev, const TActorContext &ctx) {
        if (!ShutdownState) {
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_WORKER, "Started session shutdown " << TKqpRequestInfo("", SessionId));
            auto softTimeout = ev->Get()->SoftTimeoutMs;
            auto hardTimeout = ev->Get()->HardTimeoutMs;
            ShutdownState = TSessionShutdownState(softTimeout, hardTimeout);
            ScheduleNextShutdownTick(ctx);
        }
    }

    void HandleReady(TEvKqp::TEvIdleTimeout::TPtr &ev, const TActorContext &ctx) {
        auto timerId = ev->Get()->TimerId;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, "Received TEvIdleTimeout in ready state, timer id: "
            << timerId << ", sender: " << ev->Sender);

        if (timerId == IdleTimerId) {
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo("", SessionId)
                << "Worker idle timeout, worker destroyed");
            Counters->ReportWorkerClosedIdle(Settings.DbCounters);
            FinalCleanup(ctx);
        }
    }

    void HandleCompileQuery(TEvKqp::TEvCompileResponse::TPtr &ev, const TActorContext &ctx) {
        auto compileResult = ev->Get()->CompileResult;

        Y_VERIFY(compileResult);
        Y_VERIFY(QueryState);

        if (compileResult->Status != Ydb::StatusIds::SUCCESS) {
            if (ReplyQueryCompileError(compileResult, ctx)) {
                StartIdleTimer(ctx);
                Become(&TKqpWorkerActor::ReadyState);
            } else {
                FinalCleanup(ctx);
            }

            return;
        }

        QueryState->QueryTraits = compileResult->QueryTraits;

        if (!QueryState->ForceNewEngineState.ForcedNewEngine.has_value()) {
            // first query in tx
            QueryState->ForceNewEngineState.ForceNewEnginePercent = ev->Get()->ForceNewEnginePercent;
            QueryState->ForceNewEngineState.ForceNewEngineLevel = ev->Get()->ForceNewEngineLevel;
        }

        auto& queryRequest = QueryState->Request;

        QueryState->CompileStats.Swap(&ev->Get()->Stats);

        if (queryRequest.GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE) {
            if (ReplyPrepareResult(compileResult, ctx)) {
                StartIdleTimer(ctx);
                Become(&TKqpWorkerActor::ReadyState);
            } else {
                FinalCleanup(ctx);
            }

            return;
        }

        QueryState->QueryCompileResult = compileResult;

        switch (queryRequest.GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                break;

            default:
                Y_VERIFY_S(false, "Unexpected action on successful compile result: "
                    << NKikimrKqp::EQueryAction_Name(queryRequest.GetAction()));
                break;
        }

        NCpuTime::TCpuTimer timer;
        PerformQuery(ctx);

        // PerformQuery can reset QueryState
        if (QueryState) {
            QueryState->CpuTime += timer.GetTime();
        }
    }

    void HandleCompileQuery(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ReplyBusy(ev, ctx);
    }

    void HandleCompileQuery(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);

        Y_VERIFY(QueryState);
        ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId,
            TKqpRequestInfo(QueryState->TraceId, SessionId), Ydb::StatusIds::BAD_SESSION,
                "Session is being closed", ctx);

        Counters->ReportWorkerClosedError(Settings.DbCounters);
        FinalCleanup(ctx);
    }

    void HandleCompileQuery(TEvKqp::TEvPingSessionRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        ReplyPingStatus(ev->Sender, proxyRequestId, false, ctx);
    }

    void HandleCompileQuery(TEvKqp::TEvIdleTimeout::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandlePerformQuery(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ReplyBusy(ev, ctx);
    }

    void HandlePerformQuery(TEvKqp::TEvCompileResponse::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);

        LOG_ERROR_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo("", SessionId)
            << "Unexpected compile response while in PerformQuery state.");
    }

    void HandlePerformQuery(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
        QueryState->KeepSession = false;
    }

    void HandlePerformQuery(TEvKqp::TEvPingSessionRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        ReplyPingStatus(ev->Sender, proxyRequestId, false, ctx);
    }

    void HandlePerformQuery(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->QueryId != QueryId) {
            return;
        }

        Y_VERIFY(QueryState);
        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, SessionId, QueryState->TraceId);

        if (ev->Get()->Finished) {
            QueryState->QueryResult = QueryState->AsyncQueryResult->GetResult();
            QueryState->AsyncQueryResult.Reset();

            if (!QueryState->OldEngineFallback) {
                auto& x = QueryState->ForceNewEngineState;
                if (x.ForcedNewEngine && *x.ForcedNewEngine) {
                    auto status = GetYdbStatus(QueryState->QueryResult);
                    bool failForTests = FailForcedNewEngineExecution.load(std::memory_order_relaxed);

                    if (status != Ydb::StatusIds::SUCCESS || failForTests) {
                        if (x.ForceNewEngineLevel == 0 || x.ForceNewEngineLevel == 1) {
                            bool shouldFallback = status != Ydb::StatusIds::CANCELLED
                                               && status != Ydb::StatusIds::ABORTED
                                               && status != Ydb::StatusIds::OVERLOADED
                                               && status != Ydb::StatusIds::PRECONDITION_FAILED
                                               && status != Ydb::StatusIds::UNAVAILABLE
                                               && status != Ydb::StatusIds::UNDETERMINED;
                            if (shouldFallback) {
                                QueryState->ForceNewEngineState = {};
                                QueryState->NewEngineCompatibleQuery = false;

                                GetServiceCounters(AppData()->Counters, "kqp")->GetCounter("Requests/OldEngineFallback", true)->Inc();

                                LOG_ERROR_S(ctx, NKikimrServices::KQP_WORKER, "OldEngine fallback request"
                                    << ", satus: " << Ydb::StatusIds::StatusCode_Name(status)
                                    << ", issues: " << QueryState->QueryResult.Issues().ToString());

                                NCpuTime::TCpuTimer timer;
                                HandleQueryRequest(timer, true, ctx);
                                return;
                            }
                        }

                        if (failForTests) {
                            QueryState->QueryResult.SetStatus(NYql::TIssuesIds::DEFAULT_ERROR);
                            QueryState->QueryResult.AddIssue(YqlIssue(TPosition(), TIssuesIds::DEFAULT_ERROR, "Failed for test."));
                            GetServiceCounters(AppData()->Counters, "kqp")->GetCounter("Requests/ForceNewEngineExecError", true)->Inc();
                        }
                    }
                }
            }

            QueryCleanup(ctx);
        } else {
            NCpuTime::TCpuTimer timer(QueryState->CpuTime);
            ContinueQueryProcess(ctx);
        }
    }

    void HandlePerformQuery(TEvKqp::TEvIdleTimeout::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandlePerformCleanup(TEvKqp::TEvQueryRequest::TPtr &ev, const TActorContext &ctx) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());

        if (!CheckRequest(requestInfo, ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        Y_VERIFY(CleanupState);
        if (CleanupState->Final) {
            ReplyProcessError(ev->Sender, proxyRequestId, requestInfo, Ydb::StatusIds::BAD_SESSION,
                "Session is being closed", ctx);
        } else {
            auto busyStatus = Settings.Service.GetUseSessionBusyStatus()
                ? Ydb::StatusIds::SESSION_BUSY
                : Ydb::StatusIds::PRECONDITION_FAILED;

            ReplyProcessError(ev->Sender, proxyRequestId, requestInfo,
                busyStatus, "Pending previous query completion", ctx);
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvCompileResponse::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);

        Y_VERIFY(CleanupState);
        if (!CleanupState->Final) {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo("", SessionId)
                << "Unexpected compile response while in PerformCleanup state.");
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvCloseSessionRequest::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);

        Y_VERIFY(CleanupState);
        if (!CleanupState->Final) {
            Y_VERIFY(QueryState);
            QueryState->KeepSession = false;
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvPingSessionRequest::TPtr &ev, const TActorContext &ctx) {
        Y_VERIFY(CleanupState);

        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        TKqpRequestInfo requestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());

        if (CleanupState->Final) {
            ReplyProcessError(ev->Sender, proxyRequestId, requestInfo,
                Ydb::StatusIds::BAD_SESSION, "Session is being closed", ctx);
        } else {
            ReplyPingStatus(ev->Sender, proxyRequestId, false, ctx);
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvContinueProcess::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->QueryId != QueryId) {
            return;
        }

        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, SessionId);

        if (ev->Get()->Finished) {
            Y_VERIFY(CleanupState);
            auto result = CleanupState->AsyncResult->GetResult();
            if (!result.Success()) {
                LOG_ERROR_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo("", SessionId)
                    << "Failed to cleanup: " << result.Issues().ToString());
            }

            EndCleanup(ctx);
        } else {
            ContinueCleanup(ctx);
        }
    }

    void HandlePerformCleanup(TEvKqp::TEvIdleTimeout::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    STFUNC(ReadyState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqp::TEvQueryRequest, HandleReady);
            HFunc(TEvKqp::TEvCompileResponse, HandleReady);
            HFunc(TEvKqp::TEvCloseSessionRequest, HandleReady);
            HFunc(TEvKqp::TEvPingSessionRequest, HandleReady);
            HFunc(TEvKqp::TEvContinueProcess, HandleReady);
            HFunc(TEvKqp::TEvIdleTimeout, HandleReady);
            HFunc(TEvKqp::TEvInitiateSessionShutdown, HandleInitiateShutdown);
            HFunc(TEvKqp::TEvContinueShutdown, HandleContinueShutdown);
        default:
            Y_FAIL("TKqpWorkerActor, ReadyState: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    STFUNC(CompileQueryState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqp::TEvQueryRequest, HandleCompileQuery);
            HFunc(TEvKqp::TEvCompileResponse, HandleCompileQuery);
            HFunc(TEvKqp::TEvCloseSessionRequest, HandleCompileQuery);
            HFunc(TEvKqp::TEvPingSessionRequest, HandleCompileQuery);
            HFunc(TEvKqp::TEvIdleTimeout, HandleCompileQuery);
            HFunc(TEvKqp::TEvInitiateSessionShutdown, HandleInitiateShutdown);
            HFunc(TEvKqp::TEvContinueShutdown, HandleContinueShutdown);
        default:
            Y_FAIL("TKqpWorkerActor, CompileQueryState: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    STFUNC(PerformQueryState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqp::TEvQueryRequest, HandlePerformQuery);
            HFunc(TEvKqp::TEvCompileResponse, HandlePerformQuery);
            HFunc(TEvKqp::TEvCloseSessionRequest, HandlePerformQuery);
            HFunc(TEvKqp::TEvPingSessionRequest, HandlePerformQuery);
            HFunc(TEvKqp::TEvContinueProcess, HandlePerformQuery);
            HFunc(TEvKqp::TEvIdleTimeout, HandlePerformQuery);
            HFunc(TEvKqp::TEvInitiateSessionShutdown, HandleInitiateShutdown);
            HFunc(TEvKqp::TEvContinueShutdown, HandleContinueShutdown);
        default:
            Y_FAIL("TKqpWorkerActor, PerformQueryState: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    STFUNC(PerformCleanupState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqp::TEvQueryRequest, HandlePerformCleanup);
            HFunc(TEvKqp::TEvCompileResponse, HandlePerformCleanup);
            HFunc(TEvKqp::TEvCloseSessionRequest, HandlePerformCleanup);
            HFunc(TEvKqp::TEvPingSessionRequest, HandlePerformCleanup);
            HFunc(TEvKqp::TEvContinueProcess, HandlePerformCleanup);
            HFunc(TEvKqp::TEvIdleTimeout, HandlePerformCleanup);
            HFunc(TEvKqp::TEvInitiateSessionShutdown, HandleInitiateShutdown);
            HFunc(TEvKqp::TEvContinueShutdown, HandleContinueShutdown);
        default:
            Y_FAIL("TKqpWorkerActor, PerformCleanupState: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    bool CheckLegacyYql(const TKqpRequestInfo& requestInfo, const NKikimrKqp::TQueryRequest& queryRequest,
        const TActorContext& ctx)
    {
        switch (queryRequest.GetType()) {
            case NKikimrKqp::QUERY_TYPE_UNDEFINED:
                switch (queryRequest.GetAction()) {
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

        LOG_NOTICE_S(ctx, NKikimrServices::KQP_WORKER, requestInfo << "Legacy YQL request"
            << ", action: " << (ui32)queryRequest.GetAction()
            << ", type: " << (ui32)queryRequest.GetType()
            << ", query: \"" << queryRequest.GetQuery().substr(0, 1000) << "\"");

        return false;
    }

    IKqpHost::TBeginTxResult BeginTransaction(const Ydb::Table::TransactionSettings& settings) {
        NKikimrKqp::EIsolationLevel isolation = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;
        bool readonly = true;

        switch (settings.tx_mode_case()) {
            case Ydb::Table::TransactionSettings::kSerializableReadWrite:
                isolation = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                readonly = false;
                break;

            case Ydb::Table::TransactionSettings::kOnlineReadOnly:
                isolation = settings.online_read_only().allow_inconsistent_reads()
                    ? NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED
                    : NKikimrKqp::ISOLATION_LEVEL_READ_COMMITTED;
                readonly = true;
                break;

            case Ydb::Table::TransactionSettings::kStaleReadOnly:
                isolation = NKikimrKqp::ISOLATION_LEVEL_READ_STALE;
                readonly = true;
                break;

            default:
                break;
        };

        return KqpHost->BeginTransaction(isolation, readonly);
    }

    bool CompileQuery(const TActorContext& ctx) {
        if (!Settings.LongSession) {
            return false;
        }

        Y_VERIFY(QueryState);
        auto& queryRequest = QueryState->Request;

        switch (queryRequest.GetType()) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
                break;

            default:
                return false;
        }

        TMaybe<TKqpQueryId> query;
        TMaybe<TString> uid;

        bool keepInCache = false;
        switch (queryRequest.GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, queryRequest.GetQuery());
                keepInCache = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            case NKikimrKqp::QUERY_ACTION_PREPARE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, queryRequest.GetQuery());
                keepInCache = true;
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                uid = queryRequest.GetPreparedQuery();
                keepInCache = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            default:
                return false;
        }

        if (query) {
            query->Settings.DocumentApiRestricted = IsDocumentApiRestricted(QueryState->RequestType);
        }

        auto compileDeadline = QueryState->QueryDeadlines.TimeoutAt;
        if (QueryState->QueryDeadlines.CancelAt) {
            compileDeadline = Min(compileDeadline, QueryState->QueryDeadlines.CancelAt);
        }

        auto compileRequestActor = CreateKqpCompileRequestActor(ctx.SelfID, QueryState->UserToken, uid,
            std::move(query), keepInCache, compileDeadline, Settings.DbCounters);
        ctx.ExecutorThread.RegisterActor(compileRequestActor);

        Become(&TKqpWorkerActor::CompileQueryState);
        return true;
    }

    void PerformQuery(const TActorContext& ctx) {
        Y_VERIFY(QueryState);
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        TYqlLogScope logScope(ctx, NKikimrServices::KQP_YQL, SessionId, QueryState->TraceId);

        Gateway->SetToken(Settings.Cluster, QueryState->UserToken);
        auto& queryRequest = QueryState->Request;

        IKqpHost::TBeginTxResult beginTxResult;
        auto onError = [this, &ctx, &requestInfo, &beginTxResult]
            (Ydb::StatusIds::StatusCode status, const TString& message) {
                ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId, requestInfo, status, message, ctx);

                if (beginTxResult.Success()) {
                    auto deleteResult = KqpHost->DeleteTransaction(beginTxResult.TxId);
                    if (!deleteResult.Success()) {
                        LOG_ERROR_S(ctx, NKikimrServices::KQP_WORKER, "Failed to delete empty tx: "
                            << beginTxResult.TxId << ", error: " << deleteResult.Issues().ToString());
                    }
                }

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

        bool commit = false;

        if (queryRequest.HasTxControl()) {
            const auto& txControl = queryRequest.GetTxControl();
            commit = txControl.commit_tx();

            switch (txControl.tx_selector_case()) {
                case Ydb::Table::TransactionControl::kTxId: {
                    Y_VERIFY_DEBUG(!QueryState->TxId.empty());
                    break;
                }

                case Ydb::Table::TransactionControl::kBeginTx: {
                    beginTxResult = BeginTransaction(txControl.begin_tx());

                    Counters->ReportBeginTransaction(Settings.DbCounters,
                        beginTxResult.EvictedTx,
                        beginTxResult.CurrentActiveTx,
                        beginTxResult.CurrentAbortedTx);

                    if (!beginTxResult.Success()) {
                        QueryState->AsyncQueryResult = MakeKikimrResultHolder(
                            NCommon::ResultFromErrors<TQueryResult>(beginTxResult.Issues()));
                        ContinueQueryProcess(ctx);
                        Become(&TKqpWorkerActor::PerformQueryState);
                        return;
                    }

                    Counters->ReportTxCreated(Settings.DbCounters);

                    QueryState->TxId = beginTxResult.TxId;
                    if (commit) {
                        QueryState->InteractiveTx = false;
                    }

                    break;
                }

                default:
                    onBadRequest("Unexpected transaction selector value");
                    return;
            }
        }

        auto action = queryRequest.GetAction();
        auto queryType = queryRequest.GetType();
        if (QueryState->QueryCompileResult) {
            if (action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
                Y_VERIFY(queryType == NKikimrKqp::QUERY_TYPE_SQL_DML);
                queryType = NKikimrKqp::QUERY_TYPE_PREPARED_DML;
                action = NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED;
            }
        }

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE: {
                if (!ExecuteQuery(queryRequest, queryType, commit, QueryState->RequestActorId)) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_EXPLAIN: {
                // Force reply flags
                QueryState->ReplyFlags |= NKikimrKqp::QUERY_REPLY_FLAG_PLAN | NKikimrKqp::QUERY_REPLY_FLAG_AST;
                if (!ExplainQuery(ctx, queryRequest.GetQuery(), queryType)) {
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
                if (!ValidateQuery(ctx, queryRequest.GetQuery(), queryType)) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_PREPARE: {
                if (!PrepareQuery(ctx, queryRequest.GetQuery(), queryType, commit)) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED: {
                // NOTE: For compatibility with old clients, remove once not used.
                const TString& query = queryRequest.HasPreparedQuery()
                     ? queryRequest.GetPreparedQuery()
                     : queryRequest.GetQuery();

                TPreparedQueryConstPtr preparedQuery;

                if (QueryState->QueryCompileResult) {
                    Y_VERIFY(queryType == NKikimrKqp::QUERY_TYPE_PREPARED_DML);

                    bool newEngineCompatibleTx = !QueryState->OldEngineFallback
                        && QueryState->ForceNewEngineState.ForceNewEnginePercent > 0;

                    bool forcedOldEngine = false;

                    if (newEngineCompatibleTx &&
                        QueryState->ForceNewEngineState.ForcedNewEngine.has_value() &&
                        QueryState->ForceNewEngineState.ForcedNewEngine.value() == false)
                    {
                        // newEngineCompatibleTx = false;
                        forcedOldEngine = true;
                    }

                    QueryState->NewEngineCompatibleQuery = (bool) QueryState->QueryCompileResult->PreparedQueryNewEngine
                        && newEngineCompatibleTx;

                    // select engine according to deferred effects
                    auto effectsEngine = KqpHost->GetTransactionInfo(QueryState->TxId)->TxEngine;

                    if (newEngineCompatibleTx) {
                        if (QueryState->ForceNewEngineState.ForceNewEngineLevel == 0) {
                            if (QueryState->InteractiveTx || !QueryState->QueryTraits) {
                                newEngineCompatibleTx = false;
                                QueryState->NewEngineCompatibleQuery = false;
                            } else {
                                const auto& traits = QueryState->QueryTraits.value();
                                if (!traits.ReadOnly || traits.WithJoin || traits.WithSqlIn || traits.WithIndex) {
                                    newEngineCompatibleTx = false;
                                    QueryState->NewEngineCompatibleQuery = false;
                                }
                            }
                        } else if (QueryState->ForceNewEngineState.ForceNewEngineLevel == 1) {
                            if (QueryState->InteractiveTx || !QueryState->QueryTraits) {
                                newEngineCompatibleTx = false;
                                QueryState->NewEngineCompatibleQuery = false;
                            } else {
                                const auto& traits = QueryState->QueryTraits.value();
                                if (!traits.ReadOnly) {
                                    newEngineCompatibleTx = false;
                                    QueryState->NewEngineCompatibleQuery = false;
                                }
                            }
                        } else if (QueryState->ForceNewEngineState.ForceNewEngineLevel == 2) {
                            if (!QueryState->QueryTraits.has_value() || !QueryState->QueryTraits->ReadOnly) {
                                QueryState->NewEngineCompatibleQuery = false;
                                // but Tx is still NE Compatible, i.e. RO-queries can be executed with NewEngine
                            }

                            if (commit) {
                                if (effectsEngine) {
                                    if (*effectsEngine == TKqpTransactionInfo::EEngine::NewEngine) {
                                        QueryState->NewEngineCompatibleQuery = true;
                                    } else {
                                        QueryState->NewEngineCompatibleQuery = false;
                                    }
                                } else {
                                    // Y_VERIFY(false);
                                }
                            }
                        } else if (QueryState->ForceNewEngineState.ForceNewEngineLevel == 3) {
                            newEngineCompatibleTx = true;
                            QueryState->NewEngineCompatibleQuery = true;
                        } else {
                            YQL_ENSURE(false);
                        }
                    }

                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, "-- NE Compatible: "
                        << " tx: " << newEngineCompatibleTx
                        << ", query: " << QueryState->NewEngineCompatibleQuery
                        << ", interactive: " << QueryState->InteractiveTx
                        << ", preparedNewEngine: " << (bool) QueryState->QueryCompileResult->PreparedQueryNewEngine
                        << ", effects: " << (effectsEngine ? (int) *effectsEngine : -1)
                        << ", forcedNewEngine: " << (QueryState->ForceNewEngineState.ForcedNewEngine
                            ? ToString(*QueryState->ForceNewEngineState.ForcedNewEngine)
                            : "<none>")
                        << ", traits: " << (QueryState->QueryCompileResult->QueryTraits
                            ? QueryState->QueryCompileResult->QueryTraits->ToString()
                            : "<none>")
                        << ", commit: " << commit
                        << ", text: " << queryRequest.GetQuery());

                    if (newEngineCompatibleTx && !forcedOldEngine) {
                        if (QueryState->NewEngineCompatibleQuery) {
                            if (QueryState->ForceNewEngineState.ForcedNewEngine && *QueryState->ForceNewEngineState.ForcedNewEngine) {
                                preparedQuery = QueryState->QueryCompileResult->PreparedQueryNewEngine;
                                LOG_INFO_S(ctx, NKikimrServices::KQP_WORKER, "Force NewEngine query execution (as part of tx)");
                            } else if (!effectsEngine && QueryState->ForceNewEngineState.ForceNewEnginePercent >= RandomNumber((ui32) 100)) {
                                preparedQuery = QueryState->QueryCompileResult->PreparedQueryNewEngine;
                                QueryState->ForceNewEngineState.ForcedNewEngine = true;

                                KqpHost->ForceTxNewEngine(
                                    QueryState->TxId,
                                    QueryState->ForceNewEngineState.ForceNewEnginePercent,
                                    QueryState->ForceNewEngineState.ForceNewEngineLevel
                                );
                                LOG_INFO_S(ctx, NKikimrServices::KQP_WORKER, "Force NewEngine query execution (new tx)");
                            } else {
                                QueryState->ForceNewEngineState.ForcedNewEngine = false;
                                KqpHost->ForceTxOldEngine(QueryState->TxId);
                                preparedQuery = QueryState->QueryCompileResult->PreparedQuery;
                                LOG_INFO_S(ctx, NKikimrServices::KQP_WORKER, "Force OldEngine query execution (new tx)");
                            }
                        } else {
                            if (!QueryState->ForceNewEngineState.ForcedNewEngine.has_value()) {
                                QueryState->ForceNewEngineState.ForcedNewEngine = false;
                                KqpHost->ForceTxOldEngine(QueryState->TxId);
                            }

                            preparedQuery = QueryState->QueryCompileResult->PreparedQuery;
                        }
                    } else {
                        preparedQuery = QueryState->QueryCompileResult->PreparedQuery;
                    }
                } else if (Settings.LongSession) {
                    onError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Prepared query not found: " << query);
                    return;
                } else {
                    NKikimrKqp::TPreparedQuery tmp;
                    if (!tmp.ParseFromString(query)) {
                        onBadRequest("Failed to parse prepared query.");
                        return;
                    }
                    preparedQuery = std::make_shared<const NKikimrKqp::TPreparedQuery>(std::move(tmp));
                }

                YQL_ENSURE(preparedQuery);

                QueryState->Request.SetQuery(preparedQuery->GetText());

                if (!ExecutePreparedQuery(preparedQuery, queryType, std::move(*QueryState->Request.MutableParameters()),
                    commit, GetStatsMode(queryRequest, EKikimrStatsMode::Basic)))
                {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_BEGIN_TX: {
                TQueryResult result;
                result.SetSuccess();
                QueryState->AsyncQueryResult = MakeKikimrResultHolder(std::move(result));
                ContinueQueryProcess(ctx);
                Become(&TKqpWorkerActor::PerformQueryState);
                return;
            }

            case NKikimrKqp::QUERY_ACTION_COMMIT_TX: {
                if (!CommitTx(commit, GetStatsMode(queryRequest, EKikimrStatsMode::Basic))) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX: {
                if (!RollbackTx(commit)) {
                    onBadRequest(QueryState->Error);
                    return;
                }
                break;
            }

            default: {
                onBadRequest("Unknown query action");
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
            StopIdleTimer(ctx);
            Counters->ReportQueriesPerWorker(Settings.DbCounters, QueryId);

            MakeNewQueryState();
        }

        if (Settings.LongSession) {
            if (isFinal) {
                auto abortedCount = KqpHost->AbortAll();
                Counters->ReportTxAborted(Settings.DbCounters, abortedCount);
            }
            CleanupState->AsyncResult = KqpHost->RollbackAborted();
        } else {
            if (isFinal && QueryState->TxId) {
                CleanupState->AsyncResult = KqpHost->RollbackTransaction(QueryState->TxId, CreateRollbackSettings());
            }
        }

        if (!CleanupState->AsyncResult) {
            EndCleanup(ctx);
        } else {
            ContinueCleanup(ctx);
        }
    }

    void EndCleanup(const TActorContext &ctx) {
        Y_VERIFY(CleanupState);

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
                StartIdleTimer(ctx);
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

    bool ExecuteQuery(NKikimrKqp::TQueryRequest& queryRequest, NKikimrKqp::EQueryType type, bool commit,
        const TActorId& requestActorId)
    {
        const auto& query = queryRequest.GetQuery();
        auto* parameters = queryRequest.MutableParameters();
        auto statsMode = GetStatsMode(queryRequest, EKikimrStatsMode::Basic);

        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_AST_DML: {
                bool isSql = (type == NKikimrKqp::QUERY_TYPE_SQL_DML);

                NYql::IKikimrQueryExecutor::TExecuteSettings execSettings;
                execSettings.CommitTx = commit;
                execSettings.StatsMode = statsMode;
                execSettings.Deadlines = QueryState->QueryDeadlines;
                execSettings.Limits = GetQueryLimits(Settings);
                execSettings.StrictDml = false;
                execSettings.UseNewEngine = UseNewEngine();
                execSettings.DocumentApiRestricted = IsDocumentApiRestricted(QueryState->RequestType);

                QueryState->AsyncQueryResult = KqpHost->ExecuteDataQuery(QueryState->TxId, query, isSql,
                    std::move(*parameters), execSettings);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_DDL: {
                QueryState->AsyncQueryResult = KqpHost->ExecuteSchemeQuery(query, true);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT: {
                IKqpHost::TExecScriptSettings execSettings;
                execSettings.Deadlines = QueryState->QueryDeadlines;
                execSettings.StatsMode = statsMode;
                QueryState->AsyncQueryResult = KqpHost->ExecuteYqlScript(query, std::move(*parameters), execSettings);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                IKqpHost::TExecScriptSettings execSettings;
                execSettings.Deadlines = QueryState->QueryDeadlines;
                execSettings.StatsMode = statsMode;
                QueryState->AsyncQueryResult = KqpHost->StreamExecuteYqlScript(query, std::move(*parameters),
                    requestActorId, execSettings);
                break;
            }

            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN: {
                bool isSql = (type == NKikimrKqp::QUERY_TYPE_SQL_SCAN);

                NYql::IKikimrQueryExecutor::TExecuteSettings execSettings;
                execSettings.StatsMode = statsMode;
                execSettings.Deadlines = QueryState->QueryDeadlines;
                execSettings.Limits = GetQueryLimits(Settings);
                execSettings.RlPath = QueryState->RlPath;

                QueryState->AsyncQueryResult = KqpHost->ExecuteScanQuery(query, isSql, std::move(*parameters),
                    requestActorId, execSettings);
                break;
            }

            default: {
                QueryState->Error = "Unexpected query type.";
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
                // prepareSettings.UseNewEngine = use default settings
                prepareSettings.DocumentApiRestricted = IsDocumentApiRestricted(QueryState->RequestType);
                QueryState->AsyncQueryResult = KqpHost->PrepareDataQuery(query, prepareSettings);
                break;
            }

            default:
                QueryState->Error = "Unexpected query type.";
                return false;
        }

        return true;
    }

    bool ExecutePreparedQuery(TPreparedQueryConstPtr& query, NKikimrKqp::EQueryType type,
        NKikimrMiniKQL::TParams&& parameters, bool commit, EKikimrStatsMode statsMode)
    {
        if (type == NKikimrKqp::QUERY_TYPE_PREPARED_DML) {
            NYql::IKikimrQueryExecutor::TExecuteSettings execSettings;
            execSettings.CommitTx = commit;
            execSettings.StatsMode = statsMode;
            execSettings.Deadlines = QueryState->QueryDeadlines;
            execSettings.Limits = GetQueryLimits(Settings);

            QueryState->AsyncQueryResult = KqpHost->ExecuteDataQuery(QueryState->TxId, query, std::move(parameters),
                execSettings);
            return true;
        } else {
            QueryState->Error = "Unexpected query type.";
            return false;
        }
    }

    bool CommitTx(bool commit, EKikimrStatsMode statsMode) {
        if (!commit) {
            QueryState->Error = "Commit should be true for commit transaction request.";
            return false;
        }
        if (QueryState->TxId.empty()) {
            QueryState->Error = "Empty tx_id for commit transaction request.";
            return false;
        }

        IKikimrQueryExecutor::TExecuteSettings execSettings;
        execSettings.CommitTx = true;
        execSettings.StatsMode = statsMode;
        execSettings.Deadlines = QueryState->QueryDeadlines;
        execSettings.Limits = GetQueryLimits(Settings);
        execSettings.UseNewEngine = UseNewEngine();

        QueryState->AsyncQueryResult = KqpHost->CommitTransaction(QueryState->TxId, execSettings);
        return true;
    }

    bool RollbackTx(bool commit) {
        if (commit) {
            QueryState->Error = "Commit should be false for rollback transaction request.";
            return false;
        }
        if (QueryState->TxId.empty()) {
            QueryState->Error = "Empty tx_id for rollback transaction request.";
            return false;
        }

        QueryState->AsyncQueryResult = KqpHost->RollbackTransaction(QueryState->TxId, CreateRollbackSettings());
        return true;
    }

    void ContinueQueryProcess(const TActorContext &ctx) {
        Y_VERIFY(QueryState);

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
        Y_VERIFY(CleanupState);

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
        Y_VERIFY(QueryState);

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& record = responseEv->Record.GetRef();
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        bool keepSession = QueryState->KeepSession;
        if (keepSession) {
            response.SetSessionId(SessionId);
        }

        ctx.Send(QueryState->Sender, responseEv.Release(), 0, QueryState->ProxyRequestId);
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, requestInfo
            << "Sent query response back to proxy, proxyRequestId: " << QueryState->ProxyRequestId
            << ", proxyId: " << QueryState->Sender.ToString());

        QueryState.Reset();

        if (Settings.LongSession) {
            if (status == Ydb::StatusIds::INTERNAL_ERROR) {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, requestInfo
                    << "Worker destroyed due to internal error");
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                return false;
            }
            if (status == Ydb::StatusIds::BAD_SESSION) {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, requestInfo
                    << "Worker destroyed due to session error");
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                return false;
            }
        } else {
            if (status != Ydb::StatusIds::SUCCESS) {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, requestInfo
                    << "Worker destroyed due to query error");
                Counters->ReportWorkerClosedError(Settings.DbCounters);
                return false;
            }
        }

        if (!keepSession) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, requestInfo
                << "Worker destroyed due to negative keep session flag");
            Counters->ReportWorkerClosedRequest(Settings.DbCounters);
            return false;
        }

        return true;
    }

    bool ReplyPrepareResult(const TKqpCompileResult::TConstPtr& compileResult, const TActorContext &ctx) {
        auto responseEv = MakeHolder<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);

        auto ru = CpuTimeToUnit(TDuration::MicroSeconds(QueryState->CompileStats.GetCpuTimeUs()));
        responseEv->Record.GetRef().SetConsumedRu(ru);

        return Reply(std::move(responseEv), ctx);
    }

    bool ReplyQueryCompileError(const TKqpCompileResult::TConstPtr& compileResult, const TActorContext &ctx) {
        auto responseEv = MakeHolder<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);

        KqpHost->AbortTransaction(QueryState->TxId);
        FillTxInfo(responseEv->Record);

        responseEv->Record.GetRef().SetConsumedRu(1);

        return Reply(std::move(responseEv), ctx);
    }

    ETableReadType ExtractMostHeavyReadType(const TString& queryPlan) {
        ETableReadType maxReadType = ETableReadType::Other;

        if (queryPlan.empty()) {
            return maxReadType;
        }

        NJson::TJsonValue root;
        NJson::ReadJsonTree(queryPlan, &root, false);

        if (root.Has("tables")) {
            for (const auto& table : root["tables"].GetArray()) {
                if (!table.Has("reads")) {
                    continue;
                }

                for (const auto& read : table["reads"].GetArray()) {
                    Y_VERIFY(read.Has("type"));
                    const auto& type = read["type"].GetString();

                    if (type == "Scan") {
                        maxReadType = Max(maxReadType, ETableReadType::Scan);
                    } else if (type == "FullScan") {
                        return ETableReadType::FullScan;
                    }
                }
            }
        }

        return maxReadType;
    }

    bool ReplyQueryResult(const TActorContext& ctx) {
        Y_VERIFY(QueryState);
        auto& queryRequest = QueryState->Request;
        auto& queryResult = QueryState->QueryResult;

        auto responseEv = MakeHolder<TEvKqp::TEvQueryResponse>();
        FillResponse(responseEv->Record);

        auto& record = responseEv->Record.GetRef();
        auto status = record.GetYdbStatus();

        auto now = TInstant::Now();
        auto queryDuration = now - QueryState->StartTime;

        if (status == Ydb::StatusIds::SUCCESS) {
            Counters->ReportQueryLatency(Settings.DbCounters, queryRequest.GetAction(), queryDuration);

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

            if (QueryState->QueryCompileResult && QueryState->QueryCompileResult->PreparedQueryNewEngine
                && QueryState->NewEngineCompatibleQuery)
            {
                ui64 computeCpuTimeUs = 0;
                for (auto& execution : queryResult.QueryStats.GetExecutions()) {
                    computeCpuTimeUs += execution.GetCpuTimeUs();
                }

                // query can be executed with NewEngine
                if (QueryState->ForceNewEngineState.ForcedNewEngine && *QueryState->ForceNewEngineState.ForcedNewEngine) {
                    Counters->ReportNewEngineForcedQueryStats(queryRequest.GetAction(), queryDuration, computeCpuTimeUs);
                } else {
                    Counters->ReportNewEngineCompatibleQueryStats(queryRequest.GetAction(), queryDuration, computeCpuTimeUs);
                }
            }
        }

        if (queryResult.SqlVersion) {
            Counters->ReportSqlVersion(Settings.DbCounters, *queryResult.SqlVersion);
        }

        if (Settings.LongSession && status != Ydb::StatusIds::SUCCESS) {
            bool isQueryById = queryRequest.GetAction() == NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED;

            TMaybe<TString> invalidatedId;
            if (HasSchemeOrFatalIssues(queryResult.Issues())) {
                auto compileResult = QueryState->QueryCompileResult;

                if (compileResult) {
                    invalidatedId = InvalidateQuery(*compileResult, ctx);
                }
            }

            if (invalidatedId) {
                LOG_NOTICE_S(ctx, NKikimrServices::KQP_WORKER, TKqpRequestInfo(QueryState->TraceId, SessionId)
                    << "Invalidating query due to scheme error: " << *invalidatedId);

                TIssues issues;
                issues.AddIssue(YqlIssue(TPosition(), TIssuesIds::KIKIMR_QUERY_INVALIDATED,
                    TStringBuilder() << "Query invalidated due to scheme error."));

                AddQueryIssues(*record.MutableResponse(), issues);

                if (isQueryById) {
                    // Avoid double retry for client on query invalidation. Return NOT_FOUND immediately on
                    // query invalidation.
                    status = Ydb::StatusIds::NOT_FOUND;
                    record.SetYdbStatus(status);
                }
            }
        }

        FillTxInfo(responseEv->Record);

        auto& stats = queryResult.QueryStats;
        stats.SetDurationUs(queryDuration.MicroSeconds());
        stats.SetWorkerCpuTimeUs(QueryState->CpuTime.MicroSeconds());
        if (QueryState->QueryCompileResult) {
            stats.MutableCompilation()->Swap(&QueryState->CompileStats);
        }

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        if (IsExecuteAction(queryRequest.GetAction())) {
            auto ru = CalcRequestUnit(stats);
            record.SetConsumedRu(ru);
            CollectSystemViewQueryStats(ctx, &stats, queryDuration, queryRequest.GetDatabase(), ru);
            SlowLogQuery(ctx, requestInfo, queryDuration, status, [&record](){
                ui64 resultsSize = 0;
                for (auto& result : record.GetResponse().GetResults()) {
                    resultsSize += result.ByteSize();
                }
                return resultsSize;
            });
        }

        bool reportStats = (GetStatsMode(queryRequest, EKikimrStatsMode::None) != EKikimrStatsMode::None);

        if (reportStats) {
            // TODO: For compatibility with old rpc handlers, deprecate.
            FillQueryProfile(stats, *record.MutableResponse());

            record.MutableResponse()->MutableQueryStats()->Swap(&stats);
            record.MutableResponse()->SetQueryPlan(queryResult.QueryPlan);
        }

        AddTrailingInfo(responseEv->Record.GetRef());
        return Reply(std::move(responseEv), ctx);
    }

    template<class TEvRecord>
    void AddTrailingInfo(TEvRecord& record) {
        if (ShutdownState) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKER, "Session ["  << SessionId  << "] is closing, set trailing metadata to request session shutdown");
            record.SetWorkerIsClosing(true);
        }
    }

    bool ReplyPingStatus(const TActorId& sender, ui64 proxyRequestId, bool ready, const TActorContext& ctx) {
        auto ev = MakeHolder<TEvKqp::TEvPingSessionResponse>();
        auto& record = ev->Record;
        record.SetStatus(Ydb::StatusIds::SUCCESS);
        record.MutableResponse()->SetSessionStatus(ready
            ? Ydb::Table::KeepAliveResult::SESSION_STATUS_READY
            : Ydb::Table::KeepAliveResult::SESSION_STATUS_BUSY);

        AddTrailingInfo(record);
        return ctx.Send(sender, ev.Release(), 0, proxyRequestId);
    }

    bool ReplyProcessError(const TActorId& sender, ui64 proxyRequestId, const TKqpRequestInfo& requestInfo,
        Ydb::StatusIds::StatusCode ydbStatus, const TString& message, const TActorContext& ctx)
    {
        LOG_WARN_S(ctx, NKikimrServices::KQP_WORKER, requestInfo << message);

        auto response = TEvKqp::TEvProcessResponse::Error(ydbStatus, message);

        AddTrailingInfo(response->Record);
        return ctx.Send(sender, response.Release(), 0, proxyRequestId);
    }

    bool CheckRequest(const TKqpRequestInfo& requestInfo, const TActorId& sender, ui64 proxyRequestId,
        const TActorContext& ctx)
    {
        if (requestInfo.GetSessionId() != SessionId) {
            TString error = TStringBuilder() << "Invalid session, expected: " << SessionId << ", request ignored";
            ReplyProcessError(sender, proxyRequestId, requestInfo, Ydb::StatusIds::BAD_SESSION, error, ctx);
            return false;
        }

        return true;
    }

    void ReplyBusy(TEvKqp::TEvQueryRequest::TPtr& ev, const TActorContext& ctx) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());

        if (!CheckRequest(requestInfo, ev->Sender, proxyRequestId, ctx)) {
            return;
        }

        auto busyStatus = Settings.Service.GetUseSessionBusyStatus()
            ? Ydb::StatusIds::SESSION_BUSY
            : Ydb::StatusIds::PRECONDITION_FAILED;

        ReplyProcessError(ev->Sender, proxyRequestId, requestInfo, busyStatus,
            "Pending previous query completion", ctx);
    }

    void CollectSystemViewQueryStats(const TActorContext& ctx,
        const NKqpProto::TKqpStatsQuery* stats, TDuration queryDuration,
        const TString& database, ui64 requestUnits)
    {
        auto type = QueryState->Request.GetType();
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                auto userSID = NACLib::TUserToken(QueryState->UserToken).GetUserSID();
                NSysView::CollectQueryStats(ctx, stats, queryDuration, ExtractQueryText(),
                    userSID, QueryState->ParametersSize, database, type, requestUnits);
            }
            default:
                break;
        }
    }

    TString ExtractQueryText() const {
        auto compileResult = QueryState->QueryCompileResult;
        if (compileResult) {
            if (compileResult->Query) {
                return compileResult->Query->Text;
            }
            return {};
        }
        return QueryState->Request.GetQuery();
    }

    void SlowLogQuery(const TActorContext &ctx, const TKqpRequestInfo& requestInfo, const TDuration& duration,
        Ydb::StatusIds::StatusCode status, const std::function<ui64()>& resultsSizeFunc)
    {
        auto logSettings = ctx.LoggerSettings();
        if (!logSettings) {
            return;
        }

        ui32 thresholdMs = 0;
        NActors::NLog::EPriority priority;

        if (logSettings->Satisfies(NActors::NLog::PRI_TRACE, NKikimrServices::KQP_SLOW_LOG)) {
            priority = NActors::NLog::PRI_TRACE;
            thresholdMs = Config->_KqpSlowLogTraceThresholdMs.Get().GetRef();
        } else if (logSettings->Satisfies(NActors::NLog::PRI_NOTICE, NKikimrServices::KQP_SLOW_LOG)) {
            priority = NActors::NLog::PRI_NOTICE;
            thresholdMs = Config->_KqpSlowLogNoticeThresholdMs.Get().GetRef();
        } else if (logSettings->Satisfies(NActors::NLog::PRI_WARN, NKikimrServices::KQP_SLOW_LOG)) {
            priority = NActors::NLog::PRI_WARN;
            thresholdMs = Config->_KqpSlowLogWarningThresholdMs.Get().GetRef();
        } else {
            return;
        }

        if (duration >= TDuration::MilliSeconds(thresholdMs)) {
            auto username = NACLib::TUserToken(QueryState->UserToken).GetUserSID();
            if (username.empty()) {
                username = "UNAUTHENTICATED";
            }

            auto queryText = ExtractQueryText();

            auto paramsText = TStringBuilder()
                << ToString(QueryState->ParametersSize)
                << 'b';

            ui64 resultsSize = 0;
            if (resultsSizeFunc) {
                resultsSize = resultsSizeFunc();
            }

            LOG_LOG_S(ctx, priority, NKikimrServices::KQP_SLOW_LOG, requestInfo
                << "Slow query, duration: " << duration.ToString()
                << ", status: " << status
                << ", user: " << username
                << ", results: " << resultsSize << 'b'
                << ", text: \"" << EscapeC(queryText) << '"'
                << ", parameters: " << paramsText);
        }
    }

    void FillCompileStatus(const TKqpCompileResult::TConstPtr& compileResult,
        TEvKqp::TProtoArenaHolder<NKikimrKqp::TEvQueryResponse>& record)
    {
        auto& ev = record.GetRef();

        ev.SetYdbStatus(compileResult->Status);

        auto& response = *ev.MutableResponse();
        AddQueryIssues(response, compileResult->Issues);

        if (compileResult->Status == Ydb::StatusIds::SUCCESS) {
            response.SetPreparedQuery(compileResult->Uid);

            auto& preparedQuery = compileResult->PreparedQuery;
            response.MutableQueryParameters()->CopyFrom(preparedQuery->GetParameters());

            if (preparedQuery->KqlsSize() > 0) {
                response.SetQueryAst(preparedQuery->GetKqls(0).GetAst());
                response.SetQueryPlan(preparedQuery->GetKqls(0).GetPlan());
            }
        }
    }

    void FillQueryProfile(const NKqpProto::TKqpStatsQuery& stats, NKikimrKqp::TQueryResponse& response) {
        auto& kqlProfile = *response.MutableProfile()->AddKqlProfiles();
        for (auto& execStats : stats.GetExecutions()) {
            auto& txStats = *kqlProfile.AddMkqlProfiles()->MutableTxStats();

            txStats.SetDurationUs(execStats.GetDurationUs());
            for (auto& tableStats : execStats.GetTables()) {
                auto& txTableStats = *txStats.AddTableAccessStats();

                txTableStats.MutableTableInfo()->SetName(tableStats.GetTablePath());
                if (tableStats.GetReadRows() > 0) {
                    txTableStats.MutableSelectRange()->SetRows(tableStats.GetReadRows());
                    txTableStats.MutableSelectRange()->SetBytes(tableStats.GetReadBytes());
                }
                if (tableStats.GetWriteRows() > 0) {
                    txTableStats.MutableUpdateRow()->SetCount(tableStats.GetWriteRows());
                    txTableStats.MutableUpdateRow()->SetBytes(tableStats.GetWriteBytes());
                }
                if (tableStats.GetEraseRows() > 0) {
                    txTableStats.MutableEraseRow()->SetCount(tableStats.GetEraseRows());
                }
            }
        }
    }

    void FillResponse(TEvKqp::TProtoArenaHolder<NKikimrKqp::TEvQueryResponse>& record) {
        Y_VERIFY(QueryState);
        auto& queryRequest = QueryState->Request;

        auto& queryResult = QueryState->QueryResult;
        auto arena = queryResult.ProtobufArenaPtr;
        if (arena) {
            record.Realloc(arena);
        }
        auto& ev = record.GetRef();

        bool replyResults = IsExecuteAction(queryRequest.GetAction());
        bool replyPlan = true;
        bool replyAst = true;

        // TODO: Handle in KQP to avoid generation of redundant data
        replyResults = replyResults && (QueryState->ReplyFlags & NKikimrKqp::QUERY_REPLY_FLAG_RESULTS);
        replyPlan = replyPlan && (QueryState->ReplyFlags & NKikimrKqp::QUERY_REPLY_FLAG_PLAN);
        replyAst = replyAst && (QueryState->ReplyFlags & NKikimrKqp::QUERY_REPLY_FLAG_AST);

        auto ydbStatus = GetYdbStatus(queryResult);
        auto issues = queryResult.Issues();

        ev.SetYdbStatus(ydbStatus);
        if (QueryState->QueryCompileResult) {
            AddQueryIssues(*ev.MutableResponse(), QueryState->QueryCompileResult->Issues);
        }
        AddQueryIssues(*ev.MutableResponse(), issues);

        if (replyResults) {
            for (auto& result : queryResult.Results) {
                // If we have result it must be allocated on protobuf arena
                Y_ASSERT(result->GetArena());
                Y_ASSERT(ev.MutableResponse()->GetArena() == result->GetArena());
                ev.MutableResponse()->AddResults()->Swap(result);
            }
        }

        /*
         * TODO:
         * For Scan/NewEngine plan will be set later on rpc_* level from stats and execution profiles, so
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
        switch (queryRequest.GetAction()) {
        case NKikimrKqp::QUERY_ACTION_PREPARE:
            replyQueryId = true;
            replyQueryParameters = true;
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            replyQueryParameters = replyQueryId = queryRequest.GetQueryCachePolicy().keep_in_cache();
            break;

        case NKikimrKqp::QUERY_ACTION_PARSE:
        case NKikimrKqp::QUERY_ACTION_VALIDATE:
            replyQueryParameters = true;
            break;

        default:
            break;
        }

        if (replyQueryParameters) {
            if (QueryState->QueryCompileResult) {
                ev.MutableResponse()->MutableQueryParameters()->CopyFrom(
                    QueryState->QueryCompileResult->PreparedQuery->GetParameters());
            } else {
                YQL_ENSURE(queryResult.PreparedQuery);
                ev.MutableResponse()->MutableQueryParameters()->CopyFrom(
                    queryResult.PreparedQuery->GetParameters());
            }
        }

        if (replyQueryId) {
            TString queryId;
            if (QueryState->QueryCompileResult) {
                queryId = QueryState->QueryCompileResult->Uid;
            } else {
                YQL_ENSURE(!Settings.LongSession);
                Y_PROTOBUF_SUPPRESS_NODISCARD queryResult.PreparedQuery->SerializeToString(&queryId);
            }

            ev.MutableResponse()->SetPreparedQuery(queryId);
        }
    }

    void FillTxInfo(TEvKqp::TProtoArenaHolder<NKikimrKqp::TEvQueryResponse>& record) {
        auto& ev = record.GetRef();

        auto txInfo = QueryState->TxId.empty()
            ? KqpHost->GetTransactionInfo()
            : KqpHost->GetTransactionInfo(QueryState->TxId);

        TString replyTxId;
        if (txInfo) {
            Counters->ReportTransaction(Settings.DbCounters, *txInfo);

            switch (txInfo->Status) {
                case TKqpTransactionInfo::EStatus::Active:
                    replyTxId = QueryState->TxId;
                    break;
                default:
                    break;
            }
        }

        ev.MutableResponse()->MutableTxMeta()->set_id(replyTxId);
    }

    void MakeNewQueryState() {
        ++QueryId;
        QueryState.Reset(MakeHolder<TKqpQueryState>());
    }

    TString InvalidateQuery(const TKqpCompileResult& compileResult, const TActorContext& ctx) {
        auto invalidateEv = MakeHolder<TEvKqp::TEvCompileInvalidateRequest>(compileResult.Uid, Settings.DbCounters);
        ctx.Send(MakeKqpCompileServiceID(ctx.SelfID.NodeId()), invalidateEv.Release());

        return compileResult.Uid;
    }

    void ScheduleNextShutdownTick(const TActorContext& ctx) {
        ctx.Schedule(TDuration::MilliSeconds(ShutdownState->GetNextTickMs()), new TEvKqp::TEvContinueShutdown());
    }

    void CheckContinueShutdown(const TActorContext& ctx) {
        Y_VERIFY(ShutdownState);
        ShutdownState->MoveToNextState();
        if (ShutdownState->HardTimeoutReached()){
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_WORKER, "Reached hard shutdown timeout " << TKqpRequestInfo("", SessionId));
            if (CleanupState) {
                if (!CleanupState->Final) {
                    Y_VERIFY(QueryState);
                    QueryState->KeepSession = false;
                }
            } else if (QueryState) {
                QueryState->KeepSession = false;
            } else {
                FinalCleanup(ctx);
            }

        } else {
            ScheduleNextShutdownTick(ctx);
            LOG_INFO_S(ctx, NKikimrServices::KQP_WORKER, "Schedule next shutdown tick " << TKqpRequestInfo("", SessionId));
        }
    }

    void StartIdleTimer(const TActorContext& ctx) {
        StopIdleTimer(ctx);

        ++IdleTimerId;
        auto idleDuration = TDuration::Seconds(Config->_KqpSessionIdleTimeoutSec.Get().GetRef());
        IdleTimerActorId = CreateLongTimer(ctx, idleDuration,
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvKqp::TEvIdleTimeout(IdleTimerId)));
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, "Created long timer for idle timeout, timer id: " << IdleTimerId
            << ", duration: " << idleDuration << ", actor: " << IdleTimerActorId);
    }

    void StopIdleTimer(const TActorContext& ctx) {
        if (IdleTimerActorId) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_WORKER, "Destroying long timer actor for idle timout: "
                << IdleTimerActorId);
            ctx.Send(IdleTimerActorId, new TEvents::TEvPoisonPill());
        }
        IdleTimerActorId = TActorId();
    }

    IKikimrQueryExecutor::TExecuteSettings CreateRollbackSettings() {
        YQL_ENSURE(QueryState);

        IKikimrQueryExecutor::TExecuteSettings settings;
        settings.RollbackTx = true;
        settings.Deadlines.TimeoutAt = TInstant::Now() + TDuration::Minutes(1);
        settings.UseNewEngine = UseNewEngine();

        return settings;
    }

    TMaybe<bool> UseNewEngine() const {
        YQL_ENSURE(QueryState);

        if (auto txInfo = KqpHost->GetTransactionInfo(QueryState->TxId)) {
            if (auto engine = txInfo->TxEngine; engine.has_value()) {
                return *engine == TKqpTransactionInfo::EEngine::NewEngine;
            }
        }

        return Nothing();
    }

    static bool IsExecuteAction(const NKikimrKqp::EQueryAction& action) {
        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                return true;

            default:
                return false;
        }
    }

    static bool IsDocumentApiRestricted(const TString& requestType) {
        return requestType != DocumentApiRequestType;
    }

    static TKikimrQueryLimits GetQueryLimits(const TKqpWorkerSettings& settings) {
        const auto& queryLimitsProto = settings.Service.GetQueryLimits();
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
    TActorId Owner;
    TString SessionId;
    TKqpWorkerSettings Settings;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpRequestCounters> RequestCounters;
    TKikimrConfiguration::TPtr Config;
    TInstant CreationTime;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    ui32 QueryId;
    THolder<TKqpQueryState> QueryState;
    THolder<TKqpCleanupState> CleanupState;
    ui32 IdleTimerId;
    TActorId IdleTimerActorId;
    std::optional<TSessionShutdownState> ShutdownState;
};

} // namespace

IActor* CreateKqpWorkerActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpWorkerActor(owner, sessionId, kqpSettings, workerSettings, moduleResolverState, counters);
}

} // namespace NKqp
} // namespace NKikimr
