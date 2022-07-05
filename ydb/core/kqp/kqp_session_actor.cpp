#include "kqp_impl.h"
#include "provider/yql_kikimr_provider.h"
#include "common/kqp_timeouts.h"
#include "common/kqp_ru_calc.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

#define LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)


struct TKqpQueryState {
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    NKikimrKqp::TQueryRequest Request;
    TString TraceId;

    TInstant StartTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;

    TString UserToken;
};

EKikimrStatsMode GetStatsMode(const NKikimrKqp::TQueryRequest& queryRequest, EKikimrStatsMode minMode) {
    switch (queryRequest.GetCollectStats()) {
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            return EKikimrStatsMode::Basic;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return EKikimrStatsMode::Full;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
            return EKikimrStatsMode::Profile;
        default:
            return std::max(EKikimrStatsMode::None, minMode);
    }
}

class TKqpSessionActor : public TActorBootstrapped<TKqpSessionActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SESSION_ACTOR;
    }

   TKqpSessionActor(const TActorId& owner, const TString& sessionId, const TKqpSettings::TConstPtr& kqpSettings,
        const TKqpWorkerSettings& workerSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
        TIntrusivePtr<TKqpCounters> counters)
        : Owner(owner)
        , SessionId(sessionId)
        , Settings(workerSettings)
        , WorkerActor(CreateKqpWorkerActor(owner, sessionId, kqpSettings, workerSettings,
                    moduleResolverState, counters))
    {
        TKikimrConfiguration::TPtr config = MakeIntrusive<TKikimrConfiguration>();
        config->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), Settings.Cluster, kqpSettings->Settings, false);
        IdleDuration = TDuration::Seconds(*config->_KqpSessionIdleTimeoutSec.Get());
    }

    void Bootstrap() {
        WorkerId = RegisterWithSameMailbox(WorkerActor.release());
        Become(&TKqpSessionActor::MainState);
        StartIdleTimer();
    }

    NYql::TKikimrQueryDeadlines GetQueryDeadlines(const NKikimrKqp::TQueryRequest& queryRequest) {
        NYql::TKikimrQueryDeadlines res;

        auto now = TAppData::TimeProvider->Now();
        if (queryRequest.GetCancelAfterMs()) {
            res.CancelAt = now + TDuration::MilliSeconds(queryRequest.GetCancelAfterMs());
        }

        auto timeoutMs = GetQueryTimeout(queryRequest.GetType(), queryRequest.GetTimeoutMs(), Settings.Service);
        res.TimeoutAt = now + timeoutMs;
        return res;
    }

    void MakeNewQueryState() {
        ++QueryId;
        Y_ENSURE(!QueryState);
        QueryState = std::make_unique<TKqpQueryState>();
    }

    void Handle(TEvKqp::TEvQueryRequest::TPtr &ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());
        Y_ENSURE(requestInfo.GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << requestInfo.GetSessionId());

        MakeNewQueryState();
        QueryState->Request.Swap(event.MutableRequest());
        auto& queryRequest = QueryState->Request;

        if (queryRequest.GetDatabase() != Settings.Database) {
            TString message = TStringBuilder() << "Wrong database, expected:" << Settings.Database
                << ", got: " << queryRequest.GetDatabase();
            ReplyProcessError(requestInfo, Ydb::StatusIds::BAD_REQUEST, message);
            QueryState.reset();
            return;
        }

        Y_ENSURE(queryRequest.HasAction());
        auto action = queryRequest.GetAction();
        Y_ENSURE(queryRequest.HasType());
        auto type = queryRequest.GetType();

        LOG_D(requestInfo << "Received request,"
            << " proxyRequestId: " << proxyRequestId
            << " query: " << (queryRequest.HasQuery() ? queryRequest.GetQuery().Quote() : "")
            << " prepared: " << queryRequest.HasPreparedQuery()
            << " tx_control: " << queryRequest.HasTxControl()
            << " action: " << action
            << " type: " << type
        );

        switch (action) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
        case NKikimrKqp::QUERY_ACTION_PREPARE:
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:

            switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
                break;

            // not supported yet
            case NKikimrKqp::QUERY_TYPE_AST_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            // should not be compiled. TODO: forward to request executer
            case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
            default:
                Y_ENSURE(false, "type: " << type << " is not supported");
                return;
            }
            break;

        // not supported yet
        case NKikimrKqp::QUERY_ACTION_EXPLAIN:
        case NKikimrKqp::QUERY_ACTION_VALIDATE:
        case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
        case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
        case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
        case NKikimrKqp::QUERY_ACTION_PARSE:
        default:
            Y_ENSURE(false, "action: " << action << " is not supported");
            return;
        }

        QueryState->Sender = ev->Sender;
        QueryState->ProxyRequestId = proxyRequestId;
        QueryState->TraceId = requestInfo.GetTraceId();
        QueryState->StartTime = TInstant::Now();
        QueryState->UserToken = event.GetUserToken();
        QueryState->QueryDeadlines = GetQueryDeadlines(queryRequest);

        StopIdleTimer();

        CompileQuery();
    }

    void CompileQuery() {
        Y_ENSURE(QueryState);
        auto& queryRequest = QueryState->Request;

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
                Y_ENSURE(false);
        }

        auto compileDeadline = QueryState->QueryDeadlines.TimeoutAt;
        if (QueryState->QueryDeadlines.CancelAt) {
            compileDeadline = Min(compileDeadline, QueryState->QueryDeadlines.CancelAt);
        }

        auto compileRequestActor = CreateKqpCompileRequestActor(SelfId(), QueryState->UserToken, uid,
            std::move(query), keepInCache, compileDeadline, Settings.DbCounters);
        TlsActivationContext->ExecutorThread.RegisterActor(compileRequestActor);
    }

    void Handle(TEvKqp::TEvCompileResponse::TPtr &ev) {
        auto compileResult = ev->Get()->CompileResult;

        Y_ENSURE(compileResult);
        Y_ENSURE(QueryState);

        if (compileResult->Status != Ydb::StatusIds::SUCCESS) {
            if (ReplyQueryCompileError(compileResult)) {
                StartIdleTimer();
            } else {
                FinalCleanup();
            }

            return;
        }

        const ui32 compiledVersion = compileResult->PreparedQuery->GetVersion();
        Y_ENSURE(compiledVersion == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1, "invalid compiled version");

        auto& queryRequest = QueryState->Request;
        if (queryRequest.GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE) {
            if (ReplyPrepareResult(compileResult, ev->Get()->Stats)) {
                StartIdleTimer();
            } else {
                FinalCleanup();
            }
            return;
        }

        PerformQuery(compileResult);
    }

    void PerformQuery(TKqpCompileResult::TConstPtr compileResult) {
        Y_ENSURE(QueryState);
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& queryRequest = QueryState->Request;
        bool nonInteractive = false;

        if (queryRequest.HasTxControl()) {
            // TODO Create transaction handle context
            TString out;
            NProtoBuf::TextFormat::PrintToString(queryRequest.GetTxControl(), &out);
            LOG_D("queryRequest TxControl: " << out);
        }

        auto action = queryRequest.GetAction();
        auto queryType = queryRequest.GetType();

        if (action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
            Y_ENSURE(queryType == NKikimrKqp::QUERY_TYPE_SQL_DML);
            queryType = NKikimrKqp::QUERY_TYPE_PREPARED_DML;
            action = NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED;
        }

        if (action != NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED) {
            const TString& message = "Unknown query action";
            ReplyProcessError(requestInfo, Ydb::StatusIds::BAD_REQUEST, message);
            QueryState.reset();
            return;
        }

        LOG_D("nonInteractive: " << nonInteractive
                << ", serializable_rw: " << queryRequest.GetTxControl().begin_tx().has_serializable_read_write());
        Y_ENSURE(queryType == NKikimrKqp::QUERY_TYPE_PREPARED_DML);

        TPreparedQueryConstPtr preparedQuery = compileResult->PreparedQuery;
        Y_ENSURE(preparedQuery);
        QueryState->Request.SetQuery(preparedQuery->GetText());

        ExecutePreparedQuery(preparedQuery);
    }

    bool ReplyPrepareResult(const TKqpCompileResult::TConstPtr& compileResult,
            const NKqpProto::TKqpStatsCompile& compileStats) {
        auto responseEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);
        auto ru = NRuCalc::CpuTimeToUnit(TDuration::MicroSeconds(compileStats.GetCpuTimeUs()));
        responseEv->Record.GetRef().SetConsumedRu(ru);
        return Reply(std::move(responseEv));
    }

    void ExecutePreparedQuery(TPreparedQueryConstPtr& query) {
        Y_UNUSED(query);
        auto& queryRequest = QueryState->Request;
        if (false) {
            NKikimrMiniKQL::TParams parameters = std::move(*queryRequest.MutableParameters());
            Y_UNUSED(parameters);
        } else {
            EKikimrStatsMode statsMode = GetStatsMode(queryRequest, EKikimrStatsMode::Basic);
            Y_UNUSED(statsMode);
            //ReplyProcessError(requestInfo, status, message);
        }
        Y_VERIFY(false, "Success!!!");
    }

    bool ReplyQueryCompileError(const TKqpCompileResult::TConstPtr& compileResult) {
        auto responseEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);
        responseEv->Record.GetRef().SetConsumedRu(1);
        return Reply(std::move(responseEv));
    }

    bool Reply(std::unique_ptr<TEvKqp::TEvQueryResponse> responseEv) {
        Y_ENSURE(QueryState);

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& record = responseEv->Record.GetRef();
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        response.SetSessionId(SessionId);

        Send(QueryState->Sender, responseEv.release(), 0, QueryState->ProxyRequestId);
        LOG_D(requestInfo << "Sent query response back to proxy, proxyRequestId: " << QueryState->ProxyRequestId
            << ", proxyId: " << QueryState->Sender.ToString());

        QueryState.reset();

        if (status == Ydb::StatusIds::INTERNAL_ERROR) {
            LOG_D(requestInfo << "Worker destroyed due to internal error");
            //Counters->ReportWorkerClosedError(Settings.DbCounters);
            return false;
        }
        if (status == Ydb::StatusIds::BAD_SESSION) {
            LOG_D(requestInfo << "Worker destroyed due to session error");
            //Counters->ReportWorkerClosedError(Settings.DbCounters);
            return false;
        }

        return true;
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

    void Handle(TEvKqp::TEvPingSessionRequest::TPtr &ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto& evRecord = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(evRecord.GetTraceId(), evRecord.GetRequest().GetSessionId());
        Y_ENSURE(requestInfo.GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << requestInfo.GetSessionId());

        auto result = std::make_unique<TEvKqp::TEvPingSessionResponse>();
        auto& record = result->Record;
        record.SetStatus(Ydb::StatusIds::SUCCESS);
        auto sessionStatus = QueryState
            ? Ydb::Table::KeepAliveResult::SESSION_STATUS_BUSY
            : Ydb::Table::KeepAliveResult::SESSION_STATUS_READY;
        record.MutableResponse()->SetSessionStatus(sessionStatus);

        Send(ev->Sender, result.release(), 0, proxyRequestId);
    }

    void StartIdleTimer() {
        StopIdleTimer();

        ++IdleTimerId;
        IdleTimerActorId = CreateLongTimer(TlsActivationContext->AsActorContext(), IdleDuration,
                new IEventHandle(SelfId(), SelfId(), new TEvKqp::TEvIdleTimeout(IdleTimerId)));
        LOG_D("Created long timer for idle timeout, timer id: " << IdleTimerId
                << ", duration: " << IdleDuration << ", actor: " << IdleTimerActorId);
    }

    void StopIdleTimer() {
        if (IdleTimerActorId) {
            LOG_D("Destroying long timer actor for idle timout: " << IdleTimerActorId);
            Send(IdleTimerActorId, new TEvents::TEvPoisonPill());
        }
        IdleTimerActorId = TActorId();
    }

    void Handle(TEvKqp::TEvIdleTimeout::TPtr &ev) {
        auto timerId = ev->Get()->TimerId;
        LOG_D("Received TEvIdleTimeout in ready state, timer id: "
            << timerId << ", sender: " << ev->Sender);

        if (timerId == IdleTimerId) {
            LOG_N(TKqpRequestInfo("", SessionId) << "Worker idle timeout, worker destroyed");
            //Counters->ReportWorkerClosedIdle(Settings.DbCounters);
            FinalCleanup();
        }
    }

    void FinalCleanup() {
        Cleanup(true);
    }

    void Cleanup(bool isFinal = false) {
        // 1. Cleanup transactions -- QueryState->TxId

        // 2. Reply to kqp_proxy
        if (isFinal) {
            auto closeEv = std::make_unique<TEvKqp::TEvCloseSessionResponse>();
            closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            closeEv->Record.MutableResponse()->SetSessionId(SessionId);
            closeEv->Record.MutableResponse()->SetClosed(true);
            Send(Owner, closeEv.release());
            PassAway();
        } else {
            StartIdleTimer();
            QueryState.reset();
        }

        // 3. check tx locks
    }

    bool ReplyProcessError(const TKqpRequestInfo& requestInfo, Ydb::StatusIds::StatusCode ydbStatus,
            const TString& message)
    {
        LOG_W(requestInfo << message);

        auto ev = std::make_unique<TEvKqp::TEvQueryResponse>();
        ev->Record.GetRef().SetYdbStatus(ydbStatus);

        auto& response = *ev->Record.GetRef().MutableResponse();

        AddQueryIssues(response, {TIssue{message}});

        return Reply(std::move(ev));
    }

    STATEFN(MainState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, Handle);
                hFunc(TEvKqp::TEvCompileResponse, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, Handle);
                hFunc(TEvKqp::TEvPingSessionRequest, Handle);

                //hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                //hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
            default:
                UnexpectedEvent("MainState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

private:
    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TKqpSessionActor in state " << state << " recieve unexpected event " <<
                TypeName(*ev.Get()->GetBase()) << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

    void InternalError(const TString& message) {
        LOG_E("Internal error, SelfId: " << SelfId() << ", message: " << message);
        PassAway();
    }

private:
    TActorId Owner;
    TString SessionId;
    TKqpWorkerSettings Settings;
    std::unique_ptr<IActor> WorkerActor;
    std::unique_ptr<TKqpQueryState> QueryState;
    ui32 QueryId = 0;

    TActorId IdleTimerActorId;
    ui32 IdleTimerId = 0;
    TDuration IdleDuration;

    TActorId WorkerId;
};

}

IActor* CreateKqpSessionActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpSessionActor(owner, sessionId, kqpSettings, workerSettings, moduleResolverState, counters);
}

}
}
