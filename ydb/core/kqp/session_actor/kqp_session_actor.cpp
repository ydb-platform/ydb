#include "kqp_session_actor.h"
#include "kqp_tx.h"
#include "kqp_worker_common.h"

#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/compile_service/kqp_compile_service.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/executer_actor/kqp_locks_helper.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/core/util/ulid.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/printf.h>

#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/wilson/wilson_trace.h>

LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

#define LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_T(msg) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)

class TRequestFail : public yexception {
public:
    Ydb::StatusIds::StatusCode Status;
    std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> Issues;

    TRequestFail(Ydb::StatusIds::StatusCode status,
            std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {}
};

struct TKqpQueryState {
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    std::unique_ptr<TEvKqp::TEvQueryRequest> RequestEv;
    ui64 ParametersSize = 0;
    TPreparedQueryHolder::TConstPtr PreparedQuery;
    TKqpCompileResult::TConstPtr CompileResult;
    NKqpProto::TKqpStatsCompile CompileStats;
    TIntrusivePtr<TKqpTransactionContext> TxCtx;
    TQueryData::TPtr QueryData;

    TActorId RequestActorId;

    ui64 CurrentTx = 0;
    TString TraceId;
    bool IsDocumentApiRestricted = false;

    TInstant StartTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;

    NKqpProto::TKqpStatsQuery Stats;
    bool KeepSession = false;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;

    NLWTrace::TOrbit Orbit;
    NWilson::TSpan KqpSessionSpan;
    ETableReadType MaxReadType = ETableReadType::Other;

    TTxId TxId; // User tx
    bool Commit = false;
    bool Commited = false;

    NTopic::TTopicOperations TopicOperations;
    TDuration CpuTime;
    std::optional<NCpuTime::TCpuTimer> CurrentTimer;

    NKikimrKqp::EQueryAction GetAction() const {
        return RequestEv->GetAction();
    }

    bool GetKeepSession() const {
        return RequestEv->GetKeepSession();
    }

    const TString& GetQuery() const {
        return RequestEv->GetQuery();
    }

    const TString& GetPreparedQuery() const {
        return RequestEv->GetPreparedQuery();
    }

    NKikimrKqp::EQueryType GetType() const {
        return RequestEv->GetType();
    }

    void EnsureAction() {
        YQL_ENSURE(RequestEv->HasAction());
    }

    bool GetUsePublicResponseDataFormat() const {
        return RequestEv->GetUsePublicResponseDataFormat();
    }

    void SetQueryDeadlines(const NKikimrConfig::TTableServiceConfig& service) {
        auto now = TAppData::TimeProvider->Now();
        auto cancelAfter = RequestEv->GetCancelAfter();
        auto timeout = RequestEv->GetOperationTimeout();
        if (cancelAfter.MilliSeconds() > 0) {
            QueryDeadlines.CancelAt = now + cancelAfter;
        }

        auto timeoutMs = GetQueryTimeout(GetType(), timeout.MilliSeconds(), service);
        QueryDeadlines.TimeoutAt = now + timeoutMs;
    }

    bool HasTopicOperations() const {
        return RequestEv->HasTopicOperations();
    }

    bool GetQueryKeepInCache() const {
        return RequestEv->GetQueryKeepInCache();
    }

    const TString& GetDatabase() const {
        return RequestEv->GetDatabase();
    }

    TString ExtractQueryText() const {
        if (CompileResult) {
            if (CompileResult->Query) {
                return CompileResult->Query->Text;
            }
            return {};
        }
        return RequestEv->GetQuery();
    }

    const ::NKikimrKqp::TTopicOperations& GetTopicOperations() const {
        return RequestEv->GetTopicOperations();
    }

    bool NeedPersistentSnapshot() const {
        auto type = GetType();
        return (
            type == NKikimrKqp::QUERY_TYPE_SQL_SCAN ||
            type == NKikimrKqp::QUERY_TYPE_AST_SCAN
        );
    }

    bool HasTxControl() const {
        return RequestEv->HasTxControl();
    }

    const ::Ydb::Table::TransactionControl& GetTxControl() const {
        return RequestEv->GetTxControl();
    }

    const ::NKikimrMiniKQL::TParams& GetParameters() const {
        return RequestEv->GetParameters();
    }

    const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& GetYdbParameters() const {
        return RequestEv->GetYdbParameters();
    }

    Ydb::Table::QueryStatsCollection::Mode GetStatsMode() const {
        if (!RequestEv->HasCollectStats()) {
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        }

        auto cStats = RequestEv->GetCollectStats();
        if (cStats == Ydb::Table::QueryStatsCollection::STATS_COLLECTION_UNSPECIFIED) {
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        }

        return cStats;
    }

    bool CollectStatsDefined() const {
        return GetStatsMode() != Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
    }

    bool HasPreparedQuery() const {
        return RequestEv->HasPreparedQuery();
    }

    bool IsStreamResult() const {
        auto type = GetType();
        return (
            type == NKikimrKqp::QUERY_TYPE_AST_SCAN ||
            type == NKikimrKqp::QUERY_TYPE_SQL_SCAN ||
            type == NKikimrKqp::QUERY_TYPE_SQL_QUERY
        );
    }

    void ResetTimer() {
        if (CurrentTimer) {
            CpuTime += CurrentTimer->GetTime();
            CurrentTimer.reset();
        }
    }

    TDuration GetCpuTime() {
        ResetTimer();
        return CpuTime;
    }
};

struct TKqpCleanupCtx {
    std::deque<TIntrusivePtr<TKqpTransactionContext>> TransactionsToBeAborted;
    bool IsWaitingForWorkerToClose = false;
    bool Final = false;
    TInstant Start = TInstant::Now();
};

class TKqpSessionActor : public TActorBootstrapped<TKqpSessionActor> {

class TTimerGuard {
public:
    TTimerGuard(TKqpSessionActor* this_)
      : This(this_)
    {
        if (This->QueryState) {
            YQL_ENSURE(!This->QueryState->CurrentTimer);
            This->QueryState->CurrentTimer.emplace();
        }
    }

    ~TTimerGuard() {
        if (This->QueryState) {
            This->QueryState->ResetTimer();
        }
    }

private:
    TKqpSessionActor* This;
};

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SESSION_ACTOR;
    }

   TKqpSessionActor(const TActorId& owner, const TString& sessionId, const TKqpSettings::TConstPtr& kqpSettings,
            const TKqpWorkerSettings& workerSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
            TIntrusivePtr<TKqpCounters> counters)
        : Owner(owner)
        , SessionId(sessionId)
        , Counters(counters)
        , Settings(workerSettings)
        , ModuleResolverState(moduleResolverState)
        , KqpSettings(kqpSettings)
        , Config(CreateConfig(kqpSettings, workerSettings))
        , Transactions(*Config->_KqpMaxActiveTxPerSession.Get(), TDuration::Seconds(*Config->_KqpTxIdleTimeoutSec.Get()))
    {
        RequestCounters = MakeIntrusive<TKqpRequestCounters>();
        RequestCounters->Counters = Counters;
        RequestCounters->DbCounters = Settings.DbCounters;
        RequestCounters->TxProxyMon = MakeIntrusive<NTxProxy::TTxProxyMon>(AppData()->Counters);

        FillSettings.AllResultsBytesLimit = Nothing();
        FillSettings.RowsLimitPerWrite = Config->_ResultRowsLimit.Get().GetRef();
        FillSettings.Format = IDataProvider::EResultFormat::Custom;
        FillSettings.FormatDetails = TString(KikimrMkqlProtoFormat);
    }

    void Bootstrap() {
        LOG_D("session actor bootstrapped");
        Counters->ReportSessionActorCreated(Settings.DbCounters);
        CreationTime = TInstant::Now();

        Config->FeatureFlags = AppData()->FeatureFlags;

        RequestControls.Reqister(TlsActivationContext->AsActorContext());
        Become(&TKqpSessionActor::ReadyState);
    }

    TString LogPrefix() const {
        TStringBuilder result = TStringBuilder()
            << "SessionId: " << SessionId << ", "
            << "ActorId: " << SelfId() << ", "
            << "ActorState: " << CurrentStateFuncName() << ", ";
        if (Y_LIKELY(QueryState)) {
            result << "TraceId: " << QueryState->TraceId << ", ";
        }
        return result;
    }

    void MakeNewQueryState(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ++QueryId;
        YQL_ENSURE(!QueryState);
        QueryState = std::make_shared<TKqpQueryState>();
        QueryState->Sender = ev->Sender;
        QueryState->ProxyRequestId = ev->Cookie;
        QueryState->TraceId = ev->Get()->GetTraceId();
        QueryState->IsDocumentApiRestricted = IsDocumentApiRestricted(ev->Get()->GetRequestType());
        QueryState->StartTime = TInstant::Now();
        QueryState->UserToken = ev->Get()->GetUserToken();
        QueryState->ParametersSize = ev->Get()->GetParametersSize();
        QueryState->RequestActorId = ev->Get()->GetRequestActorId();
        auto selfId = SelfId();
        auto as = TActivationContext::ActorSystem();
        ev->Get()->SetClientLostAction(selfId, as);
        QueryState->RequestEv.reset(ev->Release().Release());
        QueryState->KeepSession = Settings.LongSession || QueryState->GetKeepSession();
        QueryState->SetQueryDeadlines(Settings.Service);
        auto action = QueryState->GetAction();

        NWilson::TTraceId id;
        if (false) { // change to enable Wilson tracing
            id = NWilson::TTraceId::NewTraceId(TWilsonKqp::KqpSession, Max<ui32>());
            LOG_I("wilson tracing started, id: " + std::to_string(id.GetTraceId()));
        }
        QueryState->KqpSessionSpan = NWilson::TSpan(TWilsonKqp::KqpSession, std::move(id), "Session.query." + NKikimrKqp::EQueryAction_Name(action), NWilson::EFlags::AUTO_END);
    }

    bool ConvertParameters() {
        auto& event = QueryState->RequestEv->Record;

        if (!event.GetRequest().HasParameters() && event.GetRequest().YdbParametersSize()) {
            try {
                ConvertYdbParamsToMiniKQLParams(event.GetRequest().GetYdbParameters(), *event.MutableRequest()->MutableParameters());
            } catch (const std::exception& ex) {
                TString message = TStringBuilder() << "Failed to parse query parameters. "<< ex.what();
                ReplyProcessError(QueryState->Sender, QueryState->ProxyRequestId, Ydb::StatusIds::BAD_REQUEST, message);
                return false;
            }
        }

        return true;
    }

    void ForwardRequest(TEvKqp::TEvQueryRequest::TPtr& ev) {
        if (!ConvertParameters())
            return;

        QueryState->RequestEv->PrepareRemote();

        if (!WorkerId) {
            std::unique_ptr<IActor> workerActor(CreateKqpWorkerActor(SelfId(), SessionId, KqpSettings, Settings,
                ModuleResolverState, Counters));
            WorkerId = RegisterWithSameMailbox(workerActor.release());
        }
        TlsActivationContext->Send(new IEventHandle(*WorkerId, SelfId(), QueryState->RequestEv.release(), ev->Flags, ev->Cookie,
                    nullptr, std::move(ev->TraceId)));
        Become(&TKqpSessionActor::ExecuteState);
    }

    void ForwardResponse(TEvKqp::TEvQueryResponse::TPtr& ev) {
        QueryResponse = std::unique_ptr<TEvKqp::TEvQueryResponse>(ev->Release().Release());
        Cleanup();
    }

    void ReplyTransactionNotFound(const TString& txId) {
        std::vector<TIssue> issues{YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND,
            TStringBuilder() << "Transaction not found: " << txId)};
        ReplyQueryError(Ydb::StatusIds::NOT_FOUND, "", MessageFromIssues(issues));
    }

    void RollbackTx() {
        YQL_ENSURE(QueryState->HasTxControl(), "Can't perform ROLLBACK_TX: TxControl isn't set in TQueryRequest");
        const auto& txControl = QueryState->GetTxControl();
        QueryState->Commit = txControl.commit_tx();
        auto txId = TTxId::FromString(txControl.tx_id());
        if (auto ctx = Transactions.ReleaseTransaction(txId)) {
            ctx->Invalidate();
            Transactions.AddToBeAborted(std::move(ctx));
            ReplySuccess();
        } else {
            ReplyTransactionNotFound(txControl.tx_id());
        }
    }

    void CommitTx() {
        YQL_ENSURE(QueryState->HasTxControl());
        const auto& txControl = QueryState->GetTxControl();
        YQL_ENSURE(txControl.tx_selector_case() == Ydb::Table::TransactionControl::kTxId, "Can't commit transaction - "
            << " there is no TxId in Query's TxControl");

        QueryState->Commit = txControl.commit_tx();

        auto txId = TTxId::FromString(txControl.tx_id());
        auto txCtx = Transactions.Find(txId);
        LOG_D("queryRequest TxControl: " << txControl.DebugString() << " txCtx: " << (void*)txCtx.Get());
        if (!txCtx) {
            ReplyTransactionNotFound(txControl.tx_id());
            return;
        }
        QueryState->TxCtx = std::move(txCtx);
        QueryState->QueryData = std::make_shared<TQueryData>(QueryState->TxCtx->TxAlloc);
        QueryState->TxId = txId;
        if (!CheckTransacionLocks()) {
            return;
        }

        bool replied = ExecutePhyTx(/*query*/ nullptr, /*tx*/ nullptr, /*commit*/ true);
        if (!replied) {
            Become(&TKqpSessionActor::ExecuteState);
        }
    }

    static bool IsQueryTypeSupported(NKikimrKqp::EQueryType type) {
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_QUERY:
                return true;

            // should not be compiled. TODO: forward to request executer
            // not supported yet
            case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
            case NKikimrKqp::QUERY_TYPE_UNDEFINED:
                return false;
        }
    }

    void HandleClientLost(NGRpcService::TEvClientLost::TPtr&) {
        LOG_D("Got ClientLost event, send AbortExecution to executor: "
            << ExecuterId);

        if (ExecuterId) {
            auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Client lost"); // any status code can be here

            Send(ExecuterId, abortEv.Release());
        }
        Cleanup();
    }

    void HandleReady(TEvKqp::TEvQueryRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        ui64 proxyRequestId = ev->Cookie;
        YQL_ENSURE(ev->Get()->GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << ev->Get()->GetSessionId());

        if (ev->Get()->HasYdbStatus() && ev->Get()->GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->GetQueryIssues(), issues);
            TString errMsg = issues.ToString();
            auto status = ev->Get()->GetYdbStatus();

            LOG_N(TKqpRequestInfo("", SessionId)
                << "Got invalid query request, reply with status: "
                << status
                << " msg: "
                << errMsg <<".");
            ReplyProcessError(ev->Sender, proxyRequestId, status, errMsg);
            return;
        }

        if (ShutdownState && ShutdownState->SoftTimeoutReached()) {
            // we reached the soft timeout, so at this point we don't allow to accept new queries for session.
            LOG_N("system shutdown requested: soft timeout reached, no queries can be accepted");
            ReplyProcessError(ev->Sender, proxyRequestId, Ydb::StatusIds::BAD_SESSION, "Session is under shutdown");
            CleanupAndPassAway();
            return;
        }

        MakeNewQueryState(ev);
        TTimerGuard timer(this);
        YQL_ENSURE(QueryState->GetDatabase() == Settings.Database,
                "Wrong database, expected:" << Settings.Database << ", got: " << QueryState->GetDatabase());

        QueryState->EnsureAction();
        auto action = QueryState->GetAction();

        LWTRACK(KqpSessionQueryRequest,
            QueryState->Orbit,
            QueryState->GetDatabase(),
            QueryState->GetType(),
            action,
            QueryState->GetQuery());

        LOG_D("received request,"
            << " proxyRequestId: " << proxyRequestId
            << " prepared: " << QueryState->HasPreparedQuery()
            << " tx_control: " << QueryState->HasTxControl()
            << " action: " << action
            << " type: " << QueryState->GetType()
            << " text: " << QueryState->GetQuery()
        );

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
            case NKikimrKqp::QUERY_ACTION_PREPARE:
            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED: {
                auto type = QueryState->GetType();
                YQL_ENSURE(type != NKikimrKqp::QUERY_TYPE_UNDEFINED, "query type is undefined");

                if (action == NKikimrKqp::QUERY_ACTION_PREPARE) {
                   if (QueryState->KeepSession && !Settings.LongSession) {
                        ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST)
                            << "Expected KeepSession=false for non-execute requests";
                   }
                }

                if (!IsQueryTypeSupported(type)) {
                    return ForwardRequest(ev);
                }
                break;
            }
            case NKikimrKqp::QUERY_ACTION_BEGIN_TX: {
                YQL_ENSURE(QueryState->HasTxControl(),
                    "Can't perform BEGIN_TX: TxControl isn't set in TQueryRequest");
                const auto& txControl = QueryState->GetTxControl();
                QueryState->Commit = txControl.commit_tx();
                BeginTx(txControl.begin_tx());
                ReplySuccess();
                return;
            }
            case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX: {
                return RollbackTx();
            }
            case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
                return CommitTx();
            // not supported yet
            case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
            case NKikimrKqp::QUERY_ACTION_PARSE:
                return ForwardRequest(ev);

            case NKikimrKqp::QUERY_ACTION_TOPIC:
                return AddOffsetsToTransaction(ctx);
        }

        CompileQuery();
    }

    void AddOffsetsToTransaction(const NActors::TActorContext& ctx) {
        YQL_ENSURE(QueryState);
        if (!PrepareQueryTransaction()) {
            return;
        }

        YQL_ENSURE(QueryState->HasTopicOperations());

        const NKikimrKqp::TTopicOperations& operations = QueryState->GetTopicOperations();

        TMaybe<TString> consumer;
        if (operations.HasConsumer()) {
            consumer = operations.GetConsumer();
        }

        QueryState->TopicOperations = NTopic::TTopicOperations();

        for (auto& topic : operations.GetTopics()) {
            auto path =
                CanonizePath(NPersQueue::GetFullTopicPath(ctx, QueryState->GetDatabase(), topic.path()));

            for (auto& partition : topic.partitions()) {
                if (partition.partition_offsets().empty()) {
                    QueryState->TopicOperations.AddOperation(path, partition.partition_id());
                } else {
                    for (auto& range : partition.partition_offsets()) {
                        YQL_ENSURE(consumer.Defined());

                        QueryState->TopicOperations.AddOperation(path, partition.partition_id(),
                                                                 *consumer,
                                                                 range);
                    }
                }
            }
        }

        auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        navigate->DatabaseName = CanonizePath(QueryState->GetDatabase());
        QueryState->TopicOperations.FillSchemeCacheNavigate(*navigate,
                                                            std::move(consumer));
        navigate->UserToken = QueryState->UserToken;

        Become(&TKqpSessionActor::TopicOpsState);
        ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    }

    void CompileQuery() {
        YQL_ENSURE(QueryState);
        TMaybe<TKqpQueryId> query;
        TMaybe<TString> uid;

        bool keepInCache = false;
        switch (QueryState->GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, QueryState->GetQuery(), QueryState->GetType());
                keepInCache = QueryState->GetQueryKeepInCache() && query->IsSql();
                break;

            case NKikimrKqp::QUERY_ACTION_PREPARE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, QueryState->GetQuery(), QueryState->GetType());
                keepInCache = query->IsSql();
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                uid = QueryState->GetPreparedQuery();
                keepInCache = QueryState->GetQueryKeepInCache();
                break;

            default:
                YQL_ENSURE(false);
        }

        if (query) {
            query->Settings.DocumentApiRestricted = QueryState->IsDocumentApiRestricted;
        }

        auto compileDeadline = QueryState->QueryDeadlines.TimeoutAt;
        if (QueryState->QueryDeadlines.CancelAt) {
            compileDeadline = Min(compileDeadline, QueryState->QueryDeadlines.CancelAt);
        }

        auto compileRequestActor = CreateKqpCompileRequestActor(SelfId(), QueryState->UserToken, uid,
            std::move(query), keepInCache, compileDeadline, Settings.DbCounters,
            QueryState ? std::move(QueryState->Orbit) : NLWTrace::TOrbit(),
            QueryState ? QueryState->KqpSessionSpan.GetTraceId() : NWilson::TTraceId());

        RegisterWithSameMailbox(compileRequestActor);

        Become(&TKqpSessionActor::CompileState);
    }

    void HandleCompile(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    void HandleCompile(TEvKqp::TEvCompileResponse::TPtr& ev) {
        auto compileResult = ev->Get()->CompileResult;
        TTimerGuard timer(this);
        QueryState->Orbit = std::move(ev->Get()->Orbit);
        QueryState->MaxReadType = compileResult->MaxReadType;

        YQL_ENSURE(compileResult);
        YQL_ENSURE(QueryState);

        LWTRACK(KqpSessionQueryCompiled, QueryState->Orbit, TStringBuilder() << compileResult->Status);

        if (compileResult->Status != Ydb::StatusIds::SUCCESS) {
            ReplyQueryCompileError(compileResult);
            return;
        }

        YQL_ENSURE(compileResult->PreparedQuery);
        const ui32 compiledVersion = compileResult->PreparedQuery->GetVersion();
        YQL_ENSURE(compiledVersion == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1,
            "Unexpected prepared query version: " << compiledVersion);

        QueryState->CompileResult = compileResult;
        QueryState->CompileStats.Swap(&ev->Get()->Stats);
        QueryState->PreparedQuery = compileResult->PreparedQuery;

        if (QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE) {
            ReplyPrepareResult(compileResult);
            return;
        }

        if (!PrepareQueryContext()) {
            return;
        }

        Become(&TKqpSessionActor::ExecuteState);

        QueryState->TxCtx->OnBeginQuery();

        if (QueryState->NeedPersistentSnapshot()) {
            return AcquirePersistentSnapshot();
        } else if (NeedSnapshot(*QueryState->TxCtx, *Config, /*rollback*/ false, QueryState->Commit,
            QueryState->PreparedQuery->GetPhysicalQuery()))
        {
            return AcquireMvccSnapshot();
        }

        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void AcquirePersistentSnapshot() {
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = RegisterWithSameMailbox(snapMgr);

        THashSet<TString> tablesSet;
        const auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        for (const auto& phyTx: phyQuery.GetTransactions()) {
            for (const auto& stage: phyTx.GetStages()) {
                for (const auto& tableOp: stage.GetTableOps()) {
                    tablesSet.insert(tableOp.GetTable().GetPath());
                }

                for (const auto& input : stage.GetInputs()) {
                    if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                        tablesSet.insert(input.GetStreamLookup().GetTable().GetPath());
                    }
                }

                for (const auto& source : stage.GetSources()) {
                    if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                        tablesSet.insert(source.GetReadRangesSource().GetTable().GetPath());
                    }
                }
            }
        }
        TVector<TString> tables(tablesSet.begin(), tablesSet.end());

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(tables);
        Send(snapMgrActorId, ev.release());

        QueryState->TxCtx->SnapshotHandle.ManagingActor = snapMgrActorId;
    }

    void DiscardPersistentSnapshot(const IKqpGateway::TKqpSnapshotHandle& handle) {
        if (handle.ManagingActor) { // persistent snapshot was acquired
            Send(handle.ManagingActor, new TEvKqpSnapshot::TEvDiscardSnapshot(handle.Snapshot));
        }
    }

    void AcquireMvccSnapshot() {
        LOG_D("acquire mvcc snapshot");
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = RegisterWithSameMailbox(snapMgr);

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>();
        Send(snapMgrActorId, ev.release());
    }

    Ydb::StatusIds::StatusCode StatusForSnapshotError(NKikimrIssues::TStatusIds::EStatusCode status) {
        switch (status) {
            case NKikimrIssues::TStatusIds::SCHEME_ERROR:
                return Ydb::StatusIds::SCHEME_ERROR;
            case NKikimrIssues::TStatusIds::TIMEOUT:
                return Ydb::StatusIds::TIMEOUT;
            case NKikimrIssues::TStatusIds::OVERLOADED:
                return Ydb::StatusIds::OVERLOADED;
            default:
                // snapshot is acquired before transactions execution, so we can return UNAVAILABLE here
                return Ydb::StatusIds::UNAVAILABLE;
        }
    }

    void HandleExecute(TEvKqpSnapshot::TEvCreateSnapshotResponse::TPtr& ev) {
        TTimerGuard timer(this);
        auto *response = ev->Get();

        if (response->Status != NKikimrIssues::TStatusIds::SUCCESS) {
            auto& issues = response->Issues;
            ReplyQueryError(StatusForSnapshotError(response->Status), "", MessageFromIssues(issues));
            return;
        }
        QueryState->TxCtx->SnapshotHandle.Snapshot = response->Snapshot;

        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void BeginTx(const Ydb::Table::TransactionSettings& settings) {
        QueryState->TxId = UlidGen.Next();
        QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false, AppData()->FunctionRegistry, AppData()->TimeProvider, AppData()->RandomProvider);
        QueryState->QueryData = std::make_shared<TQueryData>(QueryState->TxCtx->TxAlloc);
        QueryState->TxCtx->SetIsolationLevel(settings);
        QueryState->TxCtx->OnBeginQuery();

        if (!Transactions.CreateNew(QueryState->TxId, QueryState->TxCtx)) {
            std::vector<TIssue> issues{
                YqlIssue({}, TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS)};
            ythrow TRequestFail(Ydb::StatusIds::BAD_SESSION,
                                MessageFromIssues(issues))
                << "Too many transactions, current active: " << Transactions.Size()
                << " MaxTxPerSession: " << Transactions.MaxSize();
        }

        Counters->ReportTxCreated(Settings.DbCounters);
        Counters->ReportBeginTransaction(Settings.DbCounters, Transactions.EvictedTx, Transactions.Size(), Transactions.ToBeAbortedSize());
    }

    std::pair<bool, TIssues> ApplyTableOperations(TKqpTransactionContext* txCtx, const NKqpProto::TKqpPhyQuery& query) {
        auto isolationLevel = *txCtx->EffectiveIsolationLevel;
        bool enableImmediateEffects = Config->FeatureFlags.GetEnableKqpImmediateEffects();

        TExprContext ctx;
        bool success = txCtx->ApplyTableOperations(query.GetTableOps(), query.GetTableInfos(), isolationLevel,
            enableImmediateEffects, EKikimrQueryType::Dml, ctx);
        return {success, ctx.IssueManager.GetIssues()};
    }

    bool PrepareQueryTransaction() {
        if (QueryState->HasTxControl()) {
            const auto& txControl = QueryState->GetTxControl();

            QueryState->Commit = txControl.commit_tx();
            switch (txControl.tx_selector_case()) {
                case Ydb::Table::TransactionControl::kTxId: {
                    auto txId = TTxId::FromString(txControl.tx_id());
                    auto txCtx = Transactions.Find(txId);
                    if (!txCtx) {
                        ReplyTransactionNotFound(txControl.tx_id());
                        return false;
                    }
                    QueryState->TxCtx = txCtx;
                    QueryState->QueryData = std::make_shared<TQueryData>(QueryState->TxCtx->TxAlloc);
                    QueryState->TxId = txId;
                    break;
                }
                case Ydb::Table::TransactionControl::kBeginTx: {
                    BeginTx(txControl.begin_tx());
                    break;
               }
               case Ydb::Table::TransactionControl::TX_SELECTOR_NOT_SET:
                    ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST)
                        << "wrong TxControl: tx_selector must be set";
                    break;
            }
        } else {
            QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false, AppData()->FunctionRegistry,
                AppData()->TimeProvider, AppData()->RandomProvider);
            QueryState->QueryData = std::make_shared<TQueryData>(QueryState->TxCtx->TxAlloc);
            QueryState->TxCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;
        }

        return true;
    }

    bool PrepareQueryContext() {
        YQL_ENSURE(QueryState);
        if (!PrepareQueryTransaction()) {
            return false;
        }

        const NKqpProto::TKqpPhyQuery& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        auto [success, issues] = ApplyTableOperations(QueryState->TxCtx.Get(), phyQuery);
        if (!success) {
            YQL_ENSURE(!issues.Empty());
            ReplyQueryError(GetYdbStatus(issues), "", MessageFromIssues(issues));
            return false;
        }

        auto action = QueryState->GetAction();
        auto type = QueryState->GetType();

        if (action == NKikimrKqp::QUERY_ACTION_EXECUTE && type == NKikimrKqp::QUERY_TYPE_SQL_DML
            || action == NKikimrKqp::QUERY_ACTION_EXECUTE && type == NKikimrKqp::QUERY_TYPE_AST_DML)
        {
            type = NKikimrKqp::QUERY_TYPE_PREPARED_DML;
            action = NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED;
        }

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                YQL_ENSURE(
                    type == NKikimrKqp::QUERY_TYPE_SQL_SCAN ||
                    type == NKikimrKqp::QUERY_TYPE_AST_SCAN ||
                    type == NKikimrKqp::QUERY_TYPE_SQL_QUERY
                );
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                YQL_ENSURE(type == NKikimrKqp::QUERY_TYPE_PREPARED_DML);
                break;

            default:
                YQL_ENSURE(false, "Unexpected query action: " << action);
        }

        ParseParameters(QueryState->GetParameters());
        ParseParameters(QueryState->GetYdbParameters());
        return true;
    }

    void ParseParameters(const google::protobuf::Map<TBasicString<char>, Ydb::TypedValue>& params) {
        if (!params.size()){
            return;
        }

        for(const auto& [name, param] : params) {
            try {
                auto success = QueryState->QueryData->AddTypedValueParam(name, param);
                YQL_ENSURE(success, "Duplicate parameter: " << name);
            } catch(const yexception& ex) {
                ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
            }
        }
    }

    void ParseParameters(const NKikimrMiniKQL::TParams& parameters) {
        if (!parameters.HasType()) {
            return;
        }

        YQL_ENSURE(parameters.GetType().GetKind() == NKikimrMiniKQL::Struct, "Expected struct as query parameters type");
        auto& structType = parameters.GetType().GetStruct();
        for (ui32 i = 0; i < structType.MemberSize(); ++i) {
            const auto& memberName = structType.GetMember(i).GetName();
            YQL_ENSURE(i < parameters.GetValue().StructSize(), "Missing value for parameter: " << memberName);
            auto success = QueryState->QueryData->AddMkqlParam(memberName, structType.GetMember(i).GetType(), parameters.GetValue().GetStruct(i));
            YQL_ENSURE(success, "Duplicate parameter: " << memberName);
        }
    }

    void ReplyPrepareResult(const TKqpCompileResult::TConstPtr& compileResult) {
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, QueryResponse->Record);

        auto ru = NRuCalc::CpuTimeToUnit(TDuration::MicroSeconds(QueryState->CompileStats.GetCpuTimeUs()));
        auto& record = QueryResponse->Record.GetRef();
        record.SetConsumedRu(ru);

        Cleanup(IsFatalError(record.GetYdbStatus()));
    }

    IKqpGateway::TExecPhysicalRequest PrepareBaseRequest(TKqpQueryState *queryState, TTxAllocatorState::TPtr alloc) {
        IKqpGateway::TExecPhysicalRequest request(alloc);

        if (queryState) {
            auto now = TAppData::TimeProvider->Now();
            request.Timeout = queryState->QueryDeadlines.TimeoutAt - now;
            if (auto cancelAt = queryState->QueryDeadlines.CancelAt) {
                request.CancelAfter = cancelAt - now;
            }

            request.StatsMode = queryState->GetStatsMode();
        }

        const auto& limits = GetQueryLimits(Settings);
        request.MaxAffectedShards = limits.PhaseLimits.AffectedShardsLimit;
        request.TotalReadSizeLimitBytes = limits.PhaseLimits.TotalReadSizeLimitBytes;
        request.MkqlMemoryLimit = limits.PhaseLimits.ComputeNodeMemoryLimitBytes;
        return request;
    }


    IKqpGateway::TExecPhysicalRequest PreparePureRequest(TKqpQueryState *queryState) {
        auto request = PrepareBaseRequest(queryState, queryState->TxCtx->TxAlloc);
        request.NeedTxId = false;
        return request;
    }

    IKqpGateway::TExecPhysicalRequest PreparePhysicalRequest(TKqpQueryState *queryState,
        TTxAllocatorState::TPtr alloc)
    {
        auto request = PrepareBaseRequest(queryState, alloc);

        if (queryState) {
            request.Snapshot = queryState->TxCtx->GetSnapshot();
            request.IsolationLevel = *queryState->TxCtx->EffectiveIsolationLevel;
        } else {
            request.IsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
        }

        return request;
    }

    IKqpGateway::TExecPhysicalRequest PrepareScanRequest(TKqpQueryState *queryState) {
        auto request = PrepareBaseRequest(queryState, queryState->TxCtx->TxAlloc);

        request.MaxComputeActors = Config->_KqpMaxComputeActors.Get().GetRef();
        request.DisableLlvmForUdfStages = Config->DisableLlvmForUdfStages();
        YQL_ENSURE(queryState);
        bool enableLlvm = queryState->PreparedQuery->GetEnableLlvm().value_or(true);
        request.LlvmEnabled = enableLlvm && IsSqlQuery(queryState->GetType());
        request.Snapshot = queryState->TxCtx->GetSnapshot();

        return request;
    }

    IKqpGateway::TExecPhysicalRequest PrepareGenericRequest(TKqpQueryState *queryState) {
        auto request = PrepareBaseRequest(queryState, queryState->TxCtx->TxAlloc);

        if (queryState) {
            request.Snapshot = queryState->TxCtx->GetSnapshot();
            request.IsolationLevel = *queryState->TxCtx->EffectiveIsolationLevel;
        } else {
            request.IsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
        }

        return request;
    }

    IKqpGateway::TExecPhysicalRequest PrepareRequest(const TKqpPhyTxHolder::TConstPtr& tx, bool pure,
        TKqpQueryState *queryState)
    {
        if (pure) {
            YQL_ENSURE(tx);
            return PreparePureRequest(QueryState.get());
        }

        if (!tx) {
            return PreparePhysicalRequest(QueryState.get(), queryState->TxCtx->TxAlloc);
        }

        switch (tx->GetType()) {
            case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                // TODO: Compute is always pure, should not depend on number of stages.
                return PreparePhysicalRequest(QueryState.get(), queryState->TxCtx->TxAlloc);
            case NKqpProto::TKqpPhyTx::TYPE_DATA:
                return PreparePhysicalRequest(QueryState.get(), queryState->TxCtx->TxAlloc);
            case NKqpProto::TKqpPhyTx::TYPE_SCAN:
                return PrepareScanRequest(QueryState.get());
            case NKqpProto::TKqpPhyTx::TYPE_GENERIC:
                return PrepareGenericRequest(QueryState.get());
            default:
                YQL_ENSURE(false, "Unexpected physical tx type: " << (int)tx->GetType());
        }
    }

    void ValidateParameter(const TString& name, const NKikimrMiniKQL::TType& type) {
        auto& txCtx = QueryState->TxCtx;
        YQL_ENSURE(txCtx);
        auto parameterType = QueryState->QueryData->GetParameterType(name);
        if (!parameterType) {
            if (type.GetKind() == NKikimrMiniKQL::ETypeKind::Optional) {
                NKikimrMiniKQL::TValue value;
                QueryState->QueryData->AddMkqlParam(name, type, value);
                return;
            }

            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << "Missing value for parameter: " << name;
        }

        auto pType = ImportTypeFromProto(type, txCtx->TxAlloc->TypeEnv);
        if (pType == nullptr || !parameterType->IsSameType(*pType)) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << "Parameter " << name
                << " type mismatch, expected: " << type << ", actual: " << *parameterType;
        }
    }

    TQueryData::TPtr PrepareParameters(const TKqpPhyTxHolder::TConstPtr& tx) {
        for (const auto& paramDesc : QueryState->PreparedQuery->GetParameters()) {
            ValidateParameter(paramDesc.GetName(), paramDesc.GetType());
        }

        try {
            for(const auto& paramBinding: tx->GetParamBindings()) {
                QueryState->QueryData->MaterializeParamValue(true, paramBinding);
            }
        } catch (const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        }
        return QueryState->QueryData;
    }

    bool ShouldAcquireLocks(const NKqpProto::TKqpPhyQuery* query) {
        auto& txCtx = *QueryState->TxCtx;

        if (*txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE) {
            return false;
        }

        if (txCtx.Locks.Broken()) {
            return false;  // Do not acquire locks after first lock issue
        }

        if (!txCtx.DeferredEffects.Empty()) {
            return true; // Acquire locks in read write tx
        }

        YQL_ENSURE(query);
        for (auto& tx : query->GetTransactions()) {
            if (tx.GetHasEffects()) {
                return true; // Acquire locks in read write tx
            }
        }

        if (!QueryState->Commit) {
            return true; // Is not a commit tx
        }

        if (txCtx.GetSnapshot().IsValid()) {
            return false; // It is a read only tx with snapshot, no need to acquire locks
        }

        return true;
    }

    TQueryData::TPtr CreateKqpValueMap(const TKqpPhyTxHolder::TConstPtr& tx) {
        for (const auto& paramBinding : tx->GetParamBindings()) {
            QueryState->QueryData->MaterializeParamValue(true, paramBinding);
        }
        return QueryState->QueryData;
    }

    bool CheckTransacionLocks() {
        auto& txCtx = *QueryState->TxCtx;
        if (!txCtx.DeferredEffects.Empty() && txCtx.Locks.Broken()) {
            ReplyQueryError(Ydb::StatusIds::ABORTED, "tx has deferred effects, but locks are broken",
                MessageFromIssues(std::vector<TIssue>{txCtx.Locks.GetIssue()}));
            return false;
        }
        return true;
    }

    bool CheckTopicOperations() {
        auto& txCtx = *QueryState->TxCtx;

        if (txCtx.TopicOperations.IsValid()) {
            return true;
        }

        std::vector<TIssue> issues {
            YqlIssue({}, TIssuesIds::KIKIMR_BAD_REQUEST, "Incorrect offset ranges in the transaction.")
        };
        ReplyQueryError(Ydb::StatusIds::ABORTED, "incorrect offset ranges in the tx",
                        MessageFromIssues(issues));

        return false;
    }

    void ExecuteOrDefer() {
        auto& txCtx = *QueryState->TxCtx;

        bool haveWork = QueryState->PreparedQuery &&
                QueryState->CurrentTx < QueryState->PreparedQuery->GetPhysicalQuery().TransactionsSize()
                    || QueryState->Commit && !QueryState->Commited;

        if (!haveWork) {
            ReplySuccess();
            return;
        }

        const auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();

        auto tx = QueryState->PreparedQuery->GetPhyTxOrEmpty(QueryState->CurrentTx);
        if (!Config->FeatureFlags.GetEnableKqpImmediateEffects()) {
            while (tx && tx->GetHasEffects()) {
                YQL_ENSURE(txCtx.AddDeferredEffect(tx, CreateKqpValueMap(tx)));
                LWTRACK(KqpSessionPhyQueryDefer, QueryState->Orbit, QueryState->CurrentTx);
                if (QueryState->CurrentTx + 1 < phyQuery.TransactionsSize()) {
                    ++QueryState->CurrentTx;
                    tx = QueryState->PreparedQuery->GetPhyTx(QueryState->CurrentTx);
                } else {
                    tx = nullptr;
                    break;
                }
            }
        }

        if (!CheckTransacionLocks() || !CheckTopicOperations()) {
            return;
        }

        bool commit = false;
        if (QueryState->Commit && Config->FeatureFlags.GetEnableKqpImmediateEffects() && phyQuery.GetHasUncommittedChangesRead()) {
            // every phy tx should acquire LockTxId, so commit is sent separately at the end
            commit = QueryState->CurrentTx >= phyQuery.TransactionsSize();
        } else if (QueryState->Commit && QueryState->CurrentTx >= phyQuery.TransactionsSize() - 1) {
            if (!tx) {
                // no physical transactions left, perform commit
                commit = true;
            } else {
                // we can merge commit with last tx only for read-only transactions
                commit = txCtx.DeferredEffects.Empty();
            }
        }

        if (tx || commit) {
            bool replied = ExecutePhyTx(&phyQuery, tx, commit);
            if (!replied) {
                ++QueryState->CurrentTx;
            }
        } else {
            ReplySuccess();
        }
    }

    bool ExecutePhyTx(const NKqpProto::TKqpPhyQuery* query, const TKqpPhyTxHolder::TConstPtr& tx, bool commit) {
        auto& txCtx = *QueryState->TxCtx;

        bool pure = tx && tx->IsPureTx();
        auto request = PrepareRequest(tx, pure, QueryState.get());

        LOG_D("ExecutePhyTx, tx: " << (void*)tx.get() << " commit: " << commit
                << " txCtx.DeferredEffects.size(): " << txCtx.DeferredEffects.Size());

        if (!CheckTopicOperations()) {
            return true;
        }

        // TODO Handle timeouts -- request.Timeout, request.CancelAfter

        if (tx) {
            switch (tx->GetType()) {
                case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                case NKqpProto::TKqpPhyTx::TYPE_DATA:
                case NKqpProto::TKqpPhyTx::TYPE_SCAN:
                case NKqpProto::TKqpPhyTx::TYPE_GENERIC:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected physical tx type in data query: " << (ui32)tx->GetType());
            }

            request.Transactions.emplace_back(tx, PrepareParameters(tx));
            txCtx.HasImmediateEffects = txCtx.HasImmediateEffects || tx->GetHasEffects();
        } else {
            YQL_ENSURE(commit);

            if (!txCtx.TxHasEffects() && !txCtx.Locks.HasLocks() && !txCtx.TopicOperations.HasOperations()) {
                ReplySuccess();
                return true;
            }
        }

        if (pure) {
            if (QueryState) {
                request.Orbit = std::move(QueryState->Orbit);
            }
            request.TraceId = QueryState ? QueryState->KqpSessionSpan.GetTraceId() : NWilson::TTraceId();
            auto response = ExecutePure(std::move(request), RequestCounters, SelfId());
            ++QueryState->CurrentTx;
            ProcessExecuterResult(response.get());
            return true;
        } else if (commit) {
            QueryState->Commited = true;

            for (const auto& effect : txCtx.DeferredEffects) {
                YQL_ENSURE(effect.PhysicalTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_DATA);
                request.Transactions.emplace_back(effect.PhysicalTx, effect.Params);

                LOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction,"
                       << " current Transactions.size(): " << request.Transactions.size());
            }

            if (!txCtx.DeferredEffects.Empty()) {
                request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
            }

            if (txCtx.Locks.HasLocks() || txCtx.TopicOperations.HasReadOperations()) {
                request.ValidateLocks = !txCtx.GetSnapshot().IsValid() || txCtx.TxHasEffects() ||
                    txCtx.TopicOperations.HasReadOperations();
                request.EraseLocks = true;

                LOG_D("TExecPhysicalRequest, tx has locks, ValidateLocks: " << request.ValidateLocks
                        << " EraseLocks: " << request.EraseLocks);

                for (auto& [lockId, lock] : txCtx.Locks.LocksMap) {
                    auto dsLock = ExtractLock(lock.GetValueRef(txCtx.Locks.LockType));
                    request.DataShardLocks[dsLock.GetDataShard()].emplace_back(dsLock);
                }

            }

            request.TopicOperations = std::move(txCtx.TopicOperations);
        } else if (ShouldAcquireLocks(query)) {
            request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();
        }

        LWTRACK(KqpSessionPhyQueryProposeTx,
            QueryState->Orbit,
            QueryState->CurrentTx,
            request.Transactions.size(),
            txCtx.Locks.Size(),
            request.AcquireLocksTxId.Defined());

        SendToExecuter(std::move(request));

        return false;
    }

    void SendToExecuter(IKqpGateway::TExecPhysicalRequest&& request, bool isRollback = false) {
        if (QueryState) {
            request.Orbit = std::move(QueryState->Orbit);
        }
        request.PerRequestDataSizeLimit = RequestControls.PerRequestDataSizeLimit;
        request.MaxShardCount = RequestControls.MaxShardCount;
        request.TraceId = QueryState ? QueryState->KqpSessionSpan.GetTraceId() : NWilson::TTraceId();
        LOG_D("Sending to Executer TraceId: " << request.TraceId.GetTraceId() << " " << request.TraceId.GetSpanIdSize());

        auto executerActor = CreateKqpExecuter(std::move(request), Settings.Database,
            QueryState ? QueryState->UserToken : TIntrusiveConstPtr<NACLib::TUserToken>(),
            RequestCounters, Settings.Service.GetExecuterRetriesConfig());

        auto exId = RegisterWithSameMailbox(executerActor);
        LOG_D("Created new KQP executer: " << exId);
        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
        if (!isRollback) {
            Y_VERIFY(!ExecuterId);
            ExecuterId = exId;
        }
    }


    template<typename T>
    void HandleNoop(T&) {
    }

    void HandleTxResponse(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        if (ev->Sender == ExecuterId) {
            auto& response = ev->Get()->Record.GetResponse();
            TIssues issues;
            IssuesFromMessage(response.GetIssues(), issues);

            auto err = TStringBuilder() << "Got response from our executor: " << ev->Sender
            << ", Status: " << ev->Get()->Record.GetResponse().GetStatus()
            << ", Issues: " << issues.ToString()
            <<  " while we are in " << CurrentStateFuncName();
            LOG_E(err);
            YQL_ENSURE(false, "" << err);
        }
    }

    void HandleExecute(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    bool MergeLocksWithTxResult(const NKikimrKqp::TExecuterTxResult& result) {
        if (result.HasLocks()) {
            auto& txCtx = QueryState->TxCtx;
            const auto& locks = result.GetLocks();
            auto [success, issues] = MergeLocks(locks.GetType(), locks.GetValue(), *txCtx);
            if (!success) {
                if (!txCtx->GetSnapshot().IsValid() || txCtx->TxHasEffects()) {
                    ReplyQueryError(Ydb::StatusIds::ABORTED,  "Error while locks merge",
                        MessageFromIssues(issues));
                    return false;
                }

                if (txCtx->GetSnapshot().IsValid()) {
                    txCtx->Locks.MarkBroken(issues.back());
                }
            }
        }

        return true;
    }

    void InvalidateQuery() {
        if (QueryState->CompileResult) {
            auto invalidateEv = MakeHolder<TEvKqp::TEvCompileInvalidateRequest>(
                QueryState->CompileResult->Uid, Settings.DbCounters);

            Send(MakeKqpCompileServiceID(SelfId().NodeId()), invalidateEv.Release());
        }
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        // outdated response from dead executer.
        // it this case we should just ignore the event.
        if (ExecuterId != ev->Sender)
            return;

        TTimerGuard timer(this);
        ProcessExecuterResult(ev->Get());
    }

    void ProcessExecuterResult(TEvKqpExecuter::TEvTxResponse* ev) {
        QueryState->Orbit = std::move(ev->Orbit);

        auto* response = ev->Record.MutableResponse();

        LOG_D("TEvTxResponse, CurrentTx: " << QueryState->CurrentTx
            << "/" << (QueryState->PreparedQuery ? QueryState->PreparedQuery->GetPhysicalQuery().TransactionsSize() : 0)
            << " response.status: " << response->GetStatus());

        ExecuterId = TActorId{};

        if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
            LOG_I("TEvTxResponse has non-success status, CurrentTx: " << QueryState->CurrentTx);

            auto status = response->GetStatus();
            TIssues issues;
            IssuesFromMessage(response->GetIssues(), issues);

            // Invalidate query cache on scheme/internal errors
            switch (status) {
                case Ydb::StatusIds::SCHEME_ERROR:
                case Ydb::StatusIds::INTERNAL_ERROR:
                    InvalidateQuery();
                    issues.AddIssue(YqlIssue(TPosition(), TIssuesIds::KIKIMR_QUERY_INVALIDATED,
                        TStringBuilder() << "Query invalidated on scheme/internal error."));

                    // SCHEME_ERROR during execution is a soft (retriable) error, we abort query execution,
                    // invalidate query cache, and return ABORTED as retriable status.
                    if (status == Ydb::StatusIds::SCHEME_ERROR) {
                        status = Ydb::StatusIds::ABORTED;
                    }

                    break;

                default:
                    break;
            }

            ReplyQueryError(status, "", MessageFromIssues(issues));
            return;
        }

        YQL_ENSURE(QueryState);
        LWTRACK(KqpSessionPhyQueryTxResponse, QueryState->Orbit, QueryState->CurrentTx, ev->ResultRowsCount);

        auto& executerResults = *response->MutableResult();
        {
            auto g = QueryState->QueryData->TypeEnv().BindAllocator();
            QueryState->QueryData->AddTxResults(std::move(ev->GetTxResults()));
            QueryState->QueryData->AddTxHolders(std::move(ev->GetTxHolders()));
        }

        if (ev->LockHandle) {
            QueryState->TxCtx->Locks.LockHandle = std::move(ev->LockHandle);
        }

        if (!MergeLocksWithTxResult(executerResults)) {
            return;
        }

        if (executerResults.HasStats()) {
            auto* exec = QueryState->Stats.AddExecutions();
            exec->Swap(executerResults.MutableStats());
        }

        ExecuteOrDefer();
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        YQL_ENSURE(QueryState && QueryState->RequestActorId);
        TlsActivationContext->Send(ev->Forward(QueryState->RequestActorId));
    }

    void HandleExecute(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        YQL_ENSURE(QueryState);
        // note: RequestActorId may be TActorId{};
        TlsActivationContext->Send(ev->Forward(QueryState->RequestActorId));
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        TlsActivationContext->Send(ev->Forward(ExecuterId));
    }

    void HandleExecute(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;

        ExecuterId = TActorId{};

        const auto& issues = ev->Get()->GetIssues();
        LOG_I("Got TEvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode()));
        ReplyQueryError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), "Got AbortExecution", MessageFromIssues(issues));
    }

    void CollectSystemViewQueryStats(const NKqpProto::TKqpStatsQuery* stats, TDuration queryDuration,
        const TString& database, ui64 requestUnits)
    {
        auto type = QueryState->GetType();
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
            case NKikimrKqp::QUERY_TYPE_SQL_QUERY: {
                TString text = QueryState->ExtractQueryText();
                if (IsQueryAllowedToLog(text)) {
                    auto userSID = QueryState->UserToken->GetUserSID();
                    NSysView::CollectQueryStats(TlsActivationContext->AsActorContext(), stats, queryDuration, text,
                        userSID, QueryState->ParametersSize, database, type, requestUnits);
                }
                break;
            }
            default:
                break;
        }
    }

    void FillStats(NKikimrKqp::TEvQueryResponse* record) {
        auto *response = record->MutableResponse();
        auto* stats = &QueryState->Stats;

        stats->SetDurationUs((TInstant::Now() - QueryState->StartTime).MicroSeconds());
        stats->SetWorkerCpuTimeUs(QueryState->GetCpuTime().MicroSeconds());
        if (QueryState->CompileResult) {
            stats->MutableCompilation()->Swap(&QueryState->CompileStats);
        }

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        YQL_ENSURE(QueryState);
        if (IsExecuteAction(QueryState->GetAction())) {
            auto ru = NRuCalc::CalcRequestUnit(*stats);
            record->SetConsumedRu(ru);

            auto now = TInstant::Now();
            auto queryDuration = now - QueryState->StartTime;
            CollectSystemViewQueryStats(stats, queryDuration, QueryState->GetDatabase(), ru);
            SlowLogQuery(TlsActivationContext->AsActorContext(), Config.Get(), requestInfo, queryDuration,
                record->GetYdbStatus(), QueryState->UserToken, QueryState->ParametersSize, record,
                [this]() { return this->QueryState->ExtractQueryText(); });
        }

        if (QueryState->CollectStatsDefined()) {
            response->SetQueryPlan(SerializeAnalyzePlan(*stats));
            response->MutableQueryStats()->Swap(stats);
        }
    }

    template<class TEvRecord>
    void AddTrailingInfo(TEvRecord& record) {
        if (ShutdownState) {
            LOG_D("session is closing, set trailing metadata to request session shutdown");
            record.SetWorkerIsClosing(true);
        }
    }

    void FillTxInfo(NKikimrKqp::TQueryResponse* response) {
        YQL_ENSURE(QueryState);
        if (QueryState->Commit) {
            if (auto ctx = Transactions.ReleaseTransaction(QueryState->TxId)) {
                Transactions.AddToBeAborted(std::move(ctx));
            }
            QueryState->TxId = TTxId();
        }
        response->MutableTxMeta()->set_id(QueryState->TxId.GetHumanStr());

        if (QueryState->TxCtx) {
            auto txInfo = QueryState->TxCtx->GetInfo();
            LOG_I("txInfo "
                << " Status: " << txInfo.Status
                << " Kind: " << txInfo.Kind
                << " TotalDuration: " << txInfo.TotalDuration.SecondsFloat()*1e3
                << " ServerDuration: " << txInfo.ServerDuration.SecondsFloat()*1e3
                << " QueriesCount: " << txInfo.QueriesCount);
            Counters->ReportTransaction(Settings.DbCounters, txInfo);
        }
    }

    void UpdateQueryExecutionCountes() {
        auto now = TInstant::Now();
        auto queryDuration = now - QueryState->StartTime;

        Counters->ReportQueryLatency(Settings.DbCounters, QueryState->GetAction(), queryDuration);

        if (QueryState->MaxReadType == ETableReadType::FullScan) {
            Counters->ReportQueryWithFullScan(Settings.DbCounters);
        } else if (QueryState->MaxReadType == ETableReadType::Scan) {
            Counters->ReportQueryWithRangeScan(Settings.DbCounters);
        }

        auto& stats = QueryState->Stats;

        ui32 affectedShardsCount = 0;
        ui64 readBytesCount = 0;
        ui64 readRowsCount = 0;
        for (const auto& exec : stats.GetExecutions()) {
            for (const auto& table : exec.GetTables()) {
                affectedShardsCount = std::max(affectedShardsCount, table.GetAffectedPartitions());
                readBytesCount += table.GetReadBytes();
                readRowsCount += table.GetReadRows();
            }
        }

        Counters->ReportQueryAffectedShards(Settings.DbCounters, affectedShardsCount);
        Counters->ReportQueryReadRows(Settings.DbCounters, readRowsCount);
        Counters->ReportQueryReadBytes(Settings.DbCounters, readBytesCount);
        Counters->ReportQueryReadSets(Settings.DbCounters, stats.GetReadSetsCount());
        Counters->ReportQueryMaxShardReplySize(Settings.DbCounters, stats.GetMaxShardReplySize());
        Counters->ReportQueryMaxShardProgramSize(Settings.DbCounters, stats.GetMaxShardProgramSize());
    }

    void ReplySuccess() {
        auto resEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        std::shared_ptr<google::protobuf::Arena> arena(new google::protobuf::Arena());
        resEv->Record.Realloc(arena);
        auto *record = &resEv->Record.GetRef();
        auto *response = record->MutableResponse();

        if (QueryState->CompileResult) {
            AddQueryIssues(*response, QueryState->CompileResult->Issues);
        }

        FillStats(record);

        YQL_ENSURE(QueryState);
        if (QueryState->TxCtx) {
            QueryState->TxCtx->OnEndQuery();
        }

        if (QueryState->Commit) {
            ResetTxState();
        }

        FillTxInfo(response);

        UpdateQueryExecutionCountes();

        bool replyQueryId = false;
        bool replyQueryParameters = false;
        switch (QueryState->GetAction()) {
            case NKikimrKqp::QUERY_ACTION_PREPARE:
                replyQueryId = true;
                replyQueryParameters = true;
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                replyQueryParameters = replyQueryId = QueryState->GetQueryKeepInCache();
                break;

            case NKikimrKqp::QUERY_ACTION_PARSE:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
                replyQueryParameters = true;
                break;

            default:
                break;
        }

        if (replyQueryParameters) {
            YQL_ENSURE(QueryState->PreparedQuery);
            response->MutableQueryParameters()->CopyFrom(
                    QueryState->PreparedQuery->GetParameters());
        }

        if (replyQueryId) {
            TString queryId;
            if (QueryState->CompileResult) {
                queryId = QueryState->CompileResult->Uid;
            }
            response->SetPreparedQuery(queryId);
        }

        bool useYdbResponseFormat = QueryState->GetUsePublicResponseDataFormat();
        // Result for scan query is sent directly to target actor.
        if (QueryState->PreparedQuery && !QueryState->IsStreamResult()) {
            auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            for (size_t i = 0; i < phyQuery.ResultBindingsSize(); ++i) {
                auto& rb = phyQuery.GetResultBindings(i);
                auto txIndex = rb.GetTxResultBinding().GetTxIndex();
                auto resultIndex = rb.GetTxResultBinding().GetResultIndex();

                YQL_ENSURE(QueryState->QueryData->HasResult(txIndex, resultIndex));
                auto g = QueryState->QueryData->TypeEnv().BindAllocator();
                auto* protoRes = QueryState->QueryData->GetMkqlTxResult(txIndex, resultIndex, arena.get());
                std::optional<IDataProvider::TFillSettings> fillSettings;
                if (QueryState->PreparedQuery->ResultsSize()) {
                    YQL_ENSURE(phyQuery.ResultBindingsSize() == QueryState->PreparedQuery->ResultsSize(), ""
                            << phyQuery.ResultBindingsSize() << " != " << QueryState->PreparedQuery->ResultsSize());
                    const auto& result = QueryState->PreparedQuery->GetResults(i);
                    if (result.GetRowsLimit()) {
                        fillSettings = FillSettings;
                        fillSettings->RowsLimitPerWrite = result.GetRowsLimit();
                    }
                }
                auto* finalResult = KikimrResultToProto(*protoRes, {}, fillSettings.value_or(FillSettings), arena.get());
                if (useYdbResponseFormat) {
                    ConvertKqpQueryResultToDbResult(*finalResult, response->AddYdbResults());
                } else {
                    response->AddResults()->Swap(finalResult);
                }
            }
        }

        resEv->Record.GetRef().SetYdbStatus(Ydb::StatusIds::SUCCESS);
        LOG_D("Create QueryResponse for action: " << QueryState->GetAction() << " with SUCCESS status");

        QueryResponse = std::move(resEv);

        Cleanup();
    }

    void ReplyQueryCompileError(const TKqpCompileResult::TConstPtr& compileResult) {
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, QueryResponse->Record);

        auto txId = TTxId();
        if (QueryState->HasTxControl()) {
            const auto& txControl = QueryState->GetTxControl();
            if (txControl.tx_selector_case() == Ydb::Table::TransactionControl::kTxId) {
                txId = TTxId::FromString(txControl.tx_id());
            }
        }

        LOG_W("ReplyQueryCompileError, status " << compileResult->Status << " remove tx with tx_id: " << txId.GetHumanStr());
        if (auto ctx = Transactions.ReleaseTransaction(txId)) {
            ctx->Invalidate();
            Transactions.AddToBeAborted(std::move(ctx));
        }

        auto* record = &QueryResponse->Record.GetRef();
        FillTxInfo(record->MutableResponse());
        record->SetConsumedRu(1);

        Cleanup(IsFatalError(record->GetYdbStatus()));
    }

    void ReplyProcessError(const TActorId& sender, ui64 proxyRequestId, Ydb::StatusIds::StatusCode ydbStatus,
            const TString& message)
    {
        LOG_W("Reply process error, msg: " << message);

        auto response = TEvKqp::TEvProcessResponse::Error(ydbStatus, message);

        AddTrailingInfo(response->Record);
        Send(sender, response.Release(), 0, proxyRequestId);
    }

    void ReplyBusy(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto busyStatus = Settings.Service.GetUseSessionBusyStatus()
            ? Ydb::StatusIds::SESSION_BUSY
            : Ydb::StatusIds::PRECONDITION_FAILED;

        ReplyProcessError(ev->Sender, proxyRequestId, busyStatus, "Pending previous query completion");
    }

    static bool IsFatalError(const Ydb::StatusIds::StatusCode status) {
        switch (status) {
            case Ydb::StatusIds::INTERNAL_ERROR:
            case Ydb::StatusIds::BAD_SESSION:
                return true;
            default:
                return false;
        }
    }

    void Reply() {
        YQL_ENSURE(QueryState);
        YQL_ENSURE(Counters);

        auto& record = QueryResponse->Record.GetRef();
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        AddTrailingInfo(record);

        if (QueryState->KeepSession) {
            response.SetSessionId(SessionId);
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            if (QueryState && QueryState->KqpSessionSpan) {
                QueryState->KqpSessionSpan.EndOk();
            }
            LWTRACK(KqpSessionReplySuccess, QueryState->Orbit, record.GetArena() ? record.GetArena()->SpaceUsed() : 0);
        } else {
            if (QueryState && QueryState->KqpSessionSpan) {
                QueryState->KqpSessionSpan.EndError(response.DebugString());
            }
            LWTRACK(KqpSessionReplyError, QueryState->Orbit, TStringBuilder() << status);
        }
        Send(QueryState->Sender, QueryResponse.release(), 0, QueryState->ProxyRequestId);
        LOG_D("Sent query response back to proxy, proxyRequestId: " << QueryState->ProxyRequestId
            << ", proxyId: " << QueryState->Sender.ToString());

        if (IsFatalError(status)) {
            LOG_N("SessionActor destroyed due to " << status);
            Counters->ReportSessionActorClosedError(Settings.DbCounters);
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
        }
    }

    void HandleReady(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        LOG_I("Session closed due to explicit close event");
        Counters->ReportSessionActorClosedRequest(Settings.DbCounters);
        CleanupAndPassAway();
    }

    void HandleCompile(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        YQL_ENSURE(QueryState);
        ReplyQueryError(Ydb::StatusIds::BAD_SESSION,
                "Request cancelled due to explicit session close request");
        Counters->ReportSessionActorClosedRequest(Settings.DbCounters);
    }

    void HandleExecute(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        YQL_ENSURE(QueryState);
        QueryState->KeepSession = false;
    }

    void HandleCleanup(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        YQL_ENSURE(CleanupCtx);
        if (!CleanupCtx->Final) {
            YQL_ENSURE(QueryState);
            QueryState->KeepSession = false;
        }
    }

    void Handle(TEvKqp::TEvInitiateSessionShutdown::TPtr& ev) {
        if (!ShutdownState) {
            LOG_N("Started session shutdown");
            ShutdownState = TSessionShutdownState(ev->Get()->SoftTimeoutMs, ev->Get()->HardTimeoutMs);
            ScheduleNextShutdownTick();
        }
    }

    void ScheduleNextShutdownTick() {
        Schedule(TDuration::MilliSeconds(ShutdownState->GetNextTickMs()), new TEvKqp::TEvContinueShutdown());
    }

    void Handle(TEvKqp::TEvContinueShutdown::TPtr&) {
        YQL_ENSURE(ShutdownState);
        ShutdownState->MoveToNextState();
        if (ShutdownState->HardTimeoutReached()) {
            LOG_N("Reached hard shutdown timeout");
            Send(SelfId(), new TEvKqp::TEvCloseSessionRequest());
        } else {
            ScheduleNextShutdownTick();
            LOG_I("Schedule next shutdown tick");
        }
    }

    void SendRollbackRequest(TKqpTransactionContext* txCtx) {
        if (QueryState) {
            LWTRACK(KqpSessionSendRollback, QueryState->Orbit, QueryState->CurrentTx);
        }

        auto allocPtr = std::make_shared<TTxAllocatorState>(AppData()->FunctionRegistry,
            AppData()->TimeProvider, AppData()->RandomProvider);
        auto request = PreparePhysicalRequest(nullptr, allocPtr);

        request.EraseLocks = true;
        request.ValidateLocks = false;

        // Should tx with empty LocksMap be aborted?
        for (auto& [lockId, lock] : txCtx->Locks.LocksMap) {
            auto dsLock = ExtractLock(lock.GetValueRef(txCtx->Locks.LockType));
            request.DataShardLocks[dsLock.GetDataShard()].emplace_back(dsLock);
        }

        SendToExecuter(std::move(request), true);
    }

    void ResetTxState() {
        if (QueryState->TxCtx) {
            QueryState->TxCtx->ClearDeferredEffects();
            QueryState->TxCtx->Locks.Clear();
            QueryState->TxCtx->Finish();
        }
    }

    void HandleCleanup(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    void CleanupAndPassAway() {
        Cleanup(true);
    }

    void Cleanup(bool isFinal = false) {
        isFinal = isFinal || QueryState && !QueryState->KeepSession;

        if (QueryState && QueryState->TxCtx) {
            auto& txCtx = QueryState->TxCtx;
            if (txCtx->IsInvalidated()) {
                Transactions.AddToBeAborted(txCtx);
                Transactions.ReleaseTransaction(QueryState->TxId);
            }
            DiscardPersistentSnapshot(txCtx->SnapshotHandle);
        }

        if (isFinal)
            Counters->ReportSessionActorClosedRequest(Settings.DbCounters);

        if (isFinal) {
            Transactions.FinalCleanup();
            Counters->ReportTxAborted(Settings.DbCounters, Transactions.ToBeAbortedSize());
        }

        if (WorkerId) {
            auto ev = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);
            Send(*WorkerId, ev.release());
            WorkerId.reset();

            YQL_ENSURE(!CleanupCtx);
            CleanupCtx.reset(new TKqpCleanupCtx);
            CleanupCtx->IsWaitingForWorkerToClose = true;
        }

        if (Transactions.ToBeAbortedSize()) {
            if (!CleanupCtx)
                CleanupCtx.reset(new TKqpCleanupCtx);
            CleanupCtx->Final = isFinal;
            CleanupCtx->TransactionsToBeAborted = Transactions.ReleaseToBeAborted();
            SendRollbackRequest(CleanupCtx->TransactionsToBeAborted.front().Get());
        }

        LOG_I("Cleanup start, isFinal: " << isFinal << " CleanupCtx: " << bool{CleanupCtx}
            << " TransactionsToBeAborted.size(): " << (CleanupCtx ? CleanupCtx->TransactionsToBeAborted.size() : 0));
        if (CleanupCtx) {
            Become(&TKqpSessionActor::CleanupState);
        } else {
            EndCleanup(isFinal);
        }
    }

    void HandleCleanup(TEvKqp::TEvCloseSessionResponse::TPtr&) {
        CleanupCtx->IsWaitingForWorkerToClose = false;
        if (CleanupCtx->TransactionsToBeAborted.empty()) {
            EndCleanup(CleanupCtx->Final);
        }
    }

    void HandleCleanup(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        if (QueryState) {
            QueryState->Orbit = std::move(ev->Get()->Orbit);
        }

        auto& response = ev->Get()->Record.GetResponse();
        if (response.GetStatus() != Ydb::StatusIds::SUCCESS) {
            TIssues issues;
            IssuesFromMessage(response.GetIssues(), issues);
            LOG_E("Failed to cleanup: " << issues.ToString());
            EndCleanup(CleanupCtx->Final);
            return;
        }

        YQL_ENSURE(CleanupCtx);
        CleanupCtx->TransactionsToBeAborted.pop_front();
        if (CleanupCtx->TransactionsToBeAborted.size()) {
            SendRollbackRequest(CleanupCtx->TransactionsToBeAborted.front().Get());
        } else {
            if (!CleanupCtx->IsWaitingForWorkerToClose)
                EndCleanup(CleanupCtx->Final);
        }
    }

    void EndCleanup(bool isFinal) {
        LOG_D("EndCleanup, isFinal: " << isFinal);

        if (QueryResponse)
            Reply();

        if (CleanupCtx)
            Counters->ReportSessionActorCleanupLatency(Settings.DbCounters, TInstant::Now() - CleanupCtx->Start);

        if (isFinal) {
            auto lifeSpan = TInstant::Now() - CreationTime;
            Counters->ReportSessionActorFinished(Settings.DbCounters, lifeSpan);
            Counters->ReportQueriesPerSessionActor(Settings.DbCounters, QueryId);

            auto closeEv = std::make_unique<TEvKqp::TEvCloseSessionResponse>();
            closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            closeEv->Record.MutableResponse()->SetSessionId(SessionId);
            closeEv->Record.MutableResponse()->SetClosed(true);
            Send(Owner, closeEv.release());

            LOG_D("Session actor destroyed");
            PassAway();
        } else {
            CleanupCtx.reset();
            bool doNotKeepSession = QueryState && !QueryState->KeepSession;
            QueryState.reset();
            if (doNotKeepSession) {
                // TEvCloseSessionRequest was received in final=false CleanupState, so actor should rerun Cleanup with final=true
                CleanupAndPassAway();
            } else {
                Become(&TKqpSessionActor::ReadyState);
            }
        }
        ExecuterId = TActorId{};
    }

    template<class T>
    static google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> MessageFromIssues(const T& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issueMessage;
        for (const auto& i : issues) {
            IssueToMessage(i, issueMessage.Add());
        }
        return issueMessage;
    }

    void ReplyQueryError(Ydb::StatusIds::StatusCode ydbStatus,
        const TString& message, std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> issues = {})
    {
        LOG_W("Create QueryResponse for error on request, msg: " << message);

        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        QueryResponse->Record.GetRef().SetYdbStatus(ydbStatus);

        auto* response = QueryResponse->Record.GetRef().MutableResponse();

        Y_ENSURE(QueryState);
        if (QueryState->CompileResult) {
            AddQueryIssues(*response, QueryState->CompileResult->Issues);
        }

        if (issues) {
            for (auto& i : *issues) {
                response->AddQueryIssues()->Swap(&i);
            }
        }

        if (message) {
            IssueToMessage(TIssue{message}, response->AddQueryIssues());
        }

        if (QueryState->TxCtx) {
            QueryState->TxCtx->OnEndQuery();
            QueryState->TxCtx->Invalidate();
        }

        FillTxInfo(response);

        Cleanup(IsFatalError(ydbStatus));
    }

    STFUNC(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvQueryRequest, HandleReady);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleReady);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleTxResponse);
            default:
                UnexpectedEvent("ReadyState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STATEFN(CompileState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleCompile);
                hFunc(TEvKqp::TEvCompileResponse, HandleCompile);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleCompile);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleTxResponse);
                hFunc(NGRpcService::TEvClientLost, HandleClientLost);
            default:
                UnexpectedEvent("CompileState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleExecute);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleExecute);

                hFunc(TEvKqpExecuter::TEvStreamData, HandleExecute);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleExecute);

                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleExecute);
                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleExecute);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, HandleExecute);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleExecute);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(NGRpcService::TEvClientLost, HandleClientLost);

                // always come from WorkerActor
                hFunc(TEvKqp::TEvQueryResponse, ForwardResponse);
            default:
                UnexpectedEvent("ExecuteState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    // optional -- only if there were any TransactionsToBeAborted
    STATEFN(CleanupState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleCleanup);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleCleanup);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop);
                hFunc(TEvKqp::TEvCompileResponse, HandleNoop);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleCleanup);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(NGRpcService::TEvClientLost, HandleNoop);

                // always come from WorkerActor
                hFunc(TEvKqp::TEvCloseSessionResponse, HandleCleanup);
            default:
                UnexpectedEvent("CleanupState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STATEFN(TopicOpsState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleTopicOps);

                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTopicOps);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleTopicOps);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
            default:
                UnexpectedEvent("TopicOpsState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

private:

    TString CurrentStateFuncName() const {
        const auto& func = CurrentStateFunc();
        if (func == &TThis::ReadyState) {
            return "ReadyState";
        } else if (func == &TThis::ExecuteState) {
            return "ExecuteState";
        } else if (func == &TThis::TopicOpsState) {
            return "TopicOpsState";
        } else if (func == &TThis::CompileState) {
            return "CompileState";
        } else if (func == &TThis::CleanupState) {
            return "CleanupState";
        } else {
            return "unknown state";
        }
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TKqpSessionActor in state " << state << " recieve unexpected event " <<
                TypeName(*ev.Get()->GetBase()) << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

    void InternalError(const TString& message) {
        LOG_E("Internal error, message: " << message);
        if (QueryState) {
            ReplyQueryError(Ydb::StatusIds::INTERNAL_ERROR, message);
        } else {
            CleanupAndPassAway();
        }
    }

    void HandleTopicOps(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    void HandleTopicOps(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        NSchemeCache::TSchemeCacheNavigate* response = ev->Get()->Request.Get();

        Ydb::StatusIds_StatusCode status;
        TString message;

        if (IsAccessDenied(*response, message)) {
            ythrow TRequestFail(Ydb::StatusIds::UNAUTHORIZED) << message;
        }
        if (HasErrors(*response, message)) {
            ythrow TRequestFail(Ydb::StatusIds::SCHEME_ERROR) << message;
        }

        QueryState->TopicOperations.ProcessSchemeCacheNavigate(response->ResultSet,
                                                               status,
                                                               message);
        if (status != Ydb::StatusIds::SUCCESS) {
            ythrow TRequestFail(status) << message;
        }

        if (!TryMergeTopicOffsets(QueryState->TopicOperations, message)) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << message;
        }

        ReplySuccess();
    }

    bool IsAccessDenied(const NSchemeCache::TSchemeCacheNavigate& response,
                        TString& message)
    {
        bool denied = false;

        TStringBuilder builder;
        builder << "Access for topic(s)";
        for (auto& result : response.ResultSet) {
            if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                continue;
            }

            auto rights = NACLib::EAccessRights::ReadAttributes | NACLib::EAccessRights::WriteAttributes;
            if (result.SecurityObject && !result.SecurityObject->CheckAccess(rights, *QueryState->UserToken)) {
                builder << " '" << JoinPath(result.Path) << "'";
                denied = true;
            }
        }

        if (denied) {
            builder << " is denied for subject '" << QueryState->UserToken->GetUserSID() << "'";
            message = std::move(builder);
        }

        return denied;
    }

    bool HasErrors(const NSchemeCache::TSchemeCacheNavigate& response,
                   TString& message)
    {
        if (response.ErrorCount == 0) {
            return false;
        }

        TStringBuilder builder;

        builder << "Unable to navigate:";
        for (const auto& result : response.ResultSet) {
            if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                builder << "path: '" << JoinPath(result.Path) << "' status: " << result.Status;
            }
        }
        message = std::move(builder);

        return true;
    }

    bool TryMergeTopicOffsets(const NTopic::TTopicOperations &operations, TString& message) {
        try {
            YQL_ENSURE(QueryState);
            QueryState->TxCtx->TopicOperations.Merge(operations);
            return true;
        } catch (const NTopic::TOffsetsRangeIntersectExpection &ex) {
            message = ex.what();
            return false;
        }
    }

    void HandleTopicOps(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        YQL_ENSURE(QueryState);
        ReplyQueryError(Ydb::StatusIds::BAD_SESSION,
                "Request cancelled due to explicit session close request");
        Counters->ReportSessionActorClosedRequest(Settings.DbCounters);
    }

private:
    TActorId Owner;
    TString SessionId;

    TInstant CreationTime;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpRequestCounters> RequestCounters;
    TKqpWorkerSettings Settings;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TKqpSettings::TConstPtr KqpSettings;
    std::optional<TActorId> WorkerId;
    TActorId ExecuterId;

    std::shared_ptr<TKqpQueryState> QueryState;
    std::unique_ptr<TKqpCleanupCtx> CleanupCtx;
    ui32 QueryId = 0;
    TKikimrConfiguration::TPtr Config;
    IDataProvider::TFillSettings FillSettings;
    TTransactionsCache Transactions;
    std::unique_ptr<TEvKqp::TEvQueryResponse> QueryResponse;
    std::optional<TSessionShutdownState> ShutdownState;
    TULIDGenerator UlidGen;
    NTxProxy::TRequestControls RequestControls;
};

} // namespace

IActor* CreateKqpSessionActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpSessionActor(owner, sessionId, kqpSettings, workerSettings, moduleResolverState, counters);
}

}
}
