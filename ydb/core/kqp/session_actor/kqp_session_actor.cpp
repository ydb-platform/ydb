#include "kqp_session_actor.h"
#include "kqp_tx.h"
#include "kqp_worker_common.h"
#include "kqp_query_state.h"

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
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/utils/actor_log/log.h>

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
using namespace NSchemeCache;

namespace {

#define LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)
#define LOG_T(msg) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, LogPrefix() << msg)

void FillColumnsMeta(const NKqpProto::TKqpPhyQuery& phyQuery, NKikimrKqp::TQueryResponse& resp) {
    for (size_t i = 0; i < phyQuery.ResultBindingsSize(); ++i) {
        const auto& binding = phyQuery.GetResultBindings(i);
        auto ydbRes = resp.AddYdbResults();
        ydbRes->mutable_columns()->CopyFrom(binding.GetResultSetMeta().columns());
    }
}

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

std::unique_ptr<TEvKqp::TEvQueryResponse> AllocQueryResponse(const std::shared_ptr<TKqpQueryState>& state,
    bool disableReqArena)
{
    auto resEv = std::make_unique<TEvKqp::TEvQueryResponse>();
    auto reqArena = state->GetArena();
    if (!disableReqArena && reqArena) {
        resEv->Record.ReallocRef(reqArena);
    } else {
        auto arena = std::make_shared<google::protobuf::Arena>();
        resEv->Record.Realloc(arena);
    }
    return resEv;
}

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
            const TKqpWorkerSettings& workerSettings, NYql::IHTTPGateway::TPtr httpGateway,
            NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
            NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters)
        : Owner(owner)
        , SessionId(sessionId)
        , Counters(counters)
        , Settings(workerSettings)
        , HttpGateway(std::move(httpGateway))
        , AsyncIoFactory(std::move(asyncIoFactory))
        , CredentialsFactory(std::move(credentialsFactory))
        , ModuleResolverState(std::move(moduleResolverState))
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
        NWilson::TTraceId id;
        if (false) { // change to enable Wilson tracing
            id = NWilson::TTraceId::NewTraceId(TWilsonKqp::KqpSession, Max<ui32>());
            LOG_I("wilson tracing started, id: " + std::to_string(id.GetTraceId()));
        }
        auto selfId = SelfId();
        auto as = TActivationContext::ActorSystem();
        ev->Get()->SetClientLostAction(selfId, as);
        QueryState = std::make_shared<TKqpQueryState>(
            ev, QueryId, Settings.Database, Settings.Cluster, Settings.DbCounters, Settings.LongSession,
            Settings.Service, std::move(id));
    }

    void ForwardRequest(TEvKqp::TEvQueryRequest::TPtr& ev) {
        if (!WorkerId) {
            std::unique_ptr<IActor> workerActor(CreateKqpWorkerActor(SelfId(), SessionId, KqpSettings, Settings,
                HttpGateway, ModuleResolverState, Counters, CredentialsFactory));
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

    void ForwardResponse(TEvKqp::TEvProcessResponse::TPtr& ev) {
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        auto& record = QueryResponse->Record.GetRef();
        record.SetYdbStatus(ev->Get()->Record.GetYdbStatus());

        if (ev->Get()->Record.HasError()) {
            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ev->Get()->Record.GetError());
            IssueToMessage(issue, record.MutableResponse()->AddQueryIssues());
        }

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
        if (!CheckTransactionLocks(/*tx*/ nullptr)) {
            return;
        }

        bool replied = ExecutePhyTx(/*tx*/ nullptr, /*commit*/ true);
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
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
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
            << " rpcActor: " << QueryState->RequestActorId
            << " database: " << QueryState->GetDatabase()
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

            case NKikimrKqp::QUERY_ACTION_EXPLAIN: {
                auto type = QueryState->GetType();
                if (type != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY &&
                    type != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY &&
                    type != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT)
                {
                    return ForwardRequest(ev);
                }

                break;
            }

            // not supported yet
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

        QueryState->AddOffsetsToTransaction();

        auto navigate = QueryState->BuildSchemeCacheNavigate();

        Become(&TKqpSessionActor::TopicOpsState);
        ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    }

    void CompileQuery() {
        YQL_ENSURE(QueryState);
        auto ev = QueryState->BuildCompileRequest();
        LOG_D("Sending CompileQuery request");
        Send(MakeKqpCompileServiceID(SelfId().NodeId()), ev.release(), 0, QueryState->QueryId);
        Become(&TKqpSessionActor::CompileState);
    }

    void HandleCompile(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get();
        YQL_ENSURE(response->Request);
        // outdated response from scheme cache.
        // ignoring that.
        if (response->Request->Cookie < QueryId)
            return;

        // table versions are not the same. need the query recompilation.
        if (!QueryState->EnsureTableVersions(*response)) {
            auto ev = QueryState->BuildReCompileRequest();
            Send(MakeKqpCompileServiceID(SelfId().NodeId()), ev.release(), 0, QueryState->QueryId);
            return;
        }

        OnSuccessCompileRequest();
    }

    void Handle(TEvKqp::TEvCompileResponse::TPtr& ev) {
        // outdated event from previous query.
        // ignoring that.
        if (ev->Cookie < QueryId) {
            return;
        }

        YQL_ENSURE(QueryState);
        TTimerGuard timer(this);

        // saving compile response and checking that compilation status
        // is success.
        if (!QueryState->SaveAndCheckCompileResult(ev->Get())) {
            LWTRACK(KqpSessionQueryCompiled, QueryState->Orbit, TStringBuilder() << QueryState->CompileResult->Status);
            ReplyQueryCompileError();
            return;
        }

        LWTRACK(KqpSessionQueryCompiled, QueryState->Orbit, TStringBuilder() << QueryState->CompileResult->Status);
        // even if we have successfully compilation result, it doesn't mean anything
        // in terms of current schema version of the table if response of compilation is from the cache.
        // because of that, we are forcing to run schema version check
        if (QueryState->NeedCheckTableVersions()) {
            auto ev = QueryState->BuildNavigateKeySet();
            Send(MakeSchemeCacheID(), ev.release());
            return;
        }

        OnSuccessCompileRequest();
    }

    void OnSuccessCompileRequest() {
        if (QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE ||
            QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_EXPLAIN)
        {
            return ReplyPrepareResult();
        }

        if (!PrepareQueryContext()) {
            return;
        }

        Become(&TKqpSessionActor::ExecuteState);

        QueryState->TxCtx->OnBeginQuery();

        if (QueryState->NeedPersistentSnapshot()) {
            AcquirePersistentSnapshot();
            return;
        } else if (QueryState->NeedSnapshot(*Config)) {
            AcquireMvccSnapshot();
            return;
        }

        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void AcquirePersistentSnapshot() {
        LOG_D("acquire persistent snapshot");
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = RegisterWithSameMailbox(snapMgr);

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(QueryState->PreparedQuery->GetQueryTables(), std::move(QueryState->Orbit));
        Send(snapMgrActorId, ev.release());

        QueryState->TxCtx->SnapshotHandle.ManagingActor = snapMgrActorId;
    }

    void DiscardPersistentSnapshot(const IKqpGateway::TKqpSnapshotHandle& handle) {
        if (handle.ManagingActor) { // persistent snapshot was acquired
            Send(handle.ManagingActor, new TEvKqpSnapshot::TEvDiscardSnapshot());
        }
    }

    void AcquireMvccSnapshot() {
        LOG_D("acquire mvcc snapshot");
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = RegisterWithSameMailbox(snapMgr);

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(std::move(QueryState->Orbit));
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

        if (QueryState) {
            QueryState->Orbit = std::move(response->Orbit);
        }

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
        QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false, AppData()->FunctionRegistry,
            AppData()->TimeProvider, AppData()->RandomProvider, Config->EnableKqpImmediateEffects);
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
                AppData()->TimeProvider, AppData()->RandomProvider, Config->EnableKqpImmediateEffects);
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
        auto [success, issues] = QueryState->TxCtx->ApplyTableOperations(phyQuery.GetTableOps(), phyQuery.GetTableInfos(),
            EKikimrQueryType::Dml);
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
                    type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY ||
                    type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY ||
                    type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT
                );
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                YQL_ENSURE(type == NKikimrKqp::QUERY_TYPE_PREPARED_DML);
                break;

            default:
                YQL_ENSURE(false, "Unexpected query action: " << action);
        }

        try {
            QueryState->QueryData->ParseParameters(QueryState->GetYdbParameters());
        } catch(const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        }
        return true;
    }

    void ReplyPrepareResult() {
        YQL_ENSURE(QueryState);
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(QueryState->CompileResult, QueryResponse->Record);

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

    IKqpGateway::TExecPhysicalRequest PrepareLiteralRequest(TKqpQueryState *queryState) {
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
        YQL_ENSURE(queryState);
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

    IKqpGateway::TExecPhysicalRequest PrepareRequest(const TKqpPhyTxHolder::TConstPtr& tx, bool literal,
        TKqpQueryState *queryState)
    {
        if (literal) {
            YQL_ENSURE(tx);
            return PrepareLiteralRequest(QueryState.get());
        }

        if (!tx) {
            return PreparePhysicalRequest(QueryState.get(), queryState->TxCtx->TxAlloc);
        }

        switch (tx->GetType()) {
            case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
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

    bool CheckTransactionLocks(const TKqpPhyTxHolder::TConstPtr& tx) {
        auto& txCtx = *QueryState->TxCtx;
        if (!txCtx.DeferredEffects.Empty() && txCtx.Locks.Broken()) {
            ReplyQueryError(Ydb::StatusIds::ABORTED, "tx has deferred effects, but locks are broken",
                MessageFromIssues(std::vector<TIssue>{txCtx.Locks.GetIssue()}));
            return false;
        }

        if (tx && tx->GetHasEffects() && txCtx.Locks.Broken()) {
            ReplyQueryError(Ydb::StatusIds::ABORTED, "tx has effects, but locks are broken",
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
        bool haveWork = QueryState->PreparedQuery &&
                QueryState->CurrentTx < QueryState->PreparedQuery->GetPhysicalQuery().TransactionsSize()
                    || QueryState->Commit && !QueryState->Commited;

        if (!haveWork) {
            ReplySuccess();
            return;
        }

        TKqpPhyTxHolder::TConstPtr tx;
        try {
            tx = QueryState->GetCurrentPhyTx();
        } catch (const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        }

        if (!CheckTransactionLocks(tx) || !CheckTopicOperations()) {
            return;
        }

        if (QueryState->TxCtx->ShouldExecuteDeferredEffects()) {
            ExecuteDeferredEffectsImmediately();
        } else if (auto commit = QueryState->ShouldCommitWithCurrentTx(tx); commit || tx) {
            ExecutePhyTx(tx, commit);
        } else {
            ReplySuccess();
        }
    }

    void ExecuteDeferredEffectsImmediately() {
        YQL_ENSURE(QueryState->TxCtx->ShouldExecuteDeferredEffects());

        auto& txCtx = *QueryState->TxCtx;
        auto request = PrepareRequest(/* tx */ nullptr, /* literal */ false, QueryState.get());

        for (const auto& effect : txCtx.DeferredEffects) {
            YQL_ENSURE(effect.PhysicalTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_DATA);
            request.Transactions.emplace_back(effect.PhysicalTx, effect.Params);

            LOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction,"
                << " current Transactions.size(): " << request.Transactions.size());
        }

        request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();
        request.UseImmediateEffects = true;
        request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();

        txCtx.HasImmediateEffects = true;
        txCtx.ClearDeferredEffects();

        SendToExecuter(std::move(request));
    }

    bool ExecutePhyTx(const TKqpPhyTxHolder::TConstPtr& tx, bool commit) {
        if (tx && tx->GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
            YQL_ENSURE(QueryState->TxCtx->EffectiveIsolationLevel == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);
            YQL_ENSURE(tx->StagesSize() == 0);

            SendToSchemeExecuter(tx);
            ++QueryState->CurrentTx;
            return false;
        }

        auto& txCtx = *QueryState->TxCtx;
        bool literal = tx && tx->IsLiteralTx();

        if (commit) {
            if (txCtx.TxHasEffects() || txCtx.Locks.HasLocks() || txCtx.TopicOperations.HasOperations()) {
                // Cannot perform commit in literal execution
                literal = false;
            } else if (!tx) {
                // Commit is no-op
                ReplySuccess();
                return true;
            }
        }

        auto request = PrepareRequest(tx, literal, QueryState.get());

        LOG_D("ExecutePhyTx, tx: " << (void*)tx.get() << " literal: " << literal << " commit: " << commit
                << " txCtx.DeferredEffects.size(): " << txCtx.DeferredEffects.Size());

        if (!CheckTopicOperations()) {
            return true;
        }

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

            try {
                QueryState->QueryData->PrepareParameters(tx, QueryState->PreparedQuery, txCtx.TxAlloc->TypeEnv);
            } catch (const yexception& ex) {
                ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
            }
            request.Transactions.emplace_back(tx, QueryState->QueryData);
            txCtx.HasImmediateEffects = txCtx.HasImmediateEffects || tx->GetHasEffects();
        } else {
            YQL_ENSURE(commit);
        }

        if (literal) {
            if (QueryState) {
                request.Orbit = std::move(QueryState->Orbit);
            }
            request.TraceId = QueryState ? QueryState->KqpSessionSpan.GetTraceId() : NWilson::TTraceId();
            auto response = ExecuteLiteral(std::move(request), RequestCounters, SelfId());
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
                if (!txCtx.GetSnapshot().IsValid() || txCtx.TxHasEffects() || txCtx.TopicOperations.HasReadOperations()) {
                    LOG_D("TExecPhysicalRequest, tx has commit locks");
                    request.LocksOp = ELocksOp::Commit;
                } else {
                    LOG_D("TExecPhysicalRequest, tx has rollback locks");
                    request.LocksOp = ELocksOp::Rollback;
                }

                for (auto& [lockId, lock] : txCtx.Locks.LocksMap) {
                    auto dsLock = ExtractLock(lock.GetValueRef(txCtx.Locks.LockType));
                    request.DataShardLocks[dsLock.GetDataShard()].emplace_back(dsLock);
                }

            }

            request.TopicOperations = std::move(txCtx.TopicOperations);
        } else if (QueryState->ShouldAcquireLocks()) {
            request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();

            if (txCtx.HasUncommittedChangesRead || Config->FeatureFlags.GetEnableForceImmediateEffectsExecution()) {
                YQL_ENSURE(txCtx.EnableImmediateEffects);
                request.UseImmediateEffects = true;
            }
        }

        LWTRACK(KqpSessionPhyQueryProposeTx,
            QueryState->Orbit,
            QueryState->CurrentTx,
            request.Transactions.size(),
            txCtx.Locks.Size(),
            request.AcquireLocksTxId.Defined());

        SendToExecuter(std::move(request));
        ++QueryState->CurrentTx;
        return false;
    }

    void SendToSchemeExecuter(const TKqpPhyTxHolder::TConstPtr& tx) {
        auto userToken = QueryState ? QueryState->UserToken : TIntrusiveConstPtr<NACLib::TUserToken>();
        auto executerActor = CreateKqpSchemeExecuter(tx, SelfId(), Settings.Database, userToken,
            QueryState->TxCtx->TxAlloc);

        ExecuterId = RegisterWithSameMailbox(executerActor);
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
            RequestCounters, Settings.Service.GetAggregationConfig(), Settings.Service.GetExecuterRetriesConfig(),
            AsyncIoFactory, QueryState ? QueryState->PreparedQuery : nullptr, Settings.Service.GetChannelTransportVersion(), SelfId());

        auto exId = RegisterWithSameMailbox(executerActor);
        LOG_D("Created new KQP executer: " << exId << " isRollback: " << isRollback);
        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
        if (!isRollback) {
            Y_VERIFY(!ExecuterId);
        }
        ExecuterId = exId;
    }


    template<typename T>
    void HandleNoop(T&) {
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
        QueryState->QueryData->ClearPrunedParams();

        if (!ev->GetTxResults().empty()) {
            QueryState->QueryData->AddTxResults(QueryState->CurrentTx - 1, std::move(ev->GetTxResults()));
        }
        QueryState->QueryData->AddTxHolders(std::move(ev->GetTxHolders()));

        if (ev->LockHandle) {
            QueryState->TxCtx->Locks.LockHandle = std::move(ev->LockHandle);
        }

        auto& executerResults = *response->MutableResult();
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
        LOG_D("Forwarded TEvStreamData to " << QueryState->RequestActorId);
        TlsActivationContext->Send(ev->Forward(QueryState->RequestActorId));
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        TlsActivationContext->Send(ev->Forward(ExecuterId));
    }

    void HandleExecute(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;

        TString logMsg = TStringBuilder() << "got TEvAbortExecution in " << CurrentStateFuncName();
        LOG_I(logMsg << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode()) << " send to: " << ExecuterId);

        if (ExecuterId) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(msg.GetStatusCode(), "Request timeout exceeded");
            Send(ExecuterId, abortEv.Release());
        }

        // Do not shortcut in case of CancelAfter event. We can send this status only in case of RO TX.
        if (msg.GetStatusCode() != NYql::NDqProto::StatusIds::CANCELLED) {
            const auto& issues = ev->Get()->GetIssues();
            ExecuterId = TActorId{};
            ReplyQueryError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), logMsg, MessageFromIssues(issues));
            return;
        }
    }

    void HandleCompile(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;

        TString logMsg = TStringBuilder() << "got TEvAbortExecution in " << CurrentStateFuncName();
        LOG_I(logMsg << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode()));

        const auto& issues = ev->Get()->GetIssues();

        YQL_ENSURE(!ExecuterId);

        ReplyQueryError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), logMsg, MessageFromIssues(issues));
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
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT: {
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

        if (QueryState->ReportStats()) {
            if (QueryState->GetStatsMode() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
                response->SetQueryPlan(SerializeAnalyzePlan(*stats));
            }
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
        YQL_ENSURE(QueryState);
        auto resEv = AllocQueryResponse(QueryState, QueryState->IsStreamResult());
        auto *record = &resEv->Record.GetRef();
        auto *response = record->MutableResponse();

        if (QueryState->CompileResult) {
            AddQueryIssues(*response, QueryState->CompileResult->Issues);
        }

        FillStats(record);

        if (QueryState->TxCtx) {
            QueryState->TxCtx->OnEndQuery();
        }

        if (QueryState->Commit) {
            ResetTxState();
            Transactions.ReleaseTransaction(QueryState->TxId);
            QueryState->TxId = TTxId();
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

        // Result for scan query is sent directly to target actor.
        Y_VERIFY(response->GetArena());
        if (QueryState->PreparedQuery && !QueryState->IsStreamResult()) {
            bool useYdbResponseFormat = QueryState->GetUsePublicResponseDataFormat();
            auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            for (size_t i = 0; i < phyQuery.ResultBindingsSize(); ++i) {
                if (useYdbResponseFormat) {
                    TMaybe<ui64> effectiveRowsLimit = FillSettings.RowsLimitPerWrite;
                    if (QueryState->PreparedQuery->GetResults(i).GetRowsLimit()) {
                        effectiveRowsLimit = QueryState->PreparedQuery->GetResults(i).GetRowsLimit();
                    }
                    auto* ydbResult = QueryState->QueryData->GetYdbTxResult(phyQuery.GetResultBindings(i), response->GetArena(), effectiveRowsLimit);
                    response->AddYdbResults()->Swap(ydbResult);
                } else {
                    auto* protoRes = QueryState->QueryData->GetMkqlTxResult(phyQuery.GetResultBindings(i), response->GetArena());
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
                    auto* finalResult = KikimrResultToProto(*protoRes, {}, fillSettings.value_or(FillSettings), response->GetArena());
                    response->AddResults()->Swap(finalResult);
                }
            }
        }

        resEv->Record.GetRef().SetYdbStatus(Ydb::StatusIds::SUCCESS);
        LOG_D("Create QueryResponse for action: " << QueryState->GetAction() << " with SUCCESS status");

        QueryResponse = std::move(resEv);

        Cleanup();
    }

    void ReplyQueryCompileError() {
        YQL_ENSURE(QueryState);
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(QueryState->CompileResult, QueryResponse->Record);

        auto txId = TTxId();
        if (QueryState->HasTxControl()) {
            const auto& txControl = QueryState->GetTxControl();
            if (txControl.tx_selector_case() == Ydb::Table::TransactionControl::kTxId) {
                txId = TTxId::FromString(txControl.tx_id());
            }
        }

        LOG_W("ReplyQueryCompileError, status " << QueryState->CompileResult->Status << " remove tx with tx_id: " << txId.GetHumanStr());
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
        LOG_W("Reply process error, msg: " << message << " proxyRequestId: " << proxyRequestId);

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

            response.SetQueryPlan(preparedQuery->GetPhysicalQuery().GetQueryPlan());
            response.SetQueryAst(preparedQuery->GetPhysicalQuery().GetQueryAst());

            const auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            FillColumnsMeta(phyQuery, response);
        } else if (compileResult->Status == Ydb::StatusIds::TIMEOUT && QueryState->QueryDeadlines.CancelAt) {
            // The compile timeout cause cancelation execution of request.
            // So in case of cancel after we can reply with canceled status
            ev.SetYdbStatus(Ydb::StatusIds::CANCELLED);
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

        request.LocksOp = ELocksOp::Rollback;

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

        auto workerId = WorkerId;
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
            << " TransactionsToBeAborted.size(): " << (CleanupCtx ? CleanupCtx->TransactionsToBeAborted.size() : 0)
            << " WorkerId: " << (workerId ? *workerId : TActorId()));
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
        if (ev->Sender != ExecuterId) {
            return;
        }
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

    void Handle(TEvKqp::TEvCancelQueryRequest::TPtr& ev) {
        {
            auto abort = MakeHolder<NYql::NDq::TEvDq::TEvAbortExecution>();
            abort->Record.SetStatusCode(NYql::NDqProto::StatusIds::CANCELLED);
            abort->Record.AddIssues()->set_message("Canceled");
            Send(SelfId(), abort.Release());
        }

        {
            auto resp = MakeHolder<TEvKqp::TEvCancelQueryResponse>();
            resp->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            Send(ev->Sender, resp.Release(), 0, ev->Cookie);
        }
    }

    STFUNC(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvQueryRequest, HandleReady);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleReady);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(TEvKqp::TEvCancelQueryRequest, Handle);

                // forgotten messages from previous aborted request
                hFunc(TEvKqp::TEvCompileResponse, HandleNoop);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleNoop);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNoop);
                // message from KQP proxy in case of our reply just after kqp proxy timer tick
                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleNoop);
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
                hFunc(TEvKqp::TEvCompileResponse, Handle);

                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleCompile);
                hFunc(TEvKqp::TEvCloseSessionRequest, HandleCompile);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(NGRpcService::TEvClientLost, HandleClientLost);
                hFunc(TEvKqp::TEvCancelQueryRequest, Handle);

                // forgotten messages from previous aborted request
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleNoop);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
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

                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleExecute);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, HandleExecute);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleExecute);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(NGRpcService::TEvClientLost, HandleClientLost);
                hFunc(TEvKqp::TEvCancelQueryRequest, Handle);

                // forgotten messages from previous aborted request
                hFunc(TEvKqp::TEvCompileResponse, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNoop);

                // always come from WorkerActor
                hFunc(TEvKqp::TEvQueryResponse, ForwardResponse);
                hFunc(TEvKqp::TEvProcessResponse, ForwardResponse);
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

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleCleanup);
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(NGRpcService::TEvClientLost, HandleNoop);
                hFunc(TEvKqp::TEvCancelQueryRequest, HandleNoop);

                // forgotten messages from previous aborted request
                hFunc(TEvKqp::TEvCompileResponse, HandleNoop);
                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleNoop);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNoop);

                // always come from WorkerActor
                hFunc(TEvKqp::TEvCloseSessionResponse, HandleCleanup);
                hFunc(TEvKqp::TEvQueryResponse, HandleNoop);
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
        InternalError(TStringBuilder() << "TKqpSessionActor in state " << state << " received unexpected event " <<
                ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()) << " sender: " << ev->Sender);
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
        YQL_ENSURE(ev->Get()->Request);
        if (ev->Get()->Request->Cookie < QueryId) {
            return;
        }

        NSchemeCache::TSchemeCacheNavigate* response = ev->Get()->Request.Get();

        Ydb::StatusIds_StatusCode status;
        TString message;

        if (QueryState->IsAccessDenied(*response, message)) {
            ythrow TRequestFail(Ydb::StatusIds::UNAUTHORIZED) << message;
        }
        if (QueryState->HasErrors(*response, message)) {
            ythrow TRequestFail(Ydb::StatusIds::SCHEME_ERROR) << message;
        }

        QueryState->TopicOperations.ProcessSchemeCacheNavigate(response->ResultSet, status, message);
        if (status != Ydb::StatusIds::SUCCESS) {
            ythrow TRequestFail(status) << message;
        }

        if (!QueryState->TryMergeTopicOffsets(QueryState->TopicOperations, message)) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << message;
        }

        ReplySuccess();
    }

    void HandleTopicOps(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        YQL_ENSURE(QueryState);
        ReplyQueryError(Ydb::StatusIds::BAD_SESSION, "Request cancelled due to explicit session close request");
        Counters->ReportSessionActorClosedRequest(Settings.DbCounters);
    }

private:
    TActorId Owner;
    TString SessionId;

    TInstant CreationTime;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpRequestCounters> RequestCounters;
    TKqpWorkerSettings Settings;
    NYql::IHTTPGateway::TPtr HttpGateway;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
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
    NYql::IHTTPGateway::TPtr httpGateway, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpSessionActor(owner, sessionId, kqpSettings, workerSettings, std::move(httpGateway), std::move(asyncIoFactory), std::move(credentialsFactory), std::move(moduleResolverState), counters);
}

}
}
