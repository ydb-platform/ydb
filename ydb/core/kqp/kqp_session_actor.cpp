#include "kqp_impl.h"
#include "kqp_worker_common.h"

#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/common/kqp_transform.h>
#include <ydb/core/kqp/executer/kqp_executer.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/prepare/kqp_prepare.h>
#include <ydb/core/kqp/prepare/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/kqp/rm/kqp_snapshot_manager.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/printf.h>
#include <util/string/escape.h>

LWTRACE_USING(KQP_PROVIDER);

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

class TRequestFail : public yexception {
public:
    TKqpRequestInfo RequestInfo;
    Ydb::StatusIds::StatusCode Status;
    std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> Issues;

    TRequestFail(TKqpRequestInfo info, Ydb::StatusIds::StatusCode status,
            std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> issues = {})
        : RequestInfo(info)
        , Status(status)
        , Issues(std::move(issues))
    {}
};

struct TKqpQueryState {
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    NKikimrKqp::TQueryRequest Request;
    ui64 ParametersSize = 0;
    TPreparedQueryConstPtr PreparedQuery;
    TKqpCompileResult::TConstPtr CompileResult;
    NKqpProto::TKqpStatsCompile CompileStats;
    TIntrusivePtr<TKqpTransactionContext> TxCtx;
    std::shared_ptr<TKikimrQueryContext> QueryCtx = std::make_shared<TKikimrQueryContext>();
    TActorId RequestActorId;

    ui64 CurrentTx = 0;
    TString TraceId;

    TInstant StartTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;

    NKqpProto::TKqpStatsQuery Stats;

    TString UserToken;

    NLWTrace::TOrbit Orbit;

    TString TxId; // User tx
    bool Commit = false;
};

struct TKqpCleanupCtx {
    ui64 AbortedTransactionsCount = 0;
    ui64 TransactionsToBeAborted = 0;
    std::vector<IKqpGateway::TExecPhysicalRequest> ExecuterAbortRequests;
    bool Final = false;
    TInstant Start;
};

EKikimrStatsMode GetStatsModeInt(const NKikimrKqp::TQueryRequest& queryRequest, EKikimrStatsMode minMode) {
    switch (queryRequest.GetStatsMode()) {
        case NYql::NDqProto::DQ_STATS_MODE_BASIC:
            return EKikimrStatsMode::Basic;
        case NYql::NDqProto::DQ_STATS_MODE_PROFILE:
            return EKikimrStatsMode::Profile;
        default:
            return std::max(EKikimrStatsMode::None, minMode);
    }
}

TKikimrQueryLimits GetQueryLimits(const TKqpWorkerSettings& settings) {
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

class TKqpSessionActor : public TActorBootstrapped<TKqpSessionActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SESSION_ACTOR;
    }

    TIntrusivePtr<TKikimrConfiguration> CreateConfig(const TKqpSettings::TConstPtr& kqpSettings,
        const TKqpWorkerSettings& workerSettings)
    {
        auto cfg = MakeIntrusive<TKikimrConfiguration>();
        cfg->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), workerSettings.Cluster,
                kqpSettings->Settings, false);

        if (!workerSettings.Database.empty()) {
            cfg->_KqpTablePathPrefix = workerSettings.Database;
        }

        ApplyServiceConfig(*cfg, workerSettings.Service);

        cfg->FreezeDefaults();
        return cfg;
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
        , ExplicitTransactions(*Config->_KqpMaxActiveTxPerSession.Get())
    {
        IdleDuration = TDuration::Seconds(*Config->_KqpSessionIdleTimeoutSec.Get());

        RequestCounters = MakeIntrusive<TKqpRequestCounters>();
        RequestCounters->Counters = Counters;
        RequestCounters->DbCounters = Settings.DbCounters;
        RequestCounters->TxProxyMon = MakeIntrusive<NTxProxy::TTxProxyMon>(AppData()->Counters);
    }

    void Bootstrap() {
        Counters->ReportSessionActorCreated(Settings.DbCounters);
        CreationTime = TInstant::Now();

        Config->FeatureFlags = AppData()->FeatureFlags;

        Become(&TKqpSessionActor::ReadyState);
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
        YQL_ENSURE(!QueryState);
        QueryState = std::make_unique<TKqpQueryState>();
    }

    template<class T>
    void ForwardRequest(T& ev) {
        if (!WorkerId) {
            std::unique_ptr<IActor> workerActor(CreateKqpWorkerActor(Owner, SessionId, KqpSettings, Settings,
                    ModuleResolverState, Counters));
            WorkerId = RegisterWithSameMailbox(workerActor.release());
        }
        TlsActivationContext->Send(new IEventHandle(*WorkerId, SelfId(), ev->Release().Release(), ev->Flags, ev->Cookie,
                    nullptr, std::move(ev->TraceId)));
    }

    void ForwardResponse(TEvKqp::TEvQueryResponse::TPtr& ev) {
        TlsActivationContext->Send(new IEventHandle(Owner, SelfId(), ev->Release().Release(), ev->Flags, ev->Cookie,
                                    nullptr, std::move(ev->TraceId)));
        Cleanup();
    }

    TIntrusivePtr<TKqpTransactionContext> FindTransaction(const TString& id) {
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

    void RollbackTx(const TKqpRequestInfo& requestInfo) {
        auto& queryRequest = QueryState->Request;
        YQL_ENSURE(queryRequest.HasTxControl(),
                "Can't perform ROLLBACK_TX: TxControl isn't set in TQueryRequest");
        const auto& txControl = queryRequest.GetTxControl();
        QueryState->Commit = txControl.commit_tx();
        const auto& txId = txControl.tx_id();
        auto txCtx = FindTransaction(txId);
        if (!txCtx) {
            std::vector<TIssue> issues{YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND,
                TStringBuilder() << "Transaction not found: " << QueryState->TxId)};
            ReplyQueryError(requestInfo, Ydb::StatusIds::BAD_REQUEST, "", MessageFromIssues(issues));
        } else {
            QueryState->TxCtx = txCtx;
            txCtx->Invalidate();
            AbortedTransactions.emplace_back(txCtx);
            RemoveTransaction(txId);

            SendRollbackRequest(txCtx.Get());
            Become(&TKqpSessionActor::ExecuteState);
        }
    }

    void CommitTx() {
        auto& queryRequest = QueryState->Request;

        YQL_ENSURE(queryRequest.HasTxControl());

        auto& txControl = queryRequest.GetTxControl();
        YQL_ENSURE(txControl.tx_selector_case() == Ydb::Table::TransactionControl::kTxId, "Can't commit transaction - "
                << " there is no TxId in Query's TxControl, queryRequest: " << queryRequest.DebugString());


        QueryState->Commit = txControl.commit_tx();

        const auto& txId = txControl.tx_id();
        auto txCtx = FindTransaction(txId);
        LOG_D("queryRequest TxControl: " << txControl.DebugString() << " txCtx: " << (void*)txCtx.Get());
        if (!txCtx) {
            std::vector<TIssue> issues{YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND,
                TStringBuilder() << "Transaction not found: " << QueryState->TxId)};
            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            ReplyQueryError(requestInfo, Ydb::StatusIds::NOT_FOUND, "", MessageFromIssues(issues));
            return;
        }
        QueryState->TxCtx = std::move(txCtx);
        QueryState->TxId = txId;
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
                return true;

            // should not be compiled. TODO: forward to request executer
            // not supported yet
            case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
            case NKikimrKqp::QUERY_TYPE_AST_DML:
            case NKikimrKqp::QUERY_TYPE_UNDEFINED:
                return false;
        }
    }

    void HandleReady(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());
        YQL_ENSURE(requestInfo.GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << requestInfo.GetSessionId());

        MakeNewQueryState();

        QueryState->Request.Swap(event.MutableRequest());
        auto& queryRequest = QueryState->Request;
        YQL_ENSURE(queryRequest.GetDatabase() == Settings.Database,
                "Wrong database, expected:" << Settings.Database << ", got: " << queryRequest.GetDatabase());

        YQL_ENSURE(queryRequest.HasAction());
        auto action = queryRequest.GetAction();

        LWTRACK(KqpQueryRequest, QueryState->Orbit, queryRequest.GetDatabase(),
                queryRequest.HasType() ? queryRequest.GetType() : NKikimrKqp::QUERY_TYPE_UNDEFINED,
                action, queryRequest.GetQuery());
        LOG_D(requestInfo << "Received request,"
            << " proxyRequestId: " << proxyRequestId
            << " query: " << (queryRequest.HasQuery() ? queryRequest.GetQuery().Quote() : "")
            << " prepared: " << queryRequest.HasPreparedQuery()
            << " tx_control: " << queryRequest.HasTxControl()
            << " action: " << action
            << " type: " << (queryRequest.HasType() ? queryRequest.GetType() : NKikimrKqp::QUERY_TYPE_UNDEFINED)
        );

        QueryState->Sender = ev->Sender;
        QueryState->ProxyRequestId = proxyRequestId;
        QueryState->TraceId = requestInfo.GetTraceId();
        QueryState->StartTime = TInstant::Now();
        QueryState->UserToken = event.GetUserToken();
        QueryState->QueryDeadlines = GetQueryDeadlines(queryRequest);
        QueryState->ParametersSize = queryRequest.GetParameters().ByteSize();
        QueryState->RequestActorId = ActorIdFromProto(event.GetRequestActorId());

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
            case NKikimrKqp::QUERY_ACTION_PREPARE:
            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED: {
                YQL_ENSURE(queryRequest.HasType());
                auto type = queryRequest.GetType();
                YQL_ENSURE(type != NKikimrKqp::QUERY_TYPE_UNDEFINED, "query type is undefined");

                if (!IsQueryTypeSupported(type)) {
                    event.MutableRequest()->Swap(&QueryState->Request);
                    ForwardRequest(ev);
                    return;
                }
                break;
            }
            case NKikimrKqp::QUERY_ACTION_BEGIN_TX: {
                YQL_ENSURE(queryRequest.HasTxControl(),
                    "Can't perform BEGIN_TX: TxControl isn't set in TQueryRequest");
                auto& txControl = queryRequest.GetTxControl();
                QueryState->Commit = txControl.commit_tx();
                BeginTx(txControl.begin_tx());
                ReplySuccess();
                return;
            }
            case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX: {
                RollbackTx(requestInfo);
                return;
            }
            case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
                CommitTx();
                return;
            // not supported yet
            case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
            case NKikimrKqp::QUERY_ACTION_PARSE:
                event.MutableRequest()->Swap(&QueryState->Request);
                ForwardRequest(ev);
                return;
        }

        StopIdleTimer();

        CompileQuery();
    }

    void CompileQuery() {
        YQL_ENSURE(QueryState);
        auto& queryRequest = QueryState->Request;

        TMaybe<TKqpQueryId> query;
        TMaybe<TString> uid;

        bool keepInCache = false;
        bool scan = queryRequest.GetType() == NKikimrKqp::QUERY_TYPE_SQL_SCAN;
        switch (queryRequest.GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, queryRequest.GetQuery(), scan);
                keepInCache = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            case NKikimrKqp::QUERY_ACTION_PREPARE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, queryRequest.GetQuery(), scan);
                keepInCache = true;
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                uid = queryRequest.GetPreparedQuery();
                keepInCache = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            default:
                YQL_ENSURE(false);
        }

        auto compileDeadline = QueryState->QueryDeadlines.TimeoutAt;
        if (QueryState->QueryDeadlines.CancelAt) {
            compileDeadline = Min(compileDeadline, QueryState->QueryDeadlines.CancelAt);
        }

        auto compileRequestActor = CreateKqpCompileRequestActor(SelfId(), QueryState->UserToken, uid,
            std::move(query), keepInCache, compileDeadline, Settings.DbCounters);
        TlsActivationContext->ExecutorThread.RegisterActor(compileRequestActor);
        Become(&TKqpSessionActor::CompileState);
    }

    void HandleCompile(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    void HandleCompile(TEvKqp::TEvCompileResponse::TPtr& ev) {
        auto compileResult = ev->Get()->CompileResult;

        YQL_ENSURE(compileResult);
        YQL_ENSURE(QueryState);

        LWTRACK(KqpQueryCompiled, QueryState->Orbit, TStringBuilder() << compileResult->Status);

        if (compileResult->Status != Ydb::StatusIds::SUCCESS) {
            if (ReplyQueryCompileError(compileResult)) {
                Cleanup();
                StartIdleTimer();
            } else {
                FinalCleanup();
            }
            return;
        }

        YQL_ENSURE(compileResult->PreparedQuery);
        const ui32 compiledVersion = compileResult->PreparedQuery->GetVersion();
        YQL_ENSURE(compiledVersion == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1,
                "Invalid compiled version: " << compiledVersion);

        QueryState->CompileResult = compileResult;
        QueryState->CompileStats.Swap(&ev->Get()->Stats);
        QueryState->PreparedQuery = compileResult->PreparedQuery;
        QueryState->Request.SetQuery(QueryState->PreparedQuery->GetText());

        auto& queryRequest = QueryState->Request;
        if (queryRequest.GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE) {
            if (ReplyPrepareResult(compileResult)) {
                Cleanup();
                StartIdleTimer();
            } else {
                FinalCleanup();
            }
            return;
        }

        if (!PrepareQueryContext()) {
            return;
        }

        Become(&TKqpSessionActor::ExecuteState);

        if (queryRequest.GetType() == NKikimrKqp::QUERY_TYPE_SQL_SCAN) {
            AcquirePersistentSnapshot();
            return;
        } else if (NeedSnapshot(*QueryState->TxCtx, *Config, /*rollback*/ false, QueryState->Commit,
                &QueryState->PreparedQuery->GetPhysicalQuery(), /*preparedKql*/ nullptr)) {
            AcquireMvccSnapshot();
            return;
        }
        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void AcquirePersistentSnapshot() {
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = TlsActivationContext->ExecutorThread.RegisterActor(snapMgr);

        THashSet<TString> tablesSet;
        const auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        for (const auto& phyTx: phyQuery.GetTransactions()) {
            for (const auto& stage: phyTx.GetStages()) {
                for (const auto& tableOp: stage.GetTableOps()) {
                    tablesSet.insert(tableOp.GetTable().GetPath());
                }
            }
        }
        TVector<TString> tables(tablesSet.begin(), tablesSet.end());

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(tables);
        Send(snapMgrActorId, ev.release());

        QueryState->TxCtx->SnapshotHandle.ManagingActor = snapMgrActorId;
    }

    void AcquireMvccSnapshot() {
        LOG_D("AcquireMvccSnapshot");
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = TlsActivationContext->ExecutorThread.RegisterActor(snapMgr);

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>();
        Send(snapMgrActorId, ev.release());
    }

    void HandleExecute(TEvKqpSnapshot::TEvCreateSnapshotResponse::TPtr& ev) {
        auto *response = ev->Get();

        if (response->Status != NKikimrIssues::TStatusIds::SUCCESS) {
            // TODO
            (void)response->Issues;
            return;
        }
        QueryState->TxCtx->SnapshotHandle.Snapshot = response->Snapshot;

        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void SetIsolationLevel(const Ydb::Table::TransactionSettings& settings) {
        YQL_ENSURE(QueryState->TxCtx);
        auto& txCtx = QueryState->TxCtx;
        switch (settings.tx_mode_case()) {
            case Ydb::Table::TransactionSettings::kSerializableReadWrite:
                txCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                txCtx->Readonly = false;
                break;

            case Ydb::Table::TransactionSettings::kOnlineReadOnly:
                txCtx->EffectiveIsolationLevel = settings.online_read_only().allow_inconsistent_reads()
                    ? NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED
                    : NKikimrKqp::ISOLATION_LEVEL_READ_COMMITTED;
                txCtx->Readonly = true;
                break;

            case Ydb::Table::TransactionSettings::kStaleReadOnly:
                txCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_READ_STALE;
                txCtx->Readonly = true;
                break;
            case Ydb::Table::TransactionSettings::TX_MODE_NOT_SET:
                YQL_ENSURE(false, "tx_mode not set, settings: " << settings);
                break;
        };
    }

    void RemoveOldTransactions() {
        if (ExplicitTransactions.Size() == *Config->_KqpMaxActiveTxPerSession.Get()) {
            auto it = ExplicitTransactions.FindOldest();
            auto idleDuration = TInstant::Now() - it.Value()->LastAccessTime;
            if (idleDuration.Seconds() >= *Config->_KqpTxIdleTimeoutSec.Get()) {
                it.Value()->Invalidate();
                AbortedTransactions.emplace_back(std::move(it.Value()));
                ExplicitTransactions.Erase(it);
            } else {
                auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
                std::vector<TIssue> issues{
                    YqlIssue({}, TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS)
                };
                ythrow TRequestFail(requestInfo, Ydb::StatusIds::BAD_SESSION, MessageFromIssues(issues))
                    << "Too many transactions, current active: " << ExplicitTransactions.Size()
                    << " MaxTxPerSession: " << *Config->_KqpMaxActiveTxPerSession.Get();
            }
        }
    }

    void CreateNewTx() {
        RemoveOldTransactions();
        auto success = ExplicitTransactions.Insert(std::make_pair(QueryState->TxId, QueryState->TxCtx));
        YQL_ENSURE(success);
    }

    void BeginTx(const Ydb::Table::TransactionSettings& settings) {
        QueryState->TxId = CreateGuidAsString();
        QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false);
        SetIsolationLevel(settings);
        CreateNewTx();
    }

    std::pair<bool, TIssues> ApplyTableOperations(TKqpTransactionContext* txCtx, const NKqpProto::TKqpPhyQuery& query) {
        TVector<NKqpProto::TKqpTableOp> operations(query.GetTableOps().begin(), query.GetTableOps().end());
        TVector<NKqpProto::TKqpTableInfo> tableInfos(query.GetTableInfos().begin(), query.GetTableInfos().end());

        auto isolationLevel = *txCtx->EffectiveIsolationLevel;
        bool strictDml = Config->StrictDml.Get(Settings.Cluster).GetOrElse(false);

        TExprContext ctx;
        bool success = txCtx->ApplyTableOperations(operations, tableInfos, isolationLevel, strictDml, EKikimrQueryType::Dml, ctx);
        return {success, ctx.IssueManager.GetIssues()};
    }

    bool PrepareQueryContext() {
        YQL_ENSURE(QueryState);
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& queryRequest = QueryState->Request;

        if (queryRequest.HasTxControl()) {
            auto& txControl = queryRequest.GetTxControl();

            QueryState->Commit = txControl.commit_tx();
            switch (txControl.tx_selector_case()) {
                case Ydb::Table::TransactionControl::kTxId: {
                    TString txId = txControl.tx_id();
                    auto it = ExplicitTransactions.Find(txId);
                    if (it == ExplicitTransactions.End()) {
                        std::vector<TIssue> issues{YqlIssue(TPosition(), TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND,
                            TStringBuilder() << "Transaction not found: " << QueryState->TxId)};
                        ReplyQueryError(requestInfo, Ydb::StatusIds::NOT_FOUND, "", MessageFromIssues(issues));
                        return false;
                    }
                    QueryState->TxCtx = *it;
                    QueryState->TxId = txId;
                    QueryState->TxCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                    break;
                }
                case Ydb::Table::TransactionControl::kBeginTx: {
                    BeginTx(txControl.begin_tx());
                    break;
               }
               case Ydb::Table::TransactionControl::TX_SELECTOR_NOT_SET:
                   YQL_ENSURE(false);
            }
        } else {
            QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false);
            QueryState->TxCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;
        }

        auto& queryCtx = QueryState->QueryCtx;
        queryCtx->TimeProvider = TAppData::TimeProvider;
        queryCtx->RandomProvider = TAppData::RandomProvider;

        const NKqpProto::TKqpPhyQuery& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        auto [success, issues] = ApplyTableOperations(QueryState->TxCtx.Get(), phyQuery);
        if (!success) {
            YQL_ENSURE(!issues.Empty());
            ReplyQueryError(requestInfo, GetYdbStatus(issues), "", MessageFromIssues(issues));
            return false;
        }

        auto action = queryRequest.GetAction();
        auto queryType = queryRequest.GetType();

        if (action == NKikimrKqp::QUERY_ACTION_EXECUTE && queryType == NKikimrKqp::QUERY_TYPE_SQL_DML) {
            queryType = NKikimrKqp::QUERY_TYPE_PREPARED_DML;
            action = NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED;
        }

        YQL_ENSURE(action == NKikimrKqp::QUERY_ACTION_EXECUTE || action == NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED,
                "Unexpected query action, expected: QUERY_ACTION_EXECUTE_PREPARED, got: " << action);
        YQL_ENSURE(queryType == NKikimrKqp::QUERY_TYPE_PREPARED_DML || queryType == NKikimrKqp::QUERY_TYPE_SQL_SCAN,
                "Unexpected query type, expected: QUERY_TYPE_PREPARED_DML, got: " << queryType);

        ParseParameters(std::move(*QueryState->Request.MutableParameters()), queryCtx->Parameters);
        return true;
    }

    static void ParseParameters(NKikimrMiniKQL::TParams&& parameters, TKikimrParamsMap& map) {
        if (!parameters.HasType()) {
            return;
        }

        YQL_ENSURE(parameters.GetType().GetKind() == NKikimrMiniKQL::Struct, "Expected struct as query parameters type");
        auto& structType = *parameters.MutableType()->MutableStruct();

        for (ui32 i = 0; i < structType.MemberSize(); ++i) {
            const auto& memberName = structType.GetMember(i).GetName();
            YQL_ENSURE(i < parameters.GetValue().StructSize(), "Missing value for parameter: " << memberName);

            NKikimrMiniKQL::TParams param;
            param.MutableType()->Swap(structType.MutableMember(i)->MutableType());
            param.MutableValue()->Swap(parameters.MutableValue()->MutableStruct(i));

            auto result = map.emplace(memberName, std::move(param));
            YQL_ENSURE(result.second, "Duplicate parameter: " << memberName);
        }
    }

    bool ReplyPrepareResult(const TKqpCompileResult::TConstPtr& compileResult) {
        auto responseEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);

        auto ru = NRuCalc::CpuTimeToUnit(TDuration::MicroSeconds(QueryState->CompileStats.GetCpuTimeUs()));
        responseEv->Record.GetRef().SetConsumedRu(ru);

        return Reply(std::move(responseEv));
    }

    IKqpGateway::TExecPhysicalRequest PreparePhysicalRequest(TKqpQueryState *queryState) {
        IKqpGateway::TExecPhysicalRequest request;

        auto now = TAppData::TimeProvider->Now();
        if (queryState) {
            request.Timeout = queryState->QueryDeadlines.TimeoutAt - now;
            if (auto cancelAt = queryState->QueryDeadlines.CancelAt) {
                request.CancelAfter = cancelAt - now;
            }

            EKikimrStatsMode statsMode = GetStatsModeInt(queryState->Request, EKikimrStatsMode::Basic);
            request.StatsMode = GetStatsMode(statsMode);

            request.Snapshot = queryState->TxCtx->GetSnapshot();
            request.IsolationLevel = *queryState->TxCtx->EffectiveIsolationLevel;
        } else {
            request.IsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
        }

        const auto& limits = GetQueryLimits(Settings);
        request.MaxAffectedShards = limits.PhaseLimits.AffectedShardsLimit;
        request.TotalReadSizeLimitBytes = limits.PhaseLimits.TotalReadSizeLimitBytes;
        request.MkqlMemoryLimit = limits.PhaseLimits.ComputeNodeMemoryLimitBytes;

        return request;
    }

    IKqpGateway::TExecPhysicalRequest PrepareScanRequest(TKqpQueryState *queryState) {
        IKqpGateway::TExecPhysicalRequest request;

        request.Timeout = queryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();
        if (!request.Timeout) {
            // TODO: Just cancel request.
            request.Timeout = TDuration::MilliSeconds(1);
        }
        request.MaxComputeActors = Config->_KqpMaxComputeActors.Get().GetRef();
        EKikimrStatsMode statsMode = GetStatsModeInt(queryState->Request, EKikimrStatsMode::Basic);
        request.StatsMode = GetStatsMode(statsMode);
        request.DisableLlvmForUdfStages = Config->DisableLlvmForUdfStages();
        request.LlvmEnabled = Config->GetEnableLlvm() != EOptionalFlag::Disabled;
        request.Snapshot = QueryState->TxCtx->GetSnapshot();

        return request;
    }

    NKikimrMiniKQL::TParams* ValidateParameter(const TString& name, const NKikimrMiniKQL::TType& type) {
        auto& queryCtx = QueryState->QueryCtx;
        YQL_ENSURE(queryCtx);
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        auto parameter = queryCtx->Parameters.FindPtr(name);
        if (!parameter) {
            if (type.GetKind() == NKikimrMiniKQL::ETypeKind::Optional) {
                auto& newParameter = queryCtx->Parameters[name];
                newParameter.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Optional);
                *newParameter.MutableType()->MutableOptional()->MutableItem() = type.GetOptional().GetItem();

                return &newParameter;
            }

            ythrow TRequestFail(requestInfo, Ydb::StatusIds::BAD_REQUEST) << "Missing value for parameter: " << name;
            return nullptr;
        }

        if (!IsSameType(parameter->GetType(), type)) {
            ythrow TRequestFail(requestInfo, Ydb::StatusIds::BAD_REQUEST) << "Parameter " << name << " type mismatch, expected: " << type
                << ", actual: " << parameter->GetType();
        }

        return parameter;
    }

    TKqpParamsMap PrepareParameters(const NKqpProto::TKqpPhyTx& tx) {
        for (const auto& paramDesc : QueryState->PreparedQuery->GetParameters()) {
            ValidateParameter(paramDesc.GetName(), paramDesc.GetType());
        }

        TKqpParamsMap paramsMap(QueryState->QueryCtx);

        for (const auto& paramBinding : tx.GetParamBindings()) {

            try {
                auto& qCtx = QueryState->QueryCtx;
                auto it = paramsMap.Values.emplace(paramBinding.GetName(),
                    *GetParamValue(/*ensure*/ true, *qCtx, qCtx->TxResults, paramBinding));
                YQL_ENSURE(it.second);
            } catch (const yexception& ex) {
                auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
                ythrow TRequestFail(requestInfo, Ydb::StatusIds::BAD_REQUEST) << ex.what();
            }
        }

        return paramsMap;
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

    TKqpParamsMap GetParamsRefMap(const TParamValueMap& map) {
        TKqpParamsMap paramsMap(QueryState->QueryCtx);
        for (auto& [k, v] : map) {
            auto res = paramsMap.Values.emplace(k, NYql::NDq::TMkqlValueRef(v));
            YQL_ENSURE(res.second);
        }

        return paramsMap;
    }

    TParamValueMap CreateKqpValueMap(const NKqpProto::TKqpPhyTx& tx) {
        TParamValueMap paramsMap;
        for (const auto& paramBinding : tx.GetParamBindings()) {
            auto& qCtx = QueryState->QueryCtx;
            auto paramValueRef = *GetParamValue(/*ensure*/ true, *qCtx, qCtx->TxResults,
                    paramBinding);

            NKikimrMiniKQL::TParams param;
            param.MutableType()->CopyFrom(paramValueRef.GetType());
            param.MutableValue()->CopyFrom(paramValueRef.GetValue());

            auto [it, success] = paramsMap.emplace(paramBinding.GetName(), std::move(param));
            YQL_ENSURE(success);
        }
        return paramsMap;
    }

    bool CheckTransacionLocks() {
        auto& txCtx = *QueryState->TxCtx;
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        if (!txCtx.DeferredEffects.Empty() && txCtx.Locks.Broken()) {
            std::vector<TIssue> issues{
                YqlIssue({}, TIssuesIds::KIKIMR_LOCKS_INVALIDATED, "Transaction locks invalidated.")
            };
            ReplyQueryError(requestInfo, Ydb::StatusIds::ABORTED, "tx has deferred effects, but lock is broken",
                MessageFromIssues(issues));
            return false;
        }
        return true;
    }

    void ExecuteOrDefer() {
        auto& txCtx = *QueryState->TxCtx;

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        if (!CheckTransacionLocks()) {
            return;
        }

        const NKqpProto::TKqpPhyQuery& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        YQL_ENSURE(QueryState->CurrentTx < phyQuery.TransactionsSize());

        auto tx = std::shared_ptr<const NKqpProto::TKqpPhyTx>(QueryState->PreparedQuery,
                &phyQuery.GetTransactions(QueryState->CurrentTx));

        while (tx->GetHasEffects()) {
            if (!txCtx.AddDeferredEffect(tx, CreateKqpValueMap(*tx))) {
                ythrow TRequestFail(requestInfo, Ydb::StatusIds::BAD_REQUEST) << "Failed to mix queries with old- and new- engines";
            }
            if (QueryState->CurrentTx + 1 < phyQuery.TransactionsSize()) {
                LWTRACK(KqpPhyQueryDefer, QueryState->Orbit, QueryState->CurrentTx);
                ++QueryState->CurrentTx;
                tx = std::shared_ptr<const NKqpProto::TKqpPhyTx>(QueryState->PreparedQuery,
                        &phyQuery.GetTransactions(QueryState->CurrentTx));
            } else {
                tx = nullptr;
                break;
            }
        }

        bool commit = QueryState->Commit && QueryState->CurrentTx == phyQuery.TransactionsSize() - 1;
        if (tx || commit) {
            bool replied = ExecutePhyTx(&phyQuery, std::move(tx), commit);
            if (!replied) {
                ++QueryState->CurrentTx;
            }
        } else {
            ReplySuccess();
        }
    }

    bool ExecutePhyTx(const NKqpProto::TKqpPhyQuery* query, std::shared_ptr<const NKqpProto::TKqpPhyTx> tx, bool commit) {
        auto& txCtx = *QueryState->TxCtx;
        auto request = (query && query->GetType() == NKqpProto::TKqpPhyQuery::TYPE_SCAN)
            ? PrepareScanRequest(QueryState.get())
            : PreparePhysicalRequest(QueryState.get());
        LOG_D("ExecutePhyTx, tx: " << (void*)tx.get() << " commit: " << commit
                << " txCtx.DeferredEffects.size(): " << txCtx.DeferredEffects.Size());

        if (!CheckTransacionLocks()) {
            return true;
        }

        // TODO Handle timeouts -- request.Timeout, request.CancelAfter

        if (tx) {
            switch (tx->GetType()) {
                case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                case NKqpProto::TKqpPhyTx::TYPE_DATA:
                case NKqpProto::TKqpPhyTx::TYPE_SCAN:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected physical tx type in data query: " << (ui32)tx->GetType());
            }

            request.Transactions.emplace_back(tx, PrepareParameters(*tx));
        } else {
            YQL_ENSURE(commit);
            if (txCtx.DeferredEffects.Empty() && !txCtx.Locks.HasLocks()) {
                ReplySuccess();
                return true;
            }
        }

        if (commit) {
            for (const auto& effect : txCtx.DeferredEffects) {
                YQL_ENSURE(!effect.Node);
                YQL_ENSURE(effect.PhysicalTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_DATA);
                request.Transactions.emplace_back(effect.PhysicalTx, GetParamsRefMap(effect.Params));

                LOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction,"
                       << " current Transactions.size(): " << request.Transactions.size());
            }

            if (!txCtx.DeferredEffects.Empty()) {
                request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
            }

            if (txCtx.Locks.HasLocks()) {
                request.ValidateLocks = !(txCtx.GetSnapshot().IsValid() && txCtx.DeferredEffects.Empty());
                request.EraseLocks = true;
                LOG_D("TExecPhysicalRequest, tx has locks, ValidateLocks: " << request.ValidateLocks
                        << " EraseLocks: " << request.EraseLocks);

                for (auto& [lockId, lock] : txCtx.Locks.LocksMap) {
                    request.Locks.emplace_back(lock.GetValueRef(txCtx.Locks.LockType));
                }
            }
        } else if (ShouldAcquireLocks(query)) {
            request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();
        }

        LWTRACK(KqpPhyQueryProposeTx, QueryState->Orbit, QueryState->CurrentTx, request.Transactions.size(),
                request.Locks.size(), request.AcquireLocksTxId.Defined());
        SendToExecuter(std::move(request));
        return false;
    }

    void SendToExecuter(IKqpGateway::TExecPhysicalRequest&& request) {
        auto executerActor = CreateKqpExecuter(std::move(request), Settings.Database,
                (QueryState && QueryState->UserToken) ? TMaybe<TString>(QueryState->UserToken) : Nothing(),
                RequestCounters);
        ExecuterId = TlsActivationContext->ExecutorThread.RegisterActor(executerActor);
        LOG_D("Created new KQP executer: " << ExecuterId);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(ExecuterId);
        Send(MakeTxProxyID(), ev.release());
    }


    void HandleNoop(TEvKqpExecuter::TEvExecuterProgress::TPtr& /*ev*/) {
    }

    void HandleExecute(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyBusy(ev);
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();
        LOG_D("TEvTxResponse, CurrentTx: " << QueryState->CurrentTx << " response: " << response->DebugString());
        ExecuterId = TActorId{};

        auto& txCtx = QueryState->TxCtx;
        if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
            LOG_I("TEvTxResponse has non-success status, CurrentTx: " << QueryState->CurrentTx
                << " response->DebugString(): " << response->DebugString());

            txCtx->Invalidate();
            AbortedTransactions.emplace_back(txCtx);
            RemoveTransaction(QueryState->TxId);

            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            TIssues issues;
            IssuesFromMessage(response->GetIssues(), issues);
            ReplyQueryError(requestInfo, GetYdbStatus(issues), "", *response->MutableIssues());
            return;
        }

        YQL_ENSURE(QueryState);
        LWTRACK(KqpPhyQueryTxResponse, QueryState->Orbit, QueryState->CurrentTx, response->GetResult().ResultsSize());
        // save tx results
        auto& txResult = *response->MutableResult();
        TVector<NKikimrMiniKQL::TResult> txResults;
        txResults.resize(txResult.ResultsSize());
        for (ui32 i = 0; i < txResult.ResultsSize(); ++i) {
            txResults[i].Swap(txResult.MutableResults(i));
        }

        QueryState->QueryCtx->TxResults.emplace_back(std::move(txResults));

        // locks merge
        if (txResult.HasLocks()) {
            const auto& locks = txResult.GetLocks();
            auto [success, issues] = MergeLocks(locks.GetType(), locks.GetValue(), *txCtx);
            if (!success) {
                if (!txCtx->GetSnapshot().IsValid() || !txCtx->DeferredEffects.Empty()) {

                    auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
                    ReplyQueryError(requestInfo, Ydb::StatusIds::ABORTED,  "Error while locks merge",
                        MessageFromIssues(issues));
                    return;
                }

                if (txCtx->GetSnapshot().IsValid()) {
                    txCtx->Locks.MarkBroken(issues.back());
                }
            }
        }

        bool scan = QueryState->Request.GetType() == NKikimrKqp::QUERY_TYPE_SQL_SCAN;
        if (scan) {
            if (QueryState->RequestActorId && txResult.HasStats()) {
                auto statsEv = MakeHolder<TEvKqpExecuter::TEvStreamProfile>();
                auto& record = statsEv->Record;

                record.MutableProfile()->Swap(txResult.MutableStats());
                Send(QueryState->RequestActorId, statsEv.Release());
            }
        } else {
            if (txResult.HasStats()) {
                auto* exec = QueryState->Stats.AddExecutions();
                exec->Swap(txResult.MutableStats());
            }
        }

        if (QueryState->PreparedQuery &&
                QueryState->CurrentTx < QueryState->PreparedQuery->GetPhysicalQuery().TransactionsSize()) {
            ExecuteOrDefer();
        } else {
            ReplySuccess();
        }
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        YQL_ENSURE(QueryState && QueryState->RequestActorId);
        LOG_D("TEvStreamData: " << ev->Get()->Record.DebugString());
        TlsActivationContext->Send(ev->Forward(QueryState->RequestActorId));
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        TlsActivationContext->Send(ev->Forward(ExecuterId));
    }

    void HandleExecute(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;

        const auto& issues = ev->Get()->GetIssues();
        LOG_I("Got TEvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode()));
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        ReplyQueryError(requestInfo, NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), "Got AbortExecution", MessageFromIssues(issues));
    }

    TString ExtractQueryText() const {
        auto compileResult = QueryState->CompileResult;
        if (compileResult) {
            if (compileResult->Query) {
                return compileResult->Query->Text;
            }
            return {};
        }
        return QueryState->Request.GetQuery();
    }

    void CollectSystemViewQueryStats(const NKqpProto::TKqpStatsQuery* stats, TDuration queryDuration,
        const TString& database, ui64 requestUnits)
    {
        auto type = QueryState->Request.GetType();
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING: {
                TString text = ExtractQueryText();
                if (IsQueryAllowedToLog(text)) {
                    auto userSID = NACLib::TUserToken(QueryState->UserToken).GetUserSID();
                    NSysView::CollectQueryStats(TlsActivationContext->AsActorContext(), stats, queryDuration, text,
                        userSID, QueryState->ParametersSize, database, type, requestUnits);
                }
                break;
            }
            default:
                break;
        }
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

    // TODO: Remove? Is it actual for NewEngine?
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

    void FillStats(NKikimrKqp::TEvQueryResponse* record) {
        auto *response = record->MutableResponse();
        auto* stats = &QueryState->Stats;

        stats->SetDurationUs((TInstant::Now() - QueryState->StartTime).MicroSeconds());
        //stats->SetWorkerCpuTimeUs(QueryState->CpuTime.MicroSeconds());
        if (QueryState->CompileResult) {
            stats->MutableCompilation()->Swap(&QueryState->CompileStats);
        }

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        YQL_ENSURE(QueryState);
        const auto& queryRequest = QueryState->Request;

        if (IsExecuteAction(queryRequest.GetAction())) {
            auto ru = NRuCalc::CalcRequestUnit(*stats);
            record->SetConsumedRu(ru);

            auto now = TInstant::Now();
            auto queryDuration = now - QueryState->StartTime;
            CollectSystemViewQueryStats(stats, queryDuration, queryRequest.GetDatabase(), ru);
            SlowLogQuery(TlsActivationContext->AsActorContext(), requestInfo, queryDuration, record->GetYdbStatus(),
                [record]() {
                    ui64 resultsSize = 0;
                    for (auto& result : record->GetResponse().GetResults()) {
                        resultsSize += result.ByteSize();
                    }
                    return resultsSize;
                }
            );
        }

        bool reportStats = (GetStatsModeInt(queryRequest, EKikimrStatsMode::None) != EKikimrStatsMode::None);
        if (reportStats) {
            FillQueryProfile(*stats, *response);
            response->SetQueryPlan(SerializeAnalyzePlan(*stats));

            response->MutableQueryStats()->Swap(stats);
        }
    }

    void FillTxInfo(NKikimrKqp::TQueryResponse* response) {
        YQL_ENSURE(QueryState);
        if (QueryState->Commit) {
            RemoveTransaction(QueryState->TxId);
            QueryState->TxId = "";
        }
        response->MutableTxMeta()->set_id(QueryState->TxId);

        if (QueryState->TxCtx) {
            auto txInfo = QueryState->TxCtx->GetInfo();
            LOG_I("txInfo"
                << " Status: " << txInfo.Status
                << " Kind: " << txInfo.Kind
                << " TotalDuration: " << txInfo.TotalDuration.SecondsFloat()*1e3
                << " ServerDuration: " << txInfo.ServerDuration.SecondsFloat()*1e3
                << " QueriesCount: " << txInfo.QueriesCount);
            Counters->ReportTransaction(Settings.DbCounters, txInfo);
        }
    }

    void ReplySuccess() {
        // return result
        auto resEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        std::shared_ptr<google::protobuf::Arena> arena(new google::protobuf::Arena());
        resEv->Record.Realloc(arena);
        auto *record = &resEv->Record.GetRef();
        auto *response = record->MutableResponse();

        FillStats(record);

        if (QueryState->Commit) {
            ResetTxState();
        }

        FillTxInfo(response);

        bool replyQueryId = false;
        bool replyQueryParameters = false;
        auto& queryRequest = QueryState->Request;
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
            YQL_ENSURE(QueryState->PreparedQuery);
            response->MutableQueryParameters()->CopyFrom(
                    QueryState->PreparedQuery->GetParameters());
        }

        if (replyQueryId) {
            TString queryId;
            if (QueryState->CompileResult) {
                queryId = QueryState->CompileResult->Uid;
            } else {
                YQL_ENSURE(!Settings.LongSession);
                Y_PROTOBUF_SUPPRESS_NODISCARD QueryState->PreparedQuery->SerializeToString(&queryId);
            }

            response->SetPreparedQuery(queryId);
        }

        if (QueryState->PreparedQuery) {
            auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            for (auto& rb : phyQuery.GetResultBindings()) {
                auto txIndex = rb.GetTxResultBinding().GetTxIndex();
                auto resultIndex = rb.GetTxResultBinding().GetResultIndex();

                auto& txResults = QueryState->QueryCtx->TxResults;
                YQL_ENSURE(txIndex < txResults.size());
                YQL_ENSURE(resultIndex < txResults[txIndex].size());

                IDataProvider::TFillSettings fillSettings;
                //TODO: shoud it be taken from PreparedQuery->GetResults().GetRowsLimit() ?
                fillSettings.RowsLimitPerWrite = Config->_ResultRowsLimit.Get().GetRef();
                auto* protoRes = KikimrResultToProto(txResults[txIndex][resultIndex], {}, fillSettings, arena.get());
                response->AddResults()->Swap(protoRes);
            }
        }

        resEv->Record.GetRef().SetYdbStatus(Ydb::StatusIds::SUCCESS);
        LOG_D("Reply for action: " << queryRequest.GetAction() << " with SUCCESS status");
        Reply(std::move(resEv));

        LWTRACK(KqpQueryReplySuccess, QueryState->Orbit, arena->SpaceUsed());

        Cleanup();
    }

    bool ReplyQueryCompileError(const TKqpCompileResult::TConstPtr& compileResult) {
        auto responseEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);

        auto& queryRequest = QueryState->Request;
        TString txId = "";
        if (queryRequest.HasTxControl()) {
            auto& txControl = queryRequest.GetTxControl();

            if (txControl.tx_selector_case() == Ydb::Table::TransactionControl::kTxId) {
                txId = txControl.tx_id();
            }
        }

        LOG_W("ReplyQueryCompileError, status" << compileResult->Status << " remove tx with tx_id: " << txId);
        auto txCtx = FindTransaction(txId);
        if (txCtx) {
            txCtx->Invalidate();
            AbortedTransactions.emplace_back(txCtx);
            RemoveTransaction(txId);
        }
        txId = "";

        auto* record = &responseEv->Record.GetRef();
        FillTxInfo(record->MutableResponse());
        record->SetConsumedRu(1);

        return Reply(std::move(responseEv));
    }

    void ReplyBusy(TEvKqp::TEvQueryRequest::TPtr& ev) {

        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());

        ui64 proxyRequestId = ev->Cookie;

        auto busyStatus = Settings.Service.GetUseSessionBusyStatus()
            ? Ydb::StatusIds::SESSION_BUSY
            : Ydb::StatusIds::PRECONDITION_FAILED;

        TString message = "Pending previous query completion";
        LOG_W(requestInfo << " " << message);

        auto response = TEvKqp::TEvProcessResponse::Error(busyStatus, message);

        //AddTrailingInfo(response->Record);
        Send(ev->Sender, response.Release(), 0, proxyRequestId);
    }

    bool Reply(std::unique_ptr<TEvKqp::TEvQueryResponse> responseEv) {
        YQL_ENSURE(QueryState);

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& queryRequest = QueryState->Request;
        auto queryDuration = TInstant::Now() - QueryState->StartTime;
        YQL_ENSURE(Counters);
        Counters->ReportQueryLatency(Settings.DbCounters, queryRequest.GetAction(), queryDuration);

        auto& record = responseEv->Record.GetRef();
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        response.SetSessionId(SessionId);

        Send(QueryState->Sender, responseEv.release(), 0, QueryState->ProxyRequestId);
        LOG_D(requestInfo << "Sent query response back to proxy, proxyRequestId: " << QueryState->ProxyRequestId
            << ", proxyId: " << QueryState->Sender.ToString());

        if (status == Ydb::StatusIds::INTERNAL_ERROR) {
            LOG_D(requestInfo << "SessionActor destroyed due to internal error");
            Counters->ReportSessionActorClosedError(Settings.DbCounters);
            return false;
        }
        if (status == Ydb::StatusIds::BAD_SESSION) {
            LOG_D(requestInfo << "SessionActor destroyed due to session error");
            Counters->ReportSessionActorClosedError(Settings.DbCounters);
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

    void Handle(TEvKqp::TEvPingSessionRequest::TPtr& ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto& evRecord = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(evRecord.GetTraceId(), evRecord.GetRequest().GetSessionId());
        YQL_ENSURE(requestInfo.GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << requestInfo.GetSessionId());

        auto result = std::make_unique<TEvKqp::TEvPingSessionResponse>();
        auto& record = result->Record;
        record.SetStatus(Ydb::StatusIds::SUCCESS);
        auto sessionStatus = CurrentStateFunc() == &TThis::ReadyState
            ? Ydb::Table::KeepAliveResult::SESSION_STATUS_READY
            : Ydb::Table::KeepAliveResult::SESSION_STATUS_BUSY;
        record.MutableResponse()->SetSessionStatus(sessionStatus);
        StartIdleTimer();

        Send(ev->Sender, result.release(), 0, proxyRequestId);
    }

    void Handle(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        if (QueryState) {
            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            ReplyQueryError(requestInfo, Ydb::StatusIds::SESSION_EXPIRED,
                    "Request cancelled due to explicit session close request");
            // TODO Remove cleanup from ReplyQueryError since now it is possible
            // for TxCtx in ExplicitTransactions to leak
        }
        if (CleanupCtx) {
            CleanupCtx->Final = true;
        } else {
            FinalCleanup();
        }
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

    void Handle(TEvKqp::TEvIdleTimeout::TPtr& ev) {
        auto timerId = ev->Get()->TimerId;
        LOG_D("Received TEvIdleTimeout in ready state, timer id: "
            << timerId << ", sender: " << ev->Sender);

        if (timerId == IdleTimerId) {
            LOG_N(TKqpRequestInfo("", SessionId) << "SessionActor idle timeout, worker destroyed");
            Counters->ReportSessionActorClosedIdle(Settings.DbCounters);
            FinalCleanup();
        }
    }

    void SendRollbackRequest(TKqpTransactionContext* txCtx) {
        auto request = PreparePhysicalRequest(nullptr);

        request.EraseLocks = true;
        request.ValidateLocks = false;

        // Should tx with empty LocksMap be aborted?
        for (auto& [lockId, lock] : txCtx->Locks.LocksMap) {
            request.Locks.emplace_back(lock.GetValueRef(txCtx->Locks.LockType));
        }
        SendToExecuter(std::move(request));
    }

    void ResetTxState() {
        if (QueryState->TxCtx) {
            QueryState->TxCtx->ClearDeferredEffects();
            QueryState->TxCtx->Locks.Clear();
            QueryState->TxCtx->Finish();
        }
    }

    void FinalCleanup() {
        Cleanup(true);
    }

    void HandleNoop(TEvKqp::TEvIdleTimeout::TPtr&) {
    }

    void HandleCleanup(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto& response = ev->Get()->Record.GetResponse();
        // TODO accumulate issues and compute overall status
        YQL_ENSURE(response.GetStatus() == Ydb::StatusIds::SUCCESS);
        YQL_ENSURE(CleanupCtx);
        ++CleanupCtx->AbortedTransactionsCount;
        if (CleanupCtx->AbortedTransactionsCount == CleanupCtx->TransactionsToBeAborted) {
            EndCleanup(CleanupCtx->Final);
        }
    }

    void EndCleanup(bool isFinal) {
        LOG_D("EndCleanup, isFinal: " << isFinal);
        if (isFinal) {
            auto lifeSpan = TInstant::Now() - CreationTime;
            Counters->ReportSessionActorFinished(Settings.DbCounters, lifeSpan);
            Counters->ReportQueriesPerSessionActor(Settings.DbCounters, QueryId);

            auto closeEv = std::make_unique<TEvKqp::TEvCloseSessionResponse>();
            closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            closeEv->Record.MutableResponse()->SetSessionId(SessionId);
            closeEv->Record.MutableResponse()->SetClosed(true);
            Send(Owner, closeEv.release());

            PassAway();
        } else {
            AbortedTransactions.clear();
            CleanupCtx.reset();
            StartIdleTimer();
            Become(&TKqpSessionActor::ReadyState);
        }
    }

    void Cleanup(bool isFinal = false) {
        if (isFinal) {
            for (auto it = ExplicitTransactions.Begin(); it != ExplicitTransactions.End(); ++it) {
                it.Value()->Invalidate();
                AbortedTransactions.emplace_back(std::move(it.Value()));
            }
            ExplicitTransactions.Clear();
        }

        if (AbortedTransactions.size()) {
            YQL_ENSURE(!CleanupCtx);
            CleanupCtx.reset(new TKqpCleanupCtx);
            CleanupCtx->Final = isFinal;
            CleanupCtx->AbortedTransactionsCount = 0;
            CleanupCtx->TransactionsToBeAborted = AbortedTransactions.size();
            // TODO Rollback one-by-one to avoid burst
            for (auto& txCtx : AbortedTransactions) {
                SendRollbackRequest(txCtx.Get());
            }
        }

        LOG_I("Cleanup start, isFinal: " << isFinal << " CleanupCtx: " << bool{CleanupCtx}
            << " AbortedTransactions.size(): " << AbortedTransactions.size());
        QueryState.reset();
        if (CleanupCtx) {
            Become(&TKqpSessionActor::CleanupState);
        } else {
            EndCleanup(isFinal);
        }
    }

    template<class T>
    static google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> MessageFromIssues(const T& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issueMessage;
        for (const auto& i : issues) {
            IssueToMessage(i, issueMessage.Add());
        }

        return issueMessage;
    }

    bool ReplyQueryError(const TKqpRequestInfo& requestInfo, Ydb::StatusIds::StatusCode ydbStatus,
        const TString& message, std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> issues = {})
    {
        LOG_W("Reply error on query request: " << requestInfo << " msg: " << message);

        auto ev = std::make_unique<TEvKqp::TEvQueryResponse>();
        ev->Record.GetRef().SetYdbStatus(ydbStatus);

        LWTRACK(KqpQueryReplyError, QueryState->Orbit, message);
        auto* response = ev->Record.GetRef().MutableResponse();

        auto *queryIssue = response->AddQueryIssues();
        if (issues) {
            queryIssue->Mutableissues()->Swap(&*issues);
        }
        IssueToMessage(TIssue{message}, queryIssue);

        if (QueryState) {
            if (QueryState->TxCtx) {
                QueryState->TxCtx->Invalidate();
            }

            FillTxInfo(response);
        }

        bool canContinue = Reply(std::move(ev));
        Cleanup(!canContinue);
        return canContinue;
    }

    STATEFN(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleReady);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);

                hFunc(TEvKqp::TEvQueryResponse, ForwardResponse);
            default:
                UnexpectedEvent("ReadyState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.RequestInfo, ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STATEFN(CompileState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleCompile);
                hFunc(TEvKqp::TEvCompileResponse, HandleCompile);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, HandleNoop);
            default:
                UnexpectedEvent("CompileState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.RequestInfo, ex.Status, ex.what(), ex.Issues);
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

                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop);
                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleExecute);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, HandleExecute);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, HandleNoop);
            default:
                UnexpectedEvent("ExecuteState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.RequestInfo, ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    // optional -- only if there were any AbortedTransactions
    STATEFN(CleanupState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleCleanup);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, HandleNoop);
            default:
                UnexpectedEvent("CleanupState", ev);
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
        if (QueryState) {
            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            ReplyQueryError(requestInfo, Ydb::StatusIds::INTERNAL_ERROR, message);
        } else {
            PassAway();
        }
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

    std::unique_ptr<TKqpQueryState> QueryState;
    std::unique_ptr<TKqpCleanupCtx> CleanupCtx;
    ui32 QueryId = 0;
    TKikimrConfiguration::TPtr Config;
    TLRUCache<TString, TIntrusivePtr<TKqpTransactionContext>> ExplicitTransactions;
    std::vector<TIntrusivePtr<TKqpTransactionContext>> AbortedTransactions;

    TActorId IdleTimerActorId;
    ui32 IdleTimerId = 0;
    TDuration IdleDuration;
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
