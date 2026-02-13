#include "kqp_session_actor.h"
#include "kqp_worker_common.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/kqp_data_integrity_trails.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/kqp/common/simple/query_ast.h>
#include <ydb/core/kqp/compile_service/kqp_compile_service.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/executer_actor/kqp_locks_helper.h>
#include <ydb/core/kqp/executer_actor/kqp_partitioned_executer.h>
#include <ydb/core/kqp/executer_actor/kqp_table_resolver.h>
#include <ydb/core/kqp/executer_actor/shards_resolver/kqp_shards_resolver.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/core/util/ulid.h>
#include <ydb/core/util/stlog.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/path.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/printf.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NSchemeCache;

namespace {

std::optional<TString> TryDecodeYdbSessionId(const TString& sessionId) {
    if (sessionId.empty()) {
        return std::nullopt;
    }

    try {
        NOperationId::TOperationId opId(sessionId);
        TString id;
        const auto& ids = opId.GetValue("id");
        if (ids.size() != 1) {
            return std::nullopt;
        }

        return TString{*ids[0]};
    } catch (...) {
        return std::nullopt;
    }

    return std::nullopt;
}

#define STLOG_C(MESSAGE, ...) STLOG(PRI_CRIT, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)
#define STLOG_E(MESSAGE, ...) STLOG(PRI_ERROR, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)
#define STLOG_W(MESSAGE, ...) STLOG(PRI_WARN, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)
#define STLOG_N(MESSAGE, ...) STLOG(PRI_NOTICE, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)
#define STLOG_I(MESSAGE, ...) STLOG(PRI_INFO, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)
#define STLOG_D(MESSAGE, ...) STLOG(PRI_DEBUG, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)
#define STLOG_T(MESSAGE, ...) STLOG(PRI_TRACE, NKikimrServices::KQP_SESSION, KQPSA, LogPrefix() << MESSAGE, __VA_ARGS__)

void FillColumnsMeta(const NKqpProto::TKqpPhyQuery& phyQuery, NKikimrKqp::TQueryResponse& resp) {
    for (size_t i = 0; i < phyQuery.ResultBindingsSize(); ++i) {
        const auto& binding = phyQuery.GetResultBindings(i);
        auto ydbRes = resp.AddYdbResults();
        ydbRes->mutable_columns()->CopyFrom(binding.GetResultSetMeta().columns());
    }
}

bool FillTableSinkSettings(NKikimrKqp::TKqpTableSinkSettings& settings, const NKqpProto::TKqpSink& sink) {
    if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink
        && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>())
    {
        return sink.GetInternalSink().GetSettings().UnpackTo(&settings);
    }

    return false;
}

bool IsBatchQuery(const NKqpProto::TKqpPhyQuery& physicalQuery) {
    NKikimrKqp::TKqpTableSinkSettings sinkSettings;
    for (const auto& tx : physicalQuery.GetTransactions()) {
        for (const auto& stage : tx.GetStages()) {
            for (auto& sink : stage.GetSinks()) {
                auto isFilledSettings = FillTableSinkSettings(sinkSettings, sink);
                if (isFilledSettings && sinkSettings.HasIsBatch() && sinkSettings.GetIsBatch()) {
                    return true;
                }
            }
        }
    }
    return false;
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

struct TKqpCleanupCtx {
    std::deque<TIntrusivePtr<TKqpTransactionContext>> TransactionsToBeAborted;
    bool IsWaitingForWorkerToClose = false;
    bool IsWaitingForWorkloadServiceCleanup = false;
    bool Final = false;
    TInstant Start = TInstant::Now();

    bool CleanupFinished() {
        return TransactionsToBeAborted.empty() && !IsWaitingForWorkerToClose && !IsWaitingForWorkloadServiceCleanup;
    }
};

class TKqpSessionActor : public TActorBootstrapped<TKqpSessionActor>, IActorExceptionHandler {

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

   TKqpSessionActor(const TActorId& owner,
            TKqpQueryCachePtr queryCache,
            std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager,
            std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> caFactory,
            const TString& sessionId, const TKqpSettings::TConstPtr& kqpSettings,
            const TKqpWorkerSettings& workerSettings,
            std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
            NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
            TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
            const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
            const TActorId& kqpTempTablesAgentActor,
            std::shared_ptr<NYql::NDq::IDqChannelService> channelService)
        : Owner(owner)
        , QueryCache(std::move(queryCache))
        , SessionId(sessionId)
        , ResourceManager_(std::move(resourceManager))
        , CaFactory_(std::move(caFactory))
        , Counters(counters)
        , Settings(workerSettings)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , ModuleResolverState(std::move(moduleResolverState))
        , FederatedQuerySetup(federatedQuerySetup)
        , KqpSettings(kqpSettings)
        , Config(CreateConfig(kqpSettings, workerSettings))
        , Transactions(*Config->_KqpMaxActiveTxPerSession.Get(), TDuration::Seconds(*Config->_KqpTxIdleTimeoutSec.Get()))
        , QueryServiceConfig(queryServiceConfig)
        , KqpTempTablesAgentActor(kqpTempTablesAgentActor)
        , GUCSettings(std::make_shared<TGUCSettings>())
        , ChannelService(channelService)
    {
        RequestCounters = MakeIntrusive<TKqpRequestCounters>();
        RequestCounters->Counters = Counters;
        RequestCounters->DbCounters = Settings.DbCounters;
        RequestCounters->TxProxyMon = MakeIntrusive<NTxProxy::TTxProxyMon>(AppData()->Counters);
        CompilationCookie = std::make_shared<std::atomic<bool>>(true);

        FillSettings.AllResultsBytesLimit = Nothing();
        FillSettings.RowsLimitPerWrite = Config->_ResultRowsLimit.Get();
        FillSettings.Format = IDataProvider::EResultFormat::Custom;
        FillSettings.FormatDetails = TString(KikimrMkqlProtoFormat);
        FillGUCSettings();

        auto optSessionId = TryDecodeYdbSessionId(SessionId);
        YQL_ENSURE(optSessionId, "Can't decode ydb session Id");

        TempTablesState.Database = Settings.Database;
        TempTablesState.TempDirName = TAppData::RandomProvider->GenUuid4().AsUuidString();
        STLOG_D("Create session actor",
            (ydb_session_id, *optSessionId),
            (temp_dir_name, TempTablesState.TempDirName),
            (trace_id, TraceId()));
    }

    void Bootstrap() {
        STLOG_D("Session actor bootstrapped",
            (trace_id, TraceId()));
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
            result << "LegacyTraceId: " << QueryState->UserRequestContext->TraceId << ", ";
        }
        return result;
    }

    TString TraceId() const {
        if (QueryState && QueryState->KqpSessionSpan) {
            return QueryState->KqpSessionSpan.GetTraceId().GetHexTraceId();
        }
        return TString();
    }

    void MakeNewQueryState(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ++QueryId;
        YQL_ENSURE(!QueryState);
        auto selfId = SelfId();
        auto as = TActivationContext::ActorSystem();
        ev->Get()->SetClientLostAction(selfId, as);
        QueryState = std::make_shared<TKqpQueryState>(
            ev, QueryId, Settings.Database, Settings.ApplicationName, Settings.Cluster, Settings.DbCounters, Settings.LongSession,
            Settings.TableService, Settings.QueryService, SessionId, AppData()->MonotonicTimeProvider->Now(), Settings.MutableExecuterConfig->RuntimeParameterSizeLimit.load());
        if (QueryState->UserRequestContext->TraceId.empty()) {
            QueryState->UserRequestContext->TraceId = UlidGen.Next().ToString();
        }
    }

    void PassRequestToResourcePool() {
        if (QueryState->UserRequestContext->PoolConfig) {
            STLOG_D("Request placed into pool from cache",
                (pool_id, QueryState->UserRequestContext->PoolId),
                (trace_id, TraceId()));
            CompileQuery();
            return;
        }

        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new NWorkload::TEvPlaceRequestIntoPool(
            QueryState->UserRequestContext->DatabaseId,
            SessionId,
            QueryState->UserRequestContext->PoolId,
            QueryState->UserToken
        ), IEventHandle::FlagTrackDelivery);

        QueryState->PoolHandlerActor = MakeKqpWorkloadServiceId(SelfId().NodeId());
        Become(&TKqpSessionActor::ExecuteState);
    }

    void ForwardRequest(TEvKqp::TEvQueryRequest::TPtr& ev) {
        if (!WorkerId) {
            std::unique_ptr<IActor> workerActor(CreateKqpWorkerActor(SelfId(), SessionId, KqpSettings, Settings,
                FederatedQuerySetup, ModuleResolverState, Counters, QueryServiceConfig, GUCSettings));
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

        QueryState->Commit = true;

        auto txId = TTxId::FromString(txControl.tx_id());
        auto txCtx = Transactions.Find(txId);
        STLOG_D("QueryRequest",
            (tx_control, txControl.DebugString()),
            (tx_ctx, (uintptr_t)txCtx.Get()),
            (trace_id, TraceId()));
        if (!txCtx) {
            ReplyTransactionNotFound(txControl.tx_id());
            return;
        }
        QueryState->TxCtx = std::move(txCtx);
        QueryState->QueryData = std::make_shared<TQueryData>(QueryState->TxCtx->TxAlloc);
        QueryState->TxId.SetValue(txId);
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
        STLOG_D("Got ClientLost event, send AbortExecution to executer",
            (executer_id, ExecuterId),
            (trace_id, TraceId()));

        if (ExecuterId) {
            auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Client lost"); // any status code can be here

            Send(ExecuterId, abortEv.Release());
        } else {
            Cleanup();
        }
    }

    void Handle(TEvKqp::TEvQueryRequest::TPtr& ev) {
        if (CurrentStateFunc() != &TThis::ReadyState) {
            ReplyBusy(ev);
            return;
        }

        ui64 proxyRequestId = ev->Cookie;
        YQL_ENSURE(ev->Get()->GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << ev->Get()->GetSessionId());

        NDataIntegrity::LogIntegrityTrails(ev, TlsActivationContext->AsActorContext());

        if (ev->Get()->HasYdbStatus() && ev->Get()->GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->GetQueryIssues(), issues);
            TString errMsg = issues.ToString();
            auto status = ev->Get()->GetYdbStatus();

            STLOG_N("Got invalid query request, reply with error",
                (status, status),
                (error_msg, errMsg),
                (trace_id, TraceId()));
            ReplyProcessError(ev, status, errMsg);
            return;
        }

        if (ShutdownState && ShutdownState->SoftTimeoutReached()) {
            // we reached the soft timeout, so at this point we don't allow to accept new queries for session.
            STLOG_N("System shutdown requested: soft timeout reached, no queries can be accepted",
                (trace_id, TraceId()));
            ReplyProcessError(ev, Ydb::StatusIds::BAD_SESSION, "Session is under shutdown");
            CleanupAndPassAway();
            return;
        }

        MakeNewQueryState(ev);
        TTimerGuard timer(this);
        YQL_ENSURE(QueryState->GetDatabase() == Settings.Database,
                "Wrong database, expected:" << Settings.Database << ", got: " << QueryState->GetDatabase());

        auto action = QueryState->GetAction();

        LWTRACK(KqpSessionQueryRequest,
            QueryState->Orbit,
            QueryState->GetDatabase(),
            QueryState->GetType(),
            action,
            QueryState->GetQuery());

        STLOG_D("Received request",
            (proxy_request_id, proxyRequestId),
            (prepared, QueryState->HasPreparedQuery()),
            (has_tx_control, QueryState->HasTxControl()),
            (action, action),
            (type, QueryState->GetType()),
            (text, QueryState->GetQuery()),
            (rpc_actor, QueryState->RequestActorId),
            (database, QueryState->GetDatabase()),
            (database_id, QueryState->UserRequestContext->DatabaseId),
            (pool_id, QueryState->UserRequestContext->PoolId),
            (trace_id, TraceId()));

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXPLAIN:
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
                QueryState->CommandTagName = "BEGIN";
                ReplySuccess();
                return;
            }
            case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX: {
                QueryState->CommandTagName = "ROLLBACK";
                return RollbackTx();
            }
            case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
                QueryState->CommandTagName = "COMMIT";
                return CommitTx();

            // not supported yet
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
            case NKikimrKqp::QUERY_ACTION_PARSE:
                return ForwardRequest(ev);

            case NKikimrKqp::QUERY_ACTION_TOPIC:
                return AddOffsetsToTransaction();
        }

        QueryState->UpdateTempTablesState(TempTablesState);

        if (QueryState->UserRequestContext->PoolId) {
            PassRequestToResourcePool();
        } else {
            CompileQuery();
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == TKqpWorkloadServiceEvents::EvPlaceRequestIntoPool) {
            STLOG_I("Failed to deliver request to workload service",
                (trace_id, TraceId()));
            CompileQuery();
        }
    }

    void Handle(NWorkload::TEvContinueRequest::TPtr& ev) {
        YQL_ENSURE(QueryState);
        QueryState->ContinueTime = TInstant::Now();

        if (ev->Get()->Status == Ydb::StatusIds::UNSUPPORTED) {
            STLOG_T("Failed to place request in resource pool, feature flag is disabled",
                (trace_id, TraceId()));
            QueryState->UserRequestContext->PoolId.clear();
            CompileQuery();
            return;
        }

        const TString& poolId = ev->Get()->PoolId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS && !ev->Get()->IsDiskFull()) {
            google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
            NYql::IssuesToMessage(std::move(ev->Get()->Issues), &issues);
            ReplyQueryError(ev->Get()->Status, TStringBuilder() << "Query failed during adding/waiting in workload pool " << poolId, issues);
            return;
        }

        if (ev->Get()->IsDiskFull()) {
            STLOG_W("Database disks are without free space",
                (pool_id, poolId),(trace_id, TraceId()));
            FillQueryIssues(ev->Get()->Issues);
        }

        STLOG_D("Continue request",
            (pool_id, poolId),
            (trace_id, TraceId()));

        QueryState->PoolHandlerActor = ev->Sender;
        QueryState->UserRequestContext->PoolId = poolId;
        QueryState->UserRequestContext->PoolConfig = ev->Get()->PoolConfig;

        CompileQuery();
    }

    bool AreAllTheTopicsAndPartitionsKnown() const {
        if (QueryState->HasTopicOperations()) {
            const NKikimrKqp::TTopicOperationsRequest& operations = QueryState->GetTopicOperationsFromRequest();

            if (operations.HasTabletId()) {
                for (const auto& topic : operations.GetTopics()) {
                    auto path = CanonizePath(NPersQueue::GetFullTopicPath(QueryState->GetDatabase(), topic.path()));
                    for (const auto& partition : topic.partitions()) {
                        QueryState->TxCtx->TopicOperations.SetTabletId(path, partition.partition_id(), operations.GetTabletId());
                    }
                }
                return true;
            }

            for (const auto& topic : operations.GetTopics()) {
                auto path = CanonizePath(NPersQueue::GetFullTopicPath(QueryState->GetDatabase(), topic.path()));

                for (const auto& partition : topic.partitions()) {
                    if (!QueryState->TxCtx->TopicOperations.HasThisPartitionAlreadyBeenAdded(path, partition.partition_id())) {
                        return false;
                    }
                }
            }
        }

        if (QueryState->HasKafkaApiOperations()) {
            const NKikimrKqp::TKafkaApiOperationsRequest& operations = QueryState->GetKafkaApiOperationsFromRequest();
            for (const auto& topicAndPartition : operations.GetPartitionsInTxn()) {
                auto path = CanonizePath(NPersQueue::GetFullTopicPath(QueryState->GetDatabase(), topicAndPartition.GetTopicPath()));

                if (!QueryState->TxCtx->TopicOperations.HasThisPartitionAlreadyBeenAdded(path, topicAndPartition.GetPartitionId())) {
                    return false;
                }
            }

            for (const auto& offsetInRequest : operations.GetOffsetsInTxn()) {
                auto path = CanonizePath(NPersQueue::GetFullTopicPath(QueryState->GetDatabase(), offsetInRequest.GetTopicPath()));

                if (!QueryState->TxCtx->TopicOperations.HasThisPartitionAlreadyBeenAdded(path, offsetInRequest.GetPartitionId())) {
                    return false;
                }
            }
        }

        return true;
    }

    void AddOffsetsToTransaction() {
        YQL_ENSURE(QueryState);
        if (!PrepareQueryTransaction()) {
            return;
        }

        QueryState->FillTopicOperations();

        if (!AreAllTheTopicsAndPartitionsKnown()) {
            auto navigate = QueryState->BuildSchemeCacheNavigate();
            Become(&TKqpSessionActor::ExecuteState);
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
            return;
        }

        TString message;
        if (!QueryState->TryMergeTopicOffsets(QueryState->TopicOperations, message)) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << message;
        }

        if (HasTopicWriteOperations() && !HasTopicApiWriteOperations() && !HasKafkaApiWriteOperations()) {
            Become(&TKqpSessionActor::ExecuteState);
            Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId, 0, QueryState->QueryId);
            return;
        }

        ReplySuccess();
    }

    void CompileQuery() {
        YQL_ENSURE(QueryState);
        QueryState->CompilationRunning = true;

        Become(&TKqpSessionActor::ExecuteState);

        // quick path
        if (QueryState->TryGetFromCache(*QueryCache, GUCSettings, Counters, SelfId()) && !QueryState->CompileResult->NeedToSplit) {
            LWTRACK(KqpSessionQueryCompiled, QueryState->Orbit, TStringBuilder() << QueryState->CompileResult->Status);

            // even if we have successfully compilation result, it doesn't mean anything
            // in terms of current schema version of the table if response of compilation is from the cache.
            // because of that, we are forcing to run schema version check
            QueryState->CompileResult->IncUsage();
            if (QueryState->NeedCheckTableVersions()) {
                auto ev = QueryState->BuildNavigateKeySet();
                Send(MakeSchemeCacheID(), ev.release());
                return;
            }

            OnSuccessCompileRequest();
            return;
        }

        // TODO: in some cases we could reply right here (e.g. there is uid and query is missing), but
        // for extra sanity we make extra hop to the compile service, which might handle the issue better

        auto ev = QueryState->BuildCompileRequest(CompilationCookie, GUCSettings);
        STLOG_D("Sending CompileQuery request",
            (trace_id, TraceId()));

        Send(MakeKqpCompileServiceID(SelfId().NodeId()), ev.release(), 0, QueryState->QueryId,
            QueryState->KqpSessionSpan.GetTraceId());
    }

    void CompileSplittedQuery() {
        YQL_ENSURE(QueryState);
        auto ev = QueryState->BuildCompileSplittedRequest(CompilationCookie, GUCSettings);
        STLOG_D("Sending CompileSplittedQuery request",
            (trace_id, TraceId()));

        Send(MakeKqpCompileServiceID(SelfId().NodeId()), ev.release(), 0, QueryState->QueryId,
            QueryState->KqpSessionSpan.GetTraceId());
        Become(&TKqpSessionActor::ExecuteState);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get();
        YQL_ENSURE(response->Request);
        YQL_ENSURE(QueryState);
        // outdated response from scheme cache.
        // ignoring that.
        if (response->Request->Cookie < QueryId)
            return;

        if (QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_TOPIC) {
            ProcessTopicOps(ev);
            return;
        }

        // table versions are not the same. need the query recompilation.
        if (!QueryState->EnsureTableVersions(*response)) {
            if (QueryState->QueryPhysicalGraph) {
                // Query recompilation is not allowed with restore physical graph request
                ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED, "Restore query state failed, table versions are not the same");
                return;
            }

            auto ev = QueryState->BuildReCompileRequest(CompilationCookie, GUCSettings);
            Send(MakeKqpCompileServiceID(SelfId().NodeId()), ev.release(), 0, QueryState->QueryId,
                QueryState->KqpSessionSpan.GetTraceId());
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

            if (QueryState->CompileResult->NeedToSplit) {
                if (!QueryState->HasTxControl()) {
                    YQL_ENSURE(QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_EXECUTE || QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_EXPLAIN);
                    auto ev = QueryState->BuildSplitRequest(CompilationCookie, GUCSettings);
                    Send(MakeKqpCompileServiceID(SelfId().NodeId()), ev.release(), 0, QueryState->QueryId,
                        QueryState->KqpSessionSpan.GetTraceId());
                } else {
                    NYql::TIssues issues;
                    ReplyQueryError(
                        ::Ydb::StatusIds::StatusCode::StatusIds_StatusCode_BAD_REQUEST,
                        "CTAS statement can be executed only in NoTx mode.",
                        MessageFromIssues(issues));
                }
            } else {
                ReplyQueryCompileError();
            }
            return;
        }

        QueryState->CompileResult->IncUsage();
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

    void Handle(TEvKqp::TEvParseResponse::TPtr& ev) {
        QueryState->SaveAndCheckParseResult(std::move(*ev->Get()));
        CompileStatement();
    }

    void CompileStatement() {
        // quick path
        if (QueryState->TryGetFromCache(*QueryCache, GUCSettings, Counters, SelfId()) && !QueryState->CompileResult->NeedToSplit) {
            LWTRACK(KqpSessionQueryCompiled, QueryState->Orbit, TStringBuilder() << QueryState->CompileResult->Status);

            QueryState->CompileResult->IncUsage();
            // even if we have successfully compilation result, it doesn't mean anything
            // in terms of current schema version of the table if response of compilation is from the cache.
            // because of that, we are forcing to run schema version check
            if (QueryState->NeedCheckTableVersions()) {
                auto ev = QueryState->BuildNavigateKeySet();
                Send(MakeSchemeCacheID(), ev.release());
                return;
            }

            OnSuccessCompileRequest();
            return;
        }

        // TODO: in some cases we could reply right here (e.g. there is uid and query is missing), but
        // for extra sanity we make extra hop to the compile service, which might handle the issue better

        auto request = QueryState->BuildCompileRequest(CompilationCookie, GUCSettings);
        STLOG_D("Sending CompileQuery request (statement)",
            (trace_id, TraceId()));

        Send(MakeKqpCompileServiceID(SelfId().NodeId()), request.release(), 0, QueryState->QueryId,
            QueryState->KqpSessionSpan.GetTraceId());
    }

    void Handle(TEvKqp::TEvSplitResponse::TPtr& ev) {
        // outdated event from previous query.
        // ignoring that.
        if (ev->Cookie < QueryId) {
            return;
        }

        YQL_ENSURE(QueryState);
        TTimerGuard timer(this);
        if (!QueryState->SaveAndCheckSplitResult(ev->Get())) {
            ReplySplitError(ev->Get());
            return;
        }
        OnSuccessSplitRequest();
    }

    void OnSuccessSplitRequest() {
        YQL_ENSURE(ExecuteNextStatementPart());
    }

    bool ExecuteNextStatementPart() {
        if (QueryState->PrepareNextStatementPart()) {
            CompileSplittedQuery();
            return true;
        }
        return false;
    }

    void OnSuccessCompileRequest() {
        if (QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_EXPLAIN) {
            TVector<IKqpGateway::TPhysicalTxData> txs;
            std::map<TString, TString> secureParams;
            bool isValidParams = true;
            auto txAlloc = std::make_shared<TTxAllocatorState>(AppData()->FunctionRegistry, AppData()->TimeProvider, AppData()->RandomProvider);
            const auto& parameters = QueryState->GetYdbParameters();
            QueryState->QueryData = std::make_shared<TQueryData>(txAlloc);
            QueryState->QueryData->ParseParameters(parameters);

            for (const auto& tx : QueryState->PreparedQuery->GetTransactions()) {
                for (const auto& secretName : tx->GetSecretNames()) {
                    secureParams.emplace(secretName, "");
                }

                txs.emplace_back(tx, QueryState->QueryData);
                try {
                    QueryState->QueryData->PrepareParameters(tx, QueryState->PreparedQuery, txAlloc->TypeEnv);
                } catch (const yexception& ex) {
                    // TODO: throw exception instead of silent ignore?
                    // ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
                    isValidParams = false;
                }
            }

            if (!txs.empty() && txs.front().Body->GetType() != NKqpProto::TKqpPhyTx::TYPE_SCHEME && isValidParams) {
                auto tasksGraph = TKqpTasksGraph(Settings.Database, txs, txAlloc, {}, Settings.TableService.GetAggregationConfig(), RequestCounters, {}, nullptr);
                tasksGraph.GetMeta().AllowOlapDataQuery = Settings.TableService.GetAllowOlapDataQuery();
                tasksGraph.GetMeta().UserRequestContext = QueryState ? QueryState->UserRequestContext : MakeIntrusive<TUserRequestContext>("", Settings.Database, SessionId);
                tasksGraph.GetMeta().SecureParams = std::move(secureParams);

                // Resolve tables
                {
                    auto kqpTableResolver = CreateKqpTableResolver(SelfId(), 0, QueryState->UserToken, tasksGraph, true);
                    RegisterWithSameMailbox(kqpTableResolver);
                    auto resolveEv = co_await ActorWaitForEvent<TEvKqpExecuter::TEvTableResolveStatus>(0);
                    if (resolveEv->Get()->Status != Ydb::StatusIds::SUCCESS) {
                        ReplyResolveError(*resolveEv->Get());
                        co_return;
                    }
                }

                // Resolve shards
                {
                    TSet<ui64> shardIds;
                    for (const auto& [stageId, stageInfo] : tasksGraph.GetStagesInfo()) {
                        if (stageInfo.Meta.ShardKey) {
                            for (const auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                                shardIds.insert(partition.ShardId);
                            }
                        }
                    }

                    if (!shardIds.empty()) {
                        // TODO: initialize tasksGraph.GetMeta().UseFollowers ?
                        auto kqpShardsResolver = CreateKqpShardsResolver(SelfId(), 0, false, std::move(shardIds));
                        RegisterWithSameMailbox(kqpShardsResolver);
                        auto resolveEv = co_await ActorWaitForEvent<NShardResolver::TEvShardsResolveStatus>(0);
                        if (resolveEv->Get()->Status != Ydb::StatusIds::SUCCESS) {
                            ReplyResolveError(*resolveEv->Get());
                            co_return;
                        }

                        tasksGraph.ResolveShards(std::move(resolveEv->Get()->ShardsToNodes));
                    }
                }

                bool needResourcesSnapshot = tasksGraph.GetMeta().IsScan;
                if (!tasksGraph.GetMeta().IsScan) {
                    for (const auto& transaction : txs) {
                        for (const auto& stage : transaction.Body->GetStages()) {
                            if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kExternalSource) {
                                needResourcesSnapshot = true;
                            }
                        }
                    }
                    // TODO: better set `needResourcesSnapshot`
                }

                // Get resources snapshot
                TVector<NKikimrKqp::TKqpNodeResources> resourcesSnapshot;
                if (needResourcesSnapshot) {
                    resourcesSnapshot = GetKqpResourceManager()->GetClusterResources();
                }

                try {
                    tasksGraph.BuildAllTasks({}, resourcesSnapshot, nullptr, nullptr);
                    // TODO: fill tasks count into result
                    // Cerr << tasksGraph.DumpToString();
                } catch (const std::exception&) {
                    // TODO: send warning to user that we failed to estimate number of tasks.
                }
            }

            co_return ReplyPrepareResult();
        }

        if (QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE) {
            co_return ReplyPrepareResult();
        }

        if (!PrepareQueryContext()) {
            co_return;
        }

        Become(&TKqpSessionActor::ExecuteState);

        QueryState->TxCtx->OnBeginQuery();

        if (!CheckScriptExecutionState()) {
            co_return;
        }

        if (QueryState->NeedPersistentSnapshot()) {
            AcquirePersistentSnapshot();
            co_return;
        } else if (QueryState->NeedSnapshot(*Config)) {
            AcquireMvccSnapshot();
            co_return;
        }

        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void AcquirePersistentSnapshot() {
        STLOG_D("Acquire persistent snapshot",
            (trace_id, TraceId()));
        AcquireSnapshotSpan = NWilson::TSpan(TWilsonKqp::SessionAcquireSnapshot, QueryState->KqpSessionSpan.GetTraceId(),
            "SessionActor.AcquirePersistentSnapshot");
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = RegisterWithSameMailbox(snapMgr);

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(QueryState->PreparedQuery->GetQueryTables(), QueryId, std::move(QueryState->Orbit));
        Send(snapMgrActorId, ev.release());

        QueryState->TxCtx->SnapshotHandle.ManagingActor = snapMgrActorId;
    }

    void DiscardPersistentSnapshot(const IKqpGateway::TKqpSnapshotHandle& handle) {
        if (handle.ManagingActor) { // persistent snapshot was acquired
            Send(handle.ManagingActor, new TEvKqpSnapshot::TEvDiscardSnapshot());
        }
    }

    void AcquireMvccSnapshot() {
        AcquireSnapshotSpan = NWilson::TSpan(TWilsonKqp::SessionAcquireSnapshot, QueryState->KqpSessionSpan.GetTraceId(),
            "SessionActor.AcquireMvccSnapshot");
        STLOG_D("Acquire mvcc snapshot",
            (trace_id, TraceId()));
        auto timeout = QueryState->QueryDeadlines.TimeoutAt - TAppData::TimeProvider->Now();

        auto* snapMgr = CreateKqpSnapshotManager(Settings.Database, timeout);
        auto snapMgrActorId = RegisterWithSameMailbox(snapMgr);

        auto ev = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(QueryId, std::move(QueryState->Orbit));
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

    void Handle(TEvKqpSnapshot::TEvCreateSnapshotResponse::TPtr& ev) {
        if (ev->Cookie < QueryId || CurrentStateFunc() != &TThis::ExecuteState) {
            return;
        }

        TTimerGuard timer(this);

        auto *response = ev->Get();

        if (QueryState) {
            QueryState->Orbit = std::move(response->Orbit);
        }

        STLOG_T("Read snapshot result",
            (status, StatusForSnapshotError(response->Status)),
            (step, response->Snapshot.Step),
            (tx_id, response->Snapshot.TxId),
            (trace_id, TraceId()));
        if (response->Status != NKikimrIssues::TStatusIds::SUCCESS) {
            auto& issues = response->Issues;
            AcquireSnapshotSpan.EndError(issues.ToString());
            ReplyQueryError(StatusForSnapshotError(response->Status), "", MessageFromIssues(issues));
            return;
        }
        AcquireSnapshotSpan.EndOk();
        QueryState->TxCtx->SnapshotHandle.Snapshot = response->Snapshot;

        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDefer();
    }

    void BeginTx(const Ydb::Table::TransactionSettings& settings) {
        QueryState->TxId.SetValue(UlidGen.Next());
        QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false, AppData()->FunctionRegistry,
            AppData()->TimeProvider, AppData()->RandomProvider);

        auto& alloc = QueryState->TxCtx->TxAlloc;
        ui64 mkqlInitialLimit = Settings.MkqlInitialMemoryLimit;

        const auto& queryLimitsProto = Settings.TableService.GetQueryLimits();
        const auto& phaseLimitsProto = queryLimitsProto.GetPhaseLimits();
        ui64 mkqlMaxLimit = phaseLimitsProto.GetComputeNodeMemoryLimitBytes();
        mkqlMaxLimit = mkqlMaxLimit ? mkqlMaxLimit : ui64(Settings.MkqlMaxMemoryLimit);

        alloc->Alloc->SetLimit(mkqlInitialLimit);
        alloc->Alloc->Ref().SetIncreaseMemoryLimitCallback([this, &alloc, mkqlMaxLimit](ui64 currentLimit, ui64 required) {
            if (required < mkqlMaxLimit) {
                STLOG_D("Increase memory limit",
                    (current_limit, currentLimit),
                    (required, required),
                    (trace_id, TraceId()));
                alloc->Alloc->SetLimit(required);
            }
        });

        QueryState->QueryData = std::make_shared<TQueryData>(QueryState->TxCtx->TxAlloc);
        QueryState->TxCtx->SetIsolationLevel(settings);
        QueryState->TxCtx->OnBeginQuery();

        if (QueryState->TxCtx->EffectiveIsolationLevel == NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW
                && !Settings.TableService.GetEnableSnapshotIsolationRW()) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST)
                << "Writes aren't supported for Snapshot Isolation";
        }

        if (!Transactions.CreateNew(QueryState->TxId.GetValue(), QueryState->TxCtx)) {
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

    Ydb::Table::TransactionControl GetTxControlWithImplicitTx() {
        if (!QueryState->ImplicitTxId) {
            Ydb::Table::TransactionSettings settings;
            const auto isolation = QueryState->PreparedQuery->GetPhysicalQuery().GetDefaultTxMode() != NKqpProto::ISOLATION_LEVEL_UNDEFINED
                ? QueryState->PreparedQuery->GetPhysicalQuery().GetDefaultTxMode()
                : [&]() {
                    switch (Settings.TableService.GetDefaultTxMode()) {
                    case NKikimrConfig::TTableServiceConfig::SerializableRW:
                        return NKqpProto::ISOLATION_LEVEL_SERIALIZABLE;
                    case NKikimrConfig::TTableServiceConfig::SnapshotRW:
                        return NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW;
                    case NKikimrConfig::TTableServiceConfig::SnapshotRO:
                        return NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RO;
                    case NKikimrConfig::TTableServiceConfig::StaleRO:
                        return NKqpProto::ISOLATION_LEVEL_READ_STALE;
                    default:
                        ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST)
                            << "Unknown DefaultTxMode";
                    }
                }();
            switch (isolation) {
                case NKqpProto::ISOLATION_LEVEL_SERIALIZABLE:
                    settings.mutable_serializable_read_write();
                    break;
                case NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW:
                    settings.mutable_snapshot_read_write();
                    break;
                case NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RO:
                    settings.mutable_snapshot_read_only();
                    break;
                case NKqpProto::ISOLATION_LEVEL_READ_STALE:
                    settings.mutable_stale_read_only();
                    break;
                default:
                    ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST)
                        << "Unknown DefaultTxMode";
            }
            BeginTx(settings);
            QueryState->ImplicitTxId = QueryState->TxId;
        }
        Ydb::Table::TransactionControl control;
        control.set_commit_tx(QueryState->ProcessingLastStatement() && QueryState->ProcessingLastStatementPart());
        control.set_tx_id(QueryState->ImplicitTxId->GetValue().GetHumanStr());
        return control;
    }

    bool PrepareQueryTransaction() {
        const bool hasTxControl = QueryState->HasTxControl();
        if (hasTxControl || QueryState->HasImplicitTx()) {
            const auto& txControl = hasTxControl ? QueryState->GetTxControl() : GetTxControlWithImplicitTx();

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
                    if (hasTxControl) {
                        QueryState->TxId.SetValue(txId);
                    }
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
            QueryState->TxCtx->EffectiveIsolationLevel = NKqpProto::ISOLATION_LEVEL_UNDEFINED;
        }

        return true;
    }

    bool PrepareQueryContext() {
        YQL_ENSURE(QueryState);
        if (!PrepareQueryTransaction()) {
            return false;
        }

        const NKqpProto::TKqpPhyQuery& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();

        auto checkSchemeTx = [&]() {
            for (const auto &tx : phyQuery.GetTransactions()) {
                if (tx.GetType() != NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
                    return false;
                }
            }
            return true;
        };

        if (phyQuery.HasEnableOltpSink()) {
            if (!QueryState->TxCtx->EnableOltpSink) {
                QueryState->TxCtx->EnableOltpSink = phyQuery.GetEnableOltpSink();
            }
            if (QueryState->TxCtx->EnableOltpSink != phyQuery.GetEnableOltpSink()) {
                ReplyQueryError(Ydb::StatusIds::ABORTED,
                                "Transaction execution settings have been changed (EnableOltpSink).");
                return false;
            }
        } else {
            AFL_ENSURE(checkSchemeTx());
        }

        if (phyQuery.HasEnableOlapSink()) {
            if (!QueryState->TxCtx->EnableOlapSink) {
                QueryState->TxCtx->EnableOlapSink = phyQuery.GetEnableOlapSink();
            }
            if (QueryState->TxCtx->EnableOlapSink != phyQuery.GetEnableOlapSink()) {
                ReplyQueryError(Ydb::StatusIds::ABORTED,
                                "Transaction execution settings have been changed (EnableOlapSink).");
                return false;
            }
        } else {
            AFL_ENSURE(checkSchemeTx());
        }

        if (phyQuery.HasEnableHtapTx()) {
            if (!QueryState->TxCtx->EnableHtapTx) {
                QueryState->TxCtx->EnableHtapTx = phyQuery.GetEnableHtapTx();
            }
            if (QueryState->TxCtx->EnableHtapTx != phyQuery.GetEnableHtapTx()) {
                ReplyQueryError(Ydb::StatusIds::ABORTED,
                                "Transaction execution settings have been changed (EnableHtapTx).");
                return false;
            }
        } else {
            AFL_ENSURE(checkSchemeTx());
        }

        const bool hasOlapWrite = ::NKikimr::NKqp::HasOlapTableWriteInTx(phyQuery);
        const bool hasOltpWrite = ::NKikimr::NKqp::HasOltpTableWriteInTx(phyQuery);
        const bool hasOlapRead = ::NKikimr::NKqp::HasOlapTableReadInTx(phyQuery);
        const bool hasOltpRead = ::NKikimr::NKqp::HasOltpTableReadInTx(phyQuery);
        QueryState->TxCtx->HasOlapTable |= hasOlapWrite || hasOlapRead;
        QueryState->TxCtx->HasOltpTable |= hasOltpWrite || hasOltpRead;
        QueryState->TxCtx->HasTableWrite |= hasOlapWrite || hasOltpWrite;
        QueryState->TxCtx->HasTableRead |= hasOlapRead || hasOltpRead;

        if (QueryState->TxCtx->EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_SERIALIZABLE
                && QueryState->TxCtx->EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RO
                && QueryState->TxCtx->EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW
                && QueryState->GetType() != NKikimrKqp::QUERY_TYPE_SQL_SCAN
                && QueryState->GetType() != NKikimrKqp::QUERY_TYPE_AST_SCAN
                && QueryState->GetType() != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT
                && QueryState->TxCtx->HasOlapTable) {
            ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                            TStringBuilder()
                                << "Read from column-oriented tables is not supported in Online Read-Only or Stale Read-Only transaction modes. "
                                << "Use Serializable or Snapshot Read-Only mode instead.");
            return false;
        }

        if (QueryState->TxCtx->HasOlapTable && QueryState->TxCtx->HasOltpTable && QueryState->TxCtx->HasTableWrite
                && !QueryState->TxCtx->EnableHtapTx.value_or(false) && !QueryState->IsSplitted()) {
            ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                            "Write transactions that use both row-oriented and column-oriented tables are disabled at current time.");
            return false;
        }

        QueryState->TxCtx->SetTempTables(QueryState->TempTablesState);
        if (phyQuery.GetForceImmediateEffectsExecution()) {
            Counters->ForcedImmediateEffectsExecution->Inc();
        }
        QueryState->TxCtx->ApplyPhysicalQuery(phyQuery, QueryState->Commit);
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
            const auto& parameters = QueryState->GetYdbParameters();
            QueryState->QueryData->ParseParameters(parameters);
            if (QueryState->CompileResult && QueryState->CompileResult->GetAst() && QueryState->CompileResult->GetAst()->PgAutoParamValues) {
                const auto& params = dynamic_cast<TKqpAutoParamBuilder*>(QueryState->CompileResult->GetAst()->PgAutoParamValues.Get())->Values;
                for(const auto& [name, param] : params) {
                    if (!parameters.contains(name)) {
                        QueryState->QueryData->AddTypedValueParam(name, param);
                    }
                }
            }
        } catch(const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        } catch(const TMemoryLimitExceededException&) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << BuildMemoryLimitExceptionMessage();
        }
        return true;
    }

    void ReplyPrepareResult() {
        YQL_ENSURE(QueryState);
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(QueryState->CompileResult, QueryResponse->Record);

        auto ru = NRuCalc::CpuTimeToUnit(TDuration::MicroSeconds(QueryState->CompileStats.CpuTimeUs));
        auto& record = QueryResponse->Record;
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
            request.ProgressStatsPeriod = queryState->GetProgressStatsPeriod();
            request.QueryType = queryState->GetType();
            request.OutputChunkMaxSize = queryState->GetOutputChunkMaxSize();
            if (Y_LIKELY(queryState->PreparedQuery)) {
                ui64 resultSetsCount = queryState->PreparedQuery->GetPhysicalQuery().ResultBindingsSize();
                request.AllowTrailingResults = (resultSetsCount == 1 && queryState->Statements.size() <= 1);
                request.AllowTrailingResults &= (QueryState->RequestEv->GetSupportsStreamTrailingResult());
            }
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
            request.UserTraceId = queryState->UserRequestContext->TraceId;
        } else {
            request.IsolationLevel = NKqpProto::ISOLATION_LEVEL_SERIALIZABLE;
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

        AFL_ENSURE(queryState);
        request.Snapshot = queryState->TxCtx->GetSnapshot();
        request.IsolationLevel = *queryState->TxCtx->EffectiveIsolationLevel;

        return request;
    }

    IKqpGateway::TExecPhysicalRequest PrepareRequest(const TKqpPhyTxHolder::TConstPtr& tx, bool literal,
        TKqpQueryState *queryState)
    {
        AFL_ENSURE(queryState && QueryState);
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
        const auto& txCtx = *QueryState->TxCtx;
        const bool broken = txCtx.TxManager
            ? !!txCtx.TxManager->GetLockIssue()
            : txCtx.Locks.Broken();

        if (!txCtx.DeferredEffects.Empty() && broken) {
            ReplyQueryError(Ydb::StatusIds::ABORTED, "tx has deferred effects, but locks are broken",
                MessageFromIssues(std::vector<TIssue>{txCtx.TxManager ? *txCtx.TxManager->GetLockIssue() : txCtx.Locks.GetIssue()}));
            return false;
        }

        if (tx && tx->GetHasEffects() && broken) {
            ReplyQueryError(Ydb::StatusIds::ABORTED, "tx has effects, but locks are broken",
                MessageFromIssues(std::vector<TIssue>{txCtx.TxManager ? *txCtx.TxManager->GetLockIssue() : txCtx.Locks.GetIssue()}));
            return false;
        }

        return true;
    }

    bool CheckTopicOperations() {
        const auto& txCtx = *QueryState->TxCtx;

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

    bool CheckScriptExecutionState() {
        if (!QueryState || !QueryState->SaveQueryPhysicalGraph) {
            return true;
        }

        YQL_ENSURE(QueryState->GetType() == NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_SCRIPT);
        YQL_ENSURE(QueryState->HasImplicitTx());
        YQL_ENSURE(QueryState->Commit, "Expected commit for implicit tx");

        if (!QueryState->PreparedQuery) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported without prepared query");
            return false;
        }

        const auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        if (IsBatchQuery(phyQuery)) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for batch queries");
            return false;
        }

        if (QueryState->IsSplitted()) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for perstatement query execution");
            return false;
        }

        if (phyQuery.ResultBindingsSize()) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for queries with results");
            return false;
        }

        const auto txCount = phyQuery.TransactionsSize();
        if (!txCount) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for empty queries");
            return false;
        }

        // Ensure that the transactions before the last one are precomputes

        for (size_t i = 0; i + 1 < txCount; ++i) {
            const auto tx = QueryState->PreparedQuery->GetPhyTx(i);
            bool hasWrites = tx->GetHasEffects();
            if (!hasWrites) {
                for (const auto& stage : tx->GetStages()) {
                    if (stage.SinksSize()) {
                        hasWrites = true;
                        break;
                    }
                }
            }

            if (hasWrites) {
                ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for queries with intermediate writes");
                return false;
            }
        }

        // Ensure that the last transaction can be saved

        const auto& txCtx = *QueryState->TxCtx;
        if (txCtx.TopicOperations.GetSize()) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for topic operations");
            return false;
        }

        if (txCtx.HasTableWrite) {
            if (txCtx.HasOlapTable && !txCtx.EnableOlapSink.value_or(false)) {
                ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for column table writes without enabled olap sink");
                return false;
            }

            if (txCtx.HasOltpTable && !txCtx.EnableOltpSink.value_or(false)) {
                ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for row table writes without enabled oltp sink");
                return false;
            }
        }

        const auto tx = QueryState->PreparedQuery->GetPhyTx(txCount - 1);
        for (const auto& stage : tx->GetStages()) {
            for (const auto& source : stage.GetSources()) {
                // Solomon provider may use runtime listing,
                // YT provider opens read session during compilation,
                // so we can't save and restore such queries
                // NB: for S3 provider runtime listing should be explicitly disabled during compilation
                if (const auto sourceType = source.GetExternalSource().GetType(); IsIn({TStringBuf("SolomonSource"), YtProviderName}, sourceType)) {
                    ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, TStringBuilder() << "Save state of query is not supported for queries with '" << sourceType << "' sources");
                    return false;
                }
            }
        }

        if (tx->ResultsSize()) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for queries with results");
            return false;
        }

        if (const auto txType = tx->GetType(); !IsIn({NKqpProto::TKqpPhyTx::TYPE_DATA, NKqpProto::TKqpPhyTx::TYPE_GENERIC, NKqpProto::TKqpPhyTx::TYPE_COMPUTE}, txType)) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, TStringBuilder() << "Save state of query is not supported for this tx type: " << NKqpProto::TKqpPhyTx::EType_Name(txType));
            return false;
        }

        if (tx->IsLiteralTx()) {
            ReplyQueryError(Ydb::StatusIds::UNSUPPORTED, "Save state of query is not supported for literal tx");
            return false;
        }

        return true;
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
            tx = QueryState->GetCurrentPhyTx(QueryState->TxCtx->TxAlloc->TypeEnv);
        } catch (const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        }

        if (!CheckTransactionLocks(tx) || !CheckTopicOperations()) {
            return;
        }

        const bool isBatchQuery = IsBatchQuery(QueryState->PreparedQuery->GetPhysicalQuery());
        if (isBatchQuery && (!tx || !tx->IsLiteralTx())) {
            ExecutePartitioned(tx);
        } else if (QueryState->TxCtx->ShouldExecuteDeferredEffects()) {
            ExecuteDeferredEffectsImmediately(tx);
        } else if (auto commit = QueryState->ShouldCommitWithCurrentTx(tx); commit || tx) {
            ExecutePhyTx(tx, commit);
        } else {
            ReplySuccess();
        }
    }

    void ExecutePartitioned(const TKqpPhyTxHolder::TConstPtr& tx) {
        if (!Settings.TableService.GetEnableBatchUpdates()) {
            return ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                "BATCH operations are not supported at the current time.");
        }

        if (!QueryState->TxCtx->EnableOltpSink.value_or(false)) {
            return ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                "BATCH operations are not supported at the current time.");
        }

        if (QueryState->TxCtx->HasOlapTable) {
            return ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                "BATCH operations are not supported for column tables at the current time.");
        }

        if (!QueryState->HasImplicitTx()) {
            return ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                "BATCH operation can be executed only in the implicit transaction mode.");
        }

        const auto& txCtx = *QueryState->TxCtx;
        auto request = PrepareRequest(tx, false, QueryState.get());

        try {
            QueryState->QueryData->PrepareParameters(tx, QueryState->PreparedQuery, txCtx.TxAlloc->TypeEnv);
        } catch (const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        }

        if (tx) {
            request.Transactions.emplace_back(tx, QueryState->QueryData);
        }

        QueryState->TxCtx->OnNewExecutor(false);
        QueryState->Commited = true;

        for (const auto& effect : txCtx.DeferredEffects) {
            request.Transactions.emplace_back(effect.PhysicalTx, effect.Params);
            STLOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction",
                (transactions_size, request.Transactions.size()),
                (trace_id, TraceId()));
        }

        SendToPartitionedExecuter(QueryState->TxCtx.Get(), std::move(request));
        QueryState->CurrentTx += 1;
    }

    void ExecuteDeferredEffectsImmediately(const TKqpPhyTxHolder::TConstPtr& tx) {
        YQL_ENSURE(QueryState->TxCtx->ShouldExecuteDeferredEffects());

        auto& txCtx = *QueryState->TxCtx;
        auto request = PrepareRequest(/* tx */ nullptr, /* literal */ false, QueryState.get());

        for (const auto& effect : txCtx.DeferredEffects) {
            request.Transactions.emplace_back(effect.PhysicalTx, effect.Params);

            STLOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction",
                (transactions_size, request.Transactions.size()),
                (trace_id, TraceId()));
        }

        request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();
        request.UseImmediateEffects = true;
        request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();

        txCtx.HasImmediateEffects = true;
        txCtx.ClearDeferredEffects();

        SendToExecuter(QueryState->TxCtx.Get(), std::move(request));
        if (!tx) {
            ++QueryState->CurrentTx;
        }
    }

    bool ExecutePhyTx(const TKqpPhyTxHolder::TConstPtr& tx, bool commit) {
        if (tx) {
            switch (tx->GetType()) {
                case NKqpProto::TKqpPhyTx::TYPE_SCHEME:
                    YQL_ENSURE(tx->StagesSize() == 0);
                    if (QueryState->HasTxControl() && !QueryState->HasImplicitTx() && QueryState->TxCtx->EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_UNDEFINED) {
                        ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                            "Scheme operations cannot be executed inside transaction");
                        return true;
                    }

                    SendToSchemeExecuter(tx);
                    return false;

                case NKqpProto::TKqpPhyTx::TYPE_DATA:
                case NKqpProto::TKqpPhyTx::TYPE_GENERIC:
                    if (QueryState->TxCtx->EffectiveIsolationLevel == NKqpProto::ISOLATION_LEVEL_UNDEFINED) {
                        ReplyQueryError(Ydb::StatusIds::PRECONDITION_FAILED,
                            "Data operations cannot be executed outside of transaction");
                        return true;
                    }
                    break;

                default:
                    break;
            }
        }

        if (QueryState->GetFormatsSettings().IsArrowFormat() && !AppData()->FeatureFlags.GetEnableArrowResultSetFormat()) {
            ReplyQueryError(Ydb::StatusIds::BAD_REQUEST, "Arrow result set format is not enabled. Please set EnableArrowResultSetFormat feature flag to true.");
            return true;
        }

        auto& txCtx = *QueryState->TxCtx;
        bool literal = tx && tx->IsLiteralTx();
        const bool hasLocks = txCtx.TxManager ? txCtx.TxManager->HasLocks() : txCtx.Locks.HasLocks();

        if (commit) {
            if (txCtx.TxHasEffects() || hasLocks || txCtx.TopicOperations.HasOperations()) {
                // Cannot perform commit in literal execution
                literal = false;
            } else if (!tx) {
                // Commit is no-op
                ReplySuccess();
                return true;
            }
        }

        auto request = PrepareRequest(tx, literal, QueryState.get());

        STLOG_D("ExecutePhyTx",
            (literal, literal),
            (commit, commit),
            (deferred_effects_size, txCtx.DeferredEffects.Size()),
            (tx, (uintptr_t)tx.get()),
            (trace_id, TraceId()));

        if (!CheckTopicOperations()) {
            return true;
        }

        YQL_ENSURE(tx || commit);
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
        }

        try {
            QueryState->QueryData->PrepareParameters(tx, QueryState->PreparedQuery, txCtx.TxAlloc->TypeEnv);
        } catch (const yexception& ex) {
            ythrow TRequestFail(Ydb::StatusIds::BAD_REQUEST) << ex.what();
        }

        if (tx) {
            request.Transactions.emplace_back(tx, QueryState->QueryData);
        }

        QueryState->TxCtx->OnNewExecutor(literal);

        if (literal) {
            Y_ENSURE(QueryState);
            request.Orbit = std::move(QueryState->Orbit);
            request.TraceId = QueryState->KqpSessionSpan.GetTraceId();
            auto response = ExecuteLiteral(std::move(request), RequestCounters, SelfId(), QueryState->UserRequestContext);
            ++QueryState->CurrentTx;
            ProcessExecuterResult(response.get());
            return true;
        } else if (commit) {
            QueryState->Commited = true;

            for (const auto& effect : txCtx.DeferredEffects) {
                request.Transactions.emplace_back(effect.PhysicalTx, effect.Params);

                STLOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction",
                    (transactions_size, request.Transactions.size()),
                    (trace_id, TraceId()));
            }

            if (!txCtx.DeferredEffects.Empty()) {
                request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
            }

            if (txCtx.EnableOltpSink.value_or(false)) {
                if (txCtx.TxHasEffects() || hasLocks || txCtx.TopicOperations.HasOperations()) {
                    request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();
                }

                if (hasLocks || txCtx.TopicOperations.HasOperations()) {
                    if (!txCtx.GetSnapshot().IsValid() || txCtx.TxHasEffects() || txCtx.TopicOperations.HasOperations()) {
                        STLOG_D("TExecPhysicalRequest, tx has commit locks",
                            (trace_id, TraceId()));
                        request.LocksOp = ELocksOp::Commit;
                    } else {
                        STLOG_D("TExecPhysicalRequest, tx has rollback locks",
                            (trace_id, TraceId()));
                        request.LocksOp = ELocksOp::Rollback;
                    }
                } else if (txCtx.TxHasEffects()) {
                    STLOG_D("TExecPhysicalRequest, need commit locks",
                        (trace_id, TraceId()));
                    request.LocksOp = ELocksOp::Commit;
                }
            } else {
                AFL_ENSURE(!tx || !txCtx.HasOlapTable);
                AFL_ENSURE(txCtx.DeferredEffects.Empty() || !txCtx.HasOlapTable);
                if (hasLocks || txCtx.TopicOperations.HasOperations()) {
                    bool hasUncommittedEffects = false;
                    for (auto& [lockId, lock] : txCtx.Locks.LocksMap) {
                        auto dsLock = ExtractLock(lock.GetValueRef(txCtx.Locks.LockType));
                        request.DataShardLocks[dsLock.GetDataShard()].emplace_back(dsLock);
                        hasUncommittedEffects |=  dsLock.GetHasWrites();
                    }
                    if (!txCtx.GetSnapshot().IsValid() || (tx && txCtx.TxHasEffects()) || !txCtx.DeferredEffects.Empty() || hasUncommittedEffects || txCtx.TopicOperations.HasOperations()) {
                        STLOG_D("TExecPhysicalRequest, tx has commit locks",
                            (trace_id, TraceId()));
                        request.LocksOp = ELocksOp::Commit;
                    } else {
                        STLOG_D("TExecPhysicalRequest, tx has rollback locks",
                            (trace_id, TraceId()));
                        request.LocksOp = ELocksOp::Rollback;
                    }
                }
            }
            request.TopicOperations = std::move(txCtx.TopicOperations);
        } else if (QueryState->ShouldAcquireLocks(tx) && (!txCtx.HasOlapTable || txCtx.EnableOlapSink.value_or(false))) {
            request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();

            if (!txCtx.CanDeferEffects()) {
                request.UseImmediateEffects = true;
            }
        }

        LWTRACK(KqpSessionPhyQueryProposeTx,
            QueryState->Orbit,
            QueryState->CurrentTx,
            request.Transactions.size(),
            (txCtx.TxManager ? txCtx.TxManager->GetShardsCount() : txCtx.Locks.Size()),
            request.AcquireLocksTxId.Defined());

        SendToExecuter(QueryState->TxCtx.Get(), std::move(request));
        ++QueryState->CurrentTx;
        return false;
    }

    void FillGUCSettings() {
        if (Settings.Database) {
            GUCSettings->Set("ydb_database", Settings.Database.substr(1, Settings.Database.size() - 1));
        }
        if (Settings.UserName) {
            GUCSettings->Set("ydb_user", *Settings.UserName);
        }
    }

    void SendToSchemeExecuter(const TKqpPhyTxHolder::TConstPtr& tx) {
        YQL_ENSURE(QueryState);

        auto userToken = QueryState->UserToken;
        const TString clientAddress = QueryState->ClientAddress;
        const TString requestType = QueryState->GetRequestType();
        const bool temporary = GetTemporaryTableInfo(tx).has_value();

        auto executerActor = CreateKqpSchemeExecuter(
            tx, QueryState->GetType(), SelfId(), requestType, Settings.Database, userToken, clientAddress,
            temporary, /* createTmpDir */ temporary && !TempTablesState.NeedCleaning,
            QueryState->IsCreateTableAs(), TempTablesState.TempDirName, QueryState->UserRequestContext, KqpTempTablesAgentActor);

        ExecuterId = RegisterWithSameMailbox(executerActor);

        ++QueryState->CurrentTx;
    }

    static ui32 GetResultsCount(const IKqpGateway::TExecPhysicalRequest& req) {
        ui32 results = 0;
        for (const auto& transaction : req.Transactions) {
            results += transaction.Body->ResultsSize();
        }
        return results;
    }

    void SendToExecuter(TKqpTransactionContext* txCtx, IKqpGateway::TExecPhysicalRequest&& request, bool isRollback = false) {
        bool allowSaveState = false;
        if (QueryState) {
            request.Orbit = std::move(QueryState->Orbit);
            QueryState->StatementResultSize = GetResultsCount(request);

            if (const auto& preparedQuery = QueryState->PreparedQuery) {
                allowSaveState = !isRollback && QueryState->CurrentTx + 1 == preparedQuery->GetPhysicalQuery().TransactionsSize();
            }
        }
        request.PerRequestDataSizeLimit = RequestControls.PerRequestDataSizeLimit;
        request.MaxShardCount = RequestControls.MaxShardCount;
        request.TraceId = QueryState ? QueryState->KqpSessionSpan.GetTraceId() : NWilson::TTraceId();
        request.CaFactory_ = CaFactory_;
        request.ResourceManager_ = ResourceManager_;
        request.SaveQueryPhysicalGraph = allowSaveState && QueryState->SaveQueryPhysicalGraph;
        request.QueryPhysicalGraph = allowSaveState ? QueryState->QueryPhysicalGraph : nullptr;
        STLOG_D("Sending to Executer",
            (span_id_size, request.TraceId.GetSpanIdSize()),
            (trace_id, TraceId()));

        if (txCtx->EnableOltpSink.value_or(false) && !txCtx->TxManager) {
            txCtx->TxManager = CreateKqpTransactionManager();
            txCtx->TxManager->SetAllowVolatile(AppData()->FeatureFlags.GetEnableDataShardVolatileTransactions());
        }

        if (txCtx->EnableOltpSink.value_or(false)
            && !txCtx->BufferActorId
            && (txCtx->HasTableWrite || request.TopicOperations.GetSize() != 0)) {
            txCtx->TxManager->SetTopicOperations(std::move(request.TopicOperations));
            txCtx->TxManager->AddTopicsToShards();

            auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(
                __LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);

            const auto& queryLimitsProto = Settings.TableService.GetQueryLimits();
            const auto& bufferLimitsProto = queryLimitsProto.GetBufferLimits();
            const ui64 writeBufferMemoryLimit = bufferLimitsProto.HasWriteBufferMemoryLimitBytes()
                ? bufferLimitsProto.GetWriteBufferMemoryLimitBytes()
                : ui64(Settings.MkqlMaxMemoryLimit);
            const ui64 writeBufferInitialMemoryLimit = writeBufferMemoryLimit < ui64(Settings.MkqlInitialMemoryLimit)
                ? writeBufferMemoryLimit
                : ui64(Settings.MkqlInitialMemoryLimit);
            alloc->SetLimit(writeBufferInitialMemoryLimit);
            alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc=alloc.get(), writeBufferMemoryLimit](ui64 currentLimit, ui64 required) {
                if (required < writeBufferMemoryLimit) {
                    STLOG_D("Increase memory limit",
                        (current_limit, currentLimit),
                        (required, required),
                        (trace_id, TraceId()));
                    alloc->SetLimit(required);
                }
            });

            TKqpBufferWriterSettings settings {
                .SessionActorId = SelfId(),
                .TxManager = txCtx->TxManager,
                .TraceId = request.TraceId.GetTraceId(),
                .Counters = Counters,
                .TxProxyMon = RequestCounters->TxProxyMon,
                .Alloc = std::move(alloc),
            };
            auto* actor = CreateKqpBufferWriterActor(std::move(settings));
            txCtx->BufferActorId = RegisterWithSameMailbox(actor);
        } else if (txCtx->EnableOltpSink.value_or(false) && txCtx->BufferActorId) {
            txCtx->TxManager->SetTopicOperations(std::move(request.TopicOperations));
            txCtx->TxManager->AddTopicsToShards();
        }

        std::optional<TLlvmSettings> llvmSettings;
        if (QueryState && QueryState->PreparedQuery && Settings.TableService.GetEnableKqpScanQueryUseLlvm()) {
            llvmSettings = QueryState->PreparedQuery->GetLlvmSettings();
        }

        auto executerActor = CreateKqpExecuter(std::move(request), Settings.Database,
            QueryState ? QueryState->UserToken : TIntrusiveConstPtr<NACLib::TUserToken>(),
            QueryState ? QueryState->GetFormatsSettings() : NFormats::TFormatsSettings{},
            RequestCounters, TExecuterConfig(Settings.MutableExecuterConfig, Settings.TableService),
            AsyncIoFactory, SelfId(),
            QueryState ? QueryState->UserRequestContext : MakeIntrusive<TUserRequestContext>("", Settings.Database, SessionId),
            QueryState ? QueryState->StatementResultIndex : 0, FederatedQuerySetup,
            (QueryState && QueryState->RequestEv->GetSyntax() == Ydb::Query::Syntax::SYNTAX_PG)
                ? GUCSettings : nullptr, {}, txCtx->ShardIdToTableInfo, txCtx->TxManager, txCtx->BufferActorId, /* batchOperationSettings */ Nothing(),
            llvmSettings, QueryServiceConfig, QueryState ? QueryState->Generation : 0, ChannelService);

        auto exId = RegisterWithSameMailbox(executerActor);
        STLOG_D("Created new KQP executer",
            (executer_id, exId),
            (is_rollback, isRollback),
            (trace_id, TraceId()));
        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
        if (!isRollback) {
            YQL_ENSURE(!ExecuterId);
        }
        ExecuterId = exId;
    }

    void SendToPartitionedExecuter(TKqpTransactionContext* txCtx, IKqpGateway::TExecPhysicalRequest&& request)
    {
        if (txCtx->TxHasEffects() || txCtx->Locks.HasLocks()) {
            request.AcquireLocksTxId = txCtx->Locks.GetLockTxId();
        }

        if (!txCtx->DeferredEffects.Empty()) {
            request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
        }

        if (QueryState) {
            request.Orbit = std::move(QueryState->Orbit);
            QueryState->StatementResultSize = GetResultsCount(request);
        }

        request.LocksOp = ELocksOp::Commit;
        request.PerRequestDataSizeLimit = RequestControls.PerRequestDataSizeLimit;
        request.MaxShardCount = RequestControls.MaxShardCount;
        request.TraceId = QueryState ? QueryState->KqpSessionSpan.GetTraceId() : NWilson::TTraceId();
        request.CaFactory_ = CaFactory_;
        request.ResourceManager_ = ResourceManager_;

        const auto& queryLimitsProto = Settings.TableService.GetQueryLimits();
        const auto& bufferLimitsProto = queryLimitsProto.GetBufferLimits();
        const ui64 writeBufferMemoryLimit = bufferLimitsProto.HasWriteBufferMemoryLimitBytes()
            ? bufferLimitsProto.GetWriteBufferMemoryLimitBytes()
            : ui64(Settings.MkqlMaxMemoryLimit);
        const ui64 writeBufferInitialMemoryLimit = writeBufferMemoryLimit < ui64(Settings.MkqlInitialMemoryLimit)
            ? writeBufferMemoryLimit
            : ui64(Settings.MkqlInitialMemoryLimit);

        const auto executerConfig = TExecuterConfig(Settings.MutableExecuterConfig, Settings.TableService);
        TKqpPartitionedExecuterSettings settings{
            .Request = std::move(request),
            .SessionActorId = SelfId(),
            .FuncRegistry = AppData()->FunctionRegistry,
            .TimeProvider = AppData()->TimeProvider,
            .RandomProvider = AppData()->RandomProvider,
            .Database = Settings.Database,
            .UserToken = QueryState
                ? QueryState->UserToken
                : TIntrusiveConstPtr<NACLib::TUserToken>(),
            .RequestCounters = RequestCounters,
            .ExecuterConfig = executerConfig,
            .AsyncIoFactory = AsyncIoFactory,
            .PreparedQuery = QueryState
                ? QueryState->PreparedQuery
                : nullptr,
            .UserRequestContext = QueryState
                ? QueryState->UserRequestContext
                : MakeIntrusive<TUserRequestContext>("", Settings.Database, SessionId),
            .StatementResultIndex = QueryState
                ? QueryState->StatementResultIndex
                : 0,
            .FederatedQuerySetup = FederatedQuerySetup,
            .GUCSettings = GUCSettings,
            .ShardIdToTableInfo = txCtx->ShardIdToTableInfo,
            .WriteBufferInitialMemoryLimit = writeBufferInitialMemoryLimit,
            .WriteBufferMemoryLimit = writeBufferMemoryLimit,
        };

        auto executerActor = CreateKqpPartitionedExecuter(std::move(settings), ChannelService);

        ExecuterId = RegisterWithSameMailbox(executerActor);
        STLOG_D("Created new KQP partitioned executer",
            (executer_id, ExecuterId),
            (trace_id, TraceId()));
    }


    template<typename T>
    void HandleNoop(T&) {
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

    void HandleExecute(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        if (QueryState && QueryState->RequestActorId) {
            if (ExecuterId != ev->Sender) {
                return;
            }

            if (QueryState->ReportStats()) {
                NKqpProto::TKqpStatsQuery& stats = *ev->Get()->Record.MutableQueryStats();
                NKqpProto::TKqpStatsQuery executionStats;
                executionStats.Swap(&stats);
                stats = QueryState->QueryStats.ToProto();
                stats.MutableExecutions()->MergeFrom(executionStats.GetExecutions());
                ev->Get()->Record.SetQueryPlan(SerializeAnalyzePlan(stats, QueryState->UserRequestContext->PoolId));
                stats.SetDurationUs((TInstant::Now() - QueryState->StartTime).MicroSeconds());

                if (QueryState->GetStatsMode() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
                    if (const auto compileResult = QueryState->CompileResult) {
                        if (const auto preparedQuery = compileResult->PreparedQuery) {
                            if (const auto& queryAst = preparedQuery->GetPhysicalQuery().GetQueryAst()) {
                                ev->Get()->Record.SetQueryAst(queryAst);
                            }
                        }
                    }
                }
            }

            STLOG_D("Forwarded TEvExecuterProgress to " << QueryState->RequestActorId,
                (trace_id, TraceId()));
            Send(QueryState->RequestActorId, ev->Release().Release(), 0, QueryState->ProxyRequestId);
        }
    }

    std::optional<std::pair<bool, std::pair<TString, TKqpTempTablesState::TTempTableInfo>>>
    GetTemporaryTableInfo(TKqpPhyTxHolder::TConstPtr tx) {
        if (!tx) {
            return std::nullopt;
        }

        auto optPath = tx->GetSchemeOpTempTablePath();
        if (!optPath) {
            return std::nullopt;
        }
        const auto& [isCreate, path] = *optPath;
        if (isCreate) {
            auto userToken = QueryState ? QueryState->UserToken : TIntrusiveConstPtr<NACLib::TUserToken>();
            return {{true, {JoinPath({path.first, path.second}), {path.second, path.first, userToken}}}};
        }

        auto it = TempTablesState.FindInfo(JoinPath({path.first, path.second}), true);
        if (it == TempTablesState.TempTables.end()) {
            return std::nullopt;
        }

        return {{false, {it->first, {}}}};
    }

    void UpdateTempTablesState() {
        if (!QueryState->PreparedQuery) {
            return;
        }
        auto tx = QueryState->PreparedQuery->GetPhyTxOrEmpty(QueryState->CurrentTx - 1);
        if (!tx) {
            return;
        }
        if (QueryState->IsCreateTableAs()) {
            TempTablesState.NeedCleaning = true;
            QueryState->UpdateTempTablesState(TempTablesState);
            return;
        }

        auto optInfo = GetTemporaryTableInfo(tx);
        if (optInfo) {
            auto [isCreate, info] = *optInfo;
            if (isCreate) {
                TempTablesState.NeedCleaning = true;
                TempTablesState.TempTables[info.first] = info.second;
            } else {
                TempTablesState.TempTables.erase(info.first);
            }
            QueryState->UpdateTempTablesState(TempTablesState);
        }
    }

    void FillQueryIssues(const NYql::TIssues& issues) {
        if (!QueryState) {
            STLOG_W("Try to put issues into empty QueryState",
                (issues, issues.ToOneLineString()),
                (trace_id, TraceId())
            );
            return;
        }

        if (!QueryState->Issues) {
            QueryState->Issues = issues;
            return;
        }

        QueryState->Issues.AddIssues(issues);
    }

    void ProcessExecuterResult(TEvKqpExecuter::TEvTxResponse* ev) {
        QueryState->Orbit = std::move(ev->Orbit);

        auto* response = ev->Record.MutableResponse();

        STLOG_D("TEvTxResponse",
            (current_tx, QueryState->CurrentTx),
            (transactions_size, QueryState->PreparedQuery ? QueryState->PreparedQuery->GetPhysicalQuery().TransactionsSize() : 0),
            (status, response->GetStatus()),
            (trace_id, TraceId()));

        ExecuterId = TActorId{};

        auto& executerResults = *response->MutableResult();
        if (executerResults.HasStats()) {
            QueryState->QueryStats.Executions.emplace_back();
            QueryState->QueryStats.Executions.back().Swap(executerResults.MutableStats());
        }

        QueryState->QueryStats.LocksBrokenAsBreaker += ev->LocksBrokenAsBreaker;
        QueryState->QueryStats.LocksBrokenAsVictim += ev->LocksBrokenAsVictim;

        if (QueryState->TxCtx->TxManager) {
            QueryState->ParticipantNodes = QueryState->TxCtx->TxManager->GetParticipantNodes();
        } else {
            for (auto nodeId : ev->ParticipantNodes) {
                QueryState->ParticipantNodes.emplace(nodeId);
            }
        }

        if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
            const auto executionType = ev->ExecutionType;

            STLOG_D("TEvTxResponse has non-success status",
                (current_tx, QueryState->CurrentTx),
                (execution_type, executionType),
                (status, response->GetStatus()),
                (trace_id, TraceId()));

            auto status = response->GetStatus();
            TIssues issues;
            IssuesFromMessage(response->GetIssues(), issues);

            // Invalidate query cache on scheme/internal errors
            switch (status) {
                case Ydb::StatusIds::ABORTED: {
                    if (QueryState->TxCtx->TxManager && QueryState->TxCtx->TxManager->BrokenLocks()) {
                        YQL_ENSURE(!issues.Empty());
                    } else if (ev->BrokenLockPathId) {
                        YQL_ENSURE(!QueryState->TxCtx->TxManager);
                        issues.AddIssue(GetLocksInvalidatedIssue(*QueryState->TxCtx, *ev->BrokenLockPathId));
                    } else if (ev->BrokenLockShardId) {
                        YQL_ENSURE(!QueryState->TxCtx->TxManager);
                        issues.AddIssue(GetLocksInvalidatedIssue(*QueryState->TxCtx->ShardIdToTableInfo, *ev->BrokenLockShardId));
                    }
                    break;
                }
                case Ydb::StatusIds::SCHEME_ERROR:
                case Ydb::StatusIds::INTERNAL_ERROR:
                    InvalidateQuery();
                    issues.AddIssue(YqlIssue(TPosition(), TIssuesIds::KIKIMR_QUERY_INVALIDATED,
                        TStringBuilder() << "Query invalidated on scheme/internal error during "
                            << executionType << " execution"));

                    // SCHEME_ERROR during execution is a soft (retriable) error, we abort query execution,
                    // invalidate query cache, and return ABORTED as retriable status.
                    if (status == Ydb::StatusIds::SCHEME_ERROR &&
                        executionType != TEvKqpExecuter::TEvTxResponse::EExecutionType::Scheme)
                    {
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

        UpdateTempTablesState();

        LWTRACK(KqpSessionPhyQueryTxResponse, QueryState->Orbit, QueryState->CurrentTx, ev->ResultRowsCount);
        QueryState->QueryData->ClearPrunedParams();

        if (!ev->GetTxResults().empty()) {
            QueryState->QueryData->AddTxResults(QueryState->CurrentTx - 1, std::move(ev->GetTxResults()));
        }
        QueryState->QueryData->AddTxHolders(std::move(ev->GetTxHolders()));

        QueryState->TxCtx->AcceptIncomingSnapshot(ev->Snapshot);

        if (ev->LockHandle) {
            QueryState->TxCtx->Locks.LockHandle = std::move(ev->LockHandle);
        }

        if (!QueryState->TxCtx->TxManager && !MergeLocksWithTxResult(executerResults)) {
            return;
        }

        if (!response->GetIssues().empty()){
            NYql::TIssues issues;
            NYql::IssuesFromMessage(response->GetIssues(), issues);
            FillQueryIssues(issues);
        }

        ExecuteOrDefer();
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        YQL_ENSURE(QueryState && QueryState->RequestActorId);
        STLOG_D("Forwarded TEvStreamData to " << QueryState->RequestActorId,
            (trace_id, TraceId()));

        QueryState->QueryData->AddBuiltResultIndex(ev->Get()->Record.GetQueryResultIndex());

        TlsActivationContext->Send(ev->Forward(QueryState->RequestActorId));
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        TlsActivationContext->Send(ev->Forward(ExecuterId));
    }

    void HandleExecute(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;

        STLOG_I("Got TEvAbortExecution, send it to Executer",
            (status_code, NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())),
            (executer_id, ExecuterId),
            (trace_id, TraceId()));

        auto issues = ev->Get()->GetIssues();
        TStringBuilder reason = TStringBuilder() << "Cancelling after " << (AppData()->MonotonicTimeProvider->Now() - QueryState->StartedAt).MilliSeconds() << "ms";
        if (QueryState->CompilationRunning) {
            reason << " during compilation";
        } else if (ExecuterId) {
            reason << " during execution";
        } else {
            reason << " in " << CurrentStateFuncName();
        }
        issues.AddIssue(reason);

        if (ExecuterId) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(msg.GetStatusCode(), issues);
            Send(ExecuterId, abortEv.Release(), IEventHandle::FlagTrackDelivery);
        } else {
            ReplyQueryError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), "", MessageFromIssues(issues));
        }
    }

    void Handle(TEvKqpBuffer::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();

        TString logMsg = TStringBuilder() << "got TEvKqpBuffer::TEvError in " << CurrentStateFuncName()
            << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode) << " send to: " << ExecuterId << " from: " << ev->Sender;

        if (!QueryState || !QueryState->TxCtx || QueryState->TxCtx->BufferActorId != ev->Sender) {
            STLOG_E(logMsg << ": Ignored error.",
                (trace_id, TraceId()));
            return;
        } else {
            STLOG_W(logMsg,
                (trace_id, TraceId()));
        }

        if (ExecuterId) {
            Send(ExecuterId, new TEvKqpBuffer::TEvError{msg.StatusCode, std::move(msg.Issues), std::move(msg.Stats)}, IEventHandle::FlagTrackDelivery);
        } else {
            ReplyQueryError(NYql::NDq::DqStatusToYdbStatus(msg.StatusCode), logMsg, MessageFromIssues(msg.Issues));
        }
    }

    void HandleCleanup(TEvKqpBuffer::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();

        TString logMsg = TStringBuilder() << "got TEvKqpBuffer::TEvError in " << CurrentStateFuncName()
            << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode) << " send to: " << ExecuterId << " from: " << ev->Sender;

        if (CleanupCtx->TransactionsToBeAborted.empty()) {
            STLOG_E(logMsg <<  ": Ignored error. TransactionsToBeAborted is empty.",
                (trace_id, TraceId()));
        }

        AFL_ENSURE(ExecuterId); // ExecuterId can't be empty during cleanup if TransactionsToBeAborted is not empty.

        const auto& txCtx = CleanupCtx->TransactionsToBeAborted.front();
        AFL_ENSURE(txCtx);
        if (txCtx->BufferActorId != ev->Sender) {
            STLOG_E(logMsg <<  ": Ignored error. Current BufferActorId is not sender.",
                (trace_id, TraceId()));
            return;
        } else {
            STLOG_W(logMsg,
                (trace_id, TraceId()));
        }

        Send(ExecuterId, new TEvKqpBuffer::TEvError{msg.StatusCode, std::move(msg.Issues), std::move(msg.Stats)}, IEventHandle::FlagTrackDelivery);
    }

    void CollectSystemViewQueryStats(const TKqpQueryStats* stats, TDuration queryDuration,
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
                    CollectQueryStats(TlsActivationContext->AsActorContext(), stats, queryDuration, text,
                        userSID, QueryState->ParametersSize, database, type, requestUnits);
                }
                break;
            }
            default:
                break;
        }
    }

    void FillSystemViewQueryStats(NKikimrKqp::TEvQueryResponse* record) {
        YQL_ENSURE(QueryState);
        auto* stats = &QueryState->QueryStats;

        stats->DurationUs = ((TInstant::Now() - QueryState->StartTime).MicroSeconds());
        stats->WorkerCpuTimeUs = (QueryState->GetCpuTime().MicroSeconds());
        stats->LocksBrokenAsBreaker = QueryState->QueryStats.LocksBrokenAsBreaker;
        stats->LocksBrokenAsVictim = QueryState->QueryStats.LocksBrokenAsVictim;
        if (const auto continueTime = QueryState->ContinueTime) {
            stats->QueuedTimeUs = (continueTime - QueryState->StartTime).MicroSeconds();
        }
        if (QueryState->CompileResult) {
            stats->Compilation.emplace();
            stats->Compilation->FromCache = (QueryState->CompileStats.FromCache);
            stats->Compilation->DurationUs = (QueryState->CompileStats.DurationUs);
            stats->Compilation->CpuTimeUs = (QueryState->CompileStats.CpuTimeUs);
        }

        if (IsExecuteAction(QueryState->GetAction())) {
            auto ru = CalcRequestUnit(*stats);

            if (record != nullptr) {
                record->SetConsumedRu(ru);
            }

            auto now = TInstant::Now();
            auto queryDuration = now - QueryState->StartTime;
            CollectSystemViewQueryStats(stats, queryDuration, QueryState->GetDatabase(), ru);
        }
    }

    void FillStats(NKikimrKqp::TEvQueryResponse* record) {
        YQL_ENSURE(QueryState);
        // workaround to ensure that request was not transfered to worker.
        if (WorkerId || !QueryState->RequestEv) {
            return;
        }

        FillSystemViewQueryStats(record);

        auto *response = record->MutableResponse();
        auto requestInfo = TKqpRequestInfo(QueryState->UserRequestContext->TraceId, SessionId);

        if (IsExecuteAction(QueryState->GetAction())) {
            auto queryDuration = TDuration::MicroSeconds(QueryState->QueryStats.DurationUs);
            SlowLogQuery(TlsActivationContext->AsActorContext(), Config.Get(), requestInfo, queryDuration,
                record->GetYdbStatus(), QueryState->UserToken, QueryState->ParametersSize, record,
                [this]() { return this->QueryState->ExtractQueryText(); });
        }

        if (QueryState->ReportStats()) {
            auto stats = QueryState->QueryStats.ToProto();
            if (QueryState->GetStatsMode() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
                response->SetQueryPlan(SerializeAnalyzePlan(stats, QueryState->UserRequestContext->PoolId));
                if (const auto compileResult = QueryState->CompileResult) {
                    if (const auto preparedQuery = compileResult->PreparedQuery) {
                        if (const auto& queryAst = preparedQuery->GetPhysicalQuery().GetQueryAst()) {
                            QueryState->QueryAst = queryAst;
                        }
                    }
                }
                response->SetQueryAst(QueryState->QueryAst);
            }
            response->MutableQueryStats()->Swap(&stats);
        }
    }

    template<class TEvRecord>
    void AddTrailingInfo(TEvRecord& record) {
        if (ShutdownState) {
            STLOG_D("Session is closing, set trailing metadata to request session shutdown",
                (trace_id, TraceId()));
            record.SetWorkerIsClosing(true);
        }
    }

    void FillTxInfo(NKikimrKqp::TQueryResponse* response) {
        YQL_ENSURE(QueryState);
        response->MutableTxMeta()->set_id(QueryState->TxId.GetValue().GetHumanStr());

        if (QueryState->TxCtx) {
            auto txInfo = QueryState->TxCtx->GetInfo();
            STLOG_I("TxInfo",
                (status, txInfo.Status),
                (kind, txInfo.Kind),
                (total_duration, txInfo.TotalDuration.SecondsFloat()*1e3),
                (server_duration, txInfo.ServerDuration.SecondsFloat()*1e3),
                (queries_count, txInfo.QueriesCount),
                (trace_id, TraceId()));
            Counters->ReportTransaction(Settings.DbCounters, txInfo);
        }
    }

    void FillPoolId(NKikimrKqp::TQueryResponse* response) {
        YQL_ENSURE(QueryState);
        if (QueryState->UserRequestContext) {
            if (QueryState->UserRequestContext->PoolId.empty()) {
                response->SetEffectivePoolId(NResourcePool::DEFAULT_POOL_ID);
            } else {
                response->SetEffectivePoolId(QueryState->UserRequestContext->PoolId);
            }
        }
    }

    void UpdateQueryExecutionCounters() {
        auto now = TInstant::Now();
        auto queryDuration = now - QueryState->StartTime;

        Counters->ReportQueryLatency(Settings.DbCounters, QueryState->GetAction(), queryDuration);

        if (QueryState->MaxReadType == ETableReadType::FullScan) {
            Counters->ReportQueryWithFullScan(Settings.DbCounters);
        } else if (QueryState->MaxReadType == ETableReadType::Scan) {
            Counters->ReportQueryWithRangeScan(Settings.DbCounters);
        }

        auto& stats = QueryState->QueryStats;

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
        Counters->ReportQueryReadSets(Settings.DbCounters, stats.ReadSetsCount);
        Counters->ReportQueryMaxShardReplySize(Settings.DbCounters, stats.MaxShardReplySize);
        Counters->ReportQueryMaxShardProgramSize(Settings.DbCounters, stats.MaxShardProgramSize);
    }

    void ReplySuccess() {
        YQL_ENSURE(QueryState);
        auto resEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        auto *record = &resEv->Record;
        auto *response = record->MutableResponse();

        if (QueryState->CompileResult) {
            AddQueryIssues(*response, QueryState->CompileResult->Issues);
        }

        AddQueryIssues(*response, QueryState->Issues);

        FillStats(record);

        if (QueryState->CommandTagName) {
            auto *extraInfo = response->MutableExtraInfo();
            auto* pgExtraInfo = extraInfo->MutablePgInfo();
            pgExtraInfo->SetCommandTag(*QueryState->CommandTagName);
        }

        if (QueryState->TxCtx) {
            QueryState->TxCtx->OnEndQuery();
        }

        if (QueryState->Commit) {
            TerminateBufferActor(QueryState->TxCtx);
            ResetTxState();
            Transactions.ReleaseTransaction(QueryState->TxId.GetValue());
            QueryState->TxId.Reset();
        }

        FillTxInfo(response);
        FillPoolId(response);

        UpdateQueryExecutionCounters();

        bool replyQueryId = false;
        bool replyQueryParameters = false;
        bool replyTopicOperations = false;

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

            case NKikimrKqp::QUERY_ACTION_TOPIC:
                replyTopicOperations = true;
                break;

            default:
                break;
        }

        if (replyQueryParameters) {
            YQL_ENSURE(QueryState->PreparedQuery);
            for (auto& param : QueryState->GetResultParams()) {
                *response->AddQueryParameters() = param;
            }
        }

        if (replyQueryId) {
            TString queryId;
            if (QueryState->CompileResult) {
                queryId = QueryState->CompileResult->Uid;
            }
            response->SetPreparedQuery(queryId);
        }

        if (replyTopicOperations) {
            if (HasTopicApiWriteOperations() && !HasKafkaApiWriteOperations()) {
                auto* w = response->MutableTopicOperations();
                auto* writeId = w->MutableWriteId();
                writeId->SetNodeId(SelfId().NodeId());
                writeId->SetKeyId(GetTopicWriteId());
            }
        }

        response->SetQueryDiagnostics(QueryState->ReplayMessage);

        // Result for scan query is sent directly to target actor.
        Y_ABORT_UNLESS(response->GetArena());
        if (QueryState->PreparedQuery) {
            auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            size_t trailingResultsCount = 0;
            for (size_t i = 0; i < phyQuery.ResultBindingsSize(); ++i) {
                if (QueryState->IsStreamResult()) {
                    if (QueryState->QueryData->HasTrailingTxResult(phyQuery.GetResultBindings(i))) {
                        auto ydbResult = QueryState->QueryData->GetYdbTxResult(
                            phyQuery.GetResultBindings(i), response->GetArena(),
                            QueryState->GetFormatsSettings(), {});

                        YQL_ENSURE(ydbResult);
                        ++trailingResultsCount;
                        YQL_ENSURE(trailingResultsCount <= 1);
                        response->AddYdbResults()->Swap(ydbResult);
                    }

                    continue;
                }

                TMaybe<ui64> effectiveRowsLimit = FillSettings.RowsLimitPerWrite;
                if (QueryState->PreparedQuery->GetResults(i).GetRowsLimit()) {
                    effectiveRowsLimit = QueryState->PreparedQuery->GetResults(i).GetRowsLimit();
                }

                auto* ydbResult = QueryState->QueryData->GetYdbTxResult(
                    phyQuery.GetResultBindings(i), response->GetArena(),
                    QueryState->GetFormatsSettings(), effectiveRowsLimit);
                response->AddYdbResults()->Swap(ydbResult);
            }
        }

        resEv->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
        STLOG_D("Create QueryResponse for action with SUCCESS status",
            (action, QueryState->GetAction()),
            (trace_id, TraceId()));

        QueryResponse = std::move(resEv);


        ProcessNextStatement();
    }

    void ProcessNextStatement() {
        if (ExecuteNextStatementPart()) {
            return;
        }

        if (QueryState->ProcessingLastStatement()) {
            Cleanup();
            return;
        }
        QueryState->PrepareNextStatement();
        CompileStatement();
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

        STLOG_W("ReplyQueryCompileError, remove tx",
            (status, QueryState->CompileResult->Status),
            (issues, Join(", ", QueryResponse->Record.GetResponse().GetQueryIssues())),
            (tx_id, txId.GetHumanStr()),
            (trace_id, TraceId()));

        if (auto ctx = Transactions.ReleaseTransaction(txId)) {
            ctx->Invalidate();
            Transactions.AddToBeAborted(std::move(ctx));
        }

        auto* record = &QueryResponse->Record;
        auto* response = record->MutableResponse();
        FillTxInfo(response);
        FillPoolId(response);
        record->SetConsumedRu(1);

        Cleanup(IsFatalError(record->GetYdbStatus()));
    }

    void ReplySplitError(TEvKqp::TEvSplitResponse* ev) {
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        auto& record = QueryResponse->Record;

        record.SetYdbStatus(ev->Status);
        auto& response = *record.MutableResponse();
        AddQueryIssues(response, ev->Issues);

        auto txId = TTxId();
        if (auto ctx = Transactions.ReleaseTransaction(txId)) {
            ctx->Invalidate();
            Transactions.AddToBeAborted(std::move(ctx));
        }

        FillTxInfo(&response);
        FillPoolId(&response);

        Cleanup(false);
    }

    void ReplyProcessError(const TEvKqp::TEvQueryRequest::TPtr& request, Ydb::StatusIds::StatusCode ydbStatus,
            const TString& message)
    {
        ui64 proxyRequestId = request->Cookie;
        STLOG_W("Reply query error, msg: " << message,
            (proxy_request_id, proxyRequestId),
            (trace_id, TraceId()));
        auto response = std::make_unique<TEvKqp::TEvQueryResponse>();
        response->Record.SetYdbStatus(ydbStatus);
        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message);
        NYql::TIssues issues;
        issues.AddIssue(issue);
        NYql::IssuesToMessage(issues, response->Record.MutableResponse()->MutableQueryIssues());
        AddTrailingInfo(response->Record);

        NDataIntegrity::LogIntegrityTrails(
            request->Get()->GetTraceId(),
            request->Get()->GetAction(),
            request->Get()->GetType(),
            response,
            TlsActivationContext->AsActorContext()
        );

        Send(request->Sender, response.release(), 0, proxyRequestId);
    }

    void ReplyResolveError(const TEvKqpExecuter::TEvTableResolveStatus& ev) {
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        auto& record = QueryResponse->Record;

        record.SetYdbStatus(ev.Status);
        auto& response = *record.MutableResponse();
        AddQueryIssues(response, ev.Issues);

        auto txId = TTxId();
        if (auto ctx = Transactions.ReleaseTransaction(txId)) {
            ctx->Invalidate();
            Transactions.AddToBeAborted(std::move(ctx));
        }

        FillTxInfo(&response);
        FillPoolId(&response);

        Cleanup(false);
    }

    void ReplyResolveError(const NShardResolver::TEvShardsResolveStatus& ev) {
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        auto& record = QueryResponse->Record;

        record.SetYdbStatus(ev.Status);
        auto& response = *record.MutableResponse();
        AddQueryIssues(response, ev.Issues);

        auto txId = TTxId();
        if (auto ctx = Transactions.ReleaseTransaction(txId)) {
            ctx->Invalidate();
            Transactions.AddToBeAborted(std::move(ctx));
        }

        FillTxInfo(&response);
        FillPoolId(&response);

        Cleanup(false);
    }

    void ReplyBusy(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyProcessError(ev, Ydb::StatusIds::SESSION_BUSY, "Pending previous query completion");
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
        Y_ABORT_UNLESS(QueryState);
        YQL_ENSURE(Counters);

        auto& record = QueryResponse->Record;
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        AddTrailingInfo(record);

        if (QueryState->KeepSession) {
            response.SetSessionId(SessionId);
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            if (QueryState) {
                if (QueryState->KqpSessionSpan) {
                    QueryState->KqpSessionSpan.EndOk();
                }
                LWTRACK(KqpSessionReplySuccess, QueryState->Orbit, record.GetArena() ? record.GetArena()->SpaceUsed() : 0);
            }
        } else {
            if (QueryState) {
                if (QueryState->KqpSessionSpan) {
                    QueryState->KqpSessionSpan.EndError(response.DebugString());
                }
                LWTRACK(KqpSessionReplyError, QueryState->Orbit, TStringBuilder() << status);
            }
        }

        Counters->ReportResponseStatus(Settings.DbCounters, record.ByteSize(), record.GetYdbStatus());
        for (auto& issue : record.GetResponse().GetQueryIssues()) {
            Counters->ReportIssues(Settings.DbCounters, CachedIssueCounters, issue);
        }

        NDataIntegrity::LogIntegrityTrails(
            QueryState->UserRequestContext->TraceId,
            QueryState->GetAction(),
            QueryState->GetType(),
            QueryResponse,
            TlsActivationContext->AsActorContext()
        );

        Send<ESendingType::Tail>(QueryState->Sender, QueryResponse.release(), 0, QueryState->ProxyRequestId);
        STLOG_D("Sent query response back to proxy",
            (proxy_request_id, QueryState->ProxyRequestId),
            (proxy_id, QueryState->Sender.ToString()),
            (trace_id, TraceId()));

        if (IsFatalError(status)) {
            STLOG_N("SessionActor destroyed",
            (status, status),
            (trace_id, TraceId()));
            Counters->ReportSessionActorClosedError(Settings.DbCounters);
        }
    }

    void FillCompileStatus(const TKqpCompileResult::TConstPtr& compileResult,
        NKikimrKqp::TEvQueryResponse& ev)
    {
        ev.SetYdbStatus(compileResult->Status);

        auto& response = *ev.MutableResponse();
        AddQueryIssues(response, compileResult->Issues);

        if (compileResult->Status == Ydb::StatusIds::SUCCESS) {
            response.SetPreparedQuery(compileResult->Uid);

            auto& preparedQuery = compileResult->PreparedQuery;
            for (auto& param : QueryState->GetResultParams()) {
                *response.AddQueryParameters() = param;
            }

            response.SetQueryPlan(preparedQuery->GetPhysicalQuery().GetQueryPlan());
            response.SetQueryAst(preparedQuery->GetPhysicalQuery().GetQueryAst());
            response.SetQueryDiagnostics(QueryState->ReplayMessage);

            const auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            FillColumnsMeta(phyQuery, response);
        } else {
            if (compileResult->Status == Ydb::StatusIds::TIMEOUT && QueryState->QueryDeadlines.CancelAt) {
                // The compile timeout cause cancelation execution of request.
                // So in case of cancel after we can reply with canceled status
                ev.SetYdbStatus(Ydb::StatusIds::CANCELLED);
            }

            auto& preparedQuery = compileResult->PreparedQuery;
            if (preparedQuery && QueryState->ReportStats() && QueryState->GetStatsMode() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
                response.SetQueryAst(preparedQuery->GetPhysicalQuery().GetQueryAst());
            }
        }
    }

    void HandleReady(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        STLOG_I("Session closed due to explicit close event",
            (trace_id, TraceId()));
        Counters->ReportSessionActorClosedRequest(Settings.DbCounters);
        CleanupAndPassAway();
    }

    void HandleExecute(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        YQL_ENSURE(QueryState);
        QueryState->KeepSession = false;
        {
            auto abort = MakeHolder<NYql::NDq::TEvDq::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Query execution is cancelled because session was requested to be closed.");
            Send(SelfId(), abort.Release());
        }
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
            STLOG_N("Started session shutdown",
                (trace_id, TraceId()));
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
            STLOG_N("Reached hard shutdown timeout",
                (trace_id, TraceId()));
            Send(SelfId(), new TEvKqp::TEvCloseSessionRequest());
        } else {
            ScheduleNextShutdownTick();
            STLOG_I("Schedule next shutdown tick",
                (trace_id, TraceId()));
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

        if (!txCtx->TxManager) {
            // Should tx with empty LocksMap be aborted?
            for (auto& [lockId, lock] : txCtx->Locks.LocksMap) {
                auto dsLock = ExtractLock(lock.GetValueRef(txCtx->Locks.LockType));
                request.DataShardLocks[dsLock.GetDataShard()].emplace_back(dsLock);
            }
        }

        SendToExecuter(txCtx, std::move(request), true);
    }

    void ResetTxState() {
        if (QueryState->TxCtx) {
            QueryState->TxCtx->ClearDeferredEffects();
            QueryState->TxCtx->Locks.Clear();
            QueryState->TxCtx->TxManager.reset();
            QueryState->TxCtx->Finish();
        }
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
                Transactions.ReleaseTransaction(QueryState->TxId.GetValue());
            }
            DiscardPersistentSnapshot(txCtx->SnapshotHandle);
        }

        if (isFinal)
            Counters->ReportSessionActorClosedRequest(Settings.DbCounters);

        if (isFinal) {
            // no longer intrested in any compilation responses
            CompilationCookie->store(false);
        }

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

        if (QueryState && QueryState->PoolHandlerActor) {
            if (!CleanupCtx) {
                CleanupCtx.reset(new TKqpCleanupCtx);
            }
            CleanupCtx->Final = isFinal;
            CleanupCtx->IsWaitingForWorkloadServiceCleanup = true;

            const auto& stats = QueryState->QueryStats;
            auto event = std::make_unique<NWorkload::TEvCleanupRequest>(
                QueryState->UserRequestContext->DatabaseId, SessionId, QueryState->UserRequestContext->PoolId,
                TDuration::MicroSeconds(stats.DurationUs), TDuration::MicroSeconds(stats.WorkerCpuTimeUs)
            );

            auto forwardId = MakeKqpWorkloadServiceId(SelfId().NodeId());
            Send(new IEventHandle(*QueryState->PoolHandlerActor, SelfId(), event.release(), IEventHandle::FlagForwardOnNondelivery, 0, &forwardId));
            QueryState->PoolHandlerActor = Nothing();
        }

        if (QueryState && QueryState->IsSingleNodeExecution()) {
            Counters->TotalSingleNodeReqCount->Inc();
            if (!QueryState->IsLocalExecution(SelfId().NodeId())) {
                Counters->NonLocalSingleNodeReqCount->Inc();
            }
        }

        STLOG_I("Cleanup start",
            (is_final, isFinal),
            (has_cleanup_ctx, bool{CleanupCtx}),
            (transactions_to_be_aborted_size, CleanupCtx ? CleanupCtx->TransactionsToBeAborted.size() : 0),
            (worker_id, workerId ? *workerId : TActorId()),
            (workload_service_cleanup, CleanupCtx ? CleanupCtx->IsWaitingForWorkloadServiceCleanup : false),
            (trace_id, TraceId()));
        if (CleanupCtx) {
            Become(&TKqpSessionActor::CleanupState);
        } else {
            EndCleanup(isFinal);
        }
    }

    void HandleCleanup(TEvKqp::TEvCloseSessionResponse::TPtr&) {
        CleanupCtx->IsWaitingForWorkerToClose = false;
        if (CleanupCtx->CleanupFinished()) {
            EndCleanup(CleanupCtx->Final);
        }
    }

    void HandleNoop(TEvents::TEvUndelivered::TPtr& ev) {
        // outdated TEvUndelivered from another executer.
        // it this case we should just ignore the event.
        Y_ENSURE(ExecuterId != ev->Sender);
    }

    void HandleCleanup(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        if (ev->Sender != ExecuterId) {
            return;
        }
        if (QueryState) {
            QueryState->Orbit = std::move(ev->Get()->Orbit);
        }
        ExecuterId = {};

        auto& response = ev->Get()->Record.GetResponse();
        if (response.GetStatus() != Ydb::StatusIds::SUCCESS) {
            TIssues issues;
            IssuesFromMessage(response.GetIssues(), issues);
            STLOG_E("Failed to cleanup",
                (issues, issues.ToString()),
                (trace_id, TraceId()));

            for (const auto& txCtx : CleanupCtx->TransactionsToBeAborted) {
                AFL_ENSURE(txCtx);
                // Terminate BufferActors without waiting
                TerminateBufferActor(txCtx);
            }
            CleanupCtx->TransactionsToBeAborted.clear();

            EndCleanup(CleanupCtx->Final);
            return;
        }

        YQL_ENSURE(CleanupCtx);
        CleanupCtx->TransactionsToBeAborted.pop_front();
        if (CleanupCtx->TransactionsToBeAborted.size()) {
            SendRollbackRequest(CleanupCtx->TransactionsToBeAborted.front().Get());
        } else if (CleanupCtx->CleanupFinished()) {
            EndCleanup(CleanupCtx->Final);
        }
    }

    void HandleCleanup(NWorkload::TEvCleanupResponse::TPtr& ev) {
        YQL_ENSURE(CleanupCtx);
        CleanupCtx->IsWaitingForWorkloadServiceCleanup = false;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS && ev->Get()->Status != Ydb::StatusIds::NOT_FOUND) {
            STLOG_E("Failed to cleanup workload service",
                (status, ev->Get()->Status),
                (issues, ev->Get()->Issues.ToOneLineString()),
                (trace_id, TraceId()));
        }

        if (CleanupCtx->CleanupFinished()) {
            EndCleanup(CleanupCtx->Final);
        }
    }

    void EndCleanup(bool isFinal) {
        STLOG_D("EndCleanup",
            (is_final, isFinal),
            (trace_id, TraceId()));

        if (QueryResponse)
            Reply();

        if (CleanupCtx)
            Counters->ReportSessionActorCleanupLatency(Settings.DbCounters, TInstant::Now() - CleanupCtx->Start);

        if (isFinal) {
            auto userToken = QueryState ? QueryState->UserToken : TIntrusiveConstPtr<NACLib::TUserToken>();
            Become(&TKqpSessionActor::FinalCleanupState);

            STLOG_D("Cleanup temp tables",
                (temp_tables_size, TempTablesState.TempTables.size()),
                (trace_id, TraceId()));
            auto tempTablesManager = CreateKqpTempTablesManager(
                std::move(TempTablesState), std::move(userToken), SelfId(), Settings.Database);

            RegisterWithSameMailbox(tempTablesManager);
            return;
        } else {
            CleanupCtx.reset();
            ExecuterId = TActorId{};
            bool doNotKeepSession = QueryState && !QueryState->KeepSession;
            QueryState.reset();
            if (doNotKeepSession) {
                // TEvCloseSessionRequest was received in final=false CleanupState, so actor should rerun Cleanup with final=true
                CleanupAndPassAway();
            } else {
                Become(&TKqpSessionActor::ReadyState);
            }
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

    void ReplyQueryError(Ydb::StatusIds::StatusCode ydbStatus,
        const TString& message, std::optional<google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>> issues = {})
    {
        STLOG_W("Create QueryResponse for error on request, msg: " << message,
            (status, ydbStatus),
            (issues, issues ? Join(", ", *issues) : TString()),
            (trace_id, TraceId()));
        QueryResponse = std::make_unique<TEvKqp::TEvQueryResponse>();
        QueryResponse->Record.SetYdbStatus(ydbStatus);

        auto* response = QueryResponse->Record.MutableResponse();

        Y_ENSURE(QueryState);
        if (QueryState->CompileResult) {
            AddQueryIssues(*response, QueryState->CompileResult->Issues);
        }

        FillStats(&QueryResponse->Record);

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
        FillPoolId(response);

        ExecuterId = TActorId{};
        Cleanup(IsFatalError(ydbStatus));
    }

    void Handle(TEvKqp::TEvCancelQueryRequest::TPtr& ev) {
        {
            auto abort = MakeHolder<NYql::NDq::TEvDq::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Request was canceled");
            Send(SelfId(), abort.Release());
        }

        {
            auto resp = MakeHolder<TEvKqp::TEvCancelQueryResponse>();
            resp->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            Send(ev->Sender, resp.Release(), 0, ev->Cookie);
        }
    }

    void HandleFinalCleanup(TEvents::TEvGone::TPtr&) {
        auto lifeSpan = TInstant::Now() - CreationTime;
        Counters->ReportSessionActorFinished(Settings.DbCounters, lifeSpan);
        Counters->ReportQueriesPerSessionActor(Settings.DbCounters, QueryId);

        auto closeEv = std::make_unique<TEvKqp::TEvCloseSessionResponse>();
        closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        closeEv->Record.MutableResponse()->SetSessionId(SessionId);
        closeEv->Record.MutableResponse()->SetClosed(true);
        Send(Owner, closeEv.release());

        STLOG_D("Session actor destroyed",
            (trace_id, TraceId()));
        PassAway();
    }

    void HandleFinalCleanup(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ReplyProcessError(ev, Ydb::StatusIds::BAD_SESSION, "Session is under shutdown");
    }

    STFUNC(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                // common event handles for all states.
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, Handle);
                hFunc(TEvKqp::TEvQueryRequest, Handle);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleReady);
                hFunc(TEvKqp::TEvCancelQueryRequest, Handle);

                // forgotten messages from previous aborted request
                hFunc(TEvKqp::TEvCompileResponse, HandleNoop);
                hFunc(TEvKqp::TEvSplitResponse, HandleNoop);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleNoop);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop)
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNoop);
                hFunc(TEvents::TEvUndelivered, HandleNoop);
                hFunc(NWorkload::TEvContinueRequest, HandleNoop);
                // message from KQP proxy in case of our reply just after kqp proxy timer tick
                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleNoop);
                hFunc(TEvKqpBuffer::TEvError, Handle);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleNoop);

            default:
                UnexpectedEvent("ReadyState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        } catch (const TMemoryLimitExceededException&) {
            ReplyQueryError(Ydb::StatusIds::INTERNAL_ERROR,
                BuildMemoryLimitExceptionMessage());
        }
    }

    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                // common event handles for all states.
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, Handle);
                hFunc(TEvKqp::TEvQueryRequest, Handle);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);

                hFunc(TEvents::TEvUndelivered, Handle);
                hFunc(NWorkload::TEvContinueRequest, Handle);
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleExecute);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleExecute)

                hFunc(TEvKqpExecuter::TEvStreamData, HandleExecute);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleExecute);

                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleExecute);
                hFunc(TEvKqpBuffer::TEvError, Handle);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleExecute);
                hFunc(NGRpcService::TEvClientLost, HandleClientLost);
                hFunc(TEvKqp::TEvCancelQueryRequest, Handle);

                // forgotten messages from previous aborted request
                hFunc(TEvKqp::TEvCompileResponse, Handle);
                hFunc(TEvKqp::TEvParseResponse, Handle);
                hFunc(TEvKqp::TEvSplitResponse, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);

                // always come from WorkerActor
                hFunc(TEvKqp::TEvQueryResponse, ForwardResponse);
            default:
                UnexpectedEvent("ExecuteState", ev);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
        } catch (const yexception& ex) {
            InternalError(ex.what());
        } catch (const TMemoryLimitExceededException&) {
            ReplyQueryError(Ydb::StatusIds::UNDETERMINED,
                BuildMemoryLimitExceptionMessage());
        }
    }

    // optional -- only if there were any TransactionsToBeAborted
    STATEFN(CleanupState) {
        try {
            switch (ev->GetTypeRewrite()) {
                // common event handles for all states.
                hFunc(TEvKqp::TEvInitiateSessionShutdown, Handle);
                hFunc(TEvKqp::TEvContinueShutdown, Handle);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, Handle);
                hFunc(TEvKqp::TEvQueryRequest, Handle);

                hFunc(TEvKqpExecuter::TEvTxResponse, HandleCleanup);
                hFunc(NWorkload::TEvCleanupResponse, HandleCleanup);

                hFunc(TEvKqp::TEvCloseSessionRequest, HandleCleanup);
                hFunc(NGRpcService::TEvClientLost, HandleNoop);
                hFunc(TEvKqp::TEvCancelQueryRequest, HandleNoop);

                // forgotten messages from previous aborted request
                hFunc(TEvKqp::TEvCompileResponse, HandleNoop);
                hFunc(TEvKqp::TEvSplitResponse, HandleNoop);
                hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleNoop);
                hFunc(TEvKqpBuffer::TEvError, HandleCleanup);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNoop);
                hFunc(TEvents::TEvUndelivered, HandleNoop);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleNoop);
                hFunc(TEvKqpExecuter::TEvStreamData, HandleNoop);
                hFunc(NWorkload::TEvContinueRequest, HandleNoop);

                // always come from WorkerActor
                hFunc(TEvKqp::TEvCloseSessionResponse, HandleCleanup);
                hFunc(TEvKqp::TEvQueryResponse, HandleNoop);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop)
            default:
                UnexpectedEvent("CleanupState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        } catch (const TMemoryLimitExceededException&) {
            ReplyQueryError(Ydb::StatusIds::INTERNAL_ERROR,
                BuildMemoryLimitExceptionMessage());
        }
    }

    STATEFN(FinalCleanupState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvGone, HandleFinalCleanup);
                hFunc(TEvents::TEvUndelivered, HandleNoop);
                hFunc(TEvKqpSnapshot::TEvCreateSnapshotResponse, Handle);
                hFunc(NWorkload::TEvContinueRequest, HandleNoop);
                hFunc(TEvKqp::TEvQueryRequest, HandleFinalCleanup);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        } catch (const TMemoryLimitExceededException&) {
            ReplyQueryError(Ydb::StatusIds::INTERNAL_ERROR,
                BuildMemoryLimitExceptionMessage());
        }
    }

private:

    TString CurrentStateFuncName() const {
        const auto& func = CurrentStateFunc();
        if (func == &TThis::ReadyState) {
            return "ReadyState";
        } else if (func == &TThis::ExecuteState) {
            return "ExecuteState";
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
        STLOG_E("Internal error" << message,
            (trace_id, TraceId()));
        if (QueryState) {
            ReplyQueryError(Ydb::StatusIds::INTERNAL_ERROR, message);
        } else {
            CleanupAndPassAway();
        }
    }

    TString BuildMemoryLimitExceptionMessage() const {
        if (QueryState && QueryState->TxCtx) {
            return TStringBuilder() << "Memory limit exception at " << CurrentStateFuncName()
                << ", current limit is " << QueryState->TxCtx->TxAlloc->Alloc->GetLimit() << " bytes.";
        } else {
            return TStringBuilder() << "Memory limit exception at " << CurrentStateFuncName();
        }
    }

    void ProcessTopicOps(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
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

        QueryState->TxCtx->TopicOperations.CacheSchemeCacheNavigate(response->ResultSet);

        if (HasTopicWriteOperations() && !HasTopicApiWriteOperations() && !HasKafkaApiWriteOperations()) {
            Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId, 0, QueryState->QueryId);
            return;
        }

        ReplySuccess();
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        if (CurrentStateFunc() != &TThis::ExecuteState || ev->Cookie < QueryId) {
            return;
        }

        YQL_ENSURE(QueryState);
        YQL_ENSURE(QueryState->GetAction() == NKikimrKqp::QUERY_ACTION_TOPIC);

        SetTopicWriteId(NLongTxService::TLockHandle(ev->Get()->TxId, TActivationContext::ActorSystem()));

        ReplySuccess();
    }

    bool HasTopicWriteOperations() const {
        return QueryState->TxCtx->TopicOperations.HasWriteOperations();
    }

    bool HasKafkaApiWriteOperations() const {
        return QueryState->TxCtx->TopicOperations.HasKafkaOperations() && QueryState->TxCtx->TopicOperations.HasWriteOperations();
    }

    bool HasTopicApiWriteOperations() const {
        return QueryState->TxCtx->TopicOperations.HasWriteId();
    }

    ui64 GetTopicWriteId() const {
        return QueryState->TxCtx->TopicOperations.GetWriteId();
    }

    void SetTopicWriteId(NLongTxService::TLockHandle handle) {
        QueryState->TxCtx->TopicOperations.SetWriteId(std::move(handle));
    }

    void TerminateBufferActor(TIntrusivePtr<TKqpTransactionContext> ctx) {
        if (ctx && ctx->BufferActorId) {
            Send(ctx->BufferActorId, new TEvKqpBuffer::TEvTerminate{});
            ctx->BufferActorId = {};
        }
    }

    bool OnUnhandledException(const std::exception_ptr& exception) override {
        try {
            if (exception) {
                std::rethrow_exception(exception);
            }
        } catch (const TRequestFail& ex) {
            ReplyQueryError(ex.Status, ex.what(), ex.Issues);
            return true;
        } catch (const yexception& ex) {
            InternalError(ex.what());
            return true;
        } catch (const TMemoryLimitExceededException&) {
            ReplyQueryError(Ydb::StatusIds::UNDETERMINED,
                BuildMemoryLimitExceptionMessage());
            return true;
        }

        return false;
    }

    bool OnUnhandledException(const std::exception&) override {
        return false;
    }

private:
    TActorId Owner;
    TKqpQueryCachePtr QueryCache;
    TString SessionId;

    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    // cached lookups to issue counters
    THashMap<ui32, ::NMonitoring::TDynamicCounters::TCounterPtr> CachedIssueCounters;
    TInstant CreationTime;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpRequestCounters> RequestCounters;
    TKqpWorkerSettings Settings;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    TKqpSettings::TConstPtr KqpSettings;
    std::optional<TActorId> WorkerId;
    TActorId ExecuterId;
    NWilson::TSpan AcquireSnapshotSpan;

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

    TKqpTempTablesState TempTablesState;

    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    TActorId KqpTempTablesAgentActor;
    std::shared_ptr<std::atomic<bool>> CompilationCookie;

    TGUCSettings::TPtr GUCSettings;
    std::shared_ptr<NYql::NDq::IDqChannelService> ChannelService;
};

} // namespace

IActor* CreateKqpSessionActor(const TActorId& owner,
    TKqpQueryCachePtr queryCache,
    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager,
    std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> caFactory, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const TActorId& kqpTempTablesAgentActor,
    std::shared_ptr<NYql::NDq::IDqChannelService> channelService)
{
    return new TKqpSessionActor(
        owner, std::move(queryCache),
        std::move(resourceManager), std::move(caFactory), sessionId, kqpSettings, workerSettings, federatedQuerySetup,
                                std::move(asyncIoFactory),  std::move(moduleResolverState), counters,
                                queryServiceConfig, kqpTempTablesAgentActor, channelService);
}

}
}
