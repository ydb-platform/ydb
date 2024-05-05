#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/common/rows_proto_splitter.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/proto/result_set_meta.pb.h>
#include <ydb/core/kqp/run_script_actor/kqp_run_script_actor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/generic/guid.h>
#include <util/generic/utility.h>

namespace NKikimr::NKqp {

using namespace NKikimr::NKqp::NPrivate;

namespace {

#define KQP_PROXY_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)

constexpr TDuration LEASE_DURATION = TDuration::Seconds(30);
constexpr TDuration DEADLINE_OFFSET = TDuration::Minutes(20);
constexpr TDuration BRO_RUN_INTERVAL = TDuration::Minutes(60);

TString SerializeIssues(const NYql::TIssues& issues) {
    NYql::TIssue root;
    for (const NYql::TIssue& issue : issues) {
        root.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }
    Ydb::Issue::IssueMessage rootMessage;
    if (issues) {
        NYql::IssueToMessage(root, &rootMessage);
    }
    return NProtobufJson::Proto2Json(rootMessage, NProtobufJson::TProto2JsonConfig());
}

NYql::TIssues DeserializeIssues(const TString& issuesSerialized) {
    Ydb::Issue::IssueMessage rootMessage = NProtobufJson::Json2Proto<Ydb::Issue::IssueMessage>(issuesSerialized);
    NYql::TIssue root = NYql::IssueFromMessage(rootMessage);

    NYql::TIssues issues;
    for (const auto& issuePtr : root.GetSubIssues()) {
        issues.AddIssue(*issuePtr);
    }
    return issues;
}


class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(TString sessionId = {})
        : NKikimr::TQueryBase(NKikimrServices::KQP_PROXY, sessionId)
    {}
};


class TScriptExecutionsTablesCreator : public TActorBootstrapped<TScriptExecutionsTablesCreator> {
public:
    explicit TScriptExecutionsTablesCreator(THolder<NActors::IEventBase> resultEvent)
        : ResultEvent(std::move(resultEvent))
    {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TScriptExecutionsTablesCreator>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        Become(&TScriptExecutionsTablesCreator::StateFunc);
        RunCreateScriptExecutions();
        RunCreateScriptExecutionLeases();
        RunCreateScriptResultSets();
    }

private:
    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, const char* columnType) {
        NKikimrSchemeOp::TColumnDescription desc;
        desc.SetName(columnName);
        desc.SetType(columnType);
        return desc;
    }

    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, NScheme::TTypeId columnType) {
        return Col(columnName, NScheme::TypeName(columnType));
    }

    static NKikimrSchemeOp::TTTLSettings TtlCol(const TString& columnName) {
        NKikimrSchemeOp::TTTLSettings settings;
        settings.MutableEnabled()->SetExpireAfterSeconds(DEADLINE_OFFSET.Seconds());
        settings.MutableEnabled()->SetColumnName(columnName);
        settings.MutableEnabled()->MutableSysSettings()->SetRunInterval(BRO_RUN_INTERVAL.MicroSeconds());
        return settings;
    }

    void RunCreateScriptExecutions() {
        TablesCreating++;
        Register(
            CreateTableCreator(
                { ".metadata", "script_executions" },
                {
                    Col("database", NScheme::NTypeIds::Text),
                    Col("execution_id", NScheme::NTypeIds::Text),
                    Col("run_script_actor_id", NScheme::NTypeIds::Text),
                    Col("operation_status", NScheme::NTypeIds::Int32),
                    Col("execution_status", NScheme::NTypeIds::Int32),
                    Col("finalization_status", NScheme::NTypeIds::Int32),
                    Col("execution_mode", NScheme::NTypeIds::Int32),
                    Col("start_ts", NScheme::NTypeIds::Timestamp),
                    Col("end_ts", NScheme::NTypeIds::Timestamp),
                    Col("query_text", NScheme::NTypeIds::Text),
                    Col("syntax", NScheme::NTypeIds::Int32),
                    Col("ast", NScheme::NTypeIds::Text),
                    Col("ast_compressed", NScheme::NTypeIds::String),
                    Col("ast_compression_method", NScheme::NTypeIds::Text),
                    Col("issues", NScheme::NTypeIds::JsonDocument),
                    Col("plan", NScheme::NTypeIds::JsonDocument),
                    Col("meta", NScheme::NTypeIds::JsonDocument),
                    Col("parameters", NScheme::NTypeIds::String), // TODO: store aparameters separately to support bigger storage.
                    Col("result_set_metas", NScheme::NTypeIds::JsonDocument),
                    Col("stats", NScheme::NTypeIds::JsonDocument),
                    Col("expire_at", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                    Col("customer_supplied_id", NScheme::NTypeIds::Text),
                    Col("user_token", NScheme::NTypeIds::Text),
                    Col("script_sinks", NScheme::NTypeIds::JsonDocument),
                    Col("script_secret_names", NScheme::NTypeIds::JsonDocument),
                },
                { "database", "execution_id" },
                NKikimrServices::KQP_PROXY,
                TtlCol("expire_at")
            )
        );
    }

    void RunCreateScriptExecutionLeases() {
        TablesCreating++;
        Register(
            CreateTableCreator(
                { ".metadata", "script_execution_leases" },
                {
                    Col("database", NScheme::NTypeIds::Text),
                    Col("execution_id", NScheme::NTypeIds::Text),
                    Col("lease_deadline", NScheme::NTypeIds::Timestamp),
                    Col("lease_generation", NScheme::NTypeIds::Int64),
                    Col("expire_at", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                },
                { "database", "execution_id" },
                NKikimrServices::KQP_PROXY,
                TtlCol("expire_at")
            )
        );
    }

    void RunCreateScriptResultSets() {
        TablesCreating++;
        Register(
            CreateTableCreator(
                { ".metadata", "result_sets" },
                {
                    Col("database", NScheme::NTypeIds::Text),
                    Col("execution_id", NScheme::NTypeIds::Text),
                    Col("result_set_id", NScheme::NTypeIds::Int32),
                    Col("row_id", NScheme::NTypeIds::Int64),
                    Col("expire_at", NScheme::NTypeIds::Timestamp),
                    Col("result_set", NScheme::NTypeIds::String),
                    Col("accumulated_size", NScheme::NTypeIds::Int64),
                },
                { "database", "execution_id", "result_set_id", "row_id" },
                NKikimrServices::KQP_PROXY,
                TtlCol("expire_at")
            )
        );
    }

    void Handle(TEvTableCreator::TEvCreateTableResponse::TPtr&) {
        Y_ABORT_UNLESS(TablesCreating > 0);
        if (--TablesCreating == 0) {
            Send(Owner, std::move(ResultEvent));
            PassAway();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvTableCreator::TEvCreateTableResponse, Handle);
    )

private:
    THolder<NActors::IEventBase> ResultEvent;
    NActors::TActorId Owner;
    size_t TablesCreating = 0;
};

Ydb::Query::ExecMode GetExecModeFromAction(NKikimrKqp::EQueryAction action) {
    switch (action) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            return Ydb::Query::EXEC_MODE_EXECUTE;
        case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            return Ydb::Query::EXEC_MODE_EXPLAIN;
        case NKikimrKqp::QUERY_ACTION_VALIDATE:
            return Ydb::Query::EXEC_MODE_VALIDATE;
        case NKikimrKqp::QUERY_ACTION_PARSE:
            return Ydb::Query::EXEC_MODE_PARSE;
        case NKikimrKqp::QUERY_ACTION_PREPARE:
            [[fallthrough]];
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            [[fallthrough]];
        case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
            [[fallthrough]];
        case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
            [[fallthrough]];
        case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
            [[fallthrough]];
        case NKikimrKqp::QUERY_ACTION_TOPIC:
            throw std::runtime_error(TStringBuilder() << "Unsupported query action: " << NKikimrKqp::EQueryAction_Name(action));
    }
}

class TCreateScriptOperationQuery : public TQueryBase {
public:
    TCreateScriptOperationQuery(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& req, TDuration operationTtl, TDuration resultsTtl, TDuration leaseDuration = TDuration::Zero(), TDuration maxRunTime = SCRIPT_TIMEOUT_LIMIT)
        : ExecutionId(executionId)
        , RunScriptActorId(runScriptActorId)
        , Request(req)
        , OperationTtl(operationTtl)
        , ResultsTtl(resultsTtl)
        , LeaseDuration(leaseDuration ? leaseDuration : LEASE_DURATION)
        , MaxRunTime(Max(maxRunTime, TDuration::Days(1)))
    {
        Y_ENSURE(MaxRunTime);
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TCreateScriptOperationQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $run_script_actor_id AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $execution_mode AS Int32;
            DECLARE $query_text AS Text;
            DECLARE $syntax AS Int32;
            DECLARE $meta AS JsonDocument;
            DECLARE $lease_duration AS Interval;
            DECLARE $execution_meta_ttl AS Interval;

            UPSERT INTO `.metadata/script_executions`
                (database, execution_id, run_script_actor_id, execution_status, execution_mode, start_ts, query_text, syntax, meta, expire_at)
            VALUES ($database, $execution_id, $run_script_actor_id, $execution_status, $execution_mode, CurrentUtcTimestamp(), $query_text, $syntax, $meta, CurrentUtcTimestamp() + $execution_meta_ttl);

            UPSERT INTO `.metadata/script_execution_leases`
                (database, execution_id, lease_deadline, lease_generation, expire_at)
            VALUES ($database, $execution_id, CurrentUtcTimestamp() + $lease_duration, 1, CurrentUtcTimestamp() + $execution_meta_ttl);
        )";

        NKikimrKqp::TScriptExecutionOperationMeta meta;
        SetDuration(OperationTtl, *meta.MutableOperationTtl());
        SetDuration(ResultsTtl, *meta.MutableResultsTtl());

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request.GetRequest().GetDatabase())
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$run_script_actor_id")
                .Utf8(ScriptExecutionRunnerActorIdString(RunScriptActorId))
                .Build()
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_STARTING)
                .Build()
            .AddParam("$execution_mode")
                .Int32(GetExecModeFromAction(Request.GetRequest().GetAction()))
                .Build()
            .AddParam("$query_text")
                .Utf8(Request.GetRequest().GetQuery())
                .Build()
            .AddParam("$syntax")
                .Int32(Request.GetRequest().GetSyntax())
                .Build()
            .AddParam("$meta")
                .JsonDocument(NProtobufJson::Proto2Json(meta, NProtobufJson::TProto2JsonConfig()))
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
                .Build()
            .AddParam("$execution_meta_ttl")
                .Interval(2 * std::min(static_cast<i64>(MaxRunTime.MicroSeconds()), std::numeric_limits<i64>::max() / 2))
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Create script execution operation. ExecutionId: " << ExecutionId << ". Result: " << status << ". Issues: " << issues.ToOneLineString());
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(ExecutionId));
        } else {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(status, std::move(issues)));
        }
    }

private:
    const TString ExecutionId;
    const NActors::TActorId RunScriptActorId;
    NKikimrKqp::TEvQueryRequest Request;
    const TDuration OperationTtl;
    const TDuration ResultsTtl;
    const TDuration LeaseDuration;
    const TDuration MaxRunTime;
};

struct TCreateScriptExecutionActor : public TActorBootstrapped<TCreateScriptExecutionActor> {
    TCreateScriptExecutionActor(TEvKqp::TEvScriptRequest::TPtr&& ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TDuration maxRunTime = SCRIPT_TIMEOUT_LIMIT, TDuration leaseDuration = TDuration::Zero())
        : Event(std::move(ev))
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
        , LeaseDuration(leaseDuration ? leaseDuration : LEASE_DURATION)
        , MaxRunTime(maxRunTime)
    {
    }

    void Bootstrap() {
        Become(&TCreateScriptExecutionActor::StateFunc);

        ExecutionId = CreateGuidAsString();

        auto operationTtl = Event->Get()->ForgetAfter ? Event->Get()->ForgetAfter : TDuration::Seconds(QueryServiceConfig.GetScriptForgetAfterDefaultSeconds());
        auto resultsTtl = Event->Get()->ResultsTtl ? Event->Get()->ResultsTtl : TDuration::Seconds(QueryServiceConfig.GetScriptResultsTtlDefaultSeconds());
        if (operationTtl) {
            resultsTtl = Min(operationTtl, resultsTtl);
        }

        // Start request
        RunScriptActorId = Register(CreateRunScriptActor(ExecutionId, Event->Get()->Record, Event->Get()->Record.GetRequest().GetDatabase(), 1, LeaseDuration, resultsTtl, QueryServiceConfig, Counters));
        Register(new TCreateScriptOperationQuery(ExecutionId, RunScriptActorId, Event->Get()->Record, operationTtl, resultsTtl, LeaseDuration, MaxRunTime));
    }

    void Handle(TEvPrivate::TEvCreateScriptOperationResponse::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            Send(RunScriptActorId, new NActors::TEvents::TEvWakeup());
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ScriptExecutionOperationFromExecutionId(ev->Get()->ExecutionId), ev->Get()->ExecutionId, Ydb::Query::EXEC_STATUS_STARTING, GetExecModeFromAction(Event->Get()->Record.GetRequest().GetAction())));
        } else {
            Send(RunScriptActorId, new NActors::TEvents::TEvPoison());
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ev->Get()->Status, std::move(ev->Get()->Issues)));
        }
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCreateScriptOperationResponse, Handle);
    )

private:
    TEvKqp::TEvScriptRequest::TPtr Event;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    TIntrusivePtr<TKqpCounters> Counters;
    TString ExecutionId;
    NActors::TActorId RunScriptActorId;
    const TDuration LeaseDuration;
    const TDuration MaxRunTime;
};

class TScriptLeaseUpdater : public TQueryBase {
public:
    TScriptLeaseUpdater(const TString& database, const TString& executionId, TDuration leaseDuration)
        : Database(database)
        , ExecutionId(executionId)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptLeaseUpdater::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT lease_deadline FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginTx());
        SetQueryResultHandler(&TScriptLeaseUpdater::OnGetLeaseInfo);
    }

    void OnGetLeaseInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }
        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() == 0) {
            LeaseExists = false;
            Finish(Ydb::StatusIds::BAD_REQUEST, "No such execution");
            return;
        }

        LeaseDeadline = TInstant::Now() + LeaseDuration;

        TString sql = R"(
            -- TScriptLeaseUpdater::OnGetLeaseInfo
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $lease_duration AS Interval;

            UPDATE `.metadata/script_execution_leases`
            SET lease_deadline=(CurrentUtcTimestamp() + $lease_duration)
            WHERE database = $database AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
                .Build();

        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
        SetQueryResultHandler(&TScriptLeaseUpdater::OnQueryResult);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvScriptLeaseUpdateResponse(LeaseExists, LeaseDeadline, status, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const TDuration LeaseDuration;
    TInstant LeaseDeadline;
    bool LeaseExists = true;
};

class TScriptLeaseUpdateActor : public TActorBootstrapped<TScriptLeaseUpdateActor> {
public:
    using TLeaseUpdateRetryActor = TQueryRetryActor<TScriptLeaseUpdater, TEvScriptLeaseUpdateResponse, TString, TString, TDuration>;

    TScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration, TIntrusivePtr<TKqpCounters> counters)
        : RunScriptActorId(runScriptActorId)
        , Database(database)
        , ExecutionId(executionId)
        , LeaseDuration(leaseDuration)
        , Counters(counters)
        , LeaseUpdateStartTime(TInstant::Now())
    {}

    void Bootstrap() {
        Register(new TLeaseUpdateRetryActor(
            SelfId(),
            TLeaseUpdateRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TLeaseUpdateRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), LeaseDuration / 2),
            Database, ExecutionId, LeaseDuration
        ));
        Become(&TScriptLeaseUpdateActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvScriptLeaseUpdateResponse, Handle);
    )

    void Handle(TEvScriptLeaseUpdateResponse::TPtr& ev) {
        if (Counters) {
            Counters->ReportLeaseUpdateLatency(TInstant::Now() - LeaseUpdateStartTime);
        }
        Send(RunScriptActorId, ev->Release().Release());
        PassAway();
    }

private:
    TActorId RunScriptActorId;
    TString Database;
    TString ExecutionId;
    TDuration LeaseDuration;
    TIntrusivePtr<TKqpCounters> Counters;
    TInstant LeaseUpdateStartTime;
};

class TCheckLeaseStatusActorBase : public TActorBootstrapped<TCheckLeaseStatusActorBase> {
    using TBase = TActorBootstrapped<TCheckLeaseStatusActorBase>;

    inline static const TDuration CHECK_ALIVE_REQUEST_TIMEOUT = TDuration::Seconds(60);

public:
    void Bootstrap() {
        OnBootstrap();
    }

    Ydb::StatusIds::StatusCode GetOperationStatus() const {
        return FinalOperationStatus;
    }

    Ydb::Query::ExecStatus GetExecStatus() const {
        return FinalExecStatus;
    }

    NYql::TIssues GetIssues() const {
        return FinalIssues;
    }

    void StartScriptFinalization(EFinalizationStatus finalizationStatus, const TString& executionId, const TString& database, TMaybe<Ydb::StatusIds::StatusCode> status, TMaybe<Ydb::Query::ExecStatus> execStatus, NYql::TIssues issues) {
        if (!status || !execStatus) {
            issues.AddIssue("Finalization is not complete");
        }

        ScriptFinalizeRequest = std::make_unique<TEvScriptFinalizeRequest>(finalizationStatus, executionId, database, status ? *status : Ydb::StatusIds::UNAVAILABLE, execStatus ? *execStatus : Ydb::Query::EXEC_STATUS_ABORTED, std::move(issues));
        RunScriptFinalizeRequest();

        Become(&TCheckLeaseStatusActorBase::StateFunc);
    }

    void StartLeaseChecking(TActorId runScriptActorId, const TString& executionId, const TString& database) {
        ScriptFinalizeRequest = std::make_unique<TEvScriptFinalizeRequest>(EFinalizationStatus::FS_ROLLBACK, executionId, database, Ydb::StatusIds::UNAVAILABLE, Ydb::Query::EXEC_STATUS_ABORTED, NYql::TIssues{ NYql::TIssue("Lease expired") });

        Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup());

        ui64 flags = IEventHandle::FlagTrackDelivery;
        if (runScriptActorId.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = runScriptActorId.NodeId();
        }
        Send(runScriptActorId, new TEvCheckAliveRequest(), flags);

        Become(&TCheckLeaseStatusActorBase::StateFunc);
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    virtual void OnBootstrap() = 0;
    virtual void OnLeaseVerified() = 0;
    virtual void OnScriptExecutionFinished(bool alreadyFinalized, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) = 0;

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvCheckAliveResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(NActors::TEvInterconnect::TEvNodeConnected);
        hFunc(TEvScriptExecutionFinished, Handle);
    )

    void RunScriptFinalizeRequest() {
        if (WaitFinishQuery) {
            return;
        }

        WaitFinishQuery = true;
        FinalOperationStatus = ScriptFinalizeRequest->Description.OperationStatus;
        FinalExecStatus = ScriptFinalizeRequest->Description.ExecStatus;
        FinalIssues = ScriptFinalizeRequest->Description.Issues;
        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), ScriptFinalizeRequest.release());
    }

    void Handle(TEvCheckAliveResponse::TPtr&) {
        OnLeaseVerified();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        RunScriptFinalizeRequest();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        RunScriptFinalizeRequest();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        RunScriptFinalizeRequest();
    }

    void Handle(TEvScriptExecutionFinished::TPtr& ev) {
        OnScriptExecutionFinished(ev->Get()->OperationAlreadyFinalized, ev->Get()->Status, std::move(ev->Get()->Issues));
    }

private:
    std::unique_ptr<TEvScriptFinalizeRequest> ScriptFinalizeRequest;
    Ydb::StatusIds::StatusCode FinalOperationStatus;
    Ydb::Query::ExecStatus FinalExecStatus;
    NYql::TIssues FinalIssues;

    bool WaitFinishQuery = false;
    std::optional<ui32> SubscribedOnSession;
};

class TCheckLeaseStatusQueryActor : public TQueryBase {
public:
    TCheckLeaseStatusQueryActor(const TString& database, const TString& executionId, ui64 cookie = 0)
        : Database(database)
        , ExecutionId(executionId)
        , Cookie(cookie)
    {}

    void OnRunQuery() override {
        const TString sql = R"(
            -- TCheckLeaseStatusQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                finalization_status,
                issues,
                run_script_actor_id
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT lease_deadline
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }
        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
            return;
        }

        result.TryNextRow();

        const TMaybe<TString> runScriptActorId = result.ColumnParser("run_script_actor_id").GetOptionalUtf8();
        if (!runScriptActorId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }
        if (!NKqp::ScriptExecutionRunnerActorIdFromString(*runScriptActorId, RunScriptActorId)) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

        const TMaybe<i32> finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
        if (finalizationStatus) {
            FinalizationStatus = static_cast<EFinalizationStatus>(*finalizationStatus);
        }

        TMaybe<TInstant> leaseDeadline;

        NYdb::TResultSetParser result2(ResultSets[1]);

        if (result2.RowsCount() > 0) {
            result2.TryNextRow();

            leaseDeadline = result2.ColumnParser(0).GetOptionalTimestamp();
        }

        if (leaseDeadline) {
            if (operationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state");
            } else if (*leaseDeadline < RunStartTime) {
                LeaseExpired = true;
                FinalizationStatus = EFinalizationStatus::FS_ROLLBACK;
            }
        } else if (operationStatus) {
            OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            TMaybe<i32> executionStatus = result.ColumnParser("execution_status").GetOptionalInt32();
            if (executionStatus) {
                ExecutionStatus = static_cast<Ydb::Query::ExecStatus>(*executionStatus);
            }
            const TMaybe<TString> issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument();
            if (issuesSerialized) {
                OperationIssues = DeserializeIssues(*issuesSerialized);
            }
        } else {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state");
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvLeaseCheckResult(OperationStatus, ExecutionStatus, std::move(OperationIssues), RunScriptActorId, LeaseExpired, FinalizationStatus), 0, Cookie);
        } else {
            Send(Owner, new TEvPrivate::TEvLeaseCheckResult(status, std::move(issues)), 0, Cookie);
        }
    }

private:
    const TInstant RunStartTime = TInstant::Now();
    const TString Database;
    const TString ExecutionId;
    const ui64 Cookie;
    TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
    TMaybe<Ydb::Query::ExecStatus> ExecutionStatus;
    TMaybe<EFinalizationStatus> FinalizationStatus;
    TMaybe<NYql::TIssues> OperationIssues;
    NActors::TActorId RunScriptActorId;
    bool LeaseExpired;
};

class TCheckLeaseStatusActor : public TCheckLeaseStatusActorBase {
public:
    TCheckLeaseStatusActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, ui64 cookie = 0)
        : ReplyActorId(replyActorId)
        , Database(database)
        , ExecutionId(executionId)
        , Cookie(cookie)
    {}

    void OnBootstrap() override {
        Register(new TCheckLeaseStatusQueryActor(Database, ExecutionId, Cookie));
        Become(&TCheckLeaseStatusActor::StateFunc);
    }

    void OnLeaseVerified() override {
        Reply();
    }

    void OnScriptExecutionFinished(bool alreadyFinalized, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status != Ydb::StatusIds::SUCCESS) {
            Reply(status, std::move(issues));
            return;
        }

        if (alreadyFinalized) {
            // Final status and issues are unknown, the operation must be repeated
            Response->Get()->OperationStatus = Nothing();
            Response->Get()->ExecutionStatus = Nothing();
            Response->Get()->OperationIssues = Nothing();
        } else {
            Response->Get()->OperationStatus = GetOperationStatus();
            Response->Get()->ExecutionStatus = GetExecStatus();
            Response->Get()->OperationIssues = GetIssues();
        }

        Reply();
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
    )

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        Response = std::move(ev);

        if (!Response->Get()->FinalizationStatus) {
            Reply();
        } else if (Response->Get()->LeaseExpired) {
            StartLeaseChecking(Response->Get()->RunScriptActorId, ExecutionId, Database);
        } else {
            StartScriptFinalization(*Response->Get()->FinalizationStatus, ExecutionId, Database, Response->Get()->OperationStatus, Response->Get()->ExecutionStatus, Response->Get()->Issues);
        }
    }

    void Reply() {
        Send(ReplyActorId, Response->Release());
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        Send(ReplyActorId, new TEvPrivate::TEvLeaseCheckResult(status, std::move(issues)));
        PassAway();
    }

private:
    NActors::TActorId ReplyActorId;
    TString Database;
    TString ExecutionId;
    ui64 Cookie;
    TEvPrivate::TEvLeaseCheckResult::TPtr Response;
};

class TForgetScriptExecutionOperationQueryActor : public TQueryBase {
    static constexpr i64 MAX_NUMBER_ROWS_IN_BATCH = 100000;

public:
    TForgetScriptExecutionOperationQueryActor(const TString& executionId, const TString& database, TInstant operationDeadline)
        : ExecutionId(executionId)
        , Database(database)
        , Deadline(operationDeadline)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TForgetScriptExecutionOperationQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            DELETE
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;

            SELECT MAX(result_set_id) AS max_result_set_id, MAX(row_id) AS max_row_id
            FROM `.metadata/result_sets`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            DELETE
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TForgetScriptExecutionOperationQueryActor::OnGetResultsInfo);
    }

    void OnGetResultsInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        result.TryNextRow();

        TMaybe<i64> maxResultSetId = result.ColumnParser("max_result_set_id").GetOptionalInt32();
        if (!maxResultSetId) {
            Finish();
            return;
        }
        NumberRowsInBatch = std::max(MAX_NUMBER_ROWS_IN_BATCH / (*maxResultSetId + 1), 1l);

        TMaybe<i64> maxRowId = result.ColumnParser("max_row_id").GetOptionalInt64();
        if (!maxRowId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row id is not specified");
            return;
        }
        MaxRowId = *maxRowId;

        ClearTimeInfo();
        DeleteScriptResults();
    }

    void DeleteScriptResults() {
        TString sql = R"(
            -- TForgetScriptExecutionOperationQueryActor::DeleteScriptResults
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $min_row_id AS Int64;
            DECLARE $max_row_id AS Int64;

            UPDATE `.metadata/result_sets`
            SET expire_at = CurrentUtcTimestamp()
            WHERE database = $database
              AND execution_id = $execution_id
              AND $min_row_id < row_id AND row_id <= $max_row_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$min_row_id")
                .Int64(MaxRowId - NumberRowsInBatch)
                .Build()
            .AddParam("$max_row_id")
                .Int64(MaxRowId)
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TForgetScriptExecutionOperationQueryActor::OnResultsDeleted);
    }

    void OnResultsDeleted() {
        MaxRowId -= NumberRowsInBatch;
        if (MaxRowId < 0) {
            Finish();
            return;
        }

        if (TInstant::Now() + 2 * GetAverageTime() >= Deadline) {
            Finish(Ydb::StatusIds::TIMEOUT, ForgetOperationTimeoutIssues());
            return;
        }

        DeleteScriptResults();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)));
    }

    static NYql::TIssues ForgetOperationTimeoutIssues() {
        return { NYql::TIssue("Forget script execution operation timeout") };
    }

private:
    TString ExecutionId;
    TString Database;
    TInstant Deadline;
    i64 NumberRowsInBatch = 0;
    i64 MaxRowId = 0;
};

class TForgetScriptExecutionOperationActor : public TActorBootstrapped<TForgetScriptExecutionOperationActor> {
public:
    using TForgetOperationRetryActor = TQueryRetryActor<TForgetScriptExecutionOperationQueryActor, TEvForgetScriptExecutionOperationResponse, TString, TString, TInstant>;

    explicit TForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {}

    void Bootstrap() {
        TMaybe<TString> executionId = NKqp::ScriptExecutionIdFromOperation(Request->Get()->OperationId);
        if (!executionId) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Incorrect operation id");
            return;
        }
        ExecutionId = *executionId;

        Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, ExecutionId));
        Become(&TForgetScriptExecutionOperationActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
        hFunc(TEvForgetScriptExecutionOperationResponse, Handle);
    )

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        ExecutionEntryExists = ev->Get()->Status != Ydb::StatusIds::NOT_FOUND;
        if (ExecutionEntryExists) {
            if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
                Reply(ev->Get()->Status, std::move(ev->Get()->Issues));
                return;
            }

            if (!ev->Get()->OperationStatus) {
                Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Operation is still running");
                return;
            }
        }

        TDuration minDelay = TDuration::MilliSeconds(10);
        TDuration maxTime = Request->Get()->Deadline - TInstant::Now();
        if (maxTime <= minDelay) {
            Reply(Ydb::StatusIds::TIMEOUT, TForgetScriptExecutionOperationQueryActor::ForgetOperationTimeoutIssues());
            return;
        }

        Register(new TForgetOperationRetryActor(
            SelfId(),
            TForgetOperationRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TForgetOperationRetryActor::Retryable, minDelay, TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), maxTime),
            ExecutionId, Request->Get()->Database, TInstant::Now() + maxTime
        ));
    }

    void Handle(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        Reply(ev->Get()->Status, std::move(ev->Get()->Issues));
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (!ExecutionEntryExists && status == Ydb::StatusIds::SUCCESS) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.AddIssue("No such execution");   
        }

        Send(Request->Sender, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, { NYql::TIssue(message) });
    }

private:
    TEvForgetScriptExecutionOperation::TPtr Request;
    TString ExecutionId;
    bool ExecutionEntryExists = true;
};

class TGetScriptExecutionOperationQueryActor : public TQueryBase {
public:
    TGetScriptExecutionOperationQueryActor(const TString& database, const NOperationId::TOperationId& operationId)
        : Database(database)
        , OperationId(operationId)
        , StartActorTime(TInstant::Now())
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TGetScriptExecutionOperationQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                run_script_actor_id,
                operation_status,
                execution_status,
                finalization_status,
                query_text,
                syntax,
                execution_mode,
                result_set_metas,
                plan,
                issues,
                stats,
                ast,
                ast_compressed,
                ast_compression_method
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_deadline
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        TMaybe<TString> maybeExecutionId = ScriptExecutionIdFromOperation(OperationId);
        Y_ENSURE(maybeExecutionId, "No execution id specified");
        ExecutionId = *maybeExecutionId;

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }
        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
            return;
        }

        result.TryNextRow();

        const TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
        if (operationStatus) {
            OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
        }

        const TMaybe<i32> finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
        if (finalizationStatus) {
            FinalizationStatus = static_cast<EFinalizationStatus>(*finalizationStatus);
        }

        Metadata.set_execution_id(*ScriptExecutionIdFromOperation(OperationId));

        const TMaybe<i32> executionStatus = result.ColumnParser("execution_status").GetOptionalInt32();
        if (executionStatus) {
            Metadata.set_exec_status(static_cast<Ydb::Query::ExecStatus>(*executionStatus));
        }

        const TMaybe<TString> sql = result.ColumnParser("query_text").GetOptionalUtf8();
        if (sql) {
            Metadata.mutable_script_content()->set_text(*sql);
        }

        const TMaybe<i32> syntax = result.ColumnParser("syntax").GetOptionalInt32();
        if (syntax) {
            Metadata.mutable_script_content()->set_syntax(static_cast<Ydb::Query::Syntax>(*syntax));
        }

        const TMaybe<i32> executionMode = result.ColumnParser("execution_mode").GetOptionalInt32();
        if (executionMode) {
            Metadata.set_exec_mode(static_cast<Ydb::Query::ExecMode>(*executionMode));
        }

        const TMaybe<TString> serializedStats = result.ColumnParser("stats").GetOptionalJsonDocument();
        if (serializedStats) {
            NJson::TJsonValue statsJson;
            NJson::ReadJsonTree(*serializedStats, &statsJson);
            NProtobufJson::Json2Proto(statsJson, *Metadata.mutable_exec_stats(), NProtobufJson::TJson2ProtoConfig());
        }

        const TMaybe<TString> plan = result.ColumnParser("plan").GetOptionalJsonDocument();
        if (plan) {
            Metadata.mutable_exec_stats()->set_query_plan(*plan);
        }

        TMaybe<TString> ast;
        const TMaybe<TString> astCompressionMethod = result.ColumnParser("ast_compression_method").GetOptionalUtf8();
        if (astCompressionMethod) {
            const TMaybe<TString> astCompressed = result.ColumnParser("ast_compressed").GetOptionalString();
            if (astCompressed) {
                const NFq::TCompressor compressor(*astCompressionMethod);
                ast = compressor.Decompress(*astCompressed);
            }
        } else {
            ast = result.ColumnParser("ast").GetOptionalUtf8();
        }
        if (ast) {
            Metadata.mutable_exec_stats()->set_query_ast(*ast);
        }

        const TMaybe<TString> issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument();
        if (issuesSerialized) {
            Issues = DeserializeIssues(*issuesSerialized);
        }

        const TMaybe<TString> serializedMetas = result.ColumnParser("result_set_metas").GetOptionalJsonDocument();
        if (serializedMetas) {
            NJson::TJsonValue value;
            if (!NJson::ReadJsonTree(*serializedMetas, &value) || value.GetType() != NJson::JSON_ARRAY) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result meta is corrupted");
                return;
            }

            for (auto i = 0; i < value.GetIntegerRobust(); i++) {
                const NJson::TJsonValue* metaValue;
                value.GetValuePointer(i, &metaValue);
                NProtobufJson::Json2Proto(*metaValue, *Metadata.add_result_sets_meta());
            }
        }

        const TMaybe<TString> runScriptActorIdString = result.ColumnParser("run_script_actor_id").GetOptionalUtf8();
        if (runScriptActorIdString) {
            ScriptExecutionRunnerActorIdFromString(*runScriptActorIdString, RunScriptActorId);
        }

        if (!OperationStatus) {
            // Check lease deadline
            NYdb::TResultSetParser deadlineResult(ResultSets[1]);
            if (deadlineResult.RowsCount() == 0) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state");
                return;
            }

            deadlineResult.TryNextRow();

            TMaybe<TInstant> leaseDeadline = deadlineResult.ColumnParser(0).GetOptionalTimestamp();
            if (!leaseDeadline) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state");
                return;
            }

            if (*leaseDeadline < StartActorTime) {
                LeaseExpired = true;
                FinalizationStatus = EFinalizationStatus::FS_ROLLBACK;
            }
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (OperationStatus) {
            Send(Owner, new TEvGetScriptExecutionOperationQueryResponse(true, LeaseExpired, FinalizationStatus, RunScriptActorId, ExecutionId, *OperationStatus, std::move(Issues), std::move(Metadata)));
        } else {
            Send(Owner, new TEvGetScriptExecutionOperationQueryResponse(false, LeaseExpired, FinalizationStatus, RunScriptActorId, ExecutionId, status, std::move(issues), std::move(Metadata)));
        }
    }

private:
    TString Database;
    NOperationId::TOperationId OperationId;
    TInstant StartActorTime;
    TString ExecutionId;
    std::optional<Ydb::StatusIds::StatusCode> OperationStatus;
    std::optional<EFinalizationStatus> FinalizationStatus;
    bool LeaseExpired = false;
    TActorId RunScriptActorId;
    NYql::TIssues Issues;
    Ydb::Query::ExecuteScriptMetadata Metadata;
};

class TGetScriptExecutionOperationActor : public TCheckLeaseStatusActorBase {
public:
    explicit TGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {}

    void OnBootstrap() override {
        Register(new TGetScriptExecutionOperationQueryActor(Request->Get()->Database, Request->Get()->OperationId));
        Become(&TGetScriptExecutionOperationActor::StateFunc);
    }

    void OnLeaseVerified() override {
        Reply();
    }

    void OnScriptExecutionFinished(bool alreadyFinalized, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status != Ydb::StatusIds::SUCCESS) {
            Reply(status, std::move(issues));
            return;
        }

        if (alreadyFinalized) {
            // Final status and issues are unknown, the operation must be repeated
            Response->Get()->Ready = false;
            Response->Get()->Status = Ydb::StatusIds::SUCCESS;
            Response->Get()->Issues.Clear();
            Response->Get()->Metadata.set_exec_status(Ydb::Query::ExecStatus::EXEC_STATUS_UNSPECIFIED);
        } else {
            Response->Get()->Ready = true;
            Response->Get()->Status = GetOperationStatus();
            Response->Get()->Issues = GetIssues();
            Response->Get()->Metadata.set_exec_status(GetExecStatus());
        }

        Reply();
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvGetScriptExecutionOperationQueryResponse, Handle);
    )

    void Handle(TEvGetScriptExecutionOperationQueryResponse::TPtr& ev) {
        Response = std::move(ev);

        if (!Response->Get()->FinalizationStatus) {
            Reply();
        } else if (Response->Get()->LeaseExpired) {
            StartLeaseChecking(Response->Get()->RunScriptActorId, Response->Get()->ExecutionId, Request->Get()->Database);
        } else {
            TMaybe<Ydb::Query::ExecStatus> execStatus;
            if (Response->Get()->Ready) {
                execStatus = Response->Get()->Metadata.exec_status();
            }
            StartScriptFinalization(*Response->Get()->FinalizationStatus, Response->Get()->ExecutionId, Request->Get()->Database, Response->Get()->Status, execStatus, Response->Get()->Issues);
        }
    }

    void Reply() {
        TMaybe<google::protobuf::Any> metadata;
        metadata.ConstructInPlace().PackFrom(Response->Get()->Metadata);
        Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(Response->Get()->Ready, Response->Get()->Status, std::move(Response->Get()->Issues), std::move(metadata)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(status, std::move(issues)));
        PassAway();
    }

private:
    TEvGetScriptExecutionOperation::TPtr Request;
    TEvGetScriptExecutionOperationQueryResponse::TPtr Response;
};

class TListScriptExecutionOperationsQuery : public TQueryBase {
public:
    TListScriptExecutionOperationsQuery(const TString& database, const TString& pageToken, ui64 pageSize)
        : Database(database)
        , PageToken(pageToken)
        , PageSize(pageSize)
    {}

    static std::pair<TInstant, TString> ParsePageToken(const TString& token) {
        const size_t p = token.find('|');
        if (p == TString::npos) {
            throw std::runtime_error("Invalid page token");
        }
        const ui64 ts = FromString(TStringBuf(token).SubString(0, p));
        return {TInstant::MicroSeconds(ts), token.substr(p + 1)};
    }

    static TString MakePageToken(TInstant ts, const TString& executionId) {
        return TStringBuilder() << ts.MicroSeconds() << '|' << executionId;
    }

    void OnRunQuery() override {
        TStringBuilder sql;
        if (PageToken) {
            sql << R"(
                DECLARE $execution_id AS Text;
                DECLARE $ts AS Timestamp;
            )";
        }
        sql << R"(
            -- TListScriptExecutionOperationsQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $page_size AS Uint64;

            SELECT
                execution_id,
                start_ts,
                operation_status,
                execution_status,
                query_text,
                syntax,
                execution_mode,
                issues
            FROM `.metadata/script_executions`
            WHERE database = $database AND (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL)
        )";
        if (PageToken) {
            sql << R"(
                AND (start_ts, execution_id) <= ($ts, $execution_id)
                )";
        }
        sql << R"(
            ORDER BY start_ts DESC, execution_id DESC
            LIMIT $page_size;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$page_size")
                .Uint64(PageSize + 1)
                .Build();

        if (PageToken) {
            auto pageTokenParts = ParsePageToken(PageToken);
            params
                .AddParam("$ts")
                    .Timestamp(pageTokenParts.first)
                    .Build()
                .AddParam("$execution_id")
                    .Utf8(pageTokenParts.second)
                    .Build();
        }

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }
        NYdb::TResultSetParser result(ResultSets[0]);
        Operations.reserve(result.RowsCount());

        while (result.TryNextRow()) {
            const TMaybe<TString> executionId = result.ColumnParser("execution_id").GetOptionalUtf8();
            if (!executionId) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "NULL execution id");
                return;
            }

            const TMaybe<TInstant> creationTs = result.ColumnParser("start_ts").GetOptionalTimestamp();
            if (!creationTs) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "NULL creation ts");
                return;
            }

            if (Operations.size() >= PageSize) {
                NextPageToken = MakePageToken(*creationTs, *executionId);
                break;
            }

            const TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

            Ydb::Query::ExecuteScriptMetadata metadata;
            metadata.set_execution_id(*executionId);

            const TMaybe<i32> executionStatus = result.ColumnParser("execution_status").GetOptionalInt32();
            if (executionStatus) {
                metadata.set_exec_status(static_cast<Ydb::Query::ExecStatus>(*executionStatus));
            }

            const TMaybe<TString> sql = result.ColumnParser("query_text").GetOptionalUtf8();
            if (sql) {
                metadata.mutable_script_content()->set_text(*sql);
            }

            const TMaybe<i32> syntax = result.ColumnParser("syntax").GetOptionalInt32();
            if (syntax) {
                metadata.mutable_script_content()->set_syntax(static_cast<Ydb::Query::Syntax>(*syntax));
            }

            const TMaybe<i32> executionMode = result.ColumnParser("execution_mode").GetOptionalInt32();
            if (executionMode) {
                metadata.set_exec_mode(static_cast<Ydb::Query::ExecMode>(*executionMode));
            }

            const TMaybe<TString> issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument();
            NYql::TIssues issues;
            if (issuesSerialized) {
                issues = DeserializeIssues(*issuesSerialized);
            }

            Ydb::Operations::Operation op;
            op.set_id(ScriptExecutionOperationFromExecutionId(*executionId));
            op.set_ready(operationStatus.Defined());
            if (operationStatus) {
                op.set_status(static_cast<Ydb::StatusIds::StatusCode>(*operationStatus));
            }
            for (const NYql::TIssue& issue : issues) {
                NYql::IssueToMessage(issue, op.add_issues());
            }
            op.mutable_metadata()->PackFrom(metadata);

            Operations.emplace_back(std::move(op));
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvListScriptExecutionOperationsResponse(status, std::move(issues), NextPageToken, std::move(Operations)));
    }

private:
    const TString Database;
    const TString PageToken;
    const ui64 PageSize;
    TString NextPageToken;
    std::vector<Ydb::Operations::Operation> Operations;
};

class TListScriptExecutionOperationsActor : public TActorBootstrapped<TListScriptExecutionOperationsActor> {
public:
    TListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr ev)
        : Request(std::move(ev))
    {}

    void Bootstrap() {
        const ui64 pageSize = ClampVal<ui64>(Request->Get()->PageSize, 1, 100);
        Register(new TListScriptExecutionOperationsQuery(Request->Get()->Database, Request->Get()->PageToken, pageSize));

        Become(&TListScriptExecutionOperationsActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvListScriptExecutionOperationsResponse, Handle);
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
    )

    void Handle(TEvListScriptExecutionOperationsResponse::TPtr& ev) {
        Response = std::move(ev);

        for (ui64 i = 0; i < Response->Get()->Operations.size(); ++i) {
            const Ydb::Operations::Operation& op = Response->Get()->Operations[i];
            if (!op.ready()) {
                Ydb::Query::ExecuteScriptMetadata metadata;
                op.metadata().UnpackTo(&metadata);
                Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, metadata.execution_id(), i));
                ++OperationsToCheck;
            }
        }

        if (OperationsToCheck == 0) {
            Reply();
        }
    }

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie < Response->Get()->Operations.size());

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Response->Get()->Status = ev->Get()->Status;
            Response->Get()->Issues = std::move(ev->Get()->Issues);
            Response->Get()->NextPageToken.clear();
            Response->Get()->Operations.clear();
            Reply();
            return;
        }

        if (ev->Get()->OperationStatus) {
            Ydb::Operations::Operation& op = Response->Get()->Operations[ev->Cookie];
            op.set_status(*ev->Get()->OperationStatus);
            Ydb::Query::ExecuteScriptMetadata metadata;
            op.metadata().UnpackTo(&metadata);
            Y_ABORT_UNLESS(ev->Get()->ExecutionStatus);
            metadata.set_exec_status(*ev->Get()->ExecutionStatus);
            op.mutable_metadata()->PackFrom(metadata);
            if (ev->Get()->OperationIssues) {
                for (const NYql::TIssue& issue : *ev->Get()->OperationIssues) {
                    NYql::IssueToMessage(issue, op.add_issues());
                }
            }
        }

        --OperationsToCheck;
        if (OperationsToCheck == 0) {
            Reply();
        }
    }

    void Reply() {
        Send(Request->Sender, Response->Release().Release());
        PassAway();
    }

private:
    TEvListScriptExecutionOperations::TPtr Request;
    TEvListScriptExecutionOperationsResponse::TPtr Response;
    ui64 OperationsToCheck = 0;
};

class TCancelScriptExecutionOperationActor : public NActors::TActorBootstrapped<TCancelScriptExecutionOperationActor> {
public:
    TCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {}

    void Bootstrap() {
        const TMaybe<TString> executionId = NKqp::ScriptExecutionIdFromOperation(Request->Get()->OperationId);
        if (!executionId) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Incorrect operation id");
        }
        ExecutionId = *executionId;

        Become(&TCancelScriptExecutionOperationActor::StateFunc);
        Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, ExecutionId));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
    )

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            RunScriptActor = ev->Get()->RunScriptActorId;
            if (ev->Get()->OperationStatus) {
                Reply(Ydb::StatusIds::PRECONDITION_FAILED); // Already finished.
            } else {
                if (CancelSent) { // We have not found the actor, but after it status of the operation is not defined, something strage happened.
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, "Failed to cancel script execution operation");
                } else {
                    SendCancelToRunScriptActor(); // The race: operation is still working, but it can finish before it receives cancel signal. Try to cancel first and then maybe check its status.
                }
            }
        } else {
            Reply(ev->Get()->Status, std::move(ev->Get()->Issues)); // Error getting operation in database.
        }
    }

    void SendCancelToRunScriptActor() {
        ui64 flags = IEventHandle::FlagTrackDelivery;
        if (RunScriptActor.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = RunScriptActor.NodeId();
        }
        Send(RunScriptActor, new TEvKqp::TEvCancelScriptExecutionRequest(), flags);
        CancelSent = true;
    }

    void Handle(TEvKqp::TEvCancelScriptExecutionResponse::TPtr& ev) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
        Reply(ev->Get()->Record.GetStatus(), std::move(issues));
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) { // The actor probably had finished before our cancel message arrived.
            Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, ExecutionId)); // Check if the operation has finished.
        } else {
            Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver cancel request to destination");
        }
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver cancel request to destination");
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        Send(Request->Sender, new TEvCancelScriptExecutionOperationResponse(status, std::move(issues)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        Reply(status, std::move(issues));
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        NActors::TActorBootstrapped<TCancelScriptExecutionOperationActor>::PassAway();
    }

private:
    TEvCancelScriptExecutionOperation::TPtr Request;
    TString ExecutionId;
    NActors::TActorId RunScriptActor;
    TMaybe<ui32> SubscribedOnSession;
    bool CancelSent = false;
};

class TSaveScriptExecutionResultMetaQuery : public TQueryBase {
public:
    TSaveScriptExecutionResultMetaQuery(const TString& database, const TString& executionId, const TString& serializedMetas)
        : Database(database), ExecutionId(executionId), SerializedMetas(serializedMetas)
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptExecutionResultMetaQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_metas AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET result_set_metas = $result_set_metas
            WHERE database = $database
            AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$result_set_metas")
                .JsonDocument(SerializedMetas)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvSaveScriptResultMetaFinished(status));
        } else {
            Send(Owner, new TEvSaveScriptResultMetaFinished(status, std::move(issues)));
        }
    }

private:
    const TString Database;
    const TString ExecutionId;
    const TString SerializedMetas;
};

class TSaveScriptExecutionResultQuery : public TQueryBase {
public:
    TSaveScriptExecutionResultQuery(const TString& database, const TString& executionId, i32 resultSetId,
        TMaybe<TInstant> expireAt, i64 firstRow, i64 accumulatedSize, Ydb::ResultSet resultSet)
        : Database(database)
        , ExecutionId(executionId)
        , ResultSetId(resultSetId)
        , ExpireAt(expireAt)
        , FirstRow(firstRow)
        , AccumulatedSize(accumulatedSize)
        , ResultSet(std::move(resultSet))
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptExecutionResultQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $expire_at AS Optional<Timestamp>;
            DECLARE $items AS List<Struct<row_id:Int64,result_set:String,accumulated_size:Int64>>;

            UPSERT INTO `.metadata/result_sets`
            SELECT $database as database, $execution_id as execution_id, $result_set_id as result_set_id,
                T.row_id as row_id, $expire_at as expire_at, T.result_set as result_set, T.accumulated_size as accumulated_size
            FROM AS_TABLE($items) AS T;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$result_set_id")
                .Int32(ResultSetId)
                .Build()
            .AddParam("$expire_at")
                .OptionalTimestamp(ExpireAt)
                .Build();

        auto& param = params
            .AddParam("$items");

        param
                .BeginList();

        auto row = FirstRow;
        for (const auto& rowValue : ResultSet.rows()) {
            auto rowValueSerialized = rowValue.SerializeAsString();
            SavedSize += rowValueSerialized.size();
            param
                    .AddListItem()
                    .BeginStruct()
                        .AddMember("row_id")
                            .Int64(row++)
                        .AddMember("result_set")
                            .String(std::move(rowValueSerialized))
                        .AddMember("accumulated_size")
                            .Int64(AccumulatedSize + SavedSize)
                    .EndStruct();
        }
        param
                .EndList()
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvSaveScriptResultPartFinished(status, SavedSize));
        } else {
            Send(Owner, new TEvSaveScriptResultPartFinished(status, SavedSize, std::move(issues)));
        }
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetId;
    const TMaybe<TInstant> ExpireAt;
    const i64 FirstRow;
    const i64 AccumulatedSize;
    const Ydb::ResultSet ResultSet;
    i64 SavedSize = 0;
};

class TSaveScriptExecutionResultActor : public TActorBootstrapped<TSaveScriptExecutionResultActor> {
    static constexpr ui64 MAX_NUMBER_ROWS_IN_BATCH = 10000;
    static constexpr ui64 PROGRAM_SIZE_LIMIT = 10_MB;
    static constexpr ui64 PROGRAM_BASE_SIZE = 1_MB;  // Depends on MAX_NUMBER_ROWS_IN_BATCH

public:
    TSaveScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database,
        const TString& executionId, i32 resultSetId, TMaybe<TInstant> expireAt,
        i64 firstRow, i64 accumulatedSize, Ydb::ResultSet&& resultSet)
        : ReplyActorId(replyActorId)
        , Database(database)
        , ExecutionId(executionId)
        , ResultSetId(resultSetId)
        , ExpireAt(expireAt)
        , FirstRow(firstRow)
        , AccumulatedSize(accumulatedSize)
        , RowsSplitter(std::move(resultSet), PROGRAM_SIZE_LIMIT, PROGRAM_BASE_SIZE, MAX_NUMBER_ROWS_IN_BATCH)
    {}

    void StartSaveResultQuery() {
        if (ResultSets.empty()) {
            Reply(Ydb::StatusIds::SUCCESS);
            return;
        }

        i64 numberRows = ResultSets.back().rows_size();
        Register(new TQueryRetryActor<TSaveScriptExecutionResultQuery, TEvSaveScriptResultPartFinished, TString, TString, i32, TMaybe<TInstant>, i64, i64, Ydb::ResultSet>(SelfId(), Database, ExecutionId, ResultSetId, ExpireAt, FirstRow, AccumulatedSize, ResultSets.back()));

        FirstRow += numberRows;
        ResultSets.pop_back();
    }

    void Bootstrap() {
        NFq::TSplittedResultSets splittedResultSets = RowsSplitter.Split();
        if (!splittedResultSets.Success) {
            Reply(Ydb::StatusIds::BAD_REQUEST, std::move(splittedResultSets.Issues));
            return;
        }

        ResultSets = std::move(splittedResultSets.ResultSets);
        std::reverse(ResultSets.begin(), ResultSets.end());
        StartSaveResultQuery();

        Become(&TSaveScriptExecutionResultActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvSaveScriptResultPartFinished, Handle);
    )

    void Handle(TEvSaveScriptResultPartFinished::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Reply(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        AccumulatedSize += ev->Get()->SavedSize;

        StartSaveResultQuery();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        Send(ReplyActorId, new TEvSaveScriptResultFinished(status, std::move(issues)));
        PassAway();
    }

private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetId;
    const TMaybe<TInstant> ExpireAt;
    i64 FirstRow;
    i64 AccumulatedSize;
    NFq::TRowsProtoSplitter RowsSplitter;
    TVector<Ydb::ResultSet> ResultSets;
};

std::optional<std::pair<TDuration, TDuration>> GetTtlFromSerializedMeta(const TString& serializedMeta) {
    NKikimrKqp::TScriptExecutionOperationMeta meta;
    try {
        NProtobufJson::Json2Proto(serializedMeta, meta, NProtobufJson::TJson2ProtoConfig());
        return std::pair(GetDuration(meta.GetOperationTtl()), GetDuration(meta.GetResultsTtl()));
    } catch (const NJson::TJsonException& e) {
        return std::nullopt;
    }
}

class TGetScriptExecutionResultQueryActor : public TQueryBase {
    static constexpr i64 MAX_NUMBER_ROWS_IN_BATCH = 100000;
    static constexpr i64 MAX_BATCH_SIZE = 20_MB;

public:
    TGetScriptExecutionResultQueryActor(const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant deadline)
        : Database(database)
        , ExecutionId(executionId)
        , ResultSetIndex(resultSetIndex)
        , Offset(offset)
        , RowsLimit(rowsLimit ? rowsLimit : std::numeric_limits<i64>::max())
        , SizeLimit(sizeLimit ? sizeLimit : std::numeric_limits<i64>::max())
        , Deadline(rowsLimit ? TInstant::Max() : deadline)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TGetScriptExecutionResultQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $offset AS Int64;

            SELECT result_set_metas, operation_status, issues, end_ts, meta
            FROM `.metadata/script_executions`
            WHERE database = $database
              AND execution_id = $execution_id
              AND (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            $result_set_table = (
            SELECT row_id, accumulated_size
            FROM `.metadata/result_sets`
            WHERE database = $database
              AND execution_id = $execution_id
              AND result_set_id = $result_set_id
            );

            SELECT MAX(row_id) AS max_row_id
            FROM $result_set_table
            WHERE row_id >= $offset;

            SELECT MAX(accumulated_size) AS start_accumulated_size
            FROM $result_set_table
            WHERE row_id < $offset;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$result_set_id")
                .Int32(ResultSetIndex)
                .Build()
            .AddParam("$offset")
                .Int64(Offset)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 3) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        { // columns meta
            NYdb::TResultSetParser result(ResultSets[0]);

            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "Script execution not found");
                return;
            }

            const TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
            if (!operationStatus) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Results are not ready");
                return;
            }

            const auto serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
            if (!serializedMeta) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation metainformation");
                return;
            }

            const auto endTs = result.ColumnParser("end_ts").GetOptionalTimestamp();
            if (!endTs) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation end timestamp");
                return;
            }

            const auto ttl = GetTtlFromSerializedMeta(*serializedMeta);
            if (!ttl) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Metainformation is corrupted");
                return;
            }
            const auto [_, resultsTtl] = *ttl;
            if (resultsTtl && (*endTs + resultsTtl) < TInstant::Now()){
                Finish(Ydb::StatusIds::NOT_FOUND, "Results are expired");
                return;
            }

            Ydb::StatusIds::StatusCode operationStatusCode = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            if (operationStatusCode != Ydb::StatusIds::SUCCESS) {
                const TMaybe<TString> issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument();
                if (issuesSerialized) {
                    Finish(operationStatusCode, DeserializeIssues(*issuesSerialized));
                } else {
                    Finish(operationStatusCode, "Invalid operation");
                }
                return;
            }

            const TMaybe<TString> serializedMetas = result.ColumnParser("result_set_metas").GetOptionalJsonDocument();
            if (!serializedMetas) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result meta is empty");
                return;
            }

            NJson::TJsonValue value;
            if (!NJson::ReadJsonTree(*serializedMetas, &value) || value.GetType() != NJson::JSON_ARRAY) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result meta is corrupted");
                return;
            }

            const NJson::TJsonValue* metaValue;
            if (!value.GetValuePointer(ResultSetIndex, &metaValue)) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Result set index is invalid");
                return;
            }

            Ydb::Query::Internal::ResultSetMeta meta;
            NProtobufJson::Json2Proto(*metaValue, meta);

            *ResultSet.mutable_columns() = meta.columns();
            ResultSet.set_truncated(meta.truncated());

            if (SizeLimit) {
                const i64 resultSetSize = ResultSet.ByteSizeLong();
                if (resultSetSize > SizeLimit) {
                    Finish(Ydb::StatusIds::BAD_REQUEST, "Result set meta is larger than fetch size limit");
                    return;
                }
                SizeLimit -= resultSetSize;
            }
        }

        { // max row id
            NYdb::TResultSetParser result(ResultSets[1]);
            if (result.RowsCount() != 1) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            result.TryNextRow();

            const TMaybe<i64> maxRowId = result.ColumnParser("max_row_id").GetOptionalInt64();
            if (!maxRowId) {
                HasMoreResults = false;
                Finish();
                return;
            }
            MaxRowId = *maxRowId;
        }

        { // start accumulated size
            NYdb::TResultSetParser result(ResultSets[2]);
            if (result.RowsCount() != 1) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            result.TryNextRow();
            StartAccumulatedSize = result.ColumnParser("start_accumulated_size").GetOptionalInt64().GetOrElse(0);
        }

        ClearTimeInfo();
        FetchScriptResults();
    }

    void FetchScriptResults() {
        TString sql = R"(
            -- TGetScriptExecutionResultQuery::FetchScriptResults
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $offset AS Int64;
            DECLARE $limit AS Uint64;
            DECLARE $max_accumulated_size AS int64;

            SELECT database, execution_id, result_set_id, row_id, result_set
            FROM `.metadata/result_sets`
            WHERE database = $database
              AND execution_id = $execution_id
              AND result_set_id = $result_set_id
              AND row_id >= $offset
              AND (accumulated_size IS NULL OR accumulated_size <= $max_accumulated_size)
            ORDER BY database, execution_id, result_set_id, row_id
            LIMIT $limit;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$result_set_id")
                .Int32(ResultSetIndex)
                .Build()
            .AddParam("$offset")
                .Int64(Offset)
                .Build()
            .AddParam("$limit")
                .Uint64(std::min(RowsLimit, MAX_NUMBER_ROWS_IN_BATCH))
                .Build()
            .AddParam("$max_accumulated_size")
                .Int64(StartAccumulatedSize + std::min(SizeLimit, MAX_BATCH_SIZE))
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TGetScriptExecutionResultQueryActor::OnResultsFetched);
    }

    void OnResultsFetched() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);

        if (result.RowsCount() == 0) {
            if (ResultSet.rows_size() > 0) {
                Finish();
            } else {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Failed to fetch script result due to size limit");
            }
            return;
        }

        i64 lastRowId = 0;
        while (result.TryNextRow()) {
            const TMaybe<i64> rowId = result.ColumnParser("row_id").GetOptionalInt64();
            if (!rowId) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row id is null");
                return;
            }
            lastRowId = *rowId;

            const TMaybe<TString> serializedRow = result.ColumnParser("result_set").GetOptionalString();

            if (!serializedRow) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is null");
                return;
            }

            if (serializedRow->Empty()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is empty");
                return;
            }

            StartAccumulatedSize += serializedRow->size();
            SizeLimit -= serializedRow->size();
            if (!ResultSet.add_rows()->ParseFromString(*serializedRow)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is corrupted");
                return;
            }
        }

        if (lastRowId >= MaxRowId) {
            HasMoreResults = false;
            Finish();
            return;
        }

        Offset += result.RowsCount();
        RowsLimit -= result.RowsCount();

        if (RowsLimit <= 0 || SizeLimit <= 0 || TInstant::Now() + TDuration::Seconds(5) + GetAverageTime() >= Deadline) {
            Finish();
            return;
        }

        FetchScriptResults();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvFetchScriptResultsResponse(status, std::move(ResultSet), HasMoreResults, std::move(issues)));
        } else {
            Send(Owner, new TEvFetchScriptResultsResponse(status, std::nullopt, true, std::move(issues)));
        }
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetIndex;
    i64 Offset;
    i64 RowsLimit;
    i64 SizeLimit;
    const TInstant Deadline;

    i64 MaxRowId = 0;
    i64 StartAccumulatedSize = 0;

    Ydb::ResultSet ResultSet;
    bool HasMoreResults = true;
};

class TGetScriptExecutionResultActor : public TActorBootstrapped<TGetScriptExecutionResultActor> {
public:
    TGetScriptExecutionResultActor(const TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant operationDeadline)
        : ReplyActorId(replyActorId), Database(database), ExecutionId(executionId), ResultSetIndex(resultSetIndex), Offset(offset), RowsLimit(rowsLimit), SizeLimit(sizeLimit), OperationDeadline(operationDeadline)
    {
        Y_ENSURE(RowsLimit >= 0);
        Y_ENSURE(SizeLimit >= 0);
    }

    void Bootstrap() {
        Register(new TGetScriptExecutionResultQueryActor(Database, ExecutionId, ResultSetIndex, Offset, RowsLimit, SizeLimit, OperationDeadline));
        Become(&TGetScriptExecutionResultActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvFetchScriptResultsResponse, Handle);
    )

    void Handle(TEvFetchScriptResultsResponse::TPtr& ev) {
        Send(ev->Forward(ReplyActorId));
        PassAway();
    }

private:
    const TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetIndex;
    const i64 Offset;
    const i64 RowsLimit;
    const i64 SizeLimit;
    const TInstant OperationDeadline;
};

class TSaveScriptExternalEffectActor : public TQueryBase {
public:
    explicit TSaveScriptExternalEffectActor(const TEvSaveScriptExternalEffectRequest::TDescription& request)
        : Request(request)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptExternalEffectActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $customer_supplied_id AS Text;
            DECLARE $user_token AS Text;
            DECLARE $script_sinks AS JsonDocument;
            DECLARE $script_secret_names AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET
                customer_supplied_id = $customer_supplied_id,
                user_token = $user_token,
                script_sinks = $script_sinks,
                script_secret_names = $script_secret_names
            WHERE database = $database AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request.Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(Request.ExecutionId)
                .Build()
            .AddParam("$customer_supplied_id")
                .Utf8(Request.CustomerSuppliedId)
                .Build()
            .AddParam("$user_token")
                .Utf8(Request.UserToken)
                .Build()
            .AddParam("$script_sinks")
                .JsonDocument(SerializeSinks(Request.Sinks))
                .Build()
            .AddParam("$script_secret_names")
                .JsonDocument(SerializeSecretNames(Request.SecretNames))
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptExternalEffectResponse(status, std::move(issues)));
    }

private:
    static TString SerializeSinks(const std::vector<NKqpProto::TKqpExternalSink>& sinks) {
        NJson::TJsonValue value;
        value.SetType(NJson::EJsonValueType::JSON_ARRAY);

        NJson::TJsonValue::TArray& jsonArray = value.GetArraySafe();
        jsonArray.resize(sinks.size());
        for (size_t i = 0; i < sinks.size(); ++i) {
            NProtobufJson::Proto2Json(sinks[i], jsonArray[i], NProtobufJson::TProto2JsonConfig());
        }

        NJsonWriter::TBuf serializedSinks;
        serializedSinks.WriteJsonValue(&value);

        return serializedSinks.Str();
    }

    static TString SerializeSecretNames(const std::vector<TString>& secretNames) {
        NJson::TJsonValue value;
        value.SetType(NJson::EJsonValueType::JSON_ARRAY);

        NJson::TJsonValue::TArray& jsonArray = value.GetArraySafe();
        jsonArray.resize(secretNames.size());
        for (size_t i = 0; i < secretNames.size(); ++i) {
            jsonArray[i] = NJson::TJsonValue(secretNames[i]);
        }

        NJsonWriter::TBuf serializedSecretNames;
        serializedSecretNames.WriteJsonValue(&value);

        return serializedSecretNames.Str();
    }

private:
    TEvSaveScriptExternalEffectRequest::TDescription Request;
};

class TSaveScriptFinalStatusActor : public TQueryBase {
public:
    explicit TSaveScriptFinalStatusActor(const TEvScriptFinalizeRequest::TDescription& request)
        : Request(request)
    {
        Response = std::make_unique<TEvSaveScriptFinalStatusResponse>();
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptFinalStatusActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                finalization_status,
                meta,
                customer_supplied_id,
                user_token,
                script_sinks,
                script_secret_names
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT lease_generation
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request.Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(Request.ExecutionId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginTx());
        SetQueryResultHandler(&TSaveScriptFinalStatusActor::OnGetInfo);
    }

    void OnGetInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
            return;
        }

        result.TryNextRow();

        TMaybe<i32> finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
        if (finalizationStatus) {
            if (Request.FinalizationStatus != *finalizationStatus) {
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Execution already have different finalization status");
                return;
            }
            Response->ApplicateScriptExternalEffectRequired = true;
        }

        TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

        if (Request.LeaseGeneration && !operationStatus) {
            NYdb::TResultSetParser leaseResult(ResultSets[1]);
            if (leaseResult.RowsCount() == 0) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state");
                return;
            }

            leaseResult.TryNextRow();

            TMaybe<i64> leaseGenerationInDatabase = leaseResult.ColumnParser("lease_generation").GetOptionalInt64();
            if (!leaseGenerationInDatabase) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
                return;
            }

            if (*Request.LeaseGeneration != static_cast<ui64>(*leaseGenerationInDatabase)) {
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Lease was lost");
                return;
            }
        }

        TMaybe<TString> customerSuppliedId = result.ColumnParser("customer_supplied_id").GetOptionalUtf8();
        if (customerSuppliedId) {
            Response->CustomerSuppliedId = *customerSuppliedId;
        }

        TMaybe<TString> userToken = result.ColumnParser("user_token").GetOptionalUtf8();
        if (userToken) {
            Response->UserToken = *userToken;
        }

        SerializedSinks = result.ColumnParser("script_sinks").GetOptionalJsonDocument();
        if (SerializedSinks) {
            NJson::TJsonValue value;
            if (!NJson::ReadJsonTree(*SerializedSinks, &value) || value.GetType() != NJson::JSON_ARRAY) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script sinks are corrupted");
                return;
            }

            for (auto i = 0; i < value.GetIntegerRobust(); i++) {
                const NJson::TJsonValue* serializedSink;
                value.GetValuePointer(i, &serializedSink);

                NKqpProto::TKqpExternalSink sink;
                NProtobufJson::Json2Proto(*serializedSink, sink);
                Response->Sinks.push_back(sink);
            }
        }

        SerializedSecretNames = result.ColumnParser("script_secret_names").GetOptionalJsonDocument();
        if (SerializedSecretNames) {
            NJson::TJsonValue value;
            if (!NJson::ReadJsonTree(*SerializedSecretNames, &value) || value.GetType() != NJson::JSON_ARRAY) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script secret names are corrupted");
                return;
            }

            for (auto i = 0; i < value.GetIntegerRobust(); i++) {
                const NJson::TJsonValue* serializedSecretName;
                value.GetValuePointer(i, &serializedSecretName);

                Response->SecretNames.push_back(serializedSecretName->GetString());
            }
        }

        const auto serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
        if (!serializedMeta) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation meta information");
            return;
        }

        const auto ttl = GetTtlFromSerializedMeta(*serializedMeta);
        if (!ttl) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Metainformation is corrupted");
            return;
        }
        OperationTtl = ttl->first;

        if (operationStatus) {
            FinalStatusAlreadySaved = true;
            Response->OperationAlreadyFinalized = !finalizationStatus;
            CommitTransaction();
            return;
        }

        Response->ApplicateScriptExternalEffectRequired = Response->ApplicateScriptExternalEffectRequired || HasExternalEffect();
        FinishScriptExecution();
    }

    void FinishScriptExecution() {
        TString sql = R"(
            -- TSaveScriptFinalStatusActor::FinishScriptExecution
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
            DECLARE $finalization_status AS Int32;
            DECLARE $issues AS JsonDocument;
            DECLARE $plan AS JsonDocument;
            DECLARE $stats AS JsonDocument;
            DECLARE $ast AS Optional<Text>;
            DECLARE $ast_compressed AS Optional<String>;
            DECLARE $ast_compression_method AS Optional<Text>;
            DECLARE $operation_ttl AS Interval;
            DECLARE $customer_supplied_id AS Text;
            DECLARE $user_token AS Text;
            DECLARE $script_sinks AS Optional<JsonDocument>;
            DECLARE $script_secret_names AS Optional<JsonDocument>;
            DECLARE $applicate_script_external_effect_required AS Bool;

            UPDATE `.metadata/script_executions`
            SET
                operation_status = $operation_status,
                execution_status = $execution_status,
                finalization_status = IF($applicate_script_external_effect_required, $finalization_status, NULL),
                issues = $issues,
                plan = $plan,
                end_ts = CurrentUtcTimestamp(),
                stats = $stats,
                ast = $ast,
                ast_compressed = $ast_compressed,
                ast_compression_method = $ast_compression_method,
                expire_at = IF($operation_ttl > CAST(0 AS Interval), CurrentUtcTimestamp() + $operation_ttl, NULL),
                customer_supplied_id = IF($applicate_script_external_effect_required, $customer_supplied_id, NULL),
                user_token = IF($applicate_script_external_effect_required, $user_token, NULL),
                script_sinks = IF($applicate_script_external_effect_required, $script_sinks, NULL),
                script_secret_names = IF($applicate_script_external_effect_required, $script_secret_names, NULL)
            WHERE database = $database AND execution_id = $execution_id;

            DELETE FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;
        )";

        TString serializedStats = "{}";
        if (Request.QueryStats) {
            NJson::TJsonValue statsJson;
            Ydb::TableStats::QueryStats queryStats;
            NGRpcService::FillQueryStats(queryStats, *Request.QueryStats);
            NProtobufJson::Proto2Json(queryStats, statsJson, NProtobufJson::TProto2JsonConfig());
            serializedStats = NJson::WriteJson(statsJson);
        }

        TMaybe<TString> ast;
        TMaybe<TString> astCompressed;
        TMaybe<TString> astCompressionMethod;
        if (Request.QueryAst && Request.QueryAstCompressionMethod) {
            astCompressed = *Request.QueryAst;
            astCompressionMethod = *Request.QueryAstCompressionMethod;
        } else {
            ast = Request.QueryAst.value_or("");
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request.Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(Request.ExecutionId)
                .Build()
            .AddParam("$operation_status")
                .Int32(Request.OperationStatus)
                .Build()
            .AddParam("$execution_status")
                .Int32(Request.ExecStatus)
                .Build()
            .AddParam("$finalization_status")
                .Int32(Request.FinalizationStatus)
                .Build()
            .AddParam("$issues")
                .JsonDocument(SerializeIssues(Request.Issues))
                .Build()
            .AddParam("$plan")
                .JsonDocument(Request.QueryPlan.value_or("{}"))
                .Build()
            .AddParam("$stats")
                .JsonDocument(serializedStats)
                .Build()
            .AddParam("$ast")
                .OptionalUtf8(ast)
                .Build()
            .AddParam("$ast_compressed")
                .OptionalString(astCompressed)
                .Build()
            .AddParam("$ast_compression_method")
                .OptionalUtf8(astCompressionMethod)
                .Build()
            .AddParam("$operation_ttl")
                .Interval(static_cast<i64>(OperationTtl.MicroSeconds()))
                .Build()
            .AddParam("$customer_supplied_id")
                .Utf8(Response->CustomerSuppliedId)
                .Build()
            .AddParam("$user_token")
                .Utf8(Response->UserToken)
                .Build()
            .AddParam("$script_sinks")
                .OptionalJsonDocument(SerializedSinks)
                .Build()
            .AddParam("$script_secret_names")
                .OptionalJsonDocument(SerializedSecretNames)
                .Build()
            .AddParam("$applicate_script_external_effect_required")
                .Bool(Response->ApplicateScriptExternalEffectRequired)
                .Build();

        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
        SetQueryResultHandler(&TSaveScriptFinalStatusActor::OnQueryResult);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (!FinalStatusAlreadySaved) {
            KQP_PROXY_LOG_D("Finish script execution operation. ExecutionId: " << Request.ExecutionId
                << ". " << Ydb::StatusIds::StatusCode_Name(Request.OperationStatus)
                << ". Issues: " << Request.Issues.ToOneLineString() << ". Plan: " << Request.QueryPlan.value_or(""));
        }

        Response->Status = status;
        Response->Issues = std::move(issues);

        Send(Owner, Response.release());
    }

private:
    bool HasExternalEffect() const {
        return !Response->Sinks.empty();
    }

private:
    TEvScriptFinalizeRequest::TDescription Request;
    std::unique_ptr<TEvSaveScriptFinalStatusResponse> Response;

    bool FinalStatusAlreadySaved = false;

    TDuration OperationTtl;
    TMaybe<TString> SerializedSinks;
    TMaybe<TString> SerializedSecretNames;
};

class TScriptFinalizationFinisherActor : public TQueryBase {
public:
    TScriptFinalizationFinisherActor(const TString& executionId, const TString& database, std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues)
        : ExecutionId(executionId)
        , Database(database)
        , OperationStatus(operationStatus)
        , OperationIssues(std::move(operationIssues))
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptFinalizationFinisherActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT finalization_status
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginTx());
        SetQueryResultHandler(&TScriptFinalizationFinisherActor::OnGetInfo);
    }

    void OnGetInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
            return;
        }

        result.TryNextRow();

        TMaybe<i32> finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
        if (!finalizationStatus) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Already finished");
            return;
        }

        if (OperationStatus) {
            UpdateOperationFinalStatus();
        } else {
            UpdateOnlyFinalizationStatus();
        }
    }

    void UpdateOperationFinalStatus() {
        TString sql = R"(
            -- TScriptFinalizationFinisherActor::UpdateOperationFinalStatus
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
            DECLARE $issues AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET
                operation_status = $operation_status,
                execution_status = $execution_status,
                finalization_status = NULL,
                issues = $issues,
                customer_supplied_id = NULL,
                user_token = NULL,
                script_sinks = NULL,
                script_secret_names = NULL
            WHERE database = $database AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$operation_status")
                .Int32(*OperationStatus)
                .Build()
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_FAILED)
                .Build()
            .AddParam("$issues")
                .JsonDocument(SerializeIssues(OperationIssues))
                .Build();

        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
        SetQueryResultHandler(&TScriptFinalizationFinisherActor::OnQueryResult);
    }

    void UpdateOnlyFinalizationStatus() {
        TString sql = R"(
            -- TScriptFinalizationFinisherActor::UpdateOnlyFinalizationStatus
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            UPDATE `.metadata/script_executions`
            SET
                finalization_status = NULL,
                customer_supplied_id = NULL,
                user_token = NULL,
                script_sinks = NULL,
                script_secret_names = NULL
            WHERE database = $database AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$database")
                .Utf8(Database)
                .Build();

        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
        SetQueryResultHandler(&TScriptFinalizationFinisherActor::OnQueryResult);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (OperationStatus) {
            OperationIssues.AddIssues(std::move(issues));
            Send(Owner, new TEvScriptExecutionFinished(status != Ydb::StatusIds::SUCCESS, *OperationStatus, std::move(OperationIssues)));
        } else {
            Send(Owner, new TEvScriptExecutionFinished(status != Ydb::StatusIds::SUCCESS, Ydb::StatusIds::SUCCESS, std::move(issues)));
        }
    }

private:
    TString ExecutionId;
    TString Database;
    std::optional<Ydb::StatusIds::StatusCode> OperationStatus;
    NYql::TIssues OperationIssues;
};

class TScriptProgressActor : public TQueryBase {
public:
    TScriptProgressActor(const TString& database, const TString& executionId, const TString& queryPlan, const TString&)
    : Database(database), ExecutionId(executionId), QueryPlan(queryPlan)
    {
        KQP_PROXY_LOG_D(queryPlan);
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptProgressActor::OnRunQuery
            DECLARE $execution_id AS Text;
            DECLARE $database AS Text;
            DECLARE $plan AS JsonDocument;

            UPSERT INTO `.metadata/script_executions` (execution_id, database, plan)
            VALUES ($execution_id, $database, $plan);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$plan")
                .JsonDocument(QueryPlan)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode, NYql::TIssues&&) override {
    }

private:
    TString Database;
    TString ExecutionId;
    TString QueryPlan;
};


} // anonymous namespace

NActors::IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TDuration maxRunTime) {
    return new TCreateScriptExecutionActor(std::move(ev), queryServiceConfig, counters, maxRunTime);
}

NActors::IActor* CreateScriptExecutionsTablesCreator(THolder<NActors::IEventBase> resultEvent) {
    return new TScriptExecutionsTablesCreator(std::move(resultEvent));
}

NActors::IActor* CreateForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr ev) {
    return new TForgetScriptExecutionOperationActor(std::move(ev));
}

NActors::IActor* CreateGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev) {
    return new TGetScriptExecutionOperationActor(std::move(ev));
}

NActors::IActor* CreateListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr ev) {
    return new TListScriptExecutionOperationsActor(std::move(ev));
}

NActors::IActor* CreateCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr ev) {
    return new TCancelScriptExecutionOperationActor(std::move(ev));
}

NActors::IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration, TIntrusivePtr<TKqpCounters> counters) {
    return new TScriptLeaseUpdateActor(runScriptActorId, database, executionId, leaseDuration, counters);
}

NActors::IActor* CreateSaveScriptExecutionResultMetaActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, const TString& serializedMeta) {
    return new TQueryRetryActor<TSaveScriptExecutionResultMetaQuery, TEvSaveScriptResultMetaFinished, TString, TString, TString>(runScriptActorId, database, executionId, serializedMeta);
}

NActors::IActor* CreateSaveScriptExecutionResultActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, i32 resultSetId, TMaybe<TInstant> expireAt, i64 firstRow, i64 accumulatedSize, Ydb::ResultSet&& resultSet) {
    return new TSaveScriptExecutionResultActor(runScriptActorId, database, executionId, resultSetId, expireAt, firstRow, accumulatedSize, std::move(resultSet));
}

NActors::IActor* CreateGetScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant operationDeadline) {
    return new TGetScriptExecutionResultActor(replyActorId, database, executionId, resultSetIndex, offset, rowsLimit, sizeLimit, operationDeadline);
}

NActors::IActor* CreateSaveScriptExternalEffectActor(TEvSaveScriptExternalEffectRequest::TPtr ev) {
    return new TQueryRetryActor<TSaveScriptExternalEffectActor, TEvSaveScriptExternalEffectResponse, TEvSaveScriptExternalEffectRequest::TDescription>(ev->Sender, ev->Get()->Description);
}

NActors::IActor* CreateSaveScriptFinalStatusActor(const NActors::TActorId& finalizationActorId, TEvScriptFinalizeRequest::TPtr ev) {
    return new TQueryRetryActor<TSaveScriptFinalStatusActor, TEvSaveScriptFinalStatusResponse, TEvScriptFinalizeRequest::TDescription>(finalizationActorId, ev->Get()->Description);
}

NActors::IActor* CreateScriptFinalizationFinisherActor(const NActors::TActorId& finalizationActorId, const TString& executionId, const TString& database, std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues) {
    return new TQueryRetryActor<TScriptFinalizationFinisherActor, TEvScriptExecutionFinished, TString, TString, std::optional<Ydb::StatusIds::StatusCode>, NYql::TIssues>(finalizationActorId, executionId, database, operationStatus, operationIssues);
}

NActors::IActor* CreateScriptProgressActor(const TString& executionId, const TString& database, const TString& queryPlan, const TString& queryStats) {
    return new TScriptProgressActor(database, executionId, queryPlan, queryStats);
}

namespace NPrivate {

NActors::IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record, TDuration operationTtl, TDuration resultsTtl,  TDuration leaseDuration) {
    return new TCreateScriptOperationQuery(executionId, runScriptActorId, record, operationTtl, resultsTtl, leaseDuration);
}

NActors::IActor* CreateCheckLeaseStatusActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, ui64 cookie) {
    return new TCheckLeaseStatusActor(replyActorId, database, executionId, cookie);
}

} // namespace NPrivate

} // namespace NKikimr::NKqp
