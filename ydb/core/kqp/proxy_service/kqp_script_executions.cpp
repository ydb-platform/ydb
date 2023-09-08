#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"
#include "kqp_table_creator.h"

#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/proto/result_set_meta.pb.h>
#include <ydb/core/kqp/run_script_actor/kqp_run_script_actor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>
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
                    Col("execution_mode", NScheme::NTypeIds::Int32),
                    Col("start_ts", NScheme::NTypeIds::Timestamp),
                    Col("end_ts", NScheme::NTypeIds::Timestamp),
                    Col("query_text", NScheme::NTypeIds::Text),
                    Col("syntax", NScheme::NTypeIds::Int32),
                    Col("ast", NScheme::NTypeIds::Text),
                    Col("issues", NScheme::NTypeIds::JsonDocument),
                    Col("plan", NScheme::NTypeIds::JsonDocument),
                    Col("meta", NScheme::NTypeIds::JsonDocument),
                    Col("parameters", NScheme::NTypeIds::String), // TODO: store aparameters separately to support bigger storage.
                    Col("result_set_metas", NScheme::NTypeIds::JsonDocument),
                    Col("stats", NScheme::NTypeIds::JsonDocument),
                    Col("expire_at", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                },
                { "database", "execution_id" },
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
                },
                { "database", "execution_id" }
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
                },
                { "database", "execution_id", "result_set_id", "row_id" },
                TtlCol("expire_at")
            )
        );
    }

    void Handle(TEvPrivate::TEvCreateTableResponse::TPtr&) {
        Y_VERIFY(TablesCreating > 0);
        if (--TablesCreating == 0) {
            Send(Owner, std::move(ResultEvent));
            PassAway();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCreateTableResponse, Handle);
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
            DECLARE $max_run_time AS Interval;

            UPSERT INTO `.metadata/script_executions`
                (database, execution_id, run_script_actor_id, execution_status, execution_mode, start_ts, query_text, syntax, meta, expire_at)
            VALUES ($database, $execution_id, $run_script_actor_id, $execution_status, $execution_mode, CurrentUtcTimestamp(), $query_text, $syntax, $meta, CurrentUtcTimestamp() + $max_run_time);

            UPSERT INTO `.metadata/script_execution_leases`
                (database, execution_id, lease_deadline, lease_generation)
            VALUES ($database, $execution_id, CurrentUtcTimestamp() + $lease_duration, 1);
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
            .AddParam("$max_run_time")
                .Interval(static_cast<i64>(MaxRunTime.MicroSeconds()))
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
        RunScriptActorId = Register(CreateRunScriptActor(ExecutionId, Event->Get()->Record, Event->Get()->Record.GetRequest().GetDatabase(), 1, LeaseDuration, QueryServiceConfig, Counters));
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
    using IRetryPolicy = IRetryPolicy<const Ydb::StatusIds::StatusCode&>;

    TScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration, TIntrusivePtr<TKqpCounters> counters)
        : RunScriptActorId(runScriptActorId)
        , Database(database)
        , ExecutionId(executionId)
        , LeaseDuration(leaseDuration)
        , Counters(counters)
        , LeaseUpdateStartTime(TInstant::Now())
    {}

    void CreateScriptLeaseUpdater() {
        Register(new TScriptLeaseUpdater(Database, ExecutionId, LeaseDuration));
    }

    void Bootstrap() {
        CreateScriptLeaseUpdater();
        Become(&TScriptLeaseUpdateActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvScriptLeaseUpdateResponse, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Wakeup);
    )

    void Wakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        CreateScriptLeaseUpdater();
    }

    void Handle(TEvScriptLeaseUpdateResponse::TPtr& ev) {
        auto queryStatus = ev->Get()->Status;
        if (!ev->Get()->ExecutionEntryExists && queryStatus == Ydb::StatusIds::BAD_REQUEST || queryStatus == Ydb::StatusIds::SUCCESS) {
            Reply(std::move(ev));
            return;
        }

        if (RetryState == nullptr) {
            CreateRetryState();
        }

        const TMaybe<TDuration> delay = RetryState->GetNextRetryDelay(queryStatus);
        if (delay) {
            Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            Reply(std::move(ev));
        }
    }

    void Reply(TEvScriptLeaseUpdateResponse::TPtr&& ev) {
        if (Counters) {
            Counters->ReportLeaseUpdateLatency(TInstant::Now() - LeaseUpdateStartTime);
        }
        Send(RunScriptActorId, ev->Release().Release());
        PassAway();
    }

    static ERetryErrorClass Retryable(const Ydb::StatusIds::StatusCode& status) {
        if (status == Ydb::StatusIds::SUCCESS) {
            return ERetryErrorClass::NoRetry;
        }

        if (status == Ydb::StatusIds::INTERNAL_ERROR
            || status == Ydb::StatusIds::UNAVAILABLE
            || status == Ydb::StatusIds::TIMEOUT
            || status == Ydb::StatusIds::BAD_SESSION
            || status == Ydb::StatusIds::SESSION_EXPIRED
            || status == Ydb::StatusIds::SESSION_BUSY
            || status == Ydb::StatusIds::ABORTED) {
            return ERetryErrorClass::ShortRetry;
        }

        if (status == Ydb::StatusIds::OVERLOADED) {
            return ERetryErrorClass::LongRetry;
        }

        return ERetryErrorClass::NoRetry;
    }

    void CreateRetryState() {
        IRetryPolicy::TPtr policy = IRetryPolicy::GetExponentialBackoffPolicy(Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), LeaseDuration / 2);
        RetryState = policy->CreateRetryState();
    }

private:
    TActorId RunScriptActorId;
    TString Database;
    TString ExecutionId;
    TDuration LeaseDuration;
    TIntrusivePtr<TKqpCounters> Counters;
    TInstant LeaseUpdateStartTime;
    IRetryPolicy::IRetryState::TPtr RetryState = nullptr;
};


class TScriptExecutionFinisherBase : public TQueryBase {
public:
    using TQueryBase::TQueryBase;

    void FinishScriptExecution(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus,
                               TDuration operationTtl, TDuration resultsTtl, const NYql::TIssues& issues = LeaseExpiredIssues(), TTxControl txControl = TTxControl::ContinueAndCommitTx(),
                               TMaybe<NKqpProto::TKqpStatsQuery> kqpStats = Nothing(), TMaybe<TString> queryPlan = Nothing(), TMaybe<TString> queryAst = Nothing()) {
        TString sql = R"(
            -- TScriptExecutionFinisherBase::FinishScriptExecution
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
            DECLARE $issues AS JsonDocument;
            DECLARE $plan AS JsonDocument;
            DECLARE $stats AS JsonDocument;
            DECLARE $ast AS Text;
            DECLARE $operation_ttl AS Interval;
            DECLARE $results_ttl AS Interval;

            UPDATE `.metadata/script_executions`
            SET
                operation_status = $operation_status,
                execution_status = $execution_status,
                issues = $issues,
                plan = $plan,
                end_ts = CurrentUtcTimestamp(),
                stats = $stats,
                ast = $ast,
                expire_at = IF($operation_ttl > CAST(0 AS Interval), CurrentUtcTimestamp() + $operation_ttl, NULL)
            WHERE database = $database AND execution_id = $execution_id;

            DELETE FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;

            UPDATE `.metadata/result_sets`
            SET
                expire_at = IF($results_ttl > CAST(0 AS Interval), CurrentUtcTimestamp() + $results_ttl, NULL)
            where database = $database AND execution_id = $execution_id;
        )";

        TString serializedStats = "{}";
        if (kqpStats) {
            NJson::TJsonValue statsJson;
            Ydb::TableStats::QueryStats queryStats;
            NGRpcService::FillQueryStats(queryStats, *kqpStats);
            NProtobufJson::Proto2Json(queryStats, statsJson, NProtobufJson::TProto2JsonConfig());
            serializedStats = NJson::WriteJson(statsJson);
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(executionId)
                .Build()
            .AddParam("$operation_status")
                .Int32(operationStatus)
                .Build()
            .AddParam("$execution_status")
                .Int32(execStatus)
                .Build()
            .AddParam("$issues")
                .JsonDocument(SerializeIssues(issues))
                .Build()
            .AddParam("$plan")
                .JsonDocument(queryPlan.GetOrElse("{}"))
                .Build()
            .AddParam("$stats")
                .JsonDocument(serializedStats)
                .Build()
            .AddParam("$ast")
                .Utf8(queryAst.GetOrElse(""))
                .Build()
            .AddParam("$operation_ttl")
                .Interval(static_cast<i64>(operationTtl.MicroSeconds()))
                .Build()
            .AddParam("$results_ttl")
                .Interval(static_cast<i64>(resultsTtl.MicroSeconds()))
                .Build();

        RunDataQuery(sql, &params, txControl);
    }

    void FinishScriptExecution(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus,
                               TDuration operationTtl, TDuration resultsTtl, const TString& message, TTxControl txControl = TTxControl::ContinueAndCommitTx(),
                               TMaybe<NKqpProto::TKqpStatsQuery> kqpStats = Nothing(), TMaybe<TString> queryPlan = Nothing(), TMaybe<TString> queryAst = Nothing()) {
        FinishScriptExecution(database, executionId, operationStatus, execStatus, operationTtl, resultsTtl, IssuesFromMessage(message), txControl, kqpStats, queryPlan, queryAst);
    }

    static NYql::TIssues IssuesFromMessage(const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        return issues;
    }

    static NYql::TIssues LeaseExpiredIssues() {
        return IssuesFromMessage("Lease expired");
    }
};

TMaybe<std::pair<TDuration, TDuration>> GetTtlFromSerializedMeta(const TString& serializedMeta) {
    NKikimrKqp::TScriptExecutionOperationMeta meta;
    try {
        NProtobufJson::Json2Proto(serializedMeta, meta, NProtobufJson::TJson2ProtoConfig());
        return std::pair(GetDuration(meta.GetOperationTtl()), GetDuration(meta.GetResultsTtl()));
    } catch (NJson::TJsonException &e) {
        return Nothing();
    }
}

class TScriptExecutionFinisher : public TScriptExecutionFinisherBase {
public:
    TScriptExecutionFinisher(
        const TString& executionId,
        const TString& database,
        ui64 leaseGeneration,
        Ydb::StatusIds::StatusCode operationStatus,
        Ydb::Query::ExecStatus execStatus,
        NYql::TIssues issues,
        TMaybe<NKqpProto::TKqpStatsQuery> queryStats = Nothing(),
        TMaybe<TString> queryPlan = Nothing(),
        TMaybe<TString> queryAst = Nothing()
    )
        : Database(database)
        , ExecutionId(executionId)
        , LeaseGeneration(leaseGeneration)
        , OperationStatus(operationStatus)
        , ExecStatus(execStatus)
        , Issues(std::move(issues))
        , QueryStats(std::move(queryStats))
        , QueryPlan(std::move(queryPlan))
        , QueryAst(std::move(queryAst))
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptExecutionFinisher::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT lease_generation FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;

            SELECT meta FROM `.metadata/script_executions`
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

        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnQueryResult() override {
        if (!FinishWasRun) {
            if (ResultSets.size() != 2) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }
            NYdb::TResultSetParser result(ResultSets[0]);
            if (result.RowsCount() == 0) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "No such execution");
                return;
            }

            result.TryNextRow();

            const TMaybe<i64> leaseGenerationInDatabase = result.ColumnParser(0).GetOptionalInt64();
            if (!leaseGenerationInDatabase) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
                return;
            }

            if (LeaseGeneration != static_cast<ui64>(*leaseGenerationInDatabase)) {
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Lease was lost");
                return;
            }

            NYdb::TResultSetParser metaResult(ResultSets[1]);
            metaResult.TryNextRow();

            const auto serializedMeta = metaResult.ColumnParser("meta").GetOptionalJsonDocument();
            if (!serializedMeta) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing opeartion metainformation");
                return;
            }

            const auto ttl = GetTtlFromSerializedMeta(*serializedMeta);
            if (!ttl) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Metainformation is corrupted");
                return;
            }

            const auto [operationTtl, resultsTtl] = *ttl;
            FinishScriptExecution(Database, ExecutionId, OperationStatus, ExecStatus, operationTtl, resultsTtl,
                                  Issues, TTxControl::ContinueAndCommitTx(), std::move(QueryStats), std::move(QueryPlan), std::move(QueryAst));
            FinishWasRun = true;
        } else {
            Finish();
        }
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Finish script execution operation. ExecutionId: " << ExecutionId << ". Lease generation: " <<
                        LeaseGeneration << ": " << Ydb::StatusIds::StatusCode_Name(status) << ". Issues: " << issues.ToOneLineString() << ". Plan: " << QueryPlan);
        Send(Owner, new TEvScriptExecutionFinished(status, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const ui64 LeaseGeneration;
    const Ydb::StatusIds::StatusCode OperationStatus;
    const Ydb::Query::ExecStatus ExecStatus;
    const NYql::TIssues Issues;
    const TMaybe<NKqpProto::TKqpStatsQuery> QueryStats;
    const TMaybe<TString> QueryPlan;
    const TMaybe<TString> QueryAst;
    bool FinishWasRun = false;
};

class TCheckLeaseStatusActor : public TScriptExecutionFinisherBase {
public:
    TCheckLeaseStatusActor(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode statusOnExpiredLease = Ydb::StatusIds::ABORTED, ui64 cookie = 0)
        : Database(database)
        , ExecutionId(executionId)
        , StatusOnExpiredLease(statusOnExpiredLease)
        , Cookie(cookie)
    {}

    void OnRunQuery() override {
        const TString sql = R"(
            -- TCheckLeaseStatusActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT operation_status, execution_status, issues, run_script_actor_id, meta FROM `.metadata/script_executions`
                WHERE database = $database AND execution_id = $execution_id AND
                      (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT lease_deadline FROM `.metadata/script_execution_leases`
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

        RunDataQuery(sql, &params, TTxControl::BeginTx());
        SetQueryResultHandler(&TCheckLeaseStatusActor::OnResult);
    }

    void OnResult() {
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
                auto serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
                if (!serializedMeta) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing opeartion metainformation");
                    return;
                }
                const auto ttl = GetTtlFromSerializedMeta(*serializedMeta);
                if (!ttl) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Metainformation is corrupted");
                    return;
                }
                const auto [operationTtl, resultsTtl] = *ttl;
                FinishScriptExecution(Database, ExecutionId, StatusOnExpiredLease, Ydb::Query::EXEC_STATUS_ABORTED, operationTtl, resultsTtl);
                SetQueryResultHandler(&TCheckLeaseStatusActor::OnFinishScriptExecution);
            } else {
                // OperationStatus is Nothing(): currently running
                CommitTransaction();
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
            CommitTransaction();
        } else {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state");
        }
    }

    void OnFinishScriptExecution() {
        OperationStatus = StatusOnExpiredLease;
        ExecutionStatus = Ydb::Query::EXEC_STATUS_ABORTED;
        OperationIssues = LeaseExpiredIssues();
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvLeaseCheckResult(OperationStatus, ExecutionStatus, std::move(OperationIssues), RunScriptActorId), 0, Cookie);
        } else {
            Send(Owner, new TEvPrivate::TEvLeaseCheckResult(status, std::move(issues)), 0, Cookie);
        }
    }

private:
    const TInstant RunStartTime = TInstant::Now();
    const TString Database;
    const TString ExecutionId;
    const Ydb::StatusIds::StatusCode StatusOnExpiredLease;
    const ui64 Cookie;
    TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
    TMaybe<Ydb::Query::ExecStatus> ExecutionStatus;
    TMaybe<NYql::TIssues> OperationIssues;
    NActors::TActorId RunScriptActorId;
};

class TForgetScriptExecutionOperationActor : public TQueryBase {
public:
    explicit TForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TForgetScriptExecutionOperationActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_deadline
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;
        )";

        TMaybe<TString> maybeExecutionId = ScriptExecutionIdFromOperation(Request->Get()->OperationId);
        Y_ENSURE(maybeExecutionId, "No execution id specified");
        ExecutionId = *maybeExecutionId;
        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request->Get()->Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginTx());
        SetQueryResultHandler(&TForgetScriptExecutionOperationActor::OnGetInfo);
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

        const TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

        TStringBuilder sql;
        sql << R"(
            -- TForgetScriptExecutionOperationActor::OnGetInfo
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            DELETE
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;

            DELETE
            FROM `.metadata/result_sets`
            WHERE database = $database AND execution_id = $execution_id;

        )";

        NYdb::TResultSetParser deadlineResult(ResultSets[1]);
        if (deadlineResult.RowsCount() != 0) {
            deadlineResult.TryNextRow();
            TMaybe<TInstant> leaseDeadline = deadlineResult.ColumnParser(0).GetOptionalTimestamp();
            if (!leaseDeadline) {
                // existing row with empty lease???
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state");
                return;
            }
            if (*leaseDeadline >= TInstant::Now()) {
                if (!operationStatus) {
                    Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Operation is still running");
                    return;
                }
            }
            sql << R"(
                DELETE
                FROM `.metadata/script_execution_leases`
                WHERE database = $database AND execution_id = $execution_id;

            )";
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request->Get()->Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
        SetQueryResultHandler(&TForgetScriptExecutionOperationActor::OnForgetOperation);
    }

    void OnForgetOperation() {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Request->Sender, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)));
    }

private:
    TEvForgetScriptExecutionOperation::TPtr Request;
    TString ExecutionId;
    NYql::TIssues Issues;
};

class TGetScriptExecutionOperationQueryActor : public TScriptExecutionFinisherBase {
public:
    TGetScriptExecutionOperationQueryActor(const TString& database, const NOperationId::TOperationId& operationId, bool finishIfLeaseExpired)
        : Database(database)
        , OperationId(operationId)
        , FinishIfLeaseExpired(finishIfLeaseExpired)
        , StartActorTime(TInstant::Now())
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TGetScriptExecutionOperationActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                run_script_actor_id,
                operation_status,
                execution_status,
                query_text,
                syntax,
                execution_mode,
                result_set_metas,
                plan,
                issues,
                stats,
                ast,
                meta
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_deadline
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;
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

        RunDataQuery(sql, &params, TTxControl::BeginTx());
        SetQueryResultHandler(&TGetScriptExecutionOperationQueryActor::OnGetInfo);
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

        const TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
        if (operationStatus) {
            OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
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

        const TMaybe<TString> ast = result.ColumnParser("ast").GetOptionalUtf8();
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

            auto serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
            if (!serializedMeta) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation metainformation");
                return;
            }

            const auto ttl = GetTtlFromSerializedMeta(*serializedMeta);
            if (!ttl) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Metainformation is corrupted");
                return;
            }

            const auto [operationTtl, resultsTtl] = *ttl;
            deadlineResult.TryNextRow();

            TMaybe<TInstant> leaseDeadline = deadlineResult.ColumnParser(0).GetOptionalTimestamp();
            if (!leaseDeadline) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state");
                return;
            }

            LeaseExpired = *leaseDeadline < StartActorTime;
            if (LeaseExpired && FinishIfLeaseExpired) {
                FinishScriptExecution(Database, Metadata.execution_id(), Ydb::StatusIds::ABORTED, Ydb::Query::EXEC_STATUS_ABORTED, operationTtl, resultsTtl, Issues);
                SetQueryResultHandler(&TGetScriptExecutionOperationQueryActor::OnFinishOperation);
            }
        }

        if (!LeaseExpired || !FinishIfLeaseExpired) {
            CommitTransaction();
        }
    }

    void OnFinishOperation() {
        OperationStatus = Ydb::StatusIds::ABORTED;
        Issues = LeaseExpiredIssues();
        Metadata.set_exec_status(Ydb::Query::EXEC_STATUS_ABORTED);

        Finish(Ydb::StatusIds::SUCCESS, std::move(Issues));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (OperationStatus) {
            TMaybe<google::protobuf::Any> metadata;
            metadata.ConstructInPlace().PackFrom(Metadata);
            Send(Owner, new TEvGetScriptExecutionOperationResponse(true, LeaseExpired, RunScriptActorId, *OperationStatus, std::move(Issues), std::move(metadata)));
        } else {
            Send(Owner, new TEvGetScriptExecutionOperationResponse(false, LeaseExpired, RunScriptActorId, status, std::move(issues), Nothing()));
        }
    }

private:
    TString Database;
    NOperationId::TOperationId OperationId;
    bool FinishIfLeaseExpired;
    TInstant StartActorTime;
    TString ExecutionId;
    TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
    bool LeaseExpired = false;
    TActorId RunScriptActorId;
    NYql::TIssues Issues;
    Ydb::Query::ExecuteScriptMetadata Metadata;
};

class TGetScriptExecutionOperationActor : public TActorBootstrapped<TGetScriptExecutionOperationActor> {
    using TBase = TActorBootstrapped<TGetScriptExecutionOperationActor>;

    inline static const TDuration CHECK_ALIVE_REQUEST_TIMEOUT = TDuration::Seconds(60);

public:
    explicit TGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {}

    void Bootstrap() {
        CreateGetScriptExecutionOperationQuery(false);
        Become(&TGetScriptExecutionOperationActor::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvGetScriptExecutionOperationResponse, Handle);
        hFunc(TEvCheckAliveResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
    )

    void CreateGetScriptExecutionOperationQuery(bool finishIfLeaseExpired) {
        Register(new TGetScriptExecutionOperationQueryActor(Request->Get()->Database, Request->Get()->OperationId, finishIfLeaseExpired));
    }

    void CreateFinishScriptExecutionOperationQuery() {
        if (!WaitFinishQuery) {
            WaitFinishQuery = true;
            CreateGetScriptExecutionOperationQuery(true);
        }
    }

    void Handle(TEvGetScriptExecutionOperationResponse::TPtr& ev) {
        Response = std::move(ev);

        if (WaitFinishQuery || !Response->Get()->LeaseExpired) {
            Reply();
            return;
        }

        Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup());

        NActors::TActorId runScriptActor = Response->Get()->RunScriptActorId;
        ui64 flags = IEventHandle::FlagTrackDelivery;
        if (runScriptActor.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = runScriptActor.NodeId();
        }
        Send(runScriptActor, new TEvCheckAliveRequest(), flags);
    }

    void Handle(TEvCheckAliveResponse::TPtr&) {
        Reply();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        CreateFinishScriptExecutionOperationQuery();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        CreateFinishScriptExecutionOperationQuery();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        CreateFinishScriptExecutionOperationQuery();
    }

    void Reply() {
        Send(Request->Sender, Response->Release().Release());
        PassAway();
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

private:
    TEvGetScriptExecutionOperation::TPtr Request;
    TEvGetScriptExecutionOperationResponse::TPtr Response;
    bool WaitFinishQuery = false;
    TMaybe<ui32> SubscribedOnSession;
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
                Register(new TCheckLeaseStatusActor(Request->Get()->Database, metadata.execution_id(), Ydb::StatusIds::ABORTED, i));
                ++OperationsToCheck;
            }
        }

        if (OperationsToCheck == 0) {
            Reply();
        }
    }

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        Y_VERIFY(ev->Cookie < Response->Get()->Operations.size());

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
            Y_VERIFY(ev->Get()->ExecutionStatus);
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
        Register(new TCheckLeaseStatusActor(Request->Get()->Database, ExecutionId));
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
            Register(new TCheckLeaseStatusActor(Request->Get()->Database, ExecutionId)); // Check if the operation has finished.
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

class TSaveScriptExecutionResultMetaActor : public TActorBootstrapped<TSaveScriptExecutionResultMetaActor> {
public:
    TSaveScriptExecutionResultMetaActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, const TString& serializedMetas)
        : ReplyActorId(replyActorId), Database(database), ExecutionId(executionId), SerializedMetas(serializedMetas)
    {
    }

    void Bootstrap() {
        Register(new TSaveScriptExecutionResultMetaQuery(Database, ExecutionId, SerializedMetas));

        Become(&TSaveScriptExecutionResultMetaActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvSaveScriptResultMetaFinished, Handle);
    )

    void Handle(TEvSaveScriptResultMetaFinished::TPtr& ev) {
        Send(ev->Forward(ReplyActorId));
        PassAway();
    }

private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const TString SerializedMetas;
};

class TSaveScriptExecutionResultQuery : public TQueryBase {
public:
    TSaveScriptExecutionResultQuery(const TString& database, const TString& executionId, i32 resultSetId, TInstant expireAt, i64 firstRow, std::vector<TString>&& serializedRows)
        : Database(database), ExecutionId(executionId), ResultSetId(resultSetId), ExpireAt(expireAt), FirstRow(firstRow), SerializedRows(std::move(serializedRows))
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptExecutionResultQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $expire_at AS Timestamp;
            DECLARE $items AS List<Struct<row_id:Int64,result_set:String>>;

            UPSERT INTO `.metadata/result_sets`
            SELECT $database as database, $execution_id as execution_id, $result_set_id as result_set_id,
                T.row_id as row_id, $expire_at as expire_at, T.result_set as result_set
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
                .Timestamp(ExpireAt)
                .Build();

        auto& param = params
            .AddParam("$items");

        param
                .BeginList();

        auto row = FirstRow;
        for(auto& serializedRow : SerializedRows) {
            param
                    .AddListItem()
                    .BeginStruct()
                        .AddMember("row_id")
                            .Int64(row++)
                        .AddMember("result_set")
                            .String(serializedRow)
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
            Send(Owner, new TEvSaveScriptResultFinished(status));
        } else {
            Send(Owner, new TEvSaveScriptResultFinished(status, std::move(issues)));
        }
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetId;
    const TInstant ExpireAt;
    const i64 FirstRow;
    const std::vector<TString> SerializedRows;
};

class TSaveScriptExecutionResultActor : public TActorBootstrapped<TSaveScriptExecutionResultActor> {
public:
    TSaveScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetId, TInstant expireAt, i64 firstRow, std::vector<TString>&& serializedRows)
        : ReplyActorId(replyActorId), Database(database), ExecutionId(executionId), ResultSetId(resultSetId), ExpireAt(expireAt), FirstRow(firstRow), SerializedRows(std::move(serializedRows))
    {
    }

    void Bootstrap() {
        Register(new TSaveScriptExecutionResultQuery(Database, ExecutionId, ResultSetId, ExpireAt, FirstRow, std::move(SerializedRows)));

        Become(&TSaveScriptExecutionResultActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvSaveScriptResultFinished, Handle);
    )

    void Handle(TEvSaveScriptResultFinished::TPtr& ev) {
        Send(ev->Forward(ReplyActorId));
        PassAway();
    }

private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetId;
    const TInstant ExpireAt;
    const i64 FirstRow;
    std::vector<TString> SerializedRows;
};

class TGetScriptExecutionResultQuery : public TQueryBase {
public:
    TGetScriptExecutionResultQuery(const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 limit)
        : Database(database), ExecutionId(executionId), ResultSetIndex(resultSetIndex), Offset(offset), Limit(limit)
    {
        Response = MakeHolder<TEvKqp::TEvFetchScriptResultsResponse>();
        Response->Record.SetResultSetIndex(ResultSetIndex);
    }

    void OnRunQuery() override {
        TString sql = R"(
            -- TGetScriptExecutionResultQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $offset AS Int64;
            DECLARE $limit AS Uint64;

            SELECT result_set_metas,  operation_status, issues, end_ts, meta
            FROM `.metadata/script_executions`
            WHERE database = $database
              AND execution_id = $execution_id
              AND (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT row_id, result_set
            FROM `.metadata/result_sets`
            WHERE database = $database
              AND execution_id = $execution_id
              AND result_set_id = $result_set_id
              AND row_id >= $offset
            ORDER BY row_id
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
                .Uint64(Limit)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        { // columns
            NYdb::TResultSetParser result(ResultSets[0]);

            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "Script execution not found");
                return;
            }

            TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
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

            *Response->Record.MutableResultSet()->mutable_columns() = meta.columns();
            Response->Record.MutableResultSet()->set_truncated(meta.truncated());
        }

        { // rows
            NYdb::TResultSetParser result(ResultSets[1]);

            while (result.TryNextRow()) {
                const TMaybe<TString> serializedRow = result.ColumnParser("result_set").GetOptionalString();

                if (!serializedRow) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is empty");
                    return;
                }

                if (!Response->Record.MutableResultSet()->add_rows()->ParseFromString(*serializedRow)) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is corrupted");
                    return;
                }
            }
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Response->Record.SetStatus(status);
        if (status != Ydb::StatusIds::SUCCESS) {
            Response->Record.MutableResultSet()->Clear();
        }
        if (issues) {
            NYql::IssuesToMessage(issues, Response->Record.MutableIssues());
        }
        Send(Owner, std::move(Response));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetIndex;
    const i64 Offset;
    const i64 Limit;
    THolder<TEvKqp::TEvFetchScriptResultsResponse> Response;
};

class TGetScriptExecutionResultActor : public TActorBootstrapped<TGetScriptExecutionResultActor> {
public:
    TGetScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 limit)
        : ReplyActorId(replyActorId), Database(database), ExecutionId(executionId), ResultSetIndex(resultSetIndex), Offset(offset), Limit(limit)
    {
    }

    void Bootstrap() {
        Register(new TGetScriptExecutionResultQuery(Database, ExecutionId, ResultSetIndex, Offset, Limit));

        Become(&TGetScriptExecutionResultActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvKqp::TEvFetchScriptResultsResponse, Handle);
    )

    void Handle(TEvKqp::TEvFetchScriptResultsResponse::TPtr& ev) {
        Send(ev->Forward(ReplyActorId));
        PassAway();
    }

private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetIndex;
    const i64 Offset;
    const i64 Limit;
};

} // anonymous namespace

NActors::IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TDuration maxRunTime) {
    return new TCreateScriptExecutionActor(std::move(ev), queryServiceConfig, counters, maxRunTime);
}

NActors::IActor* CreateScriptExecutionsTablesCreator(THolder<NActors::IEventBase> resultEvent) {
    return new TScriptExecutionsTablesCreator(std::move(resultEvent));
}

NActors::IActor* CreateScriptExecutionFinisher(
    const TString& executionId,
    const TString& database,
    ui64 leaseGeneration,
    Ydb::StatusIds::StatusCode operationStatus,
    Ydb::Query::ExecStatus execStatus,
    NYql::TIssues issues,
    TMaybe<NKqpProto::TKqpStatsQuery> queryStats,
    TMaybe<TString> queryPlan,
    TMaybe<TString> queryAst
    )
{
    return new TScriptExecutionFinisher(executionId, database, leaseGeneration, operationStatus, execStatus, std::move(issues), std::move(queryStats), std::move(queryPlan), std::move(queryAst));
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

NActors::IActor* CreateSaveScriptExecutionResultMetaActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, const TString& serializedMeta) {
    return new TSaveScriptExecutionResultMetaActor(replyActorId, database, executionId, serializedMeta);
}

NActors::IActor* CreateSaveScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetId, TInstant expireAt, i64 firstRow, std::vector<TString>&& serializedRows) {
    return new TSaveScriptExecutionResultActor(replyActorId, database, executionId, resultSetId, expireAt, firstRow, std::move(serializedRows));
}

NActors::IActor* CreateGetScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 limit) {
    return new TGetScriptExecutionResultActor(replyActorId, database, executionId, resultSetIndex, offset, limit);
}

namespace NPrivate {

NActors::IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record, TDuration operationTtl, TDuration resultsTtl,  TDuration leaseDuration) {
    return new TCreateScriptOperationQuery(executionId, runScriptActorId, record, operationTtl, resultsTtl, leaseDuration);
}

NActors::IActor* CreateCheckLeaseStatusActor(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode statusOnExpiredLease, ui64 cookie) {
    return new TCheckLeaseStatusActor(database, executionId, statusOnExpiredLease, cookie);
}

} // namespace NPrivate

} // namespace NKikimr::NKqp
