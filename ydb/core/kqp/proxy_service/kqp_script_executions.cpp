#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"
#include "kqp_script_execution_retries.h"

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
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/generic/guid.h>
#include <util/generic/utility.h>

namespace NKikimr::NKqp {

using namespace NKikimr::NKqp::NPrivate;

namespace {

#define KQP_PROXY_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)
#define KQP_PROXY_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)
#define KQP_PROXY_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)
#define KQP_PROXY_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)
#define KQP_PROXY_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)
#define KQP_PROXY_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)
#define KQP_PROXY_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[ScriptExecutions] " << LogPrefix() << stream)

constexpr TDuration LEASE_DURATION = TDuration::Seconds(30);
constexpr TDuration DEADLINE_OFFSET = TDuration::Minutes(20);
constexpr TDuration BRO_RUN_INTERVAL = TDuration::Minutes(60);
constexpr ui64 MAX_TRANSIENT_ISSUES_COUNT = 10;

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

NYql::TIssues DeserializeIssues(const std::string& issuesSerialized) {
    Ydb::Issue::IssueMessage rootMessage = NProtobufJson::Json2Proto<Ydb::Issue::IssueMessage>(issuesSerialized);
    NYql::TIssue root = NYql::IssueFromMessage(rootMessage);

    NYql::TIssues issues;
    for (const auto& issuePtr : root.GetSubIssues()) {
        issues.AddIssue(*issuePtr);
    }
    return issues;
}

template <typename TProto>
void SerializeBinaryProto(const TProto& proto, NJson::TJsonValue& value) {
    value.SetType(NJson::EJsonValueType::JSON_MAP);

    const auto config = NProtobufJson::TProto2JsonConfig()
        .AddStringTransform(MakeIntrusive<NProtobufJson::TBase64EncodeBytesTransform>());

    NProtobufJson::Proto2Json(proto, value["encoded_proto"], config);
}

template <typename TProto>
TString SerializeBinaryProto(const TProto& proto) {
    NJson::TJsonValue value;
    SerializeBinaryProto(proto, value);

    NJsonWriter::TBuf serializedProto;
    serializedProto.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

    return serializedProto.Str();
}

template <typename TProto>
void DeserializeBinaryProto(const NJson::TJsonValue& value, TProto& proto) {
    const auto& valueMap = value.GetMap();
    const auto encodedProto = valueMap.find("encoded_proto");
    if (encodedProto == valueMap.end()) {
        return NProtobufJson::Json2Proto(value, proto, NProtobufJson::TJson2ProtoConfig());
    }

    const auto config = NProtobufJson::TJson2ProtoConfig()
        .AddStringTransform(MakeIntrusive<NProtobufJson::TBase64DecodeBytesTransform>());

    NProtobufJson::Json2Proto(encodedProto->second, proto, config);
}

class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(const TString& operationName, const TString& executionId, TString sessionId = {})
        : NKikimr::TQueryBase(NKikimrServices::KQP_PROXY, sessionId)
    {
        SetOperationInfo(operationName, executionId);
    }
};

class TScriptExecutionsTablesCreator : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

public:
    explicit TScriptExecutionsTablesCreator()
        : TBase({
            GetScriptExecutionsCreator(),
            GetScriptExecutionLeasesCreator(),
            GetScriptResultSetsCreator()
        })
    {}

private:
    static IActor* GetScriptExecutionsCreator() {
        return CreateTableCreator(
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
                Col("transient_issues", NScheme::NTypeIds::JsonDocument), // Issues from previous query retries
                Col("plan", NScheme::NTypeIds::JsonDocument),
                Col("meta", NScheme::NTypeIds::JsonDocument),
                Col("parameters", NScheme::NTypeIds::String), // TODO: store aparameters separately to support bigger storage.
                Col("result_set_metas", NScheme::NTypeIds::JsonDocument),
                Col("stats", NScheme::NTypeIds::JsonDocument),
                Col("expire_at", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                Col("customer_supplied_id", NScheme::NTypeIds::Text),
                Col("user_token", NScheme::NTypeIds::Text), // UsedSID
                Col("user_group_sids", NScheme::NTypeIds::JsonDocument),
                Col("script_sinks", NScheme::NTypeIds::JsonDocument),
                Col("script_secret_names", NScheme::NTypeIds::JsonDocument),
                Col("retry_state", NScheme::NTypeIds::JsonDocument),
            },
            { "database", "execution_id" },
            NKikimrServices::KQP_PROXY,
            TtlCol("expire_at", DEADLINE_OFFSET, BRO_RUN_INTERVAL)
        );
    }

    static IActor* GetScriptExecutionLeasesCreator() {
        return CreateTableCreator(
            { ".metadata", "script_execution_leases" },
            {
                Col("database", NScheme::NTypeIds::Text),
                Col("execution_id", NScheme::NTypeIds::Text),
                Col("lease_deadline", NScheme::NTypeIds::Timestamp),
                Col("lease_generation", NScheme::NTypeIds::Int64),
                Col("lease_state", NScheme::NTypeIds::Int32), // Operation for which lease was created (ELeaseState)
                Col("expire_at", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
            },
            { "database", "execution_id" },
            NKikimrServices::KQP_PROXY,
            TtlCol("expire_at", DEADLINE_OFFSET, BRO_RUN_INTERVAL)
        );
    }

    static IActor* GetScriptResultSetsCreator() {
        return CreateTableCreator(
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
            TtlCol("expire_at", DEADLINE_OFFSET, BRO_RUN_INTERVAL)
        );
    }

    void OnTablesCreated(bool success, NYql::TIssues issues) override  {
        Send(Owner, new TEvScriptExecutionsTablesCreationFinished(success, std::move(issues)));
        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), new TEvStartScriptExecutionBackgroundChecks());
    }
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

NKikimrKqp::EQueryAction GetActionFromExecMode(Ydb::Query::ExecMode execMode) {
    switch (execMode) {
        case Ydb::Query::EXEC_MODE_EXECUTE:
            return NKikimrKqp::QUERY_ACTION_EXECUTE;
        case Ydb::Query::EXEC_MODE_EXPLAIN:
            return NKikimrKqp::QUERY_ACTION_EXPLAIN;
        case Ydb::Query::EXEC_MODE_VALIDATE:
            return NKikimrKqp::QUERY_ACTION_VALIDATE;
        case Ydb::Query::EXEC_MODE_PARSE:
            return NKikimrKqp::QUERY_ACTION_PARSE;
        default:
            throw std::runtime_error(TStringBuilder() << "Unsupported query execute mode: " << Ydb::Query::ExecMode_Name(execMode));
    }
}

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, bool force = false) {
    if (!issues && !force) {
        return {};
    }

    NYql::TIssue rootIssue(message);
    for (const auto& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }

    return {rootIssue};
}

class TCreateScriptOperationQuery : public TQueryBase {
public:
    TCreateScriptOperationQuery(const TString& executionId, const TActorId& runScriptActorId,
        const NKikimrKqp::TEvQueryRequest& req, const NKikimrKqp::TScriptExecutionOperationMeta& meta,
        TDuration maxRunTime, const NKikimrKqp::TScriptExecutionRetryState& retryState)
        : TQueryBase(__func__, executionId)
        , ExecutionId(executionId)
        , RunScriptActorId(runScriptActorId)
        , Request(req)
        , Meta(meta)
        , MaxRunTime(Max(maxRunTime, TDuration::Days(1)))
        , RetryState(retryState)
    {}

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
            DECLARE $lease_state AS Int32;
            DECLARE $execution_meta_ttl AS Interval;
            DECLARE $retry_state AS JsonDocument;
            DECLARE $user_sid AS Text;
            DECLARE $user_group_sids AS JsonDocument;
            DECLARE $parameters AS String;

            UPSERT INTO `.metadata/script_executions` (
                database, execution_id, run_script_actor_id, execution_status, execution_mode, start_ts,
                query_text, syntax, meta, expire_at, retry_state,
                user_token, user_group_sids, parameters
            ) VALUES (
                $database, $execution_id, $run_script_actor_id, $execution_status, $execution_mode, CurrentUtcTimestamp(),
                $query_text, $syntax, $meta, CurrentUtcTimestamp() + $execution_meta_ttl, $retry_state,
                $user_sid, $user_group_sids, $parameters
            );

            UPSERT INTO `.metadata/script_execution_leases` (
                database, execution_id, lease_deadline, lease_generation,
                expire_at, lease_state
            ) VALUES (
                $database, $execution_id, CurrentUtcTimestamp() + $lease_duration, 1,
                CurrentUtcTimestamp() + $execution_meta_ttl, $lease_state
            );
        )";

        const auto token = NACLib::TUserToken(Request.GetUserToken());

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
                .JsonDocument(SerializeBinaryProto(Meta))
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(GetDuration(Meta.GetLeaseDuration()).MicroSeconds()))
                .Build()
            .AddParam("$execution_meta_ttl")
                .Interval(2 * std::min(static_cast<i64>(MaxRunTime.MicroSeconds()), std::numeric_limits<i64>::max() / 2))
                .Build()
            .AddParam("$retry_state")
                .JsonDocument(NProtobufJson::Proto2Json(RetryState, NProtobufJson::TProto2JsonConfig()))
                .Build()
            .AddParam("$lease_state")
                .Int32(static_cast<i32>(ELeaseState::ScriptRunning))
                .Build()
            .AddParam("$user_sid")
                .Utf8(token.GetUserSID())
                .Build()
            .AddParam("$user_group_sids")
                .JsonDocument(SerializeGroupSids(token.GetGroupSIDs()))
                .Build()
            .AddParam("$parameters")
                .String(SerializeParameters())
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Create script execution operation"
            << ". RetryState: " << RetryState.ShortDebugString()
            << ". Result: " << status
            << ". Issues: " << issues.ToOneLineString());
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(ExecutionId));
        } else {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(status, std::move(issues)));
        }
    }

private:
    static TString SerializeGroupSids(const TVector<NACLib::TSID>& groupSids) {
        NJson::TJsonValue value;
        value.SetType(NJson::EJsonValueType::JSON_ARRAY);

        NJson::TJsonValue::TArray& jsonArray = value.GetArraySafe();
        jsonArray.resize(groupSids.size());
        for (size_t i = 0; i < groupSids.size(); ++i) {
            jsonArray[i] = NJson::TJsonValue(groupSids[i]);
        }

        NJsonWriter::TBuf serializedGroupSids;
        serializedGroupSids.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

        return serializedGroupSids.Str();
    }

    TString SerializeParameters() const {
        NJson::TJsonValue value;
        value.SetType(NJson::EJsonValueType::JSON_MAP);

        const auto& parameters = Request.GetRequest().GetYdbParameters();

        NJson::TJsonValue::TMapType& jsonMap = value.GetMapSafe();
        jsonMap.reserve(parameters.size());
        for (const auto& [name, value] : parameters) {
            NJson::TJsonValue paramValue;
            SerializeBinaryProto(value, paramValue);
            Y_ENSURE(jsonMap.emplace(name, std::move(paramValue)).second);
        }

        NJsonWriter::TBuf serializedParams;
        serializedParams.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

        return serializedParams.Str();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TCreateScriptOperationQuery] ExecutionId: " << ExecutionId << " RunScriptActorId: " << RunScriptActorId << ". ";
    }

private:
    const TString ExecutionId;
    const TActorId RunScriptActorId;
    const NKikimrKqp::TEvQueryRequest Request;
    const NKikimrKqp::TScriptExecutionOperationMeta Meta;
    const TDuration MaxRunTime;
    const NKikimrKqp::TScriptExecutionRetryState RetryState;
};

class TCreateScriptExecutionActor : public TActorBootstrapped<TCreateScriptExecutionActor> {
public:
    TCreateScriptExecutionActor(TEvKqp::TEvScriptRequest::TPtr&& ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TDuration maxRunTime, TDuration leaseDuration)
        : Event(std::move(ev))
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
        , ExecutionId(CreateGuidAsString())
        , LeaseDuration(leaseDuration ? leaseDuration : LEASE_DURATION)
        , MaxRunTime(maxRunTime)
    {}

    void Bootstrap() {
        Become(&TCreateScriptExecutionActor::StateFunc);

        const auto& ev = *Event->Get();
        const auto& eventProto = ev.Record;
        const auto& meta = GetOperationMeta();
        const TKqpRunScriptActorSettings settings = {
            .Database = eventProto.GetRequest().GetDatabase(),
            .ExecutionId = ExecutionId,
            .LeaseGeneration = 1,
            .LeaseDuration = LeaseDuration,
            .ResultsTtl = GetDuration(meta.GetResultsTtl()),
            .ProgressStatsPeriod = ev.ProgressStatsPeriod,
            .Counters = Counters,
        };

        // Start request
        RunScriptActorId = Register(CreateRunScriptActor(eventProto, settings, QueryServiceConfig));
        Register(new TCreateScriptOperationQuery(ExecutionId, RunScriptActorId, ev.Record, meta, MaxRunTime, GetRetryState()));
    }

    void Handle(TEvPrivate::TEvCreateScriptOperationResponse::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            Send(RunScriptActorId, new TEvents::TEvWakeup());
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(
                ScriptExecutionOperationFromExecutionId(ev->Get()->ExecutionId),
                ev->Get()->ExecutionId,
                Ydb::Query::EXEC_STATUS_STARTING,
                GetExecModeFromAction(Event->Get()->Record.GetRequest().GetAction())
            ));
        } else {
            SendFailResponse(ev->Get()->Status, ev->Get()->Issues);
        }
        PassAway();
    }

    void HandleException(const std::exception& ex) {
        SendFailResponse(Ydb::StatusIds::INTERNAL_ERROR, {NYql::TIssue(TStringBuilder() << "Got unexpected exception: " << ex.what())});
        PassAway();
    }

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(TEvPrivate::TEvCreateScriptOperationResponse, Handle),
        ExceptionFunc(std::exception, HandleException)
    )

private:
    void SendFailResponse(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) const {
        Send(RunScriptActorId, new TEvents::TEvPoison());
        Send(Event->Sender, new TEvKqp::TEvScriptResponse(status, issues));
    }

    NKikimrKqp::TScriptExecutionOperationMeta GetOperationMeta() const {
        const auto& ev = *Event->Get();
        const auto& eventProto = ev.Record;
        const auto& request = eventProto.GetRequest();

        NKikimrKqp::TScriptExecutionOperationMeta meta;
        meta.SetTraceId(eventProto.GetTraceId());
        meta.SetResourcePoolId(request.GetPoolId());
        meta.SetClientAddress(request.GetClientAddress());
        meta.SetCollectStats(request.GetCollectStats());
        *meta.MutableRlPath() = eventProto.GetRlPath();
        SetDuration(LeaseDuration, *meta.MutableLeaseDuration());
        SetDuration(ev.ProgressStatsPeriod, *meta.MutableProgressStatsPeriod());

        const auto operationTtl = ev.ForgetAfter ? ev.ForgetAfter : TDuration::Seconds(QueryServiceConfig.GetScriptForgetAfterDefaultSeconds());
        SetDuration(operationTtl, *meta.MutableOperationTtl());

        auto resultsTtl = ev.ResultsTtl ? ev.ResultsTtl : TDuration::Seconds(QueryServiceConfig.GetScriptResultsTtlDefaultSeconds());
        if (operationTtl) {
            resultsTtl = Min(operationTtl, resultsTtl);
        }

        SetDuration(resultsTtl, *meta.MutableResultsTtl());

        if (const auto timeout = TDuration::MilliSeconds(request.GetTimeoutMs())) {
            *meta.MutableTimeoutAt() = NProtoInterop::CastToProto(TInstant::Now() + timeout);
        }

        if (const auto cancelAfter = TDuration::MilliSeconds(request.GetCancelAfterMs())) {
            *meta.MutableCancelAt() = NProtoInterop::CastToProto(TInstant::Now() + cancelAfter);
        }

        return meta;
    }

    NKikimrKqp::TScriptExecutionRetryState GetRetryState() const {
        const auto& retryMapping = Event->Get()->RetryMapping;

        NKikimrKqp::TScriptExecutionRetryState retryState;
        retryState.MutableRetryPolicyMapping()->Assign(retryMapping.begin(), retryMapping.end());

        return retryState;
    }

private:
    const TEvKqp::TEvScriptRequest::TPtr Event;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    const TString ExecutionId;
    TActorId RunScriptActorId;
    const TDuration LeaseDuration;
    const TDuration MaxRunTime;
};

class TScriptLeaseUpdater : public TQueryBase {
public:
    TScriptLeaseUpdater(const TString& database, const TString& executionId, TDuration leaseDuration, i64 leaseGeneration)
        : TQueryBase(__func__, executionId)
        , Database(database)
        , ExecutionId(executionId)
        , LeaseDuration(leaseDuration)
        , LeaseGeneration(leaseGeneration)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptLeaseUpdater::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                lease_generation,
                lease_state
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

        SetQueryResultHandler(&TScriptLeaseUpdater::OnGetLeaseInfo, "Get lease info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetLeaseInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            LeaseExists = false;
            Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
            return;
        }

        const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
        if (!leaseGenerationInDatabase) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
            return;
        }

        if (LeaseGeneration != *leaseGenerationInDatabase) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Lease was lost, expected generation: " << LeaseGeneration << ", got: " << *leaseGenerationInDatabase);
            return;
        }

        const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning));
        if (leaseState != static_cast<i32>(ELeaseState::ScriptRunning)) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Script execution was terminated, expected lease state: " << static_cast<i32>(ELeaseState::ScriptRunning) << ", got: " << leaseState);
            return;
        }

        UpdateLease();
    }

    void UpdateLease() {
        // Updating the lease in the table can take a long time,
        // so the query uses CurrentUtcTimestamp(), 
        // but for the next update, LeaseDeadline is used,
        // which corresponds to a strictly shorter time.
        LeaseDeadline = TInstant::Now() + LeaseDuration;

        TString sql = R"(
            -- TScriptLeaseUpdater::UpdateLease
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $lease_duration AS Interval;

            UPDATE `.metadata/script_execution_leases`
            SET lease_deadline = (CurrentUtcTimestamp() + $lease_duration)
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

        SetQueryResultHandler(&TScriptLeaseUpdater::OnQueryResult, "Update lease");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
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
    const i64 LeaseGeneration;
    TInstant LeaseDeadline;
    bool LeaseExists = true;
};

class TScriptLeaseUpdateActor : public TActorBootstrapped<TScriptLeaseUpdateActor> {
public:
    using TLeaseUpdateRetryActor = TQueryRetryActor<TScriptLeaseUpdater, TEvScriptLeaseUpdateResponse, TString, TString, TDuration, i64>;

    TScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId,
        TDuration leaseDuration, i64 leaseGeneration, TIntrusivePtr<TKqpCounters> counters)
        : RunScriptActorId(runScriptActorId)
        , Database(database)
        , ExecutionId(executionId)
        , LeaseDuration(leaseDuration)
        , LeaseGeneration(leaseGeneration)
        , Counters(counters)
        , LeaseUpdateStartTime(TInstant::Now())
    {}

    void Bootstrap() {
        Register(new TLeaseUpdateRetryActor(
            SelfId(),
            TLeaseUpdateRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(
                TLeaseUpdateRetryActor::Retryable,
                TDuration::MilliSeconds(10),
                TDuration::MilliSeconds(200),
                TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(),
                LeaseDuration / 2
            ),
            Database, ExecutionId, LeaseDuration, LeaseGeneration
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
        Forward(ev, RunScriptActorId);
        PassAway();
    }

private:
    const TActorId RunScriptActorId;
    const TString Database;
    const TString ExecutionId;
    const TDuration LeaseDuration;
    const i64 LeaseGeneration;
    const TIntrusivePtr<TKqpCounters> Counters;
    const TInstant LeaseUpdateStartTime;
};

class TRestartScriptOperationQuery : public TQueryBase {
public:
    TRestartScriptOperationQuery(const TString& database, const TString& executionId, i64 leaseGeneration,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : TQueryBase(__func__, executionId)
        , Database(database)
        , ExecutionId(executionId)
        , LeaseGeneration(leaseGeneration)
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TRestartScriptOperationQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                finalization_status,
                execution_mode,
                syntax,
                query_text,
                parameters,
                meta,
                issues,
                transient_issues,
                user_token,
                user_group_sids
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_deadline,
                lease_generation,
                lease_state
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

        SetQueryResultHandler(&TRestartScriptOperationQuery::OnGetExecutionInfo, "Get execution info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetExecutionInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "Lease not found");
                return;
            }

            const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
            if (!leaseGenerationInDatabase) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
                return;
            }

            if (LeaseGeneration != *leaseGenerationInDatabase) {
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Lease was lost, expected generation: " << LeaseGeneration << ", got: " << *leaseGenerationInDatabase);
                return;
            }

            const auto leaseDeadline = result.ColumnParser("lease_deadline").GetOptionalTimestamp();
            if (!leaseDeadline) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Not found lease deadline for script execution");
                return;
            }

            if (*leaseDeadline > TInstant::Now()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Lease deadline is in the future");
                return;
            }

            const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning));
            if (leaseState != static_cast<i32>(ELeaseState::WaitRetry)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Expected lease state WaitRetry, but got state: " << leaseState);
                return;
            }
        }

        NKikimrKqp::TEvQueryRequest queryRequest;
        auto& request = *queryRequest.MutableRequest();

        NKikimrKqp::TScriptExecutionOperationMeta meta;

        {   // Execution info
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
                return;
            }

            const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
            if (!operationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Can not restart execution without final status");
                return;
            }

            const auto finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
            if (finalizationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Can not restart execution while finalization is not finished, current status: " << *finalizationStatus);
                return;
            }

            if (const auto issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument()) {
                TransientIssues.AddIssues(DeserializeIssues(*issuesSerialized));
            }

            if (const auto transientIssuesSerialized = result.ColumnParser("transient_issues").GetOptionalJsonDocument()) {
                const auto previousIssues = DeserializeIssues(*transientIssuesSerialized);

                TransientIssues.Reserve(TransientIssues.Size() + previousIssues.Size());
                for (const auto& issue : previousIssues) {
                    if (TransientIssues.Size() >= MAX_TRANSIENT_ISSUES_COUNT) {
                        TransientIssues.AddIssue(TStringBuilder() << "Saved only issues about last " << MAX_TRANSIENT_ISSUES_COUNT << " runs");
                        break;
                    }
                    TransientIssues.AddIssue(issue);
                }
            }

            TVector<NACLib::TSID> userGroupSids;
            if (const auto serializedUserGroupSids = result.ColumnParser("user_group_sids").GetOptionalJsonDocument()) {
                NJson::TJsonValue value;
                if (!NJson::ReadJsonTree(*serializedUserGroupSids, &value) || value.GetType() != NJson::JSON_ARRAY) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "User group sids are corrupted");
                    return;
                }

                const auto sidsSize = value.GetIntegerRobust();

                userGroupSids.reserve(sidsSize);
                for (i64 i = 0; i < sidsSize; ++i) {
                    const NJson::TJsonValue* userSid = nullptr;
                    value.GetValuePointer(i, &userSid);
                    Y_ENSURE(userSid);

                    userGroupSids.emplace_back(userSid->GetString());
                }
            }

            queryRequest.SetUserToken(NACLib::TUserToken(
                result.ColumnParser("user_token").GetOptionalUtf8().value_or(""),
                userGroupSids
            ).SerializeAsString());

            if (const auto serializedParameters = result.ColumnParser("parameters").GetOptionalString()) {
                NJson::TJsonValue value;
                if (!NJson::ReadJsonTree(*serializedParameters, &value) || value.GetType() != NJson::JSON_MAP) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Parameters are corrupted");
                    return;
                }

                auto& parameters = *request.MutableYdbParameters();
                for (const auto& [key, value] : value.GetMapSafe()) {
                    DeserializeBinaryProto(value, parameters[key]);
                }
            }

            const std::optional<TString> queryText = result.ColumnParser("query_text").GetOptionalUtf8();
            if (!queryText) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Query text is not found");
                return;
            }

            request.SetQuery(*queryText);
            request.SetAction(GetActionFromExecMode(static_cast<Ydb::Query::ExecMode>(result.ColumnParser("execution_mode").GetOptionalInt32().value_or(Ydb::Query::EXEC_MODE_UNSPECIFIED))));
            request.SetSyntax(static_cast<Ydb::Query::Syntax>(result.ColumnParser("syntax").GetOptionalInt32().value_or(Ydb::Query::SYNTAX_UNSPECIFIED)));

            const std::optional<TString> serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
            if (!serializedMeta) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation metainformation");
                return;
            }

            NJson::TJsonValue serializedMetaJson;
            if (!NJson::ReadJsonTree(*serializedMeta, &serializedMetaJson)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Operation metainformation is corrupted");
                return;
            }

            DeserializeBinaryProto(serializedMetaJson, meta);
        }

        LeaseDuration = GetDuration(meta.GetLeaseDuration());

        queryRequest.SetTraceId(meta.GetTraceId());
        *queryRequest.MutableRlPath() = meta.GetRlPath();

        request.SetDatabase(CanonizePath(Database));
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
        request.SetKeepSession(false);
        request.SetTimeoutMs((NProtoInterop::CastFromProto(meta.GetTimeoutAt()) - TInstant::Now()).MilliSeconds());
        request.SetCancelAfterMs((NProtoInterop::CastFromProto(meta.GetCancelAt()) - TInstant::Now()).MilliSeconds());
        request.SetClientAddress(meta.GetClientAddress());
        request.SetCollectStats(meta.GetCollectStats());
        request.SetPoolId(meta.GetResourcePoolId());

        const TKqpRunScriptActorSettings settings = {
            .Database = request.GetDatabase(),
            .ExecutionId = ExecutionId,
            .LeaseGeneration = LeaseGeneration + 1,
            .LeaseDuration = LeaseDuration,
            .ResultsTtl = GetDuration(meta.GetResultsTtl()),
            .ProgressStatsPeriod = GetDuration(meta.GetProgressStatsPeriod()),
            .Counters = Counters,
        };

        RunScriptActorId = Register(CreateRunScriptActor(queryRequest, settings, QueryServiceConfig));
        RestartScriptExecution();
    }

    void RestartScriptExecution() {
        TString sql = R"(
            -- TRestartScriptOperationQuery::RestartScriptExecution
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $lease_duration AS Interval;
            DECLARE $lease_generation AS Int64;
            DECLARE $lease_state AS Int32;
            DECLARE $run_script_actor_id AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $transient_issues AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET
                run_script_actor_id = $run_script_actor_id,
                operation_status = NULL,
                execution_status = $execution_status,
                finalization_status = NULL,
                start_ts = CurrentUtcTimestamp(),
                end_ts = NULL,
                ast = NULL,
                ast_compressed = NULL,
                ast_compression_method = NULL,
                issues = NULL,
                transient_issues = $transient_issues,
                plan = NULL,
                result_set_metas = NULL,
                stats = NULL
            WHERE database = $database AND execution_id = $execution_id;

            UPDATE `.metadata/script_execution_leases`
            SET
                lease_deadline = (CurrentUtcTimestamp() + $lease_duration),
                lease_generation = $lease_generation,
                lease_state = $lease_state
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
                .Build()
            .AddParam("$lease_generation")
                .Int64(LeaseGeneration + 1)
                .Build()
            .AddParam("$lease_state")
                .Int32(static_cast<i32>(ELeaseState::ScriptRunning))
                .Build()
            .AddParam("$run_script_actor_id")
                .Utf8(ScriptExecutionRunnerActorIdString(RunScriptActorId))
                .Build()
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_STARTING)
                .Build()
            .AddParam("$transient_issues")
                .JsonDocument(SerializeIssues(TransientIssues))
                .Build();

        SetQueryResultHandler(&TRestartScriptOperationQuery::OnQueryResult, "Restart script execution");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (RunScriptActorId) {
            if (status == Ydb::StatusIds::SUCCESS) {
                Send(RunScriptActorId, new TEvents::TEvWakeup());
            } else {
                Send(RunScriptActorId, new TEvents::TEvPoison());
            }
        }
        Send(Owner, new TEvScriptExecutionRestarted(status, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i64 LeaseGeneration;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    TDuration LeaseDuration;
    TActorId RunScriptActorId;
    NYql::TIssues TransientIssues;
};

class TCheckLeaseStatusActorBase : public TActorBootstrapped<TCheckLeaseStatusActorBase> {
    using TBase = TActorBootstrapped<TCheckLeaseStatusActorBase>;

    inline static const TDuration CHECK_ALIVE_REQUEST_TIMEOUT = TDuration::Seconds(60);
    inline static const ui64 MAX_CHECK_ALIVE_RETRIES = 100;

public:
    TCheckLeaseStatusActorBase(const TString& operationName, const TString& database, const TString& executionId,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : OperationName(operationName)
        , Database(database)
        , ExecutionId(executionId)
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

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

    void StartScriptFinalization(EFinalizationStatus finalizationStatus, TMaybe<Ydb::StatusIds::StatusCode> status, TMaybe<Ydb::Query::ExecStatus> execStatus, NYql::TIssues issues, i64 leaseGeneration) {
        KQP_PROXY_LOG_D("Try to finalize script execution operation, finalization action: " << static_cast<i32>(finalizationStatus));

        if (!status || !execStatus) {
            issues.AddIssue("Finalization is not complete");
        }

        SetupFinalizeRequest(finalizationStatus, status ? *status : Ydb::StatusIds::UNAVAILABLE, execStatus ? *execStatus : Ydb::Query::EXEC_STATUS_ABORTED, std::move(issues), leaseGeneration);
        RunScriptFinalizeRequest();

        Become(&TCheckLeaseStatusActorBase::StateFunc);
    }

    void StartLeaseChecking(TActorId runScriptActorId, i64 leaseGeneration) {
        RunScriptActorId = runScriptActorId;
        KQP_PROXY_LOG_W("RunScriptActorId: " << runScriptActorId << ", script execution lease is expired, start lease checking");

        SetupFinalizeRequest(EFinalizationStatus::FS_ROLLBACK, Ydb::StatusIds::UNAVAILABLE, Ydb::Query::EXEC_STATUS_ABORTED, NYql::TIssues{ NYql::TIssue("Lease expired") }, leaseGeneration);
        Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup());

        CheckAliveFlags = IEventHandle::FlagTrackDelivery;
        if (runScriptActorId.NodeId() != SelfId().NodeId()) {
            CheckAliveFlags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = runScriptActorId.NodeId();
        }
        Send(runScriptActorId, new TEvCheckAliveRequest(), CheckAliveFlags);

        Become(&TCheckLeaseStatusActorBase::StateFunc);
    }

    void RestartScriptExecution(i64 leaseGeneration) {
        KQP_PROXY_LOG_N("LeaseGeneration: " << leaseGeneration << ", restarting script execution");

        Register(new TRestartScriptOperationQuery(Database, ExecutionId, leaseGeneration, QueryServiceConfig, Counters));

        Become(&TCheckLeaseStatusActorBase::StateFunc);
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << OperationName << "] ExecutionId: " << ExecutionId << ". ";
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    virtual void OnBootstrap() = 0;
    virtual void OnLeaseVerified() = 0;
    virtual void OnScriptExecutionFinished(bool alreadyFinalized, bool waitRetry, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) = 0;
    virtual void OnScriptExecutionRestarted(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) = 0;

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvCheckAliveResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        hFunc(TEvScriptExecutionFinished, Handle);
        hFunc(TEvScriptExecutionRestarted, Handle);
    )

    void SetupFinalizeRequest(EFinalizationStatus finalizationStatus, Ydb::StatusIds::StatusCode status, Ydb::Query::ExecStatus execStatus, NYql::TIssues issues, i64 leaseGeneration) {
        ScriptFinalizeRequest = std::make_unique<TEvScriptFinalizeRequest>(
            finalizationStatus,
            ExecutionId,
            Database,
            status,
            execStatus,
            std::move(issues),
            std::nullopt,  // queryStats
            std::nullopt,  // queryPlan
            std::nullopt,  // queryAst
            leaseGeneration
        );
    }

    void RunScriptFinalizeRequest() {
        if (WaitFinishQuery || LeaseVerified) {
            return;
        }

        WaitFinishQuery = true;
        FinalOperationStatus = ScriptFinalizeRequest->Description.OperationStatus;
        FinalExecStatus = ScriptFinalizeRequest->Description.ExecStatus;
        FinalIssues = ScriptFinalizeRequest->Description.Issues;
        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), ScriptFinalizeRequest.release());
    }

    bool RetryCheckAlive() {
        CheckAliveRetries++;

        if (WaitFinishQuery || LeaseVerified) {
            // Already finished checks
            return false;
        }

        if (CheckAliveRetries >= MAX_CHECK_ALIVE_RETRIES) {
            KQP_PROXY_LOG_E("Retry limit exceeded for TRunScriptActor check alive, start finalization");
            RunScriptFinalizeRequest();
            return false;
        }

        Send(RunScriptActorId, new TEvCheckAliveRequest(), CheckAliveFlags);
        return true;
    }

    void Handle(TEvCheckAliveResponse::TPtr&) {
        if (WaitFinishQuery) {
            KQP_PROXY_LOG_E("Script execution lease was verified after started finalization");
        } else if (!LeaseVerified) {
            LeaseVerified = true;
            OnLeaseVerified();
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        KQP_PROXY_LOG_W("Deliver TRunScriptActor check alive request timeout, retry check alive");
        if (RetryCheckAlive()) {
            Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup());
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto reason = ev->Get()->Reason;
        if (reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
            KQP_PROXY_LOG_W("TRunScriptActor not found, start finalization");
            RunScriptFinalizeRequest();
        } else {
            KQP_PROXY_LOG_W("Got delivery problem to node with TRunScriptActor, reason: " << reason);
            RetryCheckAlive();
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        KQP_PROXY_LOG_W("Node with TRunScriptActor was disconnected, retry check alive");
        RetryCheckAlive();
    }

    void Handle(TEvScriptExecutionFinished::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_W("Failed to finalize script execution operation, status: " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        } else if (ev->Get()->OperationAlreadyFinalized) {
            KQP_PROXY_LOG_W("Failed to finalize script execution operation, already finalized");
        } else {
            KQP_PROXY_LOG_D("Successfully finalized script execution operation");
        }

        OnScriptExecutionFinished(ev->Get()->OperationAlreadyFinalized, ev->Get()->WaitingRetry, ev->Get()->Status, AddRootIssue("Finish script execution operation", ev->Get()->Issues));
    }

    void Handle(TEvScriptExecutionRestarted::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_E("Failed to restart script execution operation, status: " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        } else {
            KQP_PROXY_LOG_D("Successfully restarted script execution operation");
        }

        OnScriptExecutionRestarted(ev->Get()->Status, AddRootIssue("Restart script execution operation", ev->Get()->Issues));
    }

private:
    std::unique_ptr<TEvScriptFinalizeRequest> ScriptFinalizeRequest;
    Ydb::StatusIds::StatusCode FinalOperationStatus;
    Ydb::Query::ExecStatus FinalExecStatus;
    NYql::TIssues FinalIssues;

    bool WaitFinishQuery = false;
    bool LeaseVerified = false;
    std::optional<ui32> SubscribedOnSession;
    ui64 CheckAliveFlags = 0;
    ui64 CheckAliveRetries = 0;
    TActorId RunScriptActorId;

protected:
    const TString OperationName;
    const TString Database;
    TString ExecutionId;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
};

class TCheckLeaseStatusQueryActor : public TQueryBase {
    struct TLeaseInfo {
        TInstant Deadline;
        ELeaseState State = ELeaseState::ScriptRunning;
        i64 Generation = 0;
    };

public:
    TCheckLeaseStatusQueryActor(const TString& database, const TString& executionId, ui64 cookie = 0)
        : TQueryBase(__func__, executionId)
        , Database(database)
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

            SELECT
                lease_deadline,
                lease_generation,
                lease_state
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

        NYdb::TResultSetParser executionsResult(ResultSets[0]);
        if (!executionsResult.TryNextRow()) {
            Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
            return;
        }

        executionsResult.TryNextRow();

        const auto runScriptActorId = executionsResult.ColumnParser("run_script_actor_id").GetOptionalUtf8();
        if (!runScriptActorId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Not found run script actor id for script execution");
            return;
        }

        if (!NKqp::ScriptExecutionRunnerActorIdFromString(*runScriptActorId, RunScriptActorId)) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Run script actor id is corrupted");
            return;
        }

        const auto operationStatus = executionsResult.ColumnParser("operation_status").GetOptionalInt32();

        if (const auto finalizationStatus = executionsResult.ColumnParser("finalization_status").GetOptionalInt32()) {
            FinalizationStatus = static_cast<EFinalizationStatus>(*finalizationStatus);
            if (!operationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state, status should be specified during finalization");
                return;
            }
        }

        std::optional<TLeaseInfo> leaseInfo;

        NYdb::TResultSetParser leaseResult(ResultSets[1]);
        if (leaseResult.TryNextRow()) {
            const auto leaseDeadline = leaseResult.ColumnParser("lease_deadline").GetOptionalTimestamp();
            if (!leaseDeadline) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Not found lease deadline for script execution");
                return;
            }

            const auto leaseGeneration = leaseResult.ColumnParser("lease_generation").GetOptionalInt64();
            if (!leaseGeneration) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Not found lease generation for script execution");
                return;
            }

            leaseInfo = {
                .Deadline = *leaseDeadline,
                .State = static_cast<ELeaseState>(leaseResult.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning))),
                .Generation = *leaseGeneration,
            };
        }

        if (leaseInfo) {
            switch (leaseInfo->State) {
                case ELeaseState::ScriptRunning:
                    if (operationStatus) {
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state, status should be empty during query run");
                        return;
                    }
                    break;
                case ELeaseState::ScriptFinalizing:
                    [[fallthrough]];
                case ELeaseState::WaitRetry:
                    if (!operationStatus) {
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Invalid operation state, status should be specified for lease state " << static_cast<i32>(leaseInfo->State));
                        return;
                    }
                    break;
            }

            if (leaseInfo->Deadline < RunStartTime) {
                LeaseExpired = true;
                if (leaseInfo->State == ELeaseState::WaitRetry) {
                    RetryRequired = true;
                } else {
                    FinalizationStatus = EFinalizationStatus::FS_ROLLBACK;
                }
            }

            LeaseGeneration = leaseInfo->Generation;
        } else if (operationStatus) {
            OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);

            if (const auto executionStatus = executionsResult.ColumnParser("execution_status").GetOptionalInt32()) {
                ExecutionStatus = static_cast<Ydb::Query::ExecStatus>(*executionStatus);
            }

            if (const auto issuesSerialized = executionsResult.ColumnParser("issues").GetOptionalJsonDocument()) {
                OperationIssues = DeserializeIssues(*issuesSerialized);
            }
        } else {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state, operation status or lease must be specified");
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvLeaseCheckResult(
                OperationStatus, ExecutionStatus,
                std::move(OperationIssues), RunScriptActorId,
                LeaseExpired, FinalizationStatus,
                RetryRequired, LeaseGeneration
            ), 0, Cookie);
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
    i64 LeaseGeneration = 0;
    TActorId RunScriptActorId;
    bool LeaseExpired = false;
    bool RetryRequired = false;
};

class TCheckLeaseStatusActor : public TCheckLeaseStatusActorBase {
public:
    TCheckLeaseStatusActor(const TActorId& replyActorId, const TString& database, const TString& executionId,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, ui64 cookie = 0)
        : TCheckLeaseStatusActorBase(__func__, database, executionId, queryServiceConfig, counters)
        , ReplyActorId(replyActorId)
        , Cookie(cookie)
    {}

    void OnBootstrap() override {
        KQP_PROXY_LOG_D("Bootstrap. Start TCheckLeaseStatusQueryActor");
        Register(new TCheckLeaseStatusQueryActor(Database, ExecutionId, Cookie));
        Become(&TCheckLeaseStatusActor::StateFunc);
    }

    void OnLeaseVerified() override {
        Reply();
    }

    void OnScriptExecutionFinished(bool alreadyFinalized, bool waitRetry, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(waitRetry);

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

    void OnScriptExecutionRestarted(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status != Ydb::StatusIds::SUCCESS) {
            Reply(status, std::move(issues));
            return;
        }

        Response->Get()->OperationStatus = Nothing();
        Response->Get()->ExecutionStatus = Nothing();
        Response->Get()->OperationIssues = Nothing();
        Reply();
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
    )

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        Response = std::move(ev);

        const auto& event = *Response->Get();
        if (event.RetryRequired) {
            RestartScriptExecution(event.LeaseGeneration);
        } else if (event.LeaseExpired) {
            StartLeaseChecking(event.RunScriptActorId, event.LeaseGeneration);
        } else if (const auto finalizationStatus = event.FinalizationStatus) {
            StartScriptFinalization(*finalizationStatus, event.OperationStatus, event.ExecutionStatus, event.Issues, event.LeaseGeneration);
        } else {
            Reply();
        }
    }

    void Reply() {
        KQP_PROXY_LOG_D("Reply success");
        Forward(Response, ReplyActorId);
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        KQP_PROXY_LOG_W("Reply " << status << ", issues: " << issues.ToOneLineString());
        Send(ReplyActorId, new TEvPrivate::TEvLeaseCheckResult(status, std::move(issues)), 0, Cookie);
        PassAway();
    }

private:
    const TActorId ReplyActorId;
    const ui64 Cookie;
    TEvPrivate::TEvLeaseCheckResult::TPtr Response;
};

class TForgetScriptExecutionOperationQueryActor : public TQueryBase {
    static constexpr i32 MAX_NUMBER_ROWS_IN_BATCH = 100000;

public:
    TForgetScriptExecutionOperationQueryActor(const TString& executionId, const TString& database)
        : TQueryBase(__func__, executionId)
        , ExecutionId(executionId)
        , Database(database)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TForgetScriptExecutionOperationQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            DELETE
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;

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

        SetQueryResultHandler(&TForgetScriptExecutionOperationQueryActor::OnOperationDeleted, "Forget script execution operation");
        RunDataQuery(sql, &params);
    }

    void OnOperationDeleted() {
        SendResponse(Ydb::StatusIds::SUCCESS, {});

        TString sql = R"(
            -- TForgetScriptExecutionOperationQueryActor::OnOperationDeleted
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                MAX(result_set_id) AS max_result_set_id,
                MAX(row_id) AS max_row_id
            FROM `.metadata/result_sets`
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

        SetQueryResultHandler(&TForgetScriptExecutionOperationQueryActor::OnGetResultsInfo, "Get results info");
        RunDataQuery(sql, &params);
    }

    void OnGetResultsInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response for aggregation");
            return;
        }

        std::optional<i32> maxResultSetId = result.ColumnParser("max_result_set_id").GetOptionalInt32();
        if (!maxResultSetId) {
            Finish();
            return;
        }

        NumberRowsInBatch = std::max(MAX_NUMBER_ROWS_IN_BATCH / (*maxResultSetId + 1), 1);

        std::optional<i64> maxRowId = result.ColumnParser("max_row_id").GetOptionalInt64();
        if (!maxRowId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row id is not specified");
            return;
        }

        MaxRowId = *maxRowId;

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

        const i64 minRowId = MaxRowId - NumberRowsInBatch;
        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$min_row_id")
                .Int64(minRowId)
                .Build()
            .AddParam("$max_row_id")
                .Int64(MaxRowId)
                .Build();

        SetQueryResultHandler(&TForgetScriptExecutionOperationQueryActor::OnResultsDeleted, TStringBuilder() << "Delete script results in range (" << minRowId << "; " << MaxRowId << "]");
        RunDataQuery(sql, &params);
    }

    void OnResultsDeleted() {
        MaxRowId -= NumberRowsInBatch;
        if (MaxRowId < 0) {
            Finish();
            return;
        }

        DeleteScriptResults();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        SendResponse(status, std::move(issues));
    }

private:
    void SendResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        if (ResponseSent) {
            return;
        }
        ResponseSent = true;
        Send(Owner, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)));
    }

private:
    const TString ExecutionId;
    const TString Database;
    i64 NumberRowsInBatch = 0;
    i64 MaxRowId = 0;
    bool ResponseSent = false;
};

class TForgetScriptExecutionOperationActor : public TActorBootstrapped<TForgetScriptExecutionOperationActor> {
    using TForgetOperationRetryActor = TQueryRetryActor<TForgetScriptExecutionOperationQueryActor, TEvForgetScriptExecutionOperationResponse, TString, TString>;

public:
    TForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : Request(std::move(ev))
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

    void Bootstrap() {
        TString error;
        TMaybe<TString> executionId = NKqp::ScriptExecutionIdFromOperation(Request->Get()->OperationId, error);
        if (!executionId) {
            Reply(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        ExecutionId = *executionId;

        KQP_PROXY_LOG_D("Bootstrap. Start TCheckLeaseStatusActor");
        Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, ExecutionId, QueryServiceConfig, Counters));
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
                Reply(ev->Get()->Status, AddRootIssue("Check lease status", ev->Get()->Issues));
                return;
            }

            if (!ev->Get()->OperationStatus) {
                Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Operation is still running");
                return;
            }
        }

        KQP_PROXY_LOG_D("Lease check success. Start TForgetOperationRetryActor");
        Register(new TForgetOperationRetryActor(SelfId(), ExecutionId, Request->Get()->Database));
    }

    void Handle(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        Reply(ev->Get()->Status, AddRootIssue("Forget script execution operation", ev->Get()->Issues));
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TForgetScriptExecutionOperationActor] ExecutionId: " << ExecutionId << ". ";
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        if (!ExecutionEntryExists && status == Ydb::StatusIds::SUCCESS) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.AddIssue("No such execution");
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_D("Reply success");
        } else {
            KQP_PROXY_LOG_W("Reply " << status << ", issues: " << issues.ToOneLineString());
        }

        Send(Request->Sender, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, { NYql::TIssue(message) });
    }

private:
    const TEvForgetScriptExecutionOperation::TPtr Request;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    TString ExecutionId;
    bool ExecutionEntryExists = true;
};

NYql::TIssues ParseScriptExecutionIssues(NYdb::TResultSetParser& result) {
    NYql::TIssues issues;

    if (const auto issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument()) {
        issues = DeserializeIssues(*issuesSerialized);
    }

    if (const auto transientIssuesSerialized = result.ColumnParser("transient_issues").GetOptionalJsonDocument()) {
        issues.AddIssues(AddRootIssue("Previous query retries", DeserializeIssues(*transientIssuesSerialized)));
    }

    return issues;
}

class TGetScriptExecutionOperationQueryActor : public TQueryBase {
public:
    TGetScriptExecutionOperationQueryActor(const TString& database, const TString& executionId)
        : TQueryBase(__func__, executionId)
        , Database(database)
        , ExecutionId(executionId)
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
                transient_issues,
                stats,
                ast,
                ast_compressed,
                ast_compression_method
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_deadline,
                lease_generation,
                lease_state
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

        {   // Script execution info
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
                return;
            }

            if (const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32()) {
                OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            }

            if (const auto finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32()) {
                FinalizationStatus = static_cast<EFinalizationStatus>(*finalizationStatus);
            }

            Metadata.set_execution_id(ExecutionId);

            if (const auto executionStatus = result.ColumnParser("execution_status").GetOptionalInt32()) {
                Metadata.set_exec_status(static_cast<Ydb::Query::ExecStatus>(*executionStatus));
            }

            if (const auto sql = result.ColumnParser("query_text").GetOptionalUtf8()) {
                Metadata.mutable_script_content()->set_text(*sql);
            }

            if (const auto syntax = result.ColumnParser("syntax").GetOptionalInt32()) {
                Metadata.mutable_script_content()->set_syntax(static_cast<Ydb::Query::Syntax>(*syntax));
            }

            if (const auto executionMode = result.ColumnParser("execution_mode").GetOptionalInt32()) {
                Metadata.set_exec_mode(static_cast<Ydb::Query::ExecMode>(*executionMode));
            }

            if (const auto serializedStats = result.ColumnParser("stats").GetOptionalJsonDocument()) {
                NJson::TJsonValue statsJson;
                NJson::ReadJsonTree(*serializedStats, &statsJson);
                NProtobufJson::Json2Proto(statsJson, *Metadata.mutable_exec_stats(), NProtobufJson::TJson2ProtoConfig());
            }

            if (const auto plan = result.ColumnParser("plan").GetOptionalJsonDocument()) {
                Metadata.mutable_exec_stats()->set_query_plan(*plan);
            }

            std::optional<TString> ast;
            if (const std::optional<TString> astCompressionMethod = result.ColumnParser("ast_compression_method").GetOptionalUtf8()) {
                if (const std::optional<TString> astCompressed = result.ColumnParser("ast_compressed").GetOptionalString()) {
                    const NFq::TCompressor compressor(*astCompressionMethod);
                    ast = compressor.Decompress(*astCompressed);
                }
            } else {
                ast = result.ColumnParser("ast").GetOptionalUtf8();
            }
            if (ast) {
                Metadata.mutable_exec_stats()->set_query_ast(*ast);
            }

            Issues = ParseScriptExecutionIssues(result);

            if (const auto serializedMetas = result.ColumnParser("result_set_metas").GetOptionalJsonDocument()) {
                NJson::TJsonValue value;
                if (!NJson::ReadJsonTree(*serializedMetas, &value) || value.GetType() != NJson::JSON_ARRAY) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set meta is corrupted");
                    return;
                }

                for (auto i = 0; i < value.GetIntegerRobust(); ++i) {
                    const NJson::TJsonValue* metaValue;
                    value.GetValuePointer(i, &metaValue);
                    Y_ENSURE(metaValue);

                    NProtobufJson::Json2Proto(*metaValue, *Metadata.add_result_sets_meta());
                }
            }

            if (const auto runScriptActorIdString = result.ColumnParser("run_script_actor_id").GetOptionalUtf8()) {
                ScriptExecutionRunnerActorIdFromString(*runScriptActorIdString, RunScriptActorId);
            }
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (result.TryNextRow()) {
                const auto leaseDeadline = result.ColumnParser("lease_deadline").GetOptionalTimestamp();
                if (!leaseDeadline) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Lease deadline not found");
                    return;
                }

                const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
                if (!leaseGenerationInDatabase) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Lease generation not found");
                    return;
                }

                LeaseGeneration = *leaseGenerationInDatabase;
                LeaseStatus = static_cast<ELeaseState>(result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning)));

                if (*leaseDeadline < StartActorTime) {
                    LeaseExpired = true;
                    if (*LeaseStatus == ELeaseState::WaitRetry) {
                        RetryRequired = true;
                    } else {
                        FinalizationStatus = EFinalizationStatus::FS_ROLLBACK;
                    }
                }
            } else if (!OperationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state, lease not found for running query");
                return;
            }
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        bool ready = !!OperationStatus;
        if (FinalizationStatus || LeaseStatus && *LeaseStatus == ELeaseState::WaitRetry) {
            ready = false;
            OperationStatus = std::nullopt;
        }

        if (!OperationStatus || status != Ydb::StatusIds::SUCCESS) {
            OperationStatus = status;
        }

        if (issues) {
            auto newIssues = AddRootIssue("Get script execution operation info", issues);
            newIssues.AddIssues(AddRootIssue("Script execution issues", Issues));
            std::swap(Issues, newIssues);
        }

        Send(Owner, new TEvGetScriptExecutionOperationQueryResponse(
            ready, LeaseExpired,
            FinalizationStatus, RunScriptActorId,
            ExecutionId, *OperationStatus,
            std::move(Issues), std::move(Metadata),
            RetryRequired, LeaseGeneration
        ));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const TInstant StartActorTime;
    std::optional<Ydb::StatusIds::StatusCode> OperationStatus;
    std::optional<EFinalizationStatus> FinalizationStatus;
    std::optional<ELeaseState> LeaseStatus;
    i64 LeaseGeneration = 0;
    bool LeaseExpired = false;
    bool RetryRequired = false;
    TActorId RunScriptActorId;
    NYql::TIssues Issues;
    Ydb::Query::ExecuteScriptMetadata Metadata;
};

class TGetScriptExecutionOperationActor : public TCheckLeaseStatusActorBase {
public:
    TGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : TCheckLeaseStatusActorBase(__func__, ev->Get()->Database, "", queryServiceConfig, counters)
        , Request(std::move(ev))
    {}

    void OnBootstrap() override {
        TString error;
        const auto executionId = ScriptExecutionIdFromOperation(Request->Get()->OperationId, error);
        if (!executionId) {
            Reply(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        ExecutionId = *executionId;

        KQP_PROXY_LOG_D("Bootstrap. Start TGetScriptExecutionOperationQueryActor");
        Register(new TGetScriptExecutionOperationQueryActor(Database, ExecutionId));
        Become(&TGetScriptExecutionOperationActor::StateFunc);
    }

    void OnLeaseVerified() override {
        Reply();
    }

    void OnScriptExecutionFinished(bool alreadyFinalized, bool waitRetry, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status != Ydb::StatusIds::SUCCESS) {
            Reply(status, std::move(issues));
            return;
        }

        // Otherwise final status and issues are unknown, the operation must be repeated
        if (!alreadyFinalized && !waitRetry) {
            Response->Get()->Ready = true;
            Response->Get()->Status = GetOperationStatus();
            Response->Get()->Issues = GetIssues();
            Response->Get()->Metadata.set_exec_status(GetExecStatus());
        }

        Reply();
    }

    void OnScriptExecutionRestarted(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status != Ydb::StatusIds::SUCCESS) {
            Reply(status, std::move(issues));
            return;
        }

        Response->Get()->Ready = false;
        Response->Get()->Status = Ydb::StatusIds::SUCCESS;
        Response->Get()->Metadata.set_exec_status(Ydb::Query::ExecStatus::EXEC_STATUS_STARTING);

        Reply();
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvGetScriptExecutionOperationQueryResponse, Handle);
    )

    void Handle(TEvGetScriptExecutionOperationQueryResponse::TPtr& ev) {
        Response = std::move(ev);
        KQP_PROXY_LOG_T("Extracted script execution operation"
            << ", status: " << Response->Get()->Status
            << ", issues: " << Response->Get()->Issues.ToOneLineString()
            << ", metadata: " << Response->Get()->Metadata.DebugString());

        const auto& event = *Response->Get();
        if (event.RetryRequired) {
            RestartScriptExecution(event.LeaseGeneration);
        } else if (event.LeaseExpired) {
            StartLeaseChecking(event.RunScriptActorId, event.LeaseGeneration);
        } else if (const auto finalizationStatus = event.FinalizationStatus) {
            TMaybe<Ydb::Query::ExecStatus> execStatus;
            if (Response->Get()->Ready) {
                execStatus = Response->Get()->Metadata.exec_status();
            }
            StartScriptFinalization(*Response->Get()->FinalizationStatus, Response->Get()->Status, execStatus, Response->Get()->Issues, event.LeaseGeneration);
        } else {
            Reply();
        }
    }

    void Reply() {
        KQP_PROXY_LOG_D("Reply success");
        TMaybe<google::protobuf::Any> metadata;
        metadata.ConstructInPlace().PackFrom(Response->Get()->Metadata);
        Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(Response->Get()->Ready, Response->Get()->Status, std::move(Response->Get()->Issues), std::move(metadata)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        KQP_PROXY_LOG_W("Reply " << status << ", issues: " << issues.ToOneLineString());
        Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(status, std::move(issues)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, {NYql::TIssue(message)});
    }

private:
    TEvGetScriptExecutionOperation::TPtr Request;
    TEvGetScriptExecutionOperationQueryResponse::TPtr Response;
};

class TListScriptExecutionOperationsQuery : public TQueryBase {
public:
    TListScriptExecutionOperationsQuery(const TString& database, const TString& pageToken, ui64 pageSize)
        : TQueryBase(__func__, "")
        , Database(database)
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

    static TString MakePageToken(TInstant ts, const std::string& executionId) {
        return TStringBuilder() << ts.MicroSeconds() << '|' << executionId;
    }

    void OnRunQuery() override {
        SetOperationInfo(OperationName, Owner.ToString());

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
                issues,
                transient_issues
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
            const auto executionId = result.ColumnParser("execution_id").GetOptionalUtf8();
            if (!executionId) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Execution id not found");
                return;
            }

            const auto creationTs = result.ColumnParser("start_ts").GetOptionalTimestamp();
            if (!creationTs) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Start ts not found");
                return;
            }

            if (Operations.size() >= PageSize) {
                NextPageToken = MakePageToken(*creationTs, *executionId);
                break;
            }

            const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

            Ydb::Query::ExecuteScriptMetadata metadata;
            metadata.set_execution_id(*executionId);

            if (const auto executionStatus = result.ColumnParser("execution_status").GetOptionalInt32()) {
                metadata.set_exec_status(static_cast<Ydb::Query::ExecStatus>(*executionStatus));
            }

            if (const auto sql = result.ColumnParser("query_text").GetOptionalUtf8()) {
                metadata.mutable_script_content()->set_text(*sql);
            }

            if (const auto syntax = result.ColumnParser("syntax").GetOptionalInt32()) {
                metadata.mutable_script_content()->set_syntax(static_cast<Ydb::Query::Syntax>(*syntax));
            }

            if (const auto executionMode = result.ColumnParser("execution_mode").GetOptionalInt32()) {
                metadata.set_exec_mode(static_cast<Ydb::Query::ExecMode>(*executionMode));
            }

            Ydb::Operations::Operation op;
            op.set_id(ScriptExecutionOperationFromExecutionId(*executionId));
            op.set_ready(operationStatus.has_value());
            if (operationStatus) {
                op.set_status(static_cast<Ydb::StatusIds::StatusCode>(*operationStatus));
            }
            for (const NYql::TIssue& issue : ParseScriptExecutionIssues(result)) {
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
    TListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : Request(std::move(ev))
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

    void Bootstrap() {
        KQP_PROXY_LOG_D("Bootstrap. Start TListScriptExecutionOperationsQuery");
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
                KQP_PROXY_LOG_D("ExecutionId: " << metadata.execution_id() << ", start TCheckLeaseStatusActor #" << i);
                Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, metadata.execution_id(), QueryServiceConfig, Counters, i));
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
            KQP_PROXY_LOG_W("Lease check failed #" << ev->Cookie);
            Response->Get()->Status = ev->Get()->Status;
            Response->Get()->Issues = std::move(ev->Get()->Issues);
            Response->Get()->NextPageToken.clear();
            Response->Get()->Operations.clear();
            Reply();
            return;
        }

        KQP_PROXY_LOG_D("Lease check success #" << ev->Cookie);

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

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TListScriptExecutionOperationsActor] ActorId: " << SelfId() << ". ";
    }

    void Reply() {
        KQP_PROXY_LOG_D("Reply " << Response->Get()->Status << ", issues: " << Response->Get()->Issues.ToOneLineString());
        Forward(Response, Request->Sender);
        PassAway();
    }

private:
    const TEvListScriptExecutionOperations::TPtr Request;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    TEvListScriptExecutionOperationsResponse::TPtr Response;
    ui64 OperationsToCheck = 0;
};

class TCancelScriptExecutionOperationActor : public TActorBootstrapped<TCancelScriptExecutionOperationActor> {
    using TBase = TActorBootstrapped<TCancelScriptExecutionOperationActor>;

public:
    TCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : Request(std::move(ev))
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

    void Bootstrap() {
        TString error;
        const auto executionId = NKqp::ScriptExecutionIdFromOperation(Request->Get()->OperationId, error);
        if (!executionId) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, error);
        }

        ExecutionId = *executionId;

        KQP_PROXY_LOG_D("Bootstrap. Start TCheckLeaseStatusActor");
        Become(&TCancelScriptExecutionOperationActor::StateFunc);
        Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, ExecutionId, QueryServiceConfig, Counters));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionResponse, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
    )

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            RunScriptActor = ev->Get()->RunScriptActorId;
            KQP_PROXY_LOG_D("Check lease success, RunScriptActor: " << RunScriptActor);
            if (ev->Get()->OperationStatus) {
                Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Script execution operation is already finished");
            } else {
                if (CancelSent) { // We have not found the actor, but after it status of the operation is not defined, something strage happened.
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, "Failed to cancel script execution operation after undelivered event");
                } else {
                    SendCancelToRunScriptActor(); // The race: operation is still working, but it can finish before it receives cancel signal. Try to cancel first and then maybe check its status.
                }
            }
        } else {
            KQP_PROXY_LOG_W("Check lease failed");
            Reply(ev->Get()->Status, std::move(ev->Get()->Issues)); // Error getting operation in database.
        }
    }

    void SendCancelToRunScriptActor() {
        KQP_PROXY_LOG_D("Send cancel request to RunScriptActor: " << RunScriptActor);
        ui64 flags = IEventHandle::FlagTrackDelivery;
        if (RunScriptActor.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = RunScriptActor.NodeId();
        }
        Send(RunScriptActor, new TEvKqp::TEvCancelScriptExecutionRequest(), flags);
        CancelSent = true;
    }

    void Handle(TEvKqp::TEvCancelScriptExecutionResponse::TPtr& ev) {
        KQP_PROXY_LOG_D("Got cancel response from RunScriptActor: " << RunScriptActor);
        NYql::TIssues issues;
        NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
        Reply(ev->Get()->Record.GetStatus(), std::move(issues));
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == TEvents::TEvUndelivered::ReasonActorUnknown) { // The actor probably had finished before our cancel message arrived.
            KQP_PROXY_LOG_D("Got delivery problem to RunScriptActor: " << RunScriptActor << ", maybe already finished");
            Register(new TCheckLeaseStatusActor(SelfId(), Request->Get()->Database, ExecutionId, QueryServiceConfig, Counters)); // Check if the operation has finished.
        } else {
            Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver cancel request to destination");
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver cancel request to destination");
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TCancelScriptExecutionOperationActor] ExecutionId: " << ExecutionId << ". ";
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        KQP_PROXY_LOG_D("Reply " << status << ", issues: " << issues.ToOneLineString());
        Send(Request->Sender, new TEvCancelScriptExecutionOperationResponse(status, std::move(issues)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        Reply(status, std::move(issues));
    }

private:
    const TEvCancelScriptExecutionOperation::TPtr Request;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    TString ExecutionId;
    TActorId RunScriptActor;
    TMaybe<ui32> SubscribedOnSession;
    bool CancelSent = false;
};

class TSaveScriptExecutionResultMetaQuery : public TQueryBase {
public:
    TSaveScriptExecutionResultMetaQuery(const TString& database, const TString& executionId, const TString& serializedMetas)
        : TQueryBase(__func__, executionId)
        , Database(database)
        , ExecutionId(executionId)
        , SerializedMetas(serializedMetas)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptExecutionResultMetaQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_metas AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET result_set_metas = $result_set_metas
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
            .AddParam("$result_set_metas")
                .JsonDocument(SerializedMetas)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptResultMetaFinished(status, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const TString SerializedMetas;
};

class TSaveScriptExecutionResultQuery : public TQueryBase {
public:
    TSaveScriptExecutionResultQuery(const TString& database, const TString& executionId, i32 resultSetId,
        std::optional<TInstant> expireAt, i64 firstRow, i64 accumulatedSize, Ydb::ResultSet resultSet)
        : TQueryBase(__func__, executionId)
        , Database(database)
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
            SELECT
                $database AS database,
                $execution_id AS execution_id,
                $result_set_id AS result_set_id,
                $expire_at AS expire_at,
                T.row_id AS row_id,
                T.result_set AS result_set,
                T.accumulated_size AS accumulated_size
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

        param.BeginList();

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
        Send(Owner, new TEvSaveScriptResultPartFinished(status, SavedSize, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetId;
    const std::optional<TInstant> ExpireAt;
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
    TSaveScriptExecutionResultActor(const TActorId& replyActorId, const TString& database,
        const TString& executionId, i32 resultSetId, std::optional<TInstant> expireAt,
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
        KQP_PROXY_LOG_D("Start saving rows range [" << FirstRow << "; " << FirstRow + numberRows << ")");
        Register(new TQueryRetryActor<TSaveScriptExecutionResultQuery, TEvSaveScriptResultPartFinished, TString, TString, i32, std::optional<TInstant>, i64, i64, Ydb::ResultSet>(SelfId(), Database, ExecutionId, ResultSetId, ExpireAt, FirstRow, AccumulatedSize, ResultSets.back()));

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
            KQP_PROXY_LOG_W("Failed to save result part");
            Reply(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        KQP_PROXY_LOG_D("Result part successfully saved");
        AccumulatedSize += ev->Get()->SavedSize;

        StartSaveResultQuery();
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TSaveScriptExecutionResultActor] ExecutionId: " << ExecutionId << " ResultSetId: " << ResultSetId << ". ";
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        KQP_PROXY_LOG_D("Reply " << status << ", issues: " << issues.ToOneLineString());
        Send(ReplyActorId, new TEvSaveScriptResultFinished(status, ResultSetId, std::move(issues)));
        PassAway();
    }

private:
    const TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetId;
    const std::optional<TInstant> ExpireAt;
    i64 FirstRow;
    i64 AccumulatedSize;
    NFq::TRowsProtoSplitter RowsSplitter;
    TVector<Ydb::ResultSet> ResultSets;
};

class TGetScriptExecutionResultQueryActor : public TQueryBase {
public:
    TGetScriptExecutionResultQueryActor(const TString& database, const TString& executionId, i32 resultSetIndex,
        i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant deadline)
        : TQueryBase(__func__, executionId)
        , Database(database)
        , ExecutionId(executionId)
        , ResultSetIndex(resultSetIndex)
        , Offset(offset)
        , RowsLimit(rowsLimit)
        , SizeLimit(sizeLimit)
        , Deadline(rowsLimit ? TInstant::Max() : deadline)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TGetScriptExecutionResultQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                result_set_metas,
                operation_status,
                issues,
                transient_issues,
                end_ts,
                meta
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

        SetQueryResultHandler(&TGetScriptExecutionResultQueryActor::OnGetResultsInfo, "Get results info");
        RunDataQuery(sql, &params);
    }

    void OnGetResultsInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Script execution not found");
            return;
        }

        const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

        if (operationStatus) {
            const auto serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
            if (!serializedMeta) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation metainformation");
                return;
            }

            NJson::TJsonValue serializedMetaJson;
            if (!NJson::ReadJsonTree(*serializedMeta, &serializedMetaJson)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Operation metainformation is corrupted");
                return;
            }

            NKikimrKqp::TScriptExecutionOperationMeta meta;
            DeserializeBinaryProto(serializedMetaJson, meta);

            const auto endTs = result.ColumnParser("end_ts").GetOptionalTimestamp();
            if (!endTs) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation end timestamp");
                return;
            }

            const auto resultsTtl = GetDuration(meta.GetResultsTtl());
            if (resultsTtl && (*endTs + resultsTtl) < TInstant::Now()){
                Finish(Ydb::StatusIds::NOT_FOUND, "Results are expired");
                return;
            }
        }

        const auto serializedMetas = result.ColumnParser("result_set_metas").GetOptionalJsonDocument();

        if (!serializedMetas) {
            if (!operationStatus) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Result is not ready");
                return;
            }

            const auto operationStatusCode = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            if (operationStatusCode != Ydb::StatusIds::SUCCESS) {
                NYql::TIssue rootIssue("Script execution failed without results");
                for (const auto& issue : ParseScriptExecutionIssues(result)) {
                    rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
                }
                Finish(operationStatusCode, NYql::TIssues{rootIssue});
                return;
            }

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
        Y_ENSURE(metaValue);

        Ydb::Query::Internal::ResultSetMeta meta;
        NProtobufJson::Json2Proto(*metaValue, meta);

        if (!operationStatus) {
            if (!meta.enabled_runtime_results()) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Results are not ready");
                return;
            }
            HasMoreResults = !meta.finished();
            NumberOfSavedRows = meta.number_rows();
        }

        *ResultSet.mutable_columns() = meta.columns();
        ResultSet.set_truncated(meta.truncated());
        ResultSetSize = ResultSet.ByteSizeLong();

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
            DECLARE $max_row_id AS Int64;
            DECLARE $limit AS Uint64;

            SELECT
                database,
                execution_id,
                result_set_id,
                row_id,
                result_set
            FROM `.metadata/result_sets`
            WHERE database = $database
              AND execution_id = $execution_id
              AND result_set_id = $result_set_id
              AND row_id >= $offset
              AND row_id < $max_row_id
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
            .AddParam("$max_row_id")
                .Int64(NumberOfSavedRows)
                .Build()
            .AddParam("$limit")
                .Uint64(RowsLimit ? RowsLimit + 1 : std::numeric_limits<ui64>::max())
                .Build();

        SetQueryResultHandler(&TGetScriptExecutionResultQueryActor::OnQueryResult, TStringBuilder() << "Fetch results for offset " << Offset);
        RunStreamQuery(sql, &params, SizeLimit ? SizeLimit : 60_MB);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser result(resultSet);
        while (result.TryNextRow()) {
            const std::optional<TString> serializedRow = result.ColumnParser("result_set").GetOptionalString();
            if (!serializedRow) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is null");
                return;
            }

            if (serializedRow->empty()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is empty");
                return;
            }

            i64 rowSize = serializedRow->size();
            if (SizeLimit && ResultSet.rows_size() && ResultSetSize + rowSize + AdditionalRowSize > SizeLimit) {
                CancelFetchQuery();
                return;
            }

            if (RowsLimit && ResultSet.rows_size() >= RowsLimit) {
                CancelFetchQuery();
                return;
            }

            ResultSetSize += rowSize;
            if (!ResultSet.add_rows()->ParseFromString(*serializedRow)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is corrupted");
                return;
            }

            // Initialize AdditionalRowSize
            if (ResultSet.rows_size() == 1) {
                AdditionalRowSize = static_cast<i64>(ResultSet.ByteSizeLong()) - ResultSetSize;
            }
            ResultSetSize += AdditionalRowSize;
        }

        if (TInstant::Now() + TDuration::Seconds(5) + GetAverageTime() >= Deadline) {
            CancelFetchQuery();
        }
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvFetchScriptResultsResponse(status, std::move(ResultSet), HasMoreResults, std::move(issues)));
        } else {
            Send(Owner, new TEvFetchScriptResultsResponse(status, std::nullopt, true, std::move(issues)));
        }
    }

private:
    void CancelFetchQuery() {
        HasMoreResults = true;
        CancelStreamQuery();
    }

private:
    const TString Database;
    const TString ExecutionId;
    const i32 ResultSetIndex;
    const i64 Offset;
    const i64 RowsLimit;
    const i64 SizeLimit;
    const TInstant Deadline;

    i64 NumberOfSavedRows = std::numeric_limits<i64>::max();
    i64 ResultSetSize = 0;
    i64 AdditionalRowSize = 0;
    Ydb::ResultSet ResultSet;
    bool HasMoreResults = false;
};

class TGetScriptExecutionResultActor : public TActorBootstrapped<TGetScriptExecutionResultActor> {
public:
    TGetScriptExecutionResultActor(const TActorId& replyActorId, const TString& database, const TString& executionId,
        i32 resultSetIndex, i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant operationDeadline)
        : ReplyActorId(replyActorId)
        , Database(database)
        , ExecutionId(executionId)
        , ResultSetIndex(resultSetIndex)
        , Offset(offset)
        , RowsLimit(rowsLimit)
        , SizeLimit(sizeLimit)
        , OperationDeadline(operationDeadline)
    {}

    void Bootstrap() {
        if (RowsLimit < 0 || SizeLimit < 0) {
            Send(ReplyActorId, new TEvFetchScriptResultsResponse(Ydb::StatusIds::BAD_REQUEST, std::nullopt, true, {NYql::TIssue("Result rows limit and size limit should not be negative")}));
            PassAway();
            return;
        }

        Register(new TGetScriptExecutionResultQueryActor(Database, ExecutionId, ResultSetIndex, Offset, RowsLimit, SizeLimit, OperationDeadline));
        Become(&TGetScriptExecutionResultActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvFetchScriptResultsResponse, Handle);
    )

    void Handle(TEvFetchScriptResultsResponse::TPtr& ev) {
        Forward(ev, ReplyActorId);
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
        : TQueryBase(__func__, request.ExecutionId)
        , Request(request)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TSaveScriptExternalEffectActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $customer_supplied_id AS Text;
            DECLARE $script_sinks AS JsonDocument;
            DECLARE $script_secret_names AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET
                customer_supplied_id = $customer_supplied_id,
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
            SerializeBinaryProto(sinks[i], jsonArray[i]);
        }

        NJsonWriter::TBuf serializedSinks;
        serializedSinks.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

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
        serializedSecretNames.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

        return serializedSecretNames.Str();
    }

private:
    const TEvSaveScriptExternalEffectRequest::TDescription Request;
};

struct LeaseFinalizationInfo {
    TString Sql;
    TDuration Backoff;
    ELeaseState NewLeaseState = ELeaseState::ScriptFinalizing;
};

LeaseFinalizationInfo GetLeaseFinalizationSql(TInstant now, Ydb::StatusIds::StatusCode status, NKikimrKqp::TScriptExecutionRetryState& retryState, NYql::TIssues& issues) {
    const auto& policy = TRetryPolicyItem::FromProto(status, retryState);
    TRetryLimiter retryLimiter(
        retryState.GetRetryCounter(),
        NProtoInterop::CastFromProto(retryState.GetRetryCounterUpdatedAt()),
        retryState.GetRetryRate()
    );

    const bool retry = retryLimiter.UpdateOnRetry(TInstant::Now(), policy);
    retryState.SetRetryCounter(retryLimiter.RetryCount);
    *retryState.MutableRetryCounterUpdatedAt() = NProtoInterop::CastToProto(now);
    retryState.SetRetryRate(retryLimiter.RetryRate);

    if (retry) {
        issues = AddRootIssue(TStringBuilder()
            << "Script execution operation failed with code " << Ydb::StatusIds::StatusCode_Name(status)
            << " and will be restarted (RetryCount: " << retryLimiter.RetryCount << ", Backoff: " << retryLimiter.Backoff << ", RetryRate: " << retryLimiter.RetryRate << ")"
            << " at " << now, issues);

        return {
            .Sql = R"(
                UPSERT INTO `.metadata/script_execution_leases` (
                    database, execution_id, lease_deadline, lease_state
                ) VALUES (
                    $database, $execution_id, $retry_deadline, $lease_state
                );
            )",
            .Backoff = retryLimiter.Backoff,
            .NewLeaseState = ELeaseState::WaitRetry
        };
    } else {
        if (retryState.RetryPolicyMappingSize()) {
            TStringBuilder finalIssue;
            finalIssue << "Script execution operation failed with code " << Ydb::StatusIds::StatusCode_Name(status);
            if (policy.RetryCount) {
                finalIssue << " (" << retryLimiter.LastError << ")";
            }
            issues = AddRootIssue(finalIssue << " at " << now, issues);
        }

        return {
            .Sql = R"(
                DELETE FROM `.metadata/script_execution_leases`
                WHERE database = $database AND execution_id = $execution_id;
            )"
        };
    }
}

class TSaveScriptFinalStatusActor : public TQueryBase {
public:
    explicit TSaveScriptFinalStatusActor(const TEvScriptFinalizeRequest::TDescription& request)
        : TQueryBase(__func__, request.ExecutionId)
        , Request(request)
        , Response(std::make_unique<TEvSaveScriptFinalStatusResponse>())
    {}

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
                script_secret_names,
                retry_state
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_generation,
                lease_state
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

        SetQueryResultHandler(&TSaveScriptFinalStatusActor::OnGetInfo, "Get operation info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {   // Execution info
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
                return;
            }

            const auto finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
            if (finalizationStatus) {
                if (Request.FinalizationStatus != *finalizationStatus) {
                    Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Execution already have different finalization status");
                    return;
                }
                Response->ApplicateScriptExternalEffectRequired = true;
            }

            if (const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32()) {
                FinalStatusAlreadySaved = true;
                Response->OperationAlreadyFinalized = !finalizationStatus;
                CommitTransaction();
                return;
            }

            if (const auto customerSuppliedId = result.ColumnParser("customer_supplied_id").GetOptionalUtf8()) {
                Response->CustomerSuppliedId = *customerSuppliedId;
            }

            if (const auto userToken = result.ColumnParser("user_token").GetOptionalUtf8()) {
                Response->UserToken = *userToken;
            }

            if (SerializedSinks = result.ColumnParser("script_sinks").GetOptionalJsonDocument()) {
                NJson::TJsonValue value;
                if (!NJson::ReadJsonTree(*SerializedSinks, &value) || value.GetType() != NJson::JSON_ARRAY) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script sinks are corrupted");
                    return;
                }

                for (auto i = 0; i < value.GetIntegerRobust(); ++i) {
                    const NJson::TJsonValue* serializedSink;
                    value.GetValuePointer(i, &serializedSink);
                    Y_ENSURE(serializedSink);

                    NKqpProto::TKqpExternalSink sink;
                    DeserializeBinaryProto(*serializedSink, sink);
                    Response->Sinks.push_back(sink);
                }
            }

            if (SerializedSecretNames = result.ColumnParser("script_secret_names").GetOptionalJsonDocument()) {
                NJson::TJsonValue value;
                if (!NJson::ReadJsonTree(*SerializedSecretNames, &value) || value.GetType() != NJson::JSON_ARRAY) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script secret names are corrupted");
                    return;
                }

                for (auto i = 0; i < value.GetIntegerRobust(); i++) {
                    const NJson::TJsonValue* serializedSecretName;
                    value.GetValuePointer(i, &serializedSecretName);
                    Y_ENSURE(serializedSecretName);

                    Response->SecretNames.push_back(serializedSecretName->GetString());
                }
            }

            if (const auto serializedRetryState = result.ColumnParser("retry_state").GetOptionalJsonDocument()) {
                NProtobufJson::Json2Proto(TStringBuf(*serializedRetryState), RetryState, NProtobufJson::TJson2ProtoConfig());
            }

            const auto serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
            if (!serializedMeta) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation metainformation");
                return;
            }

            NJson::TJsonValue serializedMetaJson;
            if (!NJson::ReadJsonTree(*serializedMeta, &serializedMetaJson)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Operation metainformation is corrupted");
                return;
            }

            NKikimrKqp::TScriptExecutionOperationMeta meta;
            DeserializeBinaryProto(serializedMetaJson, meta);
            OperationTtl = GetDuration(meta.GetOperationTtl());
            LeaseDuration = GetDuration(meta.GetLeaseDuration());
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state, expected existing lease before finalization (maybe already finalized)");
                return;
            }

            const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
            if (!leaseGenerationInDatabase) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
                return;
            }

            if (Request.LeaseGeneration != *leaseGenerationInDatabase) {
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Lease was lost, expected generation: " << Request.LeaseGeneration << ", got: " << *leaseGenerationInDatabase);
                return;
            }

            const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning));
            if (!IsIn({ELeaseState::ScriptRunning, ELeaseState::ScriptFinalizing}, static_cast<ELeaseState>(leaseState))) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Lease is not in expected state: " << leaseState);
                return;
            }
        }

        Response->ApplicateScriptExternalEffectRequired = Response->ApplicateScriptExternalEffectRequired || HasExternalEffect();
        FinishScriptExecution();
    }

    void FinishScriptExecution() {
        auto sql = TStringBuilder() << R"(
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
            DECLARE $script_sinks AS Optional<JsonDocument>;
            DECLARE $script_secret_names AS Optional<JsonDocument>;
            DECLARE $applicate_script_external_effect_required AS Bool;
            DECLARE $retry_state AS JsonDocument;
            DECLARE $retry_deadline AS Timestamp;
            DECLARE $lease_state AS Int32;

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
                script_sinks = IF($applicate_script_external_effect_required, $script_sinks, NULL),
                script_secret_names = IF($applicate_script_external_effect_required, $script_secret_names, NULL),
                retry_state = $retry_state
            WHERE database = $database AND execution_id = $execution_id;
        )";

        TInstant retryDeadline = TInstant::Now();
        ELeaseState leaseState = ELeaseState::ScriptFinalizing;
        if (!Response->ApplicateScriptExternalEffectRequired) {
            const auto leaseInfo = GetLeaseFinalizationSql(retryDeadline, Request.OperationStatus, RetryState, Request.Issues);
            sql << leaseInfo.Sql;
            retryDeadline += leaseInfo.Backoff;
            leaseState = leaseInfo.NewLeaseState;
            Response->WaitRetry = leaseInfo.NewLeaseState == ELeaseState::WaitRetry;
        } else {
            retryDeadline += LeaseDuration;
            sql <<  R"(
                UPSERT INTO `.metadata/script_execution_leases` (
                    database, execution_id, lease_deadline, lease_state
                ) VALUES (
                    $database, $execution_id, $retry_deadline, $lease_state
                );
            )";
        }

        TString serializedStats = "{}";
        if (Request.QueryStats) {
            NJson::TJsonValue statsJson;
            Ydb::TableStats::QueryStats queryStats;
            NGRpcService::FillQueryStats(queryStats, *Request.QueryStats);
            NProtobufJson::Proto2Json(queryStats, statsJson, NProtobufJson::TProto2JsonConfig());
            TStringStream statsStream;
            NJson::WriteJson(&statsStream, &statsJson, {
                .DoubleNDigits = 17,
                .FloatToStringMode = PREC_NDIGITS,
                .ValidateUtf8 = false,
                .WriteNanAsString = true,
            });
            serializedStats = statsStream.Str();
        }

        std::optional<TString> ast;
        std::optional<TString> astCompressed;
        std::optional<TString> astCompressionMethod;
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
            .AddParam("$script_sinks")
                .OptionalJsonDocument(SerializedSinks)
                .Build()
            .AddParam("$script_secret_names")
                .OptionalJsonDocument(SerializedSecretNames)
                .Build()
            .AddParam("$applicate_script_external_effect_required")
                .Bool(Response->ApplicateScriptExternalEffectRequired)
                .Build()
            .AddParam("$retry_state")
                .JsonDocument(NProtobufJson::Proto2Json(RetryState, NProtobufJson::TProto2JsonConfig()))
                .Build()
            .AddParam("$retry_deadline")
                .Timestamp(retryDeadline)
                .Build()
            .AddParam("$lease_state")
                .Int32(static_cast<i32>(leaseState))
                .Build();

        SetQueryResultHandler(&TSaveScriptFinalStatusActor::OnQueryResult, "Update final status");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (!FinalStatusAlreadySaved) {
            KQP_PROXY_LOG_D("Finish script execution operation"
                << ". Status: " << Ydb::StatusIds::StatusCode_Name(Request.OperationStatus)
                << ". Issues: " << Request.Issues.ToOneLineString());
        }

        Response->Status = status;
        Response->Issues = std::move(issues);

        Send(Owner, Response.release());
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TSaveScriptFinalStatusActor] ExecutionId: " << Request.ExecutionId << ". ";
    }

    bool HasExternalEffect() const {
        return !Response->Sinks.empty();
    }

private:
    TEvScriptFinalizeRequest::TDescription Request;
    std::unique_ptr<TEvSaveScriptFinalStatusResponse> Response;

    bool FinalStatusAlreadySaved = false;

    TDuration OperationTtl;
    TDuration LeaseDuration;
    std::optional<TString> SerializedSinks;
    std::optional<TString> SerializedSecretNames;
    NKikimrKqp::TScriptExecutionRetryState RetryState;
};

class TScriptFinalizationFinisherActor : public TQueryBase {
public:
    TScriptFinalizationFinisherActor(const TString& executionId, const TString& database,
        std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues,
        i64 leaseGeneration)
        : TQueryBase(__func__, executionId)
        , ExecutionId(executionId)
        , Database(database)
        , OperationStatus(operationStatus)
        , OperationIssues(AddRootIssue("Script finalization failed", operationIssues))
        , LeaseGeneration(leaseGeneration)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptFinalizationFinisherActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                finalization_status,
                issues,
                retry_state
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_generation,
                lease_state
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

        SetQueryResultHandler(&TScriptFinalizationFinisherActor::OnGetInfo, "Get operation info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (result.TryNextRow()) {
                const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
                if (!leaseGenerationInDatabase) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
                    return;
                }

                if (LeaseGeneration != *leaseGenerationInDatabase) {
                    Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Lease was lost, expected generation: " << LeaseGeneration << ", got: " << *leaseGenerationInDatabase);
                    return;
                }

                const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning));
                if (!IsIn({ELeaseState::ScriptRunning, ELeaseState::ScriptFinalizing}, static_cast<ELeaseState>(leaseState))) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Lease is not in expected state: " << leaseState);
                    return;
                }
            }
        }

        {   // Execution info
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "No such execution");
                return;
            }

            const auto finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
            if (!finalizationStatus) {
                AlreadyFinished = true;
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Already finished");
                return;
            }

            if (const auto currentStatus = result.ColumnParser("operation_status").GetOptionalInt32()) {
                const auto status = static_cast<Ydb::StatusIds::StatusCode>(*currentStatus);
                if (!OperationStatus || status != Ydb::StatusIds::SUCCESS) {
                    OperationStatus = status;
                }
            }
            if (!OperationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Operation status not found after finalization");
                return;
            }

            const auto executionStatus = result.ColumnParser("execution_status").GetOptionalInt32();
            if (!executionStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Execution status not found after finalization");
                return;
            }

            ExecutionStatus = static_cast<Ydb::Query::ExecStatus>(*executionStatus);

            if (const auto serializedRetryState = result.ColumnParser("retry_state").GetOptionalJsonDocument()) {
                NProtobufJson::Json2Proto(TStringBuf(*serializedRetryState), RetryState, NProtobufJson::TJson2ProtoConfig());
            }

            if (const auto issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument()) {
                OperationIssues.AddIssues(DeserializeIssues(*issuesSerialized));
            }
        }

        UpdateOperationFinalStatus();
    }

    void UpdateOperationFinalStatus() {
        auto sql = TStringBuilder() << R"(
            -- TScriptFinalizationFinisherActor::UpdateOperationFinalStatus
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
            DECLARE $issues AS JsonDocument;
            DECLARE $retry_state AS JsonDocument;
            DECLARE $retry_deadline AS Timestamp;

            UPDATE `.metadata/script_executions`
            SET
                operation_status = $operation_status,
                execution_status = $execution_status,
                finalization_status = NULL,
                issues = $issues,
                script_sinks = NULL,
                customer_supplied_id = NULL,
                script_secret_names = NULL,
                retry_state = $retry_state
            WHERE database = $database AND execution_id = $execution_id;
        )";

        Y_ENSURE(OperationStatus);

        TInstant retryDeadline = TInstant::Now();
        const auto leaseInfo = GetLeaseFinalizationSql(retryDeadline, *OperationStatus, RetryState, OperationIssues);
        sql << leaseInfo.Sql;
        retryDeadline += leaseInfo.Backoff;
        WaitRetry = leaseInfo.NewLeaseState == ELeaseState::WaitRetry;

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
                .Int32(ExecutionStatus)
                .Build()
            .AddParam("$issues")
                .JsonDocument(SerializeIssues(OperationIssues))
                .Build()
            .AddParam("$retry_state")
                .JsonDocument(NProtobufJson::Proto2Json(RetryState, NProtobufJson::TProto2JsonConfig()))
                .Build()
            .AddParam("$retry_deadline")
                .Timestamp(retryDeadline)
                .Build()
            .AddParam("$lease_state")
                .Int32(static_cast<i32>(leaseInfo.NewLeaseState))
                .Build();

        SetQueryResultHandler(&TScriptFinalizationFinisherActor::OnQueryResult, "Update final status");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (!OperationStatus) {
            OperationStatus = status;
        }

        if (issues) {
            OperationIssues.AddIssues(AddRootIssue(TStringBuilder() << "Update final status failed " << status, issues));
        }

        Send(Owner, new TEvScriptExecutionFinished(AlreadyFinished, WaitRetry, *OperationStatus, std::move(OperationIssues)));
    }

private:
    const TString ExecutionId;
    const TString Database;
    std::optional<Ydb::StatusIds::StatusCode> OperationStatus;
    Ydb::Query::ExecStatus ExecutionStatus = Ydb::Query::EXEC_STATUS_UNSPECIFIED;
    NYql::TIssues OperationIssues;
    NKikimrKqp::TScriptExecutionRetryState RetryState;
    const i64 LeaseGeneration;
    bool AlreadyFinished = false;
    bool WaitRetry = false;
};

class TScriptProgressActor : public TQueryBase {
public:
    TScriptProgressActor(const TString& database, const TString& executionId, const TString& queryPlan)
        : TQueryBase(__func__, executionId)
        , Database(database)
        , ExecutionId(executionId)
        , QueryPlan(queryPlan)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            -- TScriptProgressActor::OnRunQuery
            DECLARE $execution_id AS Text;
            DECLARE $database AS Text;
            DECLARE $plan AS JsonDocument;
            DECLARE $execution_status AS Int32;

            UPSERT INTO `.metadata/script_executions` (
                execution_id, database, plan, execution_status
            ) VALUES (
                $execution_id, $database, $plan, $execution_status
            );
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
                .Build()
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_RUNNING)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode, NYql::TIssues&&) override {
    }

private:
    const TString Database;
    const TString ExecutionId;
    const TString QueryPlan;
};

class TListExpiredLeasesQueryActor : public TQueryBase {
public:
    TListExpiredLeasesQueryActor()
        : TQueryBase(__func__, "")
        , LeaseDeadline(TInstant::Now())
    {}

    void OnRunQuery() override {
        SetOperationInfo(OperationName, Owner.ToString());

        TString sql = R"(
            -- TListExpiredLeasesQueryActor::OnRunQuery
            DECLARE $max_lease_deadline AS Timestamp;

            SELECT
                database,
                execution_id
            FROM `.metadata/script_execution_leases`
            WHERE lease_deadline < $max_lease_deadline
              AND (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL)
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$max_lease_deadline")
                .Timestamp(LeaseDeadline)
                .Build();

        RunStreamQuery(sql, &params);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        std::vector<TEvListExpiredLeasesResponse::TLeaseInfo> leases;
        leases.reserve(resultSet.RowsCount());

        NYdb::TResultSetParser result(resultSet);
        while (result.TryNextRow()) {
            const std::optional<TString> database = result.ColumnParser("database").GetOptionalUtf8();
            if (!database) {
                KQP_PROXY_LOG_E("Database field is null for script execution lease");
                continue;
            }

            const std::optional<TString> executionId = result.ColumnParser("execution_id").GetOptionalUtf8();
            if (!executionId) {
                KQP_PROXY_LOG_E("Execution id field is null for script execution lease in database " << *database);
                continue;
            }

            leases.push_back({*database, *executionId});
        }

        if (!leases.empty()) {
            KQP_PROXY_LOG_D(LogPrefix() << "Found " << leases.size() << " expired leases");
            Send(Owner, new TEvListExpiredLeasesResponse(std::move(leases)));
        }
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvListExpiredLeasesResponse(status, std::move(issues)));
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TListExpiredLeasesQueryActor] OwnerId: " << Owner << " LeaseDeadline: " << LeaseDeadline << ". ";
    }

private:
    const TInstant LeaseDeadline;
};

class TRefreshScriptExecutionLeasesActor : public TActorBootstrapped<TRefreshScriptExecutionLeasesActor> {
public:
    TRefreshScriptExecutionLeasesActor(const TActorId& replyActorId, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : ReplyActorId(replyActorId)
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

    void Bootstrap() {
        KQP_PROXY_LOG_D("Bootstrap. Start TListExpiredLeasesQueryActor");
        Register(new TListExpiredLeasesQueryActor());

        Become(&TRefreshScriptExecutionLeasesActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvListExpiredLeasesResponse, Handle);
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
    )

    void Handle(TEvListExpiredLeasesResponse::TPtr& ev) {
        const auto& leases = ev->Get()->Leases;
        KQP_PROXY_LOG_D("Got list expired leases response, found " << leases.size() << " expired leases");

        for (const auto& lease : leases) {
            KQP_PROXY_LOG_D("ExecutionId: " << lease.ExecutionId << ", start TCheckLeaseStatusActor #" << CookieId);
            Register(new TCheckLeaseStatusActor(SelfId(), lease.Database, lease.ExecutionId, QueryServiceConfig, Counters, CookieId++));
            ++OperationsToCheck;
        }

        if (const auto status = ev->Get()->Status) {
            if (status != Ydb::StatusIds::SUCCESS) {
                KQP_PROXY_LOG_W("List expired leases failed with status " << *status << ", issues: " << ev->Get()->Issues.ToOneLineString());

                Success = false;
                Issues.AddIssues(AddRootIssue(
                    TStringBuilder() << "Failed to list expired leases (" << *status << ")",
                    ev->Get()->Issues,
                    true
                ));
            } else {
                KQP_PROXY_LOG_D("List expired leases successfully completed");
            }

            MaybeFinish();
        }
    }

    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie < CookieId);

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("Lease check failed #" << ev->Cookie << ", status: " << status << ", issues: " << issues.ToOneLineString());

            Success = false;
            Issues.AddIssues(AddRootIssue(
                TStringBuilder() <<"Lease check failed #" << ev->Cookie << " (" << status << ")",
                ev->Get()->Issues,
                true
            ));
        } else {
            KQP_PROXY_LOG_D("Lease check success #" << ev->Cookie);
        }

        --OperationsToCheck;
        MaybeFinish();
    }

private:
    void MaybeFinish() {
        if (OperationsToCheck) {
            return;
        }

        Send(ReplyActorId, new TEvRefreshScriptExecutionLeasesResponse(Success, std::move(Issues)));
        PassAway();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TRefreshScriptExecutionLeasesActor] ActorId: " << SelfId() << ". ";
    }

private:
    const TActorId ReplyActorId;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;

    ui64 CookieId = 0;
    ui64 OperationsToCheck = 0;
    bool Success = true;
    NYql::TIssues Issues;
};

} // anonymous namespace

IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TDuration maxRunTime) {
    return new TCreateScriptExecutionActor(std::move(ev), queryServiceConfig, counters, maxRunTime, LEASE_DURATION);
}

IActor* CreateScriptExecutionsTablesCreator() {
    return new TScriptExecutionsTablesCreator();
}

IActor* CreateForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TForgetScriptExecutionOperationActor(std::move(ev), queryServiceConfig, counters);
}

IActor* CreateGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TGetScriptExecutionOperationActor(std::move(ev), queryServiceConfig, counters);
}

IActor* CreateListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TListScriptExecutionOperationsActor(std::move(ev), queryServiceConfig, counters);
}

IActor* CreateCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TCancelScriptExecutionOperationActor(std::move(ev), queryServiceConfig, counters);
}

IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration, i64 leaseGeneration, TIntrusivePtr<TKqpCounters> counters) {
    return new TScriptLeaseUpdateActor(runScriptActorId, database, executionId, leaseDuration, leaseGeneration, counters);
}

IActor* CreateSaveScriptExecutionResultMetaActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, const TString& serializedMeta) {
    return new TQueryRetryActor<TSaveScriptExecutionResultMetaQuery, TEvSaveScriptResultMetaFinished, TString, TString, TString>(runScriptActorId, database, executionId, serializedMeta);
}

IActor* CreateSaveScriptExecutionResultActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, i32 resultSetId, std::optional<TInstant> expireAt, i64 firstRow, i64 accumulatedSize, Ydb::ResultSet&& resultSet) {
    return new TSaveScriptExecutionResultActor(runScriptActorId, database, executionId, resultSetId, expireAt, firstRow, accumulatedSize, std::move(resultSet));
}

IActor* CreateGetScriptExecutionResultActor(const TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant operationDeadline) {
    return new TGetScriptExecutionResultActor(replyActorId, database, executionId, resultSetIndex, offset, rowsLimit, sizeLimit, operationDeadline);
}

IActor* CreateSaveScriptExternalEffectActor(TEvSaveScriptExternalEffectRequest::TPtr ev) {
    return new TQueryRetryActor<TSaveScriptExternalEffectActor, TEvSaveScriptExternalEffectResponse, TEvSaveScriptExternalEffectRequest::TDescription>(ev->Sender, ev->Get()->Description);
}

IActor* CreateSaveScriptFinalStatusActor(const TActorId& finalizationActorId, TEvScriptFinalizeRequest::TPtr ev) {
    return new TQueryRetryActor<TSaveScriptFinalStatusActor, TEvSaveScriptFinalStatusResponse, TEvScriptFinalizeRequest::TDescription>(finalizationActorId, ev->Get()->Description);
}

IActor* CreateScriptFinalizationFinisherActor(const TActorId& finalizationActorId, const TString& executionId, const TString& database, std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues, i64 leaseGeneration) {
    return new TQueryRetryActor<TScriptFinalizationFinisherActor, TEvScriptExecutionFinished, TString, TString, std::optional<Ydb::StatusIds::StatusCode>, NYql::TIssues, i64>(finalizationActorId, executionId, database, operationStatus, operationIssues, leaseGeneration);
}

IActor* CreateScriptProgressActor(const TString& executionId, const TString& database, const TString& queryPlan) {
    return new TScriptProgressActor(database, executionId, queryPlan);
}

IActor* CreateRefreshScriptExecutionLeasesActor(const TActorId& replyActorId, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TRefreshScriptExecutionLeasesActor(replyActorId, queryServiceConfig, counters);
}

namespace NPrivate {

IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record, const NKikimrKqp::TScriptExecutionOperationMeta& meta) {
    return new TCreateScriptOperationQuery(executionId, runScriptActorId, record, meta, SCRIPT_TIMEOUT_LIMIT, {});
}

IActor* CreateCheckLeaseStatusActor(const TActorId& replyActorId, const TString& database, const TString& executionId, ui64 cookie) {
    return new TCheckLeaseStatusActor(replyActorId, database, executionId, {}, MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()), cookie);
}

} // namespace NPrivate

} // namespace NKikimr::NKqp
