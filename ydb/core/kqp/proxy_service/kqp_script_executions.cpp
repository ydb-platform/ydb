#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"

#include <ydb/core/fq/libs/common/rows_proto_splitter.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/proxy_service/proto/result_set_meta.pb.h>
#include <ydb/core/kqp/proxy_service/script_executions_utils/kqp_script_execution_compression.h>
#include <ydb/core/kqp/proxy_service/script_executions_utils/kqp_script_execution_retries.h>
#include <ydb/core/kqp/run_script_actor/kqp_run_script_actor.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/system/env.h>
#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <optional>
#include <memory>
#include <vector>

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

Ydb::Query::ExecMode GetExecModeFromAction(const NKikimrKqp::EQueryAction action) {
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
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
        case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
        case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
        case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
        case NKikimrKqp::QUERY_ACTION_TOPIC:
            throw yexception() << "Unsupported query action: " << NKikimrKqp::EQueryAction_Name(action);
    }
}

template<typename TDerived, typename TResponse>
class TQueryBase : public NKikimr::TQueryBase, public TQueryRetryActorMixin<TDerived, TResponse> {
    using TBase = NKikimr::TQueryBase;

public:
    using TThis = TDerived;

    struct TSettings {
        TString Database;  // Only for logging, request will be executed in AppData()->TenantName
        TString ExecutionId;
        TString SessionId;
        std::optional<i64> LeaseGeneration;
    };

    TQueryBase(const TString& operationName, TSettings&& settings)
        : TBase(NKikimrServices::KQP_PROXY, settings.SessionId, {}, /* isSystemUser */ true)
        , Database(std::move(settings.Database))
        , ExecutionId(std::move(settings.ExecutionId))
        , LeaseGeneration(settings.LeaseGeneration.value_or(0))
    {
        SetOperationInfo(operationName, CreateTraceId(settings));
    }

private:
    template<typename T>
    std::optional<std::vector<T>> ParseJsonArray(NYdb::TResultSetParser& result, const TString& columnName, const TString& error, std::function<void(const NJson::TJsonValue&, T&)> itemParser) {
        std::vector<T> parsed;

        if (const auto& serializedJson = result.ColumnParser(columnName).GetOptionalJsonDocument()) {
            NJson::TJsonValue value;
            if (!NJson::ReadJsonTree(*serializedJson, &value) || value.GetType() != NJson::JSON_ARRAY) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, error);
                return std::nullopt;
            }

            const auto parsedSize = value.GetIntegerRobust();
            parsed.reserve(parsedSize);
            for (i64 i = 0; i < parsedSize; ++i) {
                const NJson::TJsonValue* serializedItem = nullptr;
                value.GetValuePointer(i, &serializedItem);

                if (!serializedItem) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, error);
                    return std::nullopt;
                }

                itemParser(*serializedItem, parsed.emplace_back());
            }
        }

        return std::move(parsed);
    }

protected:
    static TString CreateTraceId(const TSettings& settings) {
        TStringBuilder result;

        if (settings.ExecutionId) {
            result << "ExecutionId: " << settings.ExecutionId;
        }

        if (settings.Database) {
            result << ", RequestDatabase: " << settings.Database;
        }

        if (settings.SessionId) {
            result << ", RequestSessionId: " << settings.SessionId;
        }

        if (settings.LeaseGeneration) {
            result << ", LeaseGeneration: " << *settings.LeaseGeneration;
        }

        return result;
    }

    void OnQueryResult() override {
        Finish();
    }

    NYdb::TParamsBuilder CreateParams() const {
        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build();
        return params;
    }

    static NYql::TIssues ParseScriptExecutionIssues(NYdb::TResultSetParser& result) {
        NYql::TIssues issues;

        if (const auto& issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument()) {
            issues = DeserializeIssues(*issuesSerialized);
        }

        if (const auto& transientIssuesSerialized = result.ColumnParser("transient_issues").GetOptionalJsonDocument()) {
            issues.AddIssues(AddRootIssue("Previous query retries", DeserializeIssues(*transientIssuesSerialized), /* addEmptyRoot */ false));
        }

        return issues;
    }

    std::optional<NKikimrKqp::TScriptExecutionRetryState> ParseRetryState(NYdb::TResultSetParser& result) {
        NKikimrKqp::TScriptExecutionRetryState retryState;

        if (const auto& serializedRetryState = result.ColumnParser("retry_state").GetOptionalJsonDocument()) {
            NJson::TJsonValue value;
            if (!NJson::ReadJsonTree(*serializedRetryState, &value)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Retry state is corrupted");
                return std::nullopt;
            }

            NProtobufJson::Json2Proto(value, retryState);
        }

        return std::move(retryState);
    }

    std::optional<std::vector<Ydb::Query::ResultSetMeta>> ParseResultSetMetas(NYdb::TResultSetParser& result) {
        return ParseJsonArray<Ydb::Query::ResultSetMeta>(result, "result_set_metas", "Result set meta is corrupted", [](const NJson::TJsonValue& value, Ydb::Query::ResultSetMeta& dst) {
            NProtobufJson::Json2Proto(value, dst);
        });
    }

    std::optional<std::vector<NKqpProto::TKqpExternalSink>> ParseScriptSinks(NYdb::TResultSetParser& result) {
        return ParseJsonArray<NKqpProto::TKqpExternalSink>(result, "script_sinks", "Script sinks are corrupted", [](const NJson::TJsonValue& value, NKqpProto::TKqpExternalSink& dst) {
            DeserializeBinaryProto(value, dst);
        });
    }

    std::optional<std::vector<TString>> ParseSecretNames(NYdb::TResultSetParser& result) {
        return ParseJsonArray<TString>(result, "script_secret_names", "Script secret names are corrupted", [](const NJson::TJsonValue& value, TString& dst) {
            dst = value.GetString();
        });
    }

    std::optional<std::vector<NACLib::TSID>> ParseUserGroupSids(NYdb::TResultSetParser& result) {
        return ParseJsonArray<NACLib::TSID>(result, "user_group_sids", "User group SIDs are corrupted", [](const NJson::TJsonValue& value, NACLib::TSID& dst) {
            dst = value.GetString();
        });
    }

    bool ValidateLease(NYdb::TResultSetParser& result, const ELeaseState expectedLeaseState) {
        Y_VALIDATE(LeaseGeneration, "Lease generation is not set");

        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Script execution operation not found");
            return false;
        }

        const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
        if (!leaseGenerationInDatabase) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation, lease was lost");
            return false;
        }

        if (LeaseGeneration != *leaseGenerationInDatabase) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Lease was lost, expected generation: " << LeaseGeneration << ", got: " << *leaseGenerationInDatabase);
            return false;
        }

        const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning));
        if (leaseState != static_cast<i32>(expectedLeaseState)) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Script execution has lease state: " << leaseState << ", when for operation " << OperationName << " expected lease state: " << expectedLeaseState);
            return false;
        }

        return true;
    }

    bool ValidateLease(const NYdb::TResultSet& resultSet, const ELeaseState expectedLeaseState) {
        NYdb::TResultSetParser result(resultSet);
        return ValidateLease(result, expectedLeaseState);
    }

    const TString Database;
    TString ExecutionId;
    const i64 LeaseGeneration = 0;
};

template<typename TDerived, typename TResponse>
class TQueryWithAccessValidationBase : public TQueryBase<TDerived, TResponse> {
    using TBase = TQueryBase<TDerived, TResponse>;

public:
    using TBase::LogPrefix;

    TQueryWithAccessValidationBase(const TString& operationName, TBase::TSettings&& settings, std::optional<TString> userSID)
        : TBase(operationName, std::move(settings))
        , UserSID(std::move(userSID))
    {}

protected:
    bool ValidateUserSID() {
        if (const auto& error = CheckScriptExecutionAccess(UserSID)) {
            KQP_PROXY_LOG_W("Token validation failed: " << error);
            TBase::Finish(Ydb::StatusIds::UNAUTHORIZED, error);
            return false;
        }

        return true;
    }

    bool ValidateAccess(NYdb::TResultSetParser& result) {
        if (!UserSID) {
            return true;
        }

        if (const auto& ownerUser = result.ColumnParser("user_token").GetOptionalUtf8(); ownerUser && *ownerUser != *UserSID) {
            KQP_PROXY_LOG_W("Access denied for user " << *UserSID);
            TBase::Finish(Ydb::StatusIds::UNAUTHORIZED, "Access denied. User is not owner of script execution operation.");
            return false;
        }

        return true;
    }

    std::optional<TString> UserSID;
};

class TScriptExecutionsTablesCreator final : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

    static constexpr TDuration TTL_DEADLINE_OFFSET = TDuration::Minutes(20);
    static constexpr TDuration TTL_BRO_RUN_INTERVAL = TDuration::Minutes(60);

public:
    TScriptExecutionsTablesCreator(const bool enableSecureScriptExecutions, const ui64 generation)
        : TBase({
            GetScriptExecutionsCreator(enableSecureScriptExecutions),
            GetScriptExecutionLeasesCreator(enableSecureScriptExecutions),
            GetScriptResultSetsCreator(enableSecureScriptExecutions)
        })
        , Generation(generation)
    {}

private:
    static NACLib::TDiffACL GetTableACL(const bool enableSecureScriptExecutions) {
        NACLib::TDiffACL acl;
        acl.ClearAccess();
        acl.SetInterruptInheritance(enableSecureScriptExecutions);
        return acl;
    }

    static IActor* GetScriptExecutionsCreator(const bool enableSecureScriptExecutions) {
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
                Col("plan_compressed", NScheme::NTypeIds::String),
                Col("plan_compression_method", NScheme::NTypeIds::Text),
                Col("meta", NScheme::NTypeIds::JsonDocument),
                Col("streaming_disposition", NScheme::NTypeIds::Json),
                Col("parameters", NScheme::NTypeIds::String), // TODO: store parameters separately to support bigger storage.
                Col("result_set_metas", NScheme::NTypeIds::JsonDocument),
                Col("stats", NScheme::NTypeIds::JsonDocument),
                Col("expire_at", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                Col("customer_supplied_id", NScheme::NTypeIds::Text),
                Col("user_token", NScheme::NTypeIds::Text), // UserSID
                Col("user_group_sids", NScheme::NTypeIds::JsonDocument),
                Col("script_sinks", NScheme::NTypeIds::JsonDocument),
                Col("script_secret_names", NScheme::NTypeIds::JsonDocument),
                Col("retry_state", NScheme::NTypeIds::JsonDocument),
                Col("graph_compressed", NScheme::NTypeIds::String),
                Col("graph_compression_method", NScheme::NTypeIds::Text),
                Col("lease_generation", NScheme::NTypeIds::Int64), // Incremented after each script execution retry
            },
            { "database", "execution_id" },
            NKikimrServices::KQP_PROXY,
            TtlCol("expire_at", TTL_DEADLINE_OFFSET, TTL_BRO_RUN_INTERVAL),
            /* database */ {},
            /* isSystemUser */ enableSecureScriptExecutions,
            /* partitioningPolicy */ Nothing(),
            GetTableACL(enableSecureScriptExecutions)
        );
    }

    static IActor* GetScriptExecutionLeasesCreator(const bool enableSecureScriptExecutions) {
        // Allowed lease states in `script_execution_leases`, keyed by `execution_id`
        // (the lease for the script execution operation itself):
        //
        // 1. While the script execution operation is running, a runtime lease is stored
        //    in the table with state `ScriptRunning` and a deadline of approximately
        //    `now + lease_duration`. `TRunScriptActor` periodically extends the lease
        //    deadline.
        //
        //    - `TScriptExecutionLeaseCheckActor` periodically checks the
        //      `script_execution_leases` table and finds expired leases. Once a runtime
        //      lease expires, `TScriptExecutionLeaseCheckActor` checks the availability
        //      of `TRunScriptActor` and, in case of an error, starts finalization of the
        //      script execution operation.
        //
        // 2. If the script execution has external effects, such as S3 writes, then after
        //    it finishes successfully or is canceled, the lease state in the table is
        //    changed to `ScriptFinalizing`, and finalization is started. For example,
        //    this may roll back S3 uploads after a failure. In this case, the lease
        //    deadline is equal to the finalization start time plus the lease duration.
        //    The lease deadline is not extended in this state; we assume that
        //    finalization will complete within the specified deadline.
        //
        //    - If the lease expires in this state,
        //      `TScriptExecutionLeaseCheckActor` starts finalization of the script
        //      execution operation.
        //
        // 3. If the script execution operation has a retry policy and fails with a
        //    retriable status, a lease in state `WaitRetry` is created after
        //    finalization. In this case, the lease deadline represents the retry backoff
        //    time.
        //
        //    - When a lease with status `WaitRetry` expires,
        //      `TScriptExecutionLeaseCheckActor` restarts the script execution operation
        //      with the same ID.

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
            TtlCol("expire_at", TTL_DEADLINE_OFFSET, TTL_BRO_RUN_INTERVAL),
            /* database */ {},
            /* isSystemUser */ enableSecureScriptExecutions,
            /* partitioningPolicy */ Nothing(),
            GetTableACL(enableSecureScriptExecutions)
        );
    }

    static IActor* GetScriptResultSetsCreator(const bool enableSecureScriptExecutions) {
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
            TtlCol("expire_at", TTL_DEADLINE_OFFSET, TTL_BRO_RUN_INTERVAL),
            /* database */ {},
            /* isSystemUser */ enableSecureScriptExecutions,
            /* partitioningPolicy */ Nothing(),
            GetTableACL(enableSecureScriptExecutions)
        );
    }

    void OnTablesCreated(const bool success, NYql::TIssues issues) override  {
        Send(Owner, new TEvScriptExecutionsTablesCreationFinished(success, std::move(issues)), /* flags */ 0, Generation);
        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), new TEvStartScriptExecutionBackgroundChecks());
    }

    const ui64 Generation = 0;
};

// Creates new script execution, may requested from gRPC API / streaming query creation or modification

class TCreateScriptOperationQuery final : public TQueryBase<TCreateScriptOperationQuery, TEvPrivate::TEvCreateScriptOperationResponse> {
public:
    TCreateScriptOperationQuery(TString executionId, const TActorId& runScriptActorId, NKikimrKqp::TEvQueryRequest req,
        NKikimrKqp::TScriptExecutionOperationMeta meta, const TDuration maxRunTime,
        NKikimrKqp::TScriptExecutionRetryState retryState, std::optional<NKikimrKqp::TQueryPhysicalGraph> physicalGraph,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, std::shared_ptr<NYql::NPq::NProto::StreamingDisposition> streamingDisposition, const i64 generation)
        : TQueryBase(__func__, {.Database = req.GetRequest().GetDatabase(), .ExecutionId = std::move(executionId), .LeaseGeneration = generation})
        , RunScriptActorId(runScriptActorId)
        , Request(std::move(req))
        , Meta(std::move(meta))
        , MaxRunTime(Max(maxRunTime, TDuration::Days(1)))
        , RetryState(std::move(retryState))
        , PhysicalGraph(std::move(physicalGraph))
        , StreamingDisposition(std::move(streamingDisposition))
        , Compressor(queryServiceConfig.GetQueryArtifactsCompressionMethod(), queryServiceConfig.GetQueryArtifactsCompressionMinSize())
    {}

private:
    void OnRunQuery() override {
        const auto metaTtl = std::min(MaxRunTime.MicroSeconds(), NYql::NUdf::MAX_TIMESTAMP - 1);
        KQP_PROXY_LOG_D("Creating query in database, meta ttl: " << TDuration::MicroSeconds(metaTtl));

        constexpr char sql[] = R"(
            -- TCreateScriptOperationQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $run_script_actor_id AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $execution_mode AS Int32;
            DECLARE $query_text AS Text;
            DECLARE $syntax AS Int32;
            DECLARE $meta AS JsonDocument;
            DECLARE $streaming_disposition AS Optional<Json>;
            DECLARE $lease_duration AS Interval;
            DECLARE $lease_state AS Int32;
            DECLARE $execution_meta_ttl AS Interval;
            DECLARE $retry_state AS JsonDocument;
            DECLARE $user_sid AS Optional<Text>;
            DECLARE $user_group_sids AS Optional<JsonDocument>;
            DECLARE $parameters AS String;
            DECLARE $graph_compressed AS Optional<String>;
            DECLARE $graph_compression_method AS Optional<Text>;
            DECLARE $lease_generation AS Int64;

            UPSERT INTO `.metadata/script_executions` (
                database, execution_id, run_script_actor_id, execution_status, execution_mode, start_ts,
                query_text, syntax, meta, streaming_disposition, expire_at, retry_state,
                user_token, user_group_sids, parameters,
                graph_compressed, graph_compression_method, lease_generation
            ) VALUES (
                $database, $execution_id, $run_script_actor_id, $execution_status, $execution_mode, CurrentUtcTimestamp(),
                $query_text, $syntax, $meta, $streaming_disposition, CurrentUtcTimestamp() + $execution_meta_ttl, $retry_state,
                $user_sid, $user_group_sids, $parameters,
                $graph_compressed, $graph_compression_method, $lease_generation
            );

            UPSERT INTO `.metadata/script_execution_leases` (
                database, execution_id, lease_deadline, lease_generation,
                expire_at, lease_state
            ) VALUES (
                $database, $execution_id, CurrentUtcTimestamp() + $lease_duration, $lease_generation,
                CurrentUtcTimestamp() + $execution_meta_ttl, $lease_state
            );
        )";

        std::optional<TString> userSID;
        std::optional<TString> userGroupSIDs;
        if (Request.HasUserToken()) {
            const NACLib::TUserToken token(Request.GetUserToken());
            userSID = token.GetUserSID();
            userGroupSIDs = SequenceToJsonString(token.GetGroupSIDs());
        }

        std::optional<TString> graphCompressionMethod;
        std::optional<TString> graphCompressed;
        if (PhysicalGraph) {
            std::tie(graphCompressionMethod, graphCompressed) = Compressor.Compress(PhysicalGraph->SerializeAsString());
        }

        auto params = CreateParams();
        params
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
            .AddParam("$streaming_disposition")
                .OptionalJson(StreamingDisposition ? std::optional<std::string>(SerializeBinaryProto(*StreamingDisposition)) : std::nullopt)
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(std::min(NProtoInterop::CastFromProto(Meta.GetLeaseDuration()).MicroSeconds(), NYql::NUdf::MAX_TIMESTAMP - 1)))
                .Build()
            .AddParam("$execution_meta_ttl")
                .Interval(metaTtl)
                .Build()
            .AddParam("$retry_state")
                .JsonDocument(NProtobufJson::Proto2Json(RetryState, NProtobufJson::TProto2JsonConfig()))
                .Build()
            .AddParam("$lease_state")
                .Int32(static_cast<i32>(ELeaseState::ScriptRunning))
                .Build()
            .AddParam("$user_sid")
                .OptionalUtf8(userSID)
                .Build()
            .AddParam("$user_group_sids")
                .OptionalJsonDocument(userGroupSIDs)
                .Build()
            .AddParam("$parameters")
                .String(SerializeParameters())
                .Build()
            .AddParam("$graph_compressed")
                .OptionalString(graphCompressed)
                .Build()
            .AddParam("$graph_compression_method")
                .OptionalUtf8(graphCompressionMethod)
                .Build()
            .AddParam("$lease_generation")
                .Int64(LeaseGeneration)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Create script execution operation"
            << ", RetryState: " << RetryState.ShortDebugString()
            << ", has PhysicalGraph: " << PhysicalGraph.has_value()
            << ", Result: " << status
            << ", Issues: " << issues.ToOneLineString());

        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(ExecutionId, std::move(issues)));
        } else {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(status, std::move(issues)));
        }
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

    const TActorId RunScriptActorId;
    const NKikimrKqp::TEvQueryRequest Request;
    const NKikimrKqp::TScriptExecutionOperationMeta Meta;
    const TDuration MaxRunTime;
    const NKikimrKqp::TScriptExecutionRetryState RetryState;
    const std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    const std::shared_ptr<NYql::NPq::NProto::StreamingDisposition> StreamingDisposition;
    const TCompressor Compressor;
};

class TCreateScriptExecutionActor final : public TActorBootstrapped<TCreateScriptExecutionActor>, IActorExceptionHandler {
public:
    TCreateScriptExecutionActor(TEvKqp::TEvScriptRequest::TPtr&& ev, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, const TDuration maxRunTime, const TDuration leaseDuration)
        : Event(std::move(ev))
        , QueryServiceConfig(std::move(queryServiceConfig))
        , Counters(std::move(counters))
        , ExecutionId(Event->Get()->ExecutionId ? *Event->Get()->ExecutionId : CreateGuidAsString())
        , LeaseDuration(leaseDuration ? leaseDuration : LEASE_DURATION)
        , MaxRunTime(maxRunTime)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);

        const auto& ev = *Event->Get();
        const auto& eventProto = ev.Record;
        const auto& request = eventProto.GetRequest();
        if (ev.SaveQueryPhysicalGraph) {
            if (request.GetAction() != NKikimrKqp::QUERY_ACTION_EXECUTE) {
                SendFailResponse(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Save query physical graph is allowed only for execute action (got " << NKikimrKqp::EQueryAction_Name(request.GetAction()) << ")");
                return;
            }

            if (request.HasTxControl()) {
                SendFailResponse(Ydb::StatusIds::INTERNAL_ERROR, "Save query physical graph is not allowed inside not implicit transaction");
                return;
            }
        }

        auto meta = GetOperationMeta();

        // Start request
        RunScriptActorId = Register(CreateRunScriptActor(eventProto, {
            .Database = request.GetDatabase(),
            .ExecutionId = ExecutionId,
            .LeaseGeneration = ev.Generation,
            .LeaseDuration = LeaseDuration,
            .ResultsTtl = NProtoInterop::CastFromProto(meta.GetResultsTtl()),
            .ProgressStatsPeriod = ev.ProgressStatsPeriod,
            .Counters = Counters,
            .SaveQueryPhysicalGraph = ev.SaveQueryPhysicalGraph,
            .PhysicalGraph = ev.QueryPhysicalGraph,
            .DisableDefaultTimeout = ev.DisableDefaultTimeout,
            .CheckpointId = ev.CheckpointId,
            .StreamingQueryPath = ev.StreamingQueryPath,
            .CustomerSuppliedId = ev.CustomerSuppliedId,
            .StreamingDisposition = ev.StreamingDisposition,
        }, QueryServiceConfig));

        auto disposition = ev.StreamingDisposition;
        if (ev.QueryPhysicalGraph && ev.QueryPhysicalGraph->GetZeroCheckpointSaved()) {
            disposition = nullptr; // Do not save disposition if state already saved
        }

        const auto& creatorId = Register(TCreateScriptOperationQuery::MakeRetry(SelfId(), ExecutionId, RunScriptActorId, ev.Record, std::move(meta), MaxRunTime, GetRetryState(), ev.QueryPhysicalGraph, QueryServiceConfig, std::move(disposition), ev.Generation));
        KQP_PROXY_LOG_D("Bootstrap. Start TCreateScriptOperationQuery " << creatorId << ", RunScriptActorId: " << RunScriptActorId);
    }

    void Handle(TEvPrivate::TEvCreateScriptOperationResponse::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_W("Create script operation " << ev->Sender << " failed " << status << ", Issues: " << ev->Get()->Issues.ToOneLineString());
            SendFailResponse(status, AddRootIssue("Internal error. Failed to save meta information about new script execution", ev->Get()->Issues));
            return;
        }

        KQP_PROXY_LOG_D("Create script operation " << ev->Sender << " succeeded, RunScriptActorId: " << RunScriptActorId);

        Send(Event->Sender, new TEvKqp::TEvScriptResponse(
            ScriptExecutionOperationFromExecutionId(ev->Get()->ExecutionId),
            ev->Get()->ExecutionId,
            Ydb::Query::EXEC_STATUS_STARTING,
            GetExecModeFromAction(Event->Get()->Record.GetRequest().GetAction())
        ));
        Send(RunScriptActorId, new TEvents::TEvWakeup());
        PassAway();
    }

    bool OnUnhandledException(const std::exception& ex) final {
        KQP_PROXY_LOG_E("Got unexpected exception: " << ex.what());
        SendFailResponse(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << ex.what());
        return true;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCreateScriptOperationResponse, Handle)
    )

private:
    void SendFailResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        Send(Event->Sender, new TEvKqp::TEvScriptResponse(status, std::move(issues)));
        if (RunScriptActorId) {
            Send(RunScriptActorId, new TEvents::TEvPoison());
        }
        PassAway();
    }

    void SendFailResponse(const Ydb::StatusIds::StatusCode status, const TString& error) {
        SendFailResponse(status, {NYql::TIssue(error)});
    }

    NKikimrKqp::TScriptExecutionOperationMeta GetOperationMeta() const {
        const auto& ev = *Event->Get();
        const auto& eventProto = ev.Record;
        const auto& request = eventProto.GetRequest();

        NKikimrKqp::TScriptExecutionOperationMeta meta;
        meta.SetTraceId(eventProto.GetTraceId());
        meta.SetResourcePoolId(request.GetPoolId());
        meta.SetCheckpointId(ev.CheckpointId);
        meta.SetStreamingQueryPath(ev.StreamingQueryPath);
        meta.SetCustomerSuppliedId(ev.CustomerSuppliedId);
        meta.SetClientAddress(request.GetClientAddress());
        meta.SetCollectStats(request.GetCollectStats());
        meta.SetSaveQueryPhysicalGraph(ev.SaveQueryPhysicalGraph);
        meta.SetDisableDefaultTimeout(ev.DisableDefaultTimeout);
        *meta.MutableRlPath() = eventProto.GetRlPath();
        DurationToProtoWithSaturation(LeaseDuration, meta.MutableLeaseDuration());
        DurationToProtoWithSaturation(ev.ProgressStatsPeriod, meta.MutableProgressStatsPeriod());

        const auto operationTtl = ev.ForgetAfter ? ev.ForgetAfter : TDuration::Seconds(QueryServiceConfig.GetScriptForgetAfterDefaultSeconds());
        DurationToProtoWithSaturation(operationTtl, meta.MutableOperationTtl());

        auto resultsTtl = ev.ResultsTtl ? ev.ResultsTtl : TDuration::Seconds(QueryServiceConfig.GetScriptResultsTtlDefaultSeconds());
        if (operationTtl) {
            resultsTtl = Min(operationTtl, resultsTtl);
        }

        DurationToProtoWithSaturation(resultsTtl, meta.MutableResultsTtl());

        const auto now = TInstant::Now();
        if (const auto timeout = TDuration::MilliSeconds(request.GetTimeoutMs())) {
            TimestampToProtoWithSaturation(now + timeout, meta.MutableTimeoutAt());
        }

        if (const auto cancelAfter = TDuration::MilliSeconds(request.GetCancelAfterMs())) {
            TimestampToProtoWithSaturation(now + cancelAfter, meta.MutableCancelAt());
        }

        return meta;
    }

    NKikimrKqp::TScriptExecutionRetryState GetRetryState() const {
        const auto& retryMapping = Event->Get()->RetryMapping;

        NKikimrKqp::TScriptExecutionRetryState retryState;
        retryState.MutableRetryPolicyMapping()->Assign(retryMapping.begin(), retryMapping.end());

        return retryState;
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TCreateScriptExecutionActor] OwnerId: " << Event->Sender << " ActorId: " << SelfId() << " Database: " << Event->Get()->Record.GetRequest().GetDatabase() << " ExecutionId: " << ExecutionId << ". ";
    }

    const TEvKqp::TEvScriptRequest::TPtr Event;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    const TString ExecutionId;
    TActorId RunScriptActorId;
    const TDuration LeaseDuration;
    const TDuration MaxRunTime;
};

// Created from TRunScriptActor and advances runtime lease with state ELeaseState::ScriptRunning

class TScriptLeaseUpdaterQuery final : public TQueryBase<TScriptLeaseUpdaterQuery, TEvScriptLeaseUpdateResponse> {
public:
    TScriptLeaseUpdaterQuery(TString database, TString executionId, const TDuration leaseDuration, const i64 leaseGeneration)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , LeaseDuration(leaseDuration)
    {}

private:
    void OnRunQuery() override {
        KQP_PROXY_LOG_D("Update lease on duration: " << LeaseDuration);

        constexpr char sql[] = R"(
            -- TScriptLeaseUpdaterQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                lease_generation,
                lease_state
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetLeaseInfo, "Get lease info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetLeaseInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        LeaseExists = false;

        if (ValidateLease(ResultSets[0], ELeaseState::ScriptRunning)) {
            LeaseExists = true;
            UpdateLease();
        }
    }

    void UpdateLease() {
        // Updating the lease in the table can take a long time,
        // so the query uses CurrentUtcTimestamp(), 
        // but for the next update, LeaseDeadline is used,
        // which corresponds to a strictly shorter time.
        LeaseDeadline = TInstant::Now() + LeaseDuration;

        TString sql = R"(
            -- TScriptLeaseUpdaterQuery::UpdateLease
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $lease_duration AS Interval;

            UPSERT INTO `.metadata/script_execution_leases` (
                database, execution_id, lease_deadline
            ) VALUES (
                $database, $execution_id, CurrentUtcTimestamp() + $lease_duration
            );
        )";

        auto params = CreateParams();
        params
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
                .Build();

        SetQueryResultHandler(&TThis::OnQueryResult, "Update lease");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvScriptLeaseUpdateResponse(LeaseExists, LeaseDeadline, status, std::move(issues)));
    }

    const TDuration LeaseDuration;
    const TInstant LeaseUpdateStartTime = TInstant::Now();
    TInstant LeaseDeadline;
    bool LeaseExists = true;
};

// Restart script execution after expiration of lease with state ELeaseState::WaitRetry (now used only in script executions from streaming queries).
// Can be runned multiple times in parallel for same script execution operation.

class TRestartScriptOperationQuery final : public TQueryBase<TRestartScriptOperationQuery, TEvScriptExecutionRestarted> {
    static constexpr ui64 MAX_TRANSIENT_ISSUES_COUNT = 10;

public:
    TRestartScriptOperationQuery(TString database, TString executionId, const i64 leaseGeneration,
        NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , QueryServiceConfig(std::move(queryServiceConfig))
        , Counters(std::move(counters))
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
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
                user_group_sids,
                graph_compressed,
                graph_compression_method,
                streaming_disposition
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

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetExecutionInfo, "Get execution info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetExecutionInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (!ValidateLease(result, ELeaseState::WaitRetry)) {
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
        }

        NKikimrKqp::TEvQueryRequest queryRequest;
        auto& request = *queryRequest.MutableRequest();

        NKikimrKqp::TScriptExecutionOperationMeta meta;
        std::optional<NKikimrKqp::TQueryPhysicalGraph> physicalGraph;
        std::shared_ptr<NYql::NPq::NProto::StreamingDisposition> streamingDisposition;

        {   // Execution info
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::NOT_FOUND, "Script execution operation not found");
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

            if (const auto& issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument()) {
                TransientIssues.AddIssues(DeserializeIssues(*issuesSerialized));
            }

            if (const auto& transientIssuesSerialized = result.ColumnParser("transient_issues").GetOptionalJsonDocument()) {
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

            if (const std::optional<TString>& userSID = result.ColumnParser("user_token").GetOptionalUtf8()) {
                TVector<NACLib::TSID> userGroupSids;
                if (auto userGroupSids = ParseUserGroupSids(result)) {
                    userGroupSids = std::move(*userGroupSids);
                }

                queryRequest.SetUserToken(NACLib::TUserToken(*userSID, userGroupSids).SerializeAsString());
            }

            if (const auto& serializedParameters = result.ColumnParser("parameters").GetOptionalString()) {
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

            if (const std::optional<TString>& graphCompressed = result.ColumnParser("graph_compressed").GetOptionalString()) {
                const std::optional<TString> compressionMethod = result.ColumnParser("graph_compression_method").GetOptionalUtf8();
                if (!compressionMethod) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Graph compression method is not found");
                    return;
                }

                const TCompressor compressor(*compressionMethod);
                const auto& graph = compressor.Decompress(*graphCompressed);

                if (!physicalGraph.emplace().ParseFromString(graph)) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Query physical graph is corrupted");
                    return;
                }
            }

            if (const auto& serializedStreamingDisposition = result.ColumnParser("streaming_disposition").GetOptionalJson()) {
                NJson::TJsonValue serializedStreamingDispositionJson;
                if (!NJson::ReadJsonTree(*serializedStreamingDisposition, &serializedStreamingDispositionJson)) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming disposition is corrupted");
                    return;
                }

                streamingDisposition = std::make_shared<NYql::NPq::NProto::StreamingDisposition>();
                DeserializeBinaryProto(serializedStreamingDispositionJson, *streamingDisposition);
            }

            std::optional<TString> queryText = result.ColumnParser("query_text").GetOptionalUtf8();
            if (!queryText) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Query text is not found");
                return;
            }

            *request.MutableQuery() = std::move(*queryText);
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

        LeaseDuration = NProtoInterop::CastFromProto(meta.GetLeaseDuration());

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

        const bool hasPhysicalGraph = physicalGraph.has_value();
        RunScriptActorId = Register(CreateRunScriptActor(queryRequest, {
            .Database = request.GetDatabase(),
            .ExecutionId = ExecutionId,
            .LeaseGeneration = LeaseGeneration + 1,
            .LeaseDuration = LeaseDuration,
            .ResultsTtl = NProtoInterop::CastFromProto(meta.GetResultsTtl()),
            .ProgressStatsPeriod = NProtoInterop::CastFromProto(meta.GetProgressStatsPeriod()),
            .Counters = Counters,
            .SaveQueryPhysicalGraph = meta.GetSaveQueryPhysicalGraph(),
            .PhysicalGraph = std::move(physicalGraph),
            .DisableDefaultTimeout = meta.GetDisableDefaultTimeout(),
            .CheckpointId = meta.GetCheckpointId(),
            .StreamingQueryPath = meta.GetStreamingQueryPath(),
            .CustomerSuppliedId = meta.GetCustomerSuppliedId(),
            .StreamingDisposition = streamingDisposition,
        }, QueryServiceConfig));

        KQP_PROXY_LOG_D("Restart with RunScriptActorId: " << RunScriptActorId << ", has PhysicalGraph: " << hasPhysicalGraph);
        RestartScriptExecution();
    }

    void RestartScriptExecution() {
        constexpr char sql[] = R"(
            -- TRestartScriptOperationQuery::RestartScriptExecution
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $lease_duration AS Interval;
            DECLARE $lease_generation AS Int64;
            DECLARE $lease_state AS Int32;
            DECLARE $run_script_actor_id AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $transient_issues AS JsonDocument;

            UPSERT INTO `.metadata/script_executions` (
                database, execution_id, run_script_actor_id,
                operation_status, execution_status, finalization_status,
                start_ts, end_ts,
                ast, ast_compressed, ast_compression_method,
                issues, transient_issues, plan,
                result_set_metas, stats, lease_generation
            ) VALUES (
                $database, $execution_id, $run_script_actor_id,
                NULL, $execution_status, NULL,
                CurrentUtcTimestamp(), NULL,
                NULL, NULL, NULL,
                NULL, $transient_issues, NULL,
                NULL, NULL, $lease_generation
            );

            UPSERT INTO `.metadata/script_execution_leases` (
                database, execution_id,
                lease_deadline, lease_generation, lease_state
            ) VALUES (
                $database, $execution_id,
                CurrentUtcTimestamp() + $lease_duration, $lease_generation, $lease_state
            );
        )";

        auto params = CreateParams();
        params
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

        SetQueryResultHandler(&TThis::OnQueryResult, "Restart script execution");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (RunScriptActorId) {
            if (status == Ydb::StatusIds::SUCCESS) {
                Send(RunScriptActorId, new TEvents::TEvWakeup());
            } else {
                Send(RunScriptActorId, new TEvents::TEvPoison());
            }
        }

        Send(Owner, new TEvScriptExecutionRestarted(status, std::move(issues)));
    }

    static NKikimrKqp::EQueryAction GetActionFromExecMode(Ydb::Query::ExecMode execMode) {
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

    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    TDuration LeaseDuration;
    TActorId RunScriptActorId;
    NYql::TIssues TransientIssues;
};

// Get full information about current script execution state and lease state

class TCheckLeaseStatusQueryActor final : public TQueryWithAccessValidationBase<TCheckLeaseStatusQueryActor, NPrivate::TEvPrivate::TEvLeaseCheckResult> {
public:
    struct TSettings {
        ui64 Cookie = 0;
        std::optional<TString> UserSID = std::nullopt;
    };

    TCheckLeaseStatusQueryActor(TString database, TString executionId, TSettings settings)
        : TQueryWithAccessValidationBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId)}, std::move(settings.UserSID))
        , Cookie(settings.Cookie)
        , Response(std::make_unique<NPrivate::TEvPrivate::TEvLeaseCheckResult>())
    {}

private:
    void OnRunQuery() override {
        KQP_PROXY_LOG_D("Start, Cookie: " << Cookie);

        constexpr char sql[] = R"(
            -- TCheckLeaseStatusQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                finalization_status,
                issues,
                transient_issues,
                run_script_actor_id,
                retry_state,
                result_set_metas,
                user_token
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

        auto params = CreateParams();
        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser executionsResult(ResultSets[0]);
        if (!executionsResult.TryNextRow()) {
            Response->EntryExists = false;
            Finish();
            return;
        }

        if (!ValidateAccess(executionsResult)) {
            return;
        }

        const auto& runScriptActorId = executionsResult.ColumnParser("run_script_actor_id").GetOptionalUtf8();
        if (!runScriptActorId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Not found run script actor id for script execution");
            return;
        }

        if (!NKqp::ScriptExecutionRunnerActorIdFromString(*runScriptActorId, Response->RunScriptActorId)) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Run script actor id is corrupted");
            return;
        }

        const auto operationStatus = executionsResult.ColumnParser("operation_status").GetOptionalInt32();
        if (operationStatus) {
            Response->OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
        }

        if (const auto executionStatus = executionsResult.ColumnParser("execution_status").GetOptionalInt32()) {
            Response->ExecutionStatus = static_cast<Ydb::Query::ExecStatus>(*executionStatus);
        }

        if (const auto finalizationStatus = executionsResult.ColumnParser("finalization_status").GetOptionalInt32()) {
            Response->FinalizationStatus = static_cast<EFinalizationStatus>(*finalizationStatus);
            if (!operationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state, status should be specified during finalization");
                return;
            }
        }

        Response->OperationIssues = ParseScriptExecutionIssues(executionsResult);

        if (const auto& retryState = ParseRetryState(executionsResult)) {
            Response->HasRetryPolicy = retryState->RetryPolicyMappingSize() > 0;
        } else {
            return;
        }

        if (auto resultSetMetas = ParseResultSetMetas(executionsResult)) {
            Response->ResultSetMetas = std::move(*resultSetMetas);
        } else {
            return;
        }

        if (NYdb::TResultSetParser leaseResult(ResultSets[1]); leaseResult.TryNextRow()) {
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

            Response->LeaseGeneration = *leaseGeneration;
            const auto leaseState = static_cast<ELeaseState>(leaseResult.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning)));

            switch (leaseState) {
                case ELeaseState::ScriptRunning: {
                    if (operationStatus) {
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state, status should be empty during query run");
                        return;
                    }
                    break;
                }
                case ELeaseState::ScriptFinalizing: {
                    if (!operationStatus || !Response->FinalizationStatus) {
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Invalid operation state, status and finalization status should be specified for lease state " << static_cast<i32>(leaseState));
                        return;
                    }
                    break;
                }
                case ELeaseState::WaitRetry: {
                    if (!operationStatus || Response->FinalizationStatus) {
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Invalid operation state, status should be specified and finalization must be completed for lease state " << static_cast<i32>(leaseState));
                        return;
                    }
                    if (!Response->HasRetryPolicy) {
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Invalid operation state, retry policy must be specified for lease state " << static_cast<i32>(leaseState));
                        return;
                    }
                    break;
                }
            }

            if (*leaseDeadline < RunStartTime) {
                Response->LeaseExpired = true;
                Response->RetryRequired = leaseState == ELeaseState::WaitRetry;
            }
        } else if (!operationStatus) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state, operation status or lease must be specified");
            return;
        }

        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Response->Status = status;
        Response->Issues = std::move(issues);
        Send(Owner, Response.release(), /* flags */ 0, Cookie);
    }

    const TInstant RunStartTime = TInstant::Now();
    const ui64 Cookie = 0;
    std::unique_ptr<NPrivate::TEvPrivate::TEvLeaseCheckResult> Response;
};

// When script execution lease expired TFinalizeScriptLeaseActor will run finalization or retry (depending on lease state)
// *Assumptions*:
// 1. Node with run script actor not under blocking load and may respond on ping in 30s
// 2. If node with run script actor send disconnects during one minute - node assumed as lost and script execution will be finalized
// 3. Node disconnects doesn't reapeat too often, so in one minute where will be 30s window without disconnects
// In practice it is almost always correct, but in some cases this may lead to infinite TLI retries on finalization or double start of script execution operation.
// If actor finished with SUCCESS when lease not expired now or TRunScriptActor still running.

class TFinalizeScriptLeaseActor final : public TActorBootstrapped<TFinalizeScriptLeaseActor> {
    using TBase = TActorBootstrapped<TFinalizeScriptLeaseActor>;
    using TRetryPolicy = IRetryPolicy<bool>;

    static constexpr TDuration CHECK_ALIVE_REQUEST_SOFT_TIMEOUT = TDuration::Seconds(30); // Advance on each retry of check alive
    static constexpr TDuration CHECK_ALIVE_REQUEST_HARD_TIMEOUT = TDuration::Seconds(60); // Hard timeout for all retries
    static constexpr ui64 MAX_CHECK_ALIVE_RETRIES = 50;

    enum class EWakeup {
        RetryCheckAlive,
        CheckAliveSoftTimeout,
        CheckAliveHardTimeout,
    };

public:
    // Lease check and script execution finalizations pipeline:
    // 1. Select lease status from database
    // 2. If lease expired and where is no finalization status - start lease check by sending TEvLeaseCheckRequest to run script actor
    // 3. If run script actor respond - lease not expired
    // 4. Otherwise - script execution is finished, continue script finalization

    struct TSettings {
        const ui64 Cookie = 0;
        const bool AllowRestart = true;
        const bool FailOnNotFound = true;
    };

    TFinalizeScriptLeaseActor(const TActorId& replyActorId, TString database, TString executionId,
        NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TSettings&& settings)
        : ReplyActorId(replyActorId)
        , Database(std::move(database))
        , ExecutionId(std::move(executionId))
        , QueryServiceConfig(std::move(queryServiceConfig))
        , Counters(std::move(counters))
        , Settings(std::move(settings))
    {
        Y_VALIDATE(!Settings.AllowRestart || Settings.FailOnNotFound, "Can not handle setting FailOnNotFound when restart allowed");
    }

    void Bootstrap() {
        const auto& checkerId = Register(TCheckLeaseStatusQueryActor::MakeRetry(SelfId(), Database, ExecutionId, TCheckLeaseStatusQueryActor::TSettings{}));
        KQP_PROXY_LOG_D("Bootstrap, Cookie: " << Settings.Cookie << ". Start TCheckLeaseStatusQueryActor " << checkerId);
        Become(&TThis::StateFunc);
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
            SubscribedOnSession = std::nullopt;
        }
        TBase::PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
        hFunc(TEvCheckAliveResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        hFunc(TEvScriptExecutionRestarted, HandleFinalized);
        hFunc(TEvScriptExecutionFinished, HandleFinalized);
    )

private:
    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        auto& event = *ev->Get();
        if (event.Status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_W("Failed to check lease " << ev->Sender << " status, Status: " << event.Status << ", Issues: " << event.Issues.ToOneLineString());
            return Reply(event.Status, std::move(event.Issues));
        }

        if (!event.EntryExists) {
            KQP_PROXY_LOG_W("Lease check " << ev->Sender << " finished with not found, Status: " << event.Status << ", Issues: " << event.Issues.ToOneLineString());
            return ReplyNotFound();
        }

        RunScriptActorId = event.RunScriptActorId;
        KQP_PROXY_LOG_D("Extracted script execution operation " << ev->Sender
            << ", Status: " << event.Status
            << ", Issues: " << event.Issues.ToOneLineString()
            << ", LeaseExpired: " << event.LeaseExpired
            << ", RetryRequired: " << event.RetryRequired
            << (event.OperationStatus ? ", OperationStatus: " + Ydb::StatusIds::StatusCode_Name(*event.OperationStatus) : "")
            << (event.ExecutionStatus ? ", ExecutionStatus: " + Ydb::Query::ExecStatus_Name(*event.ExecutionStatus) : "")
            << ", OperationIssues: " << event.OperationIssues.ToOneLineString()
            << (event.FinalizationStatus ? (TStringBuilder() << ", FinalizationStatus: " << static_cast<ui64>(*event.FinalizationStatus)) : TStringBuilder())
            << ", RunScriptActorId: " << RunScriptActorId
            << ", LeaseGeneration: " << event.LeaseGeneration);

        if (!event.LeaseExpired) {
            KQP_PROXY_LOG_N("Lease finalization skipped, lease not expired");
            LeaseVerified = true;
            return Reply();
        }

        if (event.RetryRequired) {
            if (Settings.AllowRestart) {
                const auto& restartActorId = Register(TRestartScriptOperationQuery::MakeRetry(SelfId(), Database, ExecutionId, event.LeaseGeneration, QueryServiceConfig, Counters));
                KQP_PROXY_LOG_N("Restarting script execution " << restartActorId << ", lease generation: " << event.LeaseGeneration);
            } else {
                KQP_PROXY_LOG_N("Lease finalization skipped, script execution wait retry");
                Reply();
            }
            return;
        }

        auto issues = event.OperationIssues;
        if (!issues) {
            issues.AddIssue(event.FinalizationStatus ? "Finalization is not complete" : "Lease expired");
        }

        // Script execution lease is expired but TRunScriptActor may be still running, so we should check it availability first

        const auto finalizationStatus = event.FinalizationStatus.GetOrElse(EFinalizationStatus::FS_ROLLBACK);
        const auto status = event.OperationStatus.GetOrElse(Ydb::StatusIds::UNAVAILABLE);

        auto execStatus = Ydb::Query::EXEC_STATUS_ABORTED;
        if (event.ExecutionStatus && IsIn({Ydb::Query::EXEC_STATUS_ABORTED, Ydb::Query::EXEC_STATUS_CANCELLED, Ydb::Query::EXEC_STATUS_COMPLETED, Ydb::Query::EXEC_STATUS_FAILED}, *event.ExecutionStatus)) {
            execStatus = *event.ExecutionStatus;
        }

        KQP_PROXY_LOG_W("Script execution lease is expired"
            << ", FinalizationStatus: " << finalizationStatus
            << ", Status: " << status
            << ", ExecStatus: " << Ydb::Query::ExecStatus_Name(execStatus)
            << ", Issues: " << issues.ToOneLineString()
            << ", LeaseGeneration: " << event.LeaseGeneration);

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
            event.LeaseGeneration,
            /* cancelledByUser */ false
        );

        CheckAliveFlags = IEventHandle::FlagTrackDelivery;
        if (RunScriptActorId.NodeId() != SelfId().NodeId()) {
            CheckAliveFlags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = RunScriptActorId.NodeId();
        }
        Send(RunScriptActorId, new TEvCheckAliveRequest(), CheckAliveFlags);
        Schedule(CHECK_ALIVE_REQUEST_SOFT_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveSoftTimeout)));
        Schedule(CHECK_ALIVE_REQUEST_HARD_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveHardTimeout)));
    }

    void Handle(TEvCheckAliveResponse::TPtr& ev) {
        if (WaitFinishQuery) {
            KQP_PROXY_LOG_W("Script execution " << ev->Sender << " lease was verified after started finalization");
        } else {
            KQP_PROXY_LOG_N("Script execution " << ev->Sender << " lease successfully verified");
            LeaseVerified = true;
            Reply();
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeup>(ev->Get()->Tag)) {
            case EWakeup::RetryCheckAlive:
                WaitRetryCheckAlive = false;
                CheckAliveRetries++;
                KQP_PROXY_LOG_D("Start check alive request #" << CheckAliveRetries + 1);
                Send(RunScriptActorId, new TEvCheckAliveRequest(), CheckAliveFlags);
                Schedule(CHECK_ALIVE_REQUEST_SOFT_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveSoftTimeout)));
                break;
            case EWakeup::CheckAliveSoftTimeout:
                KQP_PROXY_LOG_W("Deliver TRunScriptActor check alive request soft timeout, retry check alive");
                RetryCheckAlive(/* longDelay */ false);
                break;
            case EWakeup::CheckAliveHardTimeout:
                KQP_PROXY_LOG_W("Deliver TRunScriptActor check alive request hard timeout, start finalization");
                RunScriptFinalizeRequest();
                break;
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto reason = ev->Get()->Reason;
        if (reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
            // Here we know that script execution runtime state was advanced:
            // 1. Actor may by finished due to node restart
            // 2. Script execution may be already under finalization
            // 3. Script execution may be retried
            // We will start finalization only in first and second cases
            KQP_PROXY_LOG_W("Got delivery problem to " << ev->Sender << " TRunScriptActor not found, start finalization");
            RunScriptFinalizeRequest();
        } else {
            KQP_PROXY_LOG_W("Got delivery problem to " << ev->Sender << ", node with TRunScriptActor unavailable, reason: " << reason);
            RetryCheckAlive(/* longDelay */ true);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        KQP_PROXY_LOG_W("Node " << ev->Get()->NodeId << " with TRunScriptActor was disconnected, retry check alive");
        RetryCheckAlive(/* longDelay */ false);
    }

    void HandleFinalized(TEvScriptExecutionRestarted::TPtr& ev) {
        const auto status = ev->Get()->Status;
        const auto& issues = ev->Get()->Issues;
        if (status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_W("Failed to restart " << ev->Sender << " script execution operation, status: " << status << ", issues: " << issues.ToOneLineString());
        } else {
            KQP_PROXY_LOG_D("Successfully restarted " << ev->Sender << " script execution operation");
        }

        Reply(status, AddRootIssue("Restart script execution operation", issues, /* addEmptyRoot  */ false));
    }

    void HandleFinalized(TEvScriptExecutionFinished::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("Failed to finalize " << ev->Sender << " script execution operation, status: " << status << ", issues: " << issues.ToOneLineString());
            return Reply(status, AddRootIssue("Finish script execution operation failed", issues));
        }

        const auto& info = ev->Get()->Info;
        KQP_PROXY_LOG_D("Successfully finalized " << ev->Sender << " script execution operation, entry exists: " << info.ExecutionEntryExists);

        if (info.ExecutionEntryExists) {
            Reply();
        } else {
            ReplyNotFound();
        }
    }

    void RunScriptFinalizeRequest() {
        if (WaitFinishQuery) {
            return;
        }

        WaitFinishQuery = true;
        Y_VALIDATE(ScriptFinalizeRequest, "Script finalize request is not set");

        const auto& description = ScriptFinalizeRequest->Description;
        KQP_PROXY_LOG_D("Run script finalization request"
            << ", FinalizationStatus: " << static_cast<i32>(description.FinalizationStatus)
            << ", OperationStatus: " << description.OperationStatus
            << ", ExecStatus: " << Ydb::Query::ExecStatus_Name(description.ExecStatus)
            << ", Issues: " << description.Issues.ToOneLineString()
            << ", LeaseGeneration: " << description.LeaseGeneration);

        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), ScriptFinalizeRequest.release());
    }

    void RetryCheckAlive(bool longDelay) {
        if (WaitFinishQuery || WaitRetryCheckAlive) {
            // Already finished checks
            return;
        }

        if (!CheckAliveRetryState) {
            CheckAliveRetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                [](bool longDelay) {
                    return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(300),
                TDuration::Seconds(1),
                MAX_CHECK_ALIVE_RETRIES
            )->CreateRetryState();
        }

        if (const auto delay = CheckAliveRetryState->GetNextRetryDelay(longDelay)) {
            KQP_PROXY_LOG_D("Schedule retry check alive in " << *delay);
            Schedule(*delay, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RetryCheckAlive)));
            WaitRetryCheckAlive = true;
        } else {
            KQP_PROXY_LOG_E("Retry limit " << MAX_CHECK_ALIVE_RETRIES << " exceeded for TRunScriptActor check alive, start finalization");
            RunScriptFinalizeRequest();
        }
    }

    void ReplyNotFound() {
        KQP_PROXY_LOG_W("Script execution operation not found");
        EntryExists = false;
        return Reply(Settings.FailOnNotFound ? Ydb::StatusIds::NOT_FOUND : Ydb::StatusIds::SUCCESS, "Script execution operation not found");
    }

    void Reply() {
        KQP_PROXY_LOG_D("Reply success");
        Send(ReplyActorId, new TEvPrivate::TEvFinalizeScriptLeaseResult(Ydb::StatusIds::SUCCESS, {
            .LeaseVerified = LeaseVerified,
            .ExecutionEntryExists = EntryExists,
        }), /* flags */ 0, Settings.Cookie);
        PassAway();
    }

    void Reply(const Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, {NYql::TIssue(message)});
    }

    void Reply(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        KQP_PROXY_LOG_W("Reply " << status << ", issues: " << issues.ToOneLineString());
        Send(ReplyActorId, new TEvPrivate::TEvFinalizeScriptLeaseResult(status, {
            .LeaseVerified = LeaseVerified,
            .ExecutionEntryExists = EntryExists,
        }, std::move(issues)), /* flags */ 0, Settings.Cookie);
        PassAway();
    }

    TString LogPrefix() const {
        auto result = TStringBuilder() << "[TFinalizeScriptLeaseActor] OwnerId: " << ReplyActorId << " ActorId: " << SelfId() << " Database: " << Database << " ExecutionId: " << ExecutionId;
        if (RunScriptActorId) {
            result << " RunScriptActorId: " << RunScriptActorId;
        }
        return result << ". ";
    }

    const TActorId ReplyActorId;
    const TString Database;
    const TString ExecutionId;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    const TSettings Settings;
    TActorId RunScriptActorId;
    std::unique_ptr<TEvScriptFinalizeRequest> ScriptFinalizeRequest;
    bool WaitFinishQuery = false;
    bool WaitRetryCheckAlive = false;
    bool LeaseVerified = false;
    bool EntryExists = true;
    std::optional<ui32> SubscribedOnSession;
    ui64 CheckAliveFlags = 0;
    ui64 CheckAliveRetries = 0;
    TRetryPolicy::IRetryState::TPtr CheckAliveRetryState;
};

// Cleanup all meta information and results corresponding to script execution operation. NOTE: results removed on best effort

class TForgetScriptExecutionOperationQueryActor : public TQueryBase<TForgetScriptExecutionOperationQueryActor, TEvForgetScriptExecutionOperationResponse> {
    static constexpr i32 NUMBER_ROWS_IN_BATCH = 100'000;

    struct TResultSetInfo {
        i32 Id = 0;
        i64 MinRowId = 0;
        i64 MaxRowId = 0;
    };

public:
    TForgetScriptExecutionOperationQueryActor(TString executionId, TString database, std::optional<std::vector<Ydb::Query::ResultSetMeta>> resultSetMetas)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId)})
        , HasResultsInfo(resultSetMetas.has_value())
    {
        if (resultSetMetas) {
            ResultSets.reserve(resultSetMetas->size());
            for (ui64 i = 0; i < resultSetMetas->size(); ++i) {
                ResultSets.push_back({
                    .Id = static_cast<i32>(i),
                    .MaxRowId = static_cast<i64>(resultSetMetas->at(i).number_rows())
                });
            }
        }
    }

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
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

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnOperationDeleted, "Forget script execution operation");
        RunDataQuery(sql, &params);
    }

    void OnOperationDeleted() {
        SendResponse(Ydb::StatusIds::SUCCESS, {});

        if (HasResultsInfo) {
            DeleteScriptResults();
            return;
        }

        constexpr char sql[] = R"(
            -- TForgetScriptExecutionOperationQueryActor::OnOperationDeleted
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                result_set_id,
                MIN(row_id) AS min_row_id,
                MAX(row_id) AS max_row_id
            FROM `.metadata/result_sets`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL)
            GROUP BY result_set_id;
        )";

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::DeleteScriptResults, "Get results info");
        RunStreamQuery(sql, &params);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        for (NYdb::TResultSetParser result(std::move(resultSet)); result.TryNextRow();) {
            auto& resultSet = ResultSets.emplace_back();

            if (const auto resultSetId = result.ColumnParser("result_set_id").GetOptionalInt32()) {
                resultSet.Id = *resultSetId;
            } else {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set id is not specified");
                return;
            }

            if (const auto minRowId = result.ColumnParser("min_row_id").GetOptionalInt64()) {
                resultSet.MinRowId = *minRowId;
            } else {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Result set #" << resultSet.Id << " min row id is not found");
                return;
            }

            if (const auto maxRowId = result.ColumnParser("max_row_id").GetOptionalInt64()) {
                resultSet.MaxRowId = *maxRowId;
            } else {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Result set #" << resultSet.Id << " max row id is not found");
                return;
            }
        }
    }

    void DeleteScriptResults() {
        KQP_PROXY_LOG_D("Do deleting of script results, amount result sets: " << ResultSets.size());

        if (ResultSets.empty()) {
            Finish();
            return;
        }

        auto& resultSet = ResultSets.back();
        KQP_PROXY_LOG_D("Deleting rows from result set #" << resultSet.Id << ", remains rows range [" << resultSet.MinRowId << "; " << resultSet.MaxRowId << "]");

        constexpr char sql[] = R"(
            -- TForgetScriptExecutionOperationQueryActor::DeleteScriptResults
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $min_row_id AS Int64;
            DECLARE $max_row_id AS Int64;

            UPSERT INTO `.metadata/result_sets`
            SELECT
                $database AS database,
                $execution_id AS execution_id,
                $result_set_id AS result_set_id,
                row_id,
                CurrentUtcTimestamp() AS expire_at
            FROM (
                SELECT ListFromRange($min_row_id, $max_row_id + 1) AS row_id
            ) FLATTEN BY (row_id);
        )";

        const i64 minRowId = std::max(resultSet.MaxRowId - NUMBER_ROWS_IN_BATCH, resultSet.MinRowId);
        auto params = CreateParams();
        params
            .AddParam("$result_set_id")
                .Int32(resultSet.Id)
                .Build()
            .AddParam("$min_row_id")
                .Int64(minRowId)
                .Build()
            .AddParam("$max_row_id")
                .Int64(resultSet.MaxRowId)
                .Build();

        SetQueryResultHandler(&TThis::OnResultsDeleted, TStringBuilder() << "Delete script results in range [" << minRowId << "; " << resultSet.MaxRowId << "]");
        RunDataQuery(sql, &params);
    }

    void OnResultsDeleted() {
        Y_ENSURE(!ResultSets.empty());

        auto& resultSet = ResultSets.back();
        resultSet.MaxRowId -= NUMBER_ROWS_IN_BATCH + 1;
        if (resultSet.MaxRowId < resultSet.MinRowId) {
            KQP_PROXY_LOG_D("Deleting of script result set #" << resultSet.Id << " is finished, remains result sets: " << ResultSets.size() - 1);
            ResultSets.pop_back();
        }

        DeleteScriptResults();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        SendResponse(status, std::move(issues));
    }

    void SendResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        if (ResponseSent) {
            return;
        }

        ResponseSent = true;
        Send(Owner, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)));
    }

    const bool HasResultsInfo = false;
    std::vector<TResultSetInfo> ResultSets;
    bool ResponseSent = false;
};

class TForgetScriptExecutionOperationActor : public TActorBootstrapped<TForgetScriptExecutionOperationActor> {
public:
    explicit TForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr&& ev)
        : Request(std::move(ev))
        , Database(Request->Get()->Database)
    {}

    void Bootstrap() {
        auto userSID = Request->Get()->UserSID;
        if (const auto& error = CheckScriptExecutionAccess(userSID)) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, error);
        }

        TString error;
        auto executionId = NKqp::ScriptExecutionIdFromOperation(Request->Get()->OperationId, error);
        if (!executionId) {
            Reply(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        ExecutionId = std::move(*executionId);

        const auto& checkerId = Register(TCheckLeaseStatusQueryActor::MakeRetry(SelfId(), Database, ExecutionId, TCheckLeaseStatusQueryActor::TSettings{.UserSID = userSID}));
        KQP_PROXY_LOG_D("Bootstrap. Start TCheckLeaseStatusQueryActor " << checkerId);

        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
        hFunc(TEvCancelScriptExecutionOperationResponse, Handle);
        hFunc(TEvForgetScriptExecutionOperationResponse, Handle);
    )

private:
    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("Lease check " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            Reply(status, AddRootIssue("Check lease status", issues));
            return;
        }

        ResultSetMetas = std::move(ev->Get()->ResultSetMetas);
        ExecutionEntryExists = ev->Get()->EntryExists;

        if (ExecutionEntryExists && (!ev->Get()->OperationStatus || ev->Get()->FinalizationStatus || ev->Get()->HasRetryPolicy)) {
            if (Request->Get()->Settings.CancelIfRunning) {
                KQP_PROXY_LOG_I("Lease check " << ev->Sender << " finished, but operation is still running. Cancel it");
                Send(MakeKqpProxyID(SelfId().NodeId()), new TEvCancelScriptExecutionOperation(Database, Request->Get()->OperationId, Request->Get()->UserSID, {
                    .FailOnNotFound = false,
                    .FailOnAlreadyStopped = false,
                }));
            } else {
                KQP_PROXY_LOG_I("Lease check " << ev->Sender << " finished, but operation is still running");
                Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Operation is still running");
            }
            return;
        }

        KQP_PROXY_LOG_D("Lease check " << ev->Sender << " success, execution entry exists: " << ExecutionEntryExists);
        StartForgetOperation();
    }

    void Handle(TEvCancelScriptExecutionOperationResponse::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("Cancel operation " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            Reply(status, AddRootIssue("Cancel script execution operation", issues));
            return;
        }

        ExecutionEntryExists = ev->Get()->ExecutionEntryExists;

        KQP_PROXY_LOG_D("Cancel script execution operation " << ev->Sender << " finished, execution entry exists: " << ExecutionEntryExists);
        StartForgetOperation();
    }

    void Handle(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        KQP_PROXY_LOG_D("Forget operation " << ev->Sender << " finished " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        Reply(ev->Get()->Status, AddRootIssue("Forget script execution operation", ev->Get()->Issues, /* addEmptyRoot */ false));
    }

    void StartForgetOperation() {
        const auto& forgetId = Register(TForgetScriptExecutionOperationQueryActor::MakeRetry(SelfId(), ExecutionId, Database, std::move(ResultSetMetas)));
        KQP_PROXY_LOG_D("Start TForgetOperationRetryActor " << forgetId);
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        if (!ExecutionEntryExists && Request->Get()->Settings.FailOnNotFound && status == Ydb::StatusIds::SUCCESS) {
            status = Ydb::StatusIds::NOT_FOUND;
            if (!issues) {
                issues.AddIssue("Script execution operation not found");
            }
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_D("Reply success");
        } else {
            KQP_PROXY_LOG_W("Reply " << status << ", issues: " << issues.ToOneLineString());
        }

        Send(Request->Sender, new TEvForgetScriptExecutionOperationResponse(status, std::move(issues)), /* flags */ 0, Request->Cookie);
        PassAway();
    }

    void Reply(const Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, {NYql::TIssue(message)});
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TForgetScriptExecutionOperationActor] OwnerId: " << Request->Sender << " ActorId: " << SelfId() << " Database: " << Database << " ExecutionId: " << ExecutionId << ". ";
    }

    const TEvForgetScriptExecutionOperation::TPtr Request;
    const TString& Database;
    TString ExecutionId;
    std::optional<std::vector<Ydb::Query::ResultSetMeta>> ResultSetMetas;
    bool ExecutionEntryExists = true;
};

// Fetch current state of script execution operation, used by gRPC API get-operation and streaming queries sys view

class TGetScriptExecutionOperationQueryActor final : public TQueryWithAccessValidationBase<TGetScriptExecutionOperationQueryActor, TEvGetScriptExecutionOperationResponse> {
public:
    TGetScriptExecutionOperationQueryActor(TString database, NOperationId::TOperationId operationId, std::optional<TString> userSID, const bool failOnNotFound, const ui64 cookie)
        : TQueryWithAccessValidationBase(__func__, {.Database = std::move(database)}, std::move(userSID))
        , OperationId(std::move(operationId))
        , FailOnNotFound(failOnNotFound)
        , Cookie(cookie)
    {}

private:
    void OnRunQuery() override {
        if (!ValidateUserSID()) {
            return;
        }

        TString error;
        auto executionId = ScriptExecutionIdFromOperation(OperationId, error);
        if (!executionId) {
            Finish(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        ExecutionId = std::move(*executionId);
        Metadata.set_execution_id(ExecutionId);
        SetOperationInfo(OperationName, CreateTraceId({.Database = Database, .ExecutionId = ExecutionId}));

        constexpr char sql[] = R"(
            -- TGetScriptExecutionOperationQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                finalization_status,
                query_text,
                syntax,
                execution_mode,
                result_set_metas,
                plan,
                plan_compressed,
                plan_compression_method,
                issues,
                transient_issues,
                stats,
                ast,
                ast_compressed,
                ast_compression_method,
                graph_compressed IS NOT NULL AS has_graph,
                retry_state,
                user_token
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_deadline,
                lease_state
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        auto params = CreateParams();
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
                ExecutionEntryExists = false;
                Finish(FailOnNotFound ? Ydb::StatusIds::NOT_FOUND : Ydb::StatusIds::SUCCESS, "Script execution operation not found");
                return;
            }

            if (!ValidateAccess(result)) {
                return;
            }

            if (const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32()) {
                OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            }

            if (const auto executionStatus = result.ColumnParser("execution_status").GetOptionalInt32()) {
                Metadata.set_exec_status(static_cast<Ydb::Query::ExecStatus>(*executionStatus));
            }

            if (auto sql = result.ColumnParser("query_text").GetOptionalUtf8()) {
                *Metadata.mutable_script_content()->mutable_text() = std::move(*sql);
            }

            if (const auto syntax = result.ColumnParser("syntax").GetOptionalInt32()) {
                Metadata.mutable_script_content()->set_syntax(static_cast<Ydb::Query::Syntax>(*syntax));
            }

            if (const auto executionMode = result.ColumnParser("execution_mode").GetOptionalInt32()) {
                Metadata.set_exec_mode(static_cast<Ydb::Query::ExecMode>(*executionMode));
            }

            if (const auto& serializedStats = result.ColumnParser("stats").GetOptionalJsonDocument()) {
                NJson::TJsonValue statsJson;
                if (!NJson::ReadJsonTree(*serializedStats, &statsJson)) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script execution stats is corrupted");
                    return;
                }

                NProtobufJson::Json2Proto(statsJson, *Metadata.mutable_exec_stats(), NProtobufJson::TJson2ProtoConfig());
            }

            if (auto plan = result.ColumnParser("plan").GetOptionalJsonDocument()) {
                *Metadata.mutable_exec_stats()->mutable_query_plan() = std::move(*plan);
            } else if (auto plan = ParseCompressedColumn("plan", result)) {
                *Metadata.mutable_exec_stats()->mutable_query_plan() = std::move(*plan);
            } else {
                Metadata.mutable_exec_stats()->set_query_plan("{}");
            }

            if (auto ast = result.ColumnParser("ast").GetOptionalUtf8()) {
                *Metadata.mutable_exec_stats()->mutable_query_ast() = std::move(*ast);
            } else if (auto ast = ParseCompressedColumn("ast", result)) {
                *Metadata.mutable_exec_stats()->mutable_query_ast() = std::move(*ast);
            }

            OperationIssues = ParseScriptExecutionIssues(result);

            if (auto resultSetMetas = ParseResultSetMetas(result)) {
                Metadata.mutable_result_sets_meta()->Add(std::make_move_iterator(resultSetMetas->begin()), std::make_move_iterator(resultSetMetas->end()));
            } else {
                return;
            }

            if (auto retryState = ParseRetryState(result)) {
                RetryState = std::move(*retryState);
            } else {
                return;
            }

            StateSaved = result.ColumnParser("has_graph").GetBool();
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (result.TryNextRow()) {
                const auto leaseDeadline = result.ColumnParser("lease_deadline").GetOptionalTimestamp();
                if (!leaseDeadline) {
                    Finish(Ydb::StatusIds::INTERNAL_ERROR, "Lease deadline not found");
                    return;
                }

                LeaseStatus = static_cast<ELeaseState>(result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning)));
                if (*LeaseStatus == ELeaseState::WaitRetry) {
                    SuspendedUntil = *leaseDeadline;
                } else if (*LeaseStatus == ELeaseState::ScriptFinalizing) {
                    Metadata.set_exec_status(Ydb::Query::EXEC_STATUS_RUNNING); // Waiting for finalization
                }
            } else if (!OperationStatus) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state, lease not found for running query");
                return;
            }
        }

        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Finish"
            << ", OperationStatus: " << (OperationStatus ? Ydb::StatusIds::StatusCode_Name(*OperationStatus) : "null")
            << ", LeaseStatus: " << (LeaseStatus ? static_cast<i64>(*LeaseStatus) : -1));

        bool ready = !!OperationStatus;
        if (LeaseStatus && OperationStatus) {
            ready = false;
            NYql::TIssue finalizationIssue(TStringBuilder() << "Execution finished with status " << *OperationStatus << " and wait " << (*LeaseStatus == ELeaseState::ScriptFinalizing ? "finalization" : "retry"));
            finalizationIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
            OperationIssues.AddIssue(finalizationIssue);
            OperationStatus = std::nullopt;
        }

        if (!OperationStatus || status != Ydb::StatusIds::SUCCESS) {
            OperationStatus = status;
        }

        if (issues) {
            auto newIssues = AddRootIssue("Get script execution operation info", issues);
            newIssues.AddIssues(AddRootIssue("Script execution issues", OperationIssues, /* addEmptyRoot */ false));
            std::swap(OperationIssues, newIssues);
        }

        google::protobuf::Any metadata;
        metadata.PackFrom(Metadata);

        Send(Owner, new TEvGetScriptExecutionOperationResponse(*OperationStatus, {
            .Metadata = std::move(metadata),
            .Ready = ready,
            .StateSaved = StateSaved,
            .ExecutionEntryExists = ExecutionEntryExists,
            .RetryCount = RetryState.GetRetryCounter(),
            .LastFailAt = NProtoInterop::CastFromProto(RetryState.GetRetryCounterUpdatedAt()),
            .SuspendedUntil = SuspendedUntil,
            .RequestStatus = status,
        }, std::move(OperationIssues)), /* flags */ 0, Cookie);
    }

    static std::optional<TString> ParseCompressedColumn(const TString& columnName, NYdb::TResultSetParser& result) {
        if (std::optional<TString> resultCompressed = result.ColumnParser(TStringBuilder() << columnName << "_compressed").GetOptionalString()) {
            if (const std::optional<TString>& resultCompressionMethod = result.ColumnParser(TStringBuilder() << columnName << "_compression_method").GetOptionalUtf8()) {
                const TCompressor compressor(*resultCompressionMethod);
                return compressor.Decompress(*resultCompressed);
            }

            return resultCompressed;
        }

        return std::nullopt;
    }

    const NOperationId::TOperationId OperationId;
    const bool FailOnNotFound = true;
    const ui64 Cookie = 0;
    NYql::TIssues OperationIssues;
    Ydb::Query::ExecuteScriptMetadata Metadata;
    NKikimrKqp::TScriptExecutionRetryState RetryState;
    std::optional<Ydb::StatusIds::StatusCode> OperationStatus;
    std::optional<ELeaseState> LeaseStatus;
    bool StateSaved = false;
    bool ExecutionEntryExists = true;
    TInstant SuspendedUntil;
};

// List all available script execution operations with paging, used by gRPC API list-operations

class TListScriptExecutionOperationsQuery final : public TQueryWithAccessValidationBase<TListScriptExecutionOperationsQuery, TEvListScriptExecutionOperationsResponse> {
public:
    TListScriptExecutionOperationsQuery(TString database, std::optional<TString> userSID, TString pageToken, const ui64 pageSize)
        : TQueryWithAccessValidationBase(__func__, {.Database = std::move(database)}, std::move(userSID))
        , PageToken(std::move(pageToken))
        , PageSize(ClampVal<ui64>(pageSize, 1, 100))
    {}

private:
    static std::pair<TInstant, TString> ParsePageToken(const TString& token) {
        const size_t p = token.find('|');
        if (p == TString::npos) {
            throw std::runtime_error("Invalid page token");
        }

        const ui64 ts = FromString(TStringBuf(token).SubString(0, p));
        return {TInstant::MicroSeconds(ts), token.substr(p + 1)};
    }

    static TString MakePageToken(const TInstant ts, const std::string& executionId) {
        return TStringBuilder() << ts.MicroSeconds() << '|' << executionId;
    }

    void OnRunQuery() override {
        KQP_PROXY_LOG_D("List with PageToken: " << PageToken << ", PageSize: " << PageSize);

        if (!ValidateUserSID()) {
            return;
        }

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
            DECLARE $user_sid AS Optional<Text>;

            SELECT
                execution_id,
                start_ts,
                operation_status,
                execution_status,
                finalization_status,
                query_text,
                syntax,
                execution_mode,
                issues,
                transient_issues
            FROM `.metadata/script_executions`
            WHERE database = $database AND (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL)
        )";
        if (UserSID) {
            sql << R"(
                AND (user_token IS NULL OR user_token = $user_sid)
            )";
        }
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
                .Build()
            .AddParam("$user_sid")
                .OptionalUtf8(UserSID)
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

        RunDataQuery(sql, &params, TTxControl::BeginAndCommitTx(/* snapshotRead */ true));
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        Operations.reserve(result.RowsCount());

        while (result.TryNextRow()) {
            auto executionId = result.ColumnParser("execution_id").GetOptionalUtf8();
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
            const auto finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();

            Ydb::Query::ExecuteScriptMetadata metadata;
            *metadata.mutable_execution_id() = std::move(*executionId);

            if (const auto executionStatus = result.ColumnParser("execution_status").GetOptionalInt32()) {
                metadata.set_exec_status(static_cast<Ydb::Query::ExecStatus>(*executionStatus));
            }

            if (auto sql = result.ColumnParser("query_text").GetOptionalUtf8()) {
                *metadata.mutable_script_content()->mutable_text() = std::move(*sql);
            }

            if (const auto syntax = result.ColumnParser("syntax").GetOptionalInt32()) {
                metadata.mutable_script_content()->set_syntax(static_cast<Ydb::Query::Syntax>(*syntax));
            }

            if (const auto executionMode = result.ColumnParser("execution_mode").GetOptionalInt32()) {
                metadata.set_exec_mode(static_cast<Ydb::Query::ExecMode>(*executionMode));
            }

            Ydb::Operations::Operation op;
            op.set_id(ScriptExecutionOperationFromExecutionId(metadata.execution_id()));
            op.set_ready(operationStatus.has_value() && !finalizationStatus.has_value());
            if (operationStatus) {
                op.set_status(static_cast<Ydb::StatusIds::StatusCode>(*operationStatus));
            }
            for (const auto& issue : ParseScriptExecutionIssues(result)) {
                NYql::IssueToMessage(issue, op.add_issues());
            }
            op.mutable_metadata()->PackFrom(metadata);

            Operations.emplace_back(std::move(op));
        }

        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvListScriptExecutionOperationsResponse(status, std::move(issues), std::move(NextPageToken), std::move(Operations)));
    }

    const TString PageToken;
    const ui64 PageSize = 0;
    TString NextPageToken;
    std::vector<Ydb::Operations::Operation> Operations;
};

// To cancel script execution operation what currently waiting for retry we should just reset retry policy and update status

class TResetScriptExecutionRetriesQueryActor final : public TQueryBase<TResetScriptExecutionRetriesQueryActor, TEvResetScriptExecutionRetriesResponse> {
public:
    TResetScriptExecutionRetriesQueryActor(TString database, TString executionId)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId)})
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
            -- TResetScriptExecutionRetriesQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                finalization_status,
                retry_state
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);

            SELECT
                lease_state
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetQueryInfo, "Get query info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        std::optional<i32> executionStatus;
        std::optional<i32> operationStatus;
        bool hasRetries = false;

        // Script execution info
        if (NYdb::TResultSetParser result(ResultSets[0]); result.TryNextRow()) {
            executionStatus = result.ColumnParser("execution_status").GetOptionalInt32();
            operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
            if (!operationStatus || !executionStatus) {
                OperationStillRunning = true;
                KQP_PROXY_LOG_N("Can not reset retry state then operation is running now");
                return Finish();
            }

            if (result.ColumnParser("finalization_status").GetOptionalInt32()) {
                OperationStillRunning = true;
                KQP_PROXY_LOG_N("Can not reset retry state then operation is finalizing now");
                return Finish();
            }

            if (RetryState = ParseRetryState(result)) {
                hasRetries = !RetryState->GetRetryPolicyMapping().empty();
                RetryState->ClearRetryPolicyMapping();
            } else {
                return;
            }
        }

        // Lease info
        if (NYdb::TResultSetParser result(ResultSets[1]); result.TryNextRow()) {
            if (const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32()) {
                if (static_cast<ELeaseState>(*leaseState) != ELeaseState::WaitRetry) {
                    OperationStillRunning = true;
                    KQP_PROXY_LOG_N("Can not reset retry state then operation is running or not finalized, lease state: " << static_cast<ELeaseState>(*leaseState));
                    return Finish();
                }

                DropLease = true;
            }
        }

        if (!RetryState && !DropLease) {
            ExecutionEntryExists = false;
            return Finish();
        }

        if (!DropLease) {
            Y_VALIDATE(RetryState && executionStatus && operationStatus, "Unexpected operation info");

            if (!hasRetries && static_cast<Ydb::Query::ExecStatus>(*executionStatus) == Ydb::Query::EXEC_STATUS_CANCELLED && static_cast<Ydb::StatusIds_StatusCode>(*operationStatus) == Ydb::StatusIds::CANCELLED) {
                AlreadyStopped = true;
                return Finish();
            }
        }

        DropRetryState();
    }

    void DropRetryState() {
        auto sql = TStringBuilder() << R"(
            -- TResetScriptExecutionRetriesQueryActor::DropRetryState
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $retry_state AS Optional<JsonDocument>;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
        )";

        if (RetryState) {
            sql << R"(
                UPSERT INTO `.metadata/script_executions` (
                    database, execution_id, retry_state, operation_status, execution_status
                ) VALUES (
                    $database, $execution_id, $retry_state, $operation_status, $execution_status
                );
            )";
        }

        if (DropLease) {
            sql << R"(
                DELETE FROM `.metadata/script_execution_leases`
                WHERE database = $database AND execution_id = $execution_id;
            )";
        }

        auto params = CreateParams();
        params
            .AddParam("$retry_state")
                .OptionalJsonDocument(RetryState ? std::optional<std::string>(NProtobufJson::Proto2Json(*RetryState, NProtobufJson::TProto2JsonConfig())) : std::nullopt)
                .Build()
            .AddParam("$operation_status")
                .Int32(Ydb::StatusIds::CANCELLED)
                .Build()
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_CANCELLED)
                .Build();

        SetQueryResultHandler(&TThis::OnQueryResult, "Drop retry state");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvResetScriptExecutionRetriesResponse(status, {
            .OperationStillRunning = OperationStillRunning,
            .ExecutionEntryExists = ExecutionEntryExists,
            .AlreadyStopped = AlreadyStopped,
        }, std::move(issues)));
    }

    std::optional<NKikimrKqp::TScriptExecutionRetryState> RetryState;
    bool DropLease = false;
    bool OperationStillRunning = false;
    bool ExecutionEntryExists = true;
    bool AlreadyStopped = false;
};

// Script execution cancellation with handling runtime and retry cancellation
// *Assumptions*:
// 1. Retry backoff large enough to cancel script execution operation between retries
// 2. System not under high load so data transaction can not be executed during backoff period

class TCancelScriptExecutionOperationActor final : public TActorBootstrapped<TCancelScriptExecutionOperationActor> {
    using TBase = TActorBootstrapped<TCancelScriptExecutionOperationActor>;
    using TRetryPolicy = IRetryPolicy<>;

    static constexpr ui64 MAX_CANCEL_RETRIES = 10;

public:
    TCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr&& ev, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : Request(std::move(ev))
        , QueryServiceConfig(std::move(queryServiceConfig))
        , Counters(std::move(counters))
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        KQP_PROXY_LOG_D("Bootstrap");

        UserSID = Request->Get()->UserSID;
        if (const auto& error = CheckScriptExecutionAccess(UserSID)) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, error);
        }

        TString error;
        auto executionId = ScriptExecutionIdFromOperation(Request->Get()->OperationId, error);
        if (!executionId) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, error);
        }

        ExecutionId = std::move(*executionId);
        GetScriptExecutionOperationStatus();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLeaseCheckResult, Handle);
        hFunc(TEvResetScriptExecutionRetriesResponse, Handle);
        hFunc(TEvPrivate::TEvFinalizeScriptLeaseResult, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionResponse, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        sFunc(TEvents::TEvWakeup, GetScriptExecutionOperationStatus);
    )

    void PassAway() override {
        ResetSessionSubscribe();
        TBase::PassAway();
    }

private:
    void Handle(TEvPrivate::TEvLeaseCheckResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("Check lease " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            return Reply(status, AddRootIssue("Fetch script execution info failed", issues));
        }

        if (!ev->Get()->EntryExists) {
            return ReplyNotFound();
        }

        RunScriptActor = ev->Get()->RunScriptActorId;
        HasRetryPolicy = ev->Get()->HasRetryPolicy;
        const auto& operationStatus = ev->Get()->OperationStatus;
        const auto& finalizationStatus = ev->Get()->FinalizationStatus;
        const auto leaseExpired = ev->Get()->LeaseExpired;
        KQP_PROXY_LOG_D("Check lease " << ev->Sender << " success"
            << ", operation status: " << (operationStatus ? Ydb::StatusIds::StatusCode_Name(*operationStatus) : "<null>")
            << ", finalization status: " << (finalizationStatus ? ToString(*finalizationStatus) : "<null>")
            << ", has retries: " << HasRetryPolicy
            << ", lease expired: " << leaseExpired);

        // Possible script execution states:
        // 1. Is running now
        // 2. Finalization in progress
        // 3. Waiting for retry
        // 4. Script execution finished

        if (operationStatus && !finalizationStatus) {
            // Script execution operation was finished, and there is not finalization stage or finalization finished

            if (!HasRetryPolicy) {
                // No retries is planned for script execution => considered as finalized
                return ReplyAlreadyStopped();
            }

            // Reset script execution retries and mark it as cancelled
            ResetRetryPolicy();
        } else if (leaseExpired) {
            // Script execution still in running state but most likely TRunScriptActor was lost, so we should verify it existence
            FinalizeScriptLease();
        } else {
            // Script execution is running or finalization in progress, send cancel to TRunScriptActor
            SendCancelToRunScriptActor();
        }
    }

    void Handle(TEvResetScriptExecutionRetriesResponse::TPtr& ev) {
        // In case of TLI (status ABORTED) retry cancel operation
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::ABORTED) {
            KQP_PROXY_LOG_W("Reset retry state " << ev->Sender << " failed " << status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            return Reply(status, AddRootIssue("Reset retry state failed", ev->Get()->Issues));
        }

        const auto& issues = ev->Get()->Issues;
        const auto& info = ev->Get()->Info;
        KQP_PROXY_LOG_D("Reset retry state " << ev->Sender << " finished " << status
            << ", execution entry exists: " << info.ExecutionEntryExists
            << ", operation still running: " << info.OperationStillRunning
            << ", already stopped: " << info.AlreadyStopped
            << ", issues: " << issues.ToOneLineString());

        if (status == Ydb::StatusIds::SUCCESS) {
            if (!info.ExecutionEntryExists) {
                // Script execution was removed during cancellation
                return ReplyNotFound();
            }

            if (info.AlreadyStopped) {
                // Script execution was already cancelled by another operation
                return ReplyAlreadyStopped();
            }

            if (!info.OperationStillRunning) {
                // Script execution was successfully marked as cancelled
                return Reply(Ydb::StatusIds::SUCCESS);
            }
        }

        // Race with script execution retry, retry immediately because we already know that state of script execution was changed
        ScheduleRetryOrReply(status, AddRootIssue("Failed to cancel script execution, query execution is under retry", issues), /* immediate */ true);
    }

    void Handle(TEvPrivate::TEvFinalizeScriptLeaseResult::TPtr& ev) {
        // In case of TLI (status ABORTED) retry cancel operation
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::ABORTED) {
            KQP_PROXY_LOG_W("Finalize script execution " << ev->Sender << " failed " << status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            return Reply(status, AddRootIssue("Finalize script execution failed", ev->Get()->Issues));
        }

        const auto& issues = ev->Get()->Issues;
        const auto& info = ev->Get()->Info;
        KQP_PROXY_LOG_D("Finalize script execution " << ev->Sender << " finished " << status
            << ", lease verified: " << info.LeaseVerified
            << ", execution entry exists: " << info.ExecutionEntryExists
            << ", issues: " << issues.ToOneLineString());

        if (status == Ydb::StatusIds::SUCCESS) {
            if (!info.ExecutionEntryExists) {
                // Script execution was removed during finalization
                return ReplyNotFound();
            }

            if (info.LeaseVerified) {
                // TRunScriptActor still alive, so we can send cancel request to it
                return SendCancelToRunScriptActor();
            }

            // State of script execution changed (most likely it was finalized), report retry attempt and continue cancel
        }

        ScheduleRetryOrReply(status, AddRootIssue("Failed to cancel script execution, query execution finalization was not successful", issues), /* immediate */ true);
    }

    void Handle(TEvKqp::TEvCancelScriptExecutionResponse::TPtr& ev) {
        if (ev->Cookie != CancellationCookie || !RunScriptActor) {
            // Skip late events from previous retries
            return;
        }
        RunScriptActor = {};

        NYql::TIssues issues;
        NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);

        // In case of TLI (status ABORTED) retry cancel operation
        const auto status = ev->Get()->Record.GetStatus();
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::ABORTED) {
            KQP_PROXY_LOG_W("Script execution cancel failed " << status << " from RunScriptActor: " << ev->Sender << ", issues: " << issues.ToOneLineString());
            return Reply(status, AddRootIssue("Cancel script execution failed", issues));
        }

        const auto alreadyFinished = ev->Get()->Record.GetAlreadyFinished();
        const auto executionEntryExists = ev->Get()->Record.GetExecutionEntryExists();
        KQP_PROXY_LOG_D("Got cancel response from RunScriptActor: " << ev->Sender << ", status: " << status
            << ", already finished: " << alreadyFinished
            << ", issues: " << issues.ToOneLineString());

        if (status == Ydb::StatusIds::SUCCESS) {
            if (!executionEntryExists) {
                return ReplyNotFound();
            }

            if (alreadyFinished) {
                return ReplyAlreadyStopped();
            }

            return Reply(Ydb::StatusIds::SUCCESS);
        }

        ScheduleRetryOrReply(status, AddRootIssue("Failed to cancel script execution, query final status saving was not successful", issues), /* immediate */ true);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto reason = ev->Get()->Reason;
        KQP_PROXY_LOG_W("Delivery failed " << reason << " to RunScriptActor: " << ev->Sender);

        if (ev->Cookie != CancellationCookie || !RunScriptActor) {
            return;
        }
        RunScriptActor = {};

        const bool immediateRetry = reason == TEvents::TEvUndelivered::ReasonActorUnknown; // The actor probably had finished before our cancel message arrived
        ScheduleRetryOrReply(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Failed to deliver cancel request to destination (delivery problem, reason: " << reason << ")", immediateRetry);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const auto nodeId = ev->Get()->NodeId;
        KQP_PROXY_LOG_W("Delivery failed to RunScriptActor, node " << nodeId << " disconnected");

        if (!RunScriptActor || nodeId != RunScriptActor.NodeId()) {
            return;
        }
        RunScriptActor = {};

        ScheduleRetryOrReply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver cancel request to destination (node disconnected)", /* immediate */ false);
    }

    void ResetSessionSubscribe() {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
            SubscribedOnSession = Nothing();
        }
    }

    void GetScriptExecutionOperationStatus() {
        WaitRetry = false;
        const auto& checkerId = Register(TCheckLeaseStatusQueryActor::MakeRetry(SelfId(), Request->Get()->Database, ExecutionId, TCheckLeaseStatusQueryActor::TSettings{.UserSID = UserSID}));
        KQP_PROXY_LOG_D("Start TCheckLeaseStatusQueryActor " << checkerId);
    }

    void ResetRetryPolicy() {
        const auto& resetActorId = Register(TResetScriptExecutionRetriesQueryActor::MakeRetry(SelfId(), Request->Get()->Database, ExecutionId));
        KQP_PROXY_LOG_D("Start TResetRetryStateRetryActor " << resetActorId);
    }

    void FinalizeScriptLease() {
        const auto& finalizeActorId = Register(new TFinalizeScriptLeaseActor(SelfId(), Request->Get()->Database, ExecutionId, QueryServiceConfig, Counters, {
            .AllowRestart = false,
            .FailOnNotFound = false,
        }));
        KQP_PROXY_LOG_D("Start TFinalizeScriptLeaseActor " << finalizeActorId);
    }

    void SendCancelToRunScriptActor() {
        CancellationCookie++;
        KQP_PROXY_LOG_D("Send cancel request to RunScriptActor, CancellationCookie: " << CancellationCookie);
        ResetSessionSubscribe();

        ui64 flags = IEventHandle::FlagTrackDelivery;
        if (RunScriptActor.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = RunScriptActor.NodeId();
        }

        Send(RunScriptActor, new TEvKqp::TEvCancelScriptExecutionRequest(), flags, CancellationCookie);
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TCancelScriptExecutionOperationActor] OwnerId: " << Request->Sender << " ActorId: " << SelfId() << " Database: " << Request->Get()->Database << " ExecutionId: " << ExecutionId << " RunScriptActor: " << RunScriptActor << ". ";
    }

    void ScheduleRetryOrReply(const Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, const bool immediate = false) {
        if (WaitRetry) {
            return;
        }

        if (!RetryState) {
            RetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                []() {
                    return ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(100),
                TDuration::Seconds(1),
                MAX_CANCEL_RETRIES
            )->CreateRetryState();
        }

        if (auto delay = RetryState->GetNextRetryDelay()) {
            if (immediate) {
                delay = TDuration::Zero();
            }

            KQP_PROXY_LOG_W("Schedule retry for error: " << issues.ToOneLineString() << " in " << *delay);
            Issues.AddIssues(issues);
            Schedule(*delay, new TEvents::TEvWakeup());
            WaitRetry = true;
            return;
        }

        Reply(status, issues);
    }

    void ScheduleRetryOrReply(const Ydb::StatusIds::StatusCode status, const TString& message, const bool immediate = false) {
        ScheduleRetryOrReply(status, {NYql::TIssue(message)}, immediate);
    }

    void ReplyNotFound() {
        KQP_PROXY_LOG_W("Script execution operation not found");
        EntryExists = false;
        return Reply(Request->Get()->Settings.FailOnNotFound ? Ydb::StatusIds::NOT_FOUND : Ydb::StatusIds::SUCCESS, "Script execution operation not found");
    }

    void ReplyAlreadyStopped() {
        KQP_PROXY_LOG_W("Script execution operation is already finished");
        return Reply(Request->Get()->Settings.FailOnAlreadyStopped ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::SUCCESS, "Script execution operation is already finished");
    }

    void Reply(const Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
        Issues.AddIssues(issues);

        if (status == Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_D("Reply success, issues: " << Issues.ToOneLineString());
        } else {
            KQP_PROXY_LOG_W("Reply failed, status: " << status << ", issues: " << Issues.ToOneLineString());
        }

        Send(Request->Sender, new TEvCancelScriptExecutionOperationResponse(status, EntryExists, std::move(Issues)));
        PassAway();
    }

    void Reply(const Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, {NYql::TIssue(message)});
    }

    const TEvCancelScriptExecutionOperation::TPtr Request;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    std::optional<TString> UserSID;
    TString ExecutionId;
    TActorId RunScriptActor;
    bool EntryExists = true;
    bool HasRetryPolicy = false;
    ui64 CancellationCookie = 0;
    TMaybe<ui32> SubscribedOnSession;
    TRetryPolicy::IRetryState::TPtr RetryState;
    bool WaitRetry = false;
    NYql::TIssues Issues;
};

// Save information about result sets and column types, started from TRunScriptActor on each new result set

class TSaveScriptExecutionResultMetaQuery final : public TQueryBase<TSaveScriptExecutionResultMetaQuery, TEvSaveScriptResultMetaFinished> {
public:
    TSaveScriptExecutionResultMetaQuery(TString database, TString executionId, TString serializedMetas, const i64 leaseGeneration)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , SerializedMetas(std::move(serializedMetas))
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
            -- TSaveScriptExecutionResultMetaQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_metas AS JsonDocument;
            DECLARE $lease_generation AS Int64;

            UPDATE `.metadata/script_executions`
            SET result_set_metas = $result_set_metas
            WHERE database = $database
              AND execution_id = $execution_id
              AND (lease_generation IS NULL OR lease_generation = $lease_generation)
              AND operation_status IS NULL;
        )";

        auto params = CreateParams();
        params
            .AddParam("$result_set_metas")
                .JsonDocument(SerializedMetas)
                .Build()
            .AddParam("$lease_generation")
                .Int64(LeaseGeneration)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptResultMetaFinished(status, std::move(issues)));
    }

    const TString SerializedMetas;
};

// Save result data part from TRunScriptActor as it arrived from DQ
// (NOTE: lease generation and status is not validated, so should not be used with retries and results may be written after finish)

class TSaveScriptExecutionResultQuery final : public TQueryBase<TSaveScriptExecutionResultQuery, TEvSaveScriptResultPartFinished> {
public:
    TSaveScriptExecutionResultQuery(TString database, TString executionId, const i32 resultSetId,
        const std::optional<TInstant> expireAt, const i64 firstRow, const i64 accumulatedSize, Ydb::ResultSet resultSet)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId)})
        , ResultSetId(resultSetId)
        , ExpireAt(expireAt)
        , FirstRow(firstRow)
        , AccumulatedSize(accumulatedSize)
        , ResultSet(std::move(resultSet))
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
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

        auto params = CreateParams();
        params
            .AddParam("$result_set_id")
                .Int32(ResultSetId)
                .Build()
            .AddParam("$expire_at")
                .OptionalTimestamp(ExpireAt)
                .Build();

        auto& param = params.AddParam("$items");
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

        KQP_PROXY_LOG_D("Save result #" << ResultSetId << ", FirstRow: " << FirstRow << ", AccumulatedSize: " << AccumulatedSize << ", rows to save: " << ResultSet.rows_size() << ", size to save: " << SavedSize);

        RunDataQuery(sql, &params);
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptResultPartFinished(status, SavedSize, std::move(issues)));
    }

    const i32 ResultSetId = 0;
    const std::optional<TInstant> ExpireAt;
    const i64 FirstRow = 0;
    const i64 AccumulatedSize = 0;
    const Ydb::ResultSet ResultSet;
    i64 SavedSize = 0;
};

class TSaveScriptExecutionResultActor final : public TActorBootstrapped<TSaveScriptExecutionResultActor> {
    static constexpr ui64 MAX_NUMBER_ROWS_IN_BATCH = 10000;
    static constexpr ui64 PROGRAM_SIZE_LIMIT = 10_MB;
    static constexpr ui64 PROGRAM_BASE_SIZE = 1_MB;  // Depends on MAX_NUMBER_ROWS_IN_BATCH

public:
    TSaveScriptExecutionResultActor(const TActorId& replyActorId, TString database, TString executionId, const i32 resultSetId,
        const std::optional<TInstant> expireAt, const i64 firstRow, const i64 accumulatedSize, Ydb::ResultSet&& resultSet)
        : ReplyActorId(replyActorId)
        , Database(std::move(database))
        , ExecutionId(std::move(executionId))
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
        const auto& saverId = Register(TSaveScriptExecutionResultQuery::MakeRetry(SelfId(), Database, ExecutionId, ResultSetId, ExpireAt, FirstRow, AccumulatedSize, ResultSets.back()));
        KQP_PROXY_LOG_D("Start saving rows range [" << FirstRow << "; " << FirstRow + numberRows << "), remains parts: " << ResultSets.size() << ", saver actor: " << saverId);

        FirstRow += numberRows;
        ResultSets.pop_back();
    }

    void Bootstrap() {
        KQP_PROXY_LOG_D("Bootstrap. FirstRow: " << FirstRow << ", AccumulatedSize: " << AccumulatedSize);

        NFq::TSplittedResultSets splittedResultSets = RowsSplitter.Split();
        if (!splittedResultSets.Success) {
            Reply(Ydb::StatusIds::BAD_REQUEST, std::move(splittedResultSets.Issues));
            return;
        }

        ResultSets = std::move(splittedResultSets.ResultSets);
        std::reverse(ResultSets.begin(), ResultSets.end());
        StartSaveResultQuery();

        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvSaveScriptResultPartFinished, Handle);
    )

private:
    void Handle(TEvSaveScriptResultPartFinished::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_W("Failed to save result part, saver actor: " << ev->Sender);
            Reply(status, std::move(ev->Get()->Issues));
            return;
        }

        AccumulatedSize += ev->Get()->SavedSize;
        KQP_PROXY_LOG_D("Result part successfully saved, AccumulatedSize: " << AccumulatedSize << ", saver actor: " << ev->Sender);

        StartSaveResultQuery();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TSaveScriptExecutionResultActor] OwnerId: " << ReplyActorId << " ActorId: " << SelfId() << " Database: " << Database << " ExecutionId: " << ExecutionId << " ResultSetId: " << ResultSetId << ". ";
    }

    void Reply(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        KQP_PROXY_LOG_D("Reply " << status << ", issues: " << issues.ToOneLineString());
        Send(ReplyActorId, new TEvSaveScriptResultFinished(status, ResultSetId, std::move(issues)));
        PassAway();
    }

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

// Fetch script execution result part, called from gRPC API fetch script execution results. Will read only fixed result part reported in result sets meta

class TGetScriptExecutionResultQueryActor final : public TQueryWithAccessValidationBase<TGetScriptExecutionResultQueryActor, TEvFetchScriptResultsResponse> {
public:
    TGetScriptExecutionResultQueryActor(TString database, TString executionId, std::optional<TString> userSID,
        const i32 resultSetIndex, const i64 offset, const i64 rowsLimit, const i64 sizeLimit, const TInstant deadline)
        : TQueryWithAccessValidationBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId)}, std::move(userSID))
        , ResultSetIndex(resultSetIndex)
        , Offset(offset)
        , RowsLimit(rowsLimit)
        , SizeLimit(sizeLimit)
        , Deadline(rowsLimit ? TInstant::Max() : deadline)
    {}

private:
    void OnRunQuery() override {
        if (!ValidateUserSID()) {
            return;
        }

        if (RowsLimit < 0 || SizeLimit < 0) {
            KQP_PROXY_LOG_W("Invalid fetch result limits, RowsLimit: " << RowsLimit << ", SizeLimit: " << SizeLimit);
            return Finish(Ydb::StatusIds::BAD_REQUEST, "Result rows limit and size limit should not be negative");
        }

        constexpr char sql[] = R"(
            -- TGetScriptExecutionResultQueryActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                result_set_metas,
                operation_status,
                issues,
                transient_issues,
                end_ts,
                meta,
                user_token
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetResultsInfo, "Get results info");
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

        if (!ValidateAccess(result)) {
            return;
        }

        const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();

        if (operationStatus) {
            const auto& serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
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

            const auto resultsTtl = NProtoInterop::CastFromProto(meta.GetResultsTtl());
            if (resultsTtl && (*endTs + resultsTtl) < TInstant::Now()){
                Finish(Ydb::StatusIds::NOT_FOUND, "Results are expired");
                return;
            }
        }

        const auto& serializedMetas = result.ColumnParser("result_set_metas").GetOptionalJsonDocument();

        if (!serializedMetas) {
            if (!operationStatus) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Result is not ready");
                return;
            }

            const auto operationStatusCode = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            if (operationStatusCode != Ydb::StatusIds::SUCCESS) {
                Finish(operationStatusCode, AddRootIssue("Script execution failed without results", ParseScriptExecutionIssues(result)));
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

        const NJson::TJsonValue* metaValue = nullptr;
        if (!value.GetValuePointer(ResultSetIndex, &metaValue)) {
            Finish(Ydb::StatusIds::BAD_REQUEST, "Result set index is invalid");
            return;
        }
        Y_ENSURE(metaValue, "Result set index is invalid");

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
        KQP_PROXY_LOG_D("Fetch results #" << ResultSetIndex << " with offset: " << Offset << ", limit: " << RowsLimit << ", saved rows: " << NumberOfSavedRows);

        constexpr char sql[] = R"(
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

        auto params = CreateParams();
        params
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

        SetQueryResultHandler(&TThis::OnQueryResult, TStringBuilder() << "Fetch results for offset " << Offset << ", limit: " << RowsLimit);
        RunStreamQuery(sql, &params, SizeLimit ? SizeLimit : 60_MB);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser result(resultSet);
        while (result.TryNextRow()) {
            const std::optional<TString>& serializedRow = result.ColumnParser("result_set").GetOptionalString();
            if (!serializedRow) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is null");
                return;
            }

            if (serializedRow->empty()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Result set row is empty");
                return;
            }

            const i64 rowSize = serializedRow->size();
            if (SizeLimit && ResultSet.rows_size()) {
                if (const auto newSize = ResultSetSize + rowSize + AdditionalRowSize; newSize > SizeLimit) {
                    KQP_PROXY_LOG_D("Finish by SizeLimit: " << SizeLimit << ", new result size: " << newSize);
                    CancelFetchQuery();
                    return;
                }
            }

            if (RowsLimit && ResultSet.rows_size() >= RowsLimit) {
                KQP_PROXY_LOG_D("Finish by RowsLimit: " << RowsLimit);
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
            KQP_PROXY_LOG_D("Finish by operation deadline: " << Deadline);
            CancelFetchQuery();
        }
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_D("Successfully fetched " << ResultSet.rows_size() << " rows");
            Send(Owner, new TEvFetchScriptResultsResponse(status, std::move(ResultSet), HasMoreResults, std::move(issues)));
        } else {
            Send(Owner, new TEvFetchScriptResultsResponse(status, std::nullopt, true, std::move(issues)));
        }
    }

    void CancelFetchQuery() {
        HasMoreResults = true;
        CancelStreamQuery();
    }

    const i32 ResultSetIndex = 0;
    const i64 Offset = 0;
    const i64 RowsLimit = 0;
    const i64 SizeLimit = 0;
    const TInstant Deadline;
    i64 NumberOfSavedRows = std::numeric_limits<i64>::max();
    i64 ResultSetSize = 0;
    i64 AdditionalRowSize = 0;
    Ydb::ResultSet ResultSet;
    bool HasMoreResults = false;
};

// Save information about query effects like S3 writes in order to commit / rollback after operation finish, saved after query compilation in DataExecuter

class TSaveScriptExternalEffectActor final : public TQueryBase<TSaveScriptExternalEffectActor, TEvSaveScriptExternalEffectResponse> {
public:
    TSaveScriptExternalEffectActor(TString database, TString executionId,
        TEvSaveScriptExternalEffectRequest::TDescription request, const i64 leaseGeneration)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , Request(std::move(request))
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
            -- TSaveScriptExternalEffectActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                lease_generation,
                customer_supplied_id,
                script_sinks,
                script_secret_names
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetInfo, "Get operation info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Script execution operation not found");
            return;
        }

        if (result.ColumnParser("operation_status").GetOptionalInt32()) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Script execution operation already finished");
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

        if (const auto& customerSuppliedId = result.ColumnParser("customer_supplied_id").GetOptionalUtf8(); customerSuppliedId && *customerSuppliedId != Request.CustomerSuppliedId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Customer supplied id mismatch, expected: " << Request.CustomerSuppliedId << ", got: " << *customerSuppliedId);
            return;
        }

        if (auto sinks = ParseScriptSinks(result)) {
            Request.Sinks.insert(Request.Sinks.end(), sinks->begin(), sinks->end());
        } else {
            return;
        }

        if (auto secretNames = ParseSecretNames(result)) {
            Request.SecretNames.insert(Request.SecretNames.end(), secretNames->begin(), secretNames->end());
        } else {
            return;
        }

        SaveExternalEffect();
    }

    void SaveExternalEffect() {
        KQP_PROXY_LOG_D("Save #" << Request.Sinks.size() << " sinks, #" << Request.SecretNames.size() << " secret names, CustomerSuppliedId: " << Request.CustomerSuppliedId);

        constexpr char sql[] = R"(
            -- TSaveScriptExternalEffectActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $customer_supplied_id AS Text;
            DECLARE $script_sinks AS JsonDocument;
            DECLARE $script_secret_names AS JsonDocument;
            DECLARE $lease_generation AS Int64;

            UPDATE `.metadata/script_executions`
            SET
                customer_supplied_id = $customer_supplied_id,
                script_sinks = $script_sinks,
                script_secret_names = $script_secret_names
            WHERE database = $database
              AND execution_id = $execution_id
              AND (lease_generation IS NULL OR lease_generation = $lease_generation)
              AND operation_status IS NULL;
        )";

        auto params = CreateParams();
        params
            .AddParam("$customer_supplied_id")
                .Utf8(Request.CustomerSuppliedId)
                .Build()
            .AddParam("$script_sinks")
                .JsonDocument(SequenceToJsonString(Request.Sinks.size(), [&](const ui64 i, NJson::TJsonValue& value) {
                    SerializeBinaryProto(Request.Sinks[i], value);
                }))
                .Build()
            .AddParam("$script_secret_names")
                .JsonDocument(SequenceToJsonString(Request.SecretNames))
                .Build()
            .AddParam("$lease_generation")
                .Int64(LeaseGeneration)
                .Build();

        SetQueryResultHandler(&TThis::OnQueryResult, "Save external effect");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptExternalEffectResponse(status, std::move(issues)));
    }

    TEvSaveScriptExternalEffectRequest::TDescription Request;
};

// Script execution finalization first phase: save final status and change leas state from ELeaseState::ScriptRunning to ELeaseState::ScriptFinalizing or ELeaseState::WaitRetry
// Finalization will continue only if script execution has external effects, otherwise it will be finished on the first phase.
// Finalization should be idempotent, because it may run multiple times during script execution runtime.

template<typename TDerived, typename TResponse>
class TScriptFinalizationActorBase : public TQueryBase<TDerived, TResponse> {
    using TBase = TQueryBase<TDerived, TResponse>;

public:
    using TBase::TBase;

protected:
    struct TLeaseFinalizationInfo {
        TString Sql;
        TDuration Backoff;
        ELeaseState NewLeaseState = ELeaseState::ScriptFinalizing;
    };

    TLeaseFinalizationInfo GetLeaseFinalizationSql(TInstant now, Ydb::StatusIds::StatusCode status, NYql::TIssues& issues) {
        const auto& policy = TRetryPolicyItem::FromProto(status, RetryState);
        TRetryLimiter retryLimiter(RetryState);

        const bool retry = policy && retryLimiter.UpdateOnRetry(StartedAt, TInstant::Now(), *policy);
        retryLimiter.SaveToProto(RetryState);

        if (retry) {
            issues = AddRootIssue(TStringBuilder()
                << "Script execution operation failed with code " << Ydb::StatusIds::StatusCode_Name(status)
                << " and will be restarted (RetryCount: " << retryLimiter.RetryCount << ", Backoff: " << retryLimiter.Backoff << ")"
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
            if (policy) {
                TStringBuilder finalIssue;
                finalIssue << "Script execution operation failed with code " << Ydb::StatusIds::StatusCode_Name(status);
                if (policy->PolicyInitialized) {
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

    bool ValidateLease(NYdb::TResultSetParser& result) {
        const auto leaseGenerationInDatabase = result.ColumnParser("lease_generation").GetOptionalInt64();
        if (!leaseGenerationInDatabase) {
            TBase::Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unknown lease generation");
            return false;
        }

        if (TBase::LeaseGeneration != *leaseGenerationInDatabase) {
            TBase::Finish(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder() << "Lease was lost, expected generation: " << TBase::LeaseGeneration << ", got: " << *leaseGenerationInDatabase);
            return false;
        }

        const auto leaseState = result.ColumnParser("lease_state").GetOptionalInt32().value_or(static_cast<i32>(ELeaseState::ScriptRunning));
        if (!IsIn({ELeaseState::ScriptRunning, ELeaseState::ScriptFinalizing}, static_cast<ELeaseState>(leaseState))) {
            TBase::Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Lease is not in expected state: " << leaseState);
            return false;
        }

        return true;
    }

    NKikimrKqp::TScriptExecutionRetryState RetryState;
    TInstant StartedAt;
};

class TSaveScriptFinalStatusActor final : public TScriptFinalizationActorBase<TSaveScriptFinalStatusActor, TEvSaveScriptFinalStatusResponse> {
public:
    explicit TSaveScriptFinalStatusActor(TEvScriptFinalizeRequest::TDescription request)
        : TScriptFinalizationActorBase(__func__, {.Database = request.Database, .ExecutionId = request.ExecutionId, .LeaseGeneration = request.LeaseGeneration})
        , Request(std::move(request))
        , Response(std::make_unique<TEvSaveScriptFinalStatusResponse>())
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
            -- TSaveScriptFinalStatusActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                finalization_status,
                meta,
                customer_supplied_id,
                user_token,
                user_group_sids,
                script_sinks,
                script_secret_names,
                retry_state,
                start_ts,
                graph_compressed IS NOT NULL AS has_graph
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

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetInfo, "Get operation info");
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
                Response->ExecutionEntryExists = false;
                Finish();
                return;
            }

            const auto finalizationStatus = result.ColumnParser("finalization_status").GetOptionalInt32();
            if (finalizationStatus) {
                Request.FinalizationStatus = static_cast<EFinalizationStatus>(*finalizationStatus);
                Response->ApplicateScriptExternalEffectRequired = true;
            }

            if (const auto operationStatus = result.ColumnParser("operation_status").GetOptionalInt32()) {
                Response->FinalStatusAlreadySaved = true;
                CommitTransaction();
                return;
            }

            if (auto customerSuppliedId = result.ColumnParser("customer_supplied_id").GetOptionalUtf8()) {
                Response->CustomerSuppliedId = std::move(*customerSuppliedId);
            }

            if (const auto& userToken = result.ColumnParser("user_token").GetOptionalUtf8()) {
                TVector<NACLib::TSID> userGroupSids;
                if (auto userGroupSids = ParseUserGroupSids(result)) {
                    userGroupSids = std::move(*userGroupSids);
                }

                Response->UserToken = new NACLib::TUserToken(TString(*userToken), userGroupSids);
            }

            if (auto sinks = ParseScriptSinks(result)) {
                SerializedSinks = result.ColumnParser("script_sinks").GetOptionalJsonDocument();
                Response->Sinks = std::move(*sinks);
            } else {
                return;
            }

            if (auto secretNames = ParseSecretNames(result)) {
                SerializedSecretNames = result.ColumnParser("script_secret_names").GetOptionalJsonDocument();
                Response->SecretNames = std::move(*secretNames);
            } else {
                return;
            }

            if (auto retryState = ParseRetryState(result)) {
                RetryState = std::move(*retryState);
            } else {
                return;
            }

            const auto& serializedMeta = result.ColumnParser("meta").GetOptionalJsonDocument();
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
            OperationTtl = NProtoInterop::CastFromProto(meta.GetOperationTtl());
            LeaseDuration = NProtoInterop::CastFromProto(meta.GetLeaseDuration());

            if (meta.GetSaveQueryPhysicalGraph() && !result.ColumnParser("has_graph").GetBool()) {
                // Disable retries if state not saved
                RetryState.ClearRetryPolicyMapping();
            }

            if (const auto startTs = result.ColumnParser("start_ts").GetOptionalTimestamp()) {
                StartedAt = *startTs;
            } else {
                return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation start timestamp");
            }
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected operation state, expected existing lease before finalization (maybe already finalized)");
                return;
            }

            if (!ValidateLease(result)) {
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
            DECLARE $plan_compressed AS Optional<String>;
            DECLARE $plan_compression_method AS Optional<Text>;
            DECLARE $stats AS JsonDocument;
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

            UPSERT INTO `.metadata/script_executions` (
                database, execution_id, operation_status, execution_status,
                finalization_status,
                issues, plan_compressed, plan_compression_method, end_ts, stats, ast_compressed, ast_compression_method,
                expire_at,
                customer_supplied_id,
                script_sinks,
                script_secret_names,
                retry_state
            ) VALUES (
                $database, $execution_id, $operation_status, $execution_status,
                IF($applicate_script_external_effect_required, $finalization_status, NULL),
                $issues, $plan_compressed, $plan_compression_method, CurrentUtcTimestamp(), $stats, $ast_compressed, $ast_compression_method,
                IF($operation_ttl > CAST(0 AS Interval), CurrentUtcTimestamp() + $operation_ttl, NULL),
                IF($applicate_script_external_effect_required, $customer_supplied_id, NULL),
                IF($applicate_script_external_effect_required, $script_sinks, NULL),
                IF($applicate_script_external_effect_required, $script_secret_names, NULL),
                $retry_state
            );
        )";

        if (Request.CancelledByUser) {
            RetryState.ClearRetryPolicyMapping();
        }

        TInstant retryDeadline = TInstant::Now();
        ELeaseState leaseState = ELeaseState::ScriptFinalizing;
        if (!Response->ApplicateScriptExternalEffectRequired) {
            const auto leaseInfo = GetLeaseFinalizationSql(retryDeadline, Request.OperationStatus, Request.Issues);
            sql << leaseInfo.Sql;
            retryDeadline += leaseInfo.Backoff;
            leaseState = leaseInfo.NewLeaseState;
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

        const auto operationTtl = std::min(OperationTtl.MicroSeconds(), NYql::NUdf::MAX_TIMESTAMP - 1);
        KQP_PROXY_LOG_D("Do finalization with status " << Request.OperationStatus
            << ", exec status: " << Ydb::Query::ExecStatus_Name(Request.ExecStatus)
            << ", finalization status (applicate effect: " << Response->ApplicateScriptExternalEffectRequired << "): " << Request.FinalizationStatus
            << ", issues: " << Request.Issues.ToOneLineString()
            << ", retry deadline: " << retryDeadline
            << ", lease state: " << leaseState
            << ", operation_ttl: " << TDuration::MicroSeconds(operationTtl));

        auto params = CreateParams();
        params
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
            .AddParam("$plan_compressed")
                .OptionalString(Request.QueryPlan)
                .Build()
            .AddParam("$plan_compression_method")
                .OptionalUtf8(Request.QueryPlanCompressionMethod)
                .Build()
            .AddParam("$stats")
                .JsonDocument(serializedStats)
                .Build()
            .AddParam("$ast_compressed")
                .OptionalString(Request.QueryAst)
                .Build()
            .AddParam("$ast_compression_method")
                .OptionalUtf8(Request.QueryAstCompressionMethod)
                .Build()
            .AddParam("$operation_ttl")
                .Interval(operationTtl)
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

        SetQueryResultHandler(&TThis::OnQueryResult, "Update final status");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (!Response->FinalStatusAlreadySaved) {
            KQP_PROXY_LOG_D("Finish script execution operation"
                << ". Status: " << Ydb::StatusIds::StatusCode_Name(Request.OperationStatus)
                << ". Issues: " << Request.Issues.ToOneLineString());
        }

        Response->Status = status;
        Response->Issues = std::move(issues);
        Send(Owner, Response.release());
    }

    bool HasExternalEffect() const {
        return !Response->Sinks.empty();
    }

    TEvScriptFinalizeRequest::TDescription Request;
    std::unique_ptr<TEvSaveScriptFinalStatusResponse> Response;
    TDuration OperationTtl;
    TDuration LeaseDuration;
    std::optional<TString> SerializedSinks;
    std::optional<TString> SerializedSecretNames;
};

// Script execution finalization second phase: after commit / rollback of external effects remove finalization status and lease

class TScriptFinalizationFinisherActor final : public TScriptFinalizationActorBase<TScriptFinalizationFinisherActor, TEvScriptExecutionFinished>  {
public:
    TScriptFinalizationFinisherActor(TString database, TString executionId, const std::optional<Ydb::StatusIds::StatusCode> operationStatus,
        NYql::TIssues operationIssues, const i64 leaseGeneration)
        : TScriptFinalizationActorBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , OperationStatus(operationStatus)
        , OperationIssues(AddRootIssue("Script finalization failed", operationIssues, /* addEmptyRoot */ false))
    {}

private:
    void OnRunQuery() override {
        KQP_PROXY_LOG_D("Start" << (OperationStatus ? " with status " + Ydb::StatusIds::StatusCode_Name(*OperationStatus) : "") << ", issues: " << OperationIssues.ToOneLineString());

        constexpr char sql[] = R"(
            -- TScriptFinalizationFinisherActor::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                finalization_status,
                issues,
                retry_state,
                meta,
                start_ts,
                graph_compressed IS NOT NULL AS has_graph
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

        auto params = CreateParams();
        SetQueryResultHandler(&TThis::OnGetInfo, "Get operation info");
        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnGetInfo() {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {   // Lease info
            NYdb::TResultSetParser result(ResultSets[1]);
            if (result.TryNextRow() && !ValidateLease(result)) {
                return;
            }
        }

        {   // Execution info
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                ExecutionEntryExists = false;
                Finish();
                return;
            }

            if (!result.ColumnParser("finalization_status").GetOptionalInt32()) {
                AlreadyFinalized = true;
                Finish();
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

            if (auto retryState = ParseRetryState(result)) {
                RetryState = std::move(*retryState);
            } else {
                return;
            }

            if (const auto issuesSerialized = result.ColumnParser("issues").GetOptionalJsonDocument()) {
                OperationIssues.AddIssues(DeserializeIssues(*issuesSerialized));
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
            if (meta.GetSaveQueryPhysicalGraph() && !result.ColumnParser("has_graph").GetBool()) {
                // Disable retries if state not saved
                RetryState.ClearRetryPolicyMapping();
            }

            if (const auto startTs = result.ColumnParser("start_ts").GetOptionalTimestamp()) {
                StartedAt = *startTs;
            } else {
                return Finish(Ydb::StatusIds::INTERNAL_ERROR, "Missing operation start timestamp");
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

            UPSERT INTO `.metadata/script_executions` (
                database, execution_id, operation_status, execution_status,
                finalization_status, issues, script_sinks,
                customer_supplied_id, script_secret_names, retry_state
            ) VALUES (
                $database, $execution_id, $operation_status, $execution_status,
                NULL, $issues, NULL,
                NULL, NULL, $retry_state
            );
        )";

        Y_ENSURE(OperationStatus);

        TInstant retryDeadline = TInstant::Now();
        const auto leaseInfo = GetLeaseFinalizationSql(retryDeadline, *OperationStatus, OperationIssues);
        sql << leaseInfo.Sql;
        retryDeadline += leaseInfo.Backoff;

        KQP_PROXY_LOG_D("Do finalization with status " << *OperationStatus
            << ", exec status: " << Ydb::Query::ExecStatus_Name(ExecutionStatus)
            << ", issues: " << OperationIssues.ToOneLineString()
            << ", retry deadline: " << retryDeadline
            << ", lease state: " << static_cast<i32>(leaseInfo.NewLeaseState));

        auto params = CreateParams();
        params
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

        SetQueryResultHandler(&TThis::OnQueryResult, "Update final status");
        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (!OperationStatus) {
            OperationStatus = status;
        }

        if (issues) {
            OperationIssues.AddIssues(AddRootIssue(TStringBuilder() << "Update final status failed " << status, issues));
        }

        Send(Owner, new TEvScriptExecutionFinished(*OperationStatus, {
            .ExecutionEntryExists = ExecutionEntryExists,
            .AlreadyStopped = AlreadyFinalized,
        }, std::move(OperationIssues)));
    }

    std::optional<Ydb::StatusIds::StatusCode> OperationStatus;
    Ydb::Query::ExecStatus ExecutionStatus = Ydb::Query::EXEC_STATUS_UNSPECIFIED;
    NYql::TIssues OperationIssues;
    bool AlreadyFinalized = false;
    bool ExecutionEntryExists = true;
};

// Update runtime statistics of script execution, started from TRunScriptActor

class TScriptProgressActor final : public TQueryBase<TScriptProgressActor, TEvSaveScriptProgressResponse> {
public:
    TScriptProgressActor(TString database, TString executionId, std::optional<TString> queryPlan, std::optional<TString> ast,
        const i64 leaseGeneration, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , QueryPlan(std::move(queryPlan))
        , QueryAst(std::move(ast))
        , Compressor(queryServiceConfig.GetQueryArtifactsCompressionMethod(), queryServiceConfig.GetQueryArtifactsCompressionMinSize())
    {}

private:
    void OnRunQuery() override {
        const auto& artifacts = CompressScriptArtifacts(QueryAst, QueryPlan, Compressor);
        if (artifacts.Issues) {
            KQP_PROXY_LOG_N("Compress script artifacts finished with issues: " << artifacts.Issues.ToOneLineString());
        }

        auto parameters = TStringBuilder() << R"(
            -- TScriptProgressActor::OnRunQuery
            DECLARE $execution_id AS Text;
            DECLARE $database AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $lease_generation AS Int64;
        )";

        auto sql = TStringBuilder() << R"(
            UPDATE `.metadata/script_executions`
            SET
                execution_status = $execution_status
        )";

        auto params = CreateParams();
        params
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_RUNNING)
                .Build()
            .AddParam("$lease_generation")
                .Int64(LeaseGeneration)
                .Build();

        if (artifacts.Plan) {
            params
                .AddParam("$plan_compressed")
                    .OptionalString(artifacts.Plan)
                    .Build()
                .AddParam("$plan_compression_method")
                    .OptionalUtf8(artifacts.PlanCompressionMethod)
                    .Build();

            parameters << R"(
                DECLARE $plan_compressed AS Optional<String>;
                DECLARE $plan_compression_method AS Optional<Text>;
            )";

            sql << R"(
                , plan_compressed = $plan_compressed,
                plan_compression_method = $plan_compression_method
            )";
        }

        if (artifacts.Ast) {
            params
                .AddParam("$ast_compressed")
                    .OptionalString(artifacts.Ast)
                    .Build()
                .AddParam("$ast_compression_method")
                    .OptionalUtf8(artifacts.AstCompressionMethod)
                    .Build();

            parameters << R"(
                DECLARE $ast_compressed AS Optional<String>;
                DECLARE $ast_compression_method AS Optional<Text>;
            )";

            sql << R"(
                , ast_compressed = $ast_compressed,
                ast_compression_method = $ast_compression_method
            )";
        }

        sql << R"(
            WHERE database = $database
              AND execution_id = $execution_id
              AND (lease_generation IS NULL OR lease_generation = $lease_generation)
              AND operation_status IS NULL;
        )";

        RunDataQuery(parameters << sql, &params);
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptProgressResponse(status, QueryAst && status == Ydb::StatusIds::SUCCESS, std::move(issues)));
    }

    const std::optional<TString> QueryPlan;
    const std::optional<TString> QueryAst;
    const TCompressor Compressor;
};

// Find all script executions with expired lease in any status and finalize or retry them

class TListExpiredLeasesQueryActor final : public TQueryBase<TListExpiredLeasesQueryActor, TEvListExpiredLeasesResponse> {
    static constexpr ui64 MAX_LISTED_LEASES = 100;

public:
    TListExpiredLeasesQueryActor()
        : TQueryBase(__func__, {})
    {}

private:
    void OnRunQuery() override {
        SetOperationInfo(OperationName, Owner.ToString());

        constexpr char sql[] = R"(
            -- TListExpiredLeasesQueryActor::OnRunQuery
            DECLARE $max_lease_deadline AS Timestamp;
            DECLARE $max_listed_leases AS Uint64;

            SELECT
                database,
                execution_id
            FROM `.metadata/script_execution_leases`
            WHERE lease_deadline < $max_lease_deadline
              AND (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL)
            LIMIT $max_listed_leases;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$max_lease_deadline")
                .Timestamp(LeaseDeadline)
                .Build()
            .AddParam("$max_listed_leases")
                .Uint64(MAX_LISTED_LEASES)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginAndCommitTx(/* snapshotRead */ true));
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        const auto rowsCount = ResultSets[0].RowsCount();
        Leases.reserve(rowsCount);

        NYdb::TResultSetParser result(ResultSets[0]);
        while (result.TryNextRow()) {
            std::optional<TString> database = result.ColumnParser("database").GetOptionalUtf8();
            if (!database) {
                KQP_PROXY_LOG_E("Database field is null for script execution lease");
                continue;
            }

            std::optional<TString> executionId = result.ColumnParser("execution_id").GetOptionalUtf8();
            if (!executionId) {
                KQP_PROXY_LOG_E("Execution id field is null for script execution lease in database " << *database);
                continue;
            }

            Leases.emplace_back(TEvListExpiredLeasesResponse::TLeaseInfo{std::move(*database), std::move(*executionId)});
        }

        KQP_PROXY_LOG_D("Found " << Leases.size() << " expired leases (fetched rows " << rowsCount << ")");
        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvListExpiredLeasesResponse(status, std::move(Leases), std::move(issues)));
    }

    const TInstant LeaseDeadline = TInstant::Now();
    std::vector<TEvListExpiredLeasesResponse::TLeaseInfo> Leases;
};

class TRefreshScriptExecutionLeasesActor final : public TActorBootstrapped<TRefreshScriptExecutionLeasesActor> {
public:
    TRefreshScriptExecutionLeasesActor(const TActorId& replyActorId, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : ReplyActorId(replyActorId)
        , QueryServiceConfig(std::move(queryServiceConfig))
        , Counters(std::move(counters))
    {}

    void Bootstrap() {
        const auto& listerId = Register(new TListExpiredLeasesQueryActor());
        KQP_PROXY_LOG_D("Bootstrap. Started TListExpiredLeasesQueryActor: " << listerId);

        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvListExpiredLeasesResponse, Handle);
        hFunc(TEvPrivate::TEvFinalizeScriptLeaseResult, Handle);
    )

private:
    void Handle(TEvListExpiredLeasesResponse::TPtr& ev) {
        const auto& leases = ev->Get()->Leases;
        ExpiredLeasesCount = leases.size();
        KQP_PROXY_LOG_D("Got list expired leases response " << ev->Sender << ", found " << ExpiredLeasesCount << " expired leases");

        for (const auto& lease : leases) {
            const auto& checkerId = Register(new TFinalizeScriptLeaseActor(SelfId(), lease.Database, lease.ExecutionId, QueryServiceConfig, Counters, {.Cookie = CookieId++}));
            KQP_PROXY_LOG_D("Database: " << lease.Database << "ExecutionId: " << lease.ExecutionId << ", start TFinalizeScriptLeaseActor #" << CookieId << " " << checkerId);
            ++OperationsToCheck;
        }

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("List expired leases failed with status " << status << ", issues: " << issues.ToOneLineString());

            Success = false;
            Issues.AddIssues(AddRootIssue(TStringBuilder() << "Failed to list expired leases (" << status << ")", issues, true));
        } else {
            KQP_PROXY_LOG_D("List expired leases successfully completed");
        }

        MaybeFinish();
    }

    void Handle(TEvPrivate::TEvFinalizeScriptLeaseResult::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie < CookieId);
        Y_ABORT_UNLESS(OperationsToCheck > 0);
        --OperationsToCheck;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            KQP_PROXY_LOG_W("Lease check #" << ev->Cookie << " " << ev->Sender << " failed, status: " << status << ", issues: " << issues.ToOneLineString() << ", OperationsToCheck: " << OperationsToCheck);

            Success = false;
            Issues.AddIssues(AddRootIssue(TStringBuilder() << "Lease check failed #" << ev->Cookie << " (" << status << ")", issues, true));
        } else {
            KQP_PROXY_LOG_D("Lease check #" << ev->Cookie << " " << ev->Sender << " successfully completed, OperationsToCheck: " << OperationsToCheck);
        }

        MaybeFinish();
    }

    void MaybeFinish() {
        if (OperationsToCheck) {
            return;
        }

        KQP_PROXY_LOG_D("Finish, success: " << Success << ", issues: " << Issues.ToOneLineString());
        Send(ReplyActorId, new TEvRefreshScriptExecutionLeasesResponse(Success, ExpiredLeasesCount, std::move(Issues)));
        PassAway();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TRefreshScriptExecutionLeasesActor] OwnerId: " << ReplyActorId << " ActorId: " << SelfId() << ". ";
    }

    const TActorId ReplyActorId;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    ui64 CookieId = 0;
    ui64 OperationsToCheck = 0;
    ui64 ExpiredLeasesCount = 0;
    bool Success = true;
    NYql::TIssues Issues;
};

// Fetch / save script execution compilation and planning results, used when enabled checkpoints

class TSaveScriptExecutionPhysicalGraphActor final : public TQueryBase<TSaveScriptExecutionPhysicalGraphActor, TEvSaveScriptPhysicalGraphResponse> {
public:
    TSaveScriptExecutionPhysicalGraphActor(TString database, TString executionId, NKikimrKqp::TQueryPhysicalGraph physicalGraph, const i64 leaseGeneration,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId), .LeaseGeneration = leaseGeneration})
        , PhysicalGraph(std::move(physicalGraph))
        , Compressor(queryServiceConfig.GetQueryArtifactsCompressionMethod(), queryServiceConfig.GetQueryArtifactsCompressionMinSize())
    {}

private:
    void OnRunQuery() override {
        using namespace fmt::literals;

        TString streamingDispositionUpdate;
        if (PhysicalGraph.GetZeroCheckpointSaved()) {
            // Reset streaming disposition after state save
            streamingDispositionUpdate = "streaming_disposition = NULL,";
        }

        TString sql = fmt::format(R"(
            -- TSaveScriptExecutionPhysicalGraphActor::OnRunQuery
            DECLARE $execution_id AS Text;
            DECLARE $database AS Text;
            DECLARE $graph_compressed AS String;
            DECLARE $graph_compression_method AS Text;
            DECLARE $lease_generation AS Int64;

            UPDATE `.metadata/script_executions`
            SET
                {streaming_disposition}
                graph_compressed = $graph_compressed,
                graph_compression_method = $graph_compression_method
            WHERE database = $database
              AND execution_id = $execution_id
              AND (lease_generation IS NULL OR lease_generation = $lease_generation)
              AND operation_status IS NULL;
        )", "streaming_disposition"_a = streamingDispositionUpdate);

        const auto& [graphCompressionMethod, graphCompressed] = Compressor.Compress(PhysicalGraph.SerializeAsString());

        auto params = CreateParams();
        params
            .AddParam("$graph_compressed")
                .String(graphCompressed)
                .Build()
            .AddParam("$graph_compression_method")
                .Utf8(graphCompressionMethod)
                .Build()
            .AddParam("$lease_generation")
                .Int64(LeaseGeneration)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSaveScriptPhysicalGraphResponse(status, std::move(issues)));
    }

    const NKikimrKqp::TQueryPhysicalGraph PhysicalGraph;
    const TCompressor Compressor;
};

class TGetScriptExecutionPhysicalGraphActor final : public TQueryBase<TGetScriptExecutionPhysicalGraphActor, TEvGetScriptPhysicalGraphResponse> {
public:
    TGetScriptExecutionPhysicalGraphActor(TString database, TString executionId)
        : TQueryBase(__func__, {.Database = std::move(database), .ExecutionId = std::move(executionId)})
    {}

private:
    void OnRunQuery() override {
        constexpr char sql[] = R"(
            -- TGetScriptExecutionPhysicalGraphActor::OnRunQuery
            DECLARE $execution_id AS Text;
            DECLARE $database AS Text;

            SELECT
                graph_compressed,
                graph_compression_method,
                lease_generation
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id AND
                  (expire_at > CurrentUtcTimestamp() OR expire_at IS NULL);
        )";

        auto params = CreateParams();
        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            ExecutionEntryExists = false;
            Finish();
            return;
        }

        if (const std::optional<TString>& graphCompressed = result.ColumnParser("graph_compressed").GetOptionalString()) {
            const std::optional<TString>& compressionMethod = result.ColumnParser("graph_compression_method").GetOptionalUtf8();
            if (!compressionMethod) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Graph compression method is not found");
                return;
            }

            const TCompressor compressor(*compressionMethod);
            const auto& graph = compressor.Decompress(*graphCompressed);
            NKikimrKqp::TQueryPhysicalGraph graphProto;
            if (!graphProto.ParseFromString(graph)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Query physical graph is corrupted");
                return;
            }

            PhysicalGraph = std::move(graphProto);
        }

        LeaseGeneration = result.ColumnParser("lease_generation").GetOptionalInt64().value_or(1);

        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvGetScriptPhysicalGraphResponse(status, std::move(PhysicalGraph), LeaseGeneration, ExecutionEntryExists, std::move(issues)));
    }

    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    i64 LeaseGeneration = 0;
    bool ExecutionEntryExists = true;
};

} // anonymous namespace

IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, const TDuration maxRunTime) {
    TDuration leaseDuration = LEASE_DURATION;

    if (ui64 seconds = 0; TryFromString<ui64>(GetEnv("YDB_TEST_LEASE_DURATION_SEC"), seconds) && seconds) {
        leaseDuration = TDuration::Seconds(seconds);
    }

    return new TCreateScriptExecutionActor(std::move(ev), std::move(queryServiceConfig), std::move(counters), maxRunTime, leaseDuration);
}

IActor* CreateScriptExecutionsTablesCreator(const bool enableSecureScriptExecutions, const ui64 generation) {
    return new TScriptExecutionsTablesCreator(enableSecureScriptExecutions, generation);
}

IActor* CreateForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr&& ev) {
    return new TForgetScriptExecutionOperationActor(std::move(ev));
}

IActor* CreateGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr&& ev) {
    return TGetScriptExecutionOperationQueryActor::MakeRetry(ev->Sender, std::move(ev->Get()->Database), std::move(ev->Get()->OperationId), std::move(ev->Get()->UserSID), ev->Get()->FailOnNotFound, ev->Cookie);
}

IActor* CreateListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr&& ev) {
    return TListScriptExecutionOperationsQuery::MakeRetry(ev->Sender, std::move(ev->Get()->Database), std::move(ev->Get()->UserSID), std::move(ev->Get()->PageToken), ev->Get()->PageSize);
}

IActor* CreateCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr&& ev, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TCancelScriptExecutionOperationActor(std::move(ev), std::move(queryServiceConfig), std::move(counters));
}

IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, TString database, TString executionId, const TDuration leaseDuration, const i64 leaseGeneration) {
    return TScriptLeaseUpdaterQuery::MakeRetry(
        runScriptActorId,
        TQueryRetryActorBase::IRetryPolicy::GetExponentialBackoffPolicy(
            TQueryRetryActorBase::Retryable,
            TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(200),
            TDuration::Seconds(1),
            std::numeric_limits<size_t>::max(),
            leaseDuration / 2
        ),
        std::move(database), std::move(executionId), leaseDuration, leaseGeneration
    );
}

IActor* CreateSaveScriptExecutionResultMetaActor(const TActorId& runScriptActorId, TString database, TString executionId, TString serializedMeta, const i64 leaseGeneration) {
    return TSaveScriptExecutionResultMetaQuery::MakeRetry(runScriptActorId, std::move(database), std::move(executionId), std::move(serializedMeta), leaseGeneration);
}

IActor* CreateSaveScriptExecutionResultActor(const TActorId& runScriptActorId, TString database, TString executionId, const i32 resultSetId, const std::optional<TInstant> expireAt, const i64 firstRow, const i64 accumulatedSize, Ydb::ResultSet&& resultSet) {
    return new TSaveScriptExecutionResultActor(runScriptActorId, std::move(database), std::move(executionId), resultSetId, expireAt, firstRow, accumulatedSize, std::move(resultSet));
}

IActor* CreateGetScriptExecutionResultActor(const TActorId& replyActorId, TString database, TString executionId, std::optional<TString> userSID, const i32 resultSetIndex, const i64 offset, const i64 rowsLimit, const i64 sizeLimit, const TInstant operationDeadline) {
    return TGetScriptExecutionResultQueryActor::MakeRetry(replyActorId, std::move(database), std::move(executionId), std::move(userSID), resultSetIndex, offset, rowsLimit, sizeLimit, operationDeadline);
}

IActor* CreateSaveScriptExternalEffectActor(const TActorId& replyActorId, TString database, TString executionId, TEvSaveScriptExternalEffectRequest::TDescription&& info, const i64 leaseGeneration) {
    return TSaveScriptExternalEffectActor::MakeRetry(replyActorId, std::move(database), std::move(executionId), std::move(info), leaseGeneration);
}

IActor* CreateSaveScriptFinalStatusActor(const TActorId& finalizationActorId, TEvScriptFinalizeRequest::TPtr&& ev) {
    return TSaveScriptFinalStatusActor::MakeRetry(finalizationActorId, std::move(ev->Get()->Description));
}

IActor* CreateScriptFinalizationFinisherActor(const TActorId& finalizationActorId, TString database, TString executionId, const std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues, const i64 leaseGeneration) {
    return TScriptFinalizationFinisherActor::MakeRetry(finalizationActorId, std::move(database), std::move(executionId), operationStatus, std::move(operationIssues), leaseGeneration);
}

IActor* CreateScriptProgressActor(TString database, TString executionId, std::optional<TString> queryPlan, std::optional<TString> ast, const i64 leaseGeneration, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig) {
    return new TScriptProgressActor(std::move(database), std::move(executionId), std::move(queryPlan), std::move(ast), leaseGeneration, queryServiceConfig);
}

IActor* CreateRefreshScriptExecutionLeasesActor(const TActorId& replyActorId, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TRefreshScriptExecutionLeasesActor(replyActorId, std::move(queryServiceConfig), std::move(counters));
}

IActor* CreateSaveScriptExecutionPhysicalGraphActor(const TActorId& replyActorId, TString database, TString executionId, NKikimrKqp::TQueryPhysicalGraph physicalGraph, const i64 leaseGeneration, NKikimrConfig::TQueryServiceConfig queryServiceConfig) {
    return TSaveScriptExecutionPhysicalGraphActor::MakeRetry(replyActorId, std::move(database), std::move(executionId), std::move(physicalGraph), leaseGeneration, std::move(queryServiceConfig));
}

IActor* CreateGetScriptExecutionPhysicalGraphActor(const TActorId& replyActorId, TString database, TString executionId) {
    return TGetScriptExecutionPhysicalGraphActor::MakeRetry(replyActorId, std::move(database), std::move(executionId));
}

namespace NPrivate {

IActor* CreateCreateScriptOperationQueryActor(TString executionId, const TActorId& runScriptActorId, NKikimrKqp::TEvQueryRequest record, NKikimrKqp::TScriptExecutionOperationMeta meta) {
    return new TCreateScriptOperationQuery(std::move(executionId), runScriptActorId, std::move(record), std::move(meta), TDuration::Max(), {}, std::nullopt, {}, nullptr, 1);
}

IActor* CreateCheckLeaseStatusActor(TString database, TString executionId) {
    return new TCheckLeaseStatusQueryActor(std::move(database), std::move(executionId), {});
}

IActor* CreateFinalizeScriptLeaseActor(const TActorId& replyActorId, TString database, TString executionId) {
    return new TFinalizeScriptLeaseActor(replyActorId, std::move(database), std::move(executionId), {}, nullptr, {});
}

} // namespace NPrivate

} // namespace NKikimr::NKqp
