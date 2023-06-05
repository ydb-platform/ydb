#include "kqp_script_executions.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/run_script_actor/kqp_run_script_actor.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/utility.h>
#include <util/random/random.h>

namespace NKikimr::NKqp {

namespace {

#define KQP_PROXY_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)

constexpr TDuration LEASE_DURATION = TDuration::Seconds(30);

TString SerializeIssues(const NYql::TIssues& issues) {
    NYql::TIssue root;
    for (const NYql::TIssue& issue : issues) {
        root.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }
    Ydb::Issue::IssueMessage rootMessage;
    NYql::IssueToMessage(root, &rootMessage);
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

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvCreateScriptOperationResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateTableResponse,
        EvLeaseCheckResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvCreateScriptOperationResponse : public NActors::TEventLocal<TEvCreateScriptOperationResponse, EvCreateScriptOperationResponse> {
        TEvCreateScriptOperationResponse(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues)
            : Status(statusCode)
            , Issues(std::move(issues))
        {
        }

        TEvCreateScriptOperationResponse(TString executionId)
            : Status(Ydb::StatusIds::SUCCESS)
            , ExecutionId(std::move(executionId))
        {
        }

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        const TString ExecutionId;
    };

    struct TEvCreateTableResponse : public NActors::TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {
        TEvCreateTableResponse() = default;
    };

    struct TEvLeaseCheckResult : public NActors::TEventLocal<TEvLeaseCheckResult, EvLeaseCheckResult> {
        TEvLeaseCheckResult(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues, TMaybe<Ydb::StatusIds::StatusCode> operationStatus)
            : Status(statusCode)
            , Issues(std::move(issues))
            , OperationStatus(operationStatus)
        {
        }

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        const TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
    };
};


class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(TString sessionId = {})
        : NKikimr::TQueryBase(NKikimrServices::KQP_PROXY, sessionId)
    {}
};


class TTableCreator : public NActors::TActorBootstrapped<TTableCreator> {
public:
    TTableCreator(TVector<TString> pathComponents, TVector<NKikimrSchemeOp::TColumnDescription> columns, TVector<TString> keyColumns)
        : PathComponents(std::move(pathComponents))
        , Columns(std::move(columns))
        , KeyColumns(std::move(keyColumns))
    {
        Y_VERIFY(!PathComponents.empty());
        Y_VERIFY(!Columns.empty());
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TTableCreator>::Registered(sys, owner);
        Owner = owner;
    }

    STRICT_STFUNC(StateFuncCheck,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        sFunc(NActors::TEvents::TEvWakeup, CheckTableExistence);
    )

    STRICT_STFUNC(StateFuncCreate,
        hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        sFunc(NActors::TEvents::TEvWakeup, RunCreateTableRequest);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
    )

    void Bootstrap() {
        Become(&TTableCreator::StateFuncCheck);
        CheckTableExistence();
    }

    void CheckTableExistence() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto pathComponents = SplitPath(AppData()->TenantName);
        request->DatabaseName = CanonizePath(pathComponents);
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        pathComponents.insert(pathComponents.end(), PathComponents.begin(), PathComponents.end());
        entry.Path = pathComponents;
        entry.ShowPrivatePath = true;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void RunCreateTableRequest() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        NKikimrSchemeOp::TModifyScheme& modifyScheme = *request->Record.MutableTransaction()->MutableModifyScheme();
        auto pathComponents = SplitPath(AppData()->TenantName);
        for (size_t i = 0; i < PathComponents.size() - 1; ++i) {
            pathComponents.emplace_back(PathComponents[i]);
        }
        modifyScheme.SetWorkingDir(CanonizePath(pathComponents));
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
        modifyScheme.SetInternal(true);
        modifyScheme.SetAllowAccessToPrivatePaths(true);
        NKikimrSchemeOp::TTableDescription& tableDesc = *modifyScheme.MutableCreateTable();
        tableDesc.SetName(TableName());
        for (const TString& k : KeyColumns) {
            tableDesc.AddKeyColumnNames(k);
        }
        for (const NKikimrSchemeOp::TColumnDescription& col : Columns) {
            *tableDesc.AddColumns() = col;
        }
        Send(MakeTxProxyID(), std::move(request));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_VERIFY(request.ResultSet.size() == 1);
        const NSchemeCache::TSchemeCacheNavigate::TEntry& result  = request.ResultSet[0];
        if (result.Status != EStatus::Ok) {
            KQP_PROXY_LOG_D("Describe table " << TableName() << " result: " << result.Status);
        }

        switch (result.Status) {
            case EStatus::Unknown:
                [[fallthrough]];
            case EStatus::LookupError:
                [[fallthrough]];
            case EStatus::PathNotTable:
                [[fallthrough]];
            case EStatus::PathNotPath:
                [[fallthrough]];
            case EStatus::RedirectLookupError:
                Fail(result.Status);
                break;
            case EStatus::RootUnknown:
                [[fallthrough]];
            case EStatus::PathErrorUnknown:
                Become(&TTableCreator::StateFuncCreate);
                RunCreateTableRequest();
                break;
            case EStatus::TableCreationNotComplete:
                Retry();
                break;
            case EStatus::Ok:
                Success();
                break;
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        KQP_PROXY_LOG_D("TEvProposeTransactionStatus " << TableName() << ": " << ev->Get()->Record);
        const auto ssStatus = ev->Get()->Record.GetSchemeShardStatus();
        switch (ev->Get()->Status()) {
            case NTxProxy::TResultStatus::ExecComplete:
                [[fallthrough]];
            case NTxProxy::TResultStatus::ExecAlready:
                if (ssStatus == NKikimrScheme::EStatus::StatusSuccess || ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    Success(ev);
                } else {
                    Fail(ev);
                }
                break;
            case NTxProxy::TResultStatus::ProxyShardNotAvailable:
                Retry();
                break;
            case NTxProxy::TResultStatus::ExecError:
                if (ssStatus == NKikimrScheme::EStatus::StatusMultipleModifications) {
                    SubscribeOnTransaction(ev);
                // In the process of creating a database, errors of the form may occur -
                // database doesn't have storage pools at all to create tablet
                // channels to storage pool binding by profile id
                } else if (ssStatus == NKikimrScheme::EStatus::StatusInvalidParameter) {
                    Retry();
                } else {
                    Fail(ev);
                }
                break;
            case NTxProxy::TResultStatus::ExecInProgress:
                SubscribeOnTransaction(ev);
                break;
            default:
                Fail(ev);
        }
    }

    void Retry() {
        Schedule(TDuration::MilliSeconds(50 + RandomNumber<ui64>(30)), new NActors::TEvents::TEvWakeup());
    }

    void SubscribeOnTransaction(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        NActors::IActor* pipeActor = NTabletPipe::CreateClient(SelfId(), ev->Get()->Record.GetSchemeShardTabletId());
        Y_VERIFY(pipeActor);
        SchemePipeActorId = Register(pipeActor);
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        const ui64 txId = ev->Get()->Status() == NTxProxy::TResultStatus::ExecInProgress ? ev->Get()->Record.GetTxId() : ev->Get()->Record.GetPathCreateTxId();
        request->Record.SetTxId(txId);
        NTabletPipe::SendData(SelfId(), SchemePipeActorId, std::move(request));
        KQP_PROXY_LOG_D("Subscribe on create table " << TableName() << " tx: " << txId);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            KQP_PROXY_LOG_E("Create table " << TableName() << ". Tablet to pipe not conected: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status) << ", retry");
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
            SchemePipeActorId = {};
            Retry();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        KQP_PROXY_LOG_E("Create table " << TableName() << ". Tablet to pipe destroyed, retry");
        SchemePipeActorId = {};
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        KQP_PROXY_LOG_D("Create table " << TableName() << ". Transaction completed: " << ev->Get()->Record.GetTxId());
        Success(ev);
    }

    void Fail(NSchemeCache::TSchemeCacheNavigate::EStatus status) {
        KQP_PROXY_LOG_E("Failed to create table " << TableName() << ": " << status);
        Reply();
    }

    void Fail(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        KQP_PROXY_LOG_E("Failed to create table " << TableName() << ": " << ev->Get()->Status() << ". Response: " << ev->Get()->Record);
        Reply();
    }

    void Success() {
        Reply();
    }

    void Success(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        KQP_PROXY_LOG_I("Successfully created table " << TableName() << ": " << ev->Get()->Status());
        Reply();
    }

    void Success(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        KQP_PROXY_LOG_I("Successfully created table " << TableName() << ". TxId: " << ev->Get()->Record.GetTxId());
        Reply();
    }

    void Reply() {
        Send(Owner, new TEvPrivate::TEvCreateTableResponse());
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        PassAway();
    }

    const TString& TableName() const {
        return PathComponents.back();
    }

private:
    const TVector<TString> PathComponents;
    const TVector<NKikimrSchemeOp::TColumnDescription> Columns;
    const TVector<TString> KeyColumns;
    NActors::TActorId Owner;
    NActors::TActorId SchemePipeActorId;
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

    void RunCreateScriptExecutions() {
        Register(
            new TTableCreator(
                { ".metadata", "script_executions" },
                {
                    Col("database", NScheme::NTypeIds::Text),
                    Col("execution_id", NScheme::NTypeIds::Text),
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
                    Col("store_deadline", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                    Col("meta", NScheme::NTypeIds::JsonDocument),
                    Col("parameters", NScheme::NTypeIds::String), // TODO: store aparameters separately to support bigger storage.
                },
                { "database", "execution_id" }
            )
        );
    }

    void RunCreateScriptExecutionLeases() {
        Register(
            new TTableCreator(
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

    void Handle(TEvPrivate::TEvCreateTableResponse::TPtr&) {
        if (++TablesCreated == 2) {
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
    size_t TablesCreated = 0;
};

class TCreateScriptOperationQuery : public TQueryBase {
public:
    TCreateScriptOperationQuery(const TString& executionId, const NKikimrKqp::TEvQueryRequest& req)
        : ExecutionId(executionId)
        , Request(req)
    {
    }

    Ydb::Query::ExecMode GetExecMode() const {
        switch (Request.GetRequest().GetAction()) {
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
                throw std::runtime_error(TStringBuilder() << "Unsupported query action: " << NKikimrKqp::EQueryAction_Name(Request.GetRequest().GetAction()));
        }
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $execution_mode AS Int32;
            DECLARE $query_text AS Text;
            DECLARE $syntax AS Int32;
            DECLARE $lease_duration AS Interval;

            UPSERT INTO `.metadata/script_executions`
                (database, execution_id, execution_status, execution_mode, start_ts, query_text, syntax)
            VALUES ($database, $execution_id, $execution_status, $execution_mode, CurrentUtcTimestamp(), $query_text, $syntax);

            UPSERT INTO `.metadata/script_execution_leases`
                (database, execution_id, lease_deadline, lease_generation)
            VALUES ($database, $execution_id, CurrentUtcTimestamp() + $lease_duration, 1);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request.GetRequest().GetDatabase())
                .Build()
            .AddParam("$execution_id")
                .Utf8(ExecutionId)
                .Build()
            .AddParam("$execution_status")
                .Int32(Ydb::Query::EXEC_STATUS_STARTING)
                .Build()
            .AddParam("$execution_mode")
                .Int32(GetExecMode())
                .Build()
            .AddParam("$query_text")
                .Utf8(Request.GetRequest().GetQuery())
                .Build()
            .AddParam("$syntax")
                .Int32(Ydb::Query::SYNTAX_YQL_V1)
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LEASE_DURATION.MicroSeconds()))
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
    NKikimrKqp::TEvQueryRequest Request;
};

struct TCreateScriptExecutionActor : public TActorBootstrapped<TCreateScriptExecutionActor> {
    TCreateScriptExecutionActor(TEvKqp::TEvScriptRequest::TPtr&& ev)
        : Event(std::move(ev))
    {
    }

    void Bootstrap() {
        Become(&TCreateScriptExecutionActor::StateFunc);

        // Start request
        RunScriptActorId = Register(CreateRunScriptActor(Event->Get()->Record, Event->Get()->Record.GetRequest().GetDatabase(), 1));
        TString executionId = ActorIdToScriptExecutionId(RunScriptActorId);
        Register(new TCreateScriptOperationQuery(executionId, Event->Get()->Record));
    }

    void Handle(TEvPrivate::TEvCreateScriptOperationResponse::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            Send(RunScriptActorId, new NActors::TEvents::TEvWakeup());
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ScriptExecutionOperationFromExecutionId(ev->Get()->ExecutionId), ev->Get()->ExecutionId, Ydb::Query::EXEC_STATUS_STARTING, Ydb::Query::EXEC_MODE_EXECUTE));
        } else {
            Send(RunScriptActorId, new NActors::TEvents::TEvPoison());
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ev->Get()->Status, std::move(ev->Get()->Issues)));
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCreateScriptOperationResponse, Handle);
    )

private:
    TEvKqp::TEvScriptRequest::TPtr Event;
    NActors::TActorId RunScriptActorId;
};

class TScriptExecutionFinisherBase : public TQueryBase {
public:
    using TQueryBase::TQueryBase;

    void FinishScriptExecution(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus, const NYql::TIssues& issues, TTxControl txControl = TTxControl::ContinueAndCommitTx()) {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
            DECLARE $issues AS JsonDocument;

            UPDATE `.metadata/script_executions`
            SET
                operation_status = $operation_status,
                execution_status = $execution_status,
                issues = $issues,
                end_ts = CurrentUtcTimestamp()
            WHERE database = $database AND execution_id = $execution_id;

            DELETE FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;
        )";

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
                .Build();

        RunDataQuery(sql, &params, txControl);
    }

    void FinishScriptExecution(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus, const TString& message, TTxControl txControl = TTxControl::ContinueAndCommitTx()) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        FinishScriptExecution(database, executionId, operationStatus, execStatus, issues, txControl);
    }
};

class TScriptExecutionFinisher : public TScriptExecutionFinisherBase {
public:
    TScriptExecutionFinisher(
        const TString& executionId,
        const TString& database,
        ui64 leaseGeneration,
        Ydb::StatusIds::StatusCode operationStatus,
        Ydb::Query::ExecStatus execStatus,
        NYql::TIssues issues
    )
        : Database(database)
        , ExecutionId(executionId)
        , LeaseGeneration(leaseGeneration)
        , OperationStatus(operationStatus)
        , ExecStatus(execStatus)
        , Issues(std::move(issues))
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT lease_generation FROM `.metadata/script_execution_leases`
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
            if (ResultSets.size() != 1) {
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

            FinishScriptExecution(Database, ExecutionId, OperationStatus, ExecStatus, Issues);
            FinishWasRun = true;
        } else {
            Finish();
        }
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Finish script execution operation. ExecutionId: " << ExecutionId << ". Lease generation: " << LeaseGeneration << ": " << Ydb::StatusIds::StatusCode_Name(status) << ". Issues: " << issues.ToOneLineString());
        Send(Owner, new TEvScriptExecutionFinished(status, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const ui64 LeaseGeneration;
    const Ydb::StatusIds::StatusCode OperationStatus;
    const Ydb::Query::ExecStatus ExecStatus;
    const NYql::TIssues Issues;
    bool FinishWasRun = false;
};

class TGetScriptExecutionOperationActor : public TQueryBase {
public:
    explicit TGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status,
                query_text,
                syntax,
                execution_mode,
                issues
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;
        )";

        TMaybe<TString> maybeExecutionId = ScriptExecutionFromOperation(Request->Get()->OperationId);
        Y_ENSURE(maybeExecutionId, "No execution id specified");

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Request->Get()->Database)
                .Build()
            .AddParam("$execution_id")
                .Utf8(*maybeExecutionId)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
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

        const TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
        if (operationStatus) {
            Ready = true;
        }

        Ydb::Query::ExecuteScriptMetadata metadata;

        metadata.set_execution_id(*ScriptExecutionFromOperation(Request->Get()->OperationId));

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

        Metadata.ConstructInPlace().PackFrom(metadata);

        Finish(Ydb::StatusIds::SUCCESS, std::move(issues));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(Ready, status, std::move(issues), std::move(Metadata)));
    }

private:
    TEvGetScriptExecutionOperation::TPtr Request;
    bool Ready = false;
    TMaybe<google::protobuf::Any> Metadata;
};

class TListScriptExecutionOperationsActor : public TQueryBase {
public:
    TListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr ev)
        : Request(std::move(ev))
        , PageSize(ClampVal<ui64>(Request->Get()->PageSize, 1, 500))
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
        if (Request->Get()->PageToken) {
            sql << R"(
                DECLARE $execution_id AS Text;
                DECLARE $ts AS Timestamp;
            )";
        }
        sql << R"(
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
            WHERE database = $database
        )";
        if (Request->Get()->PageToken) {
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
                .Utf8(Request->Get()->Database)
                .Build()
            .AddParam("$page_size")
                .Uint64(PageSize + 1)
                .Build();

        if (Request->Get()->PageToken) {
            auto pageTokenParts = ParsePageToken(Request->Get()->PageToken);
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
        Send(Request->Sender, new TEvListScriptExecutionOperationsResponse(status, std::move(issues), NextPageToken, std::move(Operations)));
    }

private:
    TEvListScriptExecutionOperations::TPtr Request;
    ui64 PageSize = 0;
    TString NextPageToken;
    std::vector<Ydb::Operations::Operation> Operations;
};

class TCheckLeaseStatusActor : public TScriptExecutionFinisherBase {
public:
    TCheckLeaseStatusActor(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode statusOnExpiredLease = Ydb::StatusIds::ABORTED)
        : Database(database)
        , ExecutionId(executionId)
        , StatusOnExpiredLease(statusOnExpiredLease)
    {}

    void OnRunQuery() override {
        const TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT operation_status FROM `.metadata/script_executions`
                WHERE database = $database AND execution_id = $execution_id;

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

            TMaybe<i32> operationStatus = result.ColumnParser(0).GetOptionalInt32();
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
                    FinishWasRun = true;
                    FinishScriptExecution(Database, ExecutionId, StatusOnExpiredLease, Ydb::Query::EXEC_STATUS_ABORTED, "Lease expired");
                } else {
                    // OperationStatus is Nothing(): currently running
                    Finish();
                }
            } else if (operationStatus) {
                OperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
                Finish();
            } else {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Invalid operation state");
            }
        } else {
            OperationStatus = StatusOnExpiredLease;
            Finish();
        }
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvLeaseCheckResult(status, std::move(issues), OperationStatus));
    }

private:
    const TInstant RunStartTime = TInstant::Now();
    const TString Database;
    const TString ExecutionId;
    const Ydb::StatusIds::StatusCode StatusOnExpiredLease;
    TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
    bool FinishWasRun = false;
};

class TCancelScriptExecutionOperationActor : public NActors::TActorBootstrapped<TCancelScriptExecutionOperationActor> {
public:
    TCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr ev)
        : Request(std::move(ev))
    {}

    void Bootstrap() {
        const TMaybe<TString> executionId = NKqp::ScriptExecutionFromOperation(Request->Get()->OperationId);
        if (!executionId) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Incorrect operation id");
        }
        ExecutionId = *executionId;

        if (!NKqp::ScriptExecutionIdToActorId(ExecutionId, RunScriptActor)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Incorrect operation id");
        }

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
            Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver request to destination");
        }
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver request to destination");
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

} // anonymous namespace

NActors::IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev) {
    return new TCreateScriptExecutionActor(std::move(ev));
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
    NYql::TIssues issues)
{
    return new TScriptExecutionFinisher(executionId, database, leaseGeneration, operationStatus, execStatus, std::move(issues));
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

} // namespace NKikimr::NKqp
