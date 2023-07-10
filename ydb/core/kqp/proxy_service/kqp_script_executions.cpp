#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/run_script_actor/kqp_run_script_actor.h>
#include <ydb/library/services/services.pb.h>
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
#include <library/cpp/retry/retry_policy.h>

#include <util/generic/guid.h>
#include <util/generic/utility.h>
#include <util/random/random.h>

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
            case EStatus::LookupError:
                [[fallthrough]];
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

    void RunCreateScriptExecutions() {
        TablesCreating++;
        Register(
            new TTableCreator(
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
                    Col("store_deadline", NScheme::NTypeIds::Timestamp), // Will be deleted from database after this deadline.
                    Col("meta", NScheme::NTypeIds::JsonDocument),
                    Col("parameters", NScheme::NTypeIds::String), // TODO: store aparameters separately to support bigger storage.
                    Col("result_set_metas", NScheme::NTypeIds::JsonDocument),
                    Col("stats", NScheme::NTypeIds::JsonDocument),
                },
                { "database", "execution_id" }
            )
        );
    }

    void RunCreateScriptExecutionLeases() {
        TablesCreating++;
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

    void RunCreateScriptResultSets() {
        TablesCreating++;
        Register(
            new TTableCreator(
                { ".metadata", "result_sets" },
                {
                    Col("database", NScheme::NTypeIds::Text),
                    Col("execution_id", NScheme::NTypeIds::Text),
                    Col("result_set_id", NScheme::NTypeIds::Int32),
                    Col("row_id", NScheme::NTypeIds::Int64),
                    Col("expire_at", NScheme::NTypeIds::Timestamp),
                    Col("result_set", NScheme::NTypeIds::String),
                },
                { "database", "execution_id", "result_set_id", "row_id" }
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
    TCreateScriptOperationQuery(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& req, TDuration leaseDuration = TDuration::Zero())
        : ExecutionId(executionId)
        , RunScriptActorId(runScriptActorId)
        , Request(req)
        , LeaseDuration(leaseDuration ? leaseDuration : LEASE_DURATION)
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $run_script_actor_id AS Text;
            DECLARE $execution_status AS Int32;
            DECLARE $execution_mode AS Int32;
            DECLARE $query_text AS Text;
            DECLARE $syntax AS Int32;
            DECLARE $lease_duration AS Interval;

            UPSERT INTO `.metadata/script_executions`
                (database, execution_id, run_script_actor_id, execution_status, execution_mode, start_ts, query_text, syntax)
            VALUES ($database, $execution_id, $run_script_actor_id, $execution_status, $execution_mode, CurrentUtcTimestamp(), $query_text, $syntax);

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
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
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
    TDuration LeaseDuration;
};

struct TCreateScriptExecutionActor : public TActorBootstrapped<TCreateScriptExecutionActor> {
    TCreateScriptExecutionActor(TEvKqp::TEvScriptRequest::TPtr&& ev, TDuration leaseDuration = TDuration::Zero())
        : Event(std::move(ev))
        , LeaseDuration(leaseDuration ? leaseDuration : LEASE_DURATION)
    {
    }

    void Bootstrap() {
        Become(&TCreateScriptExecutionActor::StateFunc);

        ExecutionId = CreateGuidAsString();

        // Start request
        RunScriptActorId = Register(CreateRunScriptActor(ExecutionId, Event->Get()->Record, Event->Get()->Record.GetRequest().GetDatabase(), 1, LeaseDuration));
        Register(new TCreateScriptOperationQuery(ExecutionId, RunScriptActorId, Event->Get()->Record, LeaseDuration));
    }

    void Handle(TEvPrivate::TEvCreateScriptOperationResponse::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            Send(RunScriptActorId, new NActors::TEvents::TEvWakeup());
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ScriptExecutionOperationFromExecutionId(ev->Get()->ExecutionId), ev->Get()->ExecutionId, Ydb::Query::EXEC_STATUS_STARTING, GetExecModeFromAction(Event->Get()->Record.GetRequest().GetAction())));
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
    TString ExecutionId;
    NActors::TActorId RunScriptActorId;
    TDuration LeaseDuration;
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

        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $lease_deadline AS Timestamp;

            UPDATE `.metadata/script_execution_leases`
            SET lease_deadline=$lease_deadline
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
            .AddParam("$lease_deadline")
                .Timestamp(TInstant::Now() + LeaseDuration)
                .Build();

        RunDataQuery(sql, &params, TTxControl::ContinueAndCommitTx());
        SetQueryResultHandler(&TScriptLeaseUpdater::OnQueryResult);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvScriptLeaseUpdateResponse(LeaseExists, status, std::move(issues)));
    }

private:
    const TString Database;
    const TString ExecutionId;
    const TDuration LeaseDuration;
    bool LeaseExists = true;
};

class TScriptLeaseUpdateActor : public TActorBootstrapped<TScriptLeaseUpdateActor> {
public:
    using IRetryPolicy = IRetryPolicy<const Ydb::StatusIds::StatusCode&>;

    TScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration)
        : RunScriptActorId(runScriptActorId)
        , Database(database)
        , ExecutionId(executionId)
        , LeaseDuration(leaseDuration)
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
            || status == Ydb::StatusIds::SESSION_BUSY) {
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
    IRetryPolicy::IRetryState::TPtr RetryState = nullptr;
};


class TScriptExecutionFinisherBase : public TQueryBase {
public:
    using TQueryBase::TQueryBase;

    void FinishScriptExecution(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus,
                               const NYql::TIssues& issues = LeaseExpiredIssues(), TTxControl txControl = TTxControl::ContinueAndCommitTx(),
                               TMaybe<NKqpProto::TKqpStatsQuery> kqpStats = Nothing(), TMaybe<TString> queryPlan = Nothing(), TMaybe<TString> queryAst = Nothing()) {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $operation_status AS Int32;
            DECLARE $execution_status AS Int32;
            DECLARE $issues AS JsonDocument;
            DECLARE $plan AS JsonDocument;
            DECLARE $stats AS JsonDocument;
            DECLARE $ast AS Text;

            UPDATE `.metadata/script_executions`
            SET
                operation_status = $operation_status,
                execution_status = $execution_status,
                issues = $issues,
                plan = $plan,
                end_ts = CurrentUtcTimestamp(),
                stats = $stats,
                ast = $ast
            WHERE database = $database AND execution_id = $execution_id;

            DELETE FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;
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
                .Build();

        RunDataQuery(sql, &params, txControl);
    }

    void FinishScriptExecution(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus, const TString& message, TTxControl txControl = TTxControl::ContinueAndCommitTx()) {
        FinishScriptExecution(database, executionId, operationStatus, execStatus, IssuesFromMessage(message), txControl);
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

            FinishScriptExecution(Database, ExecutionId, OperationStatus, ExecStatus, Issues, TTxControl::ContinueAndCommitTx(), std::move(QueryStats), std::move(QueryPlan), std::move(QueryAst));
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
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT operation_status, execution_status, issues, run_script_actor_id FROM `.metadata/script_executions`
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
        SetQueryResultHandler(&TCheckLeaseStatusActor::OnResult);
    }

    void OnResult() {
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
                FinishScriptExecution(Database, ExecutionId, StatusOnExpiredLease, Ydb::Query::EXEC_STATUS_ABORTED);
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
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            SELECT
                operation_status,
                execution_status
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;

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
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;

            DELETE
            FROM `.metadata/script_executions`
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

class TGetScriptExecutionOperationActor : public TScriptExecutionFinisherBase {
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
                result_set_metas,
                plan,
                issues,
                stats,
                ast
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;

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
        SetQueryResultHandler(&TGetScriptExecutionOperationActor::OnGetInfo);
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
            Ready = true;
        }

        Ydb::Query::ExecuteScriptMetadata metadata;

        metadata.set_execution_id(*ScriptExecutionIdFromOperation(Request->Get()->OperationId));

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

        const TMaybe<TString> serializedStats = result.ColumnParser("stats").GetOptionalJsonDocument();
        if (serializedStats) {
            NJson::TJsonValue statsJson;
            NJson::ReadJsonTree(*serializedStats, &statsJson);
            NProtobufJson::Json2Proto(statsJson, *metadata.mutable_exec_stats(), NProtobufJson::TJson2ProtoConfig());
        }

        const TMaybe<TString> plan = result.ColumnParser("plan").GetOptionalJsonDocument();
        if (plan) {
            metadata.mutable_exec_stats()->set_query_plan(*plan);
        }

        const TMaybe<TString> ast = result.ColumnParser("ast").GetOptionalUtf8();
        if (ast) {
            metadata.mutable_exec_stats()->set_query_ast(*ast);
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
                NProtobufJson::Json2Proto(*metaValue, *metadata.add_result_sets_meta());
            }
        }

        bool finishing = false;
        if (!operationStatus) {
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

            if (*leaseDeadline < TInstant::Now()) {
                finishing = true;

                metadata.set_exec_status(Ydb::Query::EXEC_STATUS_ABORTED);
                Ready = true;
                Issues = LeaseExpiredIssues();

                FinishScriptExecution(Request->Get()->Database, ExecutionId, Ydb::StatusIds::ABORTED, Ydb::Query::EXEC_STATUS_ABORTED, Issues);
                SetQueryResultHandler(&TGetScriptExecutionOperationActor::OnFinishOperation);
            }
        }

        Metadata.ConstructInPlace().PackFrom(metadata);

        if (!finishing) {
            CommitTransaction();
        }
    }

    void OnFinishOperation() {
        Finish(Ydb::StatusIds::SUCCESS, std::move(Issues));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(Ready, status, std::move(Issues), std::move(Metadata)));
        } else {
            Send(Request->Sender, new TEvGetScriptExecutionOperationResponse(false, status, std::move(issues), Nothing()));
        }
    }

private:
    TEvGetScriptExecutionOperation::TPtr Request;
    TString ExecutionId;
    bool Ready = false;
    NYql::TIssues Issues;
    TMaybe<google::protobuf::Any> Metadata;
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
    TGetScriptExecutionResultQuery(const TString& database, const TString& executionId, i32 resultSetId, i64 offset, i64 limit)
        : Database(database), ExecutionId(executionId), ResultSetId(resultSetId), Offset(offset), Limit(limit)
    {
        Response = MakeHolder<TEvKqp::TEvFetchScriptResultsResponse>();
        Response->Record.SetResultSetIndex(ResultSetId);
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $database AS Text;
            DECLARE $execution_id AS Text;
            DECLARE $result_set_id AS Int32;
            DECLARE $offset AS Int64;
            DECLARE $limit AS Uint64;

            SELECT result_set_metas, operation_status, issues
            FROM `.metadata/script_executions`
            WHERE database = $database
              AND execution_id = $execution_id;

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
                .Int32(ResultSetId)
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
                Finish(Ydb::StatusIds::BAD_REQUEST, "Script execution not found");
                return;
            }

            TMaybe<i32> operationStatus = result.ColumnParser("operation_status").GetOptionalInt32();
            if (!operationStatus) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Results are not ready");
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
            if (!value.GetValuePointer(ResultSetId, &metaValue)) {
                Finish(Ydb::StatusIds::BAD_REQUEST, "Result set index is invalid");
                return;
            }

            Ydb::Query::ResultSetMeta meta;
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
    const i32 ResultSetId;
    const i64 Offset;
    const i64 Limit;
    THolder<TEvKqp::TEvFetchScriptResultsResponse> Response;
};

class TGetScriptExecutionResultActor : public TActorBootstrapped<TGetScriptExecutionResultActor> {
public:
    TGetScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetId, i64 offset, i64 limit)
        : ReplyActorId(replyActorId), Database(database), ExecutionId(executionId), ResultSetId(resultSetId), Offset(offset), Limit(limit)
    {
    }

    void Bootstrap() {
        Register(new TGetScriptExecutionResultQuery(Database, ExecutionId, ResultSetId, Offset, Limit));

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
    const i32 ResultSetId;
    const i64 Offset;
    const i64 Limit;
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

NActors::IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration) {
    return new TScriptLeaseUpdateActor(runScriptActorId, database, executionId, leaseDuration);
}

NActors::IActor* CreateSaveScriptExecutionResultMetaActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, const TString& serializedMeta) {
    return new TSaveScriptExecutionResultMetaActor(replyActorId, database, executionId, serializedMeta);
}

NActors::IActor* CreateSaveScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetId, TInstant expireAt, i64 firstRow, std::vector<TString>&& serializedRows) {
    return new TSaveScriptExecutionResultActor(replyActorId, database, executionId, resultSetId, expireAt, firstRow, std::move(serializedRows));
}

NActors::IActor* CreateGetScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetId, i64 offset, i64 limit) {
    return new TGetScriptExecutionResultActor(replyActorId, database, executionId, resultSetId, offset, limit);
}

namespace NPrivate {

NActors::IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record, TDuration leaseDuration) {
    return new TCreateScriptOperationQuery(executionId, runScriptActorId, record, leaseDuration);
}

NActors::IActor* CreateCheckLeaseStatusActor(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode statusOnExpiredLease, ui64 cookie) {
    return new TCheckLeaseStatusActor(database, executionId, statusOnExpiredLease, cookie);
}

} // namespace NPrivate

} // namespace NKikimr::NKqp
