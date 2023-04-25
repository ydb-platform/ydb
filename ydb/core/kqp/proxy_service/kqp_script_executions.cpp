#include "kqp_script_executions.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/kqp/run_script_actor/kqp_run_script_actor.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/random/random.h>

namespace NKikimr::NKqp {

#define KQP_PROXY_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)
#define KQP_PROXY_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, stream)

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvDataQueryResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSessionResult,
        EvDeleteSessionResult,
        EvCreateScriptOperationResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    static NYql::TIssues IssuesFromOperation(const Ydb::Operations::Operation& operation) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);
        return issues;
    }

    struct TEvDataQueryResult : public NActors::TEventLocal<TEvDataQueryResult, EvDataQueryResult> {
        TEvDataQueryResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
            : Status(status)
            , Issues(std::move(issues))
        {
        }

        TEvDataQueryResult(const Ydb::Table::ExecuteDataQueryResponse& resp)
            : TEvDataQueryResult(resp.operation().status(), IssuesFromOperation(resp.operation()))
        {
            resp.operation().result().UnpackTo(&Result);
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        Ydb::Table::ExecuteQueryResult Result;
    };

    struct TEvCreateSessionResult : public NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
        TEvCreateSessionResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
            : Status(status)
            , Issues(std::move(issues))
        {
        }

        TEvCreateSessionResult(const Ydb::Table::CreateSessionResponse& resp)
            : TEvCreateSessionResult(resp.operation().status(), IssuesFromOperation(resp.operation()))
        {
            Ydb::Table::CreateSessionResult result;
            resp.operation().result().UnpackTo(&result);
            SessionId = result.session_id();
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
    };

    struct TEvDeleteSessionResult : public NActors::TEventLocal<TEvDeleteSessionResult, EvDeleteSessionResult> {
        TEvDeleteSessionResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
            : Status(status)
            , Issues(std::move(issues))
        {
        }

        TEvDeleteSessionResult(const Ydb::Table::DeleteSessionResponse& resp)
            : TEvDeleteSessionResult(resp.operation().status(), IssuesFromOperation(resp.operation()))
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
    };

    struct TEvCreateScriptOperationResponse : public NActors::TEventLocal<TEvCreateScriptOperationResponse, EvCreateScriptOperationResponse> {
        TEvCreateScriptOperationResponse(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues)
            : Status(statusCode)
            , Issues(std::move(issues))
        {
        }

        TEvCreateScriptOperationResponse(TString operationId, TString executionId)
            : Status(Ydb::StatusIds::SUCCESS)
            , OperationId(std::move(operationId))
            , ExecutionId(std::move(executionId))
        {
        }

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        const TString OperationId;
        const TString ExecutionId;
    };
};


class TScriptExecutionsTableCreator : public NActors::TActorBootstrapped<TScriptExecutionsTableCreator> {
public:
    TScriptExecutionsTableCreator(THolder<NActors::IEventBase> resultEvent)
        : ResultEvent(std::move(resultEvent))
    {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TScriptExecutionsTableCreator>::Registered(sys, owner);
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
        Become(&TScriptExecutionsTableCreator::StateFuncCheck);
        CheckTableExistence();
    }

    void CheckTableExistence() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto pathComponents = SplitPath(AppData()->TenantName);
        request->DatabaseName = CanonizePath(pathComponents);
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        pathComponents.emplace_back(".metadata");
        pathComponents.emplace_back("script_executions");
        entry.Path = pathComponents;
        entry.ShowPrivatePath = true;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void RunCreateTableRequest() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        NKikimrSchemeOp::TModifyScheme& modifyScheme = *request->Record.MutableTransaction()->MutableModifyScheme();
        auto pathComponents = SplitPath(AppData()->TenantName);
        pathComponents.emplace_back(".metadata");
        modifyScheme.SetWorkingDir(CanonizePath(pathComponents));
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
        modifyScheme.SetInternal(true);
        modifyScheme.SetAllowAccessToPrivatePaths(true);
        NKikimrSchemeOp::TTableDescription& tableDesc = *modifyScheme.MutableCreateTable();
        tableDesc.SetName("script_executions");
        tableDesc.AddKeyColumnNames("ExecutionId");
        NKikimrSchemeOp::TColumnDescription& execCol = *tableDesc.AddColumns();
        execCol.SetName("ExecutionId");
        execCol.SetType(NScheme::TypeName(NScheme::NTypeIds::String));
        NKikimrSchemeOp::TColumnDescription& opCol = *tableDesc.AddColumns();
        opCol.SetName("OperationId");
        opCol.SetType(NScheme::TypeName(NScheme::NTypeIds::String));
        Send(MakeTxProxyID(), std::move(request));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_VERIFY(request.ResultSet.size() == 1);
        const NSchemeCache::TSchemeCacheNavigate::TEntry& result  = request.ResultSet[0];
        if (result.Status != EStatus::Ok) {
            KQP_PROXY_LOG_D("Describe script operations table result: " << result.Status);
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
                Become(&TScriptExecutionsTableCreator::StateFuncCreate);
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
        KQP_PROXY_LOG_D("TEvProposeTransactionStatus: " << ev->Get()->Record);
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
        KQP_PROXY_LOG_D("Subscribe on create script executions table tx: " << txId);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            KQP_PROXY_LOG_E("Create script executions table. Tablet to pipe not conected: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status) << ", retry");
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
            SchemePipeActorId = {};
            Retry();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        KQP_PROXY_LOG_E("Create script executions table. Tablet to pipe destroyed, retry");
        SchemePipeActorId = {};
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        KQP_PROXY_LOG_D("Create script executions table. Transaction completed: " << ev->Get()->Record.GetTxId());
        Success(ev);
    }

    void Fail(NSchemeCache::TSchemeCacheNavigate::EStatus status) {
        KQP_PROXY_LOG_E("Failed to create script executions table: " << status);
        Reply();
    }

    void Fail(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        KQP_PROXY_LOG_E("Failed to create script executions table: " << ev->Get()->Status() << ". Response: " << ev->Get()->Record);
        Reply();
    }

    void Success() {
        Reply();
    }

    void Success(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        KQP_PROXY_LOG_I("Successfully created script executions table: " << ev->Get()->Status());
        Reply();
    }

    void Success(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        KQP_PROXY_LOG_I("Successfully created script executions table. TxId: " << ev->Get()->Record.GetTxId());
        Reply();
    }

    void Reply() {
        Send(Owner, std::move(ResultEvent));
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        PassAway();
    }

private:
    THolder<NActors::IEventBase> ResultEvent;
    NActors::TActorId Owner;
    NActors::TActorId SchemePipeActorId;
};

NActors::IActor* CreateScriptExecutionsTableCreator(THolder<NActors::IEventBase> resultEvent) {
    return new TScriptExecutionsTableCreator(std::move(resultEvent));
}


// TODO: add retry logic
class TQueryBase : public NActors::TActorBootstrapped<TQueryBase> {
public:
    static constexpr char ActorName[] = "KQP_SCRIPT_EXECUTION_OPERATION_QUERY";

    explicit TQueryBase(const TString& database, const TString& sessionId = {})
        : Database(database)
        , SessionId(sessionId)
    {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TQueryBase>::Registered(sys, owner);
        Owner = owner;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvDataQueryResult, Handle);
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
        hFunc(TEvPrivate::TEvDeleteSessionResult, Handle);
    );

    void Bootstrap() {
        Become(&TQueryBase::StateFunc);

        if (SessionId) {
            RunQuery();
        } else {
            RunCreateSession();
        }
    }

    template <class TProto, class TEvent>
    void Subscribe(NThreading::TFuture<TProto>&& f) {
        f.Subscribe(
            [as = TActivationContext::ActorSystem(), selfId = SelfId()](const NThreading::TFuture<TProto>& res)
            {
                as->Send(selfId, new TEvent(res.GetValue()));
            }
        );
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            SessionId = ev->Get()->SessionId;
            DeleteSession = true;
            RunQuery();
        } else {
            KQP_PROXY_LOG_W("Failed to create session: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }
    }

    void Handle(TEvPrivate::TEvDeleteSessionResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            KQP_PROXY_LOG_W("Failed to delete session: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
        }
        PassAway();
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr& ev) {
        KQP_PROXY_LOG_D("DataQueryResult: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
        ResultSets.Swap(ev->Get()->Result.mutable_result_sets());

        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
    }

    void Finish(Ydb::StatusIds::StatusCode status, const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        Finish(status, std::move(issues));
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        OnFinish(status, std::move(issues));
        if (DeleteSession) {
            RunDeleteSession();
        } else {
            PassAway();
        }
    }

    void RunCreateSession() {
        using TEvCreateSessionRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest,
            Ydb::Table::CreateSessionResponse>;
        Ydb::Table::CreateSessionRequest req;
        Subscribe<Ydb::Table::CreateSessionResponse, TEvPrivate::TEvCreateSessionResult>(NRpcService::DoLocalRpc<TEvCreateSessionRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
    }

    void RunDeleteSession() {
        using TEvDeleteSessionRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest,
            Ydb::Table::DeleteSessionResponse>;
        Ydb::Table::DeleteSessionRequest req;
        req.set_session_id(SessionId);
        Subscribe<Ydb::Table::DeleteSessionResponse, TEvPrivate::TEvDeleteSessionResult>(NRpcService::DoLocalRpc<TEvDeleteSessionRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
    }

    void RunDataQuery(const TString& sql, NYdb::TParamsBuilder* params) {
        KQP_PROXY_LOG_D("RunDataQuery: " << sql);
        using TEvExecuteDataQueryRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest,
            Ydb::Table::ExecuteDataQueryResponse>;
        Ydb::Table::ExecuteDataQueryRequest req;
        req.set_session_id(SessionId);
        req.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        req.mutable_tx_control()->set_commit_tx(true);
        req.mutable_query()->set_yql_text(sql);
        req.mutable_query_cache_policy()->set_keep_in_cache(true);
        if (params) {
            auto p = params->Build();
            *req.mutable_parameters() = NYdb::TProtoAccessor::GetProtoMap(p);
        }
        Subscribe<Ydb::Table::ExecuteDataQueryResponse, TEvPrivate::TEvDataQueryResult>(NRpcService::DoLocalRpc<TEvExecuteDataQueryRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
    }

    virtual void RunQuery() = 0;
    virtual void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) = 0;

protected:
    const TString Database;
    TString SessionId;
    bool DeleteSession = false;

    NActors::TActorId Owner;

    google::protobuf::RepeatedPtrField<Ydb::ResultSet> ResultSets;
};

class TCreateScriptOperationQuery : public TQueryBase {
public:
    TCreateScriptOperationQuery(const TString& executionId, const TString& database)
        : TQueryBase(database)
        , ExecutionId(executionId)
    {
        Ydb::TOperationId operationId;
        operationId.SetKind(Ydb::TOperationId::SCRIPT);
        NOperationId::AddOptionalValue(operationId, "actor_id", ExecutionId);
        OperationId = NOperationId::ProtoToString(operationId);
    }

    void RunQuery() override {
        TString sql = R"(
            DECLARE $execution_id AS String;
            DECLARE $operation_id AS String;

            UPSERT INTO `.metadata/script_executions`
                (ExecutionId, OperationId)
            VALUES ($execution_id, $operation_id);
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$execution_id")
                .String(ExecutionId)
                .Build()
            .AddParam("$operation_id")
                .String(OperationId)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        KQP_PROXY_LOG_D("Create script execution operation. ExecutionId: " << ExecutionId << ". OperationId: " << OperationId << ". Result: " << status << ". Issues: " << issues.ToOneLineString());
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(OperationId, ExecutionId));
        } else {
            Send(Owner, new TEvPrivate::TEvCreateScriptOperationResponse(status, std::move(issues)));
        }
    }

private:
    const TString ExecutionId;
    TString OperationId;
};

struct TCreateScriptExecutionActor : public TActorBootstrapped<TCreateScriptExecutionActor> {
    TCreateScriptExecutionActor(TEvKqp::TEvScriptRequest::TPtr&& ev)
        : Event(std::move(ev))
    {
    }

    void Bootstrap() {
        Become(&TCreateScriptExecutionActor::StateFunc);

        // Start request
        const NActors::TActorId actorId = Register(CreateRunScriptActor(Event->Get()->Record));
        TString executionId = actorId.ToString();
        Register(new TCreateScriptOperationQuery(executionId, Event->Get()->Record.GetRequest().GetDatabase()));
    }

    void Handle(TEvPrivate::TEvCreateScriptOperationResponse::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ev->Get()->OperationId, ev->Get()->ExecutionId, Ydb::Query::EXEC_STATUS_STARTING, Ydb::Query::EXEC_MODE_EXECUTE));
        } else {
            Send(Event->Sender, new TEvKqp::TEvScriptResponse(ev->Get()->Status, std::move(ev->Get()->Issues)));
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCreateScriptOperationResponse, Handle);
    )

private:
    TEvKqp::TEvScriptRequest::TPtr Event;
};

NActors::IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev) {
    return new TCreateScriptExecutionActor(std::move(ev));
}

} // namespace NKikimr::NKqp
