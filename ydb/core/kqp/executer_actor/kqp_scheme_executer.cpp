#include "kqp_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/gateway/actors/analyze_actor.h>
#include <ydb/core/kqp/gateway/local_rpc/helper.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/services/metadata/abstract/kqp_common.h>

namespace NKikimr::NKqp {

using namespace NThreading;

namespace {

static bool CheckAlterAccess(const NACLib::TUserToken& userToken, const NSchemeCache::TSchemeCacheNavigate* navigate) {
    bool isDatabase = true; // first entry is always database

    using TEntry = NSchemeCache::TSchemeCacheNavigate::TEntry;

    for (const TEntry& entry : navigate->ResultSet) {
        if (!entry.SecurityObject) {
            continue;
        }

        const ui32 access = isDatabase ? NACLib::CreateDirectory | NACLib::CreateTable : NACLib::GenericRead | NACLib::GenericWrite;
        if (!entry.SecurityObject->CheckAccess(access, userToken)) {
            return false;
        }

        isDatabase = false;
    }

    return true;
}

class TKqpSchemeExecuter : public TActorBootstrapped<TKqpSchemeExecuter> {
    struct TEvPrivate {
        enum EEv {
            EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvMakeTempDirResult,
        };

        struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
            IKqpGateway::TGenericResult Result;
        };

        struct TEvMakeTempDirResult : public TEventLocal<TEvMakeTempDirResult, EEv::EvMakeTempDirResult> {
            IKqpGateway::TGenericResult Result;
        };
    };
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpSchemeExecuter(
        TKqpPhyTxHolder::TConstPtr phyTx, NKikimrKqp::EQueryType queryType, const TActorId& target, const TMaybe<TString>& requestType,
        const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        bool temporary, TString sessionId, TIntrusivePtr<TUserRequestContext> ctx,
        const TActorId& kqpTempTablesAgentActor)
        : PhyTx(phyTx)
        , QueryType(queryType)
        , Target(target)
        , Database(database)
        , UserToken(userToken)
        , Temporary(temporary)
        , SessionId(sessionId)
        , RequestContext(std::move(ctx))
        , RequestType(requestType)
        , KqpTempTablesAgentActor(kqpTempTablesAgentActor)
    {
        YQL_ENSURE(PhyTx);
        YQL_ENSURE(PhyTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME);

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(
            nullptr,
            TEvKqpExecuter::TEvTxResponse::EExecutionType::Scheme);
    }

    void StartBuildOperation() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void CreateTmpDirectory() {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& record = ev->Record;

        record.SetDatabaseName(Database);
        if (UserToken) {
            record.SetUserToken(UserToken->GetSerializedToken());
        }

        auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(GetSessionDirsBasePath(Database));
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        modifyScheme->SetAllowCreateInTempDir(false);
        auto* makeDir = modifyScheme->MutableMkDir();
        makeDir->SetName(SessionId);
        ActorIdToProto(KqpTempTablesAgentActor, modifyScheme->MutableTempDirOwnerActorId());

        auto promise = NewPromise<IKqpGateway::TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, false);
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvMakeTempDirResult>();
            ev->Result = future.GetValue();
            actorSystem->Send(selfId, ev.Release());
        });
        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void MakeSchemeOperationRequest() {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(Database);
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        if (RequestType) {
            ev->Record.SetRequestType(*RequestType);
        }

        const auto& schemeOp = PhyTx->GetSchemeOperation();
        switch (schemeOp.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateTable: {
                auto modifyScheme = schemeOp.GetCreateTable();
                if (Temporary) {
                    NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
                    switch (modifyScheme.GetOperationType()) {
                        case NKikimrSchemeOp::ESchemeOpCreateTable: {
                            tableDesc = modifyScheme.MutableCreateTable();
                            break;
                        }
                        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: {
                            tableDesc = modifyScheme.MutableCreateIndexedTable()->MutableTableDescription();
                            break;
                        }
                        default:
                            YQL_ENSURE(false, "Unexpected operation type");
                    }
                    const auto fullPath = JoinPath({tableDesc->GetPath(), tableDesc->GetName()});
                    YQL_ENSURE(fullPath.size() > 1);
                    tableDesc->SetName(GetCreateTempTablePath(Database, SessionId, fullPath));
                    tableDesc->SetPath(Database);
                    modifyScheme.SetAllowCreateInTempDir(true);
                }
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTable: {
                const auto& modifyScheme = schemeOp.GetDropTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTable: {
                const auto& modifyScheme = schemeOp.GetAlterTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kBuildOperation: {
                return StartBuildOperation();
            }

            case NKqpProto::TKqpSchemeOperation::kCreateUser: {
                const auto& modifyScheme = schemeOp.GetCreateUser();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterUser: {
                const auto& modifyScheme = schemeOp.GetAlterUser();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropUser: {
                const auto& modifyScheme = schemeOp.GetDropUser();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }
            case NKqpProto::TKqpSchemeOperation::kCreateExternalTable: {
                const auto& modifyScheme = schemeOp.GetCreateExternalTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }
            case NKqpProto::TKqpSchemeOperation::kAlterExternalTable: {
                const auto& modifyScheme = schemeOp.GetAlterExternalTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }
            case NKqpProto::TKqpSchemeOperation::kDropExternalTable: {
                const auto& modifyScheme = schemeOp.GetDropExternalTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateGroup: {
                const auto& modifyScheme = schemeOp.GetCreateGroup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAddGroupMembership: {
                const auto& modifyScheme = schemeOp.GetAddGroupMembership();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kRemoveGroupMembership: {
                const auto& modifyScheme = schemeOp.GetRemoveGroupMembership();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kRenameGroup: {
                const auto& modifyScheme = schemeOp.GetRenameGroup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropGroup: {
                const auto& modifyScheme = schemeOp.GetDropGroup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kModifyPermissions: {
                const auto& modifyScheme = schemeOp.GetModifyPermissions();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateColumnTable: {
                const auto& modifyScheme = schemeOp.GetCreateColumnTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterColumnTable: {
                const auto& modifyScheme = schemeOp.GetAlterColumnTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateTableStore: {
                const auto& modifyScheme = schemeOp.GetCreateTableStore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTableStore: {
                const auto& modifyScheme = schemeOp.GetAlterTableStore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTableStore: {
                const auto& modifyScheme = schemeOp.GetDropTableStore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateSequence: {
                const auto& modifyScheme = schemeOp.GetCreateSequence();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropSequence: {
                const auto& modifyScheme = schemeOp.GetDropSequence();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateReplication: {
                const auto& modifyScheme = schemeOp.GetCreateReplication();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterReplication: {
                const auto& modifyScheme = schemeOp.GetAlterReplication();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropReplication: {
                const auto& modifyScheme = schemeOp.GetDropReplication();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterSequence: {
                const auto& modifyScheme = schemeOp.GetAlterSequence();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAnalyzeTable: {
                const auto& analyzeOperation = schemeOp.GetAnalyzeTable();
                
                auto analyzePromise = NewPromise<IKqpGateway::TGenericResult>();
                
                TVector<TString> columns{analyzeOperation.columns().begin(), analyzeOperation.columns().end()};
                IActor* analyzeActor = new TAnalyzeActor(analyzeOperation.GetTablePath(), columns, analyzePromise);

                auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
                RegisterWithSameMailbox(analyzeActor);

                auto selfId = SelfId();
                analyzePromise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
                    auto ev = MakeHolder<TEvPrivate::TEvResult>();
                    ev->Result = future.GetValue();

                    actorSystem->Send(selfId, ev.Release());
                });
                
                Become(&TKqpSchemeExecuter::ExecuteState);
                return;
            }

            default:
                InternalError(TStringBuilder() << "Unexpected scheme operation: "
                    << (ui32) schemeOp.GetOperationCase());
                return;
        }

        auto promise = NewPromise<IKqpGateway::TGenericResult>();

        bool successOnNotExist = false;
        bool failedOnAlreadyExists = false;
        // exists/not exists semantics supported only in the query service.
        if (IsQueryService()) {
            successOnNotExist = ev->Record.GetTransaction().GetModifyScheme().GetSuccessOnNotExist();
            failedOnAlreadyExists = ev->Record.GetTransaction().GetModifyScheme().GetFailedOnAlreadyExists();
        }

        IActor* requestHandler = new TSchemeOpRequestHandler(
            ev.Release(),
            promise,
            failedOnAlreadyExists,
            successOnNotExist
        );
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvResult>();
            ev->Result = future.GetValue();

            actorSystem->Send(selfId, ev.Release());
        });

        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void MakeObjectRequest() {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TFactory::Construct(schemeOp.GetObjectType()));
        if (!cBehaviour) {
            InternalError(TStringBuilder() << "Unsupported object type: \"" << schemeOp.GetObjectType() << "\"");
            return;
        }

        if (!cBehaviour->GetOperationsManager()) {
            InternalError(TStringBuilder() << "Object type \"" << schemeOp.GetObjectType() << "\" does not have manager for operations");
        }

        auto* actorSystem = TActivationContext::ActorSystem();
        auto selfId = SelfId();

        NMetadata::NModifications::IOperationsManager::TExternalModificationContext context;
        context.SetDatabase(Database);
        context.SetActorSystem(actorSystem);
        if (UserToken) {
            context.SetUserToken(*UserToken);
        }

        auto resultFuture = cBehaviour->GetOperationsManager()->ExecutePrepared(schemeOp, SelfId().NodeId(), cBehaviour, context);

        using TResultFuture = NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus>;
        resultFuture.Subscribe([actorSystem, selfId](const TResultFuture& f) {
            const auto& status = f.GetValue();
            auto ev = MakeHolder<TEvPrivate::TEvResult>();
            if (status.Ok()) {
                ev->Result.SetSuccess();
            } else {
                ev->Result.SetStatus(status.GetStatus());
                if (TString message = status.GetErrorMessage()) {
                    ev->Result.AddIssue(NYql::TIssue{message});
                }
            }
            actorSystem->Send(selfId, ev.Release());
        });

        Become(&TKqpSchemeExecuter::ObjectExecuteState);
    }

    void Bootstrap() {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        if (schemeOp.GetObjectType()) {
            MakeObjectRequest();
        } else {
            if (Temporary) {
                CreateTmpDirectory();
            } else {
                MakeSchemeOperationRequest();
            }
        }
    }

public:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResult, HandleExecute);
                hFunc(TEvPrivate::TEvMakeTempDirResult, Handle);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(NSchemeShard::TEvIndexBuilder::TEvCreateResponse, Handle);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, Handle);
                hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
                hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }

    STATEFN(ObjectExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResult, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("ObjectExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }

    void Handle(TEvPrivate::TEvMakeTempDirResult::TPtr& result) {
        if (!result->Get()->Result.Success()) {   
            InternalError(TStringBuilder()
                << "Error creating temporary directory for session " << SessionId
                << ": " << result->Get()->Result.Issues().ToString(true));
        }
        MakeSchemeOperationRequest();
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        const auto* msg = ev->Get();
        TxId = msg->TxId;

        Navigate(msg->Services.SchemeCache);
    }

    void Navigate(const TActorId& schemeCache) {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        const auto& buildOp = schemeOp.GetBuildOperation();
        const auto& path = buildOp.source_path();

        const auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            TString error = TStringBuilder() << "Failed to split table path " << path;
            return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, NYql::TIssue(error));
        }

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();

        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = ::NKikimr::SplitPath(path);
        entry.RedirectRequired = false;

        {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            entry.Path = paths;
        }

        auto ev = std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(request.release());
        Send(schemeCache, ev.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult, errors# " << ev->Get()->Request.Get()->ErrorCount);

        NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

        if (resp->ErrorCount > 0 || resp->ResultSet.empty()) {
            TStringBuilder builder;
            builder << "Unable to navigate:";

            for (const auto& entry : resp->ResultSet) {
                if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    builder << " " << JoinPath(entry.Path) << " status: " << entry.Status;
                }
            }

            TString error(builder);
            LOG_E(error);
            return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssue(error));
        }

        if (UserToken && !UserToken->GetSerializedToken().empty() && !CheckAlterAccess(*UserToken, resp)) {
            LOG_E("Access check failed");
            return ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, NYql::TIssue("Unauthorized"));
        }

        auto domainInfo = resp->ResultSet.front().DomainInfo;
        if (!domainInfo) {
            LOG_E("Got empty domain info");
            return ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssue("empty domain info"));
        }

        const auto& schemeOp = PhyTx->GetSchemeOperation();
        const auto& buildOp = schemeOp.GetBuildOperation();
        SetSchemeShardId(domainInfo->ExtractSchemeShard());
        auto req = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>(TxId, Database, buildOp);
        ForwardToSchemeShard(std::move(req));
    }

    void PassAway() override {
        if (SchemePipeActorId_) {
            NTabletPipe::CloseClient(this->SelfId(), SchemePipeActorId_);
        }

        IActor::PassAway();
    }

    const TIntrusivePtr<TUserRequestContext>& GetUserRequestContext() const {
        return RequestContext;
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvCreateResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        const auto status = response.GetStatus();
        auto issuesProto = response.GetIssues();

        LOG_D("Handle TEvIndexBuilder::TEvCreateResponse " << response.ShortUtf8DebugString());

        if (status == Ydb::StatusIds::SUCCESS) {
            if (response.HasSchemeStatus() && response.GetSchemeStatus() == NKikimrScheme::EStatus::StatusAlreadyExists) {
                return ReplyErrorAndDie(status, &issuesProto);
            } else {
                DoSubscribe();
            }
        } else {
            ReplyErrorAndDie(status, &issuesProto);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Tablet not available, status: " << (ui32)ev->Get()->Status));
            return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issues);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            TStringBuilder() << "Connection to tablet was lost."));
        return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issues);
    }

    void GetIndexStatus() {
        auto request = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvGetRequest>(Database, TxId);
        ForwardToSchemeShard(std::move(request));
    }

    void DoSubscribe() {
        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        ForwardToSchemeShard(std::move(request));
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
        GetIndexStatus();
    }

    void SetSchemeShardId(ui64 schemeShardTabletId) {
        SchemeShardTabletId = schemeShardTabletId;
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvGetResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        LOG_D("Handle TEvIndexBuilder::TEvGetResponse: record# " << record.ShortDebugString());
        return ReplyErrorAndDie(record.GetStatus(), record.MutableIssues());
    }

    template<typename TEv>
    void ForwardToSchemeShard(std::unique_ptr<TEv>&& ev) {
        if (!SchemePipeActorId_) {
            Y_ABORT_UNLESS(SchemeShardTabletId);
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {.RetryLimitCount = 3};
            SchemePipeActorId_ = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), SchemeShardTabletId));
        }

        Y_ABORT_UNLESS(SchemePipeActorId_);
        NTabletPipe::SendData(SelfId(), SchemePipeActorId_, ev.release());
    }

    void HandleExecute(TEvPrivate::TEvResult::TPtr& ev) {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(GetYdbStatus(ev->Get()->Result));
        IssuesToMessage(ev->Get()->Result.Issues(), response.MutableIssues());

        Send(Target, ResponseEv.release());
        PassAway();
    }

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();
        LOG_D("Got EvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());

        ReplyErrorAndDie(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
    }

private:

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        ReplyErrorAndDie(status, &protoIssues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        ReplyErrorAndDie(status, &issues);
    }

    void UnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_C("TKqpSchemeExecuter, unexpected event: " << eventType
            << ", at state:" << state << ", selfID: " << SelfId());

        InternalError(TStringBuilder() << "Unexpected event at TKqpSchemeExecuter, state: " << state
            << ", event: " << eventType);
    }

    bool IsQueryService() const {

        switch(QueryType) {
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
                return true;
            default:
                return false;
        }

    }

    void InternalError(const NYql::TIssues& issues) {
        LOG_E(issues.ToOneLineString());
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED,
            "Internal error while executing scheme operation.");

        for (const NYql::TIssue& i : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void InternalError(const TString& message) {
        InternalError(NYql::TIssues({NYql::TIssue(message)}));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        Send(Target, ResponseEv.release());
        PassAway();
    }

private:
    TKqpPhyTxHolder::TConstPtr PhyTx;
    const NKikimrKqp::EQueryType QueryType;
    const TActorId Target;
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    bool Temporary;
    TString SessionId;
    ui64 TxId = 0;
    TActorId SchemePipeActorId_;
    ui64 SchemeShardTabletId = 0;
    TIntrusivePtr<TUserRequestContext> RequestContext;
    const TMaybe<TString> RequestType;
    const TActorId KqpTempTablesAgentActor;
};

} // namespace

IActor* CreateKqpSchemeExecuter(
    TKqpPhyTxHolder::TConstPtr phyTx, NKikimrKqp::EQueryType queryType, const TActorId& target,
    const TMaybe<TString>& requestType, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool temporary, TString sessionId,
    TIntrusivePtr<TUserRequestContext> ctx, const TActorId& kqpTempTablesAgentActor)
{
    return new TKqpSchemeExecuter(
        phyTx, queryType, target, requestType, database, userToken,
        temporary, sessionId, std::move(ctx), kqpTempTablesAgentActor);
}

} // namespace NKikimr::NKqp
