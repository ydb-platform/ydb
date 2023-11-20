#include "kqp_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/gateway/local_rpc/helper.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>

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
        };

        struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
            IKqpGateway::TGenericResult Result;
        };
    };
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpSchemeExecuter(TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target, const TMaybe<TString>& requestType, 
        const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        bool temporary, TString sessionId, TIntrusivePtr<TUserRequestContext> ctx)
        : PhyTx(phyTx)
        , Target(target)
        , Database(database)
        , UserToken(userToken)
        , Temporary(temporary)
        , SessionId(sessionId)
        , RequestContext(std::move(ctx))
        , RequestType(requestType)
    {
        YQL_ENSURE(PhyTx);
        YQL_ENSURE(PhyTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME);

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(nullptr);
    }

    void StartBuildOperation() {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        auto buildOp = schemeOp.GetBuildOperation();
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TKqpSchemeExecuter::ExecuteState);    
    }

    void Bootstrap() {
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
                    tableDesc->SetName(tableDesc->GetName() + SessionId);
                    tableDesc->SetPath(tableDesc->GetPath() + SessionId);
                }
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTable: {
                auto modifyScheme = schemeOp.GetDropTable();
                if (Temporary) {
                    auto* dropTable = modifyScheme.MutableDrop();
                    dropTable->SetName(dropTable->GetName() + SessionId);
                }
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTable: {
                auto modifyScheme = schemeOp.GetAlterTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kBuildOperation: {
                auto buildOp = schemeOp.GetBuildOperation();
                return StartBuildOperation();
            }

            default:
                InternalError(TStringBuilder() << "Unexpected scheme operation: "
                    << (ui32) schemeOp.GetOperationCase());
                return;
        }

        auto promise = NewPromise<IKqpGateway::TGenericResult>();

        bool successOnNotExist = ev->Record.GetTransaction().GetModifyScheme().GetSuccessOnNotExist();
        bool failedOnAlreadyExists = ev->Record.GetTransaction().GetModifyScheme().GetFailedOnAlreadyExists();
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

public:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResult, HandleExecute);
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


    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        const auto* msg = ev->Get();
        TxId = msg->TxId;

        Navigate(msg->Services.SchemeCache);
    }

    void Navigate(const TActorId& schemeCache) {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        auto buildOp = schemeOp.GetBuildOperation();
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
        auto buildOp = schemeOp.GetBuildOperation();
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
};

} // namespace

IActor* CreateKqpSchemeExecuter(TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target,
    const TMaybe<TString>& requestType, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool temporary, TString sessionId,
    TIntrusivePtr<TUserRequestContext> ctx)
{
    return new TKqpSchemeExecuter(phyTx, target, requestType, database, userToken, temporary, sessionId, std::move(ctx));
}

} // namespace NKikimr::NKqp
