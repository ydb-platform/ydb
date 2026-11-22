#include "service_fq_internal.h"
#include "rpc_common/rpc_common.h"
#include "rpc_deferrable.h"
#include "rpc_scheme_base.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/external_table_description.h>

namespace NKikimr {
namespace NGRpcService {

using TEvFqPrivatePingTaskRequest =
    TGrpcRequestOperationCall<Fq::Private::PingTaskRequest, Fq::Private::PingTaskResponse>;
using TEvFqPrivateGetTaskRequest =
    TGrpcRequestOperationCall<Fq::Private::GetTaskRequest, Fq::Private::GetTaskResponse>;
using TEvFqPrivateWriteTaskResultRequest =
    TGrpcRequestOperationCall<Fq::Private::WriteTaskResultRequest, Fq::Private::WriteTaskResultResponse>;
using TEvFqPrivateNodesHealthCheckRequest =
    TGrpcRequestOperationCall<Fq::Private::NodesHealthCheckRequest, Fq::Private::NodesHealthCheckResponse>;
using TEvFqPrivateCreateRateLimiterResourceRequest =
    TGrpcRequestOperationCall<Fq::Private::CreateRateLimiterResourceRequest, Fq::Private::CreateRateLimiterResourceResponse>;
using TEvFqPrivateDeleteRateLimiterResourceRequest =
    TGrpcRequestOperationCall<Fq::Private::DeleteRateLimiterResourceRequest, Fq::Private::DeleteRateLimiterResourceResponse>;

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType>
class TFqPrivateRequestRPC : public TRpcOperationRequestActor<
    TFqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType>, RpcRequestType> {

    using TBase = TRpcOperationRequestActor<
        TFqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType>,
        RpcRequestType>;

public:
    TFqPrivateRequestRPC(IRequestOpCtx* request) : TBase(request) {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        const auto req = this->GetProtoRequest();
        auto ev = MakeHolder<EvRequestType>();
        auto request = dynamic_cast<RpcRequestType*>(this->Request_.get());
        Y_ABORT_UNLESS(request);
        auto proxyCtx = dynamic_cast<IRequestProxyCtx*>(request);
        Y_ABORT_UNLESS(proxyCtx);
        TString user;
        const TString& internalToken = proxyCtx->GetSerializedToken();
        if (internalToken) {
            NACLib::TUserToken userToken(internalToken);
            user = userToken.GetUserSID();
        }
        ev->Record = *req;
        ev->User = user;
        this->Send(NFq::MakeYqPrivateProxyId(), ev.Release());
        this->Become(&TFqPrivateRequestRPC<RpcRequestType, EvRequestType, EvResponseType>::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        HFunc(EvResponseType, Handle);
    )

    void Handle(typename EvResponseType::TPtr& ev, const TActorContext& ctx) {
        SendResponse(ev, *this->Request_);
        this->Die(ctx);
    }

    template <typename TEv, typename TReq>
    void SendResponse(const TEv& ev, TReq& req) {
        if (!ev->Get()->Record) {
            req.RaiseIssues(ev->Get()->Issues);
            req.ReplyWithYdbStatus(ev->Get()->Status);
        } else {
            req.SendResult(*ev->Get()->Record, ev->Get()->Status);
        }
    }
};

using TFqPrivatePingTaskRPC = TFqPrivateRequestRPC<
    TEvFqPrivatePingTaskRequest,
    NFq::TEvents::TEvPingTaskRequest,
    NFq::TEvents::TEvPingTaskResponse>;

using TFqPrivateGetTaskRPC = TFqPrivateRequestRPC<
    TEvFqPrivateGetTaskRequest,
    NFq::TEvents::TEvGetTaskRequest,
    NFq::TEvents::TEvGetTaskResponse>;

using TFqPrivateWriteTaskResultRPC = TFqPrivateRequestRPC<
    TEvFqPrivateWriteTaskResultRequest,
    NFq::TEvents::TEvWriteTaskResultRequest,
    NFq::TEvents::TEvWriteTaskResultResponse>;

using TFqPrivateNodesHealthCheckRPC = TFqPrivateRequestRPC<
    TEvFqPrivateNodesHealthCheckRequest,
    NFq::TEvents::TEvNodesHealthCheckRequest,
    NFq::TEvents::TEvNodesHealthCheckResponse>;

using TFqPrivateCreateRateLimiterResourceRPC = TFqPrivateRequestRPC<
    TEvFqPrivateCreateRateLimiterResourceRequest,
    NFq::TEvents::TEvCreateRateLimiterResourceRequest,
    NFq::TEvents::TEvCreateRateLimiterResourceResponse>;

using TFqPrivateDeleteRateLimiterResourceRPC = TFqPrivateRequestRPC<
    TEvFqPrivateDeleteRateLimiterResourceRequest,
    NFq::TEvents::TEvDeleteRateLimiterResourceRequest,
    NFq::TEvents::TEvDeleteRateLimiterResourceResponse>;

void DoFqPrivatePingTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivatePingTaskRPC(p.release()));
}

void DoFqPrivateGetTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateGetTaskRPC(p.release()));
}

void DoFqPrivateWriteTaskResultRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateWriteTaskResultRPC(p.release()));
}

void DoFqPrivateNodesHealthCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateNodesHealthCheckRPC(p.release()));
}

void DoFqPrivateCreateRateLimiterResourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateCreateRateLimiterResourceRPC(p.release()));
}

void DoFqPrivateDeleteRateLimiterResourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateDeleteRateLimiterResourceRPC(p.release()));
}

namespace {

bool IsTrustedFqSchemaCaller(const IRequestOpCtx& request) {
    const auto* proxy = dynamic_cast<const IRequestProxyCtx*>(&request);
    const auto* token = request.GetInternalToken().Get();
    return proxy && proxy->GetAuthState().State == NYdbGrpc::TAuthState::AS_OK && token
        && !AppData()->AdministrationAllowedSIDs.empty() && IsAdministrator(AppData(), token);
}

TString ResolveFqSchemaPath(const IRequestOpCtx& request, const TString& relativePath) {
    const auto& database = request.GetDatabaseName();
    if (!database || relativePath.Contains('/') || IsPathPartContainsOnlyDots(relativePath)) {
        return {};
    }
    return CanonizePath(ChildPath(NKikimr::SplitPath(*database), relativePath));
}

using TMutationRequest = Fq::Private::MutateExternalTableRequest;
using TMutationResponse = Fq::Private::MutateExternalTableResponse;
using TEvFqPrivateMutateExternalTableRequest = TGrpcRequestOperationCall<TMutationRequest, TMutationResponse>;

class TFqPrivateMutateExternalTableRPC
    : public TRpcSchemeRequestActor<TFqPrivateMutateExternalTableRPC, TEvFqPrivateMutateExternalTableRequest>
{
    using TBase = TRpcSchemeRequestActor<TFqPrivateMutateExternalTableRPC, TEvFqPrivateMutateExternalTableRequest>;
    using TMutation = NKikimrSchemeOp::TExternalTableMutation;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        if (!IsTrustedFqSchemaCaller(*Request_)) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }

        const auto& request = *GetProtoRequest();
        const bool scoped = request.has_leased_create() || request.has_release() || request.has_leased_abort() || request.has_leased_commit() ||
                            request.has_leased_guarded_drop();
        if (scoped != request.has_expected_scope()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        if (scoped) {
            const auto& scope = request.expected_scope();
            if (scope.principal().empty() || !scope.scheme_shard_id() || !scope.domain_owner_id() || !scope.domain_path_id()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
            }
            if (scope.principal() != Request_->GetInternalToken()->GetUserSID()) {
                return Reply(Ydb::StatusIds::PRECONDITION_FAILED, "The authenticated principal differs from the expected FQ mutation scope",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
        }

        const auto path = ResolveFqSchemaPath(*Request_, request.relative_path());
        if (path.empty()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Expected a path relative to the compute database",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }
        auto proposal = CreateProposeTransaction();
        auto& modifyScheme = *proposal->Record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpExternalTableMutation);
        auto& mutation = *modifyScheme.MutableExternalTableMutation();
        mutation.SetWorkingDir(TString(ExtractParent(path)));
        mutation.SetName(TString(ExtractBase(path)));
        if (scoped) {
            const auto& scope = request.expected_scope();
            mutation.SetExpectedSchemeShardId(scope.scheme_shard_id());
            auto& domain = *mutation.MutableExpectedDomainPathId();
            domain.SetOwnerId(scope.domain_owner_id());
            domain.SetLocalId(scope.domain_path_id());
        }

        switch (request.action_case()) {
            case TMutationRequest::kCreate:
            case TMutationRequest::kLeasedCreate: {
                const auto& create = scoped ? request.leased_create() : request.create();
                const auto dataSourcePath = ResolveFqSchemaPath(*Request_, create.relative_data_source_path());
                if (create.key().empty() || dataSourcePath.empty()) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
                }

                mutation.SetAction(scoped ? TMutation::CREATE_LEASED : TMutation::CREATE);
                mutation.SetKey(create.key());
                auto& definition = *mutation.MutableDefinition();
                definition.SetSourceType("General");
                definition.SetDataSourcePath(dataSourcePath);
                definition.SetLocation(create.location());
                for (const auto& column : create.columns()) {
                    auto& result = *definition.AddColumns();
                    result.SetName(column.name());
                    result.SetType(column.type());
                    result.SetNotNull(column.not_null());
                }

                NKikimrExternalSources::TGeneral content;
                content.set_location(create.location());
                *content.mutable_attributes() = create.attributes();
                definition.SetContent(content.SerializeAsString());
                break;
            }
            case TMutationRequest::kAbort:
                mutation.SetAction(TMutation::ABORT);
                mutation.SetKey(request.abort().key());
                break;
            case TMutationRequest::kCommit:
                mutation.SetAction(TMutation::COMMIT);
                mutation.SetKey(request.commit().key());
                break;
            case TMutationRequest::kLeasedAbort:
                mutation.SetAction(TMutation::ABORT_LEASED);
                mutation.SetKey(request.leased_abort().key());
                break;
            case TMutationRequest::kLeasedCommit:
                mutation.SetAction(TMutation::COMMIT_LEASED);
                mutation.SetKey(request.leased_commit().key());
                break;
            case TMutationRequest::kRelease:
                mutation.SetAction(TMutation::RELEASE);
                mutation.SetKey(request.release().key());
                break;
            case TMutationRequest::kGuardedDrop:
            case TMutationRequest::kLeasedGuardedDrop: {
                const auto& identity = scoped ? request.leased_guarded_drop() : request.guarded_drop();
                if (!identity.owner_id() || !identity.local_path_id() || !identity.version()) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
                }

                mutation.SetAction(scoped ? TMutation::GUARDED_DROP_LEASED : TMutation::GUARDED_DROP);
                auto& expected = *mutation.MutableExpectedTable();
                expected.MutablePathId()->SetOwnerId(identity.owner_id());
                expected.MutablePathId()->SetLocalId(identity.local_path_id());
                expected.SetVersion(identity.version());
                break;
            }
            case TMutationRequest::ACTION_NOT_SET:
                return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        if (mutation.HasKey() && mutation.GetKey().empty()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        ctx.Send(MakeTxProxyID(), proposal.release());
        Become(&TFqPrivateMutateExternalTableRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleProposeStatus);
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleCreatedTable);
            default:
                return TBase::StateWork(ev);
        }
    }

    void HandleProposeStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (record.GetSchemeShardStatus() == NKikimrScheme::StatusAccessDenied) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, record.GetIssues(), ctx);
        }
        using ETxStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;
        if ((GetProtoRequest()->has_guarded_drop() || GetProtoRequest()->has_leased_guarded_drop()) &&
            ev->Get()->Status() == ETxStatus::ExecError && record.GetSchemeShardStatus() == NKikimrScheme::StatusPathDoesNotExist)
        {
            return Reply(Ydb::StatusIds::NOT_FOUND, record.GetIssues(), ctx);
        }
        const auto status = YdbStatusFromProxyStatus(ev->Get());
        if (status == Ydb::StatusIds::SUCCESS && record.GetSchemeShardStatus() == NKikimrScheme::StatusAlreadyExists) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, record.GetIssues(), ctx);
        }
        if ((GetProtoRequest()->has_create() || GetProtoRequest()->has_leased_create()) &&
            (status == Ydb::StatusIds::SUCCESS || IsTxProxyInProgress(status)))
        {
            if (!record.GetSchemeShardTabletId() || !record.GetPathId() || !record.GetExternalTableVersion()) {
                return Reply(IsTxProxyInProgress(status) ? Ydb::StatusIds::UNAVAILABLE : Ydb::StatusIds::INTERNAL_ERROR, ctx);
            }

            auto& identity = *Result_.mutable_created_table();
            identity.set_owner_id(record.GetSchemeShardTabletId());
            identity.set_local_path_id(record.GetPathId());
            identity.set_version(record.GetExternalTableVersion());
            if (status == Ydb::StatusIds::SUCCESS) {
                return ReplyWithResult(status, record.GetIssues(), Result_, ctx);
            }
        }
        TBase::Handle(ev, ctx);
    }

    void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&, const TActorContext& ctx) override {
        if (GetProtoRequest()->has_create() || GetProtoRequest()->has_leased_create()) {
            const auto& identity = Result_.created_table();
            return ForwardToSchemeShard(ctx,
                std::make_unique<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(identity.owner_id(), identity.local_path_id()));
        }
        Reply(Ydb::StatusIds::SUCCESS, ctx);
    }

    void HandleCreatedTable(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        if (record.HasReason()) {
            Request_->RaiseIssue(NYql::TIssue(record.GetReason()));
        }
        switch (record.GetStatus()) {
            case NKikimrScheme::StatusPathDoesNotExist:
                return Reply(Ydb::StatusIds::PRECONDITION_FAILED, ctx);
            case NKikimrScheme::StatusNotAvailable:
            case NKikimrScheme::StatusRedirectDomain:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            case NKikimrScheme::StatusAccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            case NKikimrScheme::StatusSuccess:
                break;
            default:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }

        const auto& path = record.GetPathDescription();
        const auto& self = path.GetSelf();
        const auto& table = path.GetExternalTableDescription();
        const auto& id = table.GetPathId();
        const auto& identity = Result_.created_table();
        if (self.GetPathType() != NKikimrSchemeOp::EPathTypeExternalTable || !self.GetCreateFinished()
            || self.GetPathState() == NKikimrSchemeOp::EPathStateDrop || id.GetOwnerId() != identity.owner_id()
            || id.GetLocalId() != identity.local_path_id() || table.GetVersion() != identity.version())
        {
            return Reply(Ydb::StatusIds::PRECONDITION_FAILED, "The external table created by the ticket is unavailable or has changed",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }
        ReplyWithResult(Ydb::StatusIds::SUCCESS, Result_, ctx);
    }

    Fq::Private::MutateExternalTableResult Result_;
};

using TEvFqPrivateDescribeExternalTableRequest = TGrpcRequestOperationCall<
    Fq::Private::DescribeExternalTableRequest, Fq::Private::DescribeExternalTableResponse>;

class TFqPrivateDescribeExternalTableRPC
    : public TRpcSchemeRequestActor<TFqPrivateDescribeExternalTableRPC, TEvFqPrivateDescribeExternalTableRequest>
{
    using TBase = TRpcSchemeRequestActor<TFqPrivateDescribeExternalTableRPC, TEvFqPrivateDescribeExternalTableRequest>;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        if (!IsTrustedFqSchemaCaller(*Request_)) {
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }
        const auto& request = *GetProtoRequest();
        if ((request.resolve_mutation_scope() && (!request.relative_path().empty() || request.has_expected_scope())) ||
            (request.has_expected_scope() && !request.relative_path().empty()) ||
            (!request.has_expected_scope() && !request.scoped_relative_path().empty()))
        {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        if (!request.resolve_mutation_scope()) {
            const auto& relativePath = request.has_expected_scope() ? request.scoped_relative_path() : request.relative_path();
            TargetPath_ = ResolveFqSchemaPath(*Request_, relativePath);
            if (TargetPath_.empty()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
            }
        }

        if (request.has_expected_scope()) {
            const auto& scope = request.expected_scope();
            if (scope.principal().empty() || !scope.scheme_shard_id() || !scope.domain_owner_id() || !scope.domain_path_id()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
            }
            if (scope.principal() != Request_->GetInternalToken()->GetUserSID()) {
                return Reply(Ydb::StatusIds::PRECONDITION_FAILED, "The authenticated principal differs from the expected FQ mutation scope",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
        }

        if (request.resolve_mutation_scope() || request.has_expected_scope()) {
            const auto& database = Request_->GetDatabaseName();
            if (!database || database->empty()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
            }
            Stage_ = EStage::Scope;
            Navigate(*database, ctx);
        } else {
            Navigate(TargetPath_, ctx);
        }
        Become(&TFqPrivateDescribeExternalTableRPC::StateWork);
    }

private:
    enum class EStage {
        Scope,
        Target,
        ConfirmAbsence,
    };

    void Navigate(const TString& path, const TActorContext& ctx) {
        auto navigate = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(navigate, *Request_);
        SetDatabase(navigate.get(), *Request_);
        navigate->Record.MutableDescribePath()->SetPath(path);
        ctx.Send(MakeTxProxyID(), navigate.release());
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleDescribe);
            default:
                return TBase::StateWork(ev);
        }
    }

    void HandleDescribe(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto& request = *GetProtoRequest();
        if (record.GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
            if (Stage_ == EStage::Scope) {
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }
            if (request.has_expected_scope() && Stage_ == EStage::Target) {
                // TxProxy can synthesize an absent path before contacting the tablet.
                Stage_ = EStage::ConfirmAbsence;
                SetSchemeShardId(request.expected_scope().scheme_shard_id());
                return ForwardToSchemeShard(ctx, std::make_unique<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(TargetPath_));
            }
            if (Stage_ == EStage::ConfirmAbsence) {
                const auto& prefix = record.GetLastExistedPrefixDescription().GetSelf();
                if (!IsEqualPaths(record.GetLastExistedPrefixPath(), *Request_->GetDatabaseName()) ||
                    prefix.GetSchemeshardId() != ScopeRootId_.OwnerId || prefix.GetPathId() != ScopeRootId_.LocalPathId)
                {
                    return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
                }
            }
            if (record.HasReason()) {
                Request_->RaiseIssue(NYql::TIssue(record.GetReason()));
            }
            return Reply(Ydb::StatusIds::NOT_FOUND, ctx);
        }
        if (record.HasReason()) {
            Request_->RaiseIssue(NYql::TIssue(record.GetReason()));
        }
        switch (record.GetStatus()) {
            case NKikimrScheme::StatusAccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            case NKikimrScheme::StatusNotAvailable:
            case NKikimrScheme::StatusRedirectDomain:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            case NKikimrScheme::StatusSchemeError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            case NKikimrScheme::StatusSuccess:
                break;
            default:
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
        }

        if (Stage_ == EStage::ConfirmAbsence) {
            return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
        }

        const auto& path = record.GetPathDescription();
        const auto& self = path.GetSelf();
        const auto& domain = path.GetDomainDescription();
        const auto& domainKey = domain.GetDomainKey();
        if (Stage_ == EStage::Scope) {
            const bool globalRoot = self.GetPathType() == NKikimrSchemeOp::EPathTypeDir && self.GetPathId() == self.GetParentPathId() &&
                                    self.GetSchemeshardId() == domainKey.GetSchemeShard() && self.GetPathId() == domainKey.GetPathId();
            if (!globalRoot && self.GetPathType() != NKikimrSchemeOp::EPathTypeSubDomain &&
                self.GetPathType() != NKikimrSchemeOp::EPathTypeExtSubDomain)
            {
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            const auto& params = domain.GetProcessingParams();
            const auto schemeShardId = params.HasSchemeShard() ? params.GetSchemeShard() : domainKey.GetSchemeShard();
            if (!schemeShardId || !domainKey.GetSchemeShard() || !domainKey.GetPathId() || !self.GetCreateFinished()) {
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }
            if (request.has_expected_scope()) {
                const auto& scope = request.expected_scope();
                if (schemeShardId != scope.scheme_shard_id() || domainKey.GetSchemeShard() != scope.domain_owner_id() ||
                    domainKey.GetPathId() != scope.domain_path_id())
                {
                    return Reply(Ydb::StatusIds::PRECONDITION_FAILED, ctx);
                }
                ScopeRootId_ = TPathId(self.GetSchemeshardId(), self.GetPathId());
                Stage_ = EStage::Target;
                return Navigate(TargetPath_, ctx);
            }

            Fq::Private::DescribeExternalTableResult result;
            auto& scope = *result.mutable_mutation_scope();
            scope.set_principal(Request_->GetInternalToken()->GetUserSID());
            scope.set_scheme_shard_id(schemeShardId);
            scope.set_domain_owner_id(domainKey.GetSchemeShard());
            scope.set_domain_path_id(domainKey.GetPathId());
            return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
        if (self.GetPathType() != NKikimrSchemeOp::EPathTypeExternalTable) {
            return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }
        if (!self.GetCreateFinished()) {
            return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
        }
        if (self.GetOwner() != Request_->GetInternalToken()->GetUserSID()) {
            return Reply(Ydb::StatusIds::PRECONDITION_FAILED, "External table is owned by a different principal",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        const auto& table = path.GetExternalTableDescription();
        const auto& pathId = table.GetPathId();
        if (!pathId.GetOwnerId() || !pathId.GetLocalId() || !table.GetVersion()) {
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }
        if (request.has_expected_scope()) {
            const auto& scope = request.expected_scope();
            if (pathId.GetOwnerId() != scope.scheme_shard_id() || domainKey.GetSchemeShard() != scope.domain_owner_id() ||
                domainKey.GetPathId() != scope.domain_path_id())
            {
                return Reply(Ydb::StatusIds::PRECONDITION_FAILED, ctx);
            }
        }

        Fq::Private::DescribeExternalTableResult result;
        auto& identity = *result.mutable_identity();
        identity.set_owner_id(pathId.GetOwnerId());
        identity.set_local_path_id(pathId.GetLocalId());
        identity.set_version(table.GetVersion());

        TString error;
        Ydb::StatusIds::StatusCode status;
        if (!FillExternalTableDescription(*result.mutable_description(), table, self, status, error)) {
            return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }
        return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    EStage Stage_ = EStage::Target;
    TString TargetPath_;
    TPathId ScopeRootId_;
};

} // namespace

void DoFqPrivateMutateExternalTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateMutateExternalTableRPC(p.release()));
}

void DoFqPrivateDescribeExternalTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateDescribeExternalTableRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
