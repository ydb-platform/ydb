#include "service_etcd.h"

#include <ydb/public/api/protos/etcd/rpc.pb.h>

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>


namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvRangeKVRequest =
    TGrpcRequestOperationCall<Ydb::Etcd::RangeRequest,
        Ydb::Etcd::RangeResponse>;

using TEvPutKVRequest =
    TGrpcRequestOperationCall<Ydb::Etcd::PutRequest,
        Ydb::Etcd::PutResponse>;

using TEvDeleteRangeKVRequest =
    TGrpcRequestOperationCall<Ydb::Etcd::DeleteRangeRequest,
        Ydb::Etcd::DeleteRangeResponse>;

} // namespace NKikimr::NGRpcService


namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

#define COPY_PRIMITIVE_FIELD(name) \
    to->set_ ## name(static_cast<decltype(to->name())>(from.name())) \
// COPY_PRIMITIVE_FIELD

#define COPY_PRIMITIVE_OPTIONAL_FIELD(name) \
    if (from.has_ ## name()) { \
        to->set_ ## name(static_cast<decltype(to->name())>(from.name())); \
    } \
// COPY_PRIMITIVE_FIELD

namespace {

void CopyProtobuf(const Ydb::Etcd::RangeRequest &from, NKikimrKeyValue::ReadRangeRequest *to) {
    to->mutable_range()->set_from_key_inclusive(from.key());
    to->mutable_range()->set_to_key_exclusive(from.range_end());
    to->set_include_data(!(from.keys_only() || from.count_only()));
}

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult::KeyValuePair &from, mvccpb::KeyValue *to) {
    to->set_key(from.key());
    to->set_value(from.value());
}

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult &from, Ydb::Etcd::RangeResponse *to) {
    to->set_count(from.pair().size());
    for (const auto &pair : from.pair()) {
        CopyProtobuf(pair, to->add_kvs());
    }
}

void CopyProtobuf(const Ydb::Etcd::PutRequest &from, NKikimrKeyValue::ExecuteTransactionRequest *to) {
    const auto cmd = to->add_commands()->mutable_write();
    cmd->set_key(from.key());
    cmd->set_value(from.value());
}

void CopyProtobuf(const NKikimrKeyValue::ExecuteTransactionResult &from, Ydb::Etcd::PutResponse *to) {

}

void CopyProtobuf(const Ydb::Etcd::DeleteRangeRequest &from, NKikimrKeyValue::ExecuteTransactionRequest *to) {
    const auto cmd = to->add_commands()->mutable_delete_range();
    cmd->mutable_range()->set_from_key_inclusive(from.key());
    cmd->mutable_range()->set_to_key_exclusive(from.range_end());
}

void CopyProtobuf(const NKikimrKeyValue::ExecuteTransactionResult &from, Ydb::Etcd::DeleteRangeResponse *to) {

}

template <typename TResult>
Ydb::StatusIds::StatusCode PullStatus(const TResult &result) {
    switch (result.status()) {
    case NKikimrKeyValue::Statuses::RSTATUS_OK:
    case NKikimrKeyValue::Statuses::RSTATUS_OVERRUN:
        return Ydb::StatusIds::SUCCESS;
    case NKikimrKeyValue::Statuses::RSTATUS_ERROR:
        return Ydb::StatusIds::GENERIC_ERROR;
    case NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT:
        return Ydb::StatusIds::TIMEOUT;
    case NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND:
        return Ydb::StatusIds::NOT_FOUND;
    case NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION:
        return Ydb::StatusIds::PRECONDITION_FAILED;
    default:
        return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

template <typename TDerived>
class TBaseEtcdRequest {
protected:
    void OnBootstrap() {
        auto self = static_cast<TDerived*>(this);
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;
        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }
        if (const auto& userToken = self->Request_->GetSerializedToken()) {
            UserToken = new NACLib::TUserToken(userToken);
        }
        SendNavigateRequest();
    }

    void SendNavigateRequest() {
        auto self = static_cast<TDerived*>(this);
        auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = req->ResultSet.emplace_back();
        entry.Path = ::NKikimr::SplitPath("/Root/mydb/kvtable");
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.SyncVersion = false;
        req->UserToken = UserToken;
        req->DatabaseName = self->Request_->GetDatabaseName().GetOrElse("");
        auto ev = new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release());
        self->Send(MakeSchemeCacheID(), ev, 0, 0, self->Span_.GetTraceId());
    }

    bool OnNavigateKeySetResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, ui32 access) {
        auto self = static_cast<TDerived*>(this);
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        auto ctx = self->ActorContext();

        if (res->Request->ResultSet.size() != 1) {
            self->Reply(StatusIds::INTERNAL_ERROR, "Received an incorrect answer from SchemeCache.", NKikimrIssues::TIssuesIds::UNEXPECTED, ctx);
            return false;
        }

        switch (request->ResultSet[0].Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
            self->Reply(StatusIds::UNAUTHORIZED, "Access denied.", NKikimrIssues::TIssuesIds::ACCESS_DENIED, ctx);
            return false;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            self->Reply(StatusIds::SCHEME_ERROR, "Path isn't exist.", NKikimrIssues::TIssuesIds::PATH_NOT_EXIST, ctx);
            return false;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            self->Reply(StatusIds::UNAVAILABLE, "Database resolve failed with no certain result.", NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR, ctx);
            return false;
        default:
            self->Reply(StatusIds::UNAVAILABLE, "Resolve error", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, ctx);
            return false;
        }

        if (!self->CheckAccess(CanonizePath(res->Request->ResultSet[0].Path), res->Request->ResultSet[0].SecurityObject, access)) {
            return false;
        }
        if (!request->ResultSet[0].SolomonVolumeInfo) {
            self->Reply(StatusIds::SCHEME_ERROR, "Table isn't keyvalue.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return false;
        }

        return true;
    }

    bool CheckAccess(const TString& path, TIntrusivePtr<TSecurityObject> securityObject, ui32 access) {
        auto self = static_cast<TDerived*>(this);
        if (!UserToken || !securityObject) {
            return true;
        }

        if (securityObject->CheckAccess(access, *UserToken)) {
            return true;
        }

        self->Reply(Ydb::StatusIds::UNAUTHORIZED,
            TStringBuilder() << "Access denied"
                << ": for# " << UserToken->GetUserSID()
                << ", path# " << path
                << ", access# " << NACLib::AccessRightsToString(access),
            NKikimrIssues::TIssuesIds::ACCESS_DENIED,
            self->ActorContext());
        return false;
    }

private:
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

template <typename TDerived, typename TRequest, bool IsOperation>
class TEtcdRequestWithOperationParamsActor : public TActorBootstrapped<TDerived> {
private:
    typedef TActorBootstrapped<TDerived> TBase;
    typedef typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type TRequestBase;

    template<typename TIn, typename TOut>
    void Fill(const TIn* in, TOut* out) {/* TODO
        auto& operationParams = in->operation_params();
        out->OperationTimeout_ = GetDuration(operationParams.operation_timeout());
        out->CancelAfter_ = GetDuration(operationParams.cancel_after());
        out->ReportCostInfo_ = operationParams.report_cost_info() == Ydb::FeatureFlag::ENABLED; */
    }

    template<typename TOut>
    void Fill(const NProtoBuf::Message*, TOut*) {
    }

public:
    enum EWakeupTag {
        WakeupTagTimeout = 10,
        WakeupTagCancel = 11,
        WakeupTagGetConfig = 21,
        WakeupTagClientLost = 22,
    };

public:
    TEtcdRequestWithOperationParamsActor(TRequestBase* request)
        : Request_(request)
    {
        Fill(GetProtoRequest(), this);
    }

    const typename TRequest::TRequest* GetProtoRequest() const {
        return TRequest::GetProtoRequest(Request_);
    }

    Ydb::Operations::OperationParams::OperationMode GetOperationMode() const {
        return GetProtoRequest()->operation_params().operation_mode();
    }

    void Bootstrap(const TActorContext &ctx) {
        HasCancel_ = static_cast<TDerived*>(this)->HasCancelOperation();

        if (OperationTimeout_) {
            OperationTimeoutTimer = CreateLongTimer(ctx, OperationTimeout_,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(WakeupTagTimeout)),
                AppData(ctx)->UserPoolId);
        }

        if (HasCancel_ && CancelAfter_) {
            CancelAfterTimer = CreateLongTimer(ctx, CancelAfter_,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(WakeupTagCancel)),
                AppData(ctx)->UserPoolId);
        }

        auto selfId = ctx.SelfID;
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto clientLostCb = [selfId, actorSystem]() {
            actorSystem->Send(selfId, new TRpcServices::TEvForgetOperation());
        };

        Request_->SetFinishAction(std::move(clientLostCb));
    }

    bool HasCancelOperation() {
        return false;
    }

    TRequestBase& Request() const {
        return *Request_;
    }

protected:
    TDuration GetOperationTimeout() {
        return OperationTimeout_;
    }

    TDuration GetCancelAfter() {
        return CancelAfter_;
    }

    void DestroyTimers() {
        auto& ctx = TlsActivationContext->AsActorContext();
        if (OperationTimeoutTimer) {
            ctx.Send(OperationTimeoutTimer, new TEvents::TEvPoisonPill);
        }
        if (CancelAfterTimer) {
            ctx.Send(CancelAfterTimer, new TEvents::TEvPoisonPill);
        }
    }

    void PassAway() override {
        DestroyTimers();
        TBase::PassAway();
    }

    TRequest* RequestPtr() {
        return static_cast<TRequest*>(Request_.get());
    }

protected:
    std::shared_ptr<TRequestBase> Request_;

    TActorId OperationTimeoutTimer;
    TActorId CancelAfterTimer;
    TDuration OperationTimeout_;
    TDuration CancelAfter_;
    bool HasCancel_ = false;
    bool ReportCostInfo_ = false;
};

template <typename TDerived, typename TRequest>
class TEtcdOperationRequestActor : public TEtcdRequestWithOperationParamsActor<TDerived, TRequest, true> {
private:
    typedef TEtcdRequestWithOperationParamsActor<TDerived, TRequest, true> TBase;

public:

    TEtcdOperationRequestActor(IRequestOpCtx* request)
        : TBase(request)
        , Span_(TWilsonGrpc::RequestActor, request->GetWilsonTraceId(),
                "RequestProxy.RpcOperationRequestActor", NWilson::EFlags::AUTO_END)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DEFERRABLE_RPC;
    }

    void OnCancelOperation(const TActorContext& ctx) {
        Y_UNUSED(ctx);
    }

    void OnForgetOperation(const TActorContext& ctx) {
        // No client is waiting for the reply, but we have to issue fake reply
        // anyway before dying to make Grpc happy.
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Closing Grpc request, client should not see this message."));
        Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
    }

    void OnOperationTimeout(const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Operation timeout."));
        Reply(Ydb::StatusIds::TIMEOUT, issues, ctx);
    }

protected:
    void StateFuncBase(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TRpcServices::TEvForgetOperation, HandleForget);
            hFunc(TEvSubscribeGrpcCancel, HandleSubscribeiGrpcCancel);
            default: {
                NYql::TIssues issues;
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unexpected event received in TEtcdOperationRequestActor::StateWork: "
                        << ev->GetTypeRewrite()));
                return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, TActivationContext::AsActorContext());
            }
        }
    }

protected:
    using TBase::Request_;

    void Reply(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message, const TActorContext& ctx)
    {
        NYql::TIssues issues;
        IssuesFromMessage(message, issues);
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, const TActorContext& ctx) {
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message, NKikimrIssues::TIssuesIds::EIssueCode issueCode, const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(issueCode, message));
        Reply(status, issues, ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, typename TRequest::TResponse& resp, const TActorContext& ctx) {
        Request_->Reply(&resp);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    template<typename TResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message,
        const TResult& result,
        const TActorContext& ctx)
    {
        Request_->SendResult(result, status, message);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    template<typename TResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status,
                         const TResult& result,
                         const TActorContext& ctx) {
        Request_->SendResult(result, status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void ReplyOperation(Ydb::Operations::Operation& operation)
    {
        Request_->SendOperation(operation);
        NWilson::EndSpanWithStatus(Span_, operation.status());
        this->PassAway();
    }

    void SetCost(ui64 ru) {
        Request_->SetRuHeader(ru);
        if (TBase::ReportCostInfo_) {
            Request_->SetCostInfo(ru);
        }
    }

protected:
    void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->Tag) {
            case TBase::WakeupTagTimeout:
                static_cast<TDerived*>(this)->OnOperationTimeout(ctx);
                break;
            case TBase::WakeupTagCancel:
                static_cast<TDerived*>(this)->OnCancelOperation(ctx);
                break;
            default:
                break;
        }
    }

    void HandleForget(TRpcServices::TEvForgetOperation::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        static_cast<TDerived*>(this)->OnForgetOperation(ctx);
    }
private:
    void HandleSubscribeiGrpcCancel(TEvSubscribeGrpcCancel::TPtr& ev) {
        auto as = TActivationContext::ActorSystem();
        PassSubscription(ev->Get(), Request_.get(), as);
    }

protected:
    NWilson::TSpan Span_;
};

template <typename TDerived, typename TRequest, typename TKVRequest>
class TEtcdRequestGrpc
    : public TEtcdOperationRequestActor<TDerived, TRequest>
    , public TBaseEtcdRequest<TEtcdRequestGrpc<TDerived, TRequest, TKVRequest>>
{
public:
    using TBase = TEtcdOperationRequestActor<TDerived, TRequest>;
    using TBase::TBase;

    template<typename T, typename = void>
    struct THasMsg: std::false_type
    {};
    template<typename T>
    struct THasMsg<T, std::enable_if_t<std::is_same<decltype(std::declval<T>().msg()), void>::value>>: std::true_type
    {};
    template<typename T>
    static constexpr bool HasMsgV = THasMsg<T>::value;

    friend class TBaseEtcdRequest<TEtcdRequestGrpc<TDerived, TRequest, TKVRequest>>;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        this->OnBootstrap();
        this->Become(&TEtcdRequestGrpc::StateFunc);
    }


protected:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TKVRequest::TResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        if (!this->OnNavigateKeySetResult(ev, static_cast<TDerived*>(this)->GetRequiredAccessRights())) {
            return;
        }

        const NKikimrSchemeOp::TSolomonVolumeDescription &desc = request->ResultSet[0].SolomonVolumeInfo->Description;
        ui64 partitionId = 0ULL; //rec.partition_id();

        if (const auto &partition = desc.GetPartitions(partitionId); partition.GetPartitionId() == partitionId) {
            KVTabletId = partition.GetTabletId();
        } else {
            Y_DEBUG_ABORT_UNLESS(false);
            for (const NKikimrSchemeOp::TSolomonVolumeDescription::TPartition &partition : desc.GetPartitions()) {
                if (partition.GetPartitionId() == partitionId)  {
                    KVTabletId = partition.GetTabletId();
                    break;
                }
            }
        }

        if (!KVTabletId) {
            this->Reply(StatusIds::INTERNAL_ERROR, "Partition wasn't found.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            return;
        }

        CreatePipe();
        SendRequest();
    }

    void SendRequest() {
        std::unique_ptr<TKVRequest> req = std::make_unique<TKVRequest>();
        auto &rec = *this->GetProtoRequest();
        CopyProtobuf(rec, &req->Record);
        req->Record.set_tablet_id(KVTabletId);
        NTabletPipe::SendData(this->SelfId(), KVPipeClient, req.release(), 0, TBase::Span_.GetTraceId());
    }

    void Handle(typename TKVRequest::TResponse::TPtr &ev) {
        auto status = PullStatus(ev->Get()->Record);
        if constexpr (HasMsgV<decltype(ev->Get()->Record)>) {
            if (status != Ydb::StatusIds::SUCCESS) {
                this->Reply(status, ev->Get()->Record.msg(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            }
        }
        typename TRequest::TResponse resp;
        CopyProtobuf(ev->Get()->Record, &resp);
        this->Reply(status, resp, TActivationContext::AsActorContext());
    }

    NTabletPipe::TClientConfig GetPipeConfig() {
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        return cfg;
    }

    void CreatePipe() {
        KVPipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), KVTabletId, GetPipeConfig()));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            this->Reply(StatusIds::UNAVAILABLE, "Failed to connect to coordination node.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(StatusIds::UNAVAILABLE, "Connection to coordination node was lost.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
    }

    virtual bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) = 0;

    void PassAway() override {
        if (KVPipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), KVPipeClient);
            KVPipeClient = {};
        }
        TBase::PassAway();
    }

protected:
    ui64 KVTabletId = 0;
    TActorId KVPipeClient;
};

class TRangeRequest
    : public TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest,
            TEvKeyValue::TEvReadRange> {
public:
    using TBase = TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest,
           TEvKeyValue::TEvReadRange>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode&, NYql::TIssues&) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

class TPutRequest
    : public TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest, TEvKeyValue::TEvExecuteTransaction> {
public:
    using TBase = TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest, TEvKeyValue::TEvExecuteTransaction>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode&, NYql::TIssues&) override {
        return true;
    }

    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::UpdateRow;
    }
};

class TDeleteRangeRequest
    : public TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest, TEvKeyValue::TEvExecuteTransaction> {
public:
    using TBase = TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest, TEvKeyValue::TEvExecuteTransaction>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode&, NYql::TIssues&) override {
        return true;
    }

    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::EraseRow;
    }
};

}

void DoRange(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    Cerr << __func__ << Endl;
    TActivationContext::AsActorContext().Register(new TRangeRequest(p.release()));
}

void DoPut(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    Cerr << __func__ << Endl;
    TActivationContext::AsActorContext().Register(new TPutRequest(p.release()));
}

void DoDeleteRange(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    Cerr << __func__ << Endl;
    TActivationContext::AsActorContext().Register(new TDeleteRangeRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
