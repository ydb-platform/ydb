#include "etcd_grpc.h"
#include "etcd_impl.h"
#include "etcd_watch.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NEtcd {

using namespace NKikimr::NGRpcService;

namespace {

template <ui32 TRpcId, typename TReq, typename TRes, typename TDerived>
class TEtcdRequestWrapperImpl
    : public IRequestCtx
    , public TEvProxyRuntimeEvent
{
friend class ::NKikimr::NGRpcService::TProtoResponseHelper;
public:
    using TRequest = TReq;
    using TResonse = TRes;

    using TFinishWrapper = std::function<void(const NYdbGrpc::IRequestContextBase::TAsyncFinishResult&)>;

    TEtcdRequestWrapperImpl(NYdbGrpc::IRequestContextBase* ctx)
        : Ctx_(ctx)
    {}
private:
    const TMaybe<TString> GetYdbToken() const override {
        return ExtractYdbToken(Ctx_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER));
    }

    bool HasClientCapability(const TString& capability) const override {
        return FindPtr(Ctx_->GetPeerMetaValues(NYdb::YDB_CLIENT_CAPABILITIES), capability);
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return ExtractDatabaseName(Ctx_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER));
    }

    TString GetRpcMethodName() const override {
        return Ctx_->GetRpcMethodName();
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        Ctx_->GetAuthState().State = state;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return Ctx_->GetAuthState();
    }

    void ReplyWithRpcStatus(grpc::StatusCode code, const TString& reason, const TString& details) override {
        Ctx_->ReplyError(code, reason, details);
    }

    void ReplyUnauthenticated(const TString& in) override {
        Ctx_->ReplyUnauthenticated(MakeAuthError(in, IssueManager));
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>&) override {
        Y_ABORT("unimplemented");
    }

    void AddServerHint(const TString& hint) override {
        Ctx_->AddTrailingMetadata(NYdb::YDB_SERVER_HINTS, hint);
    }

    void SetRuHeader(ui64 ru) override {
        Ctx_->AddTrailingMetadata(NYdb::YDB_CONSUMED_UNITS_HEADER, IntToString<10>(ru));
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        static const TIntrusiveConstPtr<NACLib::TUserToken> stub;
        return stub;
    }

    const TString& GetSerializedToken() const override {
        static const TString stub;
        return stub;
    }

    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        return ToMaybe(Ctx_->GetPeerMetaValues(key));
    }

    TVector<TStringBuf> FindClientCert() const override {
        return Ctx_->FindClientCert();
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return Ctx_->FindClientCert();
    }

    void SetDiskQuotaExceeded(bool) override {}

    bool GetDiskQuotaExceeded() const override {
        return false;
    }

    bool Validate(TString&) override {
        return true;
    }

    void SetCounters(IGRpcProxyCounters::TPtr counters) override {
        Counters = counters;
    }

    IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters;
    }

    void UseDatabase(const TString& database) override {
        Ctx_->UseDatabase(database);
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TResonse* resp = CreateResponseMessage();
        FinishRequest();
        Reply(resp, status);
        if (Ctx_->IsStreamCall()) {
            Ctx_->FinishStreamingOk();
        }
    }

    TString GetPeerName() const override {
        return Ctx_->GetPeer();
    }

    bool SslServer() const {
        return Ctx_->SslServer();
    }

    template <typename T>
    static const TRequest* GetProtoRequest(const T& req) {
        auto request = dynamic_cast<const TRequest*>(req->GetRequest());
        Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper");
        return request;
    }

    const TRequest* GetProtoRequest() const {
        return GetProtoRequest(this);
    }

    TMaybe<TString> GetTraceId() const override {
        return {};
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    const TMaybe<TString> GetSdkBuildInfo() const {
        return GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    }

    TInstant GetDeadline() const override {
        return Ctx_->Deadline();
    }

    const TMaybe<TString> GetRequestType() const override {
        return GetPeerMetaValues(NYdb::YDB_REQUEST_TYPE_HEADER);
    }

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, IRequestCtx::EStreamCtrl flag = IRequestCtx::EStreamCtrl::CONT) override {
        auto data = MakeByteBufferFromSerializedResult(std::move(in));
        Ctx_->Reply(&data, status, flag);
    }

    void SendSerializedResult(TRope&& in, Ydb::StatusIds::StatusCode status, IRequestCtx::EStreamCtrl flag = IRequestCtx::EStreamCtrl::CONT) override {
        auto data = MakeByteBufferFromSerializedResult(std::move(in));
        Ctx_->Reply(&data, status, flag);
    }

    void SetCostInfo(float) override {}

    const TString& GetRequestName() const override {
        return TRequest::descriptor()->name();
    }

    google::protobuf::Arena* GetArena() override {
        return Ctx_->GetArena();
    }

    //! Allocate Result message using protobuf arena allocator
    //! The memory will be freed automaticaly after destroying
    //! corresponding request.
    //! Do not call delete for objects allocated here!
    template <typename TResult, typename T>
    static TResult* AllocateResult(T& ctx) {
        return google::protobuf::Arena::CreateMessage<TResult>(ctx->GetArena());
    }

    void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
        Ctx_->SetNextReplyCallback(std::move(cb));
    }

    void SetFinishAction(std::function<void()>&& cb) override {
        auto shutdown = FinishWrapper(std::move(cb));
        Ctx_->GetFinishFuture().Subscribe(std::move(shutdown));
    }

    void SetCustomFinishWrapper(std::function<TFinishWrapper(std::function<void()>&&)> wrapper) {
        FinishWrapper = wrapper;
    }

    bool IsClientLost() const override {
        return Ctx_->IsClientLost();
    }

    void FinishStream(ui32) override {
        // End Of Request for streaming requests
        Ctx_->FinishStreamingOk();
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        IssueManager.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        IssueManager.RaiseIssues(issues);
    }

    const google::protobuf::Message* GetRequest() const override {
        return Ctx_->GetRequest();
    }

    void SetRespHook(TRespHook&&) override {}

    void SetRlPath(TMaybe<NKikimr::NRpcService::TRlPath>&&) override {}

    TMaybe<NKikimr::NRpcService::TRlPath> GetRlPath() const override {
        return {};
    }

    TRateLimiterMode GetRlMode() const override {
        return TRateLimiterMode::Off;
    }

    void Pass(const IFacilityProvider&) override {
        Y_ABORT("unimplemented");
    }

    void SetAuditLogHook(TAuditLogHook&& hook) override {
        AuditLogHook = std::move(hook);
    }

    void FinishRequest() override {}

    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        AuditLogParts.emplace_back(name, value);
    }

    const TAuditLogParts& GetAuditLogParts() const override {
        return AuditLogParts;
    }

    void StartTracing(NWilson::TSpan&& ) override {}

    void FinishSpan() override {}

    bool* IsTracingDecided() override {
        return nullptr;
    }

    void ReplyGrpcError(grpc::StatusCode code, const TString& msg, const TString& details = "") {
        Ctx_->ReplyError(code, msg, details);
    }

    TString GetEndpointId() const {
        return Ctx_->GetEndpointId();
    }

    void Reply(NProtoBuf::Message *resp, ui32 status) override {
        return Ctx_->Reply(resp, status);
    }

    TResonse* CreateResponseMessage() {
        return google::protobuf::Arena::CreateMessage<TResonse>(Ctx_->GetArena());
    }

    static TFinishWrapper GetStdFinishWrapper(std::function<void()>&& cb) {
        return [cb = std::move(cb)](const NYdbGrpc::IRequestContextBase::TAsyncFinishResult& future) mutable {
            Y_ASSERT(future.HasValue());
            if (future.GetValue() == NYdbGrpc::IRequestContextBase::EFinishStatus::CANCEL) {
                cb();
            }
        };
    }

    bool TryCustomAttributeProcess(const NKikimrScheme::TEvDescribeSchemeResult&, ICheckerIface*) override {
        Y_ABORT("Unimplemented!");
    }
private:
    TIntrusivePtr<NYdbGrpc::IRequestContextBase> Ctx_;
    NYql::TIssueManager IssueManager;
    IGRpcProxyCounters::TPtr Counters;
    std::function<TFinishWrapper(std::function<void()>&&)> FinishWrapper = &GetStdFinishWrapper;

    TAuditLogParts AuditLogParts;
    TAuditLogHook AuditLogHook;
};

template <typename TReq, typename TRes>
class TEtcdRequestCall : public TEtcdRequestWrapperImpl<Ev::SimpleRequest, TReq, TRes, TEtcdRequestCall<TReq, TRes>>
{
public:
    using TBase = TEtcdRequestWrapperImpl<Ev::SimpleRequest, TReq, TRes, TEtcdRequestCall<TReq, TRes>>;
    using TBase::TBase;
};

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<>
void TEtcdKVService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NKikimr::NGRpcService::CreateCounterCb(Counters, ActorSystem);

#define SETUP_ETCD_KV_METHOD(methodName, secondName)                                  \
    MakeIntrusive<NKikimr::NGRpcService::TGRpcRequest<                                 \
        etcdserverpb::Y_CAT(secondName, Request),                                       \
        etcdserverpb::Y_CAT(secondName, Response),                                       \
        TEtcdKVService>>                                                                  \
    (                                                                                      \
        this, this->GetService(), CQ,                                                       \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                      \
            NKikimr::NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());       \
            ActorSystem->Register(Make##methodName(std::make_unique<TEtcdRequestCall<          \
                etcdserverpb::Y_CAT(secondName, Request),                                       \
                etcdserverpb::Y_CAT(secondName, Response)>>(reqCtx),                             \
            Stuff));                                                                              \
        },                                                                                         \
        &etcdserverpb::KV::AsyncService::Y_CAT(Request, methodName),                                \
        Y_STRINGIZE(methodName),                                                                     \
        logger,                                                                                       \
        getCounterBlock("etcd", Y_STRINGIZE(methodName))                                               \
    )->Run()

    SETUP_ETCD_KV_METHOD(Range,Range);
    SETUP_ETCD_KV_METHOD(Put,Put);
    SETUP_ETCD_KV_METHOD(DeleteRange,DeleteRange);
    SETUP_ETCD_KV_METHOD(Txn,Txn);
    SETUP_ETCD_KV_METHOD(Compact,Compaction);

    #undef SETUP_ETCD_KV_METHOD
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<>
void TEtcdWatchService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr) {
    auto getCounterBlock = NKikimr::NGRpcService::CreateCounterCb(Counters, ActorSystem);

    using TStreamGRpcRequest = NKikimr::NGRpcServer::TGRpcStreamingRequest<
                etcdserverpb::WatchRequest,
                etcdserverpb::WatchResponse,
                TEtcdWatchService,
                NKikimrServices::GRPC_SERVER>;

    TStreamGRpcRequest::Start(this, this->GetService(), CQ, &etcdserverpb::Watch::AsyncService::RequestWatch,
        [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
            ActorSystem->Send(this->ServiceActor, new TEvWatchRequest(context.Release()));
        },
        *ActorSystem, "Watch", getCounterBlock("etcd", "Watch", true), nullptr
    );
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<>
void TEtcdLeaseService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NKikimr::NGRpcService::CreateCounterCb(Counters, ActorSystem);

#define SETUP_ETCD_LEASE_METHOD(methodName)                                           \
    MakeIntrusive<NKikimr::NGRpcService::TGRpcRequest<                                 \
        etcdserverpb::Y_CAT(methodName, Request),                                       \
        etcdserverpb::Y_CAT(methodName, Response),                                       \
        TEtcdLeaseService>>                                                               \
    (                                                                                      \
        this, this->GetService(), CQ,                                                       \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                      \
            NKikimr::NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());       \
            ActorSystem->Register(Make##methodName(std::make_unique<TEtcdRequestCall<          \
                etcdserverpb::Y_CAT(methodName, Request),                                       \
                etcdserverpb::Y_CAT(methodName, Response)>>(reqCtx),                             \
            Stuff));                                                                              \
        },                                                                                         \
        &etcdserverpb::Lease::AsyncService::Y_CAT(Request, methodName),                             \
        Y_STRINGIZE(methodName),                                                                     \
        logger,                                                                                       \
        getCounterBlock("etcd", Y_STRINGIZE(methodName))                                               \
    )->Run()

    SETUP_ETCD_LEASE_METHOD(LeaseGrant);
    SETUP_ETCD_LEASE_METHOD(LeaseRevoke);
    SETUP_ETCD_LEASE_METHOD(LeaseTimeToLive);
    SETUP_ETCD_LEASE_METHOD(LeaseLeases);

    #undef SETUP_ETCD_LEASE_METHOD

    using TStreamGRpcRequest = NKikimr::NGRpcServer::TGRpcStreamingRequest<
                etcdserverpb::LeaseKeepAliveRequest,
                etcdserverpb::LeaseKeepAliveResponse,
                TEtcdLeaseService,
                NKikimrServices::GRPC_SERVER>;

    TStreamGRpcRequest::Start(this, this->GetService(), CQ, &etcdserverpb::Lease::AsyncService::RequestLeaseKeepAlive,
        [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
            ActorSystem->Send(this->ServiceActor, new TEvLeaseKeepAliveRequest(context.Release()));
        },
        *ActorSystem, "Lease/LeaseKeepAlive", getCounterBlock("etcd", "LeaseKeepAlive", true), nullptr
    );
}

} // namespace NEtcd
