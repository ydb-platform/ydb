#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_etcd.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include "ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NGRpcService {

template <typename TDerived>
class TEtcdResponseSenderImpl : public IRequestOpCtx {
public:
    // IRequestOpCtx
    //
    void SendOperation(const Ydb::Operations::Operation& operation) override {
        auto self = Derived();
        if (operation.ready()) {
            self->FinishRequest();
        }
        auto resp = self->CreateResponseMessage();
        self->Reply(resp, operation.status());
    }

    void SendResult(const google::protobuf::Message& result,
        Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message) override
    {
        auto self = Derived();
        self->FinishRequest();
        auto resp = self->CreateResponseMessage();
/*        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        deferred->mutable_issues()->MergeFrom(message);
        if (self->CostInfo) {
            deferred->mutable_cost_info()->Swap(self->CostInfo);
        }
        auto data = deferred->mutable_result();
        data->PackFrom(result);*/
        self->Reply(resp, status);
    }

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        auto self = Derived();
        self->FinishRequest();
        auto resp = self->CreateResponseMessage();
/*        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        if (self->CostInfo) {
            deferred->mutable_cost_info()->Swap(self->CostInfo);
        }
        NYql::IssuesToMessage(self->IssueManager.GetIssues(), deferred->mutable_issues());
        auto data = deferred->mutable_result();
        data->PackFrom(result); */
        self->Reply(resp, status);
    }

private:
    TDerived* Derived() noexcept {
        return static_cast<TDerived*>(this);
    }
};

template <ui32 TRpcId, typename TReq, typename TResp, bool IsOperation, typename TDerived>
class TEtcdRequestWrapperImpl
    : public std::conditional_t<IsOperation,
        TEtcdResponseSenderImpl<TEtcdRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation, TDerived>>,
        IRequestNoOpCtx>
    , public std::conditional_t<TRpcId == TRpcServices::EvGrpcRuntimeRequest,
        TEvProxyRuntimeEvent,
        TEvProxyLegacyEvent<TRpcId, TDerived>>
{
    friend class TProtoResponseHelper;
    friend class TEtcdResponseSenderImpl<TEtcdRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation, TDerived>>;

public:
    using TRequest = TReq;
    using TResponse = TResp;

    using TFinishWrapper = std::function<void(const NYdbGrpc::IRequestContextBase::TAsyncFinishResult&)>;

    TEtcdRequestWrapperImpl(NYdbGrpc::IRequestContextBase* ctx)
        : Ctx_(ctx)
        , TraceId(GetPeerMetaValues(NYdb::YDB_TRACE_ID_HEADER))
    {
        if (!TraceId) {
            TraceId = UlidGen.Next().ToString();
        }
    }

    const TMaybe<TString> GetYdbToken() const override {
        return ExtractYdbToken(Ctx_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER));
    }

    bool HasClientCapability(const TString& capability) const override {
        return FindPtr(Ctx_->GetPeerMetaValues(NYdb::YDB_CLIENT_CAPABILITIES), capability);
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return ExtractDatabaseName(Ctx_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER));
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        auto& s = Ctx_->GetAuthState();
        s.State = state;
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

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        InternalToken_ = token;
    }

    void AddServerHint(const TString& hint) override {
        Ctx_->AddTrailingMetadata(NYdb::YDB_SERVER_HINTS, hint);
    }

    void SetRuHeader(ui64 ru) override {
        Ru = ru;
        Ctx_->AddTrailingMetadata(NYdb::YDB_CONSUMED_UNITS_HEADER, IntToString<10>(ru));
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return InternalToken_;
    }

    const TString& GetSerializedToken() const override {
        if (InternalToken_) {
            return InternalToken_->GetSerializedToken();
        }

        return EmptySerializedTokenMessage_;
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

    void SetDiskQuotaExceeded(bool disk) override {
        if (!QuotaExceeded) {
            QuotaExceeded = google::protobuf::Arena::CreateMessage<Ydb::QuotaExceeded>(GetArena());
        }
        QuotaExceeded->set_disk(disk);
    }

    bool GetDiskQuotaExceeded() const override {
        return QuotaExceeded ? QuotaExceeded->disk() : false;
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
        TResponse* resp = CreateResponseMessage();
//        TCommonResponseFiller<TResponse, TDerived::IsOp>::Fill(*resp, IssueManager.GetIssues(), CostInfo, status);
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
        return TraceId;
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return Span_.GetTraceId();
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
        // res->data() pointer is used inside grpc code.
        // So this object should be destroyed during grpc_slice destroying routine
        auto res = new TString;
        res->swap(in);

        static auto freeResult = [](void* p) -> void {
            TString* toDelete = reinterpret_cast<TString*>(p);
            delete toDelete;
        };

        grpc_slice slice = grpc_slice_new_with_user_data(
                    (void*)(res->data()), res->size(), freeResult, res);
        grpc::Slice sl = grpc::Slice(slice, grpc::Slice::STEAL_REF);
        auto data = grpc::ByteBuffer(&sl, 1);
        Ctx_->Reply(&data, status, flag);
    }

    void SetCostInfo(float consumed_units) override {
        CostInfo = google::protobuf::Arena::CreateMessage<Ydb::CostInfo>(GetArena());
        CostInfo->set_consumed_units(consumed_units);
    }

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

    void FinishStream(ui32 status) override {
        // End Of Request for streaming requests
        AuditLogRequestEnd(status);
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

    void SetRespHook(TRespHook&& hook) override {
        RespHook = std::move(hook);
    }

    void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) override {
        RlPath = std::move(path);
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return RlPath;
    }

    void Pass(const IFacilityProvider&) override {
        Y_ABORT("unimplemented");
    }

    void SetAuditLogHook(TAuditLogHook&& hook) override {
        AuditLogHook = std::move(hook);
    }

    // IRequestCtx
    //
    void FinishRequest() override {
        RequestFinished = true;
    }

    // IRequestCtxBase
    //
    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        AuditLogParts.emplace_back(name, value);
    }
    const TAuditLogParts& GetAuditLogParts() const override {
        return AuditLogParts;
    }

    void StartTracing(NWilson::TSpan&& span) override {
        Span_ = std::move(span);
    }

    void FinishSpan() override {
        Span_.End();
    }

    bool* IsTracingDecided() override {
        return &IsTracingDecided_;
    }

    void ReplyGrpcError(grpc::StatusCode code, const TString& msg, const TString& details = "") {
        Ctx_->ReplyError(code, msg, details);
    }

    TString GetEndpointId() const {
        return Ctx_->GetEndpointId();
    }

private:
    void Reply(NProtoBuf::Message *resp, ui32 status) override {
        // End Of Request for non streaming requests
        if (RequestFinished) {
            AuditLogRequestEnd(status);
        }
        if (RespHook) {
            TRespHook hook = std::move(RespHook);
            return hook(MakeIntrusive<TRespHookCtx>(Ctx_, resp, GetRequestName(), Ru, status));
        }
        return Ctx_->Reply(resp, status);
    }

    void AuditLogRequestEnd(ui32 status) {
        if (AuditLogHook) {
            AuditLogHook(status, GetAuditLogParts());
            // Drop hook to avoid double logging in case when operation implemention
            // invokes both FinishRequest() (indirectly) and FinishStream()
            AuditLogHook = nullptr;
        }
    }

    TResponse* CreateResponseMessage() {
        return google::protobuf::Arena::CreateMessage<TResponse>(Ctx_->GetArena());
    }

    static TFinishWrapper GetStdFinishWrapper(std::function<void()>&& cb) {
        return [cb = std::move(cb)](const NYdbGrpc::IRequestContextBase::TAsyncFinishResult& future) mutable {
            Y_ASSERT(future.HasValue());
            if (future.GetValue() == NYdbGrpc::IRequestContextBase::EFinishStatus::CANCEL) {
                cb();
            }
        };
    }

protected:
    NWilson::TSpan Span_;
private:
    TIntrusivePtr<NYdbGrpc::IRequestContextBase> Ctx_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    inline static const TString EmptySerializedTokenMessage_;
    NYql::TIssueManager IssueManager;
    Ydb::CostInfo* CostInfo = nullptr;
    Ydb::QuotaExceeded* QuotaExceeded = nullptr;
    ui64 Ru = 0;
    TRespHook RespHook;
    TMaybe<NRpcService::TRlPath> RlPath;
    IGRpcProxyCounters::TPtr Counters;
    std::function<TFinishWrapper(std::function<void()>&&)> FinishWrapper = &GetStdFinishWrapper;

    TAuditLogParts AuditLogParts;
    TAuditLogHook AuditLogHook;
    bool RequestFinished = false;
    bool IsTracingDecided_ = false;
    TULIDGenerator UlidGen;
    TMaybe<TString> TraceId;
};

template <typename TReq, typename TResp, bool IsOperation>
class TEtcdRequestCall
    : public std::conditional_t<TProtoHasValidate<TReq>::Value,
        TGRpcRequestValidationWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TEtcdRequestCall<TReq, TResp, IsOperation>>,
        TEtcdRequestWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TEtcdRequestCall<TReq, TResp, IsOperation>>>
{
    using TRequestIface = typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type;

public:
    template<typename TOptionalArg>
    static IActor* CreateRpcActor(TRequestIface* msg, TOptionalArg arg);

    static IActor* CreateRpcActor(TRequestIface* msg);

    static constexpr bool IsOp = IsOperation;

    using TBase = std::conditional_t<TProtoHasValidate<TReq>::Value,
        TGRpcRequestValidationWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TEtcdRequestCall<TReq, TResp, IsOperation>>,
        TEtcdRequestWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TEtcdRequestCall<TReq, TResp, IsOperation>>>;

    template <typename TCallback>
    TEtcdRequestCall(NYdbGrpc::IRequestContextBase* ctx, TCallback&& cb, TRequestAuxSettings auxSettings = {})
        : TBase(ctx)
        , PassMethod(std::forward<TCallback>(cb))
        , AuxSettings(std::move(auxSettings))
    { }

    void Pass(const IFacilityProvider& facility) override {
        try {
            PassMethod(std::move(std::unique_ptr<TRequestIface>(this)), facility);
        } catch (const std::exception& ex) {
            this->RaiseIssue(NYql::TIssue{TStringBuilder() << "unexpected exception: " << ex.what()});
        }
    }

    TRateLimiterMode GetRlMode() const override {
        return AuxSettings.RlMode;
    }

    bool TryCustomAttributeProcess(const NKikimrScheme::TEvDescribeSchemeResult& schemeData,
        ICheckerIface* iface) override
    {
        if (!AuxSettings.CustomAttributeProcessor) {
            return false;
        } else {
            AuxSettings.CustomAttributeProcessor(schemeData, iface);
            return true;
        }
    }

    NJaegerTracing::TRequestDiscriminator GetRequestDiscriminator() const override {
        return {
            .RequestType = AuxSettings.RequestType,
            .Database = TBase::GetDatabaseName(),
        };
    }

    // IRequestCtxBaseMtSafe
    //
    bool IsAuditable() const override {
        return (AuxSettings.AuditMode == TAuditMode::Auditable) && !this->IsInternalCall();
    }

private:
    std::function<void(std::unique_ptr<TRequestIface>, const IFacilityProvider&)> PassMethod;
    const TRequestAuxSettings AuxSettings;
};

template <typename TReq, typename TResp>
using TEtcdRequestOperationCall = TEtcdRequestCall<TReq, TResp, true>;


TEtcdGRpcService::TEtcdGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TEtcdGRpcService::~TEtcdGRpcService() = default;

void TEtcdGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TEtcdGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

#define SETUP_METHOD_RAW(methodName, method, rlMode, requestType, serviceType, serviceName, counterName)    \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                              \
        Ydb::serviceType::Y_CAT(methodName, Request),                                                       \
        Ydb::serviceType::Y_CAT(methodName, Response),                                                       \
        T##serviceType##GRpcService>>                                                                         \
    (                                                                                                          \
        this,                                                                                                   \
        &Service_,                                                                                              \
        CQ,                                                                                                     \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                                      \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                                \
            ActorSystem->Send(GRpcRequestProxyId, new TEtcdRequestOperationCall<                              \
                Ydb::serviceType::Y_CAT(methodName, Request),                                                \
                Ydb::serviceType::Y_CAT(methodName, Response)>(reqCtx, &method,                             \
                    TRequestAuxSettings {                                                                    \
                        .RlMode = TRateLimiterMode::rlMode,                                                 \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                         \
                    }));                                                                                      \
        },                                                                                                      \
        &Ydb::serviceType::V1::serviceName::AsyncService::Y_CAT(Request, methodName),          \
        Y_STRINGIZE(serviceType) "/" Y_STRINGIZE(methodName),                                               \
        logger,                                                                                                 \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                                  \
    )->Run()

    #define SETUP_ETCD_KV_METHOD(methodName, method, requestType) \
        SETUP_METHOD_RAW( \
            methodName, \
            method, \
            Rps, \
            requestType, \
            Etcd, \
            KV, \
            etcd \
        )
    SETUP_ETCD_KV_METHOD(Range, DoRange, ETCD_RANGE);
    SETUP_ETCD_KV_METHOD(Put, DoPut, ETCD_PUT);
    SETUP_ETCD_KV_METHOD(DeleteRange, DoDeleteRange, ETCD_DELETE_RANGE);

    #undef SETUP_ETCD_KV_METHOD
    #undef SETUP_METHOD_RAW
}

} // namespace NKikimr::NGRpcService
