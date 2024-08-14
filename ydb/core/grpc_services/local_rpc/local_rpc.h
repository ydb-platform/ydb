#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_grpc/local_grpc.h>

#include <ydb/core/base/appdata.h>

#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace NRpcService {

template<typename TResponse>
class TPromiseWrapper {
public:
    TPromiseWrapper(NThreading::TPromise<TResponse> promise)
        : Promise(promise)
    {}

    void operator()(const TResponse& resp) {
        Promise.SetValue(resp);
    }

private:
    NThreading::TPromise<TResponse> Promise;
};

template<typename TRpc, typename TCbWrapper>
class TLocalRpcCtxImplData {
protected:
    TCbWrapper CbWrapper;
    NYql::TIssueManager IssueManager;
    std::unique_ptr<Ydb::CostInfo> CostInfo;

    template<typename TCb>
    TLocalRpcCtxImplData(TCb&& cb)
        : CbWrapper(std::forward<TCb>(cb))
    {}
};

template<typename TRpc, typename TCbWrapper, bool IsOperation>
class TLocalRpcCtxImpl;

template<typename TRpc, typename TCbWrapper>
class TLocalRpcCtxImpl<TRpc, TCbWrapper, false> : public NGRpcService::IRequestNoOpCtx, public TLocalRpcCtxImplData<TRpc, TCbWrapper> {
protected:
    using TBase = TLocalRpcCtxImplData<TRpc, TCbWrapper>;

    template<typename TCb>
    TLocalRpcCtxImpl(TCb&& cb)
        : TBase(std::forward<TCb>(cb))
    {}
};

template<typename TRpc, typename TCbWrapper>
class TLocalRpcCtxImpl<TRpc, TCbWrapper, true> : public NGRpcService::IRequestOpCtx, public TLocalRpcCtxImplData<TRpc, TCbWrapper> {
protected:
    using TBase = TLocalRpcCtxImplData<TRpc, TCbWrapper>;

    template<typename TCb>
    TLocalRpcCtxImpl(TCb&& cb)
        : TBase(std::forward<TCb>(cb))
    {}

public:
    using TResp = typename TRpc::TResponse;

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        TResp resp;
        auto deferred = resp.mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        if (TBase::CostInfo) {
            deferred->mutable_cost_info()->CopyFrom(*TBase::CostInfo);
        }
        NYql::IssuesToMessage(TBase::IssueManager.GetIssues(), deferred->mutable_issues());
        auto data = deferred->mutable_result();
        data->PackFrom(result);
        TBase::CbWrapper(resp);
    }

    void SendResult(const google::protobuf::Message& result,
        Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<NGRpcService::TYdbIssueMessageType>& message) override {
        TResp resp;
        auto deferred = resp.mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        deferred->mutable_issues()->MergeFrom(message);
        if (TBase::CostInfo) {
            deferred->mutable_cost_info()->CopyFrom(*TBase::CostInfo);
        }
        auto data = deferred->mutable_result();
        data->PackFrom(result);
        TBase::CbWrapper(resp);
    }

    void SendOperation(const Ydb::Operations::Operation& operation) override {
        TResp resp;
        resp.mutable_operation()->CopyFrom(operation);
        TBase::CbWrapper(resp);
    }
};

template<typename TRpc, typename TCbWrapper, bool IsOperation = TRpc::IsOp>
class TLocalRpcCtx : public TLocalRpcCtxImpl<TRpc, TCbWrapper, IsOperation> {
public:
    static constexpr bool IsOp = IsOperation;
    using TBase = TLocalRpcCtxImpl<TRpc, TCbWrapper, IsOperation>;
    using TResp = typename TRpc::TResponse;
    using EStreamCtrl = NYdbGrpc::IRequestContextBase::EStreamCtrl;

    template<typename TProto, typename TCb>
    TLocalRpcCtx(TProto&& req, TCb&& cb,
            const TString& databaseName,
            const TMaybe<TString>& token,
            const TMaybe<TString>& requestType,
            bool internalCall)
        : TBase(std::forward<TCb>(cb))
        , Request(std::forward<TProto>(req))
        , DatabaseName(databaseName)
        , RequestType(requestType)
        , InternalCall(internalCall)
    {
        if (token && !token->empty()) {
            InternalToken = new NACLib::TUserToken(*token);
        }
    }

    bool HasClientCapability(const TString&) const override {
        return false;
    }

    const TMaybe<TString> GetDatabaseName() const override {
        if (DatabaseName.empty())
            return Nothing();
        return DatabaseName;
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return InternalToken;
    }

    const TString& GetSerializedToken() const override {
        if (InternalToken) {
            return InternalToken->GetSerializedToken();
        }
        return EmptySerializedTokenMessage_;
    }

    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        if (key == NYdb::YDB_DATABASE_HEADER) {
            return GetDatabaseName();
        }
        auto valueIt = PeerMeta.find(key);
        return valueIt == PeerMeta.end() ? Nothing() : TMaybe<TString>(valueIt->second);
    }

    void PutPeerMeta(const TString& key, const TString& value) {
        PeerMeta.insert_or_assign(key, value);
    }

    TVector<TStringBuf> FindClientCert() const override {
        Y_ABORT("Unimplemented");
        return {};
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TResp resp;
        NGRpcService::TCommonResponseFiller<TResp, IsOp>::Fill(resp, TBase::IssueManager.GetIssues(), TBase::CostInfo.get(), status);
        TBase::CbWrapper(resp);
    }

    TString GetPeerName() const override {
        return "localhost";
    }

    const TString& GetRequestName() const override {
        return TRpc::TRequest::descriptor()->name();
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        TBase::IssueManager.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        TBase::IssueManager.RaiseIssues(issues);
    }

    google::protobuf::Arena* GetArena() override {
        return &Arena;
    }

    const google::protobuf::Message* GetRequest() const override {
        return &Request;
    }

    google::protobuf::Message* GetRequestMut() override {
        return &Request;
    }

    void SetFinishAction(std::function<void()>&&) override {}

    bool IsClientLost() const override { return false; }

    void AddServerHint(const TString&) override {}

    void SetRuHeader(ui64) override {}

    // Unimplemented methods
    void ReplyWithRpcStatus(grpc::StatusCode, const TString&, const TString&) override {
        ReplyWithYdbStatus(Ydb::StatusIds::GENERIC_ERROR);
    }

    void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&&) override {
        Y_ABORT("Unimplemented for local rpc");
    }

    void FinishStream(ui32) override {
        Y_ABORT("Unimplemented for local rpc");
    }

    virtual void SendSerializedResult(TString&&, Ydb::StatusIds::StatusCode, EStreamCtrl) override {
        Y_ABORT("Unimplemented for local rpc");
    }

    TMaybe<TString> GetTraceId() const override {
        return Nothing();
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    TInstant GetDeadline() const override {
        return TInstant::Max();
    }

    const TMaybe<TString> GetRequestType() const override {
        return RequestType;
    }

    void SetCostInfo(float consumed_units) override {
        TBase::CostInfo = std::make_unique<Ydb::CostInfo>();
        TBase::CostInfo->set_consumed_units(consumed_units);
    }

    bool GetDiskQuotaExceeded() const override {
        return false;
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return Nothing();
    }

    bool IsInternalCall() const override {
        return InternalCall;
    }

    // IRequestCtx
    //
    void FinishRequest() override {}

    // IRequestCtxBase
    //
    void AddAuditLogPart(const TStringBuf&, const TString&) override {}
    const NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        Y_ABORT("unimplemented for local rpc");
    }

private:
    void Reply(NProtoBuf::Message *r, ui32) override {
        TResp* resp = dynamic_cast<TResp*>(r);
        Y_ABORT_UNLESS(resp);
        TBase::CbWrapper(*resp);
    }

private:
    typename TRpc::TRequest Request;
    const TString DatabaseName;
    const TMaybe<TString> RequestType;
    const bool InternalCall;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken;
    const TString EmptySerializedTokenMessage_;
    TMap<TString, TString> PeerMeta;
    google::protobuf::Arena Arena;
};

template<class TRequest>
concept TRequestWithOperationParams = requires(TRequest& request) {
    { request.mutable_operation_params() } -> std::convertible_to<Ydb::Operations::OperationParams*>;
};

template<TRequestWithOperationParams TRequest>
void SetRequestSyncOperationMode(TRequest& request) {
    request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
}

template<class TRequest>
void SetRequestSyncOperationMode(TRequest&) {
    // nothing
}

template<typename TRpc>
NThreading::TFuture<typename TRpc::TResponse> DoLocalRpc(typename TRpc::TRequest&& proto, const TString& database,
        const TMaybe<TString>& token, const TMaybe<TString>& requestType,
        TActorSystem* actorSystem, bool internalCall = false)
{
    auto promise = NThreading::NewPromise<typename TRpc::TResponse>();

    SetRequestSyncOperationMode(proto);

    using TCbWrapper = TPromiseWrapper<typename TRpc::TResponse>;
    auto req = new TLocalRpcCtx<TRpc, TCbWrapper>(std::move(proto), TCbWrapper(promise), database, token, requestType, internalCall);
    auto actor = TRpc::CreateRpcActor(req);
    actorSystem->Register(actor, TMailboxType::HTSwap, actorSystem->AppData<TAppData>()->UserPoolId);

    return promise.GetFuture();
}

template<typename TRpc>
NThreading::TFuture<typename TRpc::TResponse> DoLocalRpc(typename TRpc::TRequest&& proto, const TString& database, const TMaybe<TString>& token, TActorSystem* actorSystem, bool internalCall = false) {
    return DoLocalRpc<TRpc>(std::move(proto), database, token, Nothing(), actorSystem, internalCall);
}

template<typename TRpc>
NThreading::TFuture<typename TRpc::TResponse> DoLocalRpc(
        typename TRpc::TRequest&& proto,
        const TString& database,
        const TMaybe<TString>& token,
        const TMaybe<TString>& requestType,
        TActorSystem* actorSystem,
        const TMap<TString, TString>& peerMeta,
        bool internalCall = false
)
{
    auto promise = NThreading::NewPromise<typename TRpc::TResponse>();

    SetRequestSyncOperationMode(proto);

    using TCbWrapper = TPromiseWrapper<typename TRpc::TResponse>;
    auto req = new TLocalRpcCtx<TRpc, TCbWrapper>(
        std::move(proto),
        TCbWrapper(promise),
        database,
        token,
        requestType,
        internalCall
    );

    for (const auto& [key, value] : peerMeta) {
        req->PutPeerMeta(key, value);
    }

    auto actor = TRpc::CreateRpcActor(req);
    actorSystem->Register(actor, TMailboxType::HTSwap, actorSystem->AppData<TAppData>()->UserPoolId);

    return promise.GetFuture();
}

template<typename TRpc>
TActorId DoLocalRpcSameMailbox(typename TRpc::TRequest&& proto, std::function<void(typename TRpc::TResponse)>&& cb,
        const TString& database, const TMaybe<TString>& token, const TMaybe<TString>& requestType,
        const TActorContext& ctx, bool internalCall = false)
{
    SetRequestSyncOperationMode(proto);

    auto req = new TLocalRpcCtx<TRpc, std::function<void(typename TRpc::TResponse)>>(std::move(proto), std::move(cb), database, token, requestType, internalCall);
    auto actor = TRpc::CreateRpcActor(req);
    return ctx.RegisterWithSameMailbox(actor);
}

template<typename TRpc>
TActorId DoLocalRpcSameMailbox(typename TRpc::TRequest&& proto, std::function<void(typename TRpc::TResponse)>&& cb, const TString& database, const TMaybe<TString>& token, const TActorContext& ctx, bool internalCall = false) {
    return DoLocalRpcSameMailbox<TRpc>(std::move(proto), std::move(cb), database, token, Nothing(), ctx, internalCall);
}

//// Streaming part

template <typename TResponsePart>
class TStreamReadProcessor : public NGRpcService::NLocalGrpc::TContextBase {
    using TBase = NGRpcService::NLocalGrpc::TContextBase;
    using TOnResponseCallback = std::function<void(TResponsePart&&)>;

public:
    TStreamReadProcessor(std::shared_ptr<NGRpcService::IRequestCtx> baseRequest)
        : TBase(std::move(baseRequest))
    {}

    void Read(TOnResponseCallback callback) {
        if (!ResponseQueue.empty()) {
            callback(DoPopResponse());
            return;
        }

        Y_ABORT_UNLESS(!Finished, "Try to read from finished stream");
        Y_ABORT_UNLESS(!OnResponseCallback, "Can not multiply read from stream");
        OnResponseCallback = callback;
    }

    void Cancel() {
        FinishPromise.SetValue(EFinishStatus::CANCEL);
        Finished = true;
    }

    bool IsFinished() const {
        return Finished;
    }

    bool HasData() const {
        return !Finished || !ResponseQueue.empty();
    }

protected:
    const NProtoBuf::Message* GetRequest() const override {
        return GetBaseRequest().GetRequest();
    }

    NProtoBuf::Message* GetRequestMut() override {
        return GetBaseRequest().GetRequestMut();
    }

    TAsyncFinishResult GetFinishFuture() override {
        return FinishPromise.GetFuture();
    }

    bool IsStreamCall() const override {
        return true;
    }

    bool IsClientLost() const override {
        return FinishPromise.HasValue();
    }

    void SetNextReplyCallback(TOnNextReply&& callback) override {
        NextReplyCallback = callback;
    }

    void FinishStreamingOk() override {
        ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TResponsePart response;
        NGRpcService::TCommonResponseFiller<TResponsePart, false>::Fill(response, GetIssues(), nullptr, status);
        DoPushResponse(std::move(response), EStreamCtrl::FINISH);

        if (status != Ydb::StatusIds::SUCCESS) {
            FinishPromise.SetValue(EFinishStatus::ERROR);
        }
    }

    void Reply(NProtoBuf::Message* proto, ui32 status = 0) override {
        Y_UNUSED(proto, status);
        Y_ABORT("Expected TLocalGrpcContext::Reply only for stream");
    }

    void Reply(grpc::ByteBuffer* bytes, ui32 status = 0, EStreamCtrl ctrl = EStreamCtrl::CONT) override {
        Y_UNUSED(status);

        grpc::Slice slice;
        if (auto status = bytes->TrySingleSlice(&slice); !status.ok()) {
            ReplyError(status.error_code(), status.error_message(), status.error_details());
            return;
        }

        TResponsePart response;
        if (!response.ParseFromArray(slice.begin(), slice.size())) {
            RaiseIssue(NYql::TIssue("Response part is corrupted"));
            ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
            return;
        }
        DoPushResponse(std::move(response), ctrl);
    }

private:
    TResponsePart DoPopResponse() {
        Y_ABORT_UNLESS(!ResponseQueue.empty(), "Try to pop response from empty queue");

        auto response = std::move(ResponseQueue.front());
        ResponseQueue.pop();
        if (NextReplyCallback && !Finished) {
            NextReplyCallback(ResponseQueue.size());
        }
        return response;
    }

    void DoPushResponse(TResponsePart&& response, EStreamCtrl ctrl) {
        if (Finished) {
            return;
        }
        Finished = ctrl == EStreamCtrl::FINISH;

        ResponseQueue.emplace(std::move(response));
        if (OnResponseCallback) {
            OnResponseCallback(DoPopResponse());
            OnResponseCallback = nullptr;
        }
    }

private:
    bool Finished = false;
    std::queue<TResponsePart> ResponseQueue;
    NThreading::TPromise<EFinishStatus> FinishPromise = NThreading::NewPromise<EFinishStatus>();

    TOnNextReply NextReplyCallback;
    TOnResponseCallback OnResponseCallback;
};

template <typename TResponsePart>
using TStreamReadProcessorPtr = TIntrusivePtr<TStreamReadProcessor<TResponsePart>>;

using TFacilityProviderPtr = std::shared_ptr<NGRpcService::IFacilityProvider>;
TFacilityProviderPtr CreateFacilityProviderSameMailbox(TActorContext actorContext, ui64 channelBufferSize);

using TRpcActorCreator = std::function<void((std::unique_ptr<NGRpcService::IRequestNoOpCtx> p, const NGRpcService::IFacilityProvider& f))>;

template <typename TRpc>
TStreamReadProcessorPtr<typename TRpc::TResponse> DoLocalRpcStreamSameMailbox(typename TRpc::TRequest&& proto, const TString& database, const TMaybe<TString>& token, const TMaybe<TString>& requestType, TFacilityProviderPtr facilityProvider, TRpcActorCreator actorCreator, bool internalCall = false) {
    using TCbWrapper = std::function<void(const typename TRpc::TResponse&)>;
    using TLocalRpcStreamCtx = TStreamReadProcessor<typename TRpc::TResponse>;

    auto localRpcCtx = std::make_shared<TLocalRpcCtx<TRpc, TCbWrapper>>(std::move(proto), [](const typename TRpc::TResponse&) {}, database, token, requestType, internalCall);
    auto localRpcStreamCtx = MakeIntrusive<TLocalRpcStreamCtx>(std::move(localRpcCtx));
    auto localRpcRequest = std::make_unique<TRpc>(localRpcStreamCtx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});
    actorCreator(std::move(localRpcRequest), *facilityProvider);

    return localRpcStreamCtx;
}

template <typename TRpc>
TStreamReadProcessorPtr<typename TRpc::TResponse> DoLocalRpcStreamSameMailbox(typename TRpc::TRequest&& proto, const TString& database, const TMaybe<TString>& token, TFacilityProviderPtr facilityProvider, TRpcActorCreator actorCreator, bool internalCall = false) {
    return DoLocalRpcStreamSameMailbox<TRpc>(std::move(proto), database, token, Nothing(), std::move(facilityProvider), std::move(actorCreator), internalCall);
}

} // namespace NRpcService
} // namespace NKikimr
