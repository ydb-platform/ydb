#pragma once

#include <ydb/core/grpc_services/base/base.h>

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
class TLocalRpcCtx : public NGRpcService::IRequestOpCtx {
public:
    using TResp = typename TRpc::TResponse;
    template<typename TProto, typename TCb>
    TLocalRpcCtx(TProto&& req, TCb&& cb, const TString& databaseName, const TString& token)
        : Request(std::forward<TProto>(req))
        , CbWrapper(std::forward<TCb>(cb))
        , DatabaseName(databaseName)
    {
        InternalToken = new NACLib::TUserToken(token);
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

    const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
        Y_FAIL("Unimplemented");
        return TMaybe<TString>{};
    }

    TVector<TStringBuf> FindClientCert() const override {
        Y_FAIL("Unimplemented");
        return {};
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TResp resp;
        NGRpcService::TCommonResponseFiller<TResp, true>::Fill(resp, IssueManager.GetIssues(), CostInfo.get(), status);
        CbWrapper(resp);
    }

    TString GetPeerName() const override {
        return "localhost";
    }

    const TString& GetRequestName() const override {
        return TRpc::TRequest::descriptor()->name();
    }

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        TResp resp;
        auto deferred = resp.mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        if (CostInfo) {
            deferred->mutable_cost_info()->CopyFrom(*CostInfo);
        }
        NYql::IssuesToMessage(IssueManager.GetIssues(), deferred->mutable_issues());
        auto data = deferred->mutable_result();
        data->PackFrom(result);
        CbWrapper(resp);
    }

    void SendResult(const google::protobuf::Message& result,
        Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<NGRpcService::TYdbIssueMessageType>& message) override
    {
        TResp resp;
        auto deferred = resp.mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        deferred->mutable_issues()->MergeFrom(message);
        if (CostInfo) {
            deferred->mutable_cost_info()->CopyFrom(*CostInfo);
        }
        auto data = deferred->mutable_result();
        data->PackFrom(result);
        CbWrapper(resp);
    }

    void SendResult(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<NGRpcService::TYdbIssueMessageType>& message) override
    {
        TResp resp;
        auto deferred = resp.mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        deferred->mutable_issues()->MergeFrom(message);
        if (CostInfo) {
            deferred->mutable_cost_info()->CopyFrom(*CostInfo);
        }
        CbWrapper(resp);
    }

    void SendOperation(const Ydb::Operations::Operation& operation) override {
        TResp resp;
        resp.mutable_operation()->CopyFrom(operation);
        CbWrapper(resp);
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        IssueManager.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        IssueManager.RaiseIssues(issues);
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

    void SetClientLostAction(std::function<void()>&&) override {}

    bool IsClientLost() const override { return false; }

    void AddServerHint(const TString&) override {}

    void SetRuHeader(ui64) override {}

    // Unimplemented methods
    void ReplyWithRpcStatus(grpc::StatusCode, const TString&, const TString&) override {
        Y_FAIL("Unimplemented for local rpc");
    }

    void SetStreamingNotify(NGrpc::IRequestContextBase::TOnNextReply&&) override {
        Y_FAIL("Unimplemented for local rpc");
    }

    void FinishStream() override {
        Y_FAIL("Unimplemented for local rpc");
    }

    virtual void SendSerializedResult(TString&&, Ydb::StatusIds::StatusCode) override {
        Y_FAIL("Unimplemented for local rpc");
    }

    TMaybe<TString> GetTraceId() const override {
        return Nothing();
    }

    TInstant GetDeadline() const override {
        return TInstant::Max();
    }

    const TMaybe<TString> GetRequestType() const override {
        return Nothing();
    }

    void SetCostInfo(float consumed_units) override {
        CostInfo = std::make_unique<Ydb::CostInfo>();
        CostInfo->set_consumed_units(consumed_units);
    }

    void SetDiskQuotaExceeded(bool disk) override {
        if (!QuotaExceeded) {
            QuotaExceeded = std::make_unique<Ydb::QuotaExceeded>();
        }
        QuotaExceeded->set_disk(disk);
    }

    bool GetDiskQuotaExceeded() const override {
        return QuotaExceeded ? QuotaExceeded->disk() : false;
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return Nothing();
    }

private:
    void Reply(NProtoBuf::Message *r, ui32) override {
        TResp* resp = dynamic_cast<TResp*>(r);
        Y_VERIFY(resp);
        CbWrapper(*resp);
    }

private:
    typename TRpc::TRequest Request;
    TCbWrapper CbWrapper;
    const TString DatabaseName;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken;
    const TString EmptySerializedTokenMessage_;

    NYql::TIssueManager IssueManager;
    google::protobuf::Arena Arena;
    std::unique_ptr<Ydb::CostInfo> CostInfo;
    std::unique_ptr<Ydb::QuotaExceeded> QuotaExceeded;
};

template<typename TRpc>
NThreading::TFuture<typename TRpc::TResponse> DoLocalRpc(typename TRpc::TRequest&& proto, const TString& database, const TString& token, TActorSystem* actorSystem) {
    auto promise = NThreading::NewPromise<typename TRpc::TResponse>();

    proto.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);

    using TCbWrapper = TPromiseWrapper<typename TRpc::TResponse>;
    auto req = new TLocalRpcCtx<TRpc, TCbWrapper>(std::move(proto), TCbWrapper(promise), database, token);
    auto actor = TRpc::CreateRpcActor(req);
    actorSystem->Register(actor, TMailboxType::HTSwap, actorSystem->AppData<TAppData>()->UserPoolId);

    return promise.GetFuture();
}

template<typename TRpc>
TActorId DoLocalRpcSameMailbox(typename TRpc::TRequest&& proto, std::function<void(typename TRpc::TResponse)>&& cb, const TString& database, const TString& token, const TActorContext& ctx) {
    proto.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);

    auto req = new TLocalRpcCtx<TRpc, std::function<void(typename TRpc::TResponse)>>(std::move(proto), std::move(cb), database, token);
    auto actor = TRpc::CreateRpcActor(req);
    return ctx.RegisterWithSameMailbox(actor);
}

} // namespace NRpcService
} // namespace NKikimr
