#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_request_base.h>

namespace NKikimr::NGRpcService {

class TLocalGrpcContextBase : public NYdbGrpc::IRequestContextBase {
public:
    TLocalGrpcContextBase(std::shared_ptr<IRequestCtx> baseRequest)
        : Scope_{"yandexcloud:/" + baseRequest->GetDatabaseName().GetOrElse("/")}
        , BaseRequest_{std::move(baseRequest)}
        , AuthState_{/*needAuth*/true}
    {}

    virtual void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) = 0;

    NYdbGrpc::TAuthState& GetAuthState() override {
        return AuthState_;
    }

    void ReplyUnauthenticated(const TString& in) override {
        ReplyError(grpc::UNAUTHENTICATED, in);
    }

    void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details = "") override {
        NYql::TIssue issue{TStringBuilder() << "grpc code: " << code << ", msg: " << msg << " (" << details << ")"};
        issue.SetCode(code, NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR);
        RaiseIssue(issue);
        ReplyWithYdbStatus(Ydb::StatusIds::GENERIC_ERROR);
    }

    TInstant Deadline() const override {
        return BaseRequest_->GetDeadline();
    }

    TSet<TStringBuf> GetPeerMetaKeys() const override {
        Y_ABORT("TLocalGrpcContext::GetPeerMetaKeys unimplemented");
        return {};
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        if (key == "x-ydb-fq-project") {
            return {Scope_};
        }
        auto value = BaseRequest_->GetPeerMetaValues(TString{key});
        if (value) {
            return {std::move(*value)};
        }
        return {};
    }

    TVector<TStringBuf> FindClientCert() const override {
        return BaseRequest_->FindClientCert();
    }

    grpc_compression_level GetCompressionLevel() const override {
        return GRPC_COMPRESS_LEVEL_NONE;
    }

    google::protobuf::Arena* GetArena() override {
        return &Arena_;
    }

    void AddTrailingMetadata(const TString& key, const TString& value) override {
        Y_UNUSED(key, value);
    }

    void UseDatabase(const TString& database) override {
        Y_UNUSED(database);
    }

    // Streaming part

    void SetNextReplyCallback(TOnNextReply&& cb) override {
        Y_UNUSED(cb);
    }
    void FinishStreamingOk() override {}
    TAsyncFinishResult GetFinishFuture() override { return {}; }
    TString GetPeer() const override { return {}; }
    bool SslServer() const override { return false; }
    bool IsClientLost() const override { return false; }

public:
    NYql::TIssues GetIssues() {
        return IssueManager_.GetIssues();
    }

private:
    void RaiseIssue(const NYql::TIssue& issue) {
        IssueManager_.RaiseIssue(issue);
    }

private:
    TString Scope_;

    std::shared_ptr<IRequestCtx> BaseRequest_;
    NYdbGrpc::TAuthState AuthState_;

    NYql::TIssueManager IssueManager_;
    google::protobuf::Arena Arena_;
};

template<typename TReq, typename TResp>
class TLocalGrpcContext
    : public TLocalGrpcContextBase {
public:
    using TRequest = TReq;
    using TResponse = TResp;
    using TBase = TLocalGrpcContextBase;

    TLocalGrpcContext(
        TReq&& request, std::shared_ptr<IRequestCtx> baseRequest,
        std::function<void(const TResponse&)> replyCallback)
        : TBase{std::move(baseRequest)}
        , Request_{std::move(request)}
        , ReplyCallback_{std::move(replyCallback)}
    {}

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TResp resp;
        NGRpcService::TCommonResponseFiller<TResp, true>::Fill(resp, TBase::GetIssues(), nullptr, status);
        ReplyCallback_(resp);
    }

    const NProtoBuf::Message* GetRequest() const override {
        return &Request_;
    }

    //! Get mutable pointer to the request's message.
    NProtoBuf::Message* GetRequestMut() override {
        return &Request_;
    }

    void Reply(NProtoBuf::Message* proto, ui32 status = 0) override {
        Y_UNUSED(status);
        TResp* resp = dynamic_cast<TResp*>(proto);
        Y_ABORT_UNLESS(resp);
        ReplyCallback_(*resp);
    }

    void Reply(grpc::ByteBuffer* resp, ui32 status = 0, EStreamCtrl ctrl = EStreamCtrl::CONT) override {
        Y_UNUSED(resp, status, ctrl);
        Y_ABORT("TLocalGrpcContext::Reply for stream is unimplemented");
    }

private:
    TReq Request_;
    std::function<void(const TResponse&)> ReplyCallback_;
};

// Usage facade

template <typename TReq, typename TResp>
struct TLocalGrpcCallBase {
    using TRequest = TReq;
    using TResponse = TResp;

    static TIntrusivePtr<NYdbGrpc::IRequestContextBase> MakeContext(
        TReq&& request,
        std::shared_ptr<IRequestCtx>&& baseRequest,
        std::function<void(const TResp&)>&& replyCallback) {
        return new TLocalGrpcContext<TReq, TResp>{
            std::move(request), std::move(baseRequest), std::move(replyCallback)
        };
    }
};

/// Specializations are expected to derive from TLocalGrpcCallBase<TReq, TResp> and implement
///   static std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest(TReq&&, std::shared_ptr<IRequestCtx>&&, std::function<void(const TResp&)>&&)
/// Partial go
template <typename TReq>
struct TLocalGrpcCall;

template <typename TMsg, ui32 EMsgType>
class TEventBase : public TEventLocal<TEventBase<TMsg, EMsgType>, EMsgType> {
public:
    TEventBase() = default;

    TEventBase(TMsg message)
        : Message{std::move(message)}
    {}

    TMsg Message;
};

/// Specializations are expected to publicly derive from TEventBase<TMsg, ...>
template <typename TMsg>
class TEvent;

class TLocalGrpcCaller {
public:
    TLocalGrpcCaller(TActorId grpcProxyId)
        : GrpcProxyId_{std::move(grpcProxyId)}
    {}

    /// Caller has to handle TEvent<TResponse> of corresponding request
    template <typename TRequest>
    void MakeLocalCall(TRequest&& request, std::shared_ptr<IRequestCtx> baseRequest, const TActorContext& ctx) {
        using TGrpcCall = TLocalGrpcCall<std::decay_t<TRequest>>;
        using TResponse = typename TGrpcCall::TResponse;

        auto localRequest = TGrpcCall::MakeRequest(std::move(request), std::move(baseRequest), [as = ctx.ActorSystem(), selfId = ctx.SelfID](const TResponse& resp) {
            as->Send(selfId, new TEvent<TResponse>(resp));
        });
        ctx.Send(GrpcProxyId_, localRequest.release());
    }
private:
    TActorId GrpcProxyId_;
};
}
