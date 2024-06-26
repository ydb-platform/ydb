#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_request_base.h>

namespace NKikimr::NGRpcService::NLocalGrpc {

class TContextBase : public NYdbGrpc::IRequestContextBase {
public:
    TContextBase(std::shared_ptr<IRequestCtx> baseRequest)
        : BaseRequest_{std::move(baseRequest)}
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
    TString GetPeer() const override { return "localhost"; }
    bool SslServer() const override { return false; }
    bool IsClientLost() const override { return false; }

public:
    NYql::TIssues GetIssues() {
        return IssueManager_.GetIssues();
    }

protected:
    const IRequestCtx& GetBaseRequest() const noexcept {
        return *BaseRequest_;
    }

    IRequestCtx& GetBaseRequest() noexcept {
        return *BaseRequest_;
    }

    void RaiseIssue(const NYql::TIssue& issue) {
        IssueManager_.RaiseIssue(issue);
    }

private:
    std::shared_ptr<IRequestCtx> BaseRequest_;
    NYdbGrpc::TAuthState AuthState_;

    NYql::TIssueManager IssueManager_;
    google::protobuf::Arena Arena_;
};

} // namespace NKikimr::NGRpcService::NLocalGrpc
