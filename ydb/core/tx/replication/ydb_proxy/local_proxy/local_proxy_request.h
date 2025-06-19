#pragma once

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NReplication {

class TLocalProxyRequest : public NKikimr::NGRpcService::IRequestOpCtx {
public:
    using TRequest = TLocalProxyRequest;

    TLocalProxyRequest(
           // TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString path,
            TString databaseName,
            std::unique_ptr<google::protobuf::Message>&& request,
            const std::function<void(Ydb::StatusIds::StatusCode, const google::protobuf::Message*)> sendResultCallback)
        : //UserToken(userToken)
         Path(path)
        , DatabaseName(databaseName)
        , Request(std::move(request))
        , SendResultCallback(sendResultCallback)
    {
    }

    const TString path() const {
        return Path;
    }

    TMaybe<TString> GetTraceId() const override {
        return Nothing();
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return DatabaseName;
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return UserToken;
    }

    const TString& GetSerializedToken() const override {
        return DummyString;
    }

    bool IsClientLost() const override {
        return false;
    }

    const google::protobuf::Message* GetRequest() const override {
        return Request.get();
    }

    const TMaybe<TString> GetRequestType() const override {
        return Nothing();
    }

    void SetFinishAction(std::function<void()>&& cb) override {
        Y_UNUSED(cb);
    }

    google::protobuf::Arena* GetArena() override {
        return nullptr;
    }

    bool HasClientCapability(const TString& capability) const override {
        Y_UNUSED(capability);
        return false;
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        ProcessYdbStatusCode(status, NULL);
    }

    void ReplyWithRpcStatus(grpc::StatusCode code, const TString& msg = "", const TString& details = "") override {
        Y_UNUSED(code);
        Y_UNUSED(msg);
        Y_UNUSED(details);
    }

    TString GetPeerName() const override {
        return "";
    }

    TInstant GetDeadline() const override {
        return TInstant();
    }

    const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
        return Nothing();
    }

    TVector<TStringBuf> FindClientCert() const override {
        return TVector<TStringBuf>();
    }

    TMaybe<NKikimr::NRpcService::TRlPath> GetRlPath() const override {
        return Nothing();
    }

    void RaiseIssue(const NYql::TIssue& issue) override{
        Issue = issue;
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        Y_UNUSED(issues);
    }

    const TString& GetRequestName() const override {
        return DummyString;
    }

    bool GetDiskQuotaExceeded() const override {
        return false;
    }

    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        Y_UNUSED(name);
        Y_UNUSED(value);
    }

    const NKikimr::NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        return DummyAuditLogParts;
    }

    void SetRuHeader(ui64 ru) override {
        Y_UNUSED(ru);
    }

    void AddServerHint(const TString& hint) override {
        Y_UNUSED(hint);
    }

    void SetCostInfo(float consumed_units) override {
        Y_UNUSED(consumed_units);
    }

    void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
        Y_UNUSED(cb);
    }

    void FinishStream(ui32 status) override {
        Y_UNUSED(status);
    }

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl) override {
        Y_UNUSED(in);
        Y_UNUSED(status);
    }

    void Reply(NProtoBuf::Message* resp, ui32 status = 0) override {
        Y_UNUSED(resp);
        Y_UNUSED(status);
    }

    void SendOperation(const Ydb::Operations::Operation& operation) override {
        Y_UNUSED(operation);
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        Y_UNUSED(result);
        ProcessYdbStatusCode(status, &result);
    }

    void SendResult(
            const google::protobuf::Message& result,
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override
    {
        Y_UNUSED(result);
        Y_UNUSED(message);
        ProcessYdbStatusCode(status, NULL);
    }

    const Ydb::Operations::OperationParams& operation_params() const {
        return DummyParams;
    }

    static TLocalProxyRequest* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TLocalProxyRequest*>(&(*request));
    }

protected:
    void FinishRequest() override {
    }

private:
    const Ydb::Operations::OperationParams DummyParams;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString DummyString;
    const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
    const TString Path;
    const TString DatabaseName;
    std::unique_ptr<google::protobuf::Message> Request;
    const std::function<void(Ydb::StatusIds::StatusCode, const google::protobuf::Message*)> SendResultCallback;
    NYql::TIssue Issue;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status, const google::protobuf::Message* result) {
        SendResultCallback(status, result);
    }
};

}
