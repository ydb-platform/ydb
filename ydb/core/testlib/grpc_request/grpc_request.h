#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/empty.pb.h>

namespace NKikimr::Tests::NGrpc {

template<typename TRes>
struct TResultHolder {
    std::optional<Ydb::StatusIds::StatusCode> ResultStatus;
    NYql::TIssue Issue;

    TRes Result;
};

template<typename TReq, typename TRes>
class TRequestCtx: public NKikimr::NGRpcService::IRequestOpCtx {
public:
    using TRequest = TRequestCtx;

    TRequestCtx(
            const TReq& request,
            TString topicPath,
            TString databaseName,
            std::shared_ptr<TResultHolder<TRes>> resultHolder
        )
        : Request(request)
        , TopicPath(topicPath)
        , DatabaseName(databaseName)
        , ResultHolder(resultHolder)
    {
    };

    const TString path() const {
        return TopicPath;
    }

    TMaybe<TString> GetTraceId() const override {
        return Nothing();
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return DatabaseName;
    }

    TString GetRpcMethodName() const override {
        // We have no grpc method, but the closest analog is protobuf name
        if (const NProtoBuf::Message* req = GetRequest()) {
            return req->GetDescriptor()->name();
        }
        return {};
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return UserToken;
    }

    const TString& GetSerializedToken() const override {
        static TString emptyString;
        return UserToken == nullptr ? emptyString : UserToken->GetSerializedToken();
    }

    bool IsClientLost() const override {
        return false;
    };

    const google::protobuf::Message* GetRequest() const override {
        return &Request;
    };

    const TMaybe<TString> GetRequestType() const override {
        return Nothing();
    };

    void SetFinishAction(std::function<void()>&& cb) override {
        Y_UNUSED(cb);
    };

    google::protobuf::Arena* GetArena() override {
        return nullptr;
    };

    bool HasClientCapability(const TString& capability) const override {
        Y_UNUSED(capability);
        return false;
    };

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        ProcessYdbStatusCode(status, google::protobuf::Empty{});
    };

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

    void RaiseIssue(const NYql::TIssue& issue) override {
        ResultHolder->Issue = issue;
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        Y_UNUSED(issues);
    };

    const TString& GetRequestName() const override {
        return DummyString;
    };

    bool GetDiskQuotaExceeded() const override {
        return false;
    };

    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        Y_UNUSED(name);
        Y_UNUSED(value);
    };

    const NKikimr::NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        return DummyAuditLogParts;
    };

    void SetRuHeader(ui64 ru) override {
        Y_UNUSED(ru);
    };

    void AddServerHint(const TString& hint) override {
        Y_UNUSED(hint);
    };

    void SetCostInfo(float consumed_units) override {
        Y_UNUSED(consumed_units);
    };

    void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
        Y_UNUSED(cb);
    };

    void FinishStream(ui32 status) override {
        Y_UNUSED(status);
    };

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl flag = EStreamCtrl::CONT)
            override {
        Y_UNUSED(flag);
        Y_UNUSED(in);
        Y_UNUSED(status);
    };

    void Reply(NProtoBuf::Message* resp, ui32 status = 0) override {
        Y_UNUSED(resp);
        Y_UNUSED(status);
    };

    void SendOperation(const Ydb::Operations::Operation& operation) override {
        Y_UNUSED(operation);
    };

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        ProcessYdbStatusCode(status, result);
    };

    void SendResult(
            const google::protobuf::Message& result,
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {

        Y_UNUSED(message);
        ProcessYdbStatusCode(status, result);
    };

    const Ydb::Operations::OperationParams& operation_params() const {
        return DummyParams;
    }

    static TRequestCtx* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TRequestCtx*>(request.get());
    }

protected:
    void FinishRequest() override {
    };

private:
    const TReq Request;
    const Ydb::Operations::OperationParams DummyParams;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString DummyString;
    const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
    const TString TopicPath;
    const TString DatabaseName;

    std::shared_ptr<TResultHolder<TRes>> ResultHolder;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status, const google::protobuf::Message& result) {
        ResultHolder->ResultStatus = status;

        if (status == Ydb::StatusIds::SUCCESS) {
            Cerr << (TStringBuilder() << "RESULT:" << result.DebugString() << Endl);
            auto r = dynamic_cast<const TRes*>(&result);
            UNIT_ASSERT(r);

            ResultHolder->Result = *r;
        } else {
            Cerr << (TStringBuilder() << "RESULT:" << status << " " << ResultHolder->Issue.GetMessage() << Endl);
        }
    }
};
    

} // namespace NKikimr::Tests::NGrpc
