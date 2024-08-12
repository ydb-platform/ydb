#include "ydb/core/kafka_proxy/kafka_constants.h"

#include "ydb/core/grpc_services/base/base.h"

#include "ydb/core/kafka_proxy/actors/actors.h"

#include "ydb/core/kafka_proxy/kafka.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

namespace NKafka {

struct TRetentionsConversionResult {
    bool IsValid = true;
    std::optional<ui64> Ms;
    std::optional<ui64> Bytes;

    std::vector<TString> ErrorMessages;

    TAutoPtr<TEvKafka::TEvTopicModificationResponse> GetKafkaErrorResponse(TString topicName) {
        auto response = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
        response->TopicPath = topicName;

        if (ErrorMessages.size() == 0) {
            response->Status = NONE_ERROR;
        } else {
            response->Status = INVALID_CONFIG;

            TStringBuilder resultErrorMessage;
            resultErrorMessage << ErrorMessages[0];
            for (ui64 i = 1; i < ErrorMessages.size(); i++) {
                resultErrorMessage << ' ' << ErrorMessages[i];
            }
            response->Message = resultErrorMessage;
        }

        return TAutoPtr<TEvKafka::TEvTopicModificationResponse>(response.Release());
    }
};

inline TRetentionsConversionResult ConvertRetentions(std::optional<TString> retentionMs, std::optional<TString> retentionBytes) {
    TRetentionsConversionResult result;

    auto convertRetention = [&result](
            std::optional<TString>& retention,
            const TString& configName,
            std::function<void(std::optional<ui64>)> setConversionResult
    ) -> void {
        if (retention.has_value()) {
            try {
                setConversionResult(std::stoul(retention.value()));
            } catch(std::invalid_argument) {
                result.IsValid = false;
                result.ErrorMessages.push_back(TStringBuilder() << configName << " value is not a valid number.");
            }
        }
    };

    convertRetention(
            retentionMs,
            RETENTION_MS_CONFIG_NAME,
            [&result](std::optional<ui64> retention) -> void { result.Ms = retention; }
    );
    
    convertRetention(
            retentionBytes,
            RETENTION_BYTES_CONFIG_NAME,
            [&result](std::optional<ui64> retention) -> void { result.Bytes = retention; }
    );
    
    return result;
}

template<class T>
inline TStringBuilder InputLogMessage(
        TString actorType,
        std::vector<T>& topics,
        bool isValidateOnly,
        std::function<TString(T)> extractTopicName
) {
    TStringBuilder stringBuilder;
    stringBuilder << actorType << ": New request. ValidateOnly:" << isValidateOnly << ". Topics: [";
    bool isFirst = true;
    for (auto& topic : topics) {
        if (isFirst) {
            isFirst = false;
        } else {
            stringBuilder << ",";
        }
        stringBuilder << " " << extractTopicName(topic);
    }
    stringBuilder << " ]";
    return stringBuilder;
}

inline std::optional<THolder<TEvKafka::TEvTopicModificationResponse>> ValidateTopicConfigName(TString configName) {
    if (configName == COMPRESSION_TYPE) {
        auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
        result->Status = EKafkaErrors::INVALID_REQUEST;
        result->Message = TStringBuilder()
            << "Topic-level config '"
            << COMPRESSION_TYPE
            << "' is not allowed.";
        return result;
    } else {
        return std::optional<THolder<TEvKafka::TEvTopicModificationResponse>>();
    }
} 

template<class T>
inline std::unordered_set<TString> ExtractDuplicates(
        std::vector<T>& source,
        std::function<TString(T)> extractTopicName
) {
    std::unordered_set<TString> duplicates;
    std::unordered_set<TString> topicNames;
    for (auto& topic : source) {
        auto topicName = extractTopicName(topic);
        if (topicNames.contains(topicName)) {
            duplicates.insert(topicName);
        } else {
            topicNames.insert(topicName);
        }
    }
    return duplicates;
}

template<class T, class U>
class TAlterTopicActor : public NKikimr::NGRpcProxy::V1::TUpdateSchemeActor<T, U> {
    using TBase = NKikimr::NGRpcProxy::V1::TUpdateSchemeActor<T, U>;

public:

    TAlterTopicActor(
            TActorId requester, 
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName)
        : TBase(new U(
            userToken,
            topicPath,
            databaseName,
            [this](const EKafkaErrors status, const TString& message) {
                this->SendResult(status, message);
            })
        )
        , TopicPath(topicPath)
        , Requester(requester)
    {
    };

    ~TAlterTopicActor() = default;

    void SendResult(const EKafkaErrors status, const TString& message) {
        THolder<TEvKafka::TEvTopicModificationResponse> response(new TEvKafka::TEvTopicModificationResponse());
        response->Status = status;
        response->TopicPath = TopicPath;
        response->Message = message;
        TBase::Send(Requester, response.Release());
        TBase::Send(TBase::SelfId(), new TEvents::TEvPoison());
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        TBase::SendDescribeProposeRequest(ctx);
        TBase::Become(&TBase::StateWork);
    };

protected:
    const TString TopicPath;

private:
    const TActorId Requester;
    const std::shared_ptr<TString> SerializedToken;
};

class TKafkaTopicModificationRequest : public NKikimr::NGRpcService::IRequestOpCtx {
public:
    using TRequest = TKafkaTopicModificationRequest;

    TKafkaTopicModificationRequest(
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            const std::function<void(const EKafkaErrors, const TString&)> sendResultCallback)
        : UserToken(userToken)
        , TopicPath(topicPath)
        , DatabaseName(databaseName)
        , SendResultCallback(sendResultCallback)
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

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return UserToken;
    }

    const TString& GetSerializedToken() const override {
        return UserToken->GetSerializedToken();
    }

    bool IsClientLost() const override {
        return false;
    };

    virtual const google::protobuf::Message* GetRequest() const override {
        return nullptr;
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
        ProcessYdbStatusCode(status);
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
        Issue = issue;
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

    google::protobuf::Message* GetRequestMut() override {
        return nullptr;
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
        Y_UNUSED(result);
        ProcessYdbStatusCode(status);
    };

    void SendResult(
            const google::protobuf::Message& result,
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {

        Y_UNUSED(result);
        Y_UNUSED(message);
        ProcessYdbStatusCode(status);
    };

    const Ydb::Operations::OperationParams& operation_params() const {
        return DummyParams;
    }

    static TKafkaTopicModificationRequest* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TKafkaTopicModificationRequest*>(&(*request));
    }

protected:
    void FinishRequest() override {
    };

    virtual EKafkaErrors Convert(Ydb::StatusIds::StatusCode& status) {
        return ConvertErrorCode(status);
    }

private:
    const Ydb::Operations::OperationParams DummyParams;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString DummyString;
    const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
    const TString TopicPath;
    const TString DatabaseName;
    const std::function<void(const EKafkaErrors status, const TString& message)> SendResultCallback;
    NYql::TIssue Issue;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status) {
        SendResultCallback(Convert(status), Issue.GetMessage());
    }
};
}
