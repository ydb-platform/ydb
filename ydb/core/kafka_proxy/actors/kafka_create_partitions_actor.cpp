#include "kafka_create_partitions_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>


namespace NKafka {

class TKafkaCreatePartitionsRequest : public NKikimr::NGRpcService::IRequestOpCtx {
public:
    using TRequest = TKafkaCreatePartitionsRequest;

    TKafkaCreatePartitionsRequest(
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            const std::function<void(TEvKafka::TEvTopicModificationResponse::EStatus, const TString&)> sendResultCallback)
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

    void RaiseIssue(const NYql::TIssue& issue) override{
        Issue = issue;
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        Y_UNUSED(issues);
    };

    const TString& GetRequestName() const override {
        return DummyString;
    };

    void SetDiskQuotaExceeded(bool disk) override {
        Y_UNUSED(disk);
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

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status) override {
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

    void SendResult(
            Ydb::StatusIds::StatusCode status,
            const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {

        Y_UNUSED(message);
        ProcessYdbStatusCode(status);
    };

    const Ydb::Operations::OperationParams& operation_params() const {
        return DummyParams;
    }

    static TKafkaCreatePartitionsRequest* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TKafkaCreatePartitionsRequest*>(&(*request));
    }

protected:
    void FinishRequest() override {
    };

private:
    const Ydb::Operations::OperationParams DummyParams;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString DummyString;
    const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
    const TString TopicPath;
    const TString DatabaseName;
    const std::function<void(const TEvKafka::TEvTopicModificationResponse::EStatus status, const TString& message)> SendResultCallback;
    NYql::TIssue Issue;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status) {
        const auto& message = Issue.GetMessage();
        switch (status) {
            case Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SUCCESS:
                SendResultCallback(TEvKafka::TEvTopicModificationResponse::EStatus::OK, message);
                break;
            case Ydb::StatusIds::StatusCode::StatusIds_StatusCode_BAD_REQUEST:
                SendResultCallback(TEvKafka::TEvTopicModificationResponse::EStatus::BAD_REQUEST, message);
                break;
            case Ydb::StatusIds::StatusCode::StatusIds_StatusCode_SCHEME_ERROR:
                if (Issue.IssueCode == Ydb::PersQueue::ErrorCode::ACCESS_DENIED) {
                    SendResultCallback(TEvKafka::TEvTopicModificationResponse:: EStatus::TOPIC_DOES_NOT_EXIST, message);
                } else {
                    SendResultCallback(TEvKafka::TEvTopicModificationResponse::EStatus::ERROR, message);
                }
                break;
            default:
                SendResultCallback(TEvKafka::TEvTopicModificationResponse::EStatus::ERROR, message);
        }
    }
};

class TCreatePartitionsActor : public NKikimr::NGRpcProxy::V1::TUpdateSchemeActor<TCreatePartitionsActor, TKafkaCreatePartitionsRequest>{
    using TBase = NKikimr::NGRpcProxy::V1::TUpdateSchemeActor<TCreatePartitionsActor, TKafkaCreatePartitionsRequest>;
public:

    TCreatePartitionsActor(
            TActorId requester, 
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            ui32 partitionsNumber)
        : TBase(new TKafkaCreatePartitionsRequest(
            userToken,
            topicPath,
            databaseName,
            [this](const TEvKafka::TEvTopicModificationResponse::EStatus status, const TString& message) {
                this->SendResult(status, message);
            })
        )
        , Requester(requester)
        , TopicPath(topicPath)
        , PartionsNumber(partitionsNumber)
    {
        KAFKA_LOG_D(
            "Create Topic actor. DatabaseName: " << databaseName <<
            ". TopicPath: " << TopicPath <<
            ". PartitionsNumber: " << PartionsNumber);
    };

    ~TCreatePartitionsActor() = default;

    void SendResult(const TEvKafka::TEvTopicModificationResponse::EStatus status, const TString& message) {
        THolder<TEvKafka::TEvTopicModificationResponse> response(new TEvKafka::TEvTopicModificationResponse());
        response->Status = status;
        response->TopicPath = TopicPath;
        response->Message = message;
        Send(Requester, response.Release());
        Send(SelfId(), new TEvents::TEvPoison());
    }

    void ModifyPersqueueConfig(
            const TActorContext& ctx,
            NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
            const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(ctx);
        Y_UNUSED(pqGroupDescription);
        Y_UNUSED(selfInfo);

        groupConfig.SetTotalGroupCount(PartionsNumber);
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TBase::StateWork);
    };

private:
    const TActorId Requester;
    const TString TopicPath;
    const std::shared_ptr<TString> SerializedToken;
    const ui32 PartionsNumber;
};

NActors::IActor* CreateKafkaCreatePartitionsActor(
        const TContext::TPtr context,
        const ui64 correlationId,
        const TMessagePtr<TCreatePartitionsRequestData>& message
) {
    return new TKafkaCreatePartitionsActor(context, correlationId, message);
}

void TKafkaCreatePartitionsActor::Bootstrap(const NActors::TActorContext& ctx) {
    KAFKA_LOG_D(InputLogMessage());

    if (Message->ValidateOnly) {
        ProcessValidateOnly(ctx);
        return;
    }

    std::unordered_set<TString> topicNames;
    for (auto& topic : Message->Topics) {
        auto& topicName = topic.Name.value();
        if (topicNames.contains(topicName)) {
            DuplicateTopicNames.insert(topicName);
        } else {
            topicNames.insert(topicName);
        }
    }

    for (auto& topic : Message->Topics) {
        auto& topicName = topic.Name.value();

        if (DuplicateTopicNames.contains(topicName)) {
            continue;
        }

        if (topicName == "") {
            auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
            result->Status = TEvKafka::TEvTopicModificationResponse::EStatus::BAD_REQUEST;
            result->Message = "Empty topic name";
            this->TopicNamesToResponses[topicName] = TAutoPtr<TEvKafka::TEvTopicModificationResponse>(result.Release());
            continue;
        }

        ctx.Register(new TCreatePartitionsActor(
            SelfId(),
            Context->UserToken,
            topic.Name.value(),
            Context->DatabasePath,
            topic.Count
        ));

        InflyTopics++;
    }

    if (InflyTopics > 0) {
        Become(&TKafkaCreatePartitionsActor::StateWork);
    } else {
        Reply(ctx);
    }
};

void TKafkaCreatePartitionsActor::Handle(const TEvKafka::TEvTopicModificationResponse::TPtr& ev, const TActorContext& ctx) {
    auto eventPtr = ev->Release();
    TopicNamesToResponses[eventPtr->TopicPath] = eventPtr;
    InflyTopics--;
    if (InflyTopics == 0) {
        Reply(ctx);
    }
};

void TKafkaCreatePartitionsActor::Reply(const TActorContext& ctx) {
    TCreatePartitionsResponseData::TPtr response = std::make_shared<TCreatePartitionsResponseData>();
    EKafkaErrors responseStatus = NONE_ERROR;

    for (auto& requestTopic : Message->Topics) {
        auto topicName = requestTopic.Name.value();

        TCreatePartitionsResponseData::TCreatePartitionsTopicResult responseTopic;
        responseTopic.Name = topicName;

        if (TopicNamesToResponses.contains(topicName)) {
            responseTopic.ErrorMessage = TopicNamesToResponses[topicName]->Message;
        }

        auto setError= [&responseTopic, &responseStatus](EKafkaErrors status) { 
            responseTopic.ErrorCode = status;
            responseStatus = status;
        };

        if (DuplicateTopicNames.contains(topicName)) {
            setError(DUPLICATE_RESOURCE);
        } else {
            switch (TopicNamesToResponses[topicName]->Status) {
                case TEvKafka::TEvTopicModificationResponse::OK:
                    responseTopic.ErrorCode = NONE_ERROR;
                    break;
                case TEvKafka::TEvTopicModificationResponse::BAD_REQUEST:
                case TEvKafka::TEvTopicModificationResponse::TOPIC_DOES_NOT_EXIST:
                    setError(INVALID_REQUEST);
                    break;
                case TEvKafka::TEvTopicModificationResponse::ERROR:
                    setError(UNKNOWN_SERVER_ERROR);
                    break;
                case TEvKafka::TEvTopicModificationResponse::INVALID_CONFIG:
                    setError(INVALID_CONFIG);
                    break;
                }
        } 
        response->Results.push_back(responseTopic);
    }

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, responseStatus));

    Die(ctx);
};

TStringBuilder TKafkaCreatePartitionsActor::InputLogMessage() {
    TStringBuilder stringBuilder;
    stringBuilder << "Create partitions actor: New request. ValidateOnly:" << (Message->ValidateOnly != 0) << " Topics: [";

    bool isFirst = true;
    for (auto& requestTopic : Message->Topics) {
        if (isFirst) {
            isFirst = false;
        } else {
            stringBuilder << ",";
        }
        stringBuilder << " " << requestTopic.Name.value();
    }
    stringBuilder << " ]";
    return stringBuilder;
};


void TKafkaCreatePartitionsActor::ProcessValidateOnly(const NActors::TActorContext& ctx) {
    TCreatePartitionsResponseData::TPtr response = std::make_shared<TCreatePartitionsResponseData>();

    for (auto& requestTopic : Message->Topics) {
        auto topicName = requestTopic.Name.value();

        TCreatePartitionsResponseData::TCreatePartitionsTopicResult responseTopic;
        responseTopic.Name = topicName;
        responseTopic.ErrorCode = NONE_ERROR;
        response->Results.push_back(responseTopic);
    }

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, NONE_ERROR));
    Die(ctx);
};
}
