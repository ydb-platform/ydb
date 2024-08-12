#include "kafka_create_partitions_actor.h"

#include "control_plane_common.h"

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
            const std::function<void(EKafkaErrors, const TString&)> sendResultCallback)
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

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl) override {
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
    const std::function<void(const EKafkaErrors status, const TString& message)> SendResultCallback;
    NYql::TIssue Issue;

    void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status) {
        SendResultCallback(ConvertErrorCode(status), Issue.GetMessage());
    }
};

class TCreatePartitionsActor : public TAlterTopicActor<TCreatePartitionsActor, TKafkaTopicModificationRequest> {
public:

    TCreatePartitionsActor(
            TActorId requester,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            ui32 partitionsNumber)
        : TAlterTopicActor<TCreatePartitionsActor, TKafkaTopicModificationRequest>(
            requester,
            userToken,
            topicPath,
            databaseName)
        , PartionsNumber(partitionsNumber)
    {
        KAFKA_LOG_D(
            "Create partitions actor. DatabaseName: " << databaseName <<
            ". TopicPath: " << TopicPath <<
            ". PartitionsNumber: " << PartionsNumber);
    };

    void ModifyPersqueueConfig(
            NKikimr::TAppData* appData,
            NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
            const NKikimrSchemeOp::TDirEntry& selfInfo
    ) override {
        Y_UNUSED(appData);
        Y_UNUSED(pqGroupDescription);
        Y_UNUSED(selfInfo);

        groupConfig.SetTotalGroupCount(PartionsNumber);
    }

    ~TCreatePartitionsActor() = default;

private:
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

    DuplicateTopicNames = ExtractDuplicates<TCreatePartitionsRequestData::TCreatePartitionsTopic>(
        Message->Topics,
        [](TCreatePartitionsRequestData::TCreatePartitionsTopic topic) -> TString { return topic.Name.value(); });

    for (auto& topic : Message->Topics) {
        auto& topicName = topic.Name.value();

        if (DuplicateTopicNames.contains(topicName)) {
            continue;
        }

        if (topicName == "") {
            auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
            result->Status = INVALID_REQUEST;
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

        if (!TopicNamesToResponses.contains(topicName)) {
            continue;
        }

        TCreatePartitionsResponseData::TCreatePartitionsTopicResult responseTopic;
        responseTopic.Name = topicName;
        responseTopic.ErrorMessage = TopicNamesToResponses[topicName]->Message;

        EKafkaErrors status = TopicNamesToResponses[topicName]->Status;
        responseTopic.ErrorCode = status;
        if(status != NONE_ERROR) {
            responseStatus = status;
        }

        response->Results.push_back(responseTopic);
    }

    for (auto& topicName : DuplicateTopicNames) {
        TCreatePartitionsResponseData::TCreatePartitionsTopicResult responseTopic;
        responseTopic.Name = topicName;
        responseTopic.ErrorMessage = "Duplicate topic in request.";
        responseTopic.ErrorCode = INVALID_REQUEST;

        response->Results.push_back(responseTopic);

        responseStatus = INVALID_REQUEST;
    }
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, responseStatus));

    Die(ctx);
};

TStringBuilder TKafkaCreatePartitionsActor::InputLogMessage() {
    return InputLogMessage<TCreatePartitionsRequestData::TCreatePartitionsTopic>(
            "Create partitions actor",
            Message->Topics,
            Message->ValidateOnly != 0,
            [](TCreatePartitionsRequestData::TCreatePartitionsTopic topic) -> TString {
                return topic.Name.value();
            });
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
