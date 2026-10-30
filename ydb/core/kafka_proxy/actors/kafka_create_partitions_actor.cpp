#include "kafka_create_partitions_actor.h"

#include "control_plane_common.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KAFKA_PROXY


namespace NKafka {

class TCreatePartitionsActor : public TAlterTopicActor<TCreatePartitionsActor, TKafkaTopicRequestCtx> {
public:

    TCreatePartitionsActor(
            TActorId requester,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            ui32 partitionsNumber)
        : TAlterTopicActor<TCreatePartitionsActor, TKafkaTopicRequestCtx>(
            requester,
            userToken,
            topicPath,
            databaseName)
        , PartionsNumber(partitionsNumber)
    {
        YDB_LOG_DEBUG("Create partitions actor",
            {LogPrefix()},
            {"databaseName", databaseName},
            {"topicPath", TopicPath},
            {"partitionsNumber", PartionsNumber});
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
    YDB_LOG_DEBUG("Dump logPrefix, inputLogMessage",
        {LogPrefix()},
        {"inputLogMessage", InputLogMessage()});

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
            Context->Token.UserToken,
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
