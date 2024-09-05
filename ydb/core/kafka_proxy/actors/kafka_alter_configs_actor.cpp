#include "kafka_alter_configs_actor.h"

#include "control_plane_common.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>


namespace NKafka {

class TKafkaAlterConfigsRequest: public TKafkaTopicModificationRequest {
public:
    TKafkaAlterConfigsRequest(
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            const std::function<void(const EKafkaErrors, const TString&)> sendResultCallback)
        : TKafkaTopicModificationRequest(userToken, topicPath, databaseName, sendResultCallback)
    {
    };

protected:
    EKafkaErrors Convert(Ydb::StatusIds::StatusCode& status) override {
        return status == Ydb::StatusIds::BAD_REQUEST
                ? INVALID_CONFIG
                : TKafkaTopicModificationRequest::Convert(status);
    }
};

class TAlterConfigsActor : public TAlterTopicActor<TAlterConfigsActor, TKafkaAlterConfigsRequest> {
public:

    TAlterConfigsActor(
            TActorId requester,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            std::optional<ui64> retentionMs,
            std::optional<ui64> retentionBytes)
        : TAlterTopicActor<TAlterConfigsActor, TKafkaAlterConfigsRequest>(
            requester,
            userToken,
            topicPath,
            databaseName)
        , RetentionMs(retentionMs)
        , RetentionBytes(retentionBytes)
    {
        KAFKA_LOG_D("Alter configs actor. DatabaseName: " << databaseName << ". TopicPath: " << TopicPath);
    };

    ~TAlterConfigsActor() = default;

    void ModifyPersqueueConfig(
            NKikimr::TAppData* appData,
            NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
            const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(appData);
        Y_UNUSED(pqGroupDescription);
        Y_UNUSED(selfInfo);

        auto partitionConfig = groupConfig.MutablePQTabletConfig()->MutablePartitionConfig();

        if (RetentionMs.has_value()) {
            partitionConfig->SetLifetimeSeconds(RetentionMs.value() / 1000);
        }

        if (RetentionBytes.has_value()) {
            partitionConfig->SetStorageLimitBytes(RetentionBytes.value());
        }
    }

private:
    std::optional<ui64> RetentionMs;
    std::optional<ui64> RetentionBytes;
};

NActors::IActor* CreateKafkaAlterConfigsActor(
        const TContext::TPtr context,
        const ui64 correlationId,
        const TMessagePtr<TAlterConfigsRequestData>& message
) {
    return new TKafkaAlterConfigsActor(context, correlationId, message);
}

void TKafkaAlterConfigsActor::Bootstrap(const NActors::TActorContext& ctx) {

    KAFKA_LOG_D(InputLogMessage());

    if (Message->ValidateOnly) {
        ProcessValidateOnly(ctx);
        return;
    }

    DuplicateTopicNames = ExtractDuplicates<TAlterConfigsRequestData::TAlterConfigsResource>(
        Message->Resources,
        [](TAlterConfigsRequestData::TAlterConfigsResource resource) -> TString { return resource.ResourceName.value(); });

    for (auto& resource : Message->Resources) {
        auto& topicName = resource.ResourceName.value();
        if (DuplicateTopicNames.contains(topicName)) {
            continue;
        }
        if (resource.ResourceType != TOPIC_RESOURCE_TYPE) {
            auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
            result->TopicPath = topicName;
            result->Status = EKafkaErrors::INVALID_REQUEST;
            result->Message = "Only TOPIC resource type is supported.";
            this->TopicNamesToResponses[resource.ResourceName.value()] = TAutoPtr<TEvKafka::TEvTopicModificationResponse>(result.Release());
            continue;
        }

        std::optional<TString> retentionMs;
        std::optional<TString> retentionBytes;

        std::optional<THolder<TEvKafka::TEvTopicModificationResponse>> unsupportedConfigResponse;

        for (auto& config : resource.Configs) {
            unsupportedConfigResponse = ValidateTopicConfigName(config.Name.value());
            if (unsupportedConfigResponse.has_value()) {
                break;
            }

            if (config.Name.value() == RETENTION_MS_CONFIG_NAME) {
                retentionMs = config.Value;
            } else if (config.Name.value() == RETENTION_BYTES_CONFIG_NAME) {
                retentionBytes = config.Value;
            }
        }

        if (unsupportedConfigResponse.has_value()) {
            this->TopicNamesToResponses[topicName] = unsupportedConfigResponse.value();
            continue;
        }

        TRetentionsConversionResult convertedRetentions = ConvertRetentions(retentionMs, retentionBytes);

        if (!convertedRetentions.IsValid) {
            this->TopicNamesToResponses[topicName] = convertedRetentions.GetKafkaErrorResponse(topicName);
            continue;
        };

        ctx.Register(new TAlterConfigsActor(
            SelfId(),
            Context->UserToken,
            resource.ResourceName.value(),
            Context->DatabasePath,
            convertedRetentions.Ms,
            convertedRetentions.Bytes
        ));

        InflyTopics++;
    }

    if (InflyTopics > 0) {
        Become(&TKafkaAlterConfigsActor::StateWork);
    } else {
        Reply(ctx);
    }
};

void TKafkaAlterConfigsActor::Handle(const TEvKafka::TEvTopicModificationResponse::TPtr& ev, const TActorContext& ctx) {
    auto eventPtr = ev->Release();
    TopicNamesToResponses[eventPtr->TopicPath] = eventPtr;
    InflyTopics--;
    if (InflyTopics == 0) {
        Reply(ctx);
    }
};

void TKafkaAlterConfigsActor::Reply(const TActorContext& ctx) {
    TAlterConfigsResponseData::TPtr response = std::make_shared<TAlterConfigsResponseData>();
    EKafkaErrors responseStatus = NONE_ERROR;

    for (auto& requestResource : Message->Resources) {
        auto resourceName = requestResource.ResourceName.value();

        TAlterConfigsResponseData::TAlterConfigsResourceResponse responseResource;
        responseResource.ResourceName = requestResource.ResourceName;

        if (!TopicNamesToResponses.contains(resourceName)) {
            continue;
        }

        EKafkaErrors status = TopicNamesToResponses[resourceName]->Status;
        responseResource.ErrorCode = status;
        responseStatus = status;

        responseResource.ErrorMessage = TopicNamesToResponses[resourceName]->Message;
        response->Responses.push_back(responseResource);
    }

    for (auto& topicName : DuplicateTopicNames) {
        TAlterConfigsResponseData::TAlterConfigsResourceResponse responseResource;
        responseResource.ResourceName = topicName;
        responseResource.ErrorMessage = "Duplicate resource in request.";
        responseResource.ErrorCode = INVALID_REQUEST;
        response->Responses.push_back(responseResource);
        responseStatus = INVALID_REQUEST;
    }

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, responseStatus));

    Die(ctx);
};

TStringBuilder TKafkaAlterConfigsActor::InputLogMessage() {
    return InputLogMessage<TAlterConfigsRequestData::TAlterConfigsResource>(
            "Alter configs actor",
            Message->Resources,
            Message->ValidateOnly != 0,
            [](TAlterConfigsRequestData::TAlterConfigsResource resource) -> TString {
                return resource.ResourceName.value();
            });
};


void TKafkaAlterConfigsActor::ProcessValidateOnly(const NActors::TActorContext& ctx) {
    TAlterConfigsResponseData::TPtr response = std::make_shared<TAlterConfigsResponseData>();

    for (auto& requestResource : Message->Resources) {
        TAlterConfigsResponseData::TAlterConfigsResourceResponse responseResource;
        responseResource.ResourceName = requestResource.ResourceName;
        responseResource.ResourceType = requestResource.ResourceType;
        responseResource.ErrorCode = NONE_ERROR;
        response->Responses.push_back(responseResource);
    }

    KAFKA_LOG_D("KLACK TKafkaAlterConfigsActor::ProcessValidateOnly: CorrelationId == " << CorrelationId);
    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, response, NONE_ERROR));
    Die(ctx);
};
}
