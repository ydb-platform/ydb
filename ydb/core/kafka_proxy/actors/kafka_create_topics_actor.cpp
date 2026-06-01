#include "kafka_create_topics_actor.h"

#include "control_plane_common.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>


namespace NKafka {

std::optional<THolder<TEvKafka::TEvTopicModificationResponse>> ConvertCleanupPolicy(
        const std::optional<TString>& configValue, std::optional<ECleanupPolicy>& cleanupPolicy
) {
    if (configValue.value_or("") == "delete") {
        cleanupPolicy = ECleanupPolicy::REMOVE;
        return std::nullopt;
    } else if (configValue.value_or("") == "compact") {
        cleanupPolicy = ECleanupPolicy::COMPACT;
        return std::nullopt;
    }
    auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
    result->Status = EKafkaErrors::INVALID_REQUEST;
    result->Message = TStringBuilder()
        << "Topic-level config '"
        << CLEANUP_POLICY
        << "' has invalid/unsupported value: "
        << configValue.value_or("");
    return result;
}


std::optional<THolder<TEvKafka::TEvTopicModificationResponse>> ConvertTimestampType(
        const std::optional<TString>& configValue, std::optional<TString>& correctTimestampType
) {
    if (configValue == MESSAGE_TIMESTAMP_CREATE_TIME || configValue == MESSAGE_TIMESTAMP_LOG_APPEND) {
        correctTimestampType = configValue;
        return std::nullopt;
    }
    auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
    result->Status = EKafkaErrors::INVALID_REQUEST;
    result->Message = TStringBuilder()
        << "Topic-level config '"
        << MESSAGE_TIMESTAMP_TYPE
        << "' has invalid/unsupported value: "
        << configValue.value_or("");
    return result;
}

NActors::IActor* CreateKafkaCreateTopicsActor(
        const TContext::TPtr context,
        const ui64 correlationId,
        const TMessagePtr<TCreateTopicsRequestData>& message
) {
    return new TKafkaCreateTopicsActor(context, correlationId, message);
}

void TKafkaCreateTopicsActor::Bootstrap(const NActors::TActorContext& ctx) {
    KAFKA_LOG_D(InputLogMessage());

    if (Message->ValidateOnly) {
        ProcessValidateOnly(ctx);
        return;
    }

    DuplicateTopicNames = ExtractDuplicates<TCreateTopicsRequestData::TCreatableTopic>(
        Message->Topics,
        [](TCreateTopicsRequestData::TCreatableTopic topic) -> TString { return topic.Name.value(); });

    for (auto& topic : Message->Topics) {
        auto& topicName = topic.Name.value();

        if (DuplicateTopicNames.contains(topicName)) {
            continue;
        }

        if (topicName == "") {
            auto result = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
            result->Status = EKafkaErrors::INVALID_REQUEST;
            result->Message = "Empty topic name";
            this->TopicNamesToResponses[topicName] = TAutoPtr<TEvKafka::TEvTopicModificationResponse>(result.Release());
            continue;
        }

        std::optional<TString> retentionMs;
        std::optional<TString> retentionBytes;
        std::optional<ECleanupPolicy> cleanupPolicy;
        std::optional<TString> messageTimestampType;

        std::optional<THolder<TEvKafka::TEvTopicModificationResponse>> unsupportedConfigResponse;

        for (auto& config : topic.Configs) {
            unsupportedConfigResponse = ValidateTopicConfigName(config.Name.value());
            if (unsupportedConfigResponse.has_value()) {
                break;
            }
            if (config.Name.value() == RETENTION_MS_CONFIG_NAME) {
                retentionMs = config.Value;
            } else if (config.Name.value() == RETENTION_BYTES_CONFIG_NAME) {
                retentionBytes = config.Value;
            } else if (config.Name.value() == CLEANUP_POLICY) {
                unsupportedConfigResponse = ConvertCleanupPolicy(config.Value, cleanupPolicy);
                if (unsupportedConfigResponse.has_value()) {
                    break;
                }
            } else if (config.Name.value() == MESSAGE_TIMESTAMP_TYPE) {
                unsupportedConfigResponse = ConvertTimestampType(config.Value, messageTimestampType);
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

        TopicNamesToRetentions[topicName] = std::pair<std::optional<ui64>, std::optional<ui64>>(
            convertedRetentions.Ms,
            convertedRetentions.Bytes
        );

        Ydb::Topic::CreateTopicRequest request;
        request.set_path(topic.Name.value());
        request.mutable_partitioning_settings()->set_min_active_partitions(
            topic.NumPartitions == -1 ? NKikimr::AppData(ctx)->KafkaProxyConfig.GetTopicCreationDefaultPartitions() : topic.NumPartitions
        );
        if (convertedRetentions.Ms.has_value()) {
            request.mutable_retention_period()->set_seconds(convertedRetentions.Ms.value() / 1000);
        }
        if (convertedRetentions.Bytes.has_value()) {
            request.set_retention_storage_mb(convertedRetentions.Bytes.value() / 1_MB);
        }
        request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
        if (cleanupPolicy.has_value()) {
            request.mutable_attributes()->insert(
                {"_cleanup_policy", cleanupPolicy.value() == ECleanupPolicy::COMPACT ? "compact" : "delete"}
            );
        }
        if (messageTimestampType.has_value()) {
            request.mutable_attributes()->insert({"_timestamp_type", messageTimestampType.value()});
        }

        ctx.RegisterWithSameMailbox(NKikimr::NPQ::NSchema::CreateCreateTopicActor(SelfId(), NKikimr::NPQ::NSchema::TCreateTopicSettings{
            .Database = Context->DatabasePath,
            .Request = std::move(request),
            .UserToken = Context->UserToken,
            .IfNotExists = false,
        }));

        InflyTopics++;
    }

    if (InflyTopics > 0) {
        Become(&TKafkaCreateTopicsActor::StateWork);
    } else {
        Reply(ctx);
    }
};

void TKafkaCreateTopicsActor::Handle(const NKikimr::NPQ::NSchema::TEvCreateTopicResponse::TPtr& ev) {
    auto eventPtr = ev->Release();

    KAFKA_LOG_D(TStringBuilder() << "Create topics actor. Topic's " << eventPtr->Path << " response received." << std::to_string(eventPtr->Status));

    EKafkaErrors status;
    switch(eventPtr->Status) {
        case Ydb::StatusIds::SCHEME_ERROR:
            status = INVALID_REQUEST;
            break;
        case Ydb::StatusIds::ALREADY_EXISTS:
            status = TOPIC_ALREADY_EXISTS;
            break;
        default:
            status = ConvertErrorCode(eventPtr->Status);
    }

    auto response = MakeHolder<TEvKafka::TEvTopicModificationResponse>();
    response->TopicPath = eventPtr->Path;
    response->Status = status;
    response->Message = std::move(eventPtr->ErrorMessage);

    TopicNamesToResponses[eventPtr->Path] = TAutoPtr<TEvKafka::TEvTopicModificationResponse>(response.Release());
    InflyTopics--;
    if (InflyTopics == 0) {
        Reply(ActorContext());
    }
};

void TKafkaCreateTopicsActor::Reply(const TActorContext& ctx) {
    TCreateTopicsResponseData::TPtr response = std::make_shared<TCreateTopicsResponseData>();
    EKafkaErrors responseStatus = NONE_ERROR;

    for (auto& requestTopic : Message->Topics) {
        auto topicName = requestTopic.Name.value();

        if (!TopicNamesToResponses.contains(topicName)) {
            continue;
        }

        TCreateTopicsResponseData::TCreatableTopicResult responseTopic;
        responseTopic.Name = topicName;

        if (TopicNamesToResponses.contains(topicName)) {
            responseTopic.ErrorMessage = TopicNamesToResponses[topicName]->Message;
        }

        auto addConfigIfRequired = [this, &topicName, &responseTopic](std::optional<ui64> configValue, TString configName) {
            if (configValue.has_value()) {
                TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs config;
                config.Name = configName;
                config.Value = std::to_string(TopicNamesToRetentions[topicName].first.value());
                config.IsSensitive = false;
                config.ReadOnly = false;
                responseTopic.Configs.push_back(config);
            }
        };

        EKafkaErrors status = TopicNamesToResponses[topicName]->Status;
        if (status == EKafkaErrors::NONE_ERROR) {
            addConfigIfRequired(TopicNamesToRetentions[topicName].first, RETENTION_MS_CONFIG_NAME);
            addConfigIfRequired(TopicNamesToRetentions[topicName].second, RETENTION_BYTES_CONFIG_NAME);
        }
        responseTopic.ErrorCode = status;
        responseStatus = status;
        response->Topics.push_back(responseTopic);
    }

    for (auto& topicName : DuplicateTopicNames) {
        TCreateTopicsResponseData::TCreatableTopicResult responseTopic;
        responseTopic.Name = topicName;
        responseTopic.ErrorCode = INVALID_REQUEST;
        responseTopic.ErrorMessage = "Duplicate topic in request.";
        response->Topics.push_back(responseTopic);
        responseStatus = INVALID_REQUEST;
    }

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, response, responseStatus));

    Die(ctx);
};

TStringBuilder TKafkaCreateTopicsActor::InputLogMessage() {
    return InputLogMessage<TCreateTopicsRequestData::TCreatableTopic>(
            "Create topics actor",
            Message->Topics,
            Message->ValidateOnly != 0,
            [](TCreateTopicsRequestData::TCreatableTopic topic) -> TString {
                return topic.Name.value();
            });
};


void TKafkaCreateTopicsActor::ProcessValidateOnly(const NActors::TActorContext& ctx) {
    TCreateTopicsResponseData::TPtr response = std::make_shared<TCreateTopicsResponseData>();

    for (auto& requestTopic : Message->Topics) {
        auto topicName = requestTopic.Name.value();

        TCreateTopicsResponseData::TCreatableTopicResult responseTopic;
        responseTopic.Name = topicName;
        responseTopic.NumPartitions = requestTopic.NumPartitions;
        responseTopic.ErrorCode = NONE_ERROR;
        response->Topics.push_back(responseTopic);
    }

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, response, NONE_ERROR));
    Die(ctx);
};
}
