#include "kafka_create_topics_actor.h"

#include "control_plane_common.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>


namespace NKafka {

class TCreateTopicActor : public NKikimr::NGRpcProxy::V1::TPQGrpcSchemaBase<TCreateTopicActor, TKafkaTopicModificationRequest> {
    using TBase = NKikimr::NGRpcProxy::V1::TPQGrpcSchemaBase<TCreateTopicActor, TKafkaTopicModificationRequest>;
public:

    TCreateTopicActor(
            TActorId requester,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName,
            ui32 partitionsNumber,
            std::optional<ui64> retentionMs,
            std::optional<ui64> retentionBytes)
        : TBase(new TKafkaTopicModificationRequest(
            userToken,
            topicPath,
            databaseName,
            [this](EKafkaErrors status, const TString& message) {
                this->SendResult(status, message);
            })
        )
        , Requester(requester)
        , TopicPath(topicPath)
        , PartionsNumber(partitionsNumber)
        , RetentionMs(retentionMs)
        , RetentionBytes(retentionBytes)
    {
        KAFKA_LOG_D(LogMessage(databaseName));
    };

    ~TCreateTopicActor() = default;

    void SendResult(EKafkaErrors status, const TString& message) {
        THolder<TEvKafka::TEvTopicModificationResponse> response(new TEvKafka::TEvTopicModificationResponse());
        response->Status = status;
        response->TopicPath = TopicPath;
        response->Message = message;
        Send(Requester, response.Release());
        Send(SelfId(), new TEvents::TEvPoison());
    }

    void FillProposeRequest(
            NKikimr::TEvTxUserProxy::TEvProposeTransaction &proposal,
            const TActorContext &ctx,
            const TString &workingDir,
            const TString &name
    ) {
        NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
        modifyScheme.SetWorkingDir(workingDir);

        auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();
        pqDescr->SetPartitionPerTablet(1);

        Ydb::Topic::CreateTopicRequest topicRequest;
        topicRequest.mutable_partitioning_settings()->set_min_active_partitions(PartionsNumber);
        if (RetentionMs.has_value()) {
            topicRequest.mutable_retention_period()->set_seconds(RetentionMs.value() / 1000);
        }
        if (RetentionBytes.has_value()) {
            topicRequest.set_retention_storage_mb(RetentionBytes.value() / 1_MB);
        }
        topicRequest.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);

        TString error;
        TYdbPqCodes codes = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(
                name,
                topicRequest,
                modifyScheme,
                NKikimr::AppData(ctx),
                error,
                workingDir,
                proposal.Record.GetDatabaseName()
        );
        if (codes.YdbCode != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(codes.YdbCode, codes.PQCode, error);
        }
    };

    void Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendProposeRequest(ctx);
        Become(&TCreateTopicActor::StateWork);
    };

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult, TActorBase::Handle);
        default: TBase::StateWork(ev);
        }
    }

    void HandleCacheNavigateResponse(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }

private:
    const TActorId Requester;
    const TString TopicPath;
    const std::shared_ptr<TString> SerializedToken;
    const ui32 PartionsNumber;
    std::optional<ui64> RetentionMs;
    std::optional<ui64> RetentionBytes;

    TStringBuilder LogMessage(TString& databaseName) {
        TStringBuilder stringBuilder = TStringBuilder()
            << "Create topics actor. DatabaseName: " << databaseName
            << ". TopicPath: " << TopicPath
            << ". PartitionsNumber: " << PartionsNumber;
        if (RetentionMs.has_value()) {
            stringBuilder << ". RetentionMs: " << RetentionMs.value();
        }
        if (RetentionBytes.has_value()) {
            stringBuilder << ". RetentionBytes: " << RetentionBytes.value();
        }
        return stringBuilder;
    }
};

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

        ctx.Register(new TCreateTopicActor(
            SelfId(),
            Context->UserToken,
            topic.Name.value(),
            Context->DatabasePath,
            topic.NumPartitions,
            convertedRetentions.Ms,
            convertedRetentions.Bytes
        ));

        InflyTopics++;
    }

    if (InflyTopics > 0) {
        Become(&TKafkaCreateTopicsActor::StateWork);
    } else {
        Reply(ctx);
    }
};

void TKafkaCreateTopicsActor::Handle(const TEvKafka::TEvTopicModificationResponse::TPtr& ev, const TActorContext& ctx) {
    auto eventPtr = ev->Release();
    KAFKA_LOG_D(TStringBuilder() << "Create topics actor. Topic's " << eventPtr->TopicPath << " response received." << std::to_string(eventPtr->Status));
    TopicNamesToResponses[eventPtr->TopicPath] = eventPtr;
    InflyTopics--;
    if (InflyTopics == 0) {
        Reply(ctx);
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
