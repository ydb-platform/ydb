#include "kafka_describe_configs_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>


namespace NKafka {

NActors::IActor* CreateKafkaDescribeConfigsActor(
        const TContext::TPtr context,
        const ui64 correlationId,
        const TMessagePtr<TDescribeConfigsRequestData>& message
) {
    return new TKafkaDescribeConfigsActor(context, correlationId, message);
}

// https://github.com/apache/kafka/blob/b9774c0b025d4eef025c12af9c264eff1e98410c/clients/src/main/java/org/apache/kafka/common/requests/DescribeConfigsResponse.java#L150
enum class EKafkaConfigType {
    UNKNOWN = 0,
    BOOLEAN = 1,
    STRING = 2,
    INT = 3,
    SHORT = 4,
    LONG = 5,
    DOUBLE = 6,
    LIST = 7,
    CLASS = 8,
    PASSWORD = 9
};

TKafkaDescribeTopicActor::TKafkaDescribeTopicActor(
        TActorId requester,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        TString topicPath,
        TString databaseName)
    : TBase(new TDescribeConfigsRequest(
        userToken,
        topicPath,
        databaseName,
        [this](const EKafkaErrors status, const std::string& message, const google::protobuf::Message& result) {
            this->SendResult(status,TString{message}, result);
        })
    )
    , TopicPath(topicPath)
    , Requester(requester)
{
};

void TKafkaDescribeTopicActor::SendResult(const EKafkaErrors status, const TString& message, const google::protobuf::Message& result) {
    THolder<TEvKafka::TEvTopicDescribeResponse> response(new TEvKafka::TEvTopicDescribeResponse());
    response->Status = status;
    response->TopicPath = TopicPath;
    response->Message = message;
    if (status == EKafkaErrors::NONE_ERROR) {
        const auto* protoResponse = dynamic_cast<const Ydb::Topic::DescribeTopicResult*>(&result);
        response->Response = *protoResponse;
        response->PQGroupInfo = PQGroupInfo;
    }
    TBase::Send(Requester, response.Release());
    TBase::Send(TBase::SelfId(), new TEvents::TEvPoison());
}

void TKafkaDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    return TThis::TActorBase::StateWork(ev);
}

void TKafkaDescribeTopicActor::Bootstrap(const NActors::TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    TBase::SendDescribeProposeRequest(ctx);
    TBase::Become(&TThis::StateWork);
};

void TKafkaDescribeTopicActor::HandleCacheNavigateResponse(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev)) {
        return;
    }
    Ydb::Topic::DescribeTopicResult result;
    const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    const TString path = JoinSeq("/", response.Path);

    if (response.PQGroupInfo) {
        const auto& pqDescr = response.PQGroupInfo->Description;
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!NKikimr::FillTopicDescription(result, pqDescr, response.Self->Info, GetCdcStreamName(), status, error)) {
            this->Request_->RaiseIssue(NKikimr::NGRpcProxy::V1::FillIssue(error, Ydb::PersQueue::ErrorCode::ERROR));
            TBase::Reply(status, ActorContext());
            return;
        }
        PQGroupInfo = response.PQGroupInfo;

    } else {
        Ydb::Scheme::Entry *selfEntry = result.mutable_self();
        NKikimr::ConvertDirectoryEntry(response.Self->Info, selfEntry, true);
        if (const auto& name = GetCdcStreamName()) {
            selfEntry->set_name(*name);
        }
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
}

void TKafkaDescribeConfigsActor::Bootstrap(const NActors::TActorContext& ctx) {

    KAFKA_LOG_D(InputLogMessage());


    THashSet<TString> requestedTopics{};
    for (auto& resource : Message->Resources) {
        auto& topicName = resource.ResourceName.value();
        if (requestedTopics.contains(topicName)) {
            continue;
        }
        requestedTopics.insert(topicName);
        if (resource.ResourceType != TOPIC_RESOURCE_TYPE) {
            auto result = MakeHolder<TEvKafka::TEvTopicDescribeResponse>();
            result->TopicPath = topicName;
            result->Status = EKafkaErrors::INVALID_REQUEST;
            result->Message = "Only TOPIC resource type is supported.";
            this->TopicNamesToResponses[resource.ResourceName.value()] = TAutoPtr<TEvKafka::TEvTopicDescribeResponse>(result.Release());
            continue;
        }

        ctx.Register(new TKafkaDescribeTopicActor(
            SelfId(),
            Context->UserToken,
            resource.ResourceName.value(),
            Context->DatabasePath
        ));
        InflyTopics++;
    }

    if (InflyTopics > 0) {
        Become(&TKafkaDescribeConfigsActor::StateWork);
    } else {
        Reply();
    }
};

void TKafkaDescribeConfigsActor::Handle(const TEvKafka::TEvTopicDescribeResponse::TPtr& ev) {
    auto eventPtr = ev->Release();
    TopicNamesToResponses[eventPtr->TopicPath] = eventPtr;
    InflyTopics--;
    if (InflyTopics == 0) {
        Reply();
    }
};

void AddConfigEntry(
        TDescribeConfigsResponseData::TDescribeConfigsResult& descrResult, const TString& name, const TString& value,
        EKafkaConfigType type
) {
    TDescribeConfigsResponseData::TDescribeConfigsResult::ConfigsMeta::ItemType configEntry;
    configEntry.Name = name;
    configEntry.Value = value;
    configEntry.ConfigType = (ui32)type;
    configEntry.ConfigSource = EConfigSource::DEFAULT_CONFIG;
    descrResult.Configs.emplace_back(std::move(configEntry));
}


void TKafkaDescribeConfigsActor::AddDescribeResponse(
        TDescribeConfigsResponseData::TPtr& response, TEvKafka::TEvTopicDescribeResponse* ev,
        const TString& topicName, EKafkaErrors status, const TString& message
) {
    // https://kafka.apache.org/documentation/#configuration
    //TDescribeConfigsResponseData::TDescribeConfigsResult result;
    TDescribeConfigsResponseData::ResultsMeta::ItemType singleConfig;
    singleConfig.ResourceType = TOPIC_RESOURCE_TYPE;
    singleConfig.ResourceName = topicName;
    singleConfig.ErrorCode = status;
    singleConfig.ErrorMessage = message;

    if (status != EKafkaErrors::NONE_ERROR) {
        response->Results.emplace_back(std::move(singleConfig));
        return;
    }
    Y_ENSURE(ev != nullptr);
    AddConfigEntry(singleConfig, "compression.type", "producer", EKafkaConfigType::STRING);

    // these are default
    AddConfigEntry(singleConfig, "leader.replication.throttled.replicas", "", EKafkaConfigType::LIST);
    AddConfigEntry(singleConfig, "remote.storage.enable", "false", EKafkaConfigType::BOOLEAN);
    AddConfigEntry(singleConfig, "segment.jitter.ms", "0", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "local.retention.ms", "-2", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "flush.ms", "9223372036854775807", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "follower.replication.throttled.replicas", "", EKafkaConfigType::LIST);
    AddConfigEntry(singleConfig, "compression.lz4.level", "9", EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "compression.gzip.level", "-1", EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "compression.zstd.level", "3", EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "max.compaction.lag.ms", "9223372036854775807", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "min.compaction.lag.ms", "0", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "local.retention.bytes", "-2", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "preallocate", "false", EKafkaConfigType::BOOLEAN);
    AddConfigEntry(singleConfig, "min.cleanable.dirty.ratio", "0.5", EKafkaConfigType::DOUBLE);
    AddConfigEntry(singleConfig, "index.interval.bytes", "4096", EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "unclean.leader.election.enable", "false", EKafkaConfigType::BOOLEAN);
    AddConfigEntry(singleConfig, "delete.retention.ms", "86400000", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "segment.ms", "604800000", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "message.timestamp.before.max.ms", "9223372036854775807", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "segment.index.bytes", "10485760", EKafkaConfigType::INT);
    // !: non-default
    AddConfigEntry(singleConfig, "message.downconversion.enable", "false", EKafkaConfigType::BOOLEAN);
    AddConfigEntry(singleConfig, "min.insync.replicas", "3", EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "segment.bytes", "8388608" /*8_MB*/, EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "flush.messages", "1", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "message.format.version", "9", EKafkaConfigType::STRING);
    AddConfigEntry(singleConfig, "file.delete.delay.ms", "0", EKafkaConfigType::LONG);
    AddConfigEntry(singleConfig, "max.message.bytes", ToString(Context->Config.GetMaxMessageSize()), EKafkaConfigType::INT);
    AddConfigEntry(singleConfig, "message.timestamp.type", "CreateTime", EKafkaConfigType::STRING);
    AddConfigEntry(singleConfig, "message.timestamp.after.max.ms", "9223372036854775807", EKafkaConfigType::LONG);


    auto retentionTime = ev->Response.retention_period().seconds();
    if (retentionTime > 0) {
        AddConfigEntry(singleConfig, "retention.ms", ToString(retentionTime * 1000), EKafkaConfigType::LONG);
    } else {
        AddConfigEntry(singleConfig, "retention.ms", "-1", EKafkaConfigType::LONG);
    }
    auto retentionBytes = ev->Response.retention_storage_mb();
    if (retentionBytes > 0) {
        AddConfigEntry(singleConfig, "retention.bytes", ToString(retentionBytes * 1024 * 1024), EKafkaConfigType::LONG);
    } else {
        AddConfigEntry(singleConfig, "retention.bytes", "-1", EKafkaConfigType::LONG);
    }
    if (ev->PQGroupInfo && ev->PQGroupInfo->Description.GetPQTabletConfig().GetEnableCompactification()) {
        AddConfigEntry(singleConfig, "cleanup.policy", "compact", EKafkaConfigType::LIST);
    } else {
        AddConfigEntry(singleConfig, "cleanup.policy", "delete", EKafkaConfigType::LIST);
    }

    response->Results.emplace_back(std::move(singleConfig));
}

void TKafkaDescribeConfigsActor::Reply() {
    TDescribeConfigsResponseData::TPtr response = std::make_shared<TDescribeConfigsResponseData>();
    EKafkaErrors responseStatus = NONE_ERROR;
    for (auto& requestResource : Message->Resources) {
        auto resourceName = requestResource.ResourceName.value();

        auto topicRespIter = TopicNamesToResponses.find(resourceName);
        if (topicRespIter == TopicNamesToResponses.end()) {
            continue;
        }

        auto& topicResult = topicRespIter->second;
        EKafkaErrors status = topicResult->Status;
        AddDescribeResponse(response, topicResult.Get(), resourceName, status, topicResult->Message);
        responseStatus = status;
        TopicNamesToResponses.erase(topicRespIter);
    }

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, responseStatus));
    Die(ActorContext());
};

TStringBuilder TKafkaDescribeConfigsActor::InputLogMessage() {
    return InputLogMessage<TDescribeConfigsRequestData::TDescribeConfigsResource>(
            "Describe configs actor",
            Message->Resources,
            false,
            [](TDescribeConfigsRequestData::TDescribeConfigsResource resource) -> TString {
                return resource.ResourceName.value();
            });
};

}
