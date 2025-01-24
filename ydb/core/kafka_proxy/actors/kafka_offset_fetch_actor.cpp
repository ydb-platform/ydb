#include "kafka_offset_fetch_actor.h"

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/kafka_proxy/kafka_events.h>

#include "ydb/services/persqueue_v1/actors/schema_actors.h"

namespace NKafka {

NKikimr::NGRpcProxy::V1::TDescribeTopicActorSettings ConsumerOffsetSettings(std::shared_ptr<TSet<TString>> consumers, std::shared_ptr<TSet<ui32>>& partitions) {
    NKikimr::NGRpcProxy::V1::TDescribeTopicActorSettings settings {
        NKikimr::NGRpcProxy::V1::TDescribeTopicActorSettings::EMode::DescribeConsumer,
        true,
        false,
        };
    settings.Consumers = *consumers;
    TVector<ui32> partitionsVector(partitions->begin(), partitions->end());
    settings.Partitions = partitionsVector;
    return settings;
};

struct PartitionOffsets {
    ui32 ReadOffset;
    ui32 WriteOffset;
};

class TTopicOffsetActor: public NKikimr::NGRpcProxy::V1::TPQInternalSchemaActor<TTopicOffsetActor,
                                                       NKikimr::NGRpcProxy::V1::TLocalRequestBase,
                                                       NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse>,
                         public NKikimr::NGRpcProxy::V1::TDescribeTopicActorImpl,
                         public NKikimr::NGRpcProxy::V1::TCdcStreamCompatible {
    using TBase = NKikimr::NGRpcProxy::V1::TPQInternalSchemaActor<TTopicOffsetActor,
        NKikimr::NGRpcProxy::V1::TLocalRequestBase,
        NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse>;

    public:
    TTopicOffsetActor(std::shared_ptr<TSet<TString>> consumers,
            const NKikimr::NGRpcProxy::V1::TLocalRequestBase& request,
            const TActorId& requester,
            std::shared_ptr<TSet<ui32>> partitions,
            const TString& originalTopicName,
            const TString& userSID)
        : TBase(request, requester)
        , TDescribeTopicActorImpl(ConsumerOffsetSettings(consumers, partitions))
        , Requester(requester)
        , OriginalTopicName(originalTopicName)
        , UserSID(userSID)
        {
        };

    void Bootstrap(const NActors::TActorContext& ctx) override {
        Y_UNUSED(ctx);
        KAFKA_LOG_D("TopicOffsetActor: Get commited offsets for topic '" << OriginalTopicName
            << "' for user '" << UserSID << "'");
        SendDescribeProposeRequest();
        Become(&TTopicOffsetActor::StateWork);
    };

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
            default:
                if (!TDescribeTopicActorImpl::StateWork(ev, ActorContext())) {
                    TBase::StateWork(ev);
                };
        }
    }

    void RaiseError(const TString& error,
            const Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
            const Ydb::StatusIds::StatusCode status,
            const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        KAFKA_LOG_D("TopicOffsetActor: error raised for '" << OriginalTopicName << "'"
            << " for user '" << UserSID << "'."
            << " Error: '" << error << "',"
            << " ErrorCode: '" << static_cast<int>(errorCode) << "',"
            << " StatusCode: '" << status<< "'.");

        THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
        response->TopicName = OriginalTopicName;
        response->Status = ConvertErrorCode(status);

        Send(Requester, response.Release());
        Die(ctx);
    }

    void ApplyResponse(TTabletInfo& tabletInfo,
                       NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev,
                       const TActorContext& ctx) override{
        Y_UNUSED(tabletInfo);
        Y_UNUSED(ctx);
        for (auto& partResult : ev->Get()->Record.GetPartResult()) {
            std::unordered_map<TString, ui32> consumerToOffset;
            for (auto& consumerResult : partResult.GetConsumerResult()) {
                if (consumerResult.GetErrorCode() == NPersQueue::NErrorCode::OK) {
                    consumerToOffset[consumerResult.GetConsumer()] = consumerResult.GetCommitedOffset();
                }
            }
            (*PartitionIdToOffsets)[partResult.GetPartition()] = consumerToOffset;
        }
    };

    // Noop
    void ApplyResponse(TTabletInfo& tabletInfo,
            NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev,
            const TActorContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(tabletInfo);
        Y_UNUSED(ev);
    };

    // Should never be called
    bool ApplyResponse(NKikimr::TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev,
                       const TActorContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(ev);
        Y_ABORT();
    };

    void HandleCacheNavigateResponse(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override {
        const auto& response = ev->Get()->Request.Get()->ResultSet.front();
        KAFKA_LOG_D("TopicOffsetActor: TEvNavigateKeySetResult recieved for topic '" << OriginalTopicName
            << "' for user '" << UserSID << "'. PQGroupInfo is present: " << (response.PQGroupInfo.Get() != nullptr));
        if (!response.PQGroupInfo) {
            THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
            response->TopicName = OriginalTopicName;
            response->Status = UNKNOWN_TOPIC_OR_PARTITION;
            Send(Requester, response.Release());
            TActorBootstrapped::PassAway();
            return;
        }

        const auto& pqDescr = response.PQGroupInfo->Description;
        ProcessTablets(pqDescr, ActorContext());
    }

    void Reply(const TActorContext& ctx) override {
        KAFKA_LOG_D("TopicOffsetActor: replying for topic '" << OriginalTopicName
            << "' for user '" << UserSID << "'" << " with status NONE_ERROR");
        THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
        response->TopicName = OriginalTopicName;
        response->Status = NONE_ERROR;
        response->PartitionIdToOffsets = PartitionIdToOffsets;
        Send(Requester, response.Release());
        Die(ctx);
    };

    private:
        const TActorId Requester;
        const TString OriginalTopicName;
        const TString UserSID;
        std::unordered_map<ui32, ui32> PartitionIdToOffset {};
        std::shared_ptr<std::unordered_map<ui32, std::unordered_map<TString, ui32>>> PartitionIdToOffsets = std::make_shared<std::unordered_map<ui32, std::unordered_map<TString, ui32>>>();
};

NActors::IActor* CreateKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message) {
    return new TKafkaOffsetFetchActor(context, correlationId, message);
}

TOffsetFetchResponseData::TPtr TKafkaOffsetFetchActor::GetOffsetFetchResponse() {
    TOffsetFetchResponseData::TPtr response = std::make_shared<TOffsetFetchResponseData>();
    for (const auto& requestGroup : Message->Groups) {
        TOffsetFetchResponseData::TOffsetFetchResponseGroup group;
        group.GroupId = requestGroup.GroupId.value();
        for (const auto& requestTopic: requestGroup.Topics) {
            TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics topic;
            TString topicName = requestTopic.Name.value();
            topic.Name = topicName;
            if (TopicsToResponses[topicName]->Status == NONE_ERROR) {
                auto partitionsToOffsets = TopicsToResponses[topicName]->PartitionIdToOffsets;
                for (auto requestPartition: requestTopic.PartitionIndexes) {
                    TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                    partition.PartitionIndex = requestPartition;
                    if (partitionsToOffsets.get() != nullptr
                            && partitionsToOffsets->contains(requestPartition)
                            && (*partitionsToOffsets)[requestPartition].contains(requestGroup.GroupId.value())) {
                        partition.CommittedOffset = (*partitionsToOffsets)[requestPartition][requestGroup.GroupId.value()];
                        partition.ErrorCode = NONE_ERROR;
                    } else {
                        partition.ErrorCode = RESOURCE_NOT_FOUND;
                    }
                    topic.Partitions.push_back(partition);
                }
            } else {
                for (auto requestPartition: requestTopic.PartitionIndexes) {
                    TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                    partition.PartitionIndex = requestPartition;
                    partition.ErrorCode = TopicsToResponses[topicName]->Status;
                    topic.Partitions.push_back(partition);
                }
            }
            group.Topics.push_back(topic);
        }
        response->Groups.push_back(group);
    }
    if (response->Groups.size() == 1) {
        for (const auto& sourceTopic: response->Groups[0].Topics) {
            NKafka::TOffsetFetchResponseData::TOffsetFetchResponseTopic topic;
            topic.Name = sourceTopic.Name;
            for (const auto& sourcePartition: sourceTopic.Partitions) {
                NKafka::TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition partition;
                partition.CommittedOffset = sourcePartition.CommittedOffset;
                partition.PartitionIndex = sourcePartition.PartitionIndex;
                partition.ErrorCode = sourcePartition.ErrorCode;
                topic.Partitions.push_back(partition);
            }
            response->Topics.push_back(topic);
        }
    }
    return response;
}

void TKafkaOffsetFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    // If API level <= 7, Groups would be empty. In this case we convert message to level 8 and process it uniformely later
    KAFKA_LOG_D("TopicOffsetActor: new request for user " << GetUsernameOrAnonymous(Context));
    if (Message->Groups.empty()) {
        TOffsetFetchRequestData::TOffsetFetchRequestGroup group;
        group.GroupId = Message->GroupId.value();

        for (const auto& sourceTopic: Message->Topics) {
            TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic;
            topic.Name = sourceTopic.Name;
            topic.PartitionIndexes = sourceTopic.PartitionIndexes;
            group.Topics.push_back(topic);
        }
        Message->Groups.push_back(group);
    }

    for (const auto& group : Message->Groups) {
        for (const auto& topic: group.Topics) {
            ExtractPartitions(group.GroupId.value(), topic);
        }
    }

    for (const auto& topicToEntities : TopicToEntities) {
        NKikimr::NGRpcProxy::V1::TLocalRequestBase locationRequest{
            NormalizePath(Context->DatabasePath, topicToEntities.first),
            Context->DatabasePath,
            GetUserSerializedToken(Context),
        };
        ctx.Register(new TTopicOffsetActor(
            topicToEntities.second.Consumers,
            locationRequest,
            SelfId(),
            topicToEntities.second.Partitions,
            topicToEntities.first,
            GetUsernameOrAnonymous(Context)
        ));
        InflyTopics++;
    }
    Become(&TKafkaOffsetFetchActor::StateWork);
}


void TKafkaOffsetFetchActor::ExtractPartitions(const TString& group, const NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics& topic) {
    TString topicName = topic.Name.value();
    if (!TopicToEntities.contains(topicName)) {
        TopicEntities newEntities;
        TopicToEntities[topicName] = newEntities;
    }
    TopicEntities& entities = TopicToEntities[topicName];
    entities.Consumers->insert(group);
    for (auto partition: topic.PartitionIndexes) {
        entities.Partitions->insert(partition);
    }
};

void TKafkaOffsetFetchActor::Handle(TEvKafka::TEvCommitedOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
    InflyTopics--;

    auto eventPtr = ev->Release();
    TopicsToResponses[eventPtr->TopicName] = eventPtr;

    if (InflyTopics == 0) {
        auto response = GetOffsetFetchResponse();
        KAFKA_LOG_D("TopicOffsetActor: sending response to user " << GetUsernameOrAnonymous(Context));
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
        Die(ctx);
    }
}

}
