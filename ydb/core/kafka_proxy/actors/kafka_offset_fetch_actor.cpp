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
                                                       //Заменить на свои Request и Response
                                                       NKikimr::NGRpcProxy::V1::TGetPartitionsLocationRequest,
                                                       NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse>,
                         public NKikimr::NGRpcProxy::V1::TDescribeTopicActorImpl {
    using TBase = NKikimr::NGRpcProxy::V1::TPQInternalSchemaActor<TTopicOffsetActor,
        NKikimr::NGRpcProxy::V1::TGetPartitionsLocationRequest,
        NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse>;

    public:
    TTopicOffsetActor(std::shared_ptr<TSet<TString>> consumers,
            const NKikimr::NGRpcProxy::V1::TGetPartitionsLocationRequest& request,
            const TActorId& requester, 
            std::shared_ptr<TSet<ui32>> partitions)
        : TBase(request, requester)
        , TDescribeTopicActorImpl(ConsumerOffsetSettings(consumers, partitions))
        , Requester(requester)
        , TopicName(request.Topic)
        {
            Y_UNUSED(requester);
        };

    void Bootstrap(const NActors::TActorContext& ctx) override {
        Y_UNUSED(ctx);
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
        Y_UNUSED(error);
        Y_UNUSED(errorCode);
        Y_UNUSED(status);
        Y_UNUSED(ctx);

        THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
        response->TopicName = TopicName;
        response->Status = TEvKafka::TEvCommitedOffsetsResponse::EStatus::ERROR;
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
        if (!response.PQGroupInfo) {
            THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
            response->TopicName = TopicName;
            response->Status = TEvKafka::TEvCommitedOffsetsResponse::EStatus::UNKNOWN_TOPIC;
            Send(Requester, response.Release());
            TActorBootstrapped::PassAway();
            return;
        }

        const auto& pqDescr = response.PQGroupInfo->Description;
        ProcessTablets(pqDescr, ActorContext());
    }

    void Reply(const TActorContext& ctx) override {
        THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
        response->TopicName = TopicName;
        response->Status = TEvKafka::TEvCommitedOffsetsResponse::EStatus::OK;
        response->PartitionIdToOffsets = PartitionIdToOffsets;
        Send(Requester, response.Release());
        Die(ctx);
    };

    private:
        TActorId Requester;
        TString TopicName;
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
            if (UnknownTopics.contains(topicName)) {
                for (auto requestPartition: requestTopic.PartitionIndexes) {
                    TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                    partition.PartitionIndex = requestPartition;
                    partition.ErrorCode = UNKNOWN_TOPIC_OR_PARTITION;
                    topic.Partitions.push_back(partition);
                }
                group.Topics.push_back(topic);
                continue;
            }
            if (ErroredTopics.contains(topicName)) {
                for (auto requestPartition: requestTopic.PartitionIndexes) {
                    TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                    partition.PartitionIndex = requestPartition;
                    partition.ErrorCode = UNKNOWN_SERVER_ERROR;
                    topic.Partitions.push_back(partition);
                }
                group.Topics.push_back(topic);
                continue;
            }
            auto partitionsToOffsets = TopicToOffsets[topicName];
            for (auto requestPartition: requestTopic.PartitionIndexes) {
                TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                partition.PartitionIndex = requestPartition;
                if (partitionsToOffsets->contains(requestPartition) && (*partitionsToOffsets)[requestPartition].contains(requestGroup.GroupId.value())) {
                    partition.CommittedOffset = (*partitionsToOffsets)[requestPartition][requestGroup.GroupId.value()];
                    partition.ErrorCode = NONE_ERROR;
                } else {
                    partition.ErrorCode = RESOURCE_NOT_FOUND;
                }
                topic.Partitions.push_back(partition);
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
            }
            response->Topics.push_back(topic);
        }
    }
    return response;
}

void TKafkaOffsetFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    // If API level <= 7, Groups would be empty. In this case we convert message to level 8 and process it uniformely later
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
        NKikimr::NGRpcProxy::V1::TGetPartitionsLocationRequest locationRequest{};
        locationRequest.Topic = topicToEntities.first;
        locationRequest.Token = Context->UserToken->GetSerializedToken();
        locationRequest.Database = Context->DatabasePath;
        ctx.Register(new TTopicOffsetActor(
            topicToEntities.second.Consumers,
            locationRequest,
            SelfId(),
            topicToEntities.second.Partitions
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
    TopicToOffsets[ev->Get()->TopicName] = ev->Get()->PartitionIdToOffsets;
    if (ev->Get()->Status == TEvKafka::TEvCommitedOffsetsResponse::ERROR) {
        ErroredTopics.insert(ev->Get()->TopicName);
    } else if (ev->Get()->Status == TEvKafka::TEvCommitedOffsetsResponse::UNKNOWN_TOPIC) {
        UnknownTopics.insert(ev->Get()->TopicName);
    }
    if (InflyTopics == 0) {
        auto response = GetOffsetFetchResponse();
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
        Die(ctx);
    }
}

}
