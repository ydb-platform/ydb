#include "kafka_metadata_actor.h"
#include <ydb/services/persqueue_v1/actors/schema_actors.h>

namespace NKafka {
using namespace NKikimr::NGRpcProxy::V1;

void TKafkaMetadataActor::Bootstrap(const TActorContext& ctx) {
    Response->Topics.resize(Message->Topics.size());
    THashMap<TString, TActorId> partitionActors;
    for (auto i = 0u; i < Message->Topics.size(); ++i) {
        Response->Topics[i] = TMetadataResponseData::TMetadataResponseTopic{};
        auto& reqTopic = Message->Topics[i];
        Response->Topics[i].Name = reqTopic.Name.value_or("");

        if (!reqTopic.Name.value_or("")) {
            AddTopicError(Response->Topics[i], EKafkaErrors::INVALID_TOPIC_EXCEPTION);
            continue;
        }
        const auto& topicName = reqTopic.Name.value();
        TActorId child;
        auto namesIter = partitionActors.find(topicName);
        if (namesIter.IsEnd()) {
            child = SendTopicRequest(reqTopic);
        } else {
            child = namesIter->second;
        }
        TopicIndexes[child].push_back(i);
    }
    Become(&TKafkaMetadataActor::StateWork);
    RespondIfRequired(ctx);
}

TActorId TKafkaMetadataActor::SendTopicRequest(const TMetadataRequestData::TMetadataRequestTopic& topicRequest) {
    TGetPartitionsLocationRequest locationRequest{};
    locationRequest.Topic = topicRequest.Name.value();
    //ToDo: Get database?
    //ToDo: Authorization?
    PendingResponses++;
    return Register(new TPartitionsLocationActor(locationRequest, SelfId()));

} 

void TKafkaMetadataActor::AddTopicError(
    TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode
) {
    topic.ErrorCode = errorCode;
}

void TKafkaMetadataActor::AddTopicResponse(TMetadataResponseData::TMetadataResponseTopic& topic, TEvLocationResponse* response) {
    topic.ErrorCode = NONE_ERROR;
    topic.Partitions.reserve(response->Partitions.size());
    for (const auto& part : response->Partitions) {
        TMetadataResponseData::TMetadataResponseTopic::PartitionsMeta::ItemType responsePartition;
        responsePartition.PartitionIndex = part.PartitionId;
        responsePartition.ErrorCode = NONE_ERROR;
        responsePartition.LeaderEpoch = part.Generation;
        responsePartition.ReplicaNodes.push_back(part.NodeId);
        auto ins = AllClusterNodes.insert(part.NodeId);
        if (ins.second) {
            auto broker = TMetadataResponseData::TMetadataResponseBroker{};
            broker.NodeId = part.NodeId;
            broker.Host = part.Hostname;
            Response->Brokers.emplace_back(std::move(broker));
        }
        topic.Partitions.emplace_back(std::move(responsePartition));
    }
}

void TKafkaMetadataActor::HandleResponse(TEvLocationResponse::TPtr ev, const TActorContext& ctx) {
    --PendingResponses;
    
    auto actorIter = TopicIndexes.find(ev->Sender);

    Y_VERIFY_DEBUG(!actorIter.IsEnd()); 
    Y_VERIFY_DEBUG(!actorIter->second.empty());

    if (actorIter.IsEnd()) {
        LOG_CRIT_S(ctx, NKikimrServices::KAFKA_PROXY,
                   "Metadata actor: got unexpected location response, ignoring. Expect malformed/incompled reply");
        return RespondIfRequired(ctx);


    }
    if (actorIter->second.empty()) {
        LOG_CRIT_S(ctx, NKikimrServices::KAFKA_PROXY,
                   "Metadata actor: corrupted state (empty actorId in mapping). Ignored location response, expect incomplete reply");

        return RespondIfRequired(ctx);
    }
    
    //ToDo: Log and proceed on bad iter
    for (auto index : actorIter->second) {
        switch (ev->Get()->Status) {
            case Ydb::StatusIds::BAD_REQUEST:
                AddTopicError(Response->Topics[index], EKafkaErrors::INVALID_REQUEST);
                break;
            case Ydb::StatusIds::SCHEME_ERROR:
                AddTopicError(Response->Topics[index], EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
                break;
            case Ydb::StatusIds::SUCCESS:
                AddTopicResponse(Response->Topics[index], ev->Get());
                break;
            case Ydb::StatusIds::INTERNAL_ERROR:
            default:
                AddTopicError(Response->Topics[index], EKafkaErrors::UNKNOWN_SERVER_ERROR);
                break;
            
        }
    }
    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::RespondIfRequired(const TActorContext& ctx) {
    if (--PendingResponses == 0) {
        Send(Parent, new TEvKafka::TEvResponse(Cookie, Response));
        Die(ctx);
    }
}
} // namespace NKafka