#include "kafka_metadata_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>

namespace NKafka {
using namespace NKikimr::NGRpcProxy::V1;

static constexpr int ProxyNodeId = 1;

NActors::IActor* CreateKafkaMetadataActor(const TContext::TPtr context,
                                          const ui64 correlationId,
                                          const TMessagePtr<TMetadataRequestData>& message) {
    return new TKafkaMetadataActor(context, correlationId, message);
}

void TKafkaMetadataActor::Bootstrap(const TActorContext& ctx) {
    Response->Topics.resize(Message->Topics.size());

    THashMap<TString, TActorId> partitionActors;
    for (size_t i = 0; i < Message->Topics.size(); ++i) {
        Response->Topics[i] = TMetadataResponseData::TMetadataResponseTopic{};
        auto& reqTopic = Message->Topics[i];
        Response->Topics[i].Name = reqTopic.Name.value_or("");
        Response->ClusterId = "ydb-cluster";
        Response->ControllerId = 1;

        if (!reqTopic.Name.value_or("")) {
            AddTopicError(Response->Topics[i], EKafkaErrors::INVALID_TOPIC_EXCEPTION);
            continue;
        }
        const auto& topicName = reqTopic.Name.value();
        TActorId child;
        auto namesIter = partitionActors.find(topicName);
        if (namesIter.IsEnd()) {
            child = SendTopicRequest(reqTopic);
            partitionActors[topicName] = child;
        } else {
            child = namesIter->second;
        }
        TopicIndexes[child].push_back(i);
    }
    Become(&TKafkaMetadataActor::StateWork);

    RespondIfRequired(ctx);
}

TActorId TKafkaMetadataActor::SendTopicRequest(const TMetadataRequestData::TMetadataRequestTopic& topicRequest) {
    KAFKA_LOG_D("Describe partitions locations for topic '" << *topicRequest.Name << "' for user '" << Context->UserToken->GetUserSID() << "'");

    TGetPartitionsLocationRequest locationRequest{};
    locationRequest.Topic = topicRequest.Name.value();
    locationRequest.Token = Context->UserToken->GetSerializedToken();
    locationRequest.Database = Context->DatabasePath;

    PendingResponses++;

    return Register(new TPartitionsLocationActor(locationRequest, SelfId()));
} 

void TKafkaMetadataActor::AddTopicError(
    TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode
) {
    topic.ErrorCode = errorCode;
    ErrorCode = errorCode;
}

void TKafkaMetadataActor::AddTopicResponse(TMetadataResponseData::TMetadataResponseTopic& topic, TEvLocationResponse* response) {
    bool withProxy = Context->Config.HasProxy() && !Context->Config.GetProxy().GetHostname().Empty();

    topic.ErrorCode = NONE_ERROR;
    topic.TopicId = TKafkaUuid(response->SchemeShardId, response->PathId);
    if (withProxy) {
        auto broker = TMetadataResponseData::TMetadataResponseBroker{};
        broker.NodeId = ProxyNodeId;
        broker.Host = Context->Config.GetProxy().GetHostname();
        broker.Port = Context->Config.GetProxy().GetPort();
        Response->Brokers.emplace_back(std::move(broker));
    }

    topic.Partitions.reserve(response->Partitions.size());
    for (const auto& part : response->Partitions) {
        auto nodeId = withProxy ? ProxyNodeId : part.NodeId;

        TMetadataResponseData::TMetadataResponseTopic::PartitionsMeta::ItemType responsePartition;
        responsePartition.PartitionIndex = part.PartitionId;
        responsePartition.ErrorCode = NONE_ERROR;
        responsePartition.LeaderId = nodeId;
        responsePartition.LeaderEpoch = part.Generation;
        responsePartition.ReplicaNodes.push_back(nodeId);
        responsePartition.IsrNodes.push_back(nodeId);

        topic.Partitions.emplace_back(std::move(responsePartition));

        if (!withProxy) {
            auto ins = AllClusterNodes.insert(part.NodeId);
            if (ins.second) {
                auto broker = TMetadataResponseData::TMetadataResponseBroker{};
                broker.NodeId = part.NodeId;
                broker.Host = part.Hostname;
                broker.Port = Context->Config.GetListeningPort();
                Response->Brokers.emplace_back(std::move(broker));
            }
        }
    }
}

void TKafkaMetadataActor::HandleResponse(TEvLocationResponse::TPtr ev, const TActorContext& ctx) {
    --PendingResponses;

    auto* r = ev->Get();
    auto actorIter = TopicIndexes.find(ev->Sender);

    Y_VERIFY_DEBUG(!actorIter.IsEnd()); 
    Y_VERIFY_DEBUG(!actorIter->second.empty());

    if (actorIter.IsEnd()) {
        KAFKA_LOG_CRIT("Metadata actor: got unexpected location response, ignoring. Expect malformed/incompled reply");
        return RespondIfRequired(ctx);
    }

    if (actorIter->second.empty()) {
        KAFKA_LOG_CRIT("Metadata actor: corrupted state (empty actorId in mapping). Ignored location response, expect incomplete reply");

        return RespondIfRequired(ctx);
    }
    
    for (auto index : actorIter->second) {
        auto& topic = Response->Topics[index];
        if (r->Status == Ydb::StatusIds::SUCCESS) {
            KAFKA_LOG_D("Describe topic '" << topic.Name << "' location finishied successful");
            AddTopicResponse(topic, r);
        } else {
            KAFKA_LOG_ERROR("Describe topic '" << topic.Name << "' location finishied with error: Code=" << r->Status << ", Issues=" << r->Issues.ToOneLineString());
            AddTopicError(topic, ConvertErrorCode(r->Status));
        }
    }

    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::RespondIfRequired(const TActorContext& ctx) {
    if (PendingResponses == 0) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, ErrorCode));
        Die(ctx);
    }
}

TString TKafkaMetadataActor::LogPrefix() const {
    return TStringBuilder() << "TKafkaMetadataActor " << SelfId() << " ";
}

} // namespace NKafka
