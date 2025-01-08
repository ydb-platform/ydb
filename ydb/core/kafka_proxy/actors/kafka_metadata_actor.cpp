#include "kafka_metadata_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/core/grpc_services/grpc_endpoint.h>
#include <ydb/core/base/statestorage.h>

namespace NKafka {
using namespace NKikimr;
using namespace NKikimr::NGRpcProxy::V1;

NActors::IActor* CreateKafkaMetadataActor(const TContext::TPtr context,
                                          const ui64 correlationId,
                                          const TMessagePtr<TMetadataRequestData>& message) {
    return new TKafkaMetadataActor(context, correlationId, message);
}

void TKafkaMetadataActor::Bootstrap(const TActorContext& ctx) {
    Response->Topics.resize(Message->Topics.size());
    Response->ClusterId = "ydb-cluster";
    Response->ControllerId = Context->Config.HasProxy() ? ProxyNodeId : ctx.SelfID.NodeId();

    if (WithProxy) {
        AddProxyNodeToBrokers();
    } else {
        SendDiscoveryRequest();
    }

    if (Message->Topics.size() == 0 && !WithProxy) {
        AddCurrentNodeToBrokers();
    }

    if (Message->Topics.size() != 0) {
        ProcessTopics();
    }

    Become(&TKafkaMetadataActor::StateWork);
    RespondIfRequired(ctx);
}

void TKafkaMetadataActor::AddCurrentNodeToBrokers() {
    NeedCurrentNode = true;
    if (!DiscoveryRequested) {
        RequestICNodeCache();
    }
}

void TKafkaMetadataActor::SendDiscoveryRequest() {
    DiscoveryRequested = true;
    if (!DiscoveryCacheActor) {
        DiscoveryCacheActor = Register(CreateDiscoveryCache(NGRpcService::KafkaEndpointId));
    }
    PendingResponses++;
    Register(CreateDiscoverer(&MakeEndpointsBoardPath, Context->DatabasePath, SelfId(), DiscoveryCacheActor));
}


void TKafkaMetadataActor::HandleDiscoveryError(TEvDiscovery::TEvError::TPtr& ev) {
    PendingResponses--;
    if (NeedCurrentNode) {
        RequestICNodeCache();
    }
    DiscoveryRequested = false;
    for (auto& [index, ev] : PendingTopicResponses) {
        auto& topic = Response->Topics[index];
        AddTopicResponse(topic, ev->Get());
    }
    PendingTopicResponses.clear();
    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::HandleDiscoveryData(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const NActors::TActorContext& ctx) {
    PendingResponses--;
    auto res = ProcessDiscoveryData(ev);
    if (res) {
        RespondIfRequired(ctx);
    } else if (NeedCurrentNode) {
        RequestICNodeCache();
    }
    DiscoveryRequested = false;
    for (auto& [index, ev] : PendingTopicResponses) {
        auto& topic = Response->Topics[index];
        AddTopicResponse(topic, ev->Get());
    }
    PendingTopicResponses.clear();
    RespondIfRequired(ActorContext());
}

bool TKafkaMetadataActor::ProcessDiscoveryData(TEvDiscovery::TEvDiscoveryData::TPtr& ev) {
    bool expectSsl = Context->Config.HasSslCertificate();

    Ydb::Discovery::ListEndpointsResponse leResponse;
    Ydb::Discovery::ListEndpointsResult leResult;
    TString const* cachedMessage;
    if (expectSsl) {
        cachedMessage = &ev->Get()->CachedMessageData->CachedMessageSsl;
    } else {
        cachedMessage = &ev->Get()->CachedMessageData->CachedMessage;
    }
    auto ok = leResponse.ParseFromString(*cachedMessage);
    if (ok) {
        ok = leResponse.operation().result().UnpackTo(&leResult);
    }
    if (!ok)
        return false;

    for (auto& endpoint : leResult.endpoints()) {
        Nodes.insert({endpoint.node_id(), endpoint.port()});
    }
    if (NeedCurrentNode) {
        return Nodes.count(SelfId().NodeId());
    }
    return true;
}

void TKafkaMetadataActor::RequestICNodeCache() {
    PendingResponses++;
    Send(NKikimr::NIcNodeCache::CreateICNodesInfoCacheServiceId(), new NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoRequest());
}

void TKafkaMetadataActor::AddProxyNodeToBrokers() {
    auto broker = TMetadataResponseData::TMetadataResponseBroker{};
    broker.NodeId = ProxyNodeId;
    broker.Host = Context->Config.GetProxy().GetHostname();
    broker.Port = Context->Config.GetProxy().GetPort();
    Response->Brokers.emplace_back(std::move(broker));
}

void TKafkaMetadataActor::ProcessTopics() {
    THashMap<TString, TActorId> partitionActors;
    for (size_t i = 0; i < Message->Topics.size(); ++i) {
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
            partitionActors[topicName] = child;
        } else {
            child = namesIter->second;
        }
        TopicIndexes[child].push_back(i);
    }
}

void TKafkaMetadataActor::HandleNodesResponse(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto iter = ev->Get()->NodeIdsMapping->find(ctx.SelfID.NodeId());
    Y_ABORT_UNLESS(!iter.IsEnd());
	auto host = (*ev->Get()->Nodes)[iter->second].Host;
    KAFKA_LOG_D("Incoming TEvGetAllNodesInfoResponse. Host#: " << host);

    auto broker = TMetadataResponseData::TMetadataResponseBroker{};
    broker.NodeId = ctx.SelfID.NodeId();
    broker.Host = host;
    broker.Port = Context->Config.GetListeningPort();
    Response->Brokers.emplace_back(std::move(broker));

    --PendingResponses;
    RespondIfRequired(ctx);
}

TActorId TKafkaMetadataActor::SendTopicRequest(const TMetadataRequestData::TMetadataRequestTopic& topicRequest) {
    KAFKA_LOG_D("Describe partitions locations for topic '" << *topicRequest.Name << "' for user " << GetUsernameOrAnonymous(Context));

    TGetPartitionsLocationRequest locationRequest{};
    locationRequest.Topic = NormalizePath(Context->DatabasePath, topicRequest.Name.value());
    locationRequest.Token = GetUserSerializedToken(Context);
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
    topic.ErrorCode = NONE_ERROR;

    topic.Partitions.reserve(response->Partitions.size());
    for (const auto& part : response->Partitions) {
        auto nodeId = WithProxy ? ProxyNodeId : part.NodeId;

        TMetadataResponseData::TMetadataResponseTopic::PartitionsMeta::ItemType responsePartition;
        responsePartition.PartitionIndex = part.PartitionId;
        responsePartition.ErrorCode = NONE_ERROR;
        responsePartition.LeaderId = nodeId;
        responsePartition.LeaderEpoch = part.Generation;
        responsePartition.ReplicaNodes.push_back(nodeId);
        responsePartition.IsrNodes.push_back(nodeId);

        topic.Partitions.emplace_back(std::move(responsePartition));

        if (!WithProxy) {
            auto ins = AllClusterNodes.insert(part.NodeId);
            if (ins.second) {
                auto hostname = part.Hostname;
                if (hostname.StartsWith(UnderlayPrefix)) {
                    hostname = hostname.substr(sizeof(UnderlayPrefix) - 1);
                }

                auto broker = TMetadataResponseData::TMetadataResponseBroker{};
                broker.NodeId = part.NodeId;
                broker.Host = hostname;
                auto nodeIter = Nodes.find(part.NodeId);
                if (nodeIter.IsEnd())
                    broker.Port = Context->Config.GetListeningPort();
                else
                    broker.Port = nodeIter->second;
                Response->Brokers.emplace_back(std::move(broker));
            }
        }
    }
}

void TKafkaMetadataActor::HandleResponse(TEvLocationResponse::TPtr ev, const TActorContext& ctx) {
    --PendingResponses;

    auto* r = ev->Get();
    auto actorIter = TopicIndexes.find(ev->Sender);

    Y_DEBUG_ABORT_UNLESS(!actorIter.IsEnd());
    Y_DEBUG_ABORT_UNLESS(!actorIter->second.empty());

    if (actorIter.IsEnd()) {
        KAFKA_LOG_CRIT("Got unexpected location response, ignoring. Expect malformed/incompled reply");
        return RespondIfRequired(ctx);
    }

    if (actorIter->second.empty()) {
        KAFKA_LOG_CRIT("Corrupted state (empty actorId in mapping). Ignored location response, expect incomplete reply");

        return RespondIfRequired(ctx);
    }

    for (auto index : actorIter->second) {
        auto& topic = Response->Topics[index];
        if (r->Status == Ydb::StatusIds::SUCCESS) {
            KAFKA_LOG_D("Describe topic '" << topic.Name << "' location finishied successful");
            if (DiscoveryRequested) {
                PendingTopicResponses.insert(std::make_pair(index, ev));
            } else {
                AddTopicResponse(topic, r);
            }
        } else {
            KAFKA_LOG_ERROR("Describe topic '" << topic.Name << "' location finishied with error: Code=" << r->Status << ", Issues=" << r->Issues.ToOneLineString());
            AddTopicError(topic, ConvertErrorCode(r->Status));
        }
    }
    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::RespondIfRequired(const TActorContext& ctx) {
    if (PendingResponses == 0 && !PendingTopicResponses) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, ErrorCode));
        Die(ctx);
    }
}

TString TKafkaMetadataActor::LogPrefix() const {
    return TStringBuilder() << "TKafkaMetadataActor " << SelfId() << " ";
}

} // namespace NKafka
