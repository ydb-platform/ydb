#include "kafka_metadata_actor.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/grpc_services/grpc_endpoint.h>
#include <ydb/core/kafka_proxy/actors/kafka_create_topics_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <ydb/core/persqueue/public/list_topics/list_all_topics_actor.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>

namespace NKafka {
using namespace NKikimr;
using namespace NKikimr::NGRpcProxy::V1;

TActorId MakeKafkaDiscoveryCacheID() {
    static const char x[12] = "kafka_dsc_c";
    return TActorId(0, TStringBuf(x, 12));
}

NActors::IActor* CreateKafkaMetadataActor(const TContext::TPtr context,
                                          const ui64 correlationId,
                                          const TMessagePtr<TMetadataRequestData>& message,
                                          const TActorId& discoveryCacheActor) {
    return new TKafkaMetadataActor(context, correlationId, message, discoveryCacheActor);
}

void TKafkaMetadataActor::Bootstrap(const TActorContext& ctx) {
    Response->Topics.resize(Message->Topics.size());
    Response->ClusterId = "ydb-cluster";
    Response->ControllerId = Context->Config.HasProxy() ? ProxyNodeId : ctx.SelfID.NodeId();

    if (WithProxy) {
        AddProxyNodeToBrokers();
    } else {
        SendDiscoveryRequest();

        if (Message->Topics.size() == 0) {
            ctx.Register(NKikimr::NPQ::MakeListAllTopicsActor(
                    SelfId(), Context->DatabasePath, GetUserSerializedToken(Context), true, {}, {}));

            PendingResponses++;
            NeedAllNodes = true;
        }
    }

    if (Message->Topics.size() != 0) {
        ProcessTopicsFromRequest();
    }

    Become(&TKafkaMetadataActor::StateWork);
    RespondIfRequired(ctx);
}

void TKafkaMetadataActor::SendDiscoveryRequest() {
    Y_VERIFY_DEBUG(DiscoveryCacheActor);
    PendingResponses++;
    Register(CreateDiscoverer(&MakeEndpointsBoardPath, Context->DatabasePath, true, SelfId(), DiscoveryCacheActor));
}


void TKafkaMetadataActor::HandleDiscoveryError(TEvDiscovery::TEvError::TPtr& ev) {
    PendingResponses--;
    HaveError = true;
    KAFKA_LOG_ERROR("Port discovery failed for database '" << Context->DatabasePath << "' with error '" << ev->Get()->Error
                    << ", request " << CorrelationId);

    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::HandleDiscoveryData(TEvDiscovery::TEvDiscoveryData::TPtr& ev) {
    PendingResponses--;
    ProcessDiscoveryData(ev);
    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::ProcessDiscoveryData(TEvDiscovery::TEvDiscoveryData::TPtr& ev) {
    bool expectSsl = Context->Config.HasSslCertificate();

    Ydb::Discovery::ListEndpointsResponse leResponse;
    Ydb::Discovery::ListEndpointsResult leResult;
    TString const* cachedMessage;

    if (expectSsl) {
        cachedMessage = &ev->Get()->CachedMessageSsl;
    } else {
        cachedMessage = &ev->Get()->CachedMessage;
    }
    auto ok = leResponse.ParseFromString(*cachedMessage);
    if (ok) {
        ok = leResponse.operation().result().UnpackTo(&leResult);
    }
    if (!ok) {
        KAFKA_LOG_ERROR("Port discovery failed, unable to parse discovery response for request " << CorrelationId);
        HaveError = true;
        return;
    }

    for (auto& endpoint : leResult.endpoints()) {
        Nodes.insert({endpoint.node_id(), {endpoint.address(), endpoint.port()}});
    }
}

void TKafkaMetadataActor::ProcessTopicsFromRequest() {
    TVector<TString> topicsToRequest;
    for (size_t i = 0; i < Message->Topics.size(); ++i) {
        auto& reqTopic = Message->Topics[i];
        if (!reqTopic.Name.value_or("")) {
            AddTopicError(Response->Topics[i], EKafkaErrors::INVALID_TOPIC_EXCEPTION);
            continue;
        }
        AddTopic(reqTopic.Name.value_or(""), i);
    }
}

void TKafkaMetadataActor::HandleListTopics(NKikimr::TEvPQ::TEvListAllTopicsResponse::TPtr& ev) {
    Y_ABORT_UNLESS(PendingResponses > 0);
    PendingResponses--;
    auto topics = std::move(ev->Get()->Topics);
    Response->Topics.resize(topics.size());
    for (size_t i = 0; i < topics.size(); ++i) {
        AddTopic(topics[i], i);
    }
    RespondIfRequired(ActorContext());
}

void TKafkaMetadataActor::AddProxyNodeToBrokers() {
    Nodes.insert({ProxyNodeId, {Context->Config.GetProxy().GetHostname(), static_cast<ui32>(Context->Config.GetProxy().GetPort())}});
    AddBroker(ProxyNodeId, Context->Config.GetProxy().GetHostname(), Context->Config.GetProxy().GetPort());
}


void TKafkaMetadataActor::AddTopic(const TString& topic, ui64 index) {
    Response->Topics[index] = TMetadataResponseData::TMetadataResponseTopic{};
    Response->Topics[index].Name = topic;

    TActorId child;
    auto namesIter = PartitionActors.find(topic);
    if (namesIter.IsEnd()) {
        child = SendTopicRequest(topic);
        PartitionActors[topic] = child;
    } else {
        child = namesIter->second;
    }
    TopicIndexes[child].push_back(index);
}

TActorId TKafkaMetadataActor::SendTopicRequest(const TString& topic) {
    KAFKA_LOG_D("Describe partitions locations for topic '" << topic << "' for user '" << GetUsernameOrAnonymous(Context) << "'");

    TGetPartitionsLocationRequest locationRequest{};
    locationRequest.Topic = NormalizePath(Context->DatabasePath, topic);
    locationRequest.Token = GetUserSerializedToken(Context);
    locationRequest.Database = Context->DatabasePath;

    PendingResponses++;

    return Register(new TPartitionsLocationActor(locationRequest, SelfId()));
}

TVector<TKafkaMetadataActor::TNodeInfo*> TKafkaMetadataActor::CheckTopicNodes(TEvLocationResponse* response) {
    TVector<TNodeInfo*> partitionNodes;
    for (const auto& part : response->Partitions) {
        auto iter = Nodes.find(part.NodeId);
        if (iter == Nodes.end()) {
            return {};
        }
        partitionNodes.push_back(&iter->second);
    }
    return partitionNodes;
}

void TKafkaMetadataActor::AddTopicError(
    TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode
) {
    topic.ErrorCode = errorCode;
    ErrorCode = errorCode;
}

void TKafkaMetadataActor::AddTopicResponse(
        TMetadataResponseData::TMetadataResponseTopic& topic,
        TEvLocationResponse* response,
        const TVector<TKafkaMetadataActor::TNodeInfo*>& partitionNodes
) {
    topic.ErrorCode = NONE_ERROR;

    topic.Partitions.reserve(response->Partitions.size());
    auto nodeIter = partitionNodes.begin();
    for (const auto& part : response->Partitions) {
        auto nodeId = WithProxy ? ProxyNodeId : part.NodeId;

        TMetadataResponseData::TMetadataResponseTopic::PartitionsMeta::ItemType responsePartition;
        responsePartition.PartitionIndex = part.PartitionId;
        responsePartition.ErrorCode = NONE_ERROR;
        responsePartition.LeaderId = nodeId;
        responsePartition.LeaderEpoch = part.Generation;

        // adding replica nodes in a roundrobin manner based on sorted NodeId
        std::vector<ui64> nodesToAdd = {nodeId};
        if (!WithProxy && !NeedAllNodes) {
            AddBroker(nodeId, (*nodeIter)->Host, (*nodeIter)->Port);
        }
        if (!WithProxy) {
            auto nodeToAddIter = Nodes.find(part.NodeId);
            nodeToAddIter++;
            for (size_t i = 0; i < 2; ++i) {
                if (nodeToAddIter == Nodes.end()) {
                    nodeToAddIter = Nodes.begin();
                }
                if (nodeToAddIter->first == nodeId) {
                    break;
                }
                nodesToAdd.push_back(nodeToAddIter->first);
                if (!NeedAllNodes) {
                    AddBroker(nodeToAddIter->first, nodeToAddIter->second.Host, nodeToAddIter->second.Port);
                }
                nodeToAddIter++;
            }
            std::sort(nodesToAdd.begin(), nodesToAdd.end());
        }

        for (size_t i = 0; i < nodesToAdd.size(); i++) {
            responsePartition.ReplicaNodes.push_back(nodesToAdd[i]);
            responsePartition.IsrNodes.push_back(nodesToAdd[i]);
        }
        topic.Partitions.emplace_back(std::move(responsePartition));
        ++nodeIter;
    }
}

void TKafkaMetadataActor::HandleLocationResponse(TEvLocationResponse::TPtr ev, const TActorContext& ctx) {
    --PendingResponses;

    auto actorIter = TopicIndexes.find(ev->Sender);
    TSimpleSharedPtr<TEvLocationResponse> locationResponse{ev->Release()};

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
        Ydb::StatusIds::StatusCode status = locationResponse->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            KAFKA_LOG_D("Describe topic '" << topic.Name << "' location finishied successful");
            PendingTopicResponses.emplace(index, locationResponse);
        } else if (status == Ydb::StatusIds::SCHEME_ERROR
                && Message->AllowAutoTopicCreation
                && Context->Config.GetAutoCreateTopicsEnable()
                && TopicСreationAttempts.find(*topic.Name) == TopicСreationAttempts.end()
            ) {
            KAFKA_LOG_D("Sending create topic'" << topic.Name << "' request");
            TopicСreationAttempts.insert(*topic.Name);
            PendingResponses++;
            SendCreateTopicsRequest(*topic.Name, index, ctx);
        } else {
            KAFKA_LOG_ERROR("Describe topic '" << topic.Name << "' location finishied with error: Code="
                << locationResponse->Status << ", Issues=" << locationResponse->Issues.ToOneLineString());
            AddTopicError(topic, ConvertErrorCode(locationResponse->Status));
        }
    }
    if (InflyCreateTopics == 0) {
        RespondIfRequired(ctx);
    }
}

void TKafkaMetadataActor::Handle(const TEvKafka::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    // can be received only from TCreateTopicActor
    TActorId& creatorActorId = ev->Sender;
    const TTopicNameToIndex& topicNameToIndex = CreateTopicRequests[creatorActorId];
    const TString& topicName = topicNameToIndex.TopicName;
    const ui32& topicIndex = topicNameToIndex.TopicIndex;
    InflyCreateTopics--;
    PendingResponses--;
    EKafkaErrors errorCode = ev->Get()->ErrorCode;
    if (errorCode == EKafkaErrors::NONE_ERROR) {
        TActorId child = SendTopicRequest(topicName);
        TopicIndexes[child].push_back(topicIndex);
    } else {
        Response->Topics[topicIndex].ErrorCode = errorCode;
        if (InflyCreateTopics == 0) {
            RespondIfRequired(ctx);
        }
    }
}

void TKafkaMetadataActor::SendCreateTopicsRequest(const TString& topicName, ui32 index, const TActorContext& ctx) {
    InflyCreateTopics++;
    auto message = std::make_shared<NKafka::TCreateTopicsRequestData>();
    TCreateTopicsRequestData::TCreatableTopic topicToCreate;
    topicToCreate.Name = topicName;
    topicToCreate.NumPartitions = Context->Config.GetTopicCreationDefaultPartitions();
    message->Topics.push_back(topicToCreate);
    TContext::TPtr ContextForTopicCreation;
    ContextForTopicCreation = std::make_shared<TContext>(TContext(*Context));
    ContextForTopicCreation->ConnectionId = ctx.SelfID;
    ContextForTopicCreation->UserToken = Context->UserToken;
    ContextForTopicCreation->DatabasePath = Context->DatabasePath;
    ContextForTopicCreation->ResourceDatabasePath = Context->ResourceDatabasePath;
    TActorId actorId = ctx.Register(new TKafkaCreateTopicsActor(ContextForTopicCreation,
        1,
        TMessagePtr<NKafka::TCreateTopicsRequestData>({}, message)
    ));
    CreateTopicRequests[actorId] = TTopicNameToIndex{topicName, index};
}

void TKafkaMetadataActor::AddBroker(ui64 nodeId, const TString& host, ui64 port) {
    auto ins = AddedBrokerNodes.insert(nodeId);
    if (ins.second) {
        auto hostname = host;
        if (hostname.StartsWith(UnderlayPrefix)) {
            hostname = hostname.substr(sizeof(UnderlayPrefix) - 1);
        };
        auto broker = TMetadataResponseData::TMetadataResponseBroker{};
        broker.NodeId = nodeId;
        broker.Host = hostname;
        broker.Port = port;
        Response->Brokers.emplace_back(std::move(broker));
    }
}

void TKafkaMetadataActor::RespondIfRequired(const TActorContext& ctx) {
    auto Respond = [&] {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, ErrorCode));
        Die(ctx);
    };

    if (HaveError) {
        ErrorCode = EKafkaErrors::LISTENER_NOT_FOUND;
        for (auto& topic : Response->Topics) {
            AddTopicError(topic, ErrorCode);
        }
        Respond();
        return;
    }
    if (PendingResponses != 0) {
        return;
    }

    while (!PendingTopicResponses.empty()) {
        auto& [index, ev] = *PendingTopicResponses.begin();
        auto& topic = Response->Topics[index];
        if (!WithProxy) {
            auto topicNodes = CheckTopicNodes(ev.Get());
            if (topicNodes.empty()) {
                    // Already tried YDB discovery. Throw error
                    KAFKA_LOG_ERROR("Could not discovery kafka port for topic '" << topic.Name);
                    AddTopicError(topic, EKafkaErrors::LISTENER_NOT_FOUND);
            } else {
                AddTopicResponse(topic, ev.Get(), topicNodes);
            }
        } else {
            AddTopicResponse(topic, ev.Get(), {});
        }
        PendingTopicResponses.erase(PendingTopicResponses.begin());
    }

    if (NeedAllNodes) {
        for (const auto& [id, nodeInfo] : Nodes)
            AddBroker(id, nodeInfo.Host, nodeInfo.Port);
    }

    Respond();
}

TString TKafkaMetadataActor::LogPrefix() const {
    return TStringBuilder() << "TKafkaMetadataActor " << SelfId() << " ";
}

} // namespace NKafka
