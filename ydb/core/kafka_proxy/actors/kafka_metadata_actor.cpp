#include "kafka_metadata_actor.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/grpc_services/grpc_endpoint.h>
#include <ydb/core/kafka_proxy/actors/kafka_create_topics_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/list_topics/list_all_topics_actor.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/log.h>

#include <absl/container/flat_hash_set.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KAFKA_PROXY

namespace NKafka {
using namespace NKikimr;
using namespace NKikimr::NGRpcProxy::V1;
using namespace NKikimr::NPQ;

namespace {

class TTopicLocationActor: public TBaseActor<TTopicLocationActor>
                         , protected TPipeCacheClient
                         , public TConstantLogPrefix {
    static constexpr TDuration RequestTimeout = TDuration::Seconds(30);
    static constexpr ui64 TimeoutTag = 1;
    static constexpr ui64 RetryTag = 2;

public:
    TTopicLocationActor(TActorId requester, TString path, TString database, TString token)
        : TBaseActor<TTopicLocationActor>(NKikimrServices::KAFKA_PROXY)
        , TPipeCacheClient(this)
        , Requester(requester)
        , Path(std::move(path))
        , Database(std::move(database))
        , Token(std::move(token))
        , Response(MakeHolder<TEvPQProxy::TEvPartitionLocationResponse>())
    {
    }

    void Bootstrap() {
        // Unauthenticated Metadata is rejected in kafka_connection (SASL) before this
        // actor is created. Per-request ACL is DescribeSchema via the describer.
        RequestStart = TActivationContext::Now();
        Schedule(RequestTimeout, new TEvents::TEvWakeup(TimeoutTag));

        TIntrusiveConstPtr<NACLib::TUserToken> userToken;
        if (!Token.empty()) {
            userToken = new NACLib::TUserToken(Token);
        }

        DescriberId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
            SelfId(),
            CanonizePath(Database),
            {Path},
            {
                .UserToken = userToken,
                .AccessRights = NACLib::EAccessRights::DescribeSchema,
            }));
        Become(&TTopicLocationActor::StateWork);
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[TTopicLocationActor][" << Path << "]";
    }

    bool OnUnhandledException(const std::exception& exc) override {
        DoLogUnhandledException(Service, NPQ_LOG_PREFIX, exc);
        ReplyError(
            Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "Unhandled exception: " << exc.what());
        return true;
    }

private:
    void PassAway() override {
        if (DescriberId) {
            Send(DescriberId, new TEvents::TEvPoison());
            DescriberId = {};
        }
        TPipeCacheClient::Close();
        TBaseActor::PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message) {
        LocationInflight = false;
        RetryPending = false;
        if (!Response) {
            PassAway();
            return;
        }
        Response->Status = status;
        Response->Issues.AddIssue(message);
        Send(Requester, Response.Release());
        PassAway();
    }

    TDuration Remaining() const {
        const auto deadline = RequestStart + RequestTimeout;
        const auto now = TActivationContext::Now();
        return now >= deadline ? TDuration::Zero() : deadline - now;
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        DescriberId = {};
        const auto it = ev->Get()->Topics.find(Path);
        AFL_ENSURE(it != ev->Get()->Topics.end())("path", Path);
        const auto& topicInfo = it->second;
        if (topicInfo.Status != NDescriber::EStatus::SUCCESS) {
            auto status = NDescriber::Convert(topicInfo.Status);
            // Missing topic → SCHEME_ERROR so Kafka auto-create / UNKNOWN_TOPIC still work.
            if (status == Ydb::StatusIds::NOT_FOUND) {
                status = Ydb::StatusIds::SCHEME_ERROR;
            }
            return ReplyError(status, NDescriber::Description(Path, topicInfo.Status));
        }
        AFL_ENSURE(topicInfo.Self && topicInfo.Info);
        AFL_ENSURE(Response);

        Response->PathId = topicInfo.Self->Info.GetPathId();
        Response->SchemeShardId = topicInfo.Self->Info.GetSchemeshardId();
        BalancerTabletId = topicInfo.Info->Description.GetBalancerTabletID();

        SchemePartitionIds.clear();
        for (const auto& partition : topicInfo.Info->Description.GetPartitions()) {
            SchemePartitionIds.push_back(partition.GetPartitionId());
        }
        RequestLocation();
    }

    bool HasAllSchemePartitions(const NKikimrPQ::TPartitionsLocationResponse& record) const {
        absl::flat_hash_set<ui64> got;
        got.reserve(record.LocationsSize());
        for (const auto& location : record.GetLocations()) {
            got.insert(location.GetPartitionId());
        }
        for (auto id : SchemePartitionIds) {
            if (!got.contains(id)) {
                return false;
            }
        }
        return true;
    }

    void RequestLocation() {
        const auto remaining = Remaining();
        if (!remaining) {
            return ReplyError(Ydb::StatusIds::TIMEOUT, "Request timed out");
        }
        // Ask for scheme partition ids so a lagging balancer returns Status=false
        // instead of a partial live PartitionsInfo (e.g. during split).
        // Cookie 0 is reserved: old PQRB replies without echoing the request cookie.
        const ui64 cookie = ++LocationRequestGeneration;
        AFL_ENSURE(cookie != 0);
        SendToTablet(
            BalancerTabletId,
            new TEvPersQueue::TEvGetPartitionsLocation(SchemePartitionIds, remaining),
            cookie);
        LocationInflight = true;
    }

    void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev) {
        // ScheduleRetry() clears LocationInflight before setting RetryPending, so a
        // late cookie=0 reply from an old PQRB cannot land in the retry window.
        // Cookie matching below cannot defend against those (old PQRB always
        // replies with cookie 0); LocationInflight is the real safety net.
        if (!LocationInflight || RetryPending) {
            return;
        }
        if (ev->Cookie == 0) {
            return ApplyLocationResponse(ev->Get()->Record);
        }
        if (ev->Cookie != LocationRequestGeneration) {
            return;
        }
        ApplyLocationResponse(ev->Get()->Record);
    }

    void ApplyLocationResponse(const NKikimrPQ::TPartitionsLocationResponse& record) {
        if (!record.GetStatus() || !HasAllSchemePartitions(record)) {
            return ScheduleRetry();
        }

        AFL_ENSURE(Response);
        Response->Partitions.reserve(SchemePartitionIds.size());
        absl::flat_hash_set<ui64> scheme(
            SchemePartitionIds.begin(), SchemePartitionIds.end());
        for (const auto& location : record.GetLocations()) {
            if (!scheme.contains(location.GetPartitionId())) {
                continue;
            }
            TEvPQProxy::TPartitionLocationInfo part;
            part.PartitionId = location.GetPartitionId();
            part.Generation = location.GetGeneration();
            part.NodeId = location.GetNodeId();
            Response->Partitions.push_back(std::move(part));
        }
        Response->Status = Ydb::StatusIds::SUCCESS;
        Send(Requester, Response.Release());
        PassAway();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (!OnUndelivered(ev) || ev->Get()->TabletId != BalancerTabletId || !LocationInflight) {
            return;
        }
        ScheduleRetry();
    }

    void ScheduleRetry() {
        LocationInflight = false;
        if (!Remaining()) {
            return ReplyError(Ydb::StatusIds::TIMEOUT, "Request timed out");
        }
        if (!Backoff.HasMore()) {
            return ReplyError(Ydb::StatusIds::UNAVAILABLE, "Partition locations are not available");
        }
        if (RetryPending) {
            return;
        }
        RetryPending = true;
        Schedule(Backoff.Next(), new TEvents::TEvWakeup(RetryTag));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == TimeoutTag) {
            return ReplyError(Ydb::StatusIds::TIMEOUT, "Request timed out");
        }
        if (ev->Get()->Tag == RetryTag) {
            RetryPending = false;
            RequestLocation();
        }
    }

    void HandlePoison() {
        if (DescriberId) {
            Send(DescriberId, new TEvents::TEvPoison());
            DescriberId = {};
        }
        ReplyError(Ydb::StatusIds::CANCELLED, "Request was cancelled");
    }

    STRICT_STFUNC(StateWork,
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        hFunc(TEvPersQueue::TEvGetPartitionsLocationResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        cFunc(TEvents::TEvPoison::EventType, HandlePoison);
    )

    TActorId Requester;
    TString Path;
    TString Database;
    TString Token;
    THolder<TEvPQProxy::TEvPartitionLocationResponse> Response;
    TActorId DescriberId;
    ui64 BalancerTabletId = 0;
    TVector<ui64> SchemePartitionIds;
    TInstant RequestStart;
    ui64 LocationRequestGeneration = 0; // 0 is never sent; old PQRB replies with cookie 0
    bool LocationInflight = false;
    bool RetryPending = false;
    TBackoff Backoff = TBackoff(25, TDuration::MilliSeconds(10), TDuration::MilliSeconds(100));
};

} // namespace

NActors::IActor* CreateTopicLocationActor(
    const NActors::TActorId& requester,
    TString path,
    TString database,
    TString token)
{
    return new TTopicLocationActor(std::move(requester), std::move(path), std::move(database), std::move(token));
}

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
    TimeoutTimerActorId = CreateLongTimer(ctx, RequestTimeout,
        new IEventHandle(SelfId(), SelfId(), new TEvents::TEvWakeup()));
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
    YDB_LOG_ERROR("Port discovery failed for database with error request",
        {LogPrefix()},
        {"databasePath", Context->DatabasePath},
        {"error", ev->Get()->Error},
        {"correlationId", CorrelationId});

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
        YDB_LOG_ERROR("Port discovery failed, unable to parse discovery response for request",
            {LogPrefix()},
            {"correlationId", CorrelationId});
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
    AFL_ENSURE(PendingResponses > 0)("pending", PendingResponses)("database", Context->DatabasePath);
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
    YDB_LOG_DEBUG("Describe partitions locations for topic for user",
        {LogPrefix()},
        {"topic", topic},
        {"userName", GetUsernameOrAnonymous(Context)});

    PendingResponses++;

    return Register(new TTopicLocationActor(
        SelfId(),
        NormalizePath(Context->DatabasePath, topic),
        Context->DatabasePath,
        GetUserSerializedToken(Context)));
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
        YDB_LOG_CRIT("Got unexpected location response, ignoring. Expect malformed/incompled reply",
            {LogPrefix()});
        return RespondIfRequired(ctx);
    }

    if (actorIter->second.empty()) {
        YDB_LOG_CRIT("Corrupted state (empty actorId in mapping). Ignored location response, expect incomplete reply",
            {LogPrefix()});
        return RespondIfRequired(ctx);
    }

    for (auto index : actorIter->second) {
        auto& topic = Response->Topics[index];
        Ydb::StatusIds::StatusCode status = locationResponse->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            YDB_LOG_DEBUG("Describe topic location finishied successful",
                {LogPrefix()},
                {"topicName", topic.Name});
            PendingTopicResponses.emplace(index, locationResponse);
        } else if (status == Ydb::StatusIds::SCHEME_ERROR
                && Message->AllowAutoTopicCreation
                && Context->Config.GetAutoCreateTopicsEnable()
                && TopicСreationAttempts.find(*topic.Name) == TopicСreationAttempts.end()
            ) {
            YDB_LOG_DEBUG("Sending create topic' request",
                {LogPrefix()},
                {"topicName", topic.Name});
            TopicСreationAttempts.insert(*topic.Name);
            PendingResponses++;
            SendCreateTopicsRequest(*topic.Name, index, ctx);
        } else {
            YDB_LOG_ERROR("Describe topic location finishied with error",
                {LogPrefix()},
                {"topicName", topic.Name},
                {"code", locationResponse->Status},
                {"issues", locationResponse->Issues.ToOneLineString()});
            // Transient location failures (pipe retries exhausted / locations backoff) → retriable timeout.
            const EKafkaErrors kafkaError =
                (status == Ydb::StatusIds::UNAVAILABLE || status == Ydb::StatusIds::INTERNAL_ERROR)
                    ? EKafkaErrors::REQUEST_TIMED_OUT
                    : ConvertErrorCode(status);
            AddTopicError(topic, kafkaError);
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
    if (errorCode == EKafkaErrors::NONE_ERROR || errorCode == EKafkaErrors::TOPIC_ALREADY_EXISTS) {
        // Topic is available (created by us or raced with another create) — describe location.
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
    ContextForTopicCreation->Token.UserToken = Context->Token.UserToken;
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

void TKafkaMetadataActor::EnsureBrokersAndController() {
    // Unknown topics used to return brokers=[]; AdminClient then cannot CreateTopics.
    // NeedAllNodes also requires the full discovered broker set.
    if (!WithProxy && (Response->Brokers.empty() || NeedAllNodes)) {
        for (const auto& [id, nodeInfo] : Nodes) {
            AddBroker(id, nodeInfo.Host, nodeInfo.Port);
        }
    }

    // ControllerId must be one of Brokers (SelfID may differ from discovery node ids).
    if (Response->Brokers.empty()) {
        return;
    }

    for (const auto& broker : Response->Brokers) {
        if (broker.NodeId == Response->ControllerId) {
            return;
        }
    }

    // Prefer keeping ControllerId if that node is known from discovery.
    if (auto it = Nodes.find(Response->ControllerId); it != Nodes.end()) {
        AddBroker(it->first, it->second.Host, it->second.Port);
        return;
    }

    Response->ControllerId = Response->Brokers.front().NodeId;
}

void TKafkaMetadataActor::ApplyPendingTopicResponses() {
    while (!PendingTopicResponses.empty()) {
        auto& [index, ev] = *PendingTopicResponses.begin();
        auto& topic = Response->Topics[index];
        if (!WithProxy) {
            auto topicNodes = CheckTopicNodes(ev.Get());
            if (topicNodes.empty()) {
                // Already tried YDB discovery. Throw error
                YDB_LOG_ERROR("Could not discovery kafka port for topic",
                    {LogPrefix()},
                    {"topicName", topic.Name});
                AddTopicError(topic, EKafkaErrors::LISTENER_NOT_FOUND);
            } else {
                AddTopicResponse(topic, ev.Get(), topicNodes);
            }
        } else {
            AddTopicResponse(topic, ev.Get(), {});
        }
        PendingTopicResponses.erase(PendingTopicResponses.begin());
    }
}

void TKafkaMetadataActor::RespondIfRequired(const TActorContext& ctx) {
    auto Respond = [&] {
        EnsureBrokersAndController();
        CancelRequestTimeout();
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

    ApplyPendingTopicResponses();
    Respond();
}

void TKafkaMetadataActor::HandleWakeup(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    TimeoutTimerActorId = {};
    YDB_LOG_ERROR("Metadata request timed out",
        {LogPrefix()},
        {"correlationId", CorrelationId},
        {"pendingResponses", PendingResponses});
    RespondWithTimeout(ctx);
}

void TKafkaMetadataActor::RespondWithTimeout(const TActorContext& ctx) {
    ApplyPendingTopicResponses();
    EnsureBrokersAndController();

    ErrorCode = EKafkaErrors::REQUEST_TIMED_OUT;
    for (auto& topic : Response->Topics) {
        // Keep already completed topics (success or earlier error); fail only unfinished ones.
        if (topic.ErrorCode == EKafkaErrors::NONE_ERROR && topic.Partitions.empty()) {
            topic.ErrorCode = EKafkaErrors::REQUEST_TIMED_OUT;
        }
    }

    CancelRequestTimeout();
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, ErrorCode));
    Die(ctx);
}

void TKafkaMetadataActor::CancelRequestTimeout() {
    if (TimeoutTimerActorId) {
        Send(TimeoutTimerActorId, new TEvents::TEvPoison());
        TimeoutTimerActorId = {};
    }
}

NStructuredLog::TStructuredMessage TKafkaMetadataActor::LogPrefix() const {
    return YDB_LOG_CREATE_MESSAGE(
        {"actorClassName", "TKafkaMetadataActor"},
        {"selfId", SelfId()});
}

} // namespace NKafka
