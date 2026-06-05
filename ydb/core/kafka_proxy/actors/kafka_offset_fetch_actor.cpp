#include "kafka_offset_fetch_actor.h"

#include <ydb/core/kafka_proxy/actors/kafka_create_topics_actor.h>
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>
#include "ydb/core/kafka_proxy/kafka_consumer_members_metadata_initializers.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
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
            const TString& userSID,
            const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            bool requireAuthentication = false)
        : TBase(request, requester)
        , TDescribeTopicActorImpl(ConsumerOffsetSettings(consumers, partitions))
        , Requester(requester)
        , OriginalTopicName(originalTopicName)
        , UserSID(userSID)
        , UserToken(userToken)
        , RequireAuthentication(requireAuthentication)
        {
        };

    void Bootstrap(const NActors::TActorContext& ctx) override {
        Y_UNUSED(ctx);
        KAFKA_LOG_D("Get commited offsets for topic '" << OriginalTopicName
            << "' for user '" << UserSID << "'");
        SendDescribeProposeRequest();
        Become(&TTopicOffsetActor::StateWork);
    };

    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "KafkaTopicOffsetActor: ";
    }

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

        KAFKA_LOG_D("Error raised for '" << OriginalTopicName << "'"
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
            std::unordered_map<TString, TEvKafka::PartitionConsumerOffset> consumerToOffset;
            for (auto& consumerResult : partResult.GetConsumerResult()) {
                if (consumerResult.GetErrorCode() == NPersQueue::NErrorCode::OK) {
                    std::optional<TString> committedMetadata = consumerResult.HasCommittedMetadata() ?
                        static_cast<std::optional<TString>>(consumerResult.GetCommittedMetadata()) :
                        std::nullopt;
                    TEvKafka::PartitionConsumerOffset partitionConsumerOffset = TEvKafka::PartitionConsumerOffset{
                        static_cast<ui64>(partResult.GetPartition()),
                        static_cast<ui64>(consumerResult.GetCommitedOffset()),
                        committedMetadata};
                    consumerToOffset.emplace(
                        consumerResult.GetConsumer(),
                        partitionConsumerOffset);
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
        KAFKA_LOG_D("TEvNavigateKeySetResult received for topic '" << OriginalTopicName
            << "' for user '" << UserSID << "'. PQGroupInfo is present: " << (response.PQGroupInfo.Get() != nullptr));
        if (!response.PQGroupInfo) {
            THolder<TEvKafka::TEvCommitedOffsetsResponse> response(new TEvKafka::TEvCommitedOffsetsResponse());
            response->TopicName = OriginalTopicName;
            response->Status = UNKNOWN_TOPIC_OR_PARTITION;
            Send(Requester, response.Release());
            TActorBootstrapped::PassAway();
            return;
        }
        auto path = CanonizePath(NKikimr::JoinPath(response.Path));
        bool hasRights = true;
        if (UserToken && UserToken->GetSerializedToken()) {
            hasRights = response.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow, *UserToken);
        } else if (RequireAuthentication) {
            hasRights = false;
        }
        if (!hasRights) {
            RaiseError(
                "unauthenticated access is forbidden",
                Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                Ydb::StatusIds::StatusCode::StatusIds_StatusCode_UNAUTHORIZED,
                ActorContext()
            );
            return;
        }
        const auto& pqDescr = response.PQGroupInfo->Description;
        ProcessTablets(pqDescr, ActorContext());
    }

    void Reply(const TActorContext& ctx) override {
        KAFKA_LOG_D("Replying for topic '" << OriginalTopicName
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
        const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        bool RequireAuthentication;
        std::shared_ptr<std::unordered_map<ui32, std::unordered_map<TString, TEvKafka::PartitionConsumerOffset>>> PartitionIdToOffsets = std::make_shared<std::unordered_map<ui32, std::unordered_map<TString, TEvKafka::PartitionConsumerOffset>>>();
};

NActors::IActor* CreateKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message) {
    return new TKafkaOffsetFetchActor(context, correlationId, message);
}

void TKafkaOffsetFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    // If API level <= 7, Groups would be empty. In this case we convert message to level 8 and process it uniformely later
    KAFKA_LOG_D("New request for user " << GetUsernameOrAnonymous(Context));

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

    for (size_t i = 0; i < Message->Groups.size(); i++) {
        const auto& group = Message->Groups[i];
        if (group.Topics.empty()) {
            GroupsToFetch.emplace_back(group.GroupId);
            GroupIdToIndex[group.GroupId.value()] = ui32(i);
        } else {
            for (const auto& topic: group.Topics) {
                ExtractPartitions(group.GroupId.value(), topic);
            }
        }
    }
    if (!GroupsToFetch.empty()) {
        // if topics were not specified for some groups,
        // topics for such groups will be retrieved from the table
        Kqp = std::make_unique<TKqpTxHelper>(Context->ResourceDatabasePath);
        Kqp->SendCreateSessionRequest(ctx);
        KAFKA_LOG_D("Creating KQP Session");
    } else {
        FillMapWithGroupRequests();
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
                GetUsernameOrAnonymous(Context),
                Context->UserToken,
                Context->RequireAuthentication
            ));
            InflyTopics++;
        }
    }
    Become(&TKafkaOffsetFetchActor::StateWork);
}

void TKafkaOffsetFetchActor::Handle(TEvKafka::TEvCommitedOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
    InflyTopics--;

    auto eventPtr = ev->Release();
    TString topicName = eventPtr->TopicName;
    TopicsToResponses[topicName] = eventPtr;
    bool topicNotCreatedYet = false;
    auto& topicGroupRequests = GroupRequests[topicName];
    for (const auto& [topicRequest, groupId] : topicGroupRequests) {
        if (topicNotCreatedYet) {
            break;
        }
        auto topicResponse = GetOffsetResponseForTopic(topicRequest, groupId);
        for (const auto& topicPartition : topicResponse.Partitions) {
            if (topicNotCreatedYet) {
                break;
            }
            TString topicName = GetTopicNameWithoutDb(DatabasePath, *topicRequest.Name);
            TString topicPath = NormalizePath(DatabasePath, topicName);
            if (topicPartition.ErrorCode == EKafkaErrors::RESOURCE_NOT_FOUND &&
                Context->Config.GetAutoCreateConsumersEnable()) {
                // consumer is not assigned to the topic case
                TKafkaOffsetFetchActor::CreateConsumerGroupIfNecessary(topicName,
                                                                        topicPath,
                                                                        topicName,
                                                                        groupId);
                break;
            } else if (topicPartition.ErrorCode == EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION &&
                        Context->Config.GetAutoCreateTopicsEnable()) {
                // topic or partition does not exist case
                CreateTopicIfNecessary(topicName, *topicRequest.Name, ctx);
                topicNotCreatedYet = true;
                break;
            }
        }
    }
    if (InflyTopics == 0) {
        auto response = GetOffsetFetchResponse();
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
        Die(ctx);
    }
}

void TKafkaOffsetFetchActor::Handle(const TEvKafka::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    // TKafkaOffsetFetchActor can receive TEvResponse only from TKafkaCreateTopicsActor actor
    TActorId& creatorActorId = ev->Sender;
    const TString& createdTopicName = CreateTopicActorIdToName[creatorActorId];
    auto errorCode = ev->Release()->ErrorCode;
    DependantActors.erase(creatorActorId);
    if (errorCode != EKafkaErrors::NONE_ERROR) {
        InflyTopics--;
        if (InflyTopics == 0) {
            auto response = GetOffsetFetchResponse();
            Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
            Die(ctx);
        }
        return;
    }

    NKikimr::NGRpcProxy::V1::TLocalRequestBase locationRequest{
        NormalizePath(Context->DatabasePath, createdTopicName),
        Context->DatabasePath,
        GetUserSerializedToken(Context),
    };
    ctx.Register(new TTopicOffsetActor(
        TopicToEntities[createdTopicName].Consumers,
        locationRequest,
        SelfId(),
        TopicToEntities[createdTopicName].Partitions,
        createdTopicName,
        GetUsernameOrAnonymous(Context),
        Context->UserToken,
        Context->RequireAuthentication
    ));
}

void TKafkaOffsetFetchActor::Handle(NKikimr::NReplication::TEvYdbProxy::TEvAlterTopicResponse::TPtr& ev, const TActorContext& ctx) {
    NYdb::TStatus& result = ev->Get()->Result;
    if (result.GetStatus() == NYdb::EStatus::SUCCESS) {
        KAFKA_LOG_D("Handling TEvAlterTopicResponse. Status: " << result.GetStatus() << "\n");
    } else {
        KAFKA_LOG_I("Handling TEvAlterTopicResponse. Status: " << result.GetStatus() << "\n");
    }
    if (result.GetStatus() != NYdb::EStatus::SUCCESS) {
        InflyTopics--;
        if (InflyTopics == 0) {
            auto response = GetOffsetFetchResponse();
            Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
            Die(ctx);
        }
        return;
    }

    const TString& alteredTopicName = AlterTopicCookieToName[ev->Cookie];
    NKikimr::NGRpcProxy::V1::TLocalRequestBase locationRequest{
        NormalizePath(Context->DatabasePath, alteredTopicName),
        Context->DatabasePath,
        GetUserSerializedToken(Context),
    };
    TActorId actorId = SelfId();
    ctx.Register(new TTopicOffsetActor(
        TopicToEntities[alteredTopicName].Consumers,
        locationRequest,
        actorId,
        TopicToEntities[alteredTopicName].Partitions,
        alteredTopicName,
        GetUsernameOrAnonymous(Context),
        Context->UserToken,
        Context->RequireAuthentication
    ));

}

void TKafkaOffsetFetchActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("Got KQP CreateSession response");
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, std::make_shared<TOffsetFetchResponseData>(), EKafkaErrors::UNKNOWN_SERVER_ERROR));
        KAFKA_LOG_D("KQP Session Error");
        return;
    }
    NYdb::TParamsBuilder params = BuildFetchAssignmentsParams(GroupsToFetch);
    Kqp->SendYqlRequest(Sprintf(FETCH_ASSIGNMENTS.c_str(), NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant()->FormPathToResourceTable(Context->ResourceDatabasePath).c_str()), params.Build(), KqpCookie, ctx);
}

void NKafka::TKafkaOffsetFetchActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    std::vector<std::pair<std::optional<TString>, TConsumerProtocolAssignment>> assignments;
    KAFKA_LOG_D("Received KQP response");
    ParseGroupsAssignments(ev, assignments);

    if (assignments.empty()) {
        auto response = GetOffsetFetchResponse();
        KAFKA_LOG_D("Sending response to user " << GetUsernameOrAnonymous(Context));
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
        Die(ctx);
        return;
    }

    for (const auto& [groupId, consumerAssignment] : assignments) {
        ui32 groupIndex = GroupIdToIndex[*groupId];
        auto& groupRequest = Message->Groups[groupIndex];
        for (auto& partitionAssignment : consumerAssignment.AssignedPartitions) {
            NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic;
            topic.Name = partitionAssignment.Topic;
            topic.PartitionIndexes = partitionAssignment.Partitions;
            groupRequest.Topics.push_back(topic);
        }

        for (const auto& topic: groupRequest.Topics) {
            ExtractPartitions(groupRequest.GroupId.value(), topic);
        }
    }

    FillMapWithGroupRequests();

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
            GetUsernameOrAnonymous(Context),
            Context->UserToken,
            Context->RequireAuthentication
        ));
        InflyTopics++;
    }
}

void TKafkaOffsetFetchActor::ExtractPartitions(const TString& group, const NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics& topic) {
    TString topicName = topic.Name.value();
    if (!TopicToEntities.contains(topicName)) {
        TTopicEntities newEntities;
        TopicToEntities[topicName] = newEntities;
    }
    TTopicEntities& entities = TopicToEntities[topicName];
    entities.Consumers->insert(group);
    for (auto partition: topic.PartitionIndexes) {
        entities.Partitions->insert(partition);
    }
};

void TKafkaOffsetFetchActor::ParseGroupsAssignments(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev,
                                                    std::vector<std::pair<std::optional<TString>, TConsumerProtocolAssignment>>& assignments) {
    if (!ev) {
        return;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    while (parser.TryNextRow()) {
        TString assignmentStr = parser.ColumnParser("assignment").GetOptionalString().value_or("");
        TString groupId = parser.ColumnParser("consumer_group").GetUtf8().c_str();
        if (!assignmentStr.empty()) {
            TKafkaBytes assignment = assignmentStr;
            TKafkaVersion version = *(TKafkaVersion*)(assignment.value().data() + sizeof(TKafkaVersion));
            TBuffer buffer(assignment.value().data() + sizeof(TKafkaVersion), assignment.value().size_bytes() - sizeof(TKafkaVersion));
            TKafkaReadable readable(buffer);

            TConsumerProtocolAssignment consumerAssignment;
            consumerAssignment.Read(readable, version);
            assignments.emplace_back(groupId, consumerAssignment);
        }
    }
}

void TKafkaOffsetFetchActor::CreateConsumerGroupIfNecessary(const TString& topicName,
                                    const TString& topicPath,
                                    const TString& originalTopicName,
                                    const TString& groupId) {
    TTopicGroupIdAndPath consumerTopicRequest = TTopicGroupIdAndPath{groupId, topicPath};
    if (ConsumerTopicAlterRequestAttempts.find(consumerTopicRequest) == ConsumerTopicAlterRequestAttempts.end()) {
        ConsumerTopicAlterRequestAttempts.insert(consumerTopicRequest);
    } else {
        // it is enough to send a consumer addition request only once for a particular topic
        return;
    }
    InflyTopics++;

    auto request = std::make_unique<Ydb::Topic::AlterTopicRequest>();
    request.get()->set_path(topicPath);
    auto* consumer = request->add_add_consumers();
    consumer->set_name(groupId);
    AlterTopicCookie++;
    AlterTopicCookieToName[AlterTopicCookie] = originalTopicName;
    auto callback = [replyTo = SelfId(), cookie = AlterTopicCookie, path = topicName, this]
        (Ydb::StatusIds::StatusCode statusCode, const google::protobuf::Message*) {
        NYdb::NIssue::TIssues issues;
        NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), std::move(issues));
        Send(replyTo,
            new NKikimr::NReplication::TEvYdbProxy::TEvAlterTopicResponse(std::move(status)),
            0,
            cookie);
    };
    NKikimr::NGRpcService::DoAlterTopicRequest(
        std::make_unique<NKikimr::NReplication::TLocalProxyRequest>(
        topicName, DatabasePath, std::move(request), callback, Context->UserToken),
        NKikimr::NReplication::TLocalProxyActor(DatabasePath));

}

void TKafkaOffsetFetchActor::CreateTopicIfNecessary(const TString& topicName,
                                                    const TString& originalTopicName,
                                                    const TActorContext& ctx) {
    if (TopicCreateRequestAttempts.find(topicName) != TopicCreateRequestAttempts.end()) {
        return;
    }
    TopicCreateRequestAttempts.insert(topicName);
    InflyTopics++;

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
    ContextForTopicCreation->RequireAuthentication = Context->RequireAuthentication;

    TActorId actorId = ctx.Register(new TKafkaCreateTopicsActor(ContextForTopicCreation,
        1,
        TMessagePtr<NKafka::TCreateTopicsRequestData>({}, message)));
    DependantActors.insert(actorId);
    CreateTopicActorIdToName[actorId] = originalTopicName;
}

TOffsetFetchResponseData::TPtr TKafkaOffsetFetchActor::GetOffsetFetchResponse() {
    TOffsetFetchResponseData::TPtr response = std::make_shared<TOffsetFetchResponseData>();
    for (const auto& requestGroup : Message->Groups) {
        TOffsetFetchResponseData::TOffsetFetchResponseGroup group;
        group.GroupId = requestGroup.GroupId.value();
        for (const auto& requestTopic: requestGroup.Topics) {
            TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics topic
                                    = GetOffsetResponseForTopic(requestTopic, *requestGroup.GroupId);
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
                partition.Metadata = sourcePartition.Metadata;
                partition.ErrorCode = sourcePartition.ErrorCode;
                topic.Partitions.push_back(partition);
            }
            response->Topics.push_back(topic);
        }
    }
    return response;
}

TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics TKafkaOffsetFetchActor::GetOffsetResponseForTopic(
                                    TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics const &requestTopic,
                                    const TString& groupId) {
    TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics topic;
    TString topicName = requestTopic.Name.value();
    topic.Name = topicName;
    if (TopicsToResponses[topicName]->Status == NONE_ERROR) {
        auto partitionsToOffsets = TopicsToResponses[topicName]->PartitionIdToOffsets;
        for (auto requestPartition: requestTopic.PartitionIndexes) {
            TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
            partition.PartitionIndex = requestPartition;
            if (partitionsToOffsets.get() != nullptr
                    && partitionsToOffsets->contains(requestPartition)) {
                auto groupPartitionToOffset = (*partitionsToOffsets)[requestPartition].find(groupId);
                if (groupPartitionToOffset != (*partitionsToOffsets)[requestPartition].end()) {
                    partition.CommittedOffset = groupPartitionToOffset->second.Offset;
                    partition.Metadata = groupPartitionToOffset->second.Metadata;
                    partition.ErrorCode = NONE_ERROR;
                } else {
                    partition.ErrorCode = RESOURCE_NOT_FOUND;
                    KAFKA_LOG_ERROR("Group " << groupId << " not found for topic " << topicName);
                }
            } else {
                partition.ErrorCode = RESOURCE_NOT_FOUND;
                KAFKA_LOG_ERROR("Partition " << requestPartition << " not found for topic " << topicName);
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
    return topic;
}
NYdb::TParamsBuilder TKafkaOffsetFetchActor::BuildFetchAssignmentsParams(const std::vector<std::optional<TString>>& groupIds) {
    NYdb::TParamsBuilder params;

    params.AddParam("$Database").Utf8(DatabasePath).Build();
    auto& consumerGroups = params.AddParam("$ConsumerGroups").BeginList();

    for (auto& groupId: groupIds) {
        consumerGroups.AddListItem().Utf8(*groupId);
    }
    consumerGroups.EndList().Build();

    return params;
}

void TKafkaOffsetFetchActor::FillMapWithGroupRequests() {
    for (const auto& groupRequest : Message->Groups) {
        for (auto& topicRequest : groupRequest.Topics) {
            GroupRequests[*topicRequest.Name].emplace_back(topicRequest, *groupRequest.GroupId);
        }
    }
}

void NKafka::TKafkaOffsetFetchActor::Die(const TActorContext &ctx) {
    KAFKA_LOG_D("Dying.");
    for (const TActorId& actorId : DependantActors) {
        Send(actorId, new TEvents::TEvPoisonPill());
    }
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
    TBase::Die(ctx);
}

}
