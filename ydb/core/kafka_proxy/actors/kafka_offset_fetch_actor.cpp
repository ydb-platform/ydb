#include "kafka_offset_fetch_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>

#include <ydb/core/kafka_proxy/actors/kafka_create_topics_actor.h>
#include <ydb/core/kafka_proxy/actors/kafka_metadata_service.h>
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>
#include "ydb/core/kafka_proxy/kafka_consumer_members_metadata_initializers.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KAFKA_PROXY

namespace NKafka {

NActors::IActor* CreateKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message) {
    return new TKafkaOffsetFetchActor(context, correlationId, message);
}

void TKafkaOffsetFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    // If API level <= 7, Groups would be empty. In this case we convert message to level 8 and process it uniformely later
    YDB_LOG_DEBUG("New request for user",
        {LogPrefix()},
        {"userName", GetUsernameOrAnonymous(Context)});

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
        if (Context->KafkaTableFeatureFlagChanged(NKikimr::AppData()->FeatureFlags.GetEnableKafkaServerlessTransactions())) {
            Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, std::make_shared<TOffsetFetchResponseData>(), EKafkaErrors::COORDINATOR_NOT_AVAILABLE));
            Die(ctx);
            return;
        }
        // if topics were not specified for some groups,
        // topics for such groups will be retrieved from the table
        if (!NKikimr::AppData()->FeatureFlags.GetEnableKafkaServerlessTransactions()) {
            Kqp = std::make_unique<TKqpTxHelper>(Context->ResourceDatabasePath);
        } else {
            Kqp = std::make_unique<TKqpTxHelper>(Context->DatabasePath);
        }
        Kqp->SendCreateSessionRequest(ctx);
        YDB_LOG_DEBUG("Creating KQP Session",
            {LogPrefix()});
    } else {
        FillMapWithGroupRequests();
        for (const auto& topicToEntities : TopicToEntities) {
            RegisterOffsetsActor(topicToEntities.first, ctx);
            InflyTopics++;
        }
    }
    Become(&TKafkaOffsetFetchActor::StateWork);
}

void TKafkaOffsetFetchActor::Handle(TEvKafka::TEvTopicOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
    InflyTopics--;

    const auto topicIt = OffsetsActorToTopic.find(ev->Sender);
    const TString topicName = topicIt != OffsetsActorToTopic.end() ? topicIt->second : TString();

    auto converted = MakeHolder<TEvKafka::TEvCommitedOffsetsResponse>();
    converted->TopicName = topicName;
    converted->Status = ConvertErrorCode(ev->Get()->Status);
    if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
        auto offsets = std::make_shared<std::unordered_map<ui32, std::unordered_map<TString, TEvKafka::PartitionConsumerOffset>>>();
        for (const auto& part : ev->Get()->Partitions) {
            auto& consumers = (*offsets)[part.PartitionId];
            for (const auto& [consumer, offset] : part.Consumers) {
                consumers.emplace(consumer, offset);
            }
        }
        converted->PartitionIdToOffsets = std::move(offsets);
    }
    if (converted->Status == NONE_ERROR) {
        Context->RememberTopicAclOk(topicName);
    } else if (converted->Status == UNKNOWN_TOPIC_OR_PARTITION && Context->HadTopicAclOk(topicName)) {
        converted->Status = TOPIC_AUTHORIZATION_FAILED;
    }
    const bool topicExists = converted->Status == NONE_ERROR;
    TopicsToResponses[topicName].Reset(converted.Release());
    auto& topicGroupRequests = GroupRequests[topicName];
    for (const auto& [topicRequest, groupId] : topicGroupRequests) {
        TString topicNameWithoutDb = GetTopicNameWithoutDb(DatabasePath, *topicRequest.Name);
        TString topicPath = NormalizePath(DatabasePath, topicNameWithoutDb);
        if (topicExists && Context->Config.GetAutoCreateConsumersEnable()) {
            auto partitionsToOffsets = TopicsToResponses[topicName]->PartitionIdToOffsets;
            bool consumerOnTopic = false;
            if (partitionsToOffsets) {
                for (const auto& [_, consumers] : *partitionsToOffsets) {
                    if (consumers.contains(groupId)) {
                        consumerOnTopic = true;
                        break;
                    }
                }
            }
            if (!consumerOnTopic) {
                CreateConsumerGroupIfNecessary(topicNameWithoutDb, topicPath, topicNameWithoutDb, groupId);
            }
        }
        // Do not auto-create a topic reported as unknown. Apache Kafka OffsetFetch never creates
        // topics (auto.create.topics.enable applies to Metadata, not OffsetFetch). For a missing
        // topic/partition the group coordinator returns NONE + committedOffset -1; see
        // https://issues.apache.org/jira/browse/KAFKA-20165. Scheme cache also uses "unknown" to
        // hide topics without DescribeSchema; auto-create on that path would grant access. If this
        // connection already saw the topic with access, the unknown describe is mapped to AUTH.
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

    RegisterOffsetsActor(createdTopicName, ctx);
}

void TKafkaOffsetFetchActor::Handle(NKikimr::NReplication::TEvYdbProxy::TEvAlterTopicResponse::TPtr& ev, const TActorContext& ctx) {
    NYdb::TStatus& result = ev->Get()->Result;
    if (result.GetStatus() == NYdb::EStatus::SUCCESS) {
        YDB_LOG_DEBUG("Handling TEvAlterTopicResponse. \n",
            {LogPrefix()},
            {"status", result.GetStatus()});
    } else {
        YDB_LOG_INFO("Handling TEvAlterTopicResponse. \n",
            {LogPrefix()},
            {"status", result.GetStatus()});
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
    RegisterOffsetsActor(alteredTopicName, ctx);
}

void TKafkaOffsetFetchActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_DEBUG("Got KQP CreateSession response",
        {LogPrefix()});
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, std::make_shared<TOffsetFetchResponseData>(), EKafkaErrors::UNKNOWN_SERVER_ERROR));
        YDB_LOG_DEBUG("KQP Session Error",
            {LogPrefix()});
        return;
    }
    NYdb::TParamsBuilder params = BuildFetchAssignmentsParams(GroupsToFetch);
    Kqp->SendYqlRequest(Sprintf(FETCH_ASSIGNMENTS.c_str(), NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant()->FormPathToResourceTable(GetMetadataDatabasePath()).c_str()), params.Build(), KqpCookie, ctx);
}

void NKafka::TKafkaOffsetFetchActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    std::vector<std::pair<std::optional<TString>, TConsumerProtocolAssignment>> assignments;
    YDB_LOG_DEBUG("Received KQP response",
        {LogPrefix()});
    if (ev && TryRequestConsumerMetadataTablesCreation(ev->Get()->Record.GetYdbStatus(), GetMetadataDatabasePath(), Context->ResourceDatabasePath, ctx)) {
        auto response = GetOffsetFetchResponse();
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, EKafkaErrors::COORDINATOR_NOT_AVAILABLE));
        Die(ctx);
        return;
    }

    ParseGroupsAssignments(ev, assignments);

    if (assignments.empty()) {
        auto response = GetOffsetFetchResponse();
        YDB_LOG_DEBUG("Sending response to user",
            {LogPrefix()},
            {"userName", GetUsernameOrAnonymous(Context)});
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
        RegisterOffsetsActor(topicToEntities.first, ctx);
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
            auto consumerAssignment = TryReadConsumerProtocolBlob<TConsumerProtocolAssignment>(assignment);
            if (!consumerAssignment) {
                continue;
            }
            assignments.emplace_back(groupId, *consumerAssignment);
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
        topicName, DatabasePath, std::move(request), callback, Context->Token.UserToken),
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
    ContextForTopicCreation->Token.UserToken = Context->Token.UserToken;
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
                    // Existing partition, no committed offset for this group.
                    partition.CommittedOffset = -1;
                    partition.ErrorCode = NONE_ERROR;
                    YDB_LOG_DEBUG("No committed offset for group on partition",
                        {LogPrefix()},
                        {"groupId", groupId},
                        {"topicName", topicName},
                        {"requestPartition", requestPartition});
                }
            } else {
                // Kafka OffsetFetch does not fail on an unknown partition:
                // NONE + committedOffset = -1.
                partition.CommittedOffset = -1;
                partition.ErrorCode = NONE_ERROR;
                YDB_LOG_DEBUG("Partition not found for topic",
                    {LogPrefix()},
                    {"requestPartition", requestPartition},
                    {"topicName", topicName});
            }
            topic.Partitions.push_back(partition);
        }
    } else if (TopicsToResponses[topicName]->Status == UNKNOWN_TOPIC_OR_PARTITION) {
        // Kafka coordinator OffsetFetch does not check that the topic exists.
        for (auto requestPartition: requestTopic.PartitionIndexes) {
            TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
            partition.PartitionIndex = requestPartition;
            partition.CommittedOffset = -1;
            partition.ErrorCode = NONE_ERROR;
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

void TKafkaOffsetFetchActor::RegisterOffsetsActor(const TString& topicName, const TActorContext& ctx) {
    const auto& entities = TopicToEntities[topicName];
    const auto actorId = ctx.Register(CreateTopicOffsetsActor(SelfId(), {
        .Path = NormalizePath(Context->DatabasePath, topicName),
        .Database = Context->DatabasePath,
        .Token = GetUserSerializedToken(Context),
        .SelectRowToken = GetUserSerializedToken(Context),
        .PartitionIds = TVector<ui32>(entities.Partitions->begin(), entities.Partitions->end()),
        .Consumers = TVector<TString>(entities.Consumers->begin(), entities.Consumers->end()),
        .RequireSelectRow = true,
        .RequireAuthentication = Context->RequireAuthentication,
    }));
    OffsetsActorToTopic[actorId] = topicName;
}

void NKafka::TKafkaOffsetFetchActor::Die(const TActorContext &ctx) {
    YDB_LOG_DEBUG("Dying",
        {LogPrefix()});
    for (const TActorId& actorId : DependantActors) {
        Send(actorId, new TEvents::TEvPoisonPill());
    }
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
    TBase::Die(ctx);
}

TString NKafka::TKafkaOffsetFetchActor::GetMetadataDatabasePath() const {
    return NKikimr::AppData()->FeatureFlags.GetEnableKafkaServerlessTransactions() ? Context->DatabasePath : Context->ResourceDatabasePath;
}

}
