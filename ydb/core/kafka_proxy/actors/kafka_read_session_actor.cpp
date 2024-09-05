#include "kafka_read_session_actor.h"

namespace NKafka {
static constexpr TDuration WAKEUP_INTERVAL = TDuration::Seconds(1);
static constexpr TDuration LOCK_PARTITION_DELAY = TDuration::Seconds(3);
static const TString SUPPORTED_ASSIGN_STRATEGY = "roundrobin";
static const TString SUPPORTED_JOIN_GROUP_PROTOCOL = "consumer";

NActors::IActor* CreateKafkaReadSessionActor(const TContext::TPtr context, ui64 cookie) {
    return new TKafkaReadSessionActor(context, cookie);
}

void TKafkaReadSessionActor::Bootstrap(const NActors::TActorContext&) {
    Schedule(WAKEUP_INTERVAL, new TEvKafka::TEvWakeup());
    Become(&TKafkaReadSessionActor::StateWork);
}

TString TKafkaReadSessionActor::LogPrefix() {
    TStringBuilder sb;
    sb << "TKafkaReadSessionActor " << (Session == "" ? SelfId().ToString() : Session) << ": ";
    return sb;
}

void TKafkaReadSessionActor::Die(const TActorContext& ctx) {
    KAFKA_LOG_D("PassAway");
    for (auto& [topicName, topicInfo] : TopicsInfo) {
        NTabletPipe::CloseClient(ctx, topicInfo.PipeClient);
    }
    TBase::Die(ctx);
}

void TKafkaReadSessionActor::HandleWakeup(TEvKafka::TEvWakeup::TPtr, const TActorContext& ctx) {
    if (CheckHeartbeatIsExpired()) {
        KAFKA_LOG_D("Heartbeat expired");
        CloseReadSession(ctx);
        return;
    }

    for (auto& [topicName, partitions]: NewPartitionsToLockOnTime) {
        for (auto partitionsIt = partitions.begin(); partitionsIt != partitions.end(); ) {
            if (partitionsIt->LockOn <= ctx.Now()) {
                TopicPartitions[topicName].ToLock.emplace(partitionsIt->PartitionId);
                NeedRebalance = true;
                partitionsIt = partitions.erase(partitionsIt);
            } else {
                ++partitionsIt;
            }
        }
    }

    Schedule(WAKEUP_INTERVAL, new TEvKafka::TEvWakeup());
}

void TKafkaReadSessionActor::CloseReadSession(const TActorContext& /*ctx*/) {
    Send(Context->ConnectionId, new TEvKafka::TEvKillReadSession());
}

void TKafkaReadSessionActor::HandleJoinGroup(TEvKafka::TEvJoinGroupRequest::TPtr ev, const TActorContext& ctx) {
    auto joinGroupRequest = ev->Get()->Request;

    if (JoinGroupCorellationId != 0) {
        JoinGroupCorellationId = 0;
        SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, NextRequestError.Code, "JOIN_GROUP request already inflight");
        CloseReadSession(ctx);
        return;
    }

    if (NextRequestError.Code != NONE_ERROR) {
        SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, NextRequestError.Code, NextRequestError.Message);
        CloseReadSession(ctx);
        return;
    }

    if (joinGroupRequest->MemberId.has_value() && joinGroupRequest->MemberId != "" && joinGroupRequest->MemberId != Session) {
        SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, UNKNOWN_MEMBER_ID, TStringBuilder() << "unknown memberId# " << joinGroupRequest->MemberId);
        CloseReadSession(ctx);
        return;
    }

    if (!joinGroupRequest->GroupId.has_value() || (GroupId != "" && joinGroupRequest->GroupId.value() != GroupId)) {
        SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, INVALID_GROUP_ID, TStringBuilder() << "invalid groupId# " << joinGroupRequest->GroupId.value_or(""));
        CloseReadSession(ctx);
        return;
    }

    switch (ReadStep) {
        case WAIT_JOIN_GROUP: { // join first time
            if (joinGroupRequest->ProtocolType.has_value() && !joinGroupRequest->ProtocolType.value().empty() && joinGroupRequest->ProtocolType.value() != SUPPORTED_JOIN_GROUP_PROTOCOL) {
                SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, INVALID_REQUEST, TStringBuilder() << "unknown protocolType# " << joinGroupRequest->ProtocolType.value());
                CloseReadSession(ctx);
                return;
            }

            auto supportedProtocolFound = TryFillTopicsToRead(joinGroupRequest, TopicsToReadNames);

            GroupId = joinGroupRequest->GroupId.value();
            Session = TStringBuilder() << GroupId
                << "_" << ctx.SelfID.NodeId()
                << "_" << Cookie
                << "_" << TAppData::RandomProvider->GenRand64()
                << "_" << "kafka";

            if (!supportedProtocolFound) {
                SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, INCONSISTENT_GROUP_PROTOCOL, TStringBuilder() << "unsupported assign protocol. Must be " << SUPPORTED_ASSIGN_STRATEGY);
                CloseReadSession(ctx);
                return;
            }

            if (TopicsToReadNames.size() == 0) {
                SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, INVALID_REQUEST, "empty topics to read list");
                CloseReadSession(ctx);
                return;
            }

            JoinGroupCorellationId = ev->Get()->CorrelationId;
            AuthAndFindBalancers(ctx);
            break;
        }

        case READING: { // rejoin
            if (CheckTopicsListAreChanged(joinGroupRequest)) {
                SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, REBALANCE_IN_PROGRESS); // tell client rejoin to group
                CloseReadSession(ctx);
                return;
            }
            ReadStep = WAIT_SYNC_GROUP;
            SendJoinGroupResponseOk(ctx, ev->Get()->CorrelationId);
            break;
        }

        default: {
            SendJoinGroupResponseFail(ctx, ev->Get()->CorrelationId, UNKNOWN_MEMBER_ID, TStringBuilder() << "unknown memberId# " << joinGroupRequest->MemberId);
            CloseReadSession(ctx);
            return;
        }
    }

    if (joinGroupRequest->SessionTimeoutMs > 0) {
        MaxHeartbeatTimeoutMs = TDuration::MilliSeconds(joinGroupRequest->SessionTimeoutMs);
    }
}

void TKafkaReadSessionActor::HandleSyncGroup(TEvKafka::TEvSyncGroupRequest::TPtr ev, const TActorContext& ctx) {
    auto syncGroupRequest = ev->Get()->Request;

    if (NextRequestError.Code != NONE_ERROR) {
        SendSyncGroupResponseFail(ctx, ev->Get()->CorrelationId, NextRequestError.Code, NextRequestError.Message);
        CloseReadSession(ctx);
        return;
    }

    if (ReadStep != WAIT_SYNC_GROUP || (syncGroupRequest->MemberId.has_value() && syncGroupRequest->MemberId != "" && syncGroupRequest->MemberId != Session)) {
        SendSyncGroupResponseFail(ctx, ev->Get()->CorrelationId, UNKNOWN_MEMBER_ID, TStringBuilder() << "unknown memberId# " << syncGroupRequest->MemberId);
        CloseReadSession(ctx);
        return;
    }

    if (syncGroupRequest->ProtocolType.has_value() && !syncGroupRequest->ProtocolType.value().empty() && syncGroupRequest->ProtocolType.value() != SUPPORTED_JOIN_GROUP_PROTOCOL) {
        SendSyncGroupResponseFail(ctx, ev->Get()->CorrelationId, INVALID_REQUEST, TStringBuilder() << "unknown protocolType# " << syncGroupRequest->ProtocolType.value());
        CloseReadSession(ctx);
        return;
    }

    if (syncGroupRequest->GroupId != GroupId) {
        SendSyncGroupResponseFail(ctx, ev->Get()->CorrelationId, INVALID_GROUP_ID, TStringBuilder() << "invalid groupId# " << syncGroupRequest->GroupId.value_or(""));
        CloseReadSession(ctx);
        return;
    }

    if (syncGroupRequest->GenerationId != GenerationId) {
        SendSyncGroupResponseFail(ctx, ev->Get()->CorrelationId, ILLEGAL_GENERATION, TStringBuilder() << "illegal generationId# " << syncGroupRequest->GenerationId << ", must be " << GenerationId);
        return;
    }

    ReadStep = READING;
    SendSyncGroupResponseOk(ctx, ev->Get()->CorrelationId);
    NeedRebalance = false;
}

void TKafkaReadSessionActor::HandleLeaveGroup(TEvKafka::TEvLeaveGroupRequest::TPtr ev, const TActorContext& ctx) {
    auto leaveGroupRequest = ev->Get()->Request;

    if (NextRequestError.Code != NONE_ERROR) {
        SendLeaveGroupResponseFail(ctx, ev->Get()->CorrelationId, NextRequestError.Code, NextRequestError.Message);
        CloseReadSession(ctx);
        return;
    }

    if (ReadStep == EReadSessionSteps::WAIT_JOIN_GROUP || (leaveGroupRequest->MemberId.has_value() && leaveGroupRequest->MemberId != "" && leaveGroupRequest->MemberId != Session)) {
        SendLeaveGroupResponseFail(ctx, ev->Get()->CorrelationId, UNKNOWN_MEMBER_ID, TStringBuilder() << "unknown memberId# " << leaveGroupRequest->MemberId.value_or(""));
        CloseReadSession(ctx);
        return;
    }

    if (leaveGroupRequest->GroupId != GroupId) {
        SendLeaveGroupResponseFail(ctx, ev->Get()->CorrelationId, INVALID_GROUP_ID, TStringBuilder() << "invalid groupId# " << leaveGroupRequest->GroupId.value_or(""));
        CloseReadSession(ctx);
        return;
    }

    SendLeaveGroupResponseOk(ctx, ev->Get()->CorrelationId);
    CloseReadSession(ctx);
}

void TKafkaReadSessionActor::HandleHeartbeat(TEvKafka::TEvHeartbeatRequest::TPtr ev, const TActorContext& ctx) {
    auto heartbeatRequest = ev->Get()->Request;

    if (NextRequestError.Code != NONE_ERROR) {
        SendHeartbeatResponseFail(ctx, ev->Get()->CorrelationId, NextRequestError.Code, NextRequestError.Message);
        CloseReadSession(ctx);
        return;
    }

    if (ReadStep != READING || heartbeatRequest->MemberId != Session || CheckHeartbeatIsExpired()) {
        SendHeartbeatResponseFail(ctx, ev->Get()->CorrelationId, EKafkaErrors::UNKNOWN_MEMBER_ID, TStringBuilder() << "unknown memberId# " << heartbeatRequest->MemberId);
        CloseReadSession(ctx);
        return;
    }

    if (heartbeatRequest->GroupId != GroupId) {
        SendHeartbeatResponseFail(ctx, ev->Get()->CorrelationId, INVALID_GROUP_ID, TStringBuilder() << "invalid groupId# " << heartbeatRequest->GroupId.value_or(""));
        CloseReadSession(ctx);
        return;
    }

    LastHeartbeatTime = TInstant::Now();
    EKafkaErrors error = NeedRebalance || GenerationId != heartbeatRequest->GenerationId ? EKafkaErrors::REBALANCE_IN_PROGRESS : EKafkaErrors::NONE_ERROR; // if REBALANCE_IN_PROGRESS, client rejoin
    SendHeartbeatResponseOk(ctx, ev->Get()->CorrelationId, error);
}

void TKafkaReadSessionActor::SendJoinGroupResponseOk(const TActorContext&, ui64 corellationId) {
    TJoinGroupResponseData::TPtr response = std::make_shared<TJoinGroupResponseData>();

    response->ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;
    response->ProtocolName = SUPPORTED_ASSIGN_STRATEGY;
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    response->GenerationId = GenerationId;
    response->MemberId = Session;

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
}

void TKafkaReadSessionActor::SendJoinGroupResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message) {
    KAFKA_LOG_CRIT("JOIN_GROUP failed. reason# " << message);
    TJoinGroupResponseData::TPtr response = std::make_shared<TJoinGroupResponseData>();

    response->ErrorCode = error;

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaReadSessionActor::SendSyncGroupResponseOk(const TActorContext& ctx, ui64 corellationId) {
    TSyncGroupResponseData::TPtr response = std::make_shared<TSyncGroupResponseData>();

    response->ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;
    response->ProtocolName = SUPPORTED_ASSIGN_STRATEGY;
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    response->Assignment = BuildAssignmentAndInformBalancerIfRelease(ctx);

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
}

void TKafkaReadSessionActor::SendSyncGroupResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message) {
    KAFKA_LOG_CRIT("SYNC_GROUP failed. reason# " << message);
    TSyncGroupResponseData::TPtr response = std::make_shared<TSyncGroupResponseData>();

    response->ErrorCode = error;

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaReadSessionActor::SendLeaveGroupResponseOk(const TActorContext&, ui64 corellationId) {
    TLeaveGroupResponseData::TPtr response = std::make_shared<TLeaveGroupResponseData>();

    response->ErrorCode = EKafkaErrors::NONE_ERROR;

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
}

void TKafkaReadSessionActor::SendLeaveGroupResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message) {
    KAFKA_LOG_CRIT("LEAVE_GROUP failed. reason# " << message);
    TLeaveGroupResponseData::TPtr response = std::make_shared<TLeaveGroupResponseData>();

    response->ErrorCode = error;

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaReadSessionActor::SendHeartbeatResponseOk(const TActorContext&, ui64 corellationId, EKafkaErrors error) {
    THeartbeatResponseData::TPtr response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaReadSessionActor::SendHeartbeatResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message) {
    THeartbeatResponseData::TPtr response = std::make_shared<THeartbeatResponseData>();
    KAFKA_LOG_CRIT("HEARTBEAT failed. reason# " << message);
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

bool TKafkaReadSessionActor::CheckTopicsListAreChanged(const TMessagePtr<TJoinGroupRequestData> joinGroupRequestData) {
    THashSet<TString> topics;

    auto supportedProtocolFound = TryFillTopicsToRead(joinGroupRequestData, topics);
    if (!supportedProtocolFound) {
        return true;
    }

    return topics != TopicsToReadNames;
}

bool TKafkaReadSessionActor::CheckHeartbeatIsExpired() {
    auto now = TInstant::Now();
    return now - LastHeartbeatTime > MaxHeartbeatTimeoutMs;
}

bool TKafkaReadSessionActor::TryFillTopicsToRead(const TMessagePtr<TJoinGroupRequestData> joinGroupRequestData, THashSet<TString>& topics) {
    auto supportedProtocolFound = false;
    for (auto protocol: joinGroupRequestData->Protocols) {
        KAFKA_LOG_D("JOIN_GROUP assign protocol supported by client: " << protocol.Name);
        if (protocol.Name == SUPPORTED_ASSIGN_STRATEGY) {
            FillTopicsFromJoinGroupMetadata(protocol.Metadata, topics);
            supportedProtocolFound = true;
            break;
        }
    }
    return supportedProtocolFound;
}

TConsumerProtocolAssignment TKafkaReadSessionActor::BuildAssignmentAndInformBalancerIfRelease(const TActorContext& ctx) {
    TConsumerProtocolAssignment assignment;
    KAFKA_LOG_D("SYNC_GROUP topics to assign count: " << TopicPartitions.size());
    for (auto& [topicName, partitions] : TopicPartitions) {
        auto topicInfoIt = TopicsInfo.find(topicName);

        Y_ABORT_UNLESS(topicInfoIt != TopicsInfo.end());

        THashSet<ui64> finalPartitionsToRead;

        TConsumerProtocolAssignment::TopicPartition topicPartition;
        topicPartition.Topic = OriginalTopicNames[topicName];
        for (auto part: partitions.ToLock) {
            finalPartitionsToRead.emplace(part);
        }
        partitions.ToLock.clear();

        for (auto part: partitions.ReadingNow) {
            finalPartitionsToRead.emplace(part);
        }
        partitions.ReadingNow.clear();

        for (auto part: partitions.ToRelease) {
            finalPartitionsToRead.erase(part);
            InformBalancerAboutPartitionRelease(topicName, part, ctx);
        }
        partitions.ToRelease.clear();

        KAFKA_LOG_D("SYNC_GROUP partitions assigned: " << finalPartitionsToRead.size());
        for (auto part: finalPartitionsToRead) {
            KAFKA_LOG_D("SYNC_GROUP assigned partition number: " << part);
            topicPartition.Partitions.push_back(part);
            partitions.ReadingNow.emplace(part);
        }
        assignment.AssignedPartitions.push_back(topicPartition);
    }

    return assignment;
}

void TKafkaReadSessionActor::FillTopicsFromJoinGroupMetadata(TKafkaBytes& metadata, THashSet<TString>& topics) {
    TKafkaVersion version = *(TKafkaVersion*)(metadata.value().data() + sizeof(TKafkaVersion));

    TBuffer buffer(metadata.value().data() + sizeof(TKafkaVersion), metadata.value().size_bytes() - sizeof(TKafkaVersion));
    TKafkaReadable readable(buffer);

    TConsumerProtocolSubscription result;
    result.Read(readable, version);

    for (auto topic: result.Topics) {
        if (topic.has_value()) {
            auto normalizedTopicName = NormalizePath(Context->DatabasePath, topic.value());
            OriginalTopicNames[normalizedTopicName] = topic.value();
            topics.emplace(normalizedTopicName);
            KAFKA_LOG_D("JOIN_GROUP requested topic to read: " << topic);
        }
    }
}

void TKafkaReadSessionActor::HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&) {
    const auto* msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        if (msg->Dead) {
            NextRequestError.Code = EKafkaErrors::INVALID_REQUEST;
            NextRequestError.Message = TStringBuilder()
                << "one of topics is deleted, tabletId# " << msg->TabletId;
        } else {
            NextRequestError.Code = EKafkaErrors::UNKNOWN_SERVER_ERROR;
            NextRequestError.Message = TStringBuilder()
                << "unable to connect to one of topics, tabletId# " << msg->TabletId;
        }
    }
}

void TKafkaReadSessionActor::HandlePipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    ProcessBalancerDead(ev->Get()->TabletId, ctx);
}

void TKafkaReadSessionActor::ProcessBalancerDead(ui64 tabletId, const TActorContext& ctx) {
    NewPartitionsToLockOnTime.clear();

    for (auto& [topicName, topicInfo] : TopicsInfo) {
        if (topicInfo.TabletID == tabletId) {
            auto partitionsIt = TopicPartitions.find(topicName);
            if (partitionsIt == TopicPartitions.end()) {
                return;
            }
            NeedRebalance = true;
            //release all partitions
            partitionsIt->second.ToLock.clear();
            partitionsIt->second.ToRelease.clear();
            for (auto readedPartition: partitionsIt->second.ReadingNow) {
                partitionsIt->second.ToRelease.emplace(readedPartition);
            }

            topicInfo.PipeClient = CreatePipeClient(topicInfo.TabletID, ctx);
            RegisterBalancerSession(topicInfo.FullConverter->GetInternalName(), topicInfo.PipeClient, topicInfo.Groups, ctx);
            return;
        }
    }

}

void TKafkaReadSessionActor::AuthAndFindBalancers(const TActorContext& ctx) {

    auto topicConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
        AppData(ctx)->PQConfig, ""
    );
    auto topicHandler = std::make_unique<NPersQueue::TTopicsListController>(
        topicConverterFactory
    );

    TopicsToConverter = topicHandler->GetReadTopicsList(TopicsToReadNames, false, Context->DatabasePath);
    if (!TopicsToConverter.IsValid) {
        SendJoinGroupResponseFail(ctx, JoinGroupCorellationId, INVALID_REQUEST, TStringBuilder() << "topicsToConverter is not valid");
        return;
    }

    ctx.Register(new NGRpcProxy::V1::TReadInitAndAuthActor(
        ctx, ctx.SelfID, GroupId, Cookie, Session, NMsgBusProxy::CreatePersQueueMetaCacheV2Id(), MakeSchemeCacheID(), nullptr, Context->UserToken, TopicsToConverter,
        topicHandler->GetLocalCluster(), false));
}

void TKafkaReadSessionActor::HandleBalancerError(TEvPersQueue::TEvError::TPtr& ev, const TActorContext& ctx) {
    if (JoinGroupCorellationId != 0) {
        SendJoinGroupResponseFail(ctx, JoinGroupCorellationId, ConvertErrorCode(ev->Get()->Record.GetCode()), ev->Get()->Record.GetDescription());
        CloseReadSession(ctx);
        JoinGroupCorellationId = 0;
    } else {
        NextRequestError.Code = ConvertErrorCode(ev->Get()->Record.GetCode());
        NextRequestError.Message = ev->Get()->Record.GetDescription();
    }
}

void TKafkaReadSessionActor::HandleAuthOk(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("JOIN_GROUP auth success. Topics count: " << ev->Get()->TopicAndTablets.size());

    for (const auto& [name, t] : ev->Get()->TopicAndTablets) {
        auto internalName = t.TopicNameConverter->GetInternalName();
        TopicsInfo[internalName] = NGRpcProxy::TTopicHolder::FromTopicInfo(t);
        FullPathToConverter[t.TopicNameConverter->GetPrimaryPath()] = t.TopicNameConverter;
        FullPathToConverter[t.TopicNameConverter->GetSecondaryPath()] = t.TopicNameConverter;
    }

    Send(Context->ConnectionId, new TEvKafka::TEvReadSessionInfo(GroupId));

    for (auto& [topicName, topicInfo] : TopicsInfo) {
        topicInfo.PipeClient = CreatePipeClient(topicInfo.TabletID, ctx);
        RegisterBalancerSession(topicInfo.FullConverter->GetInternalName(), topicInfo.PipeClient, topicInfo.Groups, ctx);
    }

    if (JoinGroupCorellationId != 0) {
        SendJoinGroupResponseOk(ctx, JoinGroupCorellationId);
        JoinGroupCorellationId = 0;
        ReadStep = WAIT_SYNC_GROUP;
    }
}

void TKafkaReadSessionActor::HandleAuthCloseSession(NGRpcProxy::V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    if (JoinGroupCorellationId != 0) {
        SendJoinGroupResponseFail(ctx, JoinGroupCorellationId, ConvertErrorCode(ev->Get()->ErrorCode), TStringBuilder() << "auth failed. " << ev->Get()->Reason);
        JoinGroupCorellationId = 0;
    }

    CloseReadSession(ctx);
}

TActorId TKafkaReadSessionActor::CreatePipeClient(ui64 tabletId, const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.CheckAliveness = false;
    clientConfig.RetryPolicy = RetryPolicyForPipes;
    return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
}

void TKafkaReadSessionActor::RegisterBalancerSession(const TString& topic, const TActorId& pipe, const TVector<ui32>& groups, const TActorContext& ctx) {
    KAFKA_LOG_I("register session: topic# " << topic );
    auto request = MakeHolder<TEvPersQueue::TEvRegisterReadSession>();

    auto& req = request->Record;
    req.SetSession(Session);
    req.SetClientNode(Context->KafkaClient);
    ActorIdToProto(pipe, req.MutablePipeClient());
    req.SetClientId(GroupId);

    for (ui32 i = 0; i < groups.size(); ++i) {
        req.AddGroups(groups[i]);
    }

    NTabletPipe::SendData(ctx, pipe, request.Release());
}

void TKafkaReadSessionActor::HandleLockPartition(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    KAFKA_LOG_D("partition lock is coming from PQRB topic# " << record.GetTopic() <<  ", partition# " << record.GetPartition());

    Y_ABORT_UNLESS(record.GetSession() == Session);
    Y_ABORT_UNLESS(record.GetClientId() == GroupId);
    Y_ABORT_UNLESS(record.GetGeneration() > 0);

    auto path = record.GetPath();
    if (path.empty()) {
        path = record.GetTopic();
    }

    auto converterIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(path));
    if (converterIter == FullPathToConverter.end()) {
        KAFKA_LOG_I("ignored ev lock topic# " << record.GetTopic()
                 << ", partition# " << record.GetPartition()
                 << ", reason# path not recognized");
        return;
    }

    const auto topicName = converterIter->second->GetInternalName();

    auto topicInfoIt = TopicsInfo.find(topicName);
    if (topicInfoIt == TopicsInfo.end() || (topicInfoIt->second.PipeClient != ActorIdFromProto(record.GetPipeClient()))) {
        KAFKA_LOG_I("ignored ev lock topic# " << record.GetTopic()
                 << ", partition# " << record.GetPartition()
                 << ", reason# topic is unknown");
        return;
    }

    TNewPartitionToLockInfo partitionToLock;
    partitionToLock.LockOn = ctx.Now() + LOCK_PARTITION_DELAY;
    partitionToLock.PartitionId = record.GetPartition();
    NewPartitionsToLockOnTime[topicName].push_back(partitionToLock);
}

void TKafkaReadSessionActor::HandleReleasePartition(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const ui32 group = record.HasGroup() ? record.GetGroup() : 0;
    KAFKA_LOG_D("partition release is coming from PQRB topic# " << record.GetTopic() <<  ", group# " << group);

    Y_ABORT_UNLESS(record.GetSession() == Session);
    Y_ABORT_UNLESS(record.GetClientId() == GroupId);

    auto pathIt = FullPathToConverter.find(NPersQueue::NormalizeFullPath(record.GetPath()));
    Y_ABORT_UNLESS(pathIt != FullPathToConverter.end());

    auto topicInfoIt = TopicsInfo.find(pathIt->second->GetInternalName());
    Y_ABORT_UNLESS(topicInfoIt != TopicsInfo.end());

    if (topicInfoIt->second.PipeClient != ActorIdFromProto(record.GetPipeClient())) {
        KAFKA_LOG_I("ignored ev release topic# " << record.GetTopic()
                 << ", reason# topic is unknown");
        return;
    }

    auto newPartitionsToLockIt = NewPartitionsToLockOnTime.find(pathIt->second->GetInternalName());
    auto newPartitionsToLockCount = newPartitionsToLockIt == NewPartitionsToLockOnTime.end() ? 0 : newPartitionsToLockIt->second.size();

    auto topicPartitionsIt = TopicPartitions.find(pathIt->second->GetInternalName());
    Y_ABORT_UNLESS(1 <= (topicPartitionsIt.IsEnd() ? 0 : topicPartitionsIt->second.ToLock.size() + topicPartitionsIt->second.ReadingNow.size()) + newPartitionsToLockCount);

    auto partitionToRelease = record.GetGroup() - 1;

    if (newPartitionsToLockIt != NewPartitionsToLockOnTime.end()) {
        auto& newPartitions = newPartitionsToLockIt->second;
        for (auto& newPartition : newPartitions) {
            if (newPartition.PartitionId == partitionToRelease) {
                InformBalancerAboutPartitionRelease(topicInfoIt->first, partitionToRelease, ctx);

                auto tmp = std::move(newPartitions);
                newPartitions.reserve(tmp.size() - 1);

                for (auto& t : tmp) {
                    if (t.PartitionId != partitionToRelease) {
                        newPartitions.push_back(t);
                    }
                }

                return;
            }
        }
    }

    if (topicPartitionsIt != TopicPartitions.end()) {
        if (topicPartitionsIt->second.ToLock.contains(partitionToRelease)) {
            InformBalancerAboutPartitionRelease(topicInfoIt->first, partitionToRelease, ctx);
            topicPartitionsIt->second.ToLock.erase(partitionToRelease);
            return;
        }

        if (topicPartitionsIt->second.ReadingNow.contains(partitionToRelease) && !topicPartitionsIt->second.ToRelease.contains(partitionToRelease)) {
            InformBalancerAboutPartitionRelease(topicInfoIt->first, partitionToRelease, ctx);
            NeedRebalance = true;
            topicPartitionsIt->second.ReadingNow.erase(partitionToRelease);
            return;
        }
    }

    KAFKA_LOG_I("ignored ev release topic# " << record.GetTopic()
             << ", reason# partition " << partitionToRelease << " isn`t locked");
}

void TKafkaReadSessionActor::InformBalancerAboutPartitionRelease(const TString& topic, ui64 partition, const TActorContext& ctx) {
    KAFKA_LOG_I("released topic# " << topic
             << ", partition# " << partition);
    auto request = MakeHolder<TEvPersQueue::TEvPartitionReleased>();

    auto topicIt = TopicsInfo.find(topic);
    Y_ABORT_UNLESS(topicIt != TopicsInfo.end());

    auto& req = request->Record;
    req.SetSession(Session);
    ActorIdToProto(topicIt->second.PipeClient, req.MutablePipeClient());
    req.SetClientId(GroupId);
    req.SetTopic(topicIt->second.FullConverter->GetPrimaryPath());
    req.SetPartition(partition);

    NTabletPipe::SendData(ctx, topicIt->second.PipeClient, request.Release());
}

} // namespace NKafka
