#include "kafka_offset_commit_actor.h"

namespace NKafka {


NActors::IActor* CreateKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message) {
    return new TKafkaOffsetCommitActor(context, correlationId, message);
}

TString TKafkaOffsetCommitActor::LogPrefix() {
    return "TKafkaOffsetCommitActor";
}

void TKafkaOffsetCommitActor::Die(const TActorContext& ctx) {
    KAFKA_LOG_D("PassAway");
    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());
    for (const auto& tabletToPipePair: TabletIdToPipe) {
        NTabletPipe::CloseClient(ctx, tabletToPipePair.second);
    }
    TBase::Die(ctx);
}

void TKafkaOffsetCommitActor::Handle(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_CRIT("Auth failed. reason# " << ev->Get()->Reason);
    Error = ConvertErrorCode(ev->Get()->ErrorCode);
    SendFailedForAllPartitions(Error, ctx);
}

void TKafkaOffsetCommitActor::SendFailedForAllPartitions(EKafkaErrors error, const TActorContext& ctx) {
    for (auto topicReq: Message->Topics) {
        TOffsetCommitResponseData::TOffsetCommitResponseTopic topic;
        topic.Name = topicReq.Name;
        for (auto partitionRequest: topicReq.Partitions) {
            TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition partition;
            partition.PartitionIndex = partitionRequest.PartitionIndex;
            partition.ErrorCode = error;
            topic.Partitions.push_back(partition);
        }
        Response->Topics.push_back(topic);
    }
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, Error));
    Die(ctx);
}

void TKafkaOffsetCommitActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        KAFKA_LOG_CRIT("Pipe to tablet is dead. status# " << ev->Get()->Status);
        ProcessPipeProblem(msg->TabletId, ctx);
    }
}

void TKafkaOffsetCommitActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_CRIT("Pipe to tablet is destroyed");
    ProcessPipeProblem(ev->Get()->TabletId, ctx);
}

void TKafkaOffsetCommitActor::ProcessPipeProblem(ui64 tabletId, const TActorContext& ctx) {
    auto cookiesIt = TabletIdToCookies.find(tabletId);
    Y_ABORT_UNLESS(cookiesIt != TabletIdToCookies.end());

    for (auto cookie: cookiesIt->second) {
        auto requestInfoIt = CookieToRequestInfo.find(cookie);
        Y_ABORT_UNLESS(requestInfoIt != CookieToRequestInfo.end());

        if (!requestInfoIt->second.Done) {
            requestInfoIt->second.Done = true;
            AddPartitionResponse(EKafkaErrors::UNKNOWN_SERVER_ERROR, requestInfoIt->second.TopicName, requestInfoIt->second.PartitionId, ctx);
        }
    }
}

void TKafkaOffsetCommitActor::Handle(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("Auth success. Topics count: " << ev->Get()->TopicAndTablets.size());
    TopicAndTablets = std::move(ev->Get()->TopicAndTablets);

    for (auto topicReq: Message->Topics) {
        auto topicIt = TopicAndTablets.find(NormalizePath(Context->DatabasePath, topicReq.Name.value()));
        for (auto partitionRequest: topicReq.Partitions) {
            if (topicIt == TopicAndTablets.end()) {
                AddPartitionResponse(UNKNOWN_TOPIC_OR_PARTITION, topicReq.Name.value(), partitionRequest.PartitionIndex, ctx);
                continue;
            }

            auto tabletIdIt = topicIt->second.Partitions.find(partitionRequest.PartitionIndex);
            if (tabletIdIt == topicIt->second.Partitions.end()) {
                AddPartitionResponse(UNKNOWN_TOPIC_OR_PARTITION, topicReq.Name.value(), partitionRequest.PartitionIndex, ctx);
                continue;
            }

            ui64 tabletId = tabletIdIt->second.TabletId;

            if (!TabletIdToPipe.contains(tabletId)) {
                NTabletPipe::TClientConfig clientConfig;
                clientConfig.RetryPolicy = RetryPolicyForPipes;
                TabletIdToPipe[tabletId] = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
            }

            NKikimrClient::TPersQueueRequest request;
            request.MutablePartitionRequest()->SetTopic(topicIt->second.TopicNameConverter->GetPrimaryPath());
            request.MutablePartitionRequest()->SetPartition(partitionRequest.PartitionIndex);
            request.MutablePartitionRequest()->SetCookie(NextCookie);

            TRequestInfo info(topicReq.Name.value(), partitionRequest.PartitionIndex);

            CookieToRequestInfo.emplace(std::make_pair(NextCookie, info));
            TabletIdToCookies[tabletId].push_back(NextCookie);
            NextCookie++;

            auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
            commit->SetClientId(Message->GroupId.value());
            commit->SetOffset(partitionRequest.CommittedOffset);
            commit->SetStrict(true);

            PendingResponses++;
            KAFKA_LOG_D("Send commit request for group# " << Message->GroupId.value() <<
                ", topic# " << topicIt->second.TopicNameConverter->GetPrimaryPath() <<
                ", partition# " << partitionRequest.PartitionIndex <<
                ", offset# " << partitionRequest.CommittedOffset);

            TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
            req->Record.Swap(&request);

            NTabletPipe::SendData(ctx, TabletIdToPipe[tabletId], req.Release());
        }
    }
}

void TKafkaOffsetCommitActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& partitionResult = ev->Get()->Record.GetPartitionResponse();
    auto requestInfo = CookieToRequestInfo.find(partitionResult.GetCookie());
    Y_ABORT_UNLESS(requestInfo != CookieToRequestInfo.end());

    requestInfo->second.Done = true;
    if (ev->Get()->Record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        KAFKA_LOG_CRIT("Commit offset error. status# " << EErrorCode_Name(ev->Get()->Record.GetErrorCode()) << ", reason# " << ev->Get()->Record.GetErrorReason());
    }

    AddPartitionResponse(ConvertErrorCode(NGRpcProxy::V1::ConvertOldCode(ev->Get()->Record.GetErrorCode())), requestInfo->second.TopicName, requestInfo->second.PartitionId, ctx);
}

void TKafkaOffsetCommitActor::AddPartitionResponse(EKafkaErrors error, const TString& topicName, ui64 partitionId, const TActorContext& ctx) {
    if (error != NONE_ERROR) {
        Error = error;
    }

    PendingResponses--;
    TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition partitionResponse;
    partitionResponse.PartitionIndex = partitionId;
    partitionResponse.ErrorCode = error;

    auto topicIdIt = ResponseTopicIds.find(topicName);

    if (topicIdIt != ResponseTopicIds.end()) {
        Response->Topics[topicIdIt->second].Partitions.push_back(partitionResponse);
    } else {
        ResponseTopicIds[topicName] = Response->Topics.size();

        TOffsetCommitResponseData::TOffsetCommitResponseTopic topicResponse;
        topicResponse.Name = topicName;
        topicResponse.Partitions.push_back(partitionResponse);

        Response->Topics.push_back(topicResponse);
    }

    if (PendingResponses == 0) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, Error));
        Die(ctx);
    }
}

void TKafkaOffsetCommitActor::Bootstrap(const NActors::TActorContext& ctx) {
    THashSet<TString> topicsToResolve;
    for (auto topicReq: Message->Topics) {
        topicsToResolve.insert(NormalizePath(Context->DatabasePath, topicReq.Name.value()));
    }

    auto topicConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
        NKikimr::AppData(ctx)->PQConfig, ""
    );

    auto topicHandler = std::make_unique<NPersQueue::TTopicsListController>(
        topicConverterFactory
    );

    auto topicsToConverter = topicHandler->GetReadTopicsList(topicsToResolve, false, Context->DatabasePath);
    if (!topicsToConverter.IsValid) {
        KAFKA_LOG_CRIT("Commit offsets failed. reason# topicsToConverter is not valid");
        Error = INVALID_REQUEST;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    AuthInitActor = ctx.Register(new NKikimr::NGRpcProxy::V1::TReadInitAndAuthActor(
            ctx, ctx.SelfID, Message->GroupId.value(), 0, "",
            NKikimr::NMsgBusProxy::CreatePersQueueMetaCacheV2Id(), NKikimr::MakeSchemeCacheID(), nullptr, Context->UserToken, topicsToConverter,
        topicHandler->GetLocalCluster(), false)
    );

    Become(&TKafkaOffsetCommitActor::StateWork);
}

} // NKafka
