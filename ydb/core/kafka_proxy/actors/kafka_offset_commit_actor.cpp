#include "kafka_offset_commit_actor.h"
#include <ydb/library/actors/core/log.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/kafka_proxy/kafka_metrics.h>
#include "kafka_metadata_service.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KAFKA_PROXY

namespace NKafka {


NActors::IActor* CreateKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message) {
    return new TKafkaOffsetCommitActor(context, correlationId, message);
}

NActors::NStructuredLog::TStructuredMessage TKafkaOffsetCommitActor::LogPrefix() {
    return YDB_LOG_CREATE_MESSAGE(
        {"actorClassName", "TKafkaOffsetCommitActor"},
        {"selfId", SelfId()});
}

void TKafkaOffsetCommitActor::Die(const TActorContext& ctx) {
    YDB_LOG_DEBUG("PassAway",
        {LogPrefix()});
    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());
    for (const auto& tabletToPipePair: TabletIdToPipe) {
        NTabletPipe::CloseClient(ctx, tabletToPipePair.second);
    }
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
    TBase::Die(ctx);
}

TString TKafkaOffsetCommitActor::GetMetadataDatabasePath() const {
    return NKikimr::AppData()->FeatureFlags.GetEnableKafkaServerlessTransactions() ? Context->DatabasePath : Context->ResourceDatabasePath;
}

void TKafkaOffsetCommitActor::Handle(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_CRIT("Auth failed",
        {LogPrefix()},
        {"reason", ev->Get()->Reason});
    Error = ConvertErrorCode(ev->Get()->ErrorCode);
    if (Error == GROUP_ID_NOT_FOUND && Context->Config.GetAutoCreateConsumersEnable()) {
        for (auto topicReq: Message->Topics) {
            TString topicPath = NormalizePath(Context->DatabasePath, *topicReq.Name);
            CreateConsumerGroupIfNecessary(*topicReq.Name, topicPath, *Message->GroupId);
        }
        if (PendingResponses == 0) { // case when AlterTopic requests have already sent and returned an unsuccessful response
            SendFailedForAllPartitions(Error, ctx);
        }
    } else {
        SendFailedForAllPartitions(Error, ctx);
    }
}

void TKafkaOffsetCommitActor::CreateConsumerGroupIfNecessary(const TString& topicName,
                                    const TString& topicPath,
                                    const TString& groupId) {
    TTopicGroupIdAndPath consumerTopicRequest = TTopicGroupIdAndPath{groupId, topicPath};
    if (ConsumerTopicAlterRequestAttempts.find(consumerTopicRequest) == ConsumerTopicAlterRequestAttempts.end()) {
        ConsumerTopicAlterRequestAttempts.insert(consumerTopicRequest);
    } else {
        // it is enough to send a consumer addition request only once for a particular topic
        return;
    }
    PendingResponses++;

    auto request = std::make_unique<Ydb::Topic::AlterTopicRequest>();
    request.get()->set_path(topicPath);
    auto* consumer = request->add_add_consumers();
    consumer->set_name(groupId);
    AlterTopicCookie++;
    AlterTopicCookieToName[AlterTopicCookie] = topicName;
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
        topicName, Context->DatabasePath, std::move(request), callback, Context->Token.UserToken),
        NKikimr::NReplication::TLocalProxyActor(Context->DatabasePath));
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
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, error));
    Die(ctx);
}

void TKafkaOffsetCommitActor::Handle(NKikimr::NReplication::TEvYdbProxy::TEvAlterTopicResponse::TPtr& ev, const TActorContext& ctx) {
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
    PendingResponses--;
    if (result.GetStatus() != NYdb::EStatus::ALREADY_EXISTS && result.GetStatus() != NYdb::EStatus::SUCCESS) {
        SendFailedForAllPartitions(Error, ctx);
    } else if (PendingResponses == 0) {
        SendAuthRequest(ctx);
        return;
    }
}

void TKafkaOffsetCommitActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        YDB_LOG_CRIT("Pipe to tablet is dead",
            {LogPrefix()},
            {"status", ev->Get()->Status});
        ProcessPipeProblem(msg->TabletId, ctx);
    }
}

void TKafkaOffsetCommitActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_CRIT("Pipe to tablet is destroyed",
        {LogPrefix()});
    ProcessPipeProblem(ev->Get()->TabletId, ctx);
}

void TKafkaOffsetCommitActor::ProcessPipeProblem(ui64 tabletId, const TActorContext& ctx) {
    auto cookiesIt = TabletIdToCookies.find(tabletId);
    AFL_ENSURE(cookiesIt != TabletIdToCookies.end())("tablet_id", tabletId)("group", Message->GroupId.value())("database", Context->DatabasePath);

    for (auto cookie: cookiesIt->second) {
        auto requestInfoIt = CookieToRequestInfo.find(cookie);
        AFL_ENSURE(requestInfoIt != CookieToRequestInfo.end())("tablet_id", tabletId)("cookie", cookie)("database", Context->DatabasePath);

        if (!requestInfoIt->second.Done) {
            requestInfoIt->second.Done = true;
            AddPartitionResponse(EKafkaErrors::UNKNOWN_SERVER_ERROR, requestInfoIt->second.TopicName, requestInfoIt->second.PartitionId, ctx);
        }
    }
}

void TKafkaOffsetCommitActor::SendGenerationCheckRequest(const TActorContext& ctx) {
    YDB_LOG_DEBUG("Sending generation check KQP request",
        {LogPrefix()},
        {"group", Message->GroupId.value()},
        {"generationId", Message->GenerationId});

    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(*Message->GroupId).Build();
    params.AddParam("$Database").Utf8(Context->DatabasePath).Build();

    Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_GENERATION.c_str(),
                        NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant()
                        ->FormPathToResourceTable(GetMetadataDatabasePath()).c_str()),
             params.Build(), 0, ctx);
}

void TKafkaOffsetCommitActor::Handle(NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    if (TryRequestConsumerMetadataTablesCreation(record.GetYdbStatus(), GetMetadataDatabasePath(), Context->ResourceDatabasePath, ctx)) {
        Error = COORDINATOR_NOT_AVAILABLE;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        YDB_LOG_CRIT("Generation check KQP query failed",
            {LogPrefix()},
            {"group", Message->GroupId.value()},
            {"status", record.GetYdbStatus()});
        Error = UNKNOWN_SERVER_ERROR;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    auto& resp = record.GetResponse();
    if (resp.GetYdbResults().empty()) {
        Error = GROUP_ID_NOT_FOUND;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    NYdb::TResultSetParser parser(resp.GetYdbResults(0));
    if (!parser.TryNextRow()) {
        Error = GROUP_ID_NOT_FOUND;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    auto tableGeneration = parser.ColumnParser("generation").GetUint64();
    if (tableGeneration != static_cast<ui64>(Message->GenerationId)) {
        YDB_LOG_INFO("Generation mismatch",
            {LogPrefix()},
            {"group", Message->GroupId.value()},
            {"expected", Message->GenerationId},
            {"got", tableGeneration});
        Error = ILLEGAL_GENERATION;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    YDB_LOG_DEBUG("Generation check passed",
        {LogPrefix()},
        {"group", Message->GroupId.value()},
        {"generation", tableGeneration});

    SendCommits(ctx);
}

void TKafkaOffsetCommitActor::Handle(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_DEBUG("Auth success. Topics",
        {LogPrefix()},
        {"count", ev->Get()->TopicAndTablets.size()});
    TopicAndTablets = std::move(ev->Get()->TopicAndTablets);

    if (Message->GenerationId == -1) {
        SendCommits(ctx);
    } else {
        if (!NKikimr::AppData()->FeatureFlags.GetEnableKafkaServerlessTransactions()) {
            Kqp = std::make_unique<TKqpTxHelper>(Context->ResourceDatabasePath);
        } else {
            Kqp = std::make_unique<TKqpTxHelper>(Context->DatabasePath);
        }
        Kqp->SendCreateSessionRequest(ctx);
    }

}

void TKafkaOffsetCommitActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        YDB_LOG_ERROR("Failed to create KQP session",
            {LogPrefix()});
        Error = EKafkaErrors::UNKNOWN_SERVER_ERROR;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }
    SendGenerationCheckRequest(ctx);
}

void TKafkaOffsetCommitActor::SendCommits(const TActorContext& ctx) {
    std::vector<std::pair<TString, ui64>> unknownTopicPartitionResponses;
    for (auto topicReq: Message->Topics) {
        auto topicIt = TopicAndTablets.find(NormalizePath(Context->DatabasePath, topicReq.Name.value()));
        for (auto partitionRequest: topicReq.Partitions) {
            if (topicIt == TopicAndTablets.end()) {
                PendingResponses++;
                unknownTopicPartitionResponses.push_back({topicReq.Name.value(), partitionRequest.PartitionIndex});
                continue;
            }

            auto tabletIdIt = topicIt->second.Partitions.find(partitionRequest.PartitionIndex);
            if (tabletIdIt == topicIt->second.Partitions.end()) {
                PendingResponses++;
                unknownTopicPartitionResponses.push_back({topicReq.Name.value(), partitionRequest.PartitionIndex});
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

            if (partitionRequest.CommittedMetadata.has_value()) {
                commit->SetCommittedMetadata(*partitionRequest.CommittedMetadata);
            }

            PendingResponses++;
            YDB_LOG_DEBUG("Send commit request",
                {LogPrefix()},
                {"group", Message->GroupId.value()},
                {"topic", topicIt->second.TopicNameConverter->GetPrimaryPath()},
                {"partition", partitionRequest.PartitionIndex},
                {"offset", partitionRequest.CommittedOffset});

            TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
            req->Record.Swap(&request);
            NTabletPipe::SendData(ctx, TabletIdToPipe[tabletId], req.Release());
        }
    }
    for (auto [topicName, partitionInd] : unknownTopicPartitionResponses) {
        AddPartitionResponse(UNKNOWN_TOPIC_OR_PARTITION, topicName, partitionInd, ctx);
    }
}

void TKafkaOffsetCommitActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& partitionResult = ev->Get()->Record.GetPartitionResponse();
    auto requestInfo = CookieToRequestInfo.find(partitionResult.GetCookie());
    AFL_ENSURE(requestInfo != CookieToRequestInfo.end())("cookie", partitionResult.GetCookie())("database", Context->DatabasePath);

    requestInfo->second.Done = true;
    const auto pqError = ev->Get()->Record.GetErrorCode();
    // Kafka OffsetCommit does not validate against log start/end. PQ Strict
    // still rejects those commits; map the error to NONE so Java clients do
    // not treat OFFSET_OUT_OF_RANGE as a failed commit.
    if (pqError == NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE ||
        pqError == NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST) {
        YDB_LOG_DEBUG("Ignoring out-of-range commit, Kafka OffsetCommit returns NONE",
            {LogPrefix()},
            {"status", EErrorCode_Name(pqError)},
            {"reason", ev->Get()->Record.GetErrorReason()});
        ctx.Send(MakeKafkaMetricsServiceID(), new TEvKafka::TEvUpdateCounter(
            1,
            BuildLabels(
                Context,
                "",
                GetTopicNameWithoutDb(Context->DatabasePath, requestInfo->second.TopicName),
                "api.kafka.offset_commit.ignored_out_of_range",
                "")));
        AddPartitionResponse(NONE_ERROR, requestInfo->second.TopicName, requestInfo->second.PartitionId, ctx);
        return;
    }
    if (pqError != NPersQueue::NErrorCode::OK) {
        YDB_LOG_CRIT("Commit offset error",
            {LogPrefix()},
            {"status", EErrorCode_Name(pqError)},
            {"reason", ev->Get()->Record.GetErrorReason()});
    }

    AddPartitionResponse(ConvertErrorCode(NGRpcProxy::V1::ConvertOldCode(pqError)), requestInfo->second.TopicName, requestInfo->second.PartitionId, ctx);
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

void TKafkaOffsetCommitActor::SendAuthRequest(const NActors::TActorContext& ctx) {
    THashSet<TString> topicsToResolve;
    for (auto topicReq: Message->Topics) {
        topicsToResolve.insert(NormalizePath(Context->DatabasePath, topicReq.Name.value()));
    }

    auto topicConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
        true, "", ""
    );

    auto topicHandler = std::make_unique<NPersQueue::TTopicsListController>(
        topicConverterFactory
    );

    auto topicsToConverter = topicHandler->GetReadTopicsList(topicsToResolve, false, Context->DatabasePath);
    if (!topicsToConverter.IsValid) {
        YDB_LOG_CRIT("Commit offsets failed. topicsToConverter is not valid",
            {LogPrefix()});
        Error = INVALID_REQUEST;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }

    AuthInitActor = ctx.Register(new NKikimr::NGRpcProxy::V1::TReadInitAndAuthActor(
            ctx, ctx.SelfID, Message->GroupId.value(), 0, "",
            NKikimr::NMsgBusProxy::CreatePersQueueMetaCacheV2Id(), NKikimr::MakeSchemeCacheID(), nullptr, Context->Token.UserToken, topicsToConverter,
        topicHandler->GetLocalCluster(), false)
    );
}

void TKafkaOffsetCommitActor::Bootstrap(const NActors::TActorContext& ctx) {
    if (Context->KafkaTableFeatureFlagChanged(NKikimr::AppData()->FeatureFlags.GetEnableKafkaServerlessTransactions())) {
        Error = EKafkaErrors::COORDINATOR_NOT_AVAILABLE;
        SendFailedForAllPartitions(Error, ctx);
        return;
    }
    SendAuthRequest(ctx);
    Become(&TKafkaOffsetCommitActor::StateWork);
}

} // NKafka
