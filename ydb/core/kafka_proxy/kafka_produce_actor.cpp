#include "kafka_produce_actor.h"

#include <contrib/libs/protobuf/src/google/protobuf/util/time_util.h>

#include <ydb/core/base/path.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

namespace NKafka {

static constexpr TDuration WAKEUP_INTERVAL = TDuration::Seconds(1);
static constexpr TDuration TOPIC_NOT_FOUND_EXPIRATION_INTERVAL = TDuration::Seconds(15);
static constexpr TDuration REQUEST_EXPIRATION_INTERVAL = TDuration::Seconds(30);
static constexpr TDuration WRITER_EXPIRATION_INTERVAL = TDuration::Minutes(5);

TString TKafkaProduceActor::LogPrefix() {
    TStringBuilder sb;
    sb << "TKafkaProduceActor " << SelfId() << " State: ";
    auto stateFunc = CurrentStateFunc();
    if (stateFunc == &TKafkaProduceActor::StateInit) {
        sb << "Init ";
    } else if (stateFunc == &TKafkaProduceActor::StateWork) {
        sb << "Work ";
    } else if (stateFunc == &TKafkaProduceActor::StateAccepting) {
        sb << "Accepting ";
    } else {
        sb << "Unknown ";
    }
    return sb;
}

void TKafkaProduceActor::LogEvent(IEventHandle& ev) {
    KAFKA_LOG_T("Received event: " << ev.GetTypeName());
}

void TKafkaProduceActor::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    Schedule(WAKEUP_INTERVAL, new TEvKafka::TEvWakeup());
    Become(&TKafkaProduceActor::StateWork);
}

void TKafkaProduceActor::Handle(TEvKafka::TEvWakeup::TPtr /*request*/, const TActorContext& ctx) {
    KAFKA_LOG_D("Wakeup");

    SendResults(ctx);
    CleanTopics(ctx);
    CleanWriters(ctx);

    Schedule(WAKEUP_INTERVAL, new TEvKafka::TEvWakeup());

    KAFKA_LOG_T("Wakeup was completed successfully");
}

void TKafkaProduceActor::PassAway() {
    KAFKA_LOG_D("PassAway");

    for(const auto& [_, partitionWriters] : Writers) {
        for(const auto& [_, w] : partitionWriters) {
            Send(w.ActorId, new TEvents::TEvPoison());
        }
    }

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());

    TActorBootstrapped::PassAway();

    KAFKA_LOG_T("PassAway was completed successfully");
}

void TKafkaProduceActor::CleanTopics(const TActorContext& ctx) {
    const auto expired = ctx.Now() - TOPIC_NOT_FOUND_EXPIRATION_INTERVAL;

    std::map<TString, TTopicInfo> newTopics;    
    for(auto& [topicPath, topicInfo] : Topics) {
        if (!topicInfo.NotFound || topicInfo.NotFoundTime > expired) {
            newTopics[topicPath] = std::move(topicInfo);
        }
    }
    Topics = std::move(newTopics);
}

void TKafkaProduceActor::CleanWriters(const TActorContext& ctx) {
    KAFKA_LOG_D("CleanWriters");
    const auto expired = ctx.Now() - WRITER_EXPIRATION_INTERVAL;

    for (auto& [topicPath, partitionWriters] : Writers) {
        std::unordered_map<ui32, TWriterInfo> newPartitionWriters;
        for (const auto& [partitionId, writerInfo] : partitionWriters) {
            if (writerInfo.LastAccessed > expired) {
                newPartitionWriters[partitionId] = writerInfo;
            } else {
                TStringBuilder sb;
                sb << "Destroing inactive PartitionWriter. Topic='" << topicPath << "', Partition=" << partitionId;
                KAFKA_LOG_D(sb);
                Send(writerInfo.ActorId, new TEvents::TEvPoison());
            }
        }
        partitionWriters = std::move(newPartitionWriters);
    }

    KAFKA_LOG_T("CleanWriters was completed successfully");
}

void TKafkaProduceActor::EnqueueRequest(TEvKafka::TEvProduceRequest::TPtr request, const TActorContext& /*ctx*/) {
    Requests.push_back(request);
}

void TKafkaProduceActor::HandleInit(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    auto* navigate = ev.Get()->Get()->Request.Get();
    for (auto& info : navigate->ResultSet) {
        if (NSchemeCache::TSchemeCacheNavigate::EStatus::Ok == info.Status) {
            auto topicPath = "/" + NKikimr::JoinPath(info.Path);
            KAFKA_LOG_D("Received topic '" << topicPath << "' description");
            TopicsForInitialization.erase(topicPath);

            auto& topic = Topics[topicPath];
            for(auto& p : info.PQGroupInfo->Description.GetPartitions()) {
                topic.partitions[p.GetPartitionId()] = p.GetTabletId();
            }

            auto pathId = info.TableId.PathId;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId));
        }
    }

    for(auto& topicPath : TopicsForInitialization) {
        KAFKA_LOG_D("Topic '" << topicPath << "' not found");
        auto& topicInfo = Topics[topicPath];
        topicInfo.NotFound = true;
        topicInfo.NotFoundTime = ctx.Now();
    }

    TopicsForInitialization.clear();

    Become(&TKafkaProduceActor::StateWork);

    KAFKA_LOG_T("HandleInit(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr) was completed successfully");

    ProcessRequests(ctx);
}

void TKafkaProduceActor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev, const TActorContext& ctx) {
    auto& path = ev->Get()->Path;
    KAFKA_LOG_I("Topic '" << path << "' was deleted");

    auto it = Writers.find(path);
    if (it != Writers.end()) {
        for(auto& [_, writer] : it->second) {
            Send(writer.ActorId, new TEvents::TEvPoison());
        }
        Writers.erase(it);
    }

    auto& topicInfo = Topics[path];
    topicInfo.NotFound = true;
    topicInfo.NotFoundTime = ctx.Now();
    topicInfo.partitions.clear();
}

void TKafkaProduceActor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& /*ctx*/) {
    auto* e = ev->Get();
    auto& path = e->Path;
    KAFKA_LOG_I("Topic '" << path << "' was updated");

    auto& topic = Topics[path];
    topic.partitions.clear();
    for (auto& p : e->Result->GetPathDescription().GetPersQueueGroup().GetPartitions()) {
        topic.NotFound = false;
        topic.partitions[p.GetPartitionId()] = p.GetTabletId();
    }
}

void TKafkaProduceActor::Handle(TEvKafka::TEvProduceRequest::TPtr request, const TActorContext& ctx) {
    Requests.push_back(request);
    ProcessRequests(ctx);
}

void TKafkaProduceActor::ProcessRequests(const TActorContext& ctx) {
    if (&TKafkaProduceActor::StateWork != CurrentStateFunc()) {
        KAFKA_LOG_ERROR("Unexpected state");
        return;
    }

    if (Requests.empty()) {
        return;
    }

    if (EnqueueInitialization()) {
        PendingRequests.push_back({Requests.front()});
        Requests.pop_front();

        ProcessRequest(PendingRequests.back(), ctx);
    } else {
        ProcessInitializationRequests(ctx);
    }
}

size_t TKafkaProduceActor::EnqueueInitialization() {
    size_t canProcess = 0;
    bool requireInitialization = false;

    for(const auto& e : Requests) {
        auto* r = e->Get()->Request;
        for(const auto& topicData : r->TopicData) {
            const auto& topicPath = *topicData.Name;
            if (!Topics.contains(topicPath)) {
                requireInitialization = true;
                TopicsForInitialization.insert(topicPath);
            }
        }
        if (!requireInitialization) {
            ++canProcess;
        }
    }

    return canProcess;
}

THolder<TEvPartitionWriter::TEvWriteRequest> Convert(const TProduceRequestData::TTopicProduceData::TPartitionProduceData& data,
                                                     const TString& topicName,
                                                     ui64 cookie,
                                                     const TString& clientDC) {
    auto ev = MakeHolder<TEvPartitionWriter::TEvWriteRequest>();
    auto& request = ev->Record;

    const auto& batch = data.Records;
    const TString sourceId = TStringBuilder() << batch->ProducerId;

    auto* partitionRequest = request.MutablePartitionRequest();
    partitionRequest->SetTopic(topicName);
    partitionRequest->SetPartition(data.Index);
    // partitionRequest->SetCmdWriteOffset();
    partitionRequest->SetCookie(cookie);
    // partitionRequest->SetPutUnitsSize(); TODO

    for (const auto& record : batch->Records) {
        if (!record.Value) {
            continue;
        }

        NKikimrPQClient::TDataChunk proto;
        for(auto& h : record.Headers) {
            auto res = proto.AddMessageMeta();
            res->set_key(static_cast<const char*>(h.Key->data()), h.Key->size());
            res->set_value(static_cast<const char*>(h.Value->data()), h.Value->size());
        }

        {
            auto res = proto.AddMessageMeta();
            res->set_key("__key");
            res->set_value(static_cast<const char*>(record.Key->data()), record.Key->size());
        }

        proto.SetData(static_cast<const void*>(record.Value->data()), record.Value->size());

        TString str;
        bool res = proto.SerializeToString(&str);
        Y_VERIFY(res);

        auto w = partitionRequest->AddCmdWrite();

        w->SetSourceId(sourceId);
        w->SetSeqNo(batch->BaseOffset + record.OffsetDelta);
        w->SetData(str);
        w->SetCreateTimeMS(batch->BaseTimestamp + record.TimestampDelta);
        w->SetDisableDeduplication(true);
        w->SetUncompressedSize(record.Value->size());
        w->SetClientDC(clientDC);
        w->SetIgnoreQuotaDeadline(true);
    }

    return ev;
}

size_t PartsCount(const TProduceRequestData* r) {
    size_t result = 0;
    for(const auto& topicData : r->TopicData) {
        result += topicData.PartitionData.size();
    }
    return result;
}

void TKafkaProduceActor::ProcessRequest(TPendingRequest& pendingRequest, const TActorContext& ctx) {
    auto* r = pendingRequest.Request->Get()->Request;

    pendingRequest.Results.resize(PartsCount(r));
    pendingRequest.StartTime = ctx.Now();

    size_t position = 0;
    for(const auto& topicData : r->TopicData) {
        const TString& topicPath = *topicData.Name;
        for(const auto& partitionData : topicData.PartitionData) {
            const auto partitionId = partitionData.Index;

            auto writerId = PartitionWriter(topicPath, partitionId, ctx);
            if (writerId) {
                auto ownCookie = ++Cookie;
                auto& cookieInfo = Cookies[ownCookie];
                cookieInfo.TopicPath = topicPath;
                cookieInfo.PartitionId = partitionId;
                cookieInfo.Position = position;
                cookieInfo.Request = &pendingRequest;

                pendingRequest.WaitAcceptingCookies.insert(ownCookie);
                pendingRequest.WaitResultCookies.insert(ownCookie);

                auto ev = Convert(partitionData, *topicData.Name, ownCookie, ClientDC);

                Send(writerId, std::move(ev));
            } else {
                auto& result = pendingRequest.Results[position];
                result.ErrorCode = EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION;
            }

            ++position;
        }
    }

    if (pendingRequest.WaitResultCookies.empty()) {
        // All request for unknown topic or empty request
        SendResults(ctx);
    } else {
        Become(&TKafkaProduceActor::StateAccepting);
    }
}

void TKafkaProduceActor::HandleAccepting(TEvPartitionWriter::TEvWriteAccepted::TPtr request, const TActorContext& ctx) {
    auto r = request->Get();
    auto cookie = r->Cookie;

    auto it = Cookies.find(cookie);
    if (it == Cookies.end()) {
        KAFKA_LOG_W("Received TEvWriteAccepted with unexpected cookie " << cookie);
        return;
    }

    auto& cookieInfo = it->second;
    auto& expectedCookies = cookieInfo.Request->WaitAcceptingCookies;
    expectedCookies.erase(cookie);

    if (expectedCookies.empty()) {
        Become(&TKafkaProduceActor::StateWork);
        ProcessRequests(ctx);
    }
}

void TKafkaProduceActor::Handle(TEvPartitionWriter::TEvInitResult::TPtr request, const TActorContext& /*ctx*/) {
    KAFKA_LOG_D("Init " << request->Get()->ToString());
}

void TKafkaProduceActor::Handle(TEvPartitionWriter::TEvWriteResponse::TPtr request, const TActorContext& ctx) {
    auto r = request->Get();
    auto cookie = r->Record.GetPartitionResponse().GetCookie();

    auto it = Cookies.find(cookie);
    if (it == Cookies.end()) {
        KAFKA_LOG_W("Received TEvWriteResponse with unexpected cookie " << cookie);
        return;
    }

    auto& cookieInfo = it->second;
    auto& partitionResult = cookieInfo.Request->Results[cookieInfo.Position];
    partitionResult.ErrorCode = EKafkaErrors::NONE_ERROR;
    partitionResult.Value = request;
    cookieInfo.Request->WaitResultCookies.erase(cookie);

    Cookies.erase(it);

    if (!r->IsSuccess()) {
        auto wit = Writers.find(cookieInfo.TopicPath);
        if (wit != Writers.end()) {
            auto& partitions = wit->second;
            auto pit = partitions.find(cookieInfo.PartitionId);
            if (pit != partitions.end()) {
                Send(pit->second.ActorId, new TEvents::TEvPoison());
                partitions.erase(pit);
            }
        }
    }

    if (cookieInfo.Request->WaitResultCookies.empty()) {
        SendResults(ctx);
    }
}

void TKafkaProduceActor::SendResults(const TActorContext& ctx) {
    auto expireTime = ctx.Now() - REQUEST_EXPIRATION_INTERVAL;

    KAFKA_LOG_T("Sending results. QueueSize= " << PendingRequests.size() << ", ExpirationTime=" << expireTime);

    // We send the results in the order of receipt of the request
    while (!PendingRequests.empty()) {
        auto& pendingRequest = PendingRequests.front();

        // We send the response by timeout. This is possible, for example, if the event was lost or the PartitionWrite died.
        bool expired = expireTime > pendingRequest.StartTime;

        if (!expired && !pendingRequest.WaitResultCookies.empty()) {
            return;
        }

        auto* r = pendingRequest.Request->Get()->Request;
        auto cookie = pendingRequest.Request->Get()->Cookie;

        KAFKA_LOG_D("Send result for cookie " << cookie << ". Expired=" << expired);

        const auto topicsCount = r->TopicData.size();
        auto response = std::make_shared<TProduceResponseData>();
        response->Responses.resize(topicsCount);

        size_t position = 0;

        for(size_t i = 0; i < topicsCount; ++i) {
            const auto& topicData = r->TopicData[i];
            const auto partitionCount = topicData.PartitionData.size();

            auto& topicResponse =  response->Responses[i];
            topicResponse.Name = topicData.Name;
            topicResponse.PartitionResponses.resize(partitionCount);

            for(size_t j = 0; j < partitionCount; ++j) {
                const auto& partitionData = topicData.PartitionData[j];
                auto& partitionResponse = topicResponse.PartitionResponses[j];
                const auto& result = pendingRequest.Results[position++];

                partitionResponse.Index = partitionData.Index;

                if (EKafkaErrors::NONE_ERROR != result.ErrorCode) {
                    partitionResponse.ErrorCode = result.ErrorCode;
                    partitionResponse.ErrorMessage = result.ErrorMessage;
                } else {
                    auto* msg = result.Value->Get();
                    if (msg->IsSuccess()) {
                        partitionResponse.ErrorCode = EKafkaErrors::NONE_ERROR;
                        auto& writeResults = msg->Record.GetPartitionResponse().GetCmdWriteResult();

                        if (!writeResults.empty()) {
                            auto& lastResult = writeResults.at(writeResults.size() - 1);
                            partitionResponse.LogAppendTimeMs = lastResult.GetWriteTimestampMS();
                            partitionResponse.BaseOffset = lastResult.GetSeqNo();
                        }
                    } else {
                        partitionResponse.ErrorCode = EKafkaErrors::UNKNOWN_SERVER_ERROR;
                        partitionResponse.ErrorMessage = msg->GetError().Reason;
                    }
                }
            }
        }

        Send(Client, new TEvKafka::TEvResponse(cookie, response));

        if (!pendingRequest.WaitAcceptingCookies.empty()) {
            if (!expired) {
                TStringBuilder sb;
                sb << "All TEvWriteResponse were received, but not all TEvWriteAccepted. Unreceived cookies:";
                for(auto cookie : pendingRequest.WaitAcceptingCookies) {
                    sb << " " << cookie;
                }
                KAFKA_LOG_W(sb);
            }
            if (&TKafkaProduceActor::StateAccepting == CurrentStateFunc()) {
                Become(&TKafkaProduceActor::StateWork);
            }
        }

        PendingRequests.pop_front();
    }
}

void TKafkaProduceActor::ProcessInitializationRequests(const TActorContext& ctx) {
    if (TopicsForInitialization.empty()) {
        return;
    }

    Become(&TKafkaProduceActor::StateInit);

    auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();

    for(auto& topicPath : TopicsForInitialization) {
        KAFKA_LOG_D("Describe topic '" << topicPath << "'");
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(topicPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;

        request->ResultSet.emplace_back(entry);
    }

    ctx.Send(MakeSchemeCacheID(), MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(request.release()));
}

TActorId TKafkaProduceActor::PartitionWriter(const TString& topicPath, ui32 partitionId, const TActorContext& ctx) {
    auto& partitionWriters = Writers[topicPath];
    auto itp = partitionWriters.find(partitionId);
    if (itp != partitionWriters.end()) {
        auto& writerInfo = itp->second;
        writerInfo.LastAccessed = ctx.Now();
        return writerInfo.ActorId;
    }

    auto it = Topics.find(topicPath);
    if (it == Topics.end()) {
        KAFKA_LOG_ERROR("Internal error: topic '" << topicPath << "' isn`t initialized");
        return TActorId{};
    }

    auto& topicInfo = it->second;
    if (topicInfo.NotFound) {
        return TActorId{};
    }

    auto& partitions = topicInfo.partitions;
    auto pit = partitions.find(partitionId);
    if (pit == partitions.end()) {
        return TActorId{};
    }

    auto tabletId = pit->second;
    auto* writerActor = CreatePartitionWriter(SelfId(), tabletId, partitionId, SourceId,
         TPartitionWriterOpts().WithDeduplication(false));

    auto& writerInfo = partitionWriters[partitionId];
    writerInfo.ActorId = ctx.RegisterWithSameMailbox(writerActor);
    writerInfo.LastAccessed = ctx.Now();
    return writerInfo.ActorId;
}

} // namespace NKafka
