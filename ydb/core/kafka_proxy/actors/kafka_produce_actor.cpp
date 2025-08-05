#include "kafka_produce_actor.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/core/kafka_proxy/kafka_metrics.h>

#include <contrib/libs/protobuf/src/google/protobuf/util/time_util.h>

#include <ydb/core/persqueue/utils.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

namespace NKafka {

static constexpr TDuration WAKEUP_INTERVAL = TDuration::Seconds(1);
static constexpr TDuration TOPIC_OK_EXPIRATION_INTERVAL = TDuration::Minutes(15);
static constexpr TDuration TOPIC_NOT_FOUND_EXPIRATION_INTERVAL = TDuration::Seconds(15);
static constexpr TDuration TOPIC_UNATHORIZED_EXPIRATION_INTERVAL = TDuration::Minutes(1);
static constexpr TDuration REQUEST_EXPIRATION_INTERVAL = TDuration::Seconds(30);
static constexpr TDuration WRITER_EXPIRATION_INTERVAL = TDuration::Minutes(5);

NActors::IActor* CreateKafkaProduceActor(const TContext::TPtr context) {
    return new TKafkaProduceActor(context);
}

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
    KAFKA_LOG_T("Produce actor: Received event: " << ev.GetTypeName());
}

void TKafkaProduceActor::SendMetrics(const TString& topicName, size_t delta, const TString& name, const TActorContext& ctx) {
    auto topicWithoutDb = GetTopicNameWithoutDb(Context->DatabasePath, topicName);
    ctx.Send(MakeKafkaMetricsServiceID(), new TEvKafka::TEvUpdateCounter(delta, BuildLabels(Context, "", topicWithoutDb, TStringBuilder() << "api.kafka.produce." << name, "")));
    ctx.Send(MakeKafkaMetricsServiceID(), new TEvKafka::TEvUpdateCounter(delta, BuildLabels(Context, "", topicWithoutDb, "api.kafka.produce.total_messages", "")));
}

void TKafkaProduceActor::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    Schedule(WAKEUP_INTERVAL, new TEvKafka::TEvWakeup());
    Become(&TKafkaProduceActor::StateWork);
}

void TKafkaProduceActor::Handle(TEvKafka::TEvWakeup::TPtr /*request*/, const TActorContext& ctx) {
    KAFKA_LOG_T("Produce actor: Wakeup");

    SendResults(ctx);
    CleanTopics(ctx);
    CleanWriters(ctx);

    Schedule(WAKEUP_INTERVAL, new TEvKafka::TEvWakeup());

    KAFKA_LOG_T("Produce actor: Wakeup was completed successfully");
}

void TKafkaProduceActor::PassAway() {
    KAFKA_LOG_D("Produce actor: PassAway");

    for(const auto& [_, partitionWriters] : NonTransactionalWriters) {
        for(const auto& [_, w] : partitionWriters) {
            Send(w.ActorId, new TEvents::TEvPoison());
        }
    }
    for(const auto& [_, writeInfo] : TransactionalWriters) {
        Send(writeInfo.ActorId, new TEvents::TEvPoison());
    }

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());

    TActorBootstrapped::PassAway();

    KAFKA_LOG_T("Produce actor: PassAway was completed successfully");
}

void TKafkaProduceActor::CleanTopics(const TActorContext& ctx) {
    const auto now = ctx.Now();

    std::map<TString, TTopicInfo> newTopics;
    for(auto& [topicPath, topicInfo] : Topics) {
        if (topicInfo.ExpirationTime > now) {
            newTopics[topicPath] = std::move(topicInfo);
        }
    }
    Topics = std::move(newTopics);
}

void TKafkaProduceActor::CleanWriters(const TActorContext& ctx) {
    KAFKA_LOG_T("Produce actor: CleanWriters");
    const auto earliestAllowedTs = ctx.Now() - WRITER_EXPIRATION_INTERVAL;

    for (auto& [topicPath, partitionWriters] : NonTransactionalWriters) {
        for (auto it = partitionWriters.begin(); it != partitionWriters.end(); ++it) {
            if (it->second.LastAccessed < earliestAllowedTs) {
                CleanWriter({topicPath, it->first}, it->second.ActorId);
                partitionWriters.erase(it);
            }
        }
    }
    for (auto it = TransactionalWriters.begin(); it != TransactionalWriters.end(); ++it) {
        if (it->second.LastAccessed < earliestAllowedTs) {
            CleanWriter(it->first, it->second.ActorId);
            TransactionalWriters.erase(it);
        }
    }

    KAFKA_LOG_T("Produce actor: CleanWriters was completed successfully");
}

void TKafkaProduceActor::CleanWriter(const TTopicPartition& topicPartition, const TActorId& writerId) {
    KAFKA_LOG_D("Produce actor: Destroing inactive PartitionWriter. Topic='" << topicPartition.TopicPath << "', Partition=" << topicPartition.PartitionId);
    Send(writerId, new TEvents::TEvPoison());
}

void TKafkaProduceActor::EnqueueRequest(TEvKafka::TEvProduceRequest::TPtr request, const TActorContext& /*ctx*/) {
    Requests.push_back(request);
}

void TKafkaProduceActor::HandleInit(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    auto now = ctx.Now();
    auto* navigate = ev.Get()->Get()->Request.Get();
    for (auto& info : navigate->ResultSet) {
        if (NSchemeCache::TSchemeCacheNavigate::EStatus::Ok == info.Status) {
            auto topicPath = CanonizePath(NKikimr::JoinPath(info.Path));
            KAFKA_LOG_D("Produce actor: Received topic '" << topicPath << "' description");
            TopicsForInitialization.erase(topicPath);
            auto& topic = Topics[topicPath];

            topic.MeteringMode = info.PQGroupInfo->Description.GetPQTabletConfig().GetMeteringMode();

            if (!Context->RequireAuthentication || info.SecurityObject->CheckAccess(NACLib::EAccessRights::UpdateRow, *Context->UserToken)) {
                topic.Status = OK;
                topic.ExpirationTime = now + TOPIC_OK_EXPIRATION_INTERVAL;
                topic.PartitionChooser = CreatePartitionChooser(info.PQGroupInfo->Description);
            } else {
                KAFKA_LOG_W("Produce actor: Unauthorized PRODUCE to topic '" << topicPath << "'");
                topic.Status = UNAUTHORIZED;
                topic.ExpirationTime = now + TOPIC_UNATHORIZED_EXPIRATION_INTERVAL;
            }


            auto pathId = info.TableId.PathId;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId));
        }
    }

    for(auto& topicPath : TopicsForInitialization) {
        KAFKA_LOG_D("Produce actor: Topic '" << topicPath << "' not found");
        auto& topicInfo = Topics[topicPath];
        topicInfo.Status = NOT_FOUND;
        topicInfo.ExpirationTime = now + TOPIC_NOT_FOUND_EXPIRATION_INTERVAL;
    }

    TopicsForInitialization.clear();

    Become(&TKafkaProduceActor::StateWork);

    KAFKA_LOG_T("Produce actor: HandleInit(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr) was completed successfully");

    ProcessRequests(ctx);
}

void TKafkaProduceActor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev, const TActorContext& ctx) {
    auto& path = ev->Get()->Path;
    KAFKA_LOG_I("Produce actor: Topic '" << path << "' was deleted");

    auto it = NonTransactionalWriters.find(path);
    if (it != NonTransactionalWriters.end()) {
        for(auto& [_, writer] : it->second) {
            Send(writer.ActorId, new TEvents::TEvPoison());
        }
        NonTransactionalWriters.erase(it);
    }
    for (auto& [topicPartition, writer] : TransactionalWriters) {
        if (topicPartition.TopicPath == path) {
            Send(writer.ActorId, new TEvents::TEvPoison());
        }
        TransactionalWriters.erase(topicPartition);
    }

    auto& topicInfo = Topics[path];
    topicInfo.Status = NOT_FOUND;
    topicInfo.ExpirationTime = ctx.Now() + TOPIC_NOT_FOUND_EXPIRATION_INTERVAL;
    topicInfo.PartitionChooser.reset();
}

void TKafkaProduceActor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    auto* e = ev->Get();
    auto& path = e->Path;
    KAFKA_LOG_I("Produce actor: Topic '" << path << "' was updated");

    auto& topic = Topics[path];
    if (topic.Status == UNAUTHORIZED) {
        return;
    }
    topic.Status = OK;
    topic.ExpirationTime = ctx.Now() + TOPIC_OK_EXPIRATION_INTERVAL;
    topic.PartitionChooser = CreatePartitionChooser(e->Result->GetPathDescription().GetPersQueueGroup());
}

void TKafkaProduceActor::Handle(TEvKafka::TEvProduceRequest::TPtr request, const TActorContext& ctx) {
    Requests.push_back(request);
    ProcessRequests(ctx);
}

void TKafkaProduceActor::ProcessRequests(const TActorContext& ctx) {
    if (&TKafkaProduceActor::StateWork != CurrentStateFunc()) {
        KAFKA_LOG_ERROR("Produce actor: Unexpected state");
        return;
    }

    if (Requests.empty()) {
        return;
    }

    if (EnqueueInitialization()) {
        PendingRequests.push_back(std::make_shared<TPendingRequest>(Requests.front()));
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
        auto r = e->Get()->Request;
        for(const auto& topicData : r->TopicData) {
            const auto& topicPath = NormalizePath(Context->DatabasePath, *topicData.Name);
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

std::pair<EKafkaErrors, THolder<TEvPartitionWriter::TEvWriteRequest>> Convert(
    const TString& transactionalId,
    const TProduceRequestData::TTopicProduceData::TPartitionProduceData& data,
    const TString& topicName,
    ui64 cookie,
    const TString& clientDC,
    bool ruPerRequest
) {
    auto ev = MakeHolder<TEvPartitionWriter::TEvWriteRequest>();
    auto& request = ev->Record;

    const auto& batch = data.Records;

    TString sourceId;
    TBuffer buf;
    buf.Reserve(transactionalId.size() + sizeof(batch->ProducerId));
    buf.Append(transactionalId.data(), transactionalId.size());
    buf.Append(static_cast<const char*>(static_cast<const void*>(&batch->ProducerId)), sizeof(batch->ProducerId));
    buf.AsString(sourceId);
    sourceId = Base64Encode(sourceId);

    auto* partitionRequest = request.MutablePartitionRequest();
    partitionRequest->SetTopic(topicName);
    partitionRequest->SetPartition(data.Index);
    partitionRequest->SetCookie(cookie);
    if (ruPerRequest) {
        partitionRequest->SetMeteringV2Enabled(true);
    }

    ui64 totalSize = 0;

    for (ui64 batchIndex = 0; batchIndex < batch->Records.size(); ++batchIndex) {
        const auto& record = batch->Records[batchIndex];

        NKikimrPQClient::TDataChunk proto;
        proto.set_codec(NPersQueueCommon::RAW);
        for(auto& h : record.Headers) {
            auto res = proto.AddMessageMeta();
            if (h.Key) {
                res->set_key(static_cast<const char*>(h.Key->data()), h.Key->size());
            }
            if (h.Value) {
                res->set_value(static_cast<const char*>(h.Value->data()), h.Value->size());
            }
        }
        auto w = partitionRequest->AddCmdWrite();

        if (record.Key) {
            auto res = proto.AddMessageMeta();
            res->set_key("__key");
            res->set_value(static_cast<const char*>(record.Key->data()), record.Key->size());
            if (record.Key->size() <= std::numeric_limits<ui16>::max())
                w->SetMessageKey(res->value());
        }

        if (record.Value) {
            proto.SetData(static_cast<const void*>(record.Value->data()), record.Value->size());
        }

        TString str;
        bool res = proto.SerializeToString(&str);
        Y_ABORT_UNLESS(res);

        w->SetSourceId(sourceId);

        bool enableKafkaDeduplication = batch->ProducerId >= 0;
        w->SetEnableKafkaDeduplication(enableKafkaDeduplication);
        if (batch->ProducerEpoch >= 0) {
            w->SetProducerEpoch(batch->ProducerEpoch);
        } else if (batch->ProducerEpoch == -1) {
            // Kafka accepts messages with producer epoch == -1, as long as it's the first "epoch" for this producer ID,
            // and ignores sequence numbers. I.e. you can send seqnos in any order with epoch == -1.
        } else if (batch->ProducerEpoch < -1) {
            return {EKafkaErrors::INVALID_PRODUCER_EPOCH, nullptr};
        }

        // set seqno
        if (enableKafkaDeduplication) {
            if (batch->BaseSequence >= 0) {
                // Handle int32 overflow.
                w->SetSeqNo((static_cast<ui64>(batch->BaseSequence) + batchIndex) % (static_cast<ui64>(std::numeric_limits<i32>::max()) + 1));
            } else {
                KAFKA_LOG_ERROR("Idempotent producer enabled and batch base sequence is less then zero: " << batch->BaseSequence);
                return {EKafkaErrors::INVALID_RECORD, nullptr};
            }
        } else {
            w->SetSeqNo(batch->BaseOffset + record.OffsetDelta);
        }

        w->SetData(str);
        ui64 createTime = batch->BaseTimestamp + record.TimestampDelta;
        w->SetCreateTimeMS(createTime ? createTime : TInstant::Now().MilliSeconds());
        w->SetDisableDeduplication(true);
        w->SetUncompressedSize(record.Value ? record.Value->size() : 0);
        w->SetClientDC(clientDC);
        w->SetIgnoreQuotaDeadline(true);
        w->SetExternalOperation(true);

        totalSize += record.Value ? record.Value->size() : 0;
    }

    partitionRequest->SetPutUnitsSize(NPQ::PutUnitsSize(totalSize));

    return {EKafkaErrors::NONE_ERROR, std::move(ev)};
}

size_t PartsCount(const TMessagePtr<TProduceRequestData>& r) {
    size_t result = 0;
    for(const auto& topicData : r->TopicData) {
        result += topicData.PartitionData.size();
    }
    return result;
}

void TKafkaProduceActor::ProcessRequest(TPendingRequest::TPtr pendingRequest, const TActorContext& ctx) {
    auto r = pendingRequest->Request->Get()->Request;
    KAFKA_LOG_D("Processing request");

    pendingRequest->Results.resize(PartsCount(r));
    pendingRequest->StartTime = ctx.Now();

    size_t position = 0;
    bool ruPerRequest = Context->Config.GetMeteringV2Enabled();
    for(const auto& topicData : r->TopicData) {
        const TString& topicPath = NormalizePath(Context->DatabasePath, *topicData.Name);
        for(const auto& partitionData : topicData.PartitionData) {
            SendWriteRequest(partitionData, *topicData.Name, pendingRequest, position, ruPerRequest, ctx);
            ++position;
        }
    }

    if (pendingRequest->WaitResultCookies.empty()) {
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
        KAFKA_LOG_W("Produce actor: Received TEvWriteAccepted with unexpected cookie " << cookie);
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
    KAFKA_LOG_D("Produce actor: Init " << request->Get()->ToString());
}

void TKafkaProduceActor::Handle(TEvPartitionWriter::TEvWriteResponse::TPtr request, const TActorContext& ctx) {
    auto r = request->Get();
    auto cookie = r->Record.GetPartitionResponse().GetCookie();
    KAFKA_LOG_T("Handling TEvPartitionWriter::TEvWriteResponse with cookie " << cookie);

    auto it = Cookies.find(cookie);
    if (it == Cookies.end()) {
        KAFKA_LOG_W("Produce actor: Received TEvWriteResponse with unexpected cookie " << cookie);
        return;
    }

    auto& cookieInfo = it->second;
    auto& partitionResult = cookieInfo.Request->Results[cookieInfo.Position];
    partitionResult.ErrorCode = EKafkaErrors::NONE_ERROR;
    partitionResult.Value = request;
    cookieInfo.Request->WaitResultCookies.erase(cookie);

    // Missing supportive partition means that we wrote in transaction and that transaction ended, thus suppprtive partition was deleted
    // it means that we are writing in a new transaction and need to create a new partition writer (cause only partition writer in init state properly creates supportive partition)
    if (r->Record.GetErrorCode() == NPersQueue::NErrorCode::EErrorCode::KAFKA_TRANSACTION_MISSING_SUPPORTIVE_PARTITION) {
        RecreatePartitionWriterAndRetry(cookie, ctx);
        return;
    } else if (!r->IsSuccess()) {
        auto wit = NonTransactionalWriters.find(cookieInfo.TopicPath);
        if (wit != NonTransactionalWriters.end()) {
            auto& partitions = wit->second;
            auto pit = partitions.find(cookieInfo.PartitionId);
            if (pit != partitions.end()) {
                Send(pit->second.ActorId, new TEvents::TEvPoison());
                partitions.erase(pit);
            }
        }
        auto txnIt = TransactionalWriters.find({cookieInfo.TopicPath, cookieInfo.PartitionId});
        if (txnIt != TransactionalWriters.end()) {
            Send(txnIt->second.ActorId, new TEvents::TEvPoison());
            TransactionalWriters.erase(txnIt);
        }
    }

    if (cookieInfo.Request->WaitResultCookies.empty()) {
        SendResults(ctx);
    }

    Cookies.erase(cookie);
}

EKafkaErrors Convert(TEvPartitionWriter::TEvWriteResponse::EErrorCode value) {
    using EErrorCode = TEvPartitionWriter::TEvWriteResponse::EErrorCode;

    switch (value) {
        case EErrorCode::PartitionDisconnected:
        case EErrorCode::PartitionNotLocal:
            return EKafkaErrors::NOT_LEADER_OR_FOLLOWER;
        default:
            return EKafkaErrors::UNKNOWN_SERVER_ERROR;
    }
}

void TKafkaProduceActor::SendResults(const TActorContext& ctx) {
    auto expireTime = ctx.Now() - REQUEST_EXPIRATION_INTERVAL;
    KAFKA_LOG_T("Produce actor: Sending results. QueueSize= " << PendingRequests.size() << ", ExpirationTime=" << expireTime);

    // We send the results in the order of receipt of the request
    while (!PendingRequests.empty()) {
        auto pendingRequest = PendingRequests.front();

        // We send the response by timeout. This is possible, for example, if the event was lost or the PartitionWrite died.
        bool expired = expireTime > pendingRequest->StartTime;

        if (!expired && !pendingRequest->WaitResultCookies.empty()) {
            return;
        }

        auto request = pendingRequest->Request->Get()->Request;
        auto correlationId = pendingRequest->Request->Get()->CorrelationId;
        EKafkaErrors metricsErrorCode = EKafkaErrors::NONE_ERROR;

        KAFKA_LOG_D("Produce actor: Send result for correlation=" << correlationId << ". Expired=" << expired);

        const auto topicsCount = request->TopicData.size();
        auto response = std::make_shared<TProduceResponseData>();
        response->Responses.resize(topicsCount);

        size_t position = 0;

        for(size_t i = 0; i < topicsCount; ++i) {
            const auto& topicData = request->TopicData[i];
            const auto partitionCount = topicData.PartitionData.size();
            auto& topicResponse =  response->Responses[i];
            topicResponse.Name = topicData.Name;
            topicResponse.PartitionResponses.resize(partitionCount);

            for(size_t j = 0; j < partitionCount; ++j) {
                const auto& partitionData = topicData.PartitionData[j];
                auto& partitionResponse = topicResponse.PartitionResponses[j];
                const auto& result = pendingRequest->Results[position++];
                size_t recordsCount = partitionData.Records.has_value() ? partitionData.Records->Records.size() : 0;
                partitionResponse.Index = partitionData.Index;
                if (EKafkaErrors::NONE_ERROR != result.ErrorCode) {
                    KAFKA_LOG_ERROR("Produce actor: Partition result with error: ErrorCode=" << static_cast<int>(result.ErrorCode) << ", ErrorMessage=" << result.ErrorMessage << ", #01");
                    partitionResponse.ErrorCode = result.ErrorCode;
                    metricsErrorCode = result.ErrorCode;
                    partitionResponse.ErrorMessage = result.ErrorMessage;

                    SendMetrics(TStringBuilder() << topicData.Name, recordsCount, "failed_messages", ctx);
                } else {
                    auto* msg = result.Value->Get();
                    if (msg->IsSuccess()) {
                        KAFKA_LOG_T("Produce actor: Partition result success.");
                        partitionResponse.ErrorCode = EKafkaErrors::NONE_ERROR;
                        auto& writeResults = msg->Record.GetPartitionResponse().GetCmdWriteResult();
                        if (!writeResults.empty()) {
                            SendMetrics(TStringBuilder() << topicData.Name, writeResults.size(), "successful_messages", ctx);
                            auto& lastResult = writeResults.at(writeResults.size() - 1);
                            partitionResponse.LogAppendTimeMs = lastResult.GetWriteTimestampMS();
                            partitionResponse.BaseOffset = writeResults.at(0).GetOffset();
                        }
                    } else {
                        KAFKA_LOG_ERROR("Produce actor: Partition result with error: ErrorCode="
                            << static_cast<int>(Convert(msg->GetError().Code))
                            << ", ErrorMessage=" << msg->GetError().Reason
                            << ", Error from writer=" << static_cast<int>(msg->Record.GetErrorCode())
                            << ", #02");
                        SendMetrics(TStringBuilder() << topicData.Name, recordsCount, "failed_messages", ctx);

                        if (msg->Record.GetErrorCode() == NPersQueue::NErrorCode::KAFKA_INVALID_PRODUCER_EPOCH) {
                            partitionResponse.ErrorCode = EKafkaErrors::INVALID_PRODUCER_EPOCH;
                            metricsErrorCode = EKafkaErrors::INVALID_PRODUCER_EPOCH;
                            partitionResponse.ErrorMessage = msg->Record.GetErrorReason();
                        } else if (msg->Record.GetErrorCode() == NPersQueue::NErrorCode::KAFKA_OUT_OF_ORDER_SEQUENCE_NUMBER) {
                            partitionResponse.ErrorCode = EKafkaErrors::OUT_OF_ORDER_SEQUENCE_NUMBER;
                            metricsErrorCode = EKafkaErrors::OUT_OF_ORDER_SEQUENCE_NUMBER;
                            partitionResponse.ErrorMessage = msg->Record.GetErrorReason();
                        } else {
                            partitionResponse.ErrorCode = Convert(msg->GetError().Code);
                            metricsErrorCode = Convert(msg->GetError().Code);
                            partitionResponse.ErrorMessage = msg->GetError().Reason;
                        }
                    }
                }
            }
        }

        Send(Context->ConnectionId, new TEvKafka::TEvResponse(correlationId, response, metricsErrorCode));

        if (!pendingRequest->WaitAcceptingCookies.empty()) {
            if (!expired) {
                TStringBuilder sb;
                sb << "Produce actor: All TEvWriteResponse were received, but not all TEvWriteAccepted. Unreceived cookies:";
                for(auto cookie : pendingRequest->WaitAcceptingCookies) {
                    sb << " " << cookie;
                }
                KAFKA_LOG_W(sb);
            }
            if (&TKafkaProduceActor::StateAccepting == CurrentStateFunc()) {
                Become(&TKafkaProduceActor::StateWork);
            }
        }

        for(auto cookie : pendingRequest->WaitAcceptingCookies) {
            Cookies.erase(cookie);
        }
        for(auto cookie : pendingRequest->WaitResultCookies) {
            Cookies.erase(cookie);
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
        KAFKA_LOG_D("Produce actor: Describe topic '" << topicPath << "'");
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(topicPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;

        request->ResultSet.emplace_back(entry);
    }

    request->DatabaseName = CanonizePath(Context->DatabasePath);

    ctx.Send(MakeSchemeCacheID(), MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(request.release()));
}

void TKafkaProduceActor::RecreatePartitionWriterAndRetry(ui64 cookie, const TActorContext& ctx) {
    auto it = Cookies.find(cookie);
    if (it != Cookies.end()) {
        auto& cookieInfo = it->second;
        KAFKA_LOG_D("Transaction was committed. Retrying produce request as a part of next transaction");
        auto txnIt = TransactionalWriters.find({cookieInfo.TopicPath, cookieInfo.PartitionId});
        if (txnIt != TransactionalWriters.end()) {
            Send(txnIt->second.ActorId, new TEvents::TEvPoison());
            TransactionalWriters.erase(txnIt);
        }
        TProduceRequestData::TProduceRequestData::TTopicProduceData::TPartitionProduceData partitionData;
        for (const auto& topicData : cookieInfo.Request->Request->Get()->Request->TopicData) {
            TString topicPath = NormalizePath(Context->DatabasePath, *topicData.Name);
            if (topicPath == cookieInfo.TopicPath) {
                for(const auto& partitionData : topicData.PartitionData) {
                    if (partitionData.Index == static_cast<int>(cookieInfo.PartitionId)) {
                        SendWriteRequest(partitionData, topicPath, cookieInfo.Request, cookieInfo.Position, cookieInfo.RuPerRequest, ctx);
                        break;
                    }
                }
            }
        }


        Cookies.erase(it);
    }
}

void TKafkaProduceActor::SendWriteRequest(const TProduceRequestData::TTopicProduceData::TPartitionProduceData& partitionData,
                                            const TString& topicName,
                                            TPendingRequest::TPtr pendingRequest,
                                            size_t position,
                                            bool& ruPerRequest,
                                            const TActorContext& ctx
                                        ) {
    auto r = pendingRequest->Request->Get()->Request;
    const TString& topicPath = NormalizePath(Context->DatabasePath, topicName);
    const auto partitionId = partitionData.Index;
    TProducerInstanceId producerInstanceId{partitionData.Records->ProducerId, partitionData.Records->ProducerEpoch};
    TMaybe<TString> transactionalId;
    if (r->TransactionalId) {
        transactionalId.ConstructInPlace(r->TransactionalId->c_str());
    }

    auto writer = PartitionWriter({topicPath, static_cast<ui32>(partitionId)}, producerInstanceId, transactionalId, ctx);
    auto& result = pendingRequest->Results[position];
    if (OK == writer.first) {
        auto ownCookie = ++Cookie;
        auto& cookieInfo = Cookies[ownCookie];
        cookieInfo.TopicPath = topicPath;
        cookieInfo.PartitionId = partitionId;
        cookieInfo.Position = position;
        cookieInfo.RuPerRequest = ruPerRequest;
        cookieInfo.Request = pendingRequest;

        pendingRequest->WaitAcceptingCookies.insert(ownCookie);
        pendingRequest->WaitResultCookies.insert(ownCookie);

        auto [error, ev] = Convert(transactionalId.GetOrElse(""), partitionData, topicPath, ownCookie, ClientDC, ruPerRequest);
        if (error == EKafkaErrors::NONE_ERROR) {
            ruPerRequest = false;
            KAFKA_LOG_T("Sending TEvPartitionWriter::TEvWriteRequest to " << writer.second << " with cookie " << ownCookie);
            Send(writer.second, std::move(ev));
            result.ErrorCode = NONE_ERROR;
        } else {
            result.ErrorCode = error;
        }
    } else {
        switch (writer.first) {
            case NOT_FOUND:
                result.ErrorCode = EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION;
                break;
            case UNAUTHORIZED:
                result.ErrorCode = EKafkaErrors::TOPIC_AUTHORIZATION_FAILED;
                break;
            case PRODUCER_FENCED:
                result.ErrorCode = EKafkaErrors::PRODUCER_FENCED;
                break;
            default:
                result.ErrorCode = EKafkaErrors::UNKNOWN_SERVER_ERROR;
        }
    }

    if (result.ErrorCode != EKafkaErrors::NONE_ERROR) {
        KAFKA_LOG_ERROR("Write request failed with error " << result.ErrorCode << " and message " << result.ErrorMessage);
    }
}

std::pair<TKafkaProduceActor::ETopicStatus, TActorId> TKafkaProduceActor::PartitionWriter(const TTopicPartition& topicPartition, const TProducerInstanceId& producerInstanceId, const TMaybe<TString>& transactionalId, const TActorContext& ctx) {
    auto it = Topics.find(topicPartition.TopicPath);
    if (it == Topics.end()) {
        KAFKA_LOG_ERROR("Produce actor: Internal error: topic '" << topicPartition.TopicPath << "' isn`t initialized");
        return { NOT_FOUND, TActorId{} };
    }

    auto& topicInfo = it->second;
    if (topicInfo.Status != OK) {
        return { topicInfo.Status, TActorId{} };
    }

    if (transactionalId) {
        return GetOrCreateTransactionalWriter(topicPartition, topicInfo, producerInstanceId, *transactionalId, ctx);
    } else {
        return GetOrCreateNonTransactionalWriter(topicPartition, topicInfo, producerInstanceId, ctx);
    }
}

std::pair<TKafkaProduceActor::ETopicStatus, TActorId> TKafkaProduceActor::GetOrCreateNonTransactionalWriter(const TTopicPartition& topicPartition, const TTopicInfo& topicInfo, const TProducerInstanceId& producerInstanceId, const TActorContext& ctx) {
    auto& partitionWriters = NonTransactionalWriters[topicPartition.TopicPath];
    auto itp = partitionWriters.find(topicPartition.PartitionId);
    if (itp != partitionWriters.end()) {
        auto& writerInfo = itp->second;
        writerInfo.LastAccessed = ctx.Now();
        return { OK, writerInfo.ActorId };
    }

    auto* partition = topicInfo.PartitionChooser->GetPartition(topicPartition.PartitionId);
    if (!partition) {
        return { NOT_FOUND, TActorId{} };
    }

    TPartitionWriterOpts opts;
    opts.WithDeduplication(false)
        .WithSourceId(SourceId)
        .WithTopicPath(topicPartition.TopicPath)
        .WithCheckRequestUnits(topicInfo.MeteringMode, Context->RlContext)
        .WithKafkaProducerInstanceId(producerInstanceId);
    auto* writerActor = CreatePartitionWriter(SelfId(), partition->TabletId, topicPartition.PartitionId, opts);

    auto& writerInfo = partitionWriters[topicPartition.PartitionId];
    writerInfo.ActorId = ctx.RegisterWithSameMailbox(writerActor);
    writerInfo.LastAccessed = ctx.Now();
    return { OK, writerInfo.ActorId };
}

std::pair<TKafkaProduceActor::ETopicStatus, TActorId> TKafkaProduceActor::GetOrCreateTransactionalWriter(const TTopicPartition& topicPartition, const TTopicInfo& topicInfo, const TProducerInstanceId& producerInstanceId, const TString& transactionalId, const TActorContext& ctx) {
    auto it = TransactionalWriters.find(topicPartition);
    if (it != TransactionalWriters.end()) {
        auto& writerInfo = it->second;
        if (writerInfo.ProducerInstanceId < producerInstanceId) {
            // send poison pill to an old writer
            Send(it->second.ActorId, new TEvents::TEvPoison());
            // register new writer for a new producer
            return CreateTransactionalWriter(topicPartition, topicInfo, producerInstanceId, transactionalId, ctx);
        } else if (writerInfo.ProducerInstanceId > producerInstanceId) { // we received zombie produce request
            return { PRODUCER_FENCED, TActorId{} };
        } else {
            writerInfo.LastAccessed = ctx.Now();
            return { OK, writerInfo.ActorId };
        }
    } else {
        return CreateTransactionalWriter(topicPartition, topicInfo, producerInstanceId, transactionalId, ctx);
    }
}

std::pair<TKafkaProduceActor::ETopicStatus, TActorId> TKafkaProduceActor::CreateTransactionalWriter(const TTopicPartition& topicPartition, const TTopicInfo& topicInfo, const TProducerInstanceId& producerInstanceId, const TString& transactionalId, const TActorContext& ctx) {
    KAFKA_LOG_D("Created transactional writer for producerId=" << producerInstanceId.Id << " and producerEpoch=" << producerInstanceId.Epoch << " for topic-partition " << topicPartition.TopicPath << ":" << topicPartition.PartitionId);
    auto* partition = topicInfo.PartitionChooser->GetPartition(topicPartition.PartitionId);
    if (!partition) {
        return { NOT_FOUND, TActorId{} };
    }

    TPartitionWriterOpts opts;
    opts.WithDeduplication(false)
        .WithSourceId(SourceId)
        .WithTopicPath(topicPartition.TopicPath)
        .WithCheckRequestUnits(topicInfo.MeteringMode, Context->RlContext)
        .WithKafkaProducerInstanceId(producerInstanceId)
        .WithKafkaTransactionalId(transactionalId);
    auto* writerActor = CreatePartitionWriter(SelfId(), partition->TabletId, topicPartition.PartitionId, opts);

    auto& writerInfo = TransactionalWriters[topicPartition];
    writerInfo.ActorId = ctx.RegisterWithSameMailbox(writerActor);
    writerInfo.LastAccessed = ctx.Now();
    writerInfo.ProducerInstanceId = producerInstanceId;
    return { OK, writerInfo.ActorId };
}

} // namespace NKafka
