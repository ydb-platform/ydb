#include "mirrorer.h"

#include <ydb/core/persqueue/common/percentiles.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/kafka/kafka_records.h>
#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/public/lib/base/msgbus.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

#define PQ_ENSURE(condition) AFL_ENSURE(condition)("tablet_id", TabletId)("partition_id", Partition)

using namespace NPersQueue;

namespace NKikimr {
namespace NPQ {

using TPersQueueReadEvent = NYdb::NTopic::TReadSessionEvent;

constexpr NKikimrServices::TActivity::EType TMirrorer::ActorActivityType() {
    return NKikimrServices::TActivity::PERSQUEUE_MIRRORER;
}

static constexpr TDuration DEFAULT_REWIND_COMMIT_OFFSET_DELAY = TDuration::Minutes(15);
static constexpr TDuration REWIND_COMMIT_INTERVAL = TDuration::Minutes(4);

namespace {

struct TBatchInfo {
    ui32 LogicalMessageCount = 1;
    std::optional<ui64> MaxSeqNo;
};

TBatchInfo GetBatchInfo(const TPersQueueReadEvent::TDataReceivedEvent::TCompressedMessage& message) {
    if (message.GetCodec() != NYdb::NTopic::ECodec::KAFKA_BATCH) {
        return {};
    }

    auto header = NKafka::ReadKafkaBatchHeader(message.GetData());
    if (!header || header->RecordsCount <= 1) {
        return {};
    }

    const ui32 logicalMessageCount = static_cast<ui32>(header->RecordsCount);
    const auto [error, maxSeqNo] = NKafka::GetBatchMaxSeqNo(*header, message.GetSeqNo());
    if (error != NKafka::EKafkaErrors::NONE_ERROR) {
        return {};
    }
    return {
        .LogicalMessageCount = logicalMessageCount,
        .MaxSeqNo = maxSeqNo,
    };
}

ui64 GetWriteRequestEndOffset(const NKikimrClient::TPersQueuePartitionRequest& request) {
    ui64 offset = request.GetCmdWriteOffset();
    for (const auto& cmd : request.GetCmdWrite()) {
        offset += cmd.GetLogicalMessageCount();
    }
    return offset;
}

} // namespace

TMirrorer::TMirrorer(
    ui64 tabletId,
    TActorId tabletActor,
    TActorId partitionActor,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    ui32 partition,
    bool localDC,
    ui64 endOffset,
    const NKikimrPQ::TMirrorPartitionConfig& config,
    const TTabletCountersBase& counters
)
    : TBaseTabletActor(tabletId, tabletActor, NKikimrServices::PQ_MIRRORER)
    , PartitionActor(partitionActor)
    , TopicConverter(topicConverter)
    , Partition(partition)
    , IsLocalDC(localDC)
    , EndOffset(endOffset)
    , OffsetToRead(endOffset)
    , Config(config)
{
    Counters.Populate(counters);
    Counters.ResetCounters();
}

void TMirrorer::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateInitConsumer);

    StartInit(ctx);
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);

    if (AppData(ctx)->Counters) {
        auto counters = AppData(ctx)->Counters;
        TString suffix = IsLocalDC ? "Remote" : "Internal";
        MirrorerErrors = NKikimr::NPQ::TMultiCounter(
            GetServiceCounters(counters, "pqproxy|writeSession"),
            GetLabels(TopicConverter), {}, {"MirrorerErrors" + suffix}, true
        );
        MirrorerTimeLags = THolder<TPercentileCounter>(new TPercentileCounter(
            GetServiceCounters(counters, "pqproxy|mirrorWriteTimeLag"),
            GetLabels(TopicConverter),
            {{"sensor", "TimeLags" + suffix}},
            "Interval", SLOW_LATENCY_MS_INTERVALS, true
        ));
        InitTimeoutCounter = NKikimr::NPQ::TMultiCounter(
            GetServiceCounters(counters, "pqproxy|writeSession"),
            GetLabels(TopicConverter), {}, {"MirrorerInitTimeout" + suffix}, true
        );
        WriteTimeoutCounter = NKikimr::NPQ::TMultiCounter(
            GetServiceCounters(counters, "pqproxy|writeSession"),
            {}, {}, {"MirrorerWriteTimeout"}, true, "sensor", false
        );
    }
}

void TMirrorer::StartInit(const TActorContext& ctx) {
    Become(&TThis::StateInitConsumer);
    LastInitStageTimestamp = ctx.Now();
    ctx.Send(SelfId(), new TEvPQ::TEvInitCredentials);
}

void TMirrorer::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    YDB_LOG_NOTICE("Killed",
        {"logPrefix", NPQ_LOG_PREFIX});
    if (ReadSession)
        ReadSession->Close(TDuration::Zero());
    ReadSession = nullptr;
    PartitionStream = nullptr;
    CredentialsProvider = nullptr;
    Die(ctx);
}

bool TMirrorer::AddToWriteRequest(
    NKikimrClient::TPersQueuePartitionRequest& request,
    TPersQueueReadEvent::TDataReceivedEvent::TCompressedMessage& message,
    bool& incorrectRequest
) {
    if (!request.HasCmdWriteOffset()) {
        if (request.CmdWriteSize() > 0) {
            incorrectRequest = true;
            return false;
        }
        request.SetCmdWriteOffset(message.GetOffset());
    }
    if (GetWriteRequestEndOffset(request) != message.GetOffset()) {
        return false;
    }

    auto write = request.AddCmdWrite();
    write->SetData(GetSerializedData(message));
    TString producerId{message.GetProducerId()};
    for (const auto& item : message.GetMeta()->Fields) {
        if (item.first == "_encoded_producer_id") {
            producerId = Base64Decode(item.second);
            break;
        }
    }
    write->SetSourceId(NSourceIdEncoding::EncodeSimple(producerId));
    write->SetSeqNo(message.GetSeqNo());
    write->SetCreateTimeMS(message.GetCreateTime().MilliSeconds());
    if (Config.GetSyncWriteTime()) {
        write->SetWriteTimeMS(message.GetWriteTime().MilliSeconds());
    }
    write->SetDisableDeduplication(true);
    write->SetUncompressedSize(message.GetUncompressedSize());

    const auto batchInfo = GetBatchInfo(message);
    if (batchInfo.LogicalMessageCount > 1) {
        write->SetLogicalMessageCount(batchInfo.LogicalMessageCount);
        write->SetMaxSeqNo(*batchInfo.MaxSeqNo);
    }
    return true;
}

void TMirrorer::ProcessError(const TActorContext& ctx, const TString& msg) {
    if (MirrorerErrors) {
        MirrorerErrors.Inc(1);
    }

    THolder<TEvPersQueue::TEvReportPartitionError> request = MakeHolder<TEvPersQueue::TEvReportPartitionError>();
    auto& record = request->Record;
    record.SetTimestamp(ctx.Now().Seconds());
    record.SetService(NKikimrServices::PQ_MIRRORER);
    record.SetMessage(TStringBuilder() << GetLogPrefix() << ": " << msg);

    Send(PartitionActor, request.Release());
}

void TMirrorer::ProcessError(const TActorContext& ctx, const TString& msg, const NKikimrClient::TResponse& response) {
    ProcessError(ctx, msg);

    if (response.HasPartitionResponse()) {
        bool isWriteRequest = response.GetPartitionResponse().HasCookie() && response.GetPartitionResponse().GetCookie() == EEventCookie::WRITE_REQUEST_COOKIE;
        if (isWriteRequest) {
            ScheduleWithIncreasingTimeout<TEvPQ::TEvRetryWrite>(SelfId(), WriteRetryTimeout, WRITE_RETRY_TIMEOUT_MAX, ctx);
        }
    }
}

void TMirrorer::AfterSuccesWrite(const TActorContext& ctx) {
    PQ_ENSURE(WriteInFlight.empty());
    PQ_ENSURE(WriteRequestInFlight);
    YDB_LOG_INFO("Written messages with current queue bytes)",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"writeRequestInFlightValueCmdWriteSize", WriteRequestInFlight.value().CmdWriteSize()},
        {"firstOffset", WriteRequestInFlight.value().GetCmdWriteOffset()},
        {"size", Queue.size()},
        {"bytesInFlight", BytesInFlight});

    WriteRequestInFlight.reset();
    Send(SelfId(), new TEvents::TEvWakeup());

    WriteRetryTimeout = WRITE_RETRY_TIMEOUT_START;

    TryUpdateWriteTimetsamp(ctx);
}

void TMirrorer::ProcessWriteResponse(
    const TActorContext& ctx,
    const NKikimrClient::TPersQueuePartitionResponse& response
) {
    PQ_ENSURE(response.CmdWriteResultSize() == WriteInFlight.size())
        ("CmdWriteResultSize", response.CmdWriteResultSize())("expected", WriteInFlight.size())
        ("First expected offset", (WriteInFlight.empty() ? -1 : WriteInFlight.front().GetOffset()))
        ("response", response);

    NYdb::NTopic::TDeferredCommit deferredCommit;

    for (auto& result : response.GetCmdWriteResult()) {
        if (result.GetAlreadyWritten()) {
            PQ_ENSURE(!WasSuccessfulRecording)("got write result 'already written', but we have successful recording before", result);
            ProcessError(ctx, TStringBuilder() << "got write result 'already written': " << result);
        } else {
            WasSuccessfulRecording = true;
        }
        auto& writtenMessageInfo = WriteInFlight.front();
        if (MirrorerTimeLags) {
            TDuration lag = TInstant::MilliSeconds(result.GetWriteTimestampMS()) - writtenMessageInfo.GetWriteTime();
            MirrorerTimeLags->IncFor(lag.MilliSeconds(), 1);
        }
        ui64 offset = writtenMessageInfo.GetOffset();
        PQ_ENSURE((ui64)result.GetOffset() == offset);
        PQ_ENSURE(EndOffset <= offset)("EndOffset", EndOffset)("offset", offset);
        const ui64 logicalMessageCount = GetBatchInfo(writtenMessageInfo).LogicalMessageCount;
        EndOffset = offset + logicalMessageCount;
        BytesInFlight -= writtenMessageInfo.GetData().size();

        deferredCommit.Add(writtenMessageInfo.GetPartitionSession(), offset, offset + logicalMessageCount);
        WriteInFlight.pop_front();
    }

    deferredCommit.Commit();
    AfterSuccesWrite(ctx);
}

void TMirrorer::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    auto& response = ev->Get()->Record;
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        ProcessError(ctx, "status is not ok: " + response.GetErrorReason(), response);
        return;
    }
    if (response.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        ProcessError(ctx, response.GetErrorReason(), response);
        return;
    }
    if (response.HasPartitionResponse()) {
        const auto& partitionResponse = response.GetPartitionResponse();
        switch(partitionResponse.GetCookie()) {
            case EEventCookie::WRITE_REQUEST_COOKIE: {
                ProcessWriteResponse(ctx, partitionResponse);
                return;
            }
            case EEventCookie::UPDATE_WRITE_TIMESTAMP: {
                YDB_LOG_DEBUG("Got response to update write timestamp",
                    {"logPrefix", NPQ_LOG_PREFIX},
                    {"request", partitionResponse});
                return;
            }
            default: {
                ProcessError(ctx, TStringBuilder() << "unexpected partition response: " << response);
                return;
            }
        }
    }
    ProcessError(ctx, TStringBuilder() << "unexpected response: " << response);
}

void TMirrorer::Handle(TEvPQ::TEvUpdateCounters::TPtr& /*ev*/, const TActorContext& ctx) {
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);
    ctx.Send(PartitionActor, new TEvPQ::TEvMirrorerCounters(Counters));
    Counters.Cumulative().ResetCounters();
    Counters.Percentile().ResetCounters();

    if (ctx.Now() - LastStateLogTimestamp > LOG_STATE_INTERVAL) {
        LastStateLogTimestamp = ctx.Now();
        YDB_LOG_NOTICE("[STATE] read credentials credentials request",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"currentState", GetCurrentState()},
            {"session", bool(ReadSession)},
            {"provider", bool(CredentialsProvider)},
            {"inflight", CredentialsRequestInFlight});
        if (ReadSession) {
            YDB_LOG_NOTICE("[STATE] read session id",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"readSessionSessionId", ReadSession->GetSessionId()});
        }
        if (PartitionStream) {
            YDB_LOG_NOTICE("[STATE] has partition stream with id",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"partitionStreamTopicPath", PartitionStream->GetTopicPath()},
                {"partitionStreamPartitionId", PartitionStream->GetPartitionId()},
                {"partitionStreamPartitionSessionId", PartitionStream->GetPartitionSessionId()});
        } else {
            YDB_LOG_NOTICE("[STATE] hasn't partition stream",
                {"logPrefix", NPQ_LOG_PREFIX});
        }
        if (StreamStatus) {
            YDB_LOG_NOTICE("[STATE] last source partition",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"status", StreamStatus->DebugString()});
        }
        YDB_LOG_NOTICE("[STATE] next offset to read current end offset",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"offsetToRead", OffsetToRead},
            {"endOffset", EndOffset});
        YDB_LOG_NOTICE("[STATE] bytes in flight messages in write request queue",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"bytesInFlight", BytesInFlight},
            {"writeInFlightSize", WriteInFlight.size()},
            {"toWrite", Queue.size()});
        YDB_LOG_NOTICE("[STATE] wait new reader last received event read futures inflight",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"event", WaitNextReaderEventInFlight},
            {"time", LastReadEventTime},
            {"readFuturesInFlight", ReadFuturesInFlight},
            {"lastId", ReadFeatureId});
        if (!ReadFeatures.empty()) {
            const auto& oldest = *ReadFeatures.begin();
            const auto& info = oldest.second;
            YDB_LOG_NOTICE("[STATE] The oldest read future future / /",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"id", oldest.first},
                {"ts", info.first},
                {"age", (ctx.Now() - info.first)},
                {"state", info.second.Initialized()},
                {"hasValue", info.second.HasValue()},
                {"hasException", info.second.HasException()});
        }
    }
    if (!ReadSession && LastInitStageTimestamp + INIT_TIMEOUT < ctx.Now()) {
        ProcessError(ctx, TStringBuilder() << "read session was not created, the last stage of initialization was at "
            << LastInitStageTimestamp.Seconds());
        if (InitTimeoutCounter) {
            InitTimeoutCounter.Inc(1);
        }
        StartInit(ctx);
        return;
    }
    if (ReadSession && LastReadEventTime != TInstant::Zero() && ctx.Now() - LastReadEventTime > RECEIVE_READ_EVENT_TIMEOUT) {
        ProcessError(ctx, TStringBuilder() << "receive read event timeout, last event was at " << LastReadEventTime
            << " (" << ctx.Now() - LastReadEventTime << "). Read session will be recreated.");
        ScheduleConsumerCreation(ctx);
        return;
    }
    if (WriteRequestInFlight && WriteRequestTimestamp + WRITE_TIMEOUT < ctx.Now()) {
        YDB_LOG_ERROR("Write request was sent at but no response has been received yet. Tablet will be killed",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"writeRequestTimestampSeconds", WriteRequestTimestamp.Seconds()});
        if (WriteTimeoutCounter) {
            WriteTimeoutCounter.Inc(1);
        }
        ctx.Send(TabletActorId, new TEvents::TEvPoisonPill());
        return;
    }

    DoProcessNextReaderEvent(ctx, true);  // LOGBROKER-7430
}

void TMirrorer::HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    bool equalConfigs = google::protobuf::util::MessageDifferencer::Equals(
        Config,
        ev->Get()->Config.GetPartitionConfig().GetMirrorFrom()
    );
    YDB_LOG_NOTICE("Got new config, equal with",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"previous", equalConfigs});
    if (!equalConfigs) {
        Config = ev->Get()->Config.GetPartitionConfig().GetMirrorFrom();
        YDB_LOG_NOTICE("Changing config",
            {"logPrefix", NPQ_LOG_PREFIX});

        StartInit(ctx);
    }
}

void TMirrorer::TryToRead(const TActorContext& ctx) {
    if (BytesInFlight < MAX_BYTES_IN_FLIGHT && ReadSession) {
        StartWaitNextReaderEvent(ctx);
    }
}

void TMirrorer::TryToWrite(const TActorContext& ctx) {
    if (WriteRequestInFlight || Queue.empty()) {
        return;
    }

    THolder<TEvPersQueue::TEvRequest> request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto req = request->Record.MutablePartitionRequest();
    //ToDo
    req->SetTopic(TopicConverter->GetClientsideName());
    req->SetPartition(Partition);
    req->SetMessageNo(0);
    req->SetCookie(WRITE_REQUEST_COOKIE);

    bool incorrectRequest = false;
    while (!Queue.empty() && AddToWriteRequest(*req, Queue.front(), incorrectRequest)) {
        WriteInFlight.emplace_back(std::move(Queue.front()));
        Queue.pop_front();
    }
    if (incorrectRequest) {
        ProcessError(ctx, TStringBuilder() << " incorrect filling of CmdWrite request: "
            << req->DebugString());
        ScheduleConsumerCreation(ctx);
        return;
    }

    WriteRequestInFlight = request->Record.GetPartitionRequest();

    Send(TabletActorId, request.Release());
    WriteRequestTimestamp = ctx.Now();
}

void TMirrorer::TryToSplitMerge(const TActorContext& ctx) {
    if (!EndPartitionSessionEvent) {
        return;
    }
    if (!AppData(ctx)->FeatureFlags.GetEnableMirroredTopicSplitMerge()) {
        return;
    }
    if (WriteRequestInFlight || !Queue.empty()) {
        YDB_LOG_DEBUG("Postpone split-merge event until all write operations completed",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }
    const bool isSplit = EndPartitionSessionEvent->GetAdjacentPartitionIds().empty();
    if (!isSplit) {
        YDB_LOG_WARN("Topic merge not supported yet",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }
    if (EndPartitionSessionEvent->GetChildPartitionIds().empty()) {
        YDB_LOG_WARN("Split-merge operation has no child partitions",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }
    const ::NKikimrPQ::EScaleStatus value = isSplit ? NKikimrPQ::EScaleStatus::NEED_SPLIT : NKikimrPQ::EScaleStatus::NEED_MERGE;
    THolder request = MakeHolder<TEvPQ::TEvPartitionScaleStatusChanged>();
    request->Record.SetPartitionId(Partition);
    request->Record.SetScaleStatus(value);
    auto* relation = request->Record.MutableParticipatingPartitions();
    for (const auto& p : EndPartitionSessionEvent->GetChildPartitionIds()) {
        relation->AddChildPartitionIds(p);
    }
    for (const auto& p : EndPartitionSessionEvent->GetAdjacentPartitionIds()) {
        relation->AddAdjacentPartitionIds(p);
    }
    Send(PartitionActor, std::move(request));
    EndPartitionSessionEvent = std::nullopt;
}

void TMirrorer::HandleInitCredentials(TEvPQ::TEvInitCredentials::TPtr& /*ev*/, const TActorContext& ctx) {
    if (CredentialsRequestInFlight) {
        YDB_LOG_WARN("Credentials request already inflight",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }
    LastInitStageTimestamp = ctx.Now();
    CredentialsProvider = nullptr;

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
    auto future = factory->GetCredentialsProvider(Config.GetCredentials());
    future.Subscribe(
        [
            actorSystem = ctx.ActorSystem(),
            selfId = SelfId()
        ](const NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr>& result) {
            THolder<TEvPQ::TEvCredentialsCreated> ev;
            if (result.HasException()) {
                TString error;
                try {
                    result.TryRethrow();
                } catch(...) {
                    error = CurrentExceptionMessage();
                }
                ev = MakeHolder<TEvPQ::TEvCredentialsCreated>(error);
            } else {
                ev = MakeHolder<TEvPQ::TEvCredentialsCreated>(result.GetValue());
            }
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release()));
        }
    );
    CredentialsRequestInFlight = true;
}

void TMirrorer::HandleCredentialsCreated(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx) {
    CredentialsRequestInFlight = false;
    if (ev->Get()->Error) {
        ProcessError(ctx, TStringBuilder() << "cannot initialize credentials provider: " << ev->Get()->Error.value());
        ScheduleWithIncreasingTimeout<TEvPQ::TEvInitCredentials>(SelfId(), ConsumerInitInterval, CONSUMER_INIT_INTERVAL_MAX, ctx);
        return;
    }

    CredentialsProvider = ev->Get()->Credentials;
    YDB_LOG_NOTICE("Credentials provider created",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"hasCredentialsProvider", bool(CredentialsProvider)});
    ConsumerInitInterval = CONSUMER_INIT_INTERVAL_START;
    ScheduleConsumerCreation(ctx);
}

void TMirrorer::RetryWrite(const TActorContext& ctx) {
    PQ_ENSURE(WriteRequestInFlight);

    THolder<TEvPersQueue::TEvRequest> request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto req = request->Record.MutablePartitionRequest();
    req->CopyFrom(WriteRequestInFlight.value());

    Send(TabletActorId, request.Release());
    WriteRequestTimestamp = ctx.Now();
}

void TMirrorer::HandleRetryWrite(TEvPQ::TEvRetryWrite::TPtr& /*ev*/, const TActorContext& ctx) {
    RetryWrite(ctx);
}

void TMirrorer::HandleWakeup(const TActorContext& ctx) {
    TryToRead(ctx);
    TryToWrite(ctx);
    TryToSplitMerge(ctx);
}

void TMirrorer::CreateConsumer(TEvPQ::TEvCreateConsumer::TPtr&, const TActorContext& ctx) {
    if (CredentialsRequestInFlight) {
        // защита от гонки между TEvInitCredentials, TEvCredentialsCreated и TEvCreateConsumer
        // когда придёт TEvCredentialsCreated актор ещё раз отправит себе TEvCreateConsumer
        YDB_LOG_WARN("Wait for credentials response",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }

    LastInitStageTimestamp = ctx.Now();
    YDB_LOG_NOTICE("Creating new read session",
        {"logPrefix", NPQ_LOG_PREFIX});

    if (!Queue.empty()) {
        OffsetToRead = Queue.front().GetOffset();
        while (!Queue.empty()) {
            ui64 dataSize = Queue.back().GetData().size();
            PQ_ENSURE(BytesInFlight >= dataSize);
            BytesInFlight -= dataSize;
            Queue.pop_back();
        }
    }
    if (ReadSession) {
        ReadSession->Close(TDuration::Zero());
    }
    ReadSession.reset();
    PartitionStream.Reset();

    auto* factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);

    TLog log(MakeHolder<TDeferredActorLogBackend>(
        factory->GetSharedActorSystem(),
        NKikimrServices::PQ_MIRRORER
    ));

    TString logPrefix = TStringBuilder() << GetLogPrefix() << "[reader " << ++ReaderGeneration << "] ";
    log.SetFormatter([logPrefix](ELogPriority, TStringBuf message) -> TString {
        return logPrefix + message;
    });

    try {
        if (ReadSession) {
            ReadSession->Close(TDuration::Zero());
        }
        ReadSession = factory->GetReadSession(Config, Partition, CredentialsProvider, MAX_BYTES_IN_FLIGHT, log);
    } catch(...) {
        ProcessError(ctx, TStringBuilder() << "got an exception during the creation read session: " << CurrentExceptionMessage());
        ScheduleConsumerCreation(ctx);
        return;
    }

    YDB_LOG_NOTICE("Read session",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"created", ReadSession->GetSessionId()});

    Send(SelfId(), new TEvents::TEvWakeup());
    Become(&TThis::StateWork);
}

void TMirrorer::RequestSourcePartitionStatus(TEvPQ::TEvRequestPartitionStatus::TPtr&, const TActorContext&) {
    RequestSourcePartitionStatus();
}

void TMirrorer::RequestSourcePartitionStatus() {
    if (PartitionStream && ReadSession) {
        PartitionStream->RequestStatus();
    }
}

void TMirrorer::TryUpdateWriteTimetsamp(const TActorContext &ctx) {
    if (Config.GetSyncWriteTime() && !WriteRequestInFlight && StreamStatus && EndOffset == StreamStatus->GetEndOffset()) {
        YDB_LOG_INFO("Update write timestamp from original",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"topic", StreamStatus->DebugString()});
        THolder<TEvPersQueue::TEvRequest> request = MakeHolder<TEvPersQueue::TEvRequest>();
        auto req = request->Record.MutablePartitionRequest();
        req->SetTopic(TopicConverter->GetClientsideName());
        req->SetPartition(Partition);
        req->SetCookie(UPDATE_WRITE_TIMESTAMP);
        req->MutableCmdUpdateWriteTimestamp()->SetWriteTimeMS(StreamStatus->GetWriteTimeHighWatermark().MilliSeconds());
        ctx.Send(TabletActorId, request.Release());
    }
}

void TMirrorer::AddMessagesToQueue(std::vector<TPersQueueReadEvent::TDataReceivedEvent::TCompressedMessage>&& messages) {
    for (auto& msg : messages) {
        ui64 offset = msg.GetOffset();
        PQ_ENSURE(OffsetToRead <= offset);
        LastReadOffset = offset;
        ui64 messageSize = msg.GetData().size();

        Counters.Cumulative()[COUNTER_PQ_TABLET_NETWORK_BYTES_USAGE].Increment(messageSize);
        BytesInFlight += messageSize;

        OffsetToRead = offset + 1;
        Queue.emplace_back(std::move(msg));
    }
}

void TMirrorer::ScheduleConsumerCreation(const TActorContext& ctx) {
    LastInitStageTimestamp = ctx.Now();
    if (ReadSession)
        ReadSession->Close(TDuration::Zero());
    ReadSession = nullptr;
    PartitionStream = nullptr;
    EndPartitionSessionEvent = std::nullopt;
    ReadFuturesInFlight = 0;
    ReadFeatures.clear();
    WaitNextReaderEventInFlight = false;
    LastReadEventTime = TInstant::Zero();
    LastReadOffset = Nothing();

    Become(&TThis::StateInitConsumer);

    YDB_LOG_NOTICE("Schedule consumer creation",
        {"logPrefix", NPQ_LOG_PREFIX});
    ScheduleWithIncreasingTimeout<TEvPQ::TEvCreateConsumer>(SelfId(), ConsumerInitInterval, CONSUMER_INIT_INTERVAL_MAX, ctx);
}

TString TMirrorer::BuildLogPrefix() const {
    return TStringBuilder() << "[Mirrorer][" << TopicConverter->GetPrintableString() << ':' << Partition << "] ";
}

TString TMirrorer::GetCurrentState() const {
    if (CurrentStateFunc() == &TThis::StateInitConsumer) {
        return "StateInitConsumer";
    } else if (CurrentStateFunc() == &TThis::StateWork) {
        return "StateWork";
    }
    return "UNKNOWN";
}

void TMirrorer::StartWaitNextReaderEvent(const TActorContext& ctx) {
    if (WaitNextReaderEventInFlight) {
        return;
    }
    WaitNextReaderEventInFlight = true;

    ui64 futureId = ++ReadFeatureId;
    ++ReadFuturesInFlight;
    auto future = ReadSession->WaitEvent();

    future.Subscribe(
        [
            actorSystem = ctx.ActorSystem(),
            selfId = SelfId(),
            futureId=futureId
        ](const NThreading::TFuture<void>&) {
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, new TEvPQ::TEvReaderEventArrived(futureId)));
        }
    );

    if (ReadFeatures.size() < MAX_READ_FUTURES_STORE) {
        ReadFeatures[futureId] = {ctx.Now(), future};
    }
}

void TMirrorer::ProcessNextReaderEvent(TEvPQ::TEvReaderEventArrived::TPtr& ev, const TActorContext& ctx) {
    {
        --ReadFuturesInFlight;
        ReadFeatures.erase(ev->Get()->Id);
    }
    DoProcessNextReaderEvent(ctx);
}

void TMirrorer::DoProcessNextReaderEvent(const TActorContext& ctx, bool wakeup) {
    if (!WaitNextReaderEventInFlight) {
        return;
    }
    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
    YDB_LOG_DEBUG("Got next reader",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"event", bool(event)});

    if (wakeup && !event) {
        return;
    }

    WaitNextReaderEventInFlight = false;
    if (!event) {
        StartWaitNextReaderEvent(ctx);
        return;
    }
    LastReadEventTime = ctx.Now();

    if (auto* dataEvent = std::get_if<TPersQueueReadEvent::TDataReceivedEvent>(&event.value())) {
        AddMessagesToQueue(std::move(dataEvent->GetCompressedMessages()));
    } else if (auto* createStream = std::get_if<TPersQueueReadEvent::TStartPartitionSessionEvent>(&event.value())) {
        YDB_LOG_INFO("Got create stream event for and will set",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"createStreamDebug", createStream->DebugString()},
            {"offset", OffsetToRead});
        if (PartitionStream) {
            ProcessError(ctx, TStringBuilder() << " already has stream " << PartitionStream->GetPartitionSessionId()
                << ", new stream " << createStream->GetPartitionSession()->GetPartitionSessionId());
            ScheduleConsumerCreation(ctx);
            return;
        }

        PartitionStream = createStream->GetPartitionSession();
        if (Partition != PartitionStream->GetPartitionId()) {
            ProcessError(ctx, TStringBuilder() << " got stream for incorrect partition, stream: topic=" << PartitionStream->GetTopicPath()
                << " partition=" << PartitionStream->GetPartitionId());
            ScheduleConsumerCreation(ctx);
            return;
        }
        if (OffsetToRead < createStream->GetCommittedOffset()) {
            ProcessError(ctx, TStringBuilder() << "stream has commit offset more then partition end offset,"
                << "gap will be created [" << OffsetToRead << ";" << createStream->GetCommittedOffset() << ")"
           );

           OffsetToRead = createStream->GetCommittedOffset();
        }
        LastReadOffset = Nothing();
        createStream->Confirm(OffsetToRead, createStream->GetCommittedOffset());
        RequestSourcePartitionStatus();
    } else if (auto* destroyStream = std::get_if<TPersQueueReadEvent::TStopPartitionSessionEvent>(&event.value())) {
        destroyStream->Confirm();

        PartitionStream.Reset();
        YDB_LOG_INFO("Got destroy stream",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"event", destroyStream->DebugString()});
   } else if (auto* streamClosed = std::get_if<TPersQueueReadEvent::TPartitionSessionClosedEvent>(&event.value())) {
        PartitionStream.Reset();
        YDB_LOG_INFO("Got stream closed event for partition stream",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"id", streamClosed->GetPartitionSession()->GetPartitionSessionId()},
            {"reason", streamClosed->GetReason()});

        ProcessError(ctx, TStringBuilder() << " read session stream closed event");
        ScheduleConsumerCreation(ctx);
        return;

    } else if (auto* streamStatus = std::get_if<TPersQueueReadEvent::TPartitionSessionStatusEvent >(&event.value())) {
        if (PartitionStream
            && PartitionStream->GetPartitionSessionId() == streamStatus->GetPartitionSession()->GetPartitionSessionId()
        ) {
            StreamStatus = MakeHolder<TPersQueueReadEvent::TPartitionSessionStatusEvent>(*streamStatus);
            TryRewindCommittedOffset(ctx);
            ctx.Schedule(TDuration::Seconds(1), new TEvPQ::TEvRequestPartitionStatus);
            TryUpdateWriteTimetsamp(ctx);
        }
    } else if (auto* commitAck = std::get_if<TPersQueueReadEvent::TCommitOffsetAcknowledgementEvent>(&event.value())) {
        YDB_LOG_INFO("Got commit responce, commited",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"offset", commitAck->GetCommittedOffset()});
    } else if (auto* closeSessionEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event.value())) {
        ProcessError(ctx, TStringBuilder() << " read session closed: " << closeSessionEvent->DebugString());
        ScheduleConsumerCreation(ctx);
        return;
    } else if (auto* endPartitionSessionEvent = std::get_if<TPersQueueReadEvent::TEndPartitionSessionEvent>(&event.value())) {
        YDB_LOG_INFO("Got end partion session",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"event", endPartitionSessionEvent->DebugString()});
        if (EndPartitionSessionEvent.has_value()) {
            YDB_LOG_WARN("Already has end partition session event",
                {"logPrefix", NPQ_LOG_PREFIX});
            EndPartitionSessionEvent.reset();
        }
        EndPartitionSessionEvent = *endPartitionSessionEvent;
    } else {
        ProcessError(ctx, TStringBuilder() << " got unmatched event: " << event.value().index());
        ScheduleConsumerCreation(ctx);
        return;
    }

    Send(SelfId(), new TEvents::TEvWakeup());
    ConsumerInitInterval = CONSUMER_INIT_INTERVAL_START;
}

static TDuration GetRewindCommitDelay(const TActorContext& ctx) {
    const auto& mirrorConfig = AppData(ctx)->PQConfig.GetMirrorConfig();
    if (!mirrorConfig.HasRewindCommitDelaySeconds()) {
        return DEFAULT_REWIND_COMMIT_OFFSET_DELAY;
    }
    return TDuration::Seconds(mirrorConfig.GetRewindCommitDelaySeconds());
}

bool TMirrorer::TryRewindCommittedOffset(const TActorContext& ctx) {
    YDB_LOG_TRACE("TryRewindCommittedOffset",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"offsetToRead", OffsetToRead},
        {"committedOffset", StreamStatus->GetCommittedOffset()},
        {"readOffset", StreamStatus->GetReadOffset()},
        {"endOffset", StreamStatus->GetEndOffset()},
        {"secondsSinceLastInitStage", (ctx.Now() - LastInitStageTimestamp).Seconds()},
        {"secondsSinceLastRewindCommit", (ctx.Now() - LastRewindCommitTimestamp).Seconds()});
    if (!(LastReadOffset.Empty() /* never seen any data is this read session */
        && StreamStatus->GetCommittedOffset() < StreamStatus->GetEndOffset()
        && StreamStatus->GetReadOffset() == StreamStatus->GetEndOffset())) {
        return false;
    }
    const auto now = ctx.Now();
    if (now - LastInitStageTimestamp <= GetRewindCommitDelay(ctx)) {
        return false;
    }
    if (now - LastRewindCommitTimestamp <= REWIND_COMMIT_INTERVAL) {
        return false;
    }
    LastRewindCommitTimestamp = now;
    const ui64 newEndOffset = StreamStatus->GetEndOffset();
    YDB_LOG_INFO("Topic contains only old messages. Rewinding committed offset forward",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"from", StreamStatus->GetCommittedOffset()},
        {"to", newEndOffset});
    auto* factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
    auto future = factory->CommitOffset(Config, CredentialsProvider, Partition, newEndOffset);
    future.Subscribe(
        [actorSystem = ctx.ActorSystem(), selfId = SelfId(), newEndOffset](const NThreading::TFuture<NYdb::TStatus>& result) {
            NYdb::TStatus status{NYdb::EStatus::SUCCESS, {}};
            try {
                status = result.GetValue();
            } catch (...) {
                TString error = CurrentExceptionMessage();
                status = NYdb::TStatus{NYdb::EStatus::INTERNAL_ERROR, NYdb::NIssue::TIssues({NYdb::NIssue::TIssue(std::move(error)),})};
            }
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, new TEvPQ::TEvRewindCommitResult(std::move(status), newEndOffset)));
        }
    );
    return true;
}

void TMirrorer::HandleRewindCommit(TEvPQ::TEvRewindCommitResult::TPtr& ev, const TActorContext& ctx) {
<<<<<<< HEAD
    YDB_LOG_INFO("Rewind committed offset",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"result", ev->Get()->Status});
=======
    LOG_I("Rewind committed offset result: " << ev->Get()->Status << "; offset: " << ev->Get()->EndOffset);
>>>>>>> main
    if (!ev->Get()->Status.IsSuccess()) {
        ProcessError(ctx, TStringBuilder() << "failed to rewind committed offset: " << ev->Get()->Status << "; offset: " << ev->Get()->EndOffset);
        return;
    }
    EndOffset = ev->Get()->EndOffset;
    ScheduleConsumerCreation(ctx);
}

NActors::IActor* CreateMirrorer(const ui64 tabletId,
                                const NActors::TActorId& tabletActor,
                                const NActors::TActorId& partitionActor,
                                const NPersQueue::TTopicConverterPtr& topicConverter,
                                const ui32 partition,
                                const bool localDC,
                                const ui64 endOffset,
                                const NKikimrPQ::TMirrorPartitionConfig& config,
                                const TTabletCountersBase& counters) {
    return new TMirrorer(tabletId, tabletActor, partitionActor, topicConverter, partition, localDC, endOffset, config, counters);
}

}// NPQ
}// NKikimr
