#include "mirrorer.h"
#include "write_meta.h"

#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/string/join.h>

using namespace NPersQueue;

namespace NKikimr {
namespace NPQ {

using TPersQueueReadEvent = NYdb::NTopic::TReadSessionEvent;

constexpr NKikimrServices::TActivity::EType TMirrorer::ActorActivityType() {
    return NKikimrServices::TActivity::PERSQUEUE_MIRRORER;
}

TMirrorer::TMirrorer(
    TActorId tabletActor,
    TActorId partitionActor,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    ui32 partition,
    bool localDC,
    ui64 endOffset,
    const NKikimrPQ::TMirrorPartitionConfig& config,
    const TTabletCountersBase& counters
)
    : TabletActor(tabletActor)
    , PartitionActor(partitionActor)
    , TopicConverter(topicConverter)
    , Partition(partition)
    , IsLocalDC(localDC)
    , EndOffset(endOffset)
    , OffsetToRead(endOffset)
    , Config(config)
{
    Counters.Populate(counters);
}

void TMirrorer::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateInitConsumer);

    StartInit(ctx);
    ctx.Schedule(UPDATE_COUNTERS_INTERVAL, new TEvPQ::TEvUpdateCounters);

    if (AppData(ctx)->Counters) {
        TVector<std::pair<ui64, TString>> lagsIntervals{{100, "100ms"}, {200, "200ms"}, {500, "500ms"},
                                                                       {1000, "1000ms"}, {2000, "2000ms"}, {5000, "5000ms"}, {10000, "10000ms"},
                                                                       {30000, "30000ms"}, {60000, "60000ms"}, {180000,"180000ms"}, {9999999, "999999ms"}};

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
            "Interval", lagsIntervals, true
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
    LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " killed");
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
    if (request.GetCmdWriteOffset() + request.CmdWriteSize() != message.GetOffset()) {
        return false;
    }

    auto write = request.AddCmdWrite();
    write->SetData(GetSerializedData(message));
    TString producerId = message.GetProducerId();
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
    record.SetMessage(TStringBuilder() << MirrorerDescription() << ": " << msg);

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
    Y_ABORT_UNLESS(WriteInFlight.empty());
    Y_ABORT_UNLESS(WriteRequestInFlight);
    LOG_INFO_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
        << " written " <<  WriteRequestInFlight.value().CmdWriteSize()
        << " messages with first offset=" << WriteRequestInFlight.value().GetCmdWriteOffset()
        << ", current queue size: " << Queue.size() << "(" << BytesInFlight << "bytes)");

    WriteRequestInFlight.reset();
    Send(SelfId(), new TEvents::TEvWakeup());

    WriteRetryTimeout = WRITE_RETRY_TIMEOUT_START;

    TryUpdateWriteTimetsamp(ctx);
}

void TMirrorer::ProcessWriteResponse(
    const TActorContext& ctx,
    const NKikimrClient::TPersQueuePartitionResponse& response
) {
    Y_VERIFY_S(response.CmdWriteResultSize() == WriteInFlight.size(), MirrorerDescription()
        << "CmdWriteResultSize=" << response.CmdWriteResultSize() << ", but expected=" << WriteInFlight.size()
        << ". First expected offset= " << (WriteInFlight.empty() ? -1 : WriteInFlight.front().GetOffset())
        << " response: " << response);

    NYdb::NTopic::TDeferredCommit deferredCommit;

    for (auto& result : response.GetCmdWriteResult()) {
        if (result.GetAlreadyWritten()) {
            Y_VERIFY_S(
                !WasSuccessfulRecording,
                MirrorerDescription() << "got write result 'already written',"
                    <<" but we have successful recording before: " << result
            );
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
        Y_ABORT_UNLESS((ui64)result.GetOffset() == offset);
        Y_VERIFY_S(EndOffset <= offset, MirrorerDescription()
            << "end offset more the written " << EndOffset << ">" << offset);
        EndOffset = offset + 1;
        BytesInFlight -= writtenMessageInfo.GetData().size();

        deferredCommit.Add(writtenMessageInfo.GetPartitionSession(), offset);
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
                LOG_DEBUG_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
                    << " got response to update write timestamp request: " << partitionResponse);
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

    if (ctx.Now() - LastStateLogTimestamp > LOG_STATE_INTERVAL) {
        LastStateLogTimestamp = ctx.Now();
        LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << "[STATE] current state=" << GetCurrentState()
            << ", read session=" << bool(ReadSession) << ", credentials provider=" << bool(CredentialsProvider)
            << ", credentials request inflight=" << CredentialsRequestInFlight);
        if (ReadSession) {
            LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
                << "[STATE] read session id " << ReadSession->GetSessionId());
        }
        if (PartitionStream) {
            LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
                << "[STATE] has partition stream " << PartitionStream->GetTopicPath()
                << ":" << PartitionStream->GetPartitionId()
                << " with id " << PartitionStream->GetPartitionSessionId());
        } else {
            LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << "[STATE] hasn't partition stream");
        }
        if (StreamStatus) {
            LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
                << "[STATE] last source partition status: " << StreamStatus->DebugString());
        }
        LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << "[STATE] next offset to read " << OffsetToRead << ", current end offset " << EndOffset);
        LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << "[STATE] bytes in flight " << BytesInFlight
            << ", messages in write request " << WriteInFlight.size()
            << ", queue to write: " << Queue.size());
        LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << "[STATE] wait new reader event=" << WaitNextReaderEventInFlight
            << ", last received event time=" << LastReadEventTime
            << ", read futures inflight  " << ReadFuturesInFlight << ", last id=" << ReadFeatureId);
        if (!ReadFeatures.empty()) {
            const auto& oldest = *ReadFeatures.begin();
            const auto& info = oldest.second;
            LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << "[STATE] The oldest read future id=" << oldest.first
            << ", ts=" << info.first << " age=" << (ctx.Now() - info.first)
            << ", future state: " << info.second.Initialized()
            << "/" << info.second.HasValue() << "/" << info.second.HasException());
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
        LOG_ERROR_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " write request was sent at "
            << WriteRequestTimestamp.Seconds() << ", but no response has been received yet. Tablet will be killed.");
        if (WriteTimeoutCounter) {
            WriteTimeoutCounter.Inc(1);
        }
        ctx.Send(TabletActor, new TEvents::TEvPoisonPill());
        return;
    }

    DoProcessNextReaderEvent(ctx, true);  // LOGBROKER-7430
}

void TMirrorer::HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    bool equalConfigs = google::protobuf::util::MessageDifferencer::Equals(
        Config,
        ev->Get()->Config.GetPartitionConfig().GetMirrorFrom()
    );
    LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " got new config, equal with previous: " << equalConfigs);
    if (!equalConfigs) {
        Config = ev->Get()->Config.GetPartitionConfig().GetMirrorFrom();
        LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " changing config");

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

    Send(TabletActor, request.Release());
    WriteRequestTimestamp = ctx.Now();
}


void TMirrorer::HandleInitCredentials(TEvPQ::TEvInitCredentials::TPtr& /*ev*/, const TActorContext& ctx) {
    if (CredentialsRequestInFlight) {
        LOG_WARN_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " credentials request already inflight.");
        return;
    }
    LastInitStageTimestamp = ctx.Now();
    CredentialsProvider = nullptr;

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    Y_ABORT_UNLESS(factory);
    auto future = factory->GetCredentialsProvider(Config.GetCredentials());
    future.Subscribe(
        [
            actorSystem = ctx.ExecutorThread.ActorSystem,
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
    LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " credentials provider created " << bool(CredentialsProvider));
    ConsumerInitInterval = CONSUMER_INIT_INTERVAL_START;
    ScheduleConsumerCreation(ctx);
}

void TMirrorer::RetryWrite(const TActorContext& ctx) {
    Y_ABORT_UNLESS(WriteRequestInFlight);

    THolder<TEvPersQueue::TEvRequest> request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto req = request->Record.MutablePartitionRequest();
    req->CopyFrom(WriteRequestInFlight.value());

    Send(TabletActor, request.Release());
    WriteRequestTimestamp = ctx.Now();
}

void TMirrorer::HandleRetryWrite(TEvPQ::TEvRetryWrite::TPtr& /*ev*/, const TActorContext& ctx) {
    RetryWrite(ctx);
}

void TMirrorer::HandleWakeup(const TActorContext& ctx) {
    TryToRead(ctx);
    TryToWrite(ctx);
}

void TMirrorer::CreateConsumer(TEvPQ::TEvCreateConsumer::TPtr&, const TActorContext& ctx) {
    LastInitStageTimestamp = ctx.Now();
    LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " creating new read session");

    if (!Queue.empty()) {
        OffsetToRead = Queue.front().GetOffset();
        while (!Queue.empty()) {
            ui64 dataSize = Queue.back().GetData().size();
            Y_ABORT_UNLESS(BytesInFlight >= dataSize);
            BytesInFlight -= dataSize;
            Queue.pop_back();
        }
    }
    if (ReadSession) {
        ReadSession->Close(TDuration::Zero());
    }
    ReadSession.reset();
    PartitionStream.Reset();

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    Y_ABORT_UNLESS(factory);

    TLog log(MakeHolder<TDeferredActorLogBackend>(
        factory->GetSharedActorSystem(),
        NKikimrServices::PQ_MIRRORER
    ));

    TString logPrefix = TStringBuilder() << MirrorerDescription() << "[reader " << ++ReaderGeneration << "] ";
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

    LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << " read session created: " << ReadSession->GetSessionId());

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
        LOG_INFO_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << " update write timestamp from original topic: " << StreamStatus->DebugString());
        THolder<TEvPersQueue::TEvRequest> request = MakeHolder<TEvPersQueue::TEvRequest>();
        auto req = request->Record.MutablePartitionRequest();
        req->SetTopic(TopicConverter->GetClientsideName());
        req->SetPartition(Partition);
        req->SetCookie(UPDATE_WRITE_TIMESTAMP);
        req->MutableCmdUpdateWriteTimestamp()->SetWriteTimeMS(StreamStatus->GetWriteTimeHighWatermark().MilliSeconds());
        ctx.Send(TabletActor, request.Release());
    }
}

void TMirrorer::AddMessagesToQueue(TVector<TPersQueueReadEvent::TDataReceivedEvent::TCompressedMessage>&& messages) {
    for (auto& msg : messages) {
        ui64 offset = msg.GetOffset();
        Y_ABORT_UNLESS(OffsetToRead <= offset);
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
    ReadFuturesInFlight = 0;
    ReadFeatures.clear();
    WaitNextReaderEventInFlight = false;
    LastReadEventTime = TInstant::Zero();

    Become(&TThis::StateInitConsumer);

    LOG_NOTICE_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " schedule consumer creation");
    ScheduleWithIncreasingTimeout<TEvPQ::TEvCreateConsumer>(SelfId(), ConsumerInitInterval, CONSUMER_INIT_INTERVAL_MAX, ctx);
}

TString TMirrorer::MirrorerDescription() const {
    return TStringBuilder() << "[mirrorer for " << TopicConverter->GetPrintableString() << ", partition " << Partition << ']';
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
            actorSystem = ctx.ExecutorThread.ActorSystem,
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
    TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription() << " got next reader event: " << bool(event));

    if (wakeup && !event) {
        return;
    }

    WaitNextReaderEventInFlight = false;
    if (!event) {
        StartWaitNextReaderEvent(ctx);
        return;
    }
    LastReadEventTime = ctx.Now();

    if (auto* dataEvent = std::get_if<TPersQueueReadEvent::TDataReceivedEvent>(&event.GetRef())) {
        AddMessagesToQueue(std::move(dataEvent->GetCompressedMessages()));
    } else if (auto* createStream = std::get_if<TPersQueueReadEvent::TStartPartitionSessionEvent>(&event.GetRef())) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_MIRRORER,
            MirrorerDescription() << " got create stream event for '" << createStream->DebugString()
                << " and will set offset=" << OffsetToRead);
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

        createStream->Confirm(OffsetToRead, createStream->GetCommittedOffset());
        RequestSourcePartitionStatus();
    } else if (auto* destroyStream = std::get_if<TPersQueueReadEvent::TStopPartitionSessionEvent>(&event.GetRef())) {
        destroyStream->Confirm();

        PartitionStream.Reset();
        LOG_INFO_S(ctx, NKikimrServices::PQ_MIRRORER,
            MirrorerDescription()
                << " got destroy stream event: " << destroyStream->DebugString());
   } else if (auto* streamClosed = std::get_if<TPersQueueReadEvent::TPartitionSessionClosedEvent>(&event.GetRef())) {
        PartitionStream.Reset();
        LOG_INFO_S(ctx, NKikimrServices::PQ_MIRRORER,
            MirrorerDescription()
                << " got stream closed event for partition stream id: "
                << streamClosed->GetPartitionSession()->GetPartitionSessionId()
                << " reason: " << streamClosed->GetReason());

        ProcessError(ctx, TStringBuilder() << " read session stream closed event");
        ScheduleConsumerCreation(ctx);
        return;

    } else if (auto* streamStatus = std::get_if<TPersQueueReadEvent::TPartitionSessionStatusEvent >(&event.GetRef())) {
        if (PartitionStream
            && PartitionStream->GetPartitionSessionId() == streamStatus->GetPartitionSession()->GetPartitionSessionId()
        ) {
            StreamStatus = MakeHolder<TPersQueueReadEvent::TPartitionSessionStatusEvent>(*streamStatus);

            ctx.Schedule(TDuration::Seconds(1), new TEvPQ::TEvRequestPartitionStatus);
            TryUpdateWriteTimetsamp(ctx);
        }
    } else if (auto* commitAck = std::get_if<TPersQueueReadEvent::TCommitOffsetAcknowledgementEvent>(&event.GetRef())) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_MIRRORER, MirrorerDescription()
            << " got commit responce, commited offset: " << commitAck->GetCommittedOffset());
    } else if (auto* closeSessionEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event.GetRef())) {
        ProcessError(ctx, TStringBuilder() << " read session closed: " << closeSessionEvent->DebugString());
        ScheduleConsumerCreation(ctx);
        return;
    } else {
        ProcessError(ctx, TStringBuilder() << " got unmatched event: " << event.GetRef().index());
        ScheduleConsumerCreation(ctx);
        return;
    }

    Send(SelfId(), new TEvents::TEvWakeup());
    ConsumerInitInterval = CONSUMER_INIT_INTERVAL_START;
}

}// NPQ
}// NKikimr
