#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pqtablet/partition/mirrorer/mirrorer.h>
#include <ydb/core/persqueue/pqtablet/partition/mirrorer/mirrorer_factory.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;
using namespace NYdb::NTopic;

namespace {

using TTopicEvent = TReadSessionEvent::TEvent;

class TMockPartitionSession : public TPartitionSessionControl {
public:
    TMockPartitionSession(ui64 sessionId, ui32 partitionId) {
        PartitionSessionId = sessionId;
        TopicPath = "src-topic";
        ReadSessionId = "mock-read-session";
        PartitionId = partitionId;
    }

    void RequestStatus() override {
        ++RequestStatusCount;
    }

    void Commit(uint64_t, uint64_t) override {
        ++CommitCount;
    }

    void ConfirmCreate(std::optional<uint64_t>, std::optional<uint64_t>, std::optional<uint64_t>) override {
        Confirmed = true;
    }

    void ConfirmDestroy() override {
        DestroyConfirmed = true;
    }

    void ConfirmEnd(std::span<const uint32_t>) override {
    }

    ui32 RequestStatusCount = 0;
    ui32 CommitCount = 0;
    bool Confirmed = false;
    bool DestroyConfirmed = false;
};

class TMockReadSession : public IReadSession {
public:
    explicit TMockReadSession(ui64 sessionId, ui32 partitionId)
        : Partition(MakeIntrusive<TMockPartitionSession>(sessionId, partitionId))
        , SessionId("mock-session-" + ToString(sessionId))
        , Counters(MakeIntrusive<TReaderCounters>())
    {
        MakeCountersNotNull(*Counters);
    }

    void PushEvent(TTopicEvent event) {
        Events.push_back(std::move(event));
        if (WaitPromise) {
            auto promise = std::move(*WaitPromise);
            WaitPromise.Clear();
            promise.SetValue();
        }
    }

    NThreading::TFuture<void> WaitEvent() override {
        ++WaitEventCount;
        if (!Events.empty()) {
            return NThreading::MakeFuture();
        }
        WaitPromise = NThreading::NewPromise<void>();
        return WaitPromise->GetFuture();
    }

    std::vector<TTopicEvent> GetEvents(bool block, std::optional<size_t>, size_t maxByteSize) override {
        std::vector<TTopicEvent> result;
        if (auto event = GetEvent(block, maxByteSize)) {
            result.push_back(std::move(*event));
        }
        return result;
    }

    std::vector<TTopicEvent> GetEvents(const TReadSessionGetEventSettings&) override {
        return GetEvents(false, std::nullopt, std::numeric_limits<size_t>::max());
    }

    std::optional<TTopicEvent> GetEvent(bool, size_t) override {
        if (Events.empty()) {
            return std::nullopt;
        }
        auto event = std::move(Events.front());
        Events.pop_front();
        return event;
    }

    std::optional<TTopicEvent> GetEvent(const TReadSessionGetEventSettings&) override {
        return GetEvent(false, std::numeric_limits<size_t>::max());
    }

    bool Close(TDuration) override {
        Closed = true;
        return true;
    }

    TReaderCounters::TPtr GetCounters() const override {
        return Counters;
    }

    std::string GetSessionId() const override {
        return SessionId;
    }

    TIntrusivePtr<TMockPartitionSession> Partition;
    bool Closed = false;
    ui32 WaitEventCount = 0;

private:
    const std::string SessionId;
    TReaderCounters::TPtr Counters;
    std::deque<TTopicEvent> Events;
    TMaybe<NThreading::TPromise<void>> WaitPromise;
};

class TMockMirrorFactory : public IPersQueueMirrorReaderFactory {
public:
    mutable std::vector<std::shared_ptr<TMockReadSession>> Sessions;
    mutable std::vector<NThreading::TPromise<NYdb::TStatus>> CommitPromises;
    mutable ui64 LastCommitOffset = 0;
    mutable ui32 NextSessionId = 1;
    mutable bool ThrowOnCreateSession = false;
    mutable bool ThrowOnCredentials = false;
    mutable bool PendingCredentials = false;
    mutable bool ThrowOnCommit = false;
    mutable NThreading::TPromise<NYdb::TCredentialsProviderFactoryPtr> CredentialsPromise;

    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials&
    ) const override {
        if (ThrowOnCredentials) {
            ythrow yexception() << "credentials failed";
        }
        if (PendingCredentials) {
            CredentialsPromise = NThreading::NewPromise<NYdb::TCredentialsProviderFactoryPtr>();
            return CredentialsPromise.GetFuture();
        }
        return NThreading::MakeFuture(NYdb::CreateInsecureCredentialsProviderFactory());
    }

    std::shared_ptr<IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig&,
        ui32 partition,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui64,
        TMaybe<TLog>
    ) const override {
        if (ThrowOnCreateSession) {
            ythrow yexception() << "cannot create session";
        }
        auto session = std::make_shared<TMockReadSession>(NextSessionId++, partition);
        Sessions.push_back(session);
        return session;
    }

    NThreading::TFuture<NYdb::NTopic::TDescribeTopicResult> GetTopicDescription(
        const NKikimrPQ::TMirrorPartitionConfig&,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>
    ) const override {
        return NThreading::NewPromise<NYdb::NTopic::TDescribeTopicResult>().GetFuture();
    }

    NThreading::TFuture<NYdb::TStatus> CommitOffset(
        const NKikimrPQ::TMirrorPartitionConfig&,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui32,
        ui64 offset
    ) const override {
        LastCommitOffset = offset;
        if (ThrowOnCommit) {
            auto promise = NThreading::NewPromise<NYdb::TStatus>();
            promise.SetException(std::make_exception_ptr(yexception() << "commit exploded"));
            return promise.GetFuture();
        }
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        CommitPromises.push_back(promise);
        return promise.GetFuture();
    }

    TMockReadSession& Session(size_t index) const {
        UNIT_ASSERT(index < Sessions.size());
        return *Sessions[index];
    }
};

NKafka::TKafkaRecord MakeKafkaRecord(i64 timestampDelta, i64 offsetDelta, TStringBuf key, TStringBuf value) {
    NKafka::TKafkaRecord record;
    record.TimestampDelta = timestampDelta;
    record.OffsetDelta = offsetDelta;
    record.SetKey(TString{key});
    record.SetValue(TString{value});
    record.Length = record.Size(2)
        - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
    return record;
}

TString MakeKafkaBatchPayload(
    ui32 records,
    i64 producerId = 42,
    std::optional<i64> lastOffsetDelta = std::nullopt,
    i64 baseSequence = 10
) {
    NKafka::TKafkaRecordBatch batch;
    batch.BaseOffset = 100;
    batch.Magic = 2;
    batch.LastOffsetDelta = lastOffsetDelta.value_or(records ? records - 1 : 0);
    batch.BaseTimestamp = 1000;
    batch.MaxTimestamp = 1000 + records;
    batch.ProducerId = producerId;
    batch.ProducerEpoch = 3;
    batch.BaseSequence = baseSequence;
    for (ui32 i = 0; i < records; ++i) {
        batch.Records.push_back(MakeKafkaRecord(i, i, "k", "v"));
    }
    batch.BatchLength = batch.Size(2)
        - sizeof(NKafka::TKafkaRecordBatch::BaseOffsetMeta::Type)
        - sizeof(NKafka::TKafkaRecordBatch::BatchLengthMeta::Type);
    return NKafka::WriteKafkaRecordBatch(batch);
}

TIntrusivePtr<TMockPartitionSession> MakePartition(ui32 partitionId = 0) {
    return MakeIntrusive<TMockPartitionSession>(ui64(1), partitionId);
}

TReadSessionEvent::TDataReceivedEvent::TCompressedMessage MakeCompressed(
    const TIntrusivePtr<TMockPartitionSession>& partition,
    ui64 offset,
    TString data = "payload",
    ECodec codec = ECodec::RAW
) {
    auto meta = MakeIntrusive<TWriteSessionMeta>();
    auto messageMeta = MakeIntrusive<TMessageMeta>();
    TReadSessionEvent::TDataReceivedEvent::TMessageInformation info(
        offset,
        "producer",
        /*seqNo=*/ 1,
        TInstant::MilliSeconds(1000),
        TInstant::MilliSeconds(2000),
        meta,
        messageMeta,
        data.size(),
        "producer"
    );
    return TReadSessionEvent::TDataReceivedEvent::TCompressedMessage(
        codec,
        data,
        std::move(info),
        partition
    );
}

TReadSessionEvent::TDataReceivedEvent MakeDataEvent(
    const TIntrusivePtr<TMockPartitionSession>& partition,
    ui64 offset,
    TString data = "payload",
    ECodec codec = ECodec::RAW
) {
    std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> compressedMessages;
    compressedMessages.push_back(MakeCompressed(partition, offset, std::move(data), codec));
    return TReadSessionEvent::TDataReceivedEvent({}, std::move(compressedMessages), partition);
}

THolder<TEvPersQueue::TEvResponse> MakeWriteResponse(
    ui64 offset,
    ui64 cookie,
    bool alreadyWritten = false,
    NMsgBusProxy::EResponseStatus status = NMsgBusProxy::MSTATUS_OK,
    NPersQueue::NErrorCode::EErrorCode errorCode = NPersQueue::NErrorCode::OK
) {
    auto response = MakeHolder<TEvPersQueue::TEvResponse>();
    response->Record.SetStatus(status);
    response->Record.SetErrorCode(errorCode);
    response->Record.SetErrorReason("err");
    auto* part = response->Record.MutablePartitionResponse();
    part->SetCookie(cookie);
    auto* result = part->AddCmdWriteResult();
    result->SetOffset(offset);
    result->SetAlreadyWritten(alreadyWritten);
    result->SetWriteTimestampMS(2500);
    return response;
}

NPersQueue::TTopicConverterPtr MakeTopicConverter() {
    NKikimrPQ::TPQTabletConfig config;
    config.SetTopicName("dst-topic");
    config.SetTopicPath("/Root/dst-topic");
    return NPersQueue::TTopicNameConverter::ForFirstClass(config);
}

NKikimrPQ::TMirrorPartitionConfig MakeMirrorConfig() {
    NKikimrPQ::TMirrorPartitionConfig config;
    config.SetEndpoint("localhost");
    config.SetEndpointPort(2135);
    config.SetTopic("src-topic");
    config.SetConsumer("mirror-consumer");
    return config;
}

using TPersQueueCounters = TAppProtobufTabletCounters<
    NPQ::ESimpleCounters_descriptor,
    NPQ::ECumulativeCounters_descriptor,
    NPQ::EPercentileCounters_descriptor
>;

struct TEnvOptions {
    ui64 EndOffset = 0;
    bool LocalDC = false;
    TMaybe<ui32> RewindCommitDelaySeconds;
    bool EnableSplitMerge = true;
    bool PendingCredentials = false;
    bool ThrowOnCreateSession = false;
    bool ThrowOnCredentials = false;
    bool ThrowOnCommit = false;
};

struct TMirrorerEnv {
    TTestBasicRuntime Runtime;
    TActorId Tablet;
    TActorId Partition;
    TActorId Mirrorer;
    TMockMirrorFactory Factory;
    TPersQueueCounters Counters;
    std::vector<THolder<TEvPersQueue::TEvRequest>> CapturedRequests;
    std::vector<THolder<TEvPQ::TEvPartitionScaleStatusChanged>> CapturedScale;
    ui32 ErrorCount = 0;
    ui32 CounterEventCount = 0;
    bool TabletPoisoned = false;

    static constexpr ui64 TabletId = 42;
    static constexpr ui32 PartitionId = 0;

    explicit TMirrorerEnv(const TEnvOptions& options = {})
        : Runtime(1, false)
    {
        TAppPrepare prepare;
        if (options.RewindCommitDelaySeconds.Defined()) {
            prepare.PQConfig.MutableMirrorConfig()->SetRewindCommitDelaySeconds(*options.RewindCommitDelaySeconds);
        }
        Runtime.Initialize(prepare.Unwrap());
        Runtime.SetScheduledLimit(100000);
        Runtime.SetLogPriority(NKikimrServices::PQ_MIRRORER, NActors::NLog::PRI_NOTICE);
        Runtime.AdvanceCurrentTime(TDuration::Minutes(5));

        auto& app = Runtime.GetAppData(0);
        Factory.PendingCredentials = options.PendingCredentials;
        Factory.ThrowOnCreateSession = options.ThrowOnCreateSession;
        Factory.ThrowOnCredentials = options.ThrowOnCredentials;
        Factory.ThrowOnCommit = options.ThrowOnCommit;
        app.PersQueueMirrorReaderFactory = &Factory;
        if (options.RewindCommitDelaySeconds.Defined()) {
            app.PQConfig.MutableMirrorConfig()->SetRewindCommitDelaySeconds(*options.RewindCommitDelaySeconds);
        }
        app.FeatureFlags.SetEnableMirroredTopicSplitMerge(options.EnableSplitMerge);
        if (!app.Counters) {
            app.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        }

        Tablet = Runtime.AllocateEdgeActor();
        Partition = Runtime.AllocateEdgeActor();

        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == Tablet && ev->GetTypeRewrite() == TEvPersQueue::TEvRequest::EventType) {
                CapturedRequests.emplace_back(ev->Release<TEvPersQueue::TEvRequest>());
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (ev->Recipient == Tablet && ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType) {
                TabletPoisoned = true;
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            if (ev->Recipient == Partition && ev->GetTypeRewrite() == TEvPersQueue::TEvReportPartitionError::EventType) {
                ++ErrorCount;
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (ev->Recipient == Partition && ev->GetTypeRewrite() == TEvPQ::TEvMirrorerCounters::EventType) {
                ++CounterEventCount;
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (ev->Recipient == Partition && ev->GetTypeRewrite() == TEvPQ::TEvPartitionScaleStatusChanged::EventType) {
                CapturedScale.emplace_back(ev->Release<TEvPQ::TEvPartitionScaleStatusChanged>());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto converter = MakeTopicConverter();
        Mirrorer = Runtime.Register(CreateMirrorer(
            TabletId,
            Tablet,
            Partition,
            converter,
            PartitionId,
            options.LocalDC,
            options.EndOffset,
            MakeMirrorConfig(),
            Counters
        ));
        Runtime.EnableScheduleForActor(Mirrorer);
        Dispatch();
    }

    void Dispatch(TDuration timeout = TDuration::MilliSeconds(20)) {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }

    void Advance(TDuration delay) {
        Runtime.AdvanceCurrentTime(delay);
        Dispatch();
    }

    void WaitSessions(size_t count) {
        for (int i = 0; i < 80 && Factory.Sessions.size() < count; ++i) {
            Dispatch();
        }
        UNIT_ASSERT_VALUES_EQUAL(Factory.Sessions.size(), count);
    }

    void StartSession(size_t index, ui64 committedOffset, ui64 endOffset, ui32 partitionId = PartitionId) {
        WaitSessions(index + 1);
        auto& session = Factory.Session(index);
        if (partitionId != PartitionId) {
            session.Partition = MakeIntrusive<TMockPartitionSession>(ui64(index + 1), partitionId);
        }
        session.PushEvent(TReadSessionEvent::TStartPartitionSessionEvent(
            session.Partition,
            committedOffset,
            endOffset
        ));
        Dispatch();
    }

    TEvPersQueue::TEvRequest* FindRequest(ui64 cookie) {
        for (auto& req : CapturedRequests) {
            if (req->Record.GetPartitionRequest().GetCookie() == cookie) {
                return req.Get();
            }
        }
        return nullptr;
    }

    ui32 CountRequests(ui64 cookie) const {
        ui32 count = 0;
        for (const auto& req : CapturedRequests) {
            if (req->Record.GetPartitionRequest().GetCookie() == cookie) {
                ++count;
            }
        }
        return count;
    }

    void AckWrite(ui64 offset, bool alreadyWritten = false) {
        auto* req = FindRequest(1);
        UNIT_ASSERT(req);
        const ui64 cookie = req->Record.GetPartitionRequest().GetCookie();
        Runtime.Send(new IEventHandle(
            Mirrorer,
            Tablet,
            MakeWriteResponse(offset, cookie, alreadyWritten).Release()
        ), 0, true);
        Dispatch();
    }

    void SendChangeConfig(const NKikimrPQ::TMirrorPartitionConfig& mirrorFrom) {
        NKikimrPQ::TPQTabletConfig config;
        *config.MutablePartitionConfig()->MutableMirrorFrom() = mirrorFrom;
        Runtime.Send(new IEventHandle(
            Mirrorer,
            Partition,
            new TEvPQ::TEvChangePartitionConfig(MakeTopicConverter(), config)
        ), 0, true);
        Dispatch();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAppendToWriteRequest) {

Y_UNIT_TEST(FillsCmdWriteAndHandlesGap) {
    auto partition = MakePartition();
    NKikimrClient::TPersQueuePartitionRequest request;
    bool incorrect = false;
    ui64 nextOffset = 0;

    auto first = MakeCompressed(partition, 10);
    UNIT_ASSERT(AppendToWriteRequest(request, first, incorrect, nextOffset));
    UNIT_ASSERT(!incorrect);
    UNIT_ASSERT_VALUES_EQUAL(request.GetCmdWriteOffset(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(nextOffset, 11u);
    UNIT_ASSERT_VALUES_EQUAL(request.CmdWriteSize(), 1);

    auto gap = MakeCompressed(partition, 20);
    UNIT_ASSERT(!AppendToWriteRequest(request, gap, incorrect, nextOffset));
    UNIT_ASSERT(!incorrect);
    UNIT_ASSERT_VALUES_EQUAL(request.CmdWriteSize(), 1);
}

Y_UNIT_TEST(IncorrectWhenCmdWriteExistsWithoutOffset) {
    auto partition = MakePartition();
    NKikimrClient::TPersQueuePartitionRequest request;
    request.AddCmdWrite();
    bool incorrect = false;
    ui64 nextOffset = 0;
    auto msg = MakeCompressed(partition, 0);
    UNIT_ASSERT(!AppendToWriteRequest(request, msg, incorrect, nextOffset));
    UNIT_ASSERT(incorrect);
}

Y_UNIT_TEST(DecodesProducerIdAndKafkaBatch) {
    auto partition = MakePartition();
    NKikimrClient::TPersQueuePartitionRequest request;
    bool incorrect = false;
    ui64 nextOffset = 0;

    auto meta = MakeIntrusive<TWriteSessionMeta>();
    meta->Fields["_encoded_producer_id"] = Base64Encode("raw-source");
    auto messageMeta = MakeIntrusive<TMessageMeta>();
    TReadSessionEvent::TDataReceivedEvent::TMessageInformation info(
        5, "producer", 7, TInstant::MilliSeconds(1), TInstant::MilliSeconds(2),
        meta, messageMeta, 3, "producer"
    );
    auto msg = TReadSessionEvent::TDataReceivedEvent::TCompressedMessage(
        ECodec::RAW, "abc", std::move(info), partition
    );
    UNIT_ASSERT(AppendToWriteRequest(request, msg, incorrect, nextOffset));
    UNIT_ASSERT_VALUES_EQUAL(request.GetCmdWrite(0).GetSeqNo(), 7u);

    NKikimrClient::TPersQueuePartitionRequest kafkaRequest;
    nextOffset = 0;
    incorrect = false;
    auto kafka = MakeCompressed(partition, 0, MakeKafkaBatchPayload(2), ECodec::KAFKA_BATCH);
    UNIT_ASSERT(AppendToWriteRequest(kafkaRequest, kafka, incorrect, nextOffset));
    UNIT_ASSERT_VALUES_EQUAL(nextOffset, 2u);
    UNIT_ASSERT(kafkaRequest.GetCmdWrite(0).HasLogicalMessageCount());
    UNIT_ASSERT_VALUES_EQUAL(kafkaRequest.GetCmdWrite(0).GetLogicalMessageCount(), 2u);

    NKikimrClient::TPersQueuePartitionRequest singleRequest;
    nextOffset = 0;
    auto single = MakeCompressed(partition, 0, MakeKafkaBatchPayload(1), ECodec::KAFKA_BATCH);
    UNIT_ASSERT(AppendToWriteRequest(singleRequest, single, incorrect, nextOffset));
    UNIT_ASSERT_VALUES_EQUAL(nextOffset, 1u);
    UNIT_ASSERT(!singleRequest.GetCmdWrite(0).HasLogicalMessageCount());

    NKikimrClient::TPersQueuePartitionRequest garbageRequest;
    nextOffset = 0;
    auto garbage = MakeCompressed(partition, 0, "not-a-batch", ECodec::KAFKA_BATCH);
    UNIT_ASSERT(AppendToWriteRequest(garbageRequest, garbage, incorrect, nextOffset));
    UNIT_ASSERT_VALUES_EQUAL(nextOffset, 1u);

    NKikimrClient::TPersQueuePartitionRequest invalidSeq;
    nextOffset = 0;
    auto invalid = MakeCompressed(partition, 0, MakeKafkaBatchPayload(2, /*producerId=*/ -1, /*lastOffsetDelta=*/ i64{-1}), ECodec::KAFKA_BATCH);
    UNIT_ASSERT(AppendToWriteRequest(invalidSeq, invalid, incorrect, nextOffset));
    UNIT_ASSERT_VALUES_EQUAL(nextOffset, 1u);
}

} // Y_UNIT_TEST_SUITE(TAppendToWriteRequest)

Y_UNIT_TEST_SUITE(TMirrorerActor) {

Y_UNIT_TEST(WritesAndAcksAlreadyWritten) {
    TMirrorerEnv env;

    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    UNIT_ASSERT(env.Factory.Session(0).Partition->Confirmed);

    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 0));
    env.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(env.CountRequests(1), 1u);

    env.AckWrite(0, /*alreadyWritten=*/ true);
    UNIT_ASSERT(env.ErrorCount > 0);

    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 1));
    env.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(env.CountRequests(1), 2u);
    env.AckWrite(1);
}

Y_UNIT_TEST(RetriesFailedWriteAndUnexpectedResponses) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 0));
    env.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(env.CountRequests(1), 1u);

    env.Runtime.Send(new IEventHandle(
        env.Mirrorer, env.Tablet,
        MakeWriteResponse(0, 1, false, NMsgBusProxy::MSTATUS_ERROR).Release()
    ), 0, true);
    env.Advance(TDuration::MilliSeconds(5));
    UNIT_ASSERT(env.CountRequests(1) >= 2);

    auto unexpected = MakeHolder<TEvPersQueue::TEvResponse>();
    unexpected->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    unexpected->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    unexpected->Record.MutablePartitionResponse()->SetCookie(99);
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Tablet, unexpected.Release()), 0, true);
    env.Dispatch();

    auto noPart = MakeHolder<TEvPersQueue::TEvResponse>();
    noPart->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    noPart->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Tablet, noPart.Release()), 0, true);
    env.Dispatch();

    auto pqError = MakeHolder<TEvPersQueue::TEvResponse>();
    pqError->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    pqError->Record.SetErrorCode(NPersQueue::NErrorCode::BAD_REQUEST);
    pqError->Record.SetErrorReason("bad");
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Tablet, pqError.Release()), 0, true);
    env.Dispatch();
    UNIT_ASSERT(env.ErrorCount > 0);

    auto ts = MakeHolder<TEvPersQueue::TEvResponse>();
    ts->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    ts->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    ts->Record.MutablePartitionResponse()->SetCookie(2);
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Tablet, ts.Release()), 0, true);
    env.Dispatch();
}

Y_UNIT_TEST(SessionEventsAndConfigChange) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 5, 10); // committed > EndOffset=0 → gap
    UNIT_ASSERT(env.ErrorCount > 0);
    UNIT_ASSERT(env.Factory.Session(0).Partition->Confirmed);

    env.Factory.Session(0).PushEvent(TReadSessionEvent::TStopPartitionSessionEvent(
        env.Factory.Session(0).Partition, 5
    ));
    env.Dispatch();
    UNIT_ASSERT(env.Factory.Session(0).Partition->DestroyConfirmed);

    env.Runtime.Send(new IEventHandle(
        env.Mirrorer, env.Partition, new TEvPQ::TEvRequestPartitionStatus()
    ), 0, true);
    env.Dispatch();

    env.Factory.Session(0).PushEvent(TReadSessionEvent::TCommitOffsetAcknowledgementEvent(
        env.Factory.Session(0).Partition, 5
    ));
    env.Dispatch();

    env.SendChangeConfig(MakeMirrorConfig());
    auto other = MakeMirrorConfig();
    other.SetEndpoint("other-host");
    env.SendChangeConfig(other);

    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Partition, new TEvents::TEvPoisonPill()), 0, true);
    env.Dispatch();
}

Y_UNIT_TEST(WrongPartitionAndDuplicateStreamRecreateSession) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10, /*partitionId=*/ 7);
    env.Advance(TDuration::Seconds(1));
    env.WaitSessions(2);

    env.StartSession(1, 0, 10);
    env.Factory.Session(1).PushEvent(TReadSessionEvent::TStartPartitionSessionEvent(
        env.Factory.Session(1).Partition, 0, 10
    ));
    env.Dispatch();
    env.Advance(TDuration::Seconds(1));
    env.WaitSessions(3);
}

Y_UNIT_TEST(ClosedAndLostSessionRecreateConsumer) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);

    env.Factory.Session(0).PushEvent(NYdb::NTopic::TSessionClosedEvent(
        NYdb::EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()
    ));
    env.Dispatch();
    env.Advance(TDuration::Seconds(1));
    env.WaitSessions(2);

    env.StartSession(1, 0, 10);
    env.Factory.Session(1).PushEvent(TReadSessionEvent::TPartitionSessionClosedEvent(
        env.Factory.Session(1).Partition,
        TReadSessionEvent::TPartitionSessionClosedEvent::EReason::Lost
    ));
    env.Dispatch();
    env.Advance(TDuration::Seconds(1));
    env.WaitSessions(3);
}

Y_UNIT_TEST(StatusWrongSessionIgnored) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    auto other = MakeIntrusive<TMockPartitionSession>(ui64(99), ui32(0));
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        other, 0, 0, 10, TInstant::MilliSeconds(1)
    ));
    env.Dispatch();
    UNIT_ASSERT(env.Factory.CommitPromises.empty());
}

Y_UNIT_TEST(SplitMergeChildrenSendsNeedSplit) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        env.Factory.Session(0).Partition, {}, {1, 2}
    ));
    env.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(env.CapturedScale.size(), 1u);
    UNIT_ASSERT(env.CapturedScale[0]->Record.GetScaleStatus() == NKikimrPQ::EScaleStatus::NEED_SPLIT);
    UNIT_ASSERT_VALUES_EQUAL(env.CapturedScale[0]->Record.GetParticipatingPartitions().ChildPartitionIdsSize(), 2u);

    env.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        env.Factory.Session(0).Partition, {}, {3}
    ));
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        env.Factory.Session(0).Partition, {}, {4}
    ));
    env.Dispatch();
}

Y_UNIT_TEST(SplitMergeDisabledAndUnsupportedShapes) {
    TMirrorerEnv disabled({.EnableSplitMerge = false});
    disabled.Advance(TDuration::Seconds(1));
    disabled.StartSession(0, 0, 10);
    disabled.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        disabled.Factory.Session(0).Partition, {}, {1}
    ));
    disabled.Dispatch();
    UNIT_ASSERT(disabled.CapturedScale.empty());

    TMirrorerEnv merge;
    merge.Advance(TDuration::Seconds(1));
    merge.StartSession(0, 0, 10);
    merge.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        merge.Factory.Session(0).Partition, {9}, {1}
    ));
    merge.Dispatch();
    UNIT_ASSERT(merge.CapturedScale.empty());

    TMirrorerEnv emptyChildren;
    emptyChildren.Advance(TDuration::Seconds(1));
    emptyChildren.StartSession(0, 0, 10);
    emptyChildren.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        emptyChildren.Factory.Session(0).Partition, {}, {}
    ));
    emptyChildren.Dispatch();
    UNIT_ASSERT(emptyChildren.CapturedScale.empty());
}

Y_UNIT_TEST(SplitMergePostponedWhileWriteInFlight) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 0));
    env.Dispatch();
    UNIT_ASSERT(env.FindRequest(1));

    env.Factory.Session(0).PushEvent(TReadSessionEvent::TEndPartitionSessionEvent(
        env.Factory.Session(0).Partition, {}, {1}
    ));
    env.Dispatch();
    UNIT_ASSERT(env.CapturedScale.empty());

    env.AckWrite(0);
    env.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(env.CapturedScale.size(), 1u);
}

Y_UNIT_TEST(RewindOnCommitLagAndFailedResult) {
    TMirrorerEnv env({.RewindCommitDelaySeconds = 0});
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 1000);
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        env.Factory.Session(0).Partition, 0, 1000, 1000, TInstant::MilliSeconds(2000)
    ));
    env.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.CommitPromises.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.LastCommitOffset, 1000u);

    env.Factory.CommitPromises[0].SetValue(NYdb::TStatus(NYdb::EStatus::TIMEOUT, NYdb::NIssue::TIssues()));
    env.Dispatch();
    UNIT_ASSERT(env.ErrorCount > 0);

    env.Advance(TDuration::Minutes(5));
    env.WaitSessions(2);
    env.StartSession(1, 0, 1000);
    env.Factory.Session(1).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        env.Factory.Session(1).Partition, 0, 1000, 1000, TInstant::MilliSeconds(2000)
    ));
    env.Dispatch();
    UNIT_ASSERT(env.Factory.CommitPromises.size() >= 2);
    env.Factory.CommitPromises.back().SetValue(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
    env.Dispatch();
}

Y_UNIT_TEST(RewindCommitExceptionAndIntervalSkip) {
    TMirrorerEnv env({.RewindCommitDelaySeconds = 0, .ThrowOnCommit = true});
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 100);
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        env.Factory.Session(0).Partition, 0, 100, 100, TInstant::MilliSeconds(1)
    ));
    env.Dispatch();
    UNIT_ASSERT(env.ErrorCount > 0);

    TMirrorerEnv skip({.RewindCommitDelaySeconds = 0});
    skip.Advance(TDuration::Seconds(1));
    skip.StartSession(0, 0, 50);
    skip.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        skip.Factory.Session(0).Partition, 0, 50, 50, TInstant::MilliSeconds(1)
    ));
    skip.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(skip.Factory.CommitPromises.size(), 1u);
    skip.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        skip.Factory.Session(0).Partition, 0, 50, 50, TInstant::MilliSeconds(1)
    ));
    skip.Dispatch();
    UNIT_ASSERT_VALUES_EQUAL(skip.Factory.CommitPromises.size(), 1u);
}

Y_UNIT_TEST(StaleStatusesDoNotRewind) {
    TMirrorerEnv delayed; // default 15min rewind delay
    delayed.Advance(TDuration::Seconds(1));
    delayed.StartSession(0, 0, 10);
    delayed.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        delayed.Factory.Session(0).Partition, 0, 10, 10, TInstant::MilliSeconds(1)
    ));
    delayed.Dispatch();
    UNIT_ASSERT(delayed.Factory.CommitPromises.empty());

    TMirrorerEnv unread({.EndOffset = 0, .RewindCommitDelaySeconds = 0});
    unread.Advance(TDuration::Seconds(1));
    unread.StartSession(0, 0, 10);
    unread.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        unread.Factory.Session(0).Partition, 0, 3, 10, TInstant::MilliSeconds(1)
    ));
    unread.Dispatch();
    UNIT_ASSERT(unread.Factory.CommitPromises.empty());

    TMirrorerEnv afterData({.RewindCommitDelaySeconds = 0});
    afterData.Advance(TDuration::Seconds(1));
    afterData.StartSession(0, 0, 10);
    afterData.Factory.Session(0).PushEvent(MakeDataEvent(afterData.Factory.Session(0).Partition, 0));
    afterData.Dispatch();
    afterData.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        afterData.Factory.Session(0).Partition, 0, 10, 10, TInstant::MilliSeconds(1)
    ));
    afterData.Dispatch();
    UNIT_ASSERT(afterData.Factory.CommitPromises.empty());
}

Y_UNIT_TEST(AllCommittedUpdatesWriteTimestamp) {
    TMirrorerEnv env({.EndOffset = 10, .RewindCommitDelaySeconds = 0});
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 10, 10);
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        env.Factory.Session(0).Partition, 10, 10, 10, TInstant::MilliSeconds(3333)
    ));
    env.Dispatch();
    UNIT_ASSERT(env.FindRequest(2));
}

Y_UNIT_TEST(KafkaBatchWriteUsesLogicalMessageCount) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Factory.Session(0).PushEvent(MakeDataEvent(
        env.Factory.Session(0).Partition, 0, MakeKafkaBatchPayload(3), ECodec::KAFKA_BATCH
    ));
    env.Dispatch();
    auto* req = env.FindRequest(1);
    UNIT_ASSERT(req);
    UNIT_ASSERT_VALUES_EQUAL(req->Record.GetPartitionRequest().GetCmdWrite(0).GetLogicalMessageCount(), 3u);
    env.AckWrite(0);
}

Y_UNIT_TEST(QueueClearedWhenConsumerRecreated) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 20);
    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 0));
    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 5));
    env.Dispatch();
    UNIT_ASSERT(env.FindRequest(1));

    env.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionClosedEvent(
        env.Factory.Session(0).Partition,
        TReadSessionEvent::TPartitionSessionClosedEvent::EReason::Lost
    ));
    env.Dispatch();
    env.Advance(TDuration::Seconds(1));
    env.WaitSessions(2);
}

Y_UNIT_TEST(CreateSessionExceptionAndCredentialsError) {
    TMirrorerEnv env({.ThrowOnCreateSession = true});
    env.Advance(TDuration::Seconds(2));
    UNIT_ASSERT(env.Factory.Sessions.empty());
    UNIT_ASSERT(env.ErrorCount > 0);

    TMirrorerEnv creds({.ThrowOnCredentials = true});
    creds.Advance(TDuration::Seconds(1));
    UNIT_ASSERT(creds.ErrorCount > 0);
}

Y_UNIT_TEST(PendingCredentialsBlocksCreateConsumer) {
    TMirrorerEnv env({.PendingCredentials = true});
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Mirrorer, new TEvPQ::TEvInitCredentials()), 0, true);
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Mirrorer, new TEvPQ::TEvCreateConsumer()), 0, true);
    env.Dispatch();
    UNIT_ASSERT(env.Factory.Sessions.empty());
}

Y_UNIT_TEST(InitTimeoutRestartsWithoutSession) {
    TMirrorerEnv env({.PendingCredentials = true});
    env.Advance(TDuration::Minutes(3));
    UNIT_ASSERT(env.ErrorCount > 0);
}

Y_UNIT_TEST(ReadEventTimeoutRecreatesSession) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Advance(TDuration::Minutes(2));
    env.WaitSessions(2);
}

Y_UNIT_TEST(WriteTimeoutPoisonsTablet) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Factory.Session(0).PushEvent(MakeDataEvent(env.Factory.Session(0).Partition, 0));
    env.Dispatch();
    UNIT_ASSERT(env.FindRequest(1));

    for (int i = 0; i < 12 && !env.TabletPoisoned; ++i) {
        env.Factory.Session(0).PushEvent(TReadSessionEvent::TCommitOffsetAcknowledgementEvent(
            env.Factory.Session(0).Partition, 0
        ));
        env.Advance(TDuration::Minutes(1));
    }
    UNIT_ASSERT(env.TabletPoisoned);
}

Y_UNIT_TEST(StateLogAndCountersAndLocalDC) {
    TMirrorerEnv env({.LocalDC = true});
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Factory.Session(0).PushEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
        env.Factory.Session(0).Partition, 0, 1, 10, TInstant::MilliSeconds(1)
    ));
    env.Dispatch();
    env.Advance(TDuration::Minutes(2));
    UNIT_ASSERT(env.CounterEventCount > 0);
}

Y_UNIT_TEST(DoProcessNextReaderEventWakeupWithoutData) {
    TMirrorerEnv env;
    env.Advance(TDuration::Seconds(1));
    env.StartSession(0, 0, 10);
    env.Runtime.Send(new IEventHandle(env.Mirrorer, env.Mirrorer, new TEvents::TEvWakeup()), 0, true);
    env.Dispatch();
    UNIT_ASSERT(env.Factory.Session(0).WaitEventCount > 0);
}

} // Y_UNIT_TEST_SUITE(TMirrorerActor)
