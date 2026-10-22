#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/system/mutex.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <memory>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(RemoteTopicReader) {
    using namespace NTestHelpers;

    template <typename Env>
    TActorId CreateReader(Env& env, const TEvYdbProxy::TTopicReaderSettings& settings) {
        do {
            auto reader = env.GetRuntime().Register(CreateRemoteTopicReader(env.GetYdbProxy(), settings));
            env.SendAsync(reader, new TEvWorker::TEvHandshake());

            TAutoPtr<IEventHandle> ev;
            do {
                env.GetRuntime().template GrabEdgeEvents<TEvWorker::TEvHandshake, TEvWorker::TEvGone>(ev);
            } while (ev->Sender != reader);

            switch (ev->GetTypeRewrite()) {
            case TEvWorker::EvHandshake:
                return reader;
            case TEvWorker::EvGone:
                continue;
            }
        } while (true);
    }

    template <typename Env>
    auto ReadData(Env& env, TActorId& reader, const TEvYdbProxy::TTopicReaderSettings& settings) {
        do {
            reader = CreateReader(env, settings);
            env.SendAsync(reader, new TEvWorker::TEvPoll());

            TAutoPtr<IEventHandle> ev;
            do {
                env.GetRuntime().template GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(ev);
            } while (ev->Sender != reader);

            switch (ev->GetTypeRewrite()) {
            case TEvWorker::EvData:
                return ev->Get<TEvWorker::TEvData>()->Records;
            case TEvWorker::EvGone:
                continue;
            }
        } while (true);
    }

    enum class ECompressionReaderProxy { Remote, Local };
    enum class ECompressionReaderDecoder { SdkOrProxy, Worker };

    struct TCompressionBoundary {
        TMutex Mutex;
        TVector<TTopicMessage> Messages;
    };

    class TCompressionSessionTap : public TActorBootstrapped<TCompressionSessionTap> {
    public:
        TCompressionSessionTap(TActorId session, TActorId reader, TActorId proxyTap,
                std::shared_ptr<TCompressionBoundary> boundary)
            : Session(session), Reader(reader), ProxyTap(proxyTap), Boundary(std::move(boundary)) {}

        void Bootstrap() { Become(&TThis::StateWork); }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
            case TEvYdbProxy::TEvReadTopicRequest::EventType:
            case TEvYdbProxy::TEvCommitOffsetRequest::EventType:
                ev->Sender = SelfId();
                Send(ev->Forward(Session));
                break;
            case TEvYdbProxy::TEvReadTopicResponse::EventType:
                {
                    auto guard = Guard(Boundary->Mutex);
                    for (const auto& message : ev->Get<TEvYdbProxy::TEvReadTopicResponse>()->Result.Messages) {
                        if (message.GetOffset() == 1 || message.GetOffset() == 3) {
                            Boundary->Messages.push_back(message);
                        }
                    }
                }
                Send(ev->Forward(Reader));
                break;
            case TEvents::TEvPoison::EventType:
                Send(Session, new TEvents::TEvPoison());
                Send(ProxyTap, new TEvents::TEvPoison());
                PassAway();
                break;
            default:
                Send(ev->Forward(Reader));
                break;
            }
        }

    private:
        const TActorId Session;
        const TActorId Reader;
        const TActorId ProxyTap;
        const std::shared_ptr<TCompressionBoundary> Boundary;
    };

    // Preserve the distinct proxy and read-session routes. Runtime observers
    // cannot intercept actor-to-actor traffic with real actor threads.
    class TCompressionReaderTap : public TActorBootstrapped<TCompressionReaderTap> {
    public:
        TCompressionReaderTap(TActorId proxy, std::shared_ptr<TCompressionBoundary> boundary)
            : Proxy(proxy), Boundary(std::move(boundary)) {}

        void Bootstrap() { Become(&TThis::StateWork); }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
            case TEvYdbProxy::TEvCreateTopicReaderRequest::EventType:
                Reader = ev->Sender;
                ev->Sender = SelfId();
                Send(ev->Forward(Proxy));
                break;
            case TEvYdbProxy::TEvCreateTopicReaderResponse::EventType:
                {
                    auto* response = ev->Get<TEvYdbProxy::TEvCreateTopicReaderResponse>();
                    if (response->Result) {
                        response->Result = Register(new TCompressionSessionTap(response->Result, Reader, SelfId(), Boundary));
                    }
                }
                Send(ev->Forward(Reader));
                break;
            case TEvYdbProxy::TEvCommitOffsetRequest::EventType:
                ev->Sender = SelfId();
                Send(ev->Forward(Proxy));
                break;
            case TEvents::TEvPoison::EventType:
                PassAway();
                break;
            default:
                Send(ev->Forward(Reader));
                break;
            }
        }

    private:
        const TActorId Proxy;
        const std::shared_ptr<TCompressionBoundary> Boundary;
        TActorId Reader;
    };

    void CheckMixedCompression(ECompressionReaderProxy proxyKind, ECompressionReaderDecoder decoder) {
        const bool internal = decoder == ECompressionReaderDecoder::Worker;
        TFeatureFlags flags;
        flags.FeatureFlags.SetTransferInternalDataDecompression(internal);
        TEnv env(flags);
        auto& runtime = env.GetRuntime();
        UNIT_ASSERT(runtime.IsRealThreads());
        const auto proxy = proxyKind == ECompressionReaderProxy::Remote ? env.GetYdbProxy()
            : runtime.Register(CreateLocalYdbProxy(env.GetDatabase()));
        const auto create = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", NYdb::NTopic::TCreateTopicSettings()
                .BeginAddConsumer().ConsumerName("consumer").EndAddConsumer()));
        UNIT_ASSERT(create);
        UNIT_ASSERT_C(create->Get()->Result.IsSuccess(), create->Get()->Result.GetIssues().ToString());
        NYdb::NTopic::TTopicClient client(env.GetDriver());
        auto writer = client.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
            .Path("/Root/topic").ProducerId("compression-producer").MessageGroupId("compression-producer")
            .Codec(NYdb::NTopic::ECodec::RAW));
        TString largeRaw(600 * 1024, '\0');
        ui64 state = 0x6a09e667f3bcc909ULL;
        for (auto& ch : largeRaw) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ch = static_cast<char>(state & 255);
        }
        const TVector<TString> expected{"plain-raw", TString(600 * 1024, 'Q'), largeRaw,
            largeRaw + TString(128 * 1024 + 3, 'Q')};
        TVector<TString> encoded(expected.size());
        for (const size_t i : {size_t(1), size_t(3)}) {
            TStringStream output;
            TZLibCompress compressor(&output, ZLib::GZip, 1);
            compressor.Write(expected[i].data(), expected[i].size());
            compressor.Finish();
            encoded[i] = output.Str();
        }
        UNIT_ASSERT(encoded[1].size() < expected[1].size() / 4);
        UNIT_ASSERT(encoded[3].size() > 500 * 1024);
        for (size_t i = 0; i < expected.size(); ++i) {
            auto message = i == 1 || i == 3
                ? NYdb::NTopic::TWriteMessage::CompressedMessage(std::string_view(encoded[i].data(), encoded[i].size()),
                    NYdb::NTopic::ECodec::GZIP, expected[i].size())
                : NYdb::NTopic::TWriteMessage(std::string_view(expected[i].data(), expected[i].size()));
            message.SeqNo(i + 1).CreateTimestamp(TInstant::MilliSeconds(123456))
                .MessageMeta({{"compression-test", "preserved"}});
            UNIT_ASSERT(env.RunCall([&] { return writer->Write(std::move(message), nullptr, TDuration::Seconds(30)); }));
        }
        UNIT_ASSERT(env.RunCall([&] { return writer->Close(TDuration::Seconds(30)); }));
        auto settings = TEvYdbProxy::TTopicReaderSettings().ConsumerName("consumer").AutoCommit(false)
            .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/topic").AppendPartitionIds(0));
        // This is the same setting combination selected by production ReaderFn.
        settings.Decompress(!internal);
        auto boundary = std::make_shared<TCompressionBoundary>();
        const auto start = [&] {
            const auto tap = runtime.Register(new TCompressionReaderTap(proxy, boundary));
            const auto reader = runtime.Register(CreateRemoteTopicReader(tap, settings));
            env.SendAsync(reader, new TEvWorker::TEvHandshake());
            const auto handshake = runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(env.GetSender(), TDuration::Seconds(60));
            UNIT_ASSERT(handshake);
            UNIT_ASSERT_VALUES_EQUAL(handshake->Sender, reader);
            return reader;
        };
        const auto reader = start();
        // The local proxy preserves the native stored SourceId, including its
        // NUL encoding marker; the public gRPC reader returns the decoded ID.
        const TString expectedProducer = proxyKind == ECompressionReaderProxy::Local
            ? TString("\0", 1) + "compression-producer" : TString("compression-producer");
        size_t offset = 0;
        while (offset < expected.size()) {
            env.SendAsync(reader, new TEvWorker::TEvPoll());
            const auto data = runtime.GrabEdgeEvent<TEvWorker::TEvData>(env.GetSender(), TDuration::Seconds(60));
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->Sender, reader);
            UNIT_ASSERT(!data->Get()->Records.empty());
            for (const auto& record : data->Get()->Records) {
                UNIT_ASSERT(offset < expected.size());
                UNIT_ASSERT_VALUES_EQUAL(record.GetOffset(), offset);
                UNIT_ASSERT_VALUES_EQUAL(record.GetData(), expected[offset]);
                UNIT_ASSERT_VALUES_EQUAL(record.GetSeqNo(), offset + 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetCreateTime(), TInstant::MilliSeconds(123456));
                UNIT_ASSERT_VALUES_EQUAL(record.GetProducerId(), expectedProducer);
                UNIT_ASSERT_VALUES_EQUAL(record.GetMessageGroupId(), expectedProducer);
                UNIT_ASSERT(record.GetMessageMeta());
                UNIT_ASSERT_VALUES_EQUAL(record.GetMessageMeta()->Fields.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetMessageMeta()->Fields[0].first, "compression-test");
                UNIT_ASSERT_VALUES_EQUAL(record.GetMessageMeta()->Fields[0].second, "preserved");
                ++offset;
            }
        }
        {
            auto guard = Guard(boundary->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(boundary->Messages.size(), 2);
            for (size_t i = 0; i < 2; ++i) {
                const auto& message = boundary->Messages[i];
                const size_t offset = i == 0 ? 1 : 3;
                UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), offset);
                UNIT_ASSERT(message.GetCodec() == (internal ? NYdb::NTopic::ECodec::GZIP : NYdb::NTopic::ECodec::RAW));
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), internal ? encoded[offset] : expected[offset]);
            }
        }
        env.SendAsync(reader, new TEvWorker::TEvCommit(4));
        const auto committed = runtime.GrabEdgeEvent<TEvWorker::TEvCommitResult>(env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT(committed);
        UNIT_ASSERT_VALUES_EQUAL(committed->Get()->Offset, 4);
        env.SendAsync(reader, new TEvents::TEvPoison());
        // The next reader must resume after the full logical large messages.
        UNIT_ASSERT(env.RunCall([&] { return WriteTopic(env, "/Root/topic", "after-restart"); }));
        const auto restarted = start();
        env.SendAsync(restarted, new TEvWorker::TEvPoll());
        const auto data = runtime.GrabEdgeEvent<TEvWorker::TEvData>(env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT(data);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records[0].GetOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records[0].GetData(), "after-restart");
    }

    Y_UNIT_TEST(ReadsMixedCompressionRemoteSdk) {
        CheckMixedCompression(ECompressionReaderProxy::Remote, ECompressionReaderDecoder::SdkOrProxy);
    }
    Y_UNIT_TEST(ReadsMixedCompressionRemoteInternal) {
        CheckMixedCompression(ECompressionReaderProxy::Remote, ECompressionReaderDecoder::Worker);
    }
    Y_UNIT_TEST(ReadsMixedCompressionLocalProxy) {
        CheckMixedCompression(ECompressionReaderProxy::Local, ECompressionReaderDecoder::SdkOrProxy);
    }
    Y_UNIT_TEST(ReadsMixedCompressionLocalInternal) {
        CheckMixedCompression(ECompressionReaderProxy::Local, ECompressionReaderDecoder::Worker);
    }

    Y_UNIT_TEST(ReadTopic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        // create topic
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .BeginAddConsumer()
                    .ConsumerName("consumer")
                .EndAddConsumer();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("consumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0)
            );

        TActorId reader;

        // write, create reader & read
        UNIT_ASSERT(WriteTopic(env, "/Root/topic", "message-1"));
        {
            auto records = ReadData(env, reader, settings);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);

            const auto& record = records.at(0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetData(), "message-1");
        }

        // trigger commit, write new data & kill reader
        {
            env.SendAsync(reader, new TEvWorker::TEvPoll());
            UNIT_ASSERT(WriteTopic(env, "/Root/topic", "message-2"));
            env.SendAsync(reader, new TEvents::TEvPoison());
        }

        // create reader again & read
        {
            auto records = ReadData(env, reader, settings);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);

            const auto& record = records.at(0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(record.GetData(), "message-2");
        }
    }

    Y_UNIT_TEST(SourceIndexSchemeErrorIsRetryable) {
        for (const bool retryOnSchemeError : {false, true}) {
            TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());
            const auto worker = runtime.AllocateEdgeActor();
            const auto proxy = runtime.AllocateEdgeActor();
            const auto session = runtime.AllocateEdgeActor();
            const auto settings = TEvYdbProxy::TTopicReaderSettings()
                .RetryOnSchemeError(retryOnSchemeError)
                .ConsumerName("consumer")
                .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                    .Path("/Root/table/index/indexImplTable/stream")
                    .AppendPartitionIds(0));
            const auto reader = runtime.Register(CreateRemoteTopicReader(proxy, settings));
            runtime.Send(reader, worker, new TEvWorker::TEvHandshake());
            runtime.GrabEdgeEvent<TEvYdbProxy::TEvCreateTopicReaderRequest>(proxy);
            runtime.Send(reader, proxy, new TEvYdbProxy::TEvCreateTopicReaderResponse(session));
            runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(worker);
            runtime.Send(reader, session,
                new TEvYdbProxy::TEvTopicReaderGone(NYdb::TStatus(NYdb::EStatus::SCHEME_ERROR, {})));
            const auto gone = runtime.GrabEdgeEvent<TEvWorker::TEvGone>(worker);
            UNIT_ASSERT_VALUES_EQUAL(gone->Get()->Status, retryOnSchemeError
                ? TEvWorker::TEvGone::UNAVAILABLE : TEvWorker::TEvGone::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(PassAwayOnCreatingReadSession) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        auto ydbProxy = env.GetRuntime().AllocateEdgeActor();

        auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("consumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0)
            );

        auto reader = env.GetRuntime().Register(CreateRemoteTopicReader(ydbProxy, settings));
        env.SendAsync(reader, new TEvWorker::TEvHandshake());

        TAutoPtr<IEventHandle> ev;
        do {
            env.GetRuntime().template GrabEdgeEvents<TEvYdbProxy::TEvCreateTopicReaderRequest>(ev);
        } while (ev->Sender != reader);

        env.SendAsync(reader, new TEvents::TEvPoison());

        auto topicReader = env.GetRuntime().AllocateEdgeActor();
        env.SendAsync(reader, new TEvYdbProxy::TEvCreateTopicReaderResponse(topicReader));

        do {
            env.GetRuntime().template GrabEdgeEvents<TEvents::TEvPoison>(ev);
        } while (ev->Sender != reader && ev->Recipient != topicReader);
    }

    void CheckCommitQueue(const TVector<ui64>& offsets, const TVector<ui64>& expectedOffsets) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        const auto worker = runtime.AllocateEdgeActor();
        const auto ydbProxy = runtime.AllocateEdgeActor();
        const auto readSession = runtime.AllocateEdgeActor();
        const auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("consumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0)
            );

        const auto reader = runtime.Register(CreateRemoteTopicReader(ydbProxy, settings));
        runtime.Send(reader, worker, new TEvWorker::TEvHandshake());
        runtime.GrabEdgeEvent<TEvYdbProxy::TEvCreateTopicReaderRequest>(ydbProxy);
        runtime.Send(reader, ydbProxy,
            new TEvYdbProxy::TEvCreateTopicReaderResponse(readSession));
        runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(worker);
        runtime.Send(reader, readSession,
            new TEvYdbProxy::TEvStartTopicReadingSession(TString("read-session"), 7));
        auto started = runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(worker);
        UNIT_ASSERT_VALUES_EQUAL(started->Sender, reader);
        UNIT_ASSERT_VALUES_EQUAL(started->Get()->CommittedOffset, 7);

        // In this runtime Send dispatches the reader's handler synchronously.
        // All commits have been handled before we inspect the outgoing requests.
        for (const auto offset : offsets) {
            runtime.Send(reader, worker, new TEvWorker::TEvCommit(offset));
        }

        for (const auto offset : expectedOffsets) {
            auto requests = runtime.CaptureMailboxEvents(ydbProxy.Hint(), ydbProxy.NodeId());
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
            const auto& request = requests.front();
            UNIT_ASSERT_VALUES_EQUAL(request->GetTypeRewrite(), static_cast<ui32>(TEvYdbProxy::EvCommitOffsetRequest));
            UNIT_ASSERT_VALUES_EQUAL(request->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL(std::get<3>(request->Get<TEvYdbProxy::TEvCommitOffsetRequest>()->GetArgs()), offset);
            UNIT_ASSERT(runtime.CaptureMailboxEvents(worker.Hint(), worker.NodeId()).empty());

            runtime.Send(reader, ydbProxy,
                new TEvYdbProxy::TEvCommitOffsetResponse(NYdb::TStatus(NYdb::EStatus::SUCCESS, {})));
            auto result = runtime.GrabEdgeEvent<TEvWorker::TEvCommitResult>(worker);
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Offset, offset);

            auto notification = runtime.GrabEdgeEvent<TEvYdbProxy::TEvCommitOffsetRequest>(readSession);
            UNIT_ASSERT_VALUES_EQUAL(std::get<3>(notification->Get()->GetArgs()), offset);
        }

        UNIT_ASSERT(runtime.CaptureMailboxEvents(ydbProxy.Hint(), ydbProxy.NodeId()).empty());
        UNIT_ASSERT(runtime.CaptureMailboxEvents(worker.Hint(), worker.NodeId()).empty());
        UNIT_ASSERT(runtime.CaptureMailboxEvents(readSession.Hint(), readSession.NodeId()).empty());
    }

    Y_UNIT_TEST(QueuesCommitWhileAnotherCommitIsInFlight) {
        CheckCommitQueue({10, 20}, {10, 20});
    }

    Y_UNIT_TEST(CoalescesCommitsWhileAnotherCommitIsInFlight) {
        CheckCommitQueue({10, 20, 30, 25, 30, 10, 5}, {10, 30});
    }

    Y_UNIT_TEST(KeepsLargerPendingCommitWhenSmallerCommitArrives) {
        CheckCommitQueue({10, 30, 20}, {10, 30});
    }

    Y_UNIT_TEST(DropsSmallerAndDuplicateCommitsWhileAnotherCommitIsInFlight) {
        CheckCommitQueue({10, 10, 5}, {10});
    }
}

}
