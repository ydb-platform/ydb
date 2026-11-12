#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/ut/common/sdk_ut_common.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <string_view>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

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

    Y_UNIT_TEST_QUAD(ReadTopic, Compress, InternalDecompression) {
        TEnv env(TFeatureFlags().SetTransferInternalDataDecompression(InternalDecompression));
        auto codec = Compress ? NYdb::NTopic::ECodec::GZIP : NYdb::NTopic::ECodec::RAW;
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
            .Decompress(!InternalDecompression)
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0)
            );

        TActorId reader;

        // note: messages must be long and repetitive to benefit from compression
        // write, create reader & read
        UNIT_ASSERT(WriteTopic(env, "/Root/topic", "messagemessagemessagemessagemessagemessage-1", codec));
        {
            auto records = ReadData(env, reader, settings);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);

            const auto& record = records.at(0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetData(), "messagemessagemessagemessagemessagemessage-1");
        }

        // trigger commit, write new data & kill reader
        {
            env.SendAsync(reader, new TEvWorker::TEvPoll());
            UNIT_ASSERT(WriteTopic(env, "/Root/topic", "messagemessagemessagemessagemessagemessage-2", codec));
            env.SendAsync(reader, new TEvents::TEvPoison());
        }

        // create reader again & read
        {
            auto records = ReadData(env, reader, settings);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);

            const auto& record = records.at(0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(record.GetData(), "messagemessagemessagemessagemessagemessage-2");
        }

        if (!Compress) {
            return;
        }
        // trigger commit, write broken data & kill reader
        {
            env.SendAsync(reader, new TEvWorker::TEvPoll());
            UNIT_ASSERT(WriteTopic(env, "/Root/topic",
                        NYdb::NTopic::TWriteMessage::CompressedMessage(
                            "baddata",
                            NYdb::NTopic::ECodec::GZIP,   // GZIP has signature and junk message will be rejected
                            12345)));
            env.SendAsync(reader, new TEvents::TEvPoison());
        }

        // create reader again & read
        {
            reader = CreateReader(env, settings);
            env.SendAsync(reader, new TEvWorker::TEvPoll());

            TAutoPtr<IEventHandle> ev;
            do {
                env.GetRuntime().template GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(ev);
            } while (ev->Sender != reader);

            UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvWorker::EvGone);
            const auto &result = *ev->Get<TEvWorker::TEvGone>();
            UNIT_ASSERT_VALUES_EQUAL_C(result.Status, TEvWorker::TEvGone::EStatus::DATA_ERROR, result.ToString());
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

    struct TMockReader {
        TTestActorRuntime Runtime;
        TActorId Worker;
        TActorId Proxy;
        TActorId Session;
        TActorId Reader;

        TMockReader(bool failOnDataLoss, bool checkStatusForDataLoss = false) {
            Runtime.Initialize(TAppPrepare().Unwrap());
            Worker = Runtime.AllocateEdgeActor();
            Proxy = Runtime.AllocateEdgeActor();
            Session = Runtime.AllocateEdgeActor();
            const auto settings = TEvYdbProxy::TTopicReaderSettings()
                                      .FailOnDataLoss(failOnDataLoss)
                                      .CheckStatusForDataLoss(checkStatusForDataLoss)
                                      .ConsumerName("consumer")
                                      .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/topic").AppendPartitionIds(0));
            Reader = Runtime.Register(CreateRemoteTopicReader(Proxy, settings));
            Runtime.Send(Reader, Worker, new TEvWorker::TEvHandshake());
            Runtime.GrabEdgeEvent<TEvYdbProxy::TEvCreateTopicReaderRequest>(Proxy);
            Runtime.Send(Reader, Proxy, new TEvYdbProxy::TEvCreateTopicReaderResponse(Session));
            Runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(Worker);
        }

        void Start(const TString& sessionId, ui64 committedOffset) {
            Runtime.Send(Reader, Session, new TEvYdbProxy::TEvStartTopicReadingSession(sessionId, committedOffset, std::nullopt));
        }

        void Poll() {
            Runtime.Send(Reader, Worker, new TEvWorker::TEvPoll());
            Runtime.GrabEdgeEvent<TEvYdbProxy::TEvReadTopicRequest>(Session);
        }

        void Read(const TVector<ui64>& offsets) {
            TVector<TTopicMessage> messages;
            for (const auto offset : offsets) {
                messages.emplace_back(offset, "message");
            }
            Runtime.Send(Reader, Session, new TEvYdbProxy::TEvReadTopicResponse(0, std::move(messages)));
        }
    };

    void CheckCommitQueue(const TVector<ui64>& offsets, const TVector<ui64>& expectedOffsets) {
        TMockReader s(true);
        s.Start("read-session", 7);
        auto started = s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(started->Sender, s.Reader);
        UNIT_ASSERT_VALUES_EQUAL(started->Get()->CommittedOffset, 7);

        // In this runtime Send dispatches the reader's handler synchronously.
        // All commits have been handled before we inspect the outgoing requests.
        for (const auto offset : offsets) {
            s.Runtime.Send(s.Reader, s.Worker, new TEvWorker::TEvCommit(offset));
        }

        for (const auto offset : expectedOffsets) {
            auto requests = s.Runtime.CaptureMailboxEvents(s.Proxy.Hint(), s.Proxy.NodeId());
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
            const auto& request = requests.front();
            UNIT_ASSERT_VALUES_EQUAL(request->GetTypeRewrite(), static_cast<ui32>(TEvYdbProxy::EvCommitOffsetRequest));
            UNIT_ASSERT_VALUES_EQUAL(request->Sender, s.Reader);
            UNIT_ASSERT_VALUES_EQUAL(std::get<3>(request->Get<TEvYdbProxy::TEvCommitOffsetRequest>()->GetArgs()), offset);
            UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());

            s.Runtime.Send(s.Reader, s.Proxy, new TEvYdbProxy::TEvCommitOffsetResponse(NYdb::TStatus(NYdb::EStatus::SUCCESS, {})));
            auto result = s.Runtime.GrabEdgeEvent<TEvWorker::TEvCommitResult>(s.Worker);
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, s.Reader);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Offset, offset);

            auto notification = s.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvCommitOffsetRequest>(s.Session);
            UNIT_ASSERT_VALUES_EQUAL(std::get<3>(notification->Get()->GetArgs()), offset);
        }

        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Proxy.Hint(), s.Proxy.NodeId()).empty());
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Session.Hint(), s.Session.NodeId()).empty());
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

    Y_UNIT_TEST(ContiguousOffsetsFromCommittedOffsetAreDelivered) {
        TMockReader s(true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();
        s.Read({7, 8});

        const auto data = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records[0].GetOffset(), 7);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records[1].GetOffset(), 8);

        s.Poll();
        s.Read({9});
        const auto next = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(next->Get()->Records[0].GetOffset(), 9);
    }

    Y_UNIT_TEST(OffsetGapWithinBatchReportsDataLoss) {
        TMockReader s(true);
        s.Start("read-session", 0);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();
        s.Read({0, 2});

        const auto events = s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(events[0]->GetTypeRewrite(), TEvWorker::EvGone);
        UNIT_ASSERT_VALUES_EQUAL(events[0]->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
    }

    Y_UNIT_TEST(OffsetGapBetweenBatchesReportsDataLoss) {
        TMockReader s(true);
        s.Start("read-session", 0);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();
        s.Read({0});
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        s.Poll();
        s.Read({2});

        const auto events = s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(events[0]->GetTypeRewrite(), TEvWorker::EvGone);
        UNIT_ASSERT_VALUES_EQUAL(events[0]->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
    }

    Y_UNIT_TEST(FirstOffsetAfterCommittedOffsetReportsDataLoss) {
        TMockReader s(true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();
        s.Read({8});

        const auto events = s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(events[0]->GetTypeRewrite(), TEvWorker::EvGone);
        UNIT_ASSERT_VALUES_EQUAL(events[0]->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
    }

    Y_UNIT_TEST(OffsetGapIsAllowedWhenDataLossCheckIsDisabled) {
        TMockReader s(false);
        s.Start("read-session", 0);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();
        s.Read({1});

        const auto data = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records[0].GetOffset(), 1);
    }

    Y_UNIT_TEST(MultiOffsetMessageFollowedByNextOffsetIsDelivered) {
        TMockReader s(true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();

        using TMessageInformation = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation;
        TVector<TTopicMessage> messages;
        messages.emplace_back(TMessageInformation(7, "", 0, TInstant::Zero(), TInstant::Zero(), nullptr, nullptr, 0, "", 2), TString("batch"));
        messages.emplace_back(9, "next");
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvReadTopicResponse(0, std::move(messages)));

        TAutoPtr<IEventHandle> result;
        s.Runtime.GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(result);
        UNIT_ASSERT_VALUES_EQUAL(result->GetTypeRewrite(), TEvWorker::EvData);
        const auto& records = result->Get<TEvWorker::TEvData>()->Records;
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(records[0].GetOffset(), 7);
        UNIT_ASSERT_VALUES_EQUAL(records[0].GetLogicalMessageCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(records[1].GetOffset(), 9);
    }

    struct TRetentionGapTest {
        TEnv<> Env{true, ERuntimeMode::Simulated};
        const TString TopicPath = "/Root/topic";
        const TString ConsumerName = "consumer";
        const ui64 InitialOffset;

        explicit TRetentionGapTest(ui64 initialOffset = 0)
            : InitialOffset(initialOffset)
        {
            // SDK/gRPC callbacks run in real threads and must get time to complete
            // before the simulated session-start and retention deadlines expire.
            Env.GetRuntime().SetFastSimulatedTime(false);
            Env.GetRuntime().GetAppData().FeatureFlags.SetEnableTopicRetentionDeleteLastBlob(true);
            const auto create = Env.Send<TEvYdbProxy::TEvCreateTopicResponse>(Env.GetYdbProxy(),
                new TEvYdbProxy::TEvCreateTopicRequest(TopicPath, NYdb::NTopic::TCreateTopicSettings()
                    .RetentionPeriod(TDuration::Minutes(1))
                    .BeginAddConsumer().ConsumerName(ConsumerName).EndAddConsumer()));
            UNIT_ASSERT_C(create->Get()->Result.IsSuccess(), create->Get()->Result.GetIssues().ToString());
            for (ui64 offset = 0; offset < InitialOffset; ++offset) {
                UNIT_ASSERT(Env.RunCall([&] { return WriteTopic(Env, TopicPath, "committed"); }));
            }
            if (InitialOffset) {
                const auto commit = Env.Send<TEvYdbProxy::TEvCommitOffsetResponse>(Env.GetYdbProxy(),
                    new TEvYdbProxy::TEvCommitOffsetRequest(TopicPath, 0, ConsumerName, InitialOffset,
                        NYdb::NTopic::TCommitOffsetSettings()));
                UNIT_ASSERT_C(commit->Get()->Result.IsSuccess(), commit->Get()->Result.GetIssues().ToString());
            }
            UNIT_ASSERT(Env.RunCall([&] { return WriteTopic(Env, TopicPath, "expired"); }));
        }

        TEvYdbProxy::TTopicReaderSettings ReaderSettings(bool checkStatusForDataLoss = false) const {
            return TEvYdbProxy::TTopicReaderSettings()
                .AutoCommit(false)
                .FailOnDataLoss(true)
                .CheckStatusForDataLoss(checkStatusForDataLoss)
                .ConsumerName(ConsumerName)
                .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(TopicPath).AppendPartitionIds(0));
        }

        TActorId StartReader(bool checkStatusForDataLoss = false) {
            const auto reader = CreateReader(Env, ReaderSettings(checkStatusForDataLoss));
            const auto started = Env.GetRuntime().GrabEdgeEvent<TEvWorker::TEvReaderStarted>(Env.GetSender(), TDuration::Seconds(30));
            UNIT_ASSERT(started);
            UNIT_ASSERT_VALUES_EQUAL(started->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL(started->Get()->CommittedOffset, InitialOffset);
            return reader;
        }

        void WaitForFirstMessageReadStatus() {
            auto& runtime = Env.GetRuntime();
            ui64 statusReadOffset = 0;
            auto statusObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvPartitionStatus>(
                [&](NGRpcProxy::V1::TEvPQProxy::TEvPartitionStatus::TPtr& ev) {
                    if (!ev->Get()->Init) {
                        statusReadOffset = ev->Get()->ReadOffset;
                    }
                });

            runtime.AdvanceCurrentTime(TDuration::Seconds(11));
            TDispatchOptions options;
            options.CustomFinalCondition = [&] { return statusReadOffset > InitialOffset; };
            runtime.DispatchEvents(options, TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL(statusReadOffset, InitialOffset + 1);
            statusObserver.Remove();
            runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
        }

        void AssertFirstMessageAndStatusDelivered(const TActorId& reader) {
            auto& runtime = Env.GetRuntime();
            bool readPending = false;
            bool statusReceived = false;
            auto requestObserver = runtime.AddObserver<TEvYdbProxy::TEvReadTopicRequest>(
                [&](TEvYdbProxy::TEvReadTopicRequest::TPtr& ev) {
                    if (ev->Sender == reader) {
                        UNIT_ASSERT(!readPending);
                        readPending = true;
                    }
                });
            auto responseObserver = runtime.AddObserver<TEvYdbProxy::TEvReadTopicResponse>(
                [&](TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
                    if (ev->Recipient == reader) {
                        UNIT_ASSERT(readPending);
                        readPending = false;
                    }
                });
            auto statusObserver = runtime.AddObserver<TEvYdbProxy::TEvTopicReaderStatus>(
                [&](TEvYdbProxy::TEvTopicReaderStatus::TPtr& ev) {
                    if (ev->Recipient == reader) {
                        UNIT_ASSERT_C(readPending, "Proxy consumed an SDK status without a pending Poll");
                        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.ReadOffset, InitialOffset + 1);
                        statusReceived = true;
                    }
                });

            Env.SendAsync(reader, new TEvWorker::TEvPoll());
            TAutoPtr<IEventHandle> result;
            runtime.GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(result, TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL(result->GetTypeRewrite(), TEvWorker::EvData);
            UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvWorker::TEvData>()->Records.front().GetOffset(), InitialOffset);
            UNIT_ASSERT(!readPending);
            UNIT_ASSERT(!statusReceived);

            // The preceding batch has been delivered. Its status must stay in
            // the SDK until another Poll, which remains pending on an empty topic.
            Env.SendAsync(reader, new TEvWorker::TEvPoll());
            TDispatchOptions options;
            options.CustomFinalCondition = [&] { return statusReceived; };
            runtime.DispatchEvents(options, TDuration::Seconds(30));
            UNIT_ASSERT(statusReceived);
            UNIT_ASSERT(readPending);
            UNIT_ASSERT(runtime.CaptureMailboxEvents(Env.GetSender().Hint(), Env.GetSender().NodeId()).empty());

            Env.SendAsync(reader, new TEvents::TEvPoison());
            runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
        }

        void ExpireFirstMessage() {
            auto& runtime = Env.GetRuntime();
            NYdb::NTopic::TTopicClient topicClient(Env.GetDriver(), NYdb::NTopic::TTopicClientSettings()
                .DiscoveryEndpoint(Env.GetEndpoint()).Database(Env.GetDatabase()));
            ui64 retainedStartOffset = 0;
            for (ui32 attempt = 0; attempt < 10 && retainedStartOffset < InitialOffset + 1; ++attempt) {
                runtime.AdvanceCurrentTime(TDuration::Minutes(2));
                auto description = Env.RunCall([&] {
                    return topicClient.DescribeTopic(TopicPath, NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).ExtractValueSync();
                });
                UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
                const auto& partitions = description.GetTopicDescription().GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
                UNIT_ASSERT(partitions.front().GetPartitionStats());
                retainedStartOffset = partitions.front().GetPartitionStats()->GetStartOffset();
                UNIT_ASSERT_VALUES_EQUAL(partitions.front().GetPartitionStats()->GetEndOffset(), InitialOffset + 1);
            }
            // All messages, including the last unread one, are gone. No later message exists.
            UNIT_ASSERT_VALUES_EQUAL(retainedStartOffset, InitialOffset + 1);

            auto consumer = Env.RunCall([&] {
                return topicClient.DescribeConsumer(TopicPath, ConsumerName,
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).ExtractValueSync();
            });
            UNIT_ASSERT_C(consumer.IsSuccess(), consumer.GetIssues().ToString());
            const auto& consumerPartitions = consumer.GetConsumerDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(consumerPartitions.size(), 1);
            UNIT_ASSERT(consumerPartitions.front().GetPartitionConsumerStats());
            UNIT_ASSERT_VALUES_EQUAL(consumerPartitions.front().GetPartitionConsumerStats()->GetCommittedOffset(), InitialOffset);
        }

        void WriteLater() {
            const auto alter = Env.Send<TEvYdbProxy::TEvAlterTopicResponse>(Env.GetYdbProxy(),
                new TEvYdbProxy::TEvAlterTopicRequest(TopicPath,
                    NYdb::NTopic::TAlterTopicSettings().SetRetentionPeriod(TDuration::Days(1))));
            UNIT_ASSERT_C(alter->Get()->Result.IsSuccess(), alter->Get()->Result.GetIssues().ToString());
            UNIT_ASSERT(Env.RunCall([&] { return WriteTopic(Env, TopicPath, "later"); }));
        }

        void ExpireFirstMessageAndWriteLater() {
            ExpireFirstMessage();
            WriteLater();
        }

        void AssertDataLoss(const TActorId& reader) {
            auto& runtime = Env.GetRuntime();
            TAutoPtr<IEventHandle> result;
            runtime.GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(result, TDuration::Seconds(30));
            Env.SendAsync(reader, new TEvents::TEvPoison());
            runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, reader);
            if (result->GetTypeRewrite() == TEvWorker::EvData) {
                UNIT_FAIL("Reader delivered data after retention advanced beyond committed offset " << InitialOffset);
            }
            UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
            UNIT_ASSERT_STRING_CONTAINS(result->Get<TEvWorker::TEvGone>()->ErrorDescription,
                TStringBuilder() << "expected offset " << InitialOffset << ", got " << InitialOffset + 1);
        }

        void AssertStartupDataLoss(const TActorId& reader) {
            TAutoPtr<IEventHandle> result;
            Env.GetRuntime().GrabEdgeEvents<TEvWorker::TEvReaderStarted, TEvWorker::TEvGone>(
                result, TDuration::Seconds(30));
            Env.SendAsync(reader, new TEvents::TEvPoison());
            Env.GetRuntime().DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL(result->GetTypeRewrite(), TEvWorker::EvGone);
            UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
            UNIT_ASSERT_STRING_CONTAINS(result->Get<TEvWorker::TEvGone>()->ErrorDescription,
                "read offset 0 is before retained start offset 1");
        }
    };

    Y_UNIT_TEST(RetentionGapAfterSessionStartWithLaterMessageReportsDataLoss) {
        TRetentionGapTest s;
        auto& runtime = s.Env.GetRuntime();

        TVector<TAutoPtr<IEventHandle>> heldReads;
        auto readObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvRead>([&](NGRpcProxy::V1::TEvPQProxy::TEvRead::TPtr& ev) {
            heldReads.emplace_back(ev.Release());
        });

        const auto reader = s.StartReader();

        s.Env.SendAsync(reader, new TEvWorker::TEvPoll());
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return !heldReads.empty(); };
        runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT(!heldReads.empty());

        s.ExpireFirstMessageAndWriteLater();

        readObserver.Remove();
        for (auto& read : heldReads) {
            runtime.Send(read.Release());
        }

        s.AssertDataLoss(reader);
    }

    void CheckRetentionGapAfterSessionStartWithoutLaterMessage(ui64 initialOffset) {
        TRetentionGapTest s(initialOffset);
        auto& runtime = s.Env.GetRuntime();

        TVector<TAutoPtr<IEventHandle>> heldReads;
        auto readObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvRead>([&](NGRpcProxy::V1::TEvPQProxy::TEvRead::TPtr& ev) {
            heldReads.emplace_back(ev.Release());
        });
        bool emptyReadAdvancedOffset = false;
        auto responseObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvReadResponse>(
            [&](NGRpcProxy::V1::TEvPQProxy::TEvReadResponse::TPtr& ev) {
                if (ev->Get()->NextReadOffset == initialOffset + 1) {
                    for (const auto& partition : ev->Get()->Response.read_response().partition_data()) {
                        for (const auto& batch : partition.batches()) {
                            UNIT_ASSERT_VALUES_EQUAL(batch.message_data_size(), 0);
                        }
                    }
                    emptyReadAdvancedOffset = true;
                }
            });

        const auto reader = s.StartReader(true);
        s.Env.SendAsync(reader, new TEvWorker::TEvPoll());
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return !heldReads.empty(); };
        runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT(!heldReads.empty());

        s.ExpireFirstMessage();

        readObserver.Remove();
        for (auto& read : heldReads) {
            runtime.Send(read.Release());
        }

        TAutoPtr<IEventHandle> result;
        runtime.GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(result, TDuration::Seconds(60));
        s.Env.SendAsync(reader, new TEvents::TEvPoison());
        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
        UNIT_ASSERT_C(result, "Reader did not report data loss after retention removed the last message");
        UNIT_ASSERT_C(emptyReadAdvancedOffset, "Server did not advance its read offset after an empty read");
        UNIT_ASSERT_VALUES_EQUAL(result->Sender, reader);
        UNIT_ASSERT_VALUES_EQUAL(result->GetTypeRewrite(), TEvWorker::EvGone);
        UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
        UNIT_ASSERT_STRING_CONTAINS(result->Get<TEvWorker::TEvGone>()->ErrorDescription,
            TStringBuilder() << "expected offset " << initialOffset
                << " is before server read offset " << initialOffset + 1);
    }

    Y_UNIT_TEST(RetentionGapAfterSessionStartWithoutLaterMessageReportsDataLoss) {
        CheckRetentionGapAfterSessionStartWithoutLaterMessage(0);
    }

    Y_UNIT_TEST(RetentionGapAfterSessionStartWithoutLaterMessageFromNonZeroOffsetReportsDataLoss) {
        CheckRetentionGapAfterSessionStartWithoutLaterMessage(7);
    }

    Y_UNIT_TEST(RetentionGapBeforeSessionStartWithLaterMessageReportsDataLoss) {
        TRetentionGapTest s;
        s.ExpireFirstMessageAndWriteLater();
        const auto reader = CreateReader(s.Env, s.ReaderSettings());
        s.AssertStartupDataLoss(reader);
    }

    Y_UNIT_TEST(RetentionGapBeforeSessionStartWithoutLaterMessageReportsDataLoss) {
        TRetentionGapTest s;
        s.ExpireFirstMessage();
        const auto reader = CreateReader(s.Env, s.ReaderSettings());
        s.AssertStartupDataLoss(reader);
    }

    Y_UNIT_TEST(LocalRetentionGapBeforeSessionStartWithoutLaterMessageReportsDataLoss) {
        TRetentionGapTest s;
        s.ExpireFirstMessage();

        auto& runtime = s.Env.GetRuntime();
        const auto proxy = runtime.Register(CreateLocalYdbProxy(s.Env.GetDatabase()));
        const auto reader = runtime.Register(CreateRemoteTopicReader(proxy, s.ReaderSettings()));
        s.Env.SendAsync(reader, new TEvWorker::TEvHandshake());
        const auto handshake = runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(s.Env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(handshake);
        UNIT_ASSERT_VALUES_EQUAL(handshake->Sender, reader);

        s.AssertStartupDataLoss(reader);
    }

    Y_UNIT_TEST(TeardownWithPendingReadSessionKeepsGrpcServiceAlive) {
        TVector<TAutoPtr<IEventHandle>> heldReads;
        TRetentionGapTest s;
        auto& runtime = s.Env.GetRuntime();

        auto readObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvRead>([&](NGRpcProxy::V1::TEvPQProxy::TEvRead::TPtr& ev) {
            heldReads.emplace_back(ev.Release());
        });

        const auto reader = s.StartReader();
        s.Env.SendAsync(reader, new TEvWorker::TEvPoll());

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return !heldReads.empty();
        };
        runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT(!heldReads.empty());
    }

    Y_UNIT_TEST_QUAD(PeriodicStatusPollingRequiresBothDataLossChecks, FailOnDataLoss, CheckStatusForDataLoss) {
        TRetentionGapTest s;
        auto& runtime = s.Env.GetRuntime();
        TActorId sdkReader;
        auto readerObserver = runtime.AddObserver<TEvYdbProxy::TEvCreateTopicReaderResponse>(
            [&](TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
                sdkReader = ev->Get()->Result;
            });

        // Observe timer registration rather than waiting for an absent SDK RPC.
        // Status requests require both checks, independently of client Polls.
        THashSet<TActorId> wakeupRecipients;
        TTestActorRuntimeBase::TScheduledEventFilter previousFilter;
        previousFilter = runtime.SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev, TDuration delay, TInstant& deadline) {
                if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                    wakeupRecipients.insert(ev->GetRecipientRewrite());
                }
                return previousFilter(runtime, ev, delay, deadline);
            });
        Y_DEFER {
            runtime.SetScheduledEventFilter(previousFilter);
        };

        const auto settings = s.ReaderSettings(CheckStatusForDataLoss).FailOnDataLoss(FailOnDataLoss);
        const auto reader = CreateReader(s.Env, settings);
        const auto started = runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(started);
        UNIT_ASSERT_VALUES_EQUAL(started->Sender, reader);
        UNIT_ASSERT(sdkReader);
        UNIT_ASSERT_VALUES_EQUAL(wakeupRecipients.contains(sdkReader), FailOnDataLoss && CheckStatusForDataLoss);

        s.Env.SendAsync(reader, new TEvents::TEvPoison());
        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST_TWIN(StatusBeforeValidDataIsIgnoredByDefault, ReadPending) {
        TMockReader s(true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        if (ReadPending) {
            s.Poll();
        }

        // An older source can report its advanced read offset before delivering
        // the valid data, while both local data queues are still empty.
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"read-session", 8}));
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());

        if (!ReadPending) {
            s.Poll();
        }
        s.Read({7});
        const auto data = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), 7);
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
    }

    Y_UNIT_TEST(StatusReadOffsetAheadOfNextExpectedReportsDataLoss) {
        TMockReader s(true, true);
        s.Start("read-session", 0);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"read-session", 1}));

        const auto gone = s.Runtime.GrabEdgeEvent<TEvWorker::TEvGone>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(gone->Get()->Status, TEvWorker::TEvGone::DATA_LOSS);
    }

    Y_UNIT_TEST(StatusAtNextExpectedOffsetDoesNotReportDataLoss) {
        TMockReader s(true, true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"read-session", 7}));
        s.Poll();
        s.Read({7, 8});
        const auto data = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 2);
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"read-session", 9}));
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
    }

    Y_UNIT_TEST(StatusFromAnotherReadSessionIsIgnored) {
        TMockReader s(true, true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"old-session", 8}));
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
        s.Poll();
        s.Read({7});
        const auto data = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), 7);
    }

    Y_UNIT_TEST(StatusOffsetGapIsAllowedWhenDataLossCheckIsDisabled) {
        TMockReader s(false, true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"read-session", 8}));
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
        s.Poll();
        s.Read({8});
        const auto data = s.Runtime.GrabEdgeEvent<TEvWorker::TEvData>(s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), 8);
    }

    Y_UNIT_TEST_QUAD(StatusWaitsForInternalDecompression, AfterDecompression, GapAfterData) {
        TMockReader s(true, true);
        s.Runtime.GetAppData().FeatureFlags.SetTransferInternalDataDecompression(true);
        s.Start("read-session", 7);
        s.Runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(s.Worker);
        s.Poll();
        s.Read({7});
        auto wakeups = s.Runtime.CaptureMailboxEvents(s.Reader.Hint(), s.Reader.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(wakeups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(wakeups.front()->GetTypeRewrite(), TEvents::TEvWakeup::EventType);
        if (AfterDecompression) {
            s.Runtime.Send(wakeups.front().Release());
            wakeups = s.Runtime.CaptureMailboxEvents(s.Reader.Hint(), s.Reader.NodeId());
            UNIT_ASSERT_VALUES_EQUAL(wakeups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(wakeups.front()->GetTypeRewrite(), TEvents::TEvWakeup::EventType);
        }

        s.Runtime.Send(s.Reader, s.Session, new TEvYdbProxy::TEvTopicReaderStatus(
            TEvYdbProxy::TTopicReaderStatusResult{"read-session", GapAfterData ? 9u : 8u}));
        UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
        s.Runtime.Send(wakeups.front().Release());
        TAutoPtr<IEventHandle> result;
        s.Runtime.GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(result, TDuration::Seconds(30));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->GetTypeRewrite(), TEvWorker::EvData);
        UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvWorker::TEvData>()->Records.front().GetOffset(), 7);
        if (GapAfterData) {
            const auto gone = s.Runtime.GrabEdgeEvent<TEvWorker::TEvGone>(s.Worker, TDuration::Seconds(30));
            UNIT_ASSERT(gone);
            UNIT_ASSERT_VALUES_EQUAL(gone->Get()->Status, TEvWorker::TEvGone::DATA_LOSS);
            UNIT_ASSERT_STRING_CONTAINS(gone->Get()->ErrorDescription,
                "expected offset 8 is before server read offset 9");
        } else {
            UNIT_ASSERT(s.Runtime.CaptureMailboxEvents(s.Worker.Hint(), s.Worker.NodeId()).empty());
        }
    }

    Y_UNIT_TEST(DataBeforeStatusIsDeliveredOnPoll) {
        TRetentionGapTest s;
        auto& runtime = s.Env.GetRuntime();
        bool messageRead = false;
        auto readObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvReadResponse>(
            [&](NGRpcProxy::V1::TEvPQProxy::TEvReadResponse::TPtr& ev) {
                if (ev->Get()->NextReadOffset == s.InitialOffset + 1) {
                    messageRead = true;
                }
            });
        const auto reader = s.StartReader(true);

        // The SDK can prefetch data, but the proxy must leave it there until Poll.
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return messageRead; };
        runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT(messageRead);

        s.WaitForFirstMessageReadStatus();
        s.AssertFirstMessageAndStatusDelivered(reader);
    }

    Y_UNIT_TEST(InFlightReadBeforeStatusDoesNotReportDataLoss) {
        TRetentionGapTest s;
        auto& runtime = s.Env.GetRuntime();
        TVector<TAutoPtr<IEventHandle>> heldResponses;
        auto readObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvReadResponse>(
            [&](NGRpcProxy::V1::TEvPQProxy::TEvReadResponse::TPtr& ev) {
                heldResponses.emplace_back(ev.Release());
            });
        const auto reader = s.StartReader(true);

        TDispatchOptions readOptions;
        readOptions.CustomFinalCondition = [&] { return !heldResponses.empty(); };
        runtime.DispatchEvents(readOptions, TDuration::Seconds(30));
        UNIT_ASSERT(!heldResponses.empty());

        s.WaitForFirstMessageReadStatus();

        readObserver.Remove();
        for (auto& response : heldResponses) {
            runtime.Send(response.Release());
        }
        s.AssertFirstMessageAndStatusDelivered(reader);
    }

    enum class EStatusOrderingMessage {
        MultipartInOneBlob,
        MultipartAcrossBlobs,
        KafkaSingleMessageBatch,
        KafkaMultiMessageBatch,
    };

    void AssertMultipartLayout(TEnv<>& env, const TString& topicPath, bool acrossBlobs) {
        const auto description = env.GetDescription(topicPath);
        UNIT_ASSERT_VALUES_EQUAL(description.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        const auto& partitions = description.GetPathDescription().GetPersQueueGroup().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        UNIT_ASSERT(partitions[0].GetTabletId());

        auto& runtime = env.GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();
        const auto keys = NPQ::GetTabletKeys(runtime, partitions[0].GetTabletId(), edge);
        TVector<NPQ::TKey> dataKeys;
        for (const auto& key : keys) {
            if (!key.empty() && key.front() == NPQ::TKeyPrefix::TypeData) {
                dataKeys.push_back(NPQ::TKey::FromString(key));
            }
        }
        Sort(dataKeys);
        UNIT_ASSERT_C(!dataKeys.empty(), "No persisted message blobs");
        if (acrossBlobs) {
            UNIT_ASSERT_C(dataKeys.size() > 1, "Multipart message must span multiple blobs");
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(dataKeys.size(), 1, "All message parts must share one blob");
        }

        ui32 nextPart = 0;
        ui32 completedMessages = 0;
        for (const auto& key : dataKeys) {
            UNIT_ASSERT_VALUES_EQUAL(key.GetOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(key.GetPartNo(), nextPart);
            nextPart += key.GetInternalPartsCount() + key.GetCount();
            completedMessages += key.GetCount();
        }
        UNIT_ASSERT_VALUES_EQUAL(completedMessages, 1);
        UNIT_ASSERT_C(nextPart > 1, "The native message was not split into parts");
    }

    void CheckInFlightStatusOrdering(EStatusOrderingMessage messageType, bool decompress = true) {
        TEnv<> env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        runtime.SetFastSimulatedTime(false);
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableTopicMessagesBatching(true);
        flags.SetEnableTopicWriteOffsetDeltaInKeys(true);
        flags.SetTransferInternalDataDecompression(false);

        const TString topicPath = "/Root/status-ordering";
        const TString consumerName = "consumer";
        const auto create = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvCreateTopicRequest(topicPath, NYdb::NTopic::TCreateTopicSettings()
                .RetentionPeriod(TDuration::Days(1))
                .BeginAddConsumer().ConsumerName(consumerName).EndAddConsumer()));
        UNIT_ASSERT_C(create->Get()->Result.IsSuccess(), create->Get()->Result.GetIssues().ToString());

        const bool kafka = messageType == EStatusOrderingMessage::KafkaSingleMessageBatch
            || messageType == EStatusOrderingMessage::KafkaMultiMessageBatch;
        const ui32 messageCount = messageType == EStatusOrderingMessage::KafkaMultiMessageBatch ? 3 : 1;
        TVector<TString> expected;
        if (kafka) {
            constexpr size_t messageSize = 32;
            expected.assign(messageCount, TString(messageSize, 'k'));
            if (messageCount == 1) {
                // SDK batching deliberately sends a single record as RAW. Submit
                // an encoded batch to exercise the single-record Kafka format.
                NKafka::TKafkaRecordBatch batch;
                batch.Magic = 2;
                batch.Attributes = static_cast<i16>(NKafka::ECompressionType::NONE);
                batch.LastOffsetDelta = 0;
                batch.BaseTimestamp = 1000;
                batch.MaxTimestamp = 1000;
                batch.ProducerId = 0;
                batch.ProducerEpoch = 0;
                batch.BaseSequence = 1;
                NKafka::TKafkaRecord record;
                record.TimestampDelta = 0;
                record.OffsetDelta = 0;
                record.SetValue(TString(expected.front()));
                record.Length = record.Size(2)
                    - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
                batch.Records.push_back(std::move(record));
                const TString payload = NKafka::WriteKafkaRecordBatch(batch);
                UNIT_ASSERT(env.RunCall([&] {
                    auto message = NYdb::NTopic::TWriteMessage::CompressedMessage(
                        std::string_view(payload.data(), payload.size()), NYdb::NTopic::ECodec::KAFKA_BATCH, messageSize);
                    message.SeqNo(1);
                    message.CreateTimestamp(TInstant::MilliSeconds(1000));
                    return WriteTopic(env, topicPath, std::move(message), NYdb::NTopic::ECodec::RAW);
                }));
            } else {
                NYdb::NTopic::TTopicClient client(env.GetDriver(), NYdb::NTopic::TTopicClientSettings()
                    .DiscoveryEndpoint(env.GetEndpoint()).Database(env.GetDatabase()));
                env.RunCall([&] {
                    NPQ::NTest::WriteKafkaBatchMessages(client, topicPath, "status-producer", messageSize,
                        messageCount, {{1, messageCount, 'k'}});
                });
            }
        } else {
            const bool acrossBlobs = messageType == EStatusOrderingMessage::MultipartAcrossBlobs;
            // Exceed the configured blob size to force a physical blob boundary.
            // RAW prevents the payload from shrinking below the multipart threshold.
            const size_t messageSize = acrossBlobs
                ? runtime.GetAppData().PQConfig.GetMaxBlobSize() + 1_MB
                : 600_KB;
            TString payload(messageSize, '\0');
            for (size_t i = 0; i < payload.size(); ++i) {
                payload[i] = static_cast<char>('a' + (i / 1_KB) % 26);
            }
            expected.push_back(std::move(payload));
            UNIT_ASSERT(env.RunCall([&] {
                return WriteTopic(env, topicPath, expected.front(), NYdb::NTopic::ECodec::RAW);
            }));
            AssertMultipartLayout(env, topicPath, acrossBlobs);
        }

        TActorId sdkReader;
        auto readerObserver = runtime.AddObserver<TEvYdbProxy::TEvCreateTopicReaderResponse>(
            [&](TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
                sdkReader = ev->Get()->Result;
            });
        TVector<TAutoPtr<IEventHandle>> statusTimers;
        auto timerObserver = runtime.AddObserver<TEvents::TEvWakeup>(
            [&](TEvents::TEvWakeup::TPtr& ev) {
                if (ev->Recipient == sdkReader) {
                    statusTimers.emplace_back(ev.Release());
                }
            });
        TVector<TAutoPtr<IEventHandle>> heldResponses;
        auto readObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvReadResponse>(
            [&](NGRpcProxy::V1::TEvPQProxy::TEvReadResponse::TPtr& ev) {
                if (ev->Get()->NextReadOffset == messageCount) {
                    heldResponses.emplace_back(ev.Release());
                }
            });
        ui64 serverStatusOffset = 0;
        auto serverStatusObserver = runtime.AddObserver<NGRpcProxy::V1::TEvPQProxy::TEvPartitionStatus>(
            [&](NGRpcProxy::V1::TEvPQProxy::TEvPartitionStatus::TPtr& ev) {
                if (!ev->Get()->Init) {
                    serverStatusOffset = ev->Get()->ReadOffset;
                }
            });

        const auto settings = TEvYdbProxy::TTopicReaderSettings()
            .AutoCommit(false).FailOnDataLoss(true).CheckStatusForDataLoss(true)
            .Decompress(decompress).ConsumerName(consumerName)
            .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(topicPath).AppendPartitionIds(0));
        const auto reader = CreateReader(env, settings);
        const auto started = runtime.GrabEdgeEvent<TEvWorker::TEvReaderStarted>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(started);
        UNIT_ASSERT_VALUES_EQUAL(started->Sender, reader);
        UNIT_ASSERT_VALUES_EQUAL(started->Get()->CommittedOffset, 0);

        // Hold the first timer until the server has read the entire message/batch.
        // Otherwise a status with read_offset=0 could get stuck in the SDK before Poll.
        TDispatchOptions readOptions;
        readOptions.CustomFinalCondition = [&] { return !heldResponses.empty() && !statusTimers.empty(); };
        runtime.DispatchEvents(readOptions, TDuration::Seconds(30));
        UNIT_ASSERT(!heldResponses.empty());
        UNIT_ASSERT_VALUES_EQUAL(statusTimers.size(), 1);

        ui32 wireMessages = 0;
        for (const auto& response : heldResponses) {
            const auto& readResponse = response->Get<NGRpcProxy::V1::TEvPQProxy::TEvReadResponse>()->Response.read_response();
            for (const auto& partition : readResponse.partition_data()) {
                for (const auto& batch : partition.batches()) {
                    UNIT_ASSERT_VALUES_EQUAL(batch.codec(), kafka ? Ydb::Topic::CODEC_KAFKA_BATCH : Ydb::Topic::CODEC_RAW);
                    for (const auto& message : batch.message_data()) {
                        ++wireMessages;
                        if (kafka) {
                            const auto header = NKafka::ReadKafkaBatchHeader(message.data());
                            UNIT_ASSERT(header);
                            UNIT_ASSERT_VALUES_EQUAL(header->RecordsCount, messageCount);
                        }
                    }
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(wireMessages, 1, "Expected one complete native message or Kafka batch on the wire");

        ui64 deliveredOffset = 0;
        bool statusReceived = false;
        auto statusObserver = runtime.AddObserver<TEvYdbProxy::TEvTopicReaderStatus>(
            [&](TEvYdbProxy::TEvTopicReaderStatus::TPtr& ev) {
                if (ev->Recipient == reader) {
                    UNIT_ASSERT_VALUES_EQUAL_C(deliveredOffset, messageCount, "Status overtook valid data");
                    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.ReadOffset, messageCount);
                    statusReceived = true;
                }
            });

        // Keep a Poll pending so an incorrectly unbuffered status would reach the
        // reader before the withheld data, reproducing the old server behaviour.
        env.SendAsync(reader, new TEvWorker::TEvPoll());
        timerObserver.Remove();
        runtime.Send(statusTimers.front().Release());
        statusTimers.clear();
        TDispatchOptions statusOptions;
        statusOptions.CustomFinalCondition = [&] { return serverStatusOffset == messageCount; };
        runtime.DispatchEvents(statusOptions, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(serverStatusOffset, messageCount);
        UNIT_ASSERT(!statusReceived);
        UNIT_ASSERT(runtime.CaptureMailboxEvents(env.GetSender().Hint(), env.GetSender().NodeId()).empty());

        readObserver.Remove();
        for (auto& response : heldResponses) {
            runtime.Send(response.Release());
        }
        while (deliveredOffset < messageCount) {
            TAutoPtr<IEventHandle> result;
            runtime.GrabEdgeEvents<TEvWorker::TEvData, TEvWorker::TEvGone>(result, TDuration::Seconds(30));
            UNIT_ASSERT_C(result, "Reader did not deliver valid data");
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL_C(result->GetTypeRewrite(), TEvWorker::EvData,
                "Reader reported an error instead of delivering valid data");
            const auto& records = result->Get<TEvWorker::TEvData>()->Records;
            UNIT_ASSERT(!records.empty());
            for (const auto& record : records) {
                UNIT_ASSERT_VALUES_EQUAL(record.GetOffset(), deliveredOffset);
                if (kafka && !decompress) {
                    UNIT_ASSERT_VALUES_EQUAL(record.GetCodec(), NYdb::NTopic::ECodec::KAFKA_BATCH);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetLogicalMessageCount(), messageCount);
                    const auto batch = NKafka::ReadKafkaRecordBatch(record.GetData());
                    UNIT_ASSERT_VALUES_EQUAL(batch.BaseOffset, 0);
                    UNIT_ASSERT_VALUES_EQUAL(batch.Records.size(), expected.size());
                    for (size_t i = 0; i < batch.Records.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(batch.Records[i].OffsetDelta, i);
                        const auto& value = batch.Records[i].Value;
                        UNIT_ASSERT(value);
                        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(value->data(), value->size()), expected[i]);
                    }
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(record.GetLogicalMessageCount(), 1);
                    UNIT_ASSERT_C(deliveredOffset < expected.size(), "Unexpected extra message");
                    UNIT_ASSERT_C(record.GetData() == expected[deliveredOffset], "Message payload was not fully preserved");
                }
                deliveredOffset += record.GetLogicalMessageCount();
            }
            UNIT_ASSERT(!statusReceived);
            env.SendAsync(reader, new TEvWorker::TEvPoll());
        }
        UNIT_ASSERT_VALUES_EQUAL(deliveredOffset, messageCount);

        TDispatchOptions deliveredOptions;
        deliveredOptions.CustomFinalCondition = [&] { return statusReceived; };
        runtime.DispatchEvents(deliveredOptions, TDuration::Seconds(30));
        UNIT_ASSERT(statusReceived);
        UNIT_ASSERT(runtime.CaptureMailboxEvents(env.GetSender().Hint(), env.GetSender().NodeId()).empty());

        env.SendAsync(reader, new TEvents::TEvPoison());
        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(MultipartInOneBlobBeforeStatusDoesNotReportDataLoss) {
        CheckInFlightStatusOrdering(EStatusOrderingMessage::MultipartInOneBlob);
    }

    Y_UNIT_TEST(MultipartAcrossBlobsBeforeStatusDoesNotReportDataLoss) {
        CheckInFlightStatusOrdering(EStatusOrderingMessage::MultipartAcrossBlobs);
    }

    Y_UNIT_TEST_TWIN(KafkaSingleMessageBatchBeforeStatusDoesNotReportDataLoss, Decompress) {
        CheckInFlightStatusOrdering(EStatusOrderingMessage::KafkaSingleMessageBatch, Decompress);
    }

    Y_UNIT_TEST_TWIN(KafkaMultiMessageBatchBeforeStatusDoesNotReportDataLoss, Decompress) {
        CheckInFlightStatusOrdering(EStatusOrderingMessage::KafkaMultiMessageBatch, Decompress);
    }
}

}
