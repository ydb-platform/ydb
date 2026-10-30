#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <library/cpp/testing/unittest/registar.h>

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
<<<<<<< HEAD
=======

    struct TMockReader {
        TTestActorRuntime Runtime;
        TActorId Worker;
        TActorId Proxy;
        TActorId Session;
        TActorId Reader;

        TMockReader(bool failOnDataLoss) {
            Runtime.Initialize(TAppPrepare().Unwrap());
            Worker = Runtime.AllocateEdgeActor();
            Proxy = Runtime.AllocateEdgeActor();
            Session = Runtime.AllocateEdgeActor();
            const auto settings = TEvYdbProxy::TTopicReaderSettings()
                                      .FailOnDataLoss(failOnDataLoss)
                                      .ConsumerName("consumer")
                                      .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/topic").AppendPartitionIds(0));
            Reader = Runtime.Register(CreateRemoteTopicReader(Proxy, settings));
            Runtime.Send(Reader, Worker, new TEvWorker::TEvHandshake());
            Runtime.GrabEdgeEvent<TEvYdbProxy::TEvCreateTopicReaderRequest>(Proxy);
            Runtime.Send(Reader, Proxy, new TEvYdbProxy::TEvCreateTopicReaderResponse(Session));
            Runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(Worker);
        }

        void Start(const TString& sessionId, ui64 committedOffset) {
            Runtime.Send(Reader, Session, new TEvYdbProxy::TEvStartTopicReadingSession(sessionId, committedOffset));
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

        TRetentionGapTest() {
            Env.GetRuntime().GetAppData().FeatureFlags.SetEnableTopicRetentionDeleteLastBlob(true);
            const auto create = Env.Send<TEvYdbProxy::TEvCreateTopicResponse>(Env.GetYdbProxy(),
                new TEvYdbProxy::TEvCreateTopicRequest(TopicPath, NYdb::NTopic::TCreateTopicSettings()
                    .RetentionPeriod(TDuration::Minutes(1))
                    .BeginAddConsumer().ConsumerName(ConsumerName).EndAddConsumer()));
            UNIT_ASSERT_C(create->Get()->Result.IsSuccess(), create->Get()->Result.GetIssues().ToString());
            UNIT_ASSERT(Env.RunCall([&] { return WriteTopic(Env, TopicPath, "expired"); }));
        }

        TActorId StartReader() {
            auto settings = TEvYdbProxy::TTopicReaderSettings()
                .AutoCommit(false)
                .FailOnDataLoss(true)
                .ConsumerName(ConsumerName)
                .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(TopicPath).AppendPartitionIds(0));
            const auto reader = CreateReader(Env, settings);
            const auto started = Env.GetRuntime().GrabEdgeEvent<TEvWorker::TEvReaderStarted>(Env.GetSender(), TDuration::Seconds(30));
            UNIT_ASSERT(started);
            UNIT_ASSERT_VALUES_EQUAL(started->Sender, reader);
            UNIT_ASSERT_VALUES_EQUAL(started->Get()->CommittedOffset, 0);
            return reader;
        }

        void ExpireFirstMessageAndWriteLater() {
            auto& runtime = Env.GetRuntime();
            NYdb::NTopic::TTopicClient topicClient(Env.GetDriver(), NYdb::NTopic::TTopicClientSettings()
                .DiscoveryEndpoint(Env.GetEndpoint()).Database(Env.GetDatabase()));
            ui64 retainedStartOffset = 0;
            for (ui32 attempt = 0; attempt < 10 && retainedStartOffset == 0; ++attempt) {
                runtime.AdvanceCurrentTime(TDuration::Minutes(2));
                auto description = Env.RunCall([&] {
                    return topicClient.DescribeTopic(TopicPath, NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).ExtractValueSync();
                });
                UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
                const auto& partitions = description.GetTopicDescription().GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
                UNIT_ASSERT(partitions.front().GetPartitionStats());
                retainedStartOffset = partitions.front().GetPartitionStats()->GetStartOffset();
            }
            UNIT_ASSERT_VALUES_EQUAL(retainedStartOffset, 1);

            auto consumer = Env.RunCall([&] {
                return topicClient.DescribeConsumer(TopicPath, ConsumerName,
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).ExtractValueSync();
            });
            UNIT_ASSERT_C(consumer.IsSuccess(), consumer.GetIssues().ToString());
            const auto& consumerPartitions = consumer.GetConsumerDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(consumerPartitions.size(), 1);
            UNIT_ASSERT(consumerPartitions.front().GetPartitionConsumerStats());
            UNIT_ASSERT_VALUES_EQUAL(consumerPartitions.front().GetPartitionConsumerStats()->GetCommittedOffset(), 0);

            const auto alter = Env.Send<TEvYdbProxy::TEvAlterTopicResponse>(Env.GetYdbProxy(),
                new TEvYdbProxy::TEvAlterTopicRequest(TopicPath,
                    NYdb::NTopic::TAlterTopicSettings().SetRetentionPeriod(TDuration::Days(1))));
            UNIT_ASSERT_C(alter->Get()->Result.IsSuccess(), alter->Get()->Result.GetIssues().ToString());
            UNIT_ASSERT(Env.RunCall([&] { return WriteTopic(Env, TopicPath, "later"); }));
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
                UNIT_FAIL("Reader delivered data after retention advanced beyond committed offset 0");
            }
            UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::DATA_LOSS);
            UNIT_ASSERT_STRING_CONTAINS(result->Get<TEvWorker::TEvGone>()->ErrorDescription, "expected offset 0, got 1");
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

    Y_UNIT_TEST(RetentionGapBeforeSessionStartWithLaterMessageReportsDataLoss) {
        TRetentionGapTest s;
        s.ExpireFirstMessageAndWriteLater();
        const auto reader = s.StartReader();

        s.Env.SendAsync(reader, new TEvWorker::TEvPoll());
        s.AssertDataLoss(reader);
    }
>>>>>>> 86755d19db1 (Detect data loss when reader session is created and report it via DATA_LOSS status. (#56576))
}

}
