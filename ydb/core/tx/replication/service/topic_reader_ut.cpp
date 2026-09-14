#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

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
