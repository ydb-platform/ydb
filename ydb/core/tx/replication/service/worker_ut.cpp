#include "service.h"
#include "table_writer.h"
#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/string/builder.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(Worker) {
    using namespace NTestHelpers;

    // Model receiver rollback using a table with old rows and deduplication offsets.
    void CheckReceiverRollbackState(bool restoreSnapshot) {
        TEnv env;
        auto& runtime = env.GetRuntime();
        const TString topicPath = "/Root/topic";
        const TString consumer = "consumer";
        const TString baseline = R"({"key":[1],"update":{"value":"before-snapshot"}})";

        NYdb::NTopic::TTopicClient topicClient(env.GetDriver());
        const auto created = topicClient.CreateTopic(topicPath, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer().ConsumerName(consumer).EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        for (const TString& name : {TString("Live"), TString("Snapshot")}) {
            UNIT_ASSERT_VALUES_EQUAL(env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = name,
                .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            })), NMsgBusProxy::MSTATUS_OK);
        }

        auto waitCommitted = [&](ui64 offset, bool sessionClosed = false) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            while (TInstant::Now() < deadline) {
                const auto result = topicClient.DescribeConsumer(topicPath, consumer,
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)
                        .ClientTimeout(TDuration::Seconds(5))).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto& partitions = result.GetConsumerDescription().GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
                const auto& stats = partitions.front().GetPartitionConsumerStats();
                UNIT_ASSERT(stats);
                if (stats->GetCommittedOffset() == offset && (!sessionClosed || stats->GetReadSessionId().empty())) {
                    return;
                }
            }
            UNIT_FAIL("Timed out waiting for offset " << offset << ", sessionClosed=" << sessionClosed);
        };

        NYdb::NTable::TTableClient tableClient(env.GetDriver());
        const auto sessionResult = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        auto readKeys = [&](const TString& tablePath) {
            const auto result = session.ExecuteDataQuery(
                TStringBuilder() << "SELECT key FROM `" << tablePath << "` ORDER BY key",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto parser = result.GetResultSetParser(0);
            TVector<ui32> keys;
            while (parser.TryNextRow()) {
                keys.push_back(parser.ColumnParser(0).GetOptionalUint32().value());
            }
            return keys;
        };

        auto startWorker = [&](const TString& tablePath) {
            return runtime.Register(CreateWorker(env.GetSender(),
                [ydbProxy = env.GetYdbProxy(), topicPath, consumer] {
                    return CreateRemoteTopicReader(ydbProxy, TEvYdbProxy::TTopicReaderSettings()
                        .ConsumerName(consumer)
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(topicPath).AppendPartitionIds(0)));
                },
                [tablePathId = env.GetPathId(tablePath)] {
                    return CreateLocalTableWriter("/Root", tablePathId);
                }));
        };

        // Seed both the old row and its durable offset using the reader's source identity.
        const auto snapshotWriter = runtime.Register(CreateLocalTableWriter("/Root", env.GetPathId("/Root/Snapshot")));
        env.Send<TEvWorker::TEvHandshake>(snapshotWriter, new TEvWorker::TEvHandshake());
        env.Send<TEvWorker::TEvPoll>(snapshotWriter, new TEvWorker::TEvData(0, "0", {TTopicMessage(0, baseline)}));
        env.SendAsync(snapshotWriter, new TEvents::TEvPoison());

        auto worker = startWorker("/Root/Live");
        UNIT_ASSERT(WriteTopic(env, topicPath, baseline));
        TVector<ui32> allKeys{1};
        for (ui32 key = 2; key <= 31; ++key) {
            UNIT_ASSERT(WriteTopic(env, topicPath,
                TStringBuilder() << R"({"key":[)" << key << R"(],"update":{"value":"after-snapshot"}})"));
            allKeys.push_back(key);
        }
        waitCommitted(31);
        UNIT_ASSERT_VALUES_EQUAL(readKeys("/Root/Live"), allKeys);
        UNIT_ASSERT_VALUES_EQUAL(readKeys("/Root/Snapshot"), (TVector<ui32>{1}));

        env.SendAsync(worker, new TEvents::TEvPoison());
        // Close the old SDK read session before assigning the partition again.
        waitCommitted(31, true);
        const TString resumedTable = restoreSnapshot ? "/Root/Snapshot" : "/Root/Live";
        worker = startWorker(resumedTable);
        UNIT_ASSERT(WriteTopic(env, topicPath, R"({"key":[32],"update":{"value":"after-restart"}})"));
        waitCommitted(32);
        const auto keys = readKeys(resumedTable);
        env.SendAsync(worker, new TEvents::TEvPoison());
        waitCommitted(32, true);

        // Characterize the existing bug: rollback loses the 30 acknowledged rows.
        allKeys.push_back(32);
        if (restoreSnapshot) {
            UNIT_ASSERT_VALUES_EQUAL(keys, (TVector<ui32>{1, 32}));
            UNIT_ASSERT_VALUES_UNEQUAL(keys, allKeys);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(keys, allKeys);
        }
    }

    Y_UNIT_TEST(ReceiverRollbackStateSkipsCommittedRows) {
        CheckReceiverRollbackState(true);
    }

    Y_UNIT_TEST(OrdinaryWorkerRestartPreservesCommittedRows) {
        CheckReceiverRollbackState(false);
    }

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(env.GetYdbProxy(),
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic",
                    NYdb::NTopic::TCreateTopicSettings()
                        .BeginAddConsumer()
                            .ConsumerName("consumer")
                        .EndAddConsumer()
            ));

            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        }));

        auto createReaderFn = [ydbProxy = env.GetYdbProxy()]() {
            return CreateRemoteTopicReader(ydbProxy,
                TEvYdbProxy::TTopicReaderSettings()
                    .ConsumerName("consumer")
                    .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                        .Path("/Root/topic")
                        .AppendPartitionIds(0)
                    )
            );
        };

        auto createWriterFn = [tablePathId = env.GetPathId("/Root/Table")]() {
            return CreateLocalTableWriter("/Root", tablePathId);
        };

        auto worker = env.GetRuntime().Register(CreateWorker(env.GetSender(), std::move(createReaderFn), std::move(createWriterFn)));
        Y_UNUSED(worker);

        UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"({"key":[1], "update":{"value":"10"}})"));
        UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"({"key":[2], "update":{"value":"20"}})"));
        UNIT_ASSERT(WriteTopic(env, "/Root/topic", R"({"key":[3], "update":{"value":"30"}})"));
    }

    void CheckReaderRetryLimit(ui32 successfulBatches) {
        class TWriter: public TActorBootstrapped<TWriter> {
        public:
            explicit TWriter(TActorId edge)
                : Edge(edge)
            {
            }

            void Bootstrap() {
                Become(&TThis::StateWork);
            }

            void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
                Worker = ev->Sender;
                Send(Worker, new TEvWorker::TEvHandshake());
            }

            void Handle(TEvWorker::TEvData::TPtr& ev) {
                Send(Edge, new TEvWorker::TEvData(
                    ev->Get()->PartitionId, ev->Get()->Source, ev->Get()->Records));
                Send(Worker, new TEvWorker::TEvPoll());
            }

            STATEFN(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvWorker::TEvHandshake, Handle);
                    hFunc(TEvWorker::TEvData, Handle);
                    cFunc(TEvents::TEvPoison::EventType, PassAway);
                }
            }

        private:
            const TActorId Edge;
            TActorId Worker;
        };

        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto edge = env.GetSender();
        const auto topicSession = runtime.AllocateEdgeActor();
        const auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("replicationConsumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings()
                .Path("/Root/topic")
                .AppendPartitionIds(0));

        const auto worker = runtime.Register(CreateWorker(edge,
            [edge, settings] { return CreateRemoteTopicReader(edge, settings); },
            [edge] { return new TWriter(edge); }));

        TActorId previousReader;
        auto createReader = [&] {
            TAutoPtr<IEventHandle> ev;
            runtime.GrabEdgeEvents<TEvYdbProxy::TEvCreateTopicReaderRequest, TEvWorker::TEvGone>(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvYdbProxy::TEvCreateTopicReaderRequest::EventType);
            const auto reader = ev->Sender;
            UNIT_ASSERT(reader != previousReader);
            previousReader = reader;

            runtime.Send(new IEventHandle(reader, edge,
                new TEvYdbProxy::TEvCreateTopicReaderResponse(topicSession)));
            const auto read = runtime.GrabEdgeEvent<TEvYdbProxy::TEvReadTopicRequest>(topicSession);
            UNIT_ASSERT_VALUES_EQUAL(read->Sender, reader);
            return reader;
        };

        const TString issue = "Injected session failure after reader handshake";
        auto failReader = [&](TActorId reader) {
            runtime.Send(new IEventHandle(reader, topicSession,
                new TEvYdbProxy::TEvTopicReaderGone(NYdb::TStatus(NYdb::EStatus::UNAVAILABLE,
                    NYdb::NIssue::TIssues{NYdb::NIssue::TIssue(issue)}))));
        };

        for (ui32 batch = 0; batch < successfulBatches; ++batch) {
            // Use almost all of the retry budget before making progress. Each
            // successful batch must allow another series of transient failures.
            for (ui32 attempt = 0; attempt < 2; ++attempt) {
                failReader(createReader());
            }

            const auto reader = createReader();
            runtime.Send(new IEventHandle(reader, topicSession,
                new TEvYdbProxy::TEvReadTopicResponse(0, TVector<TTopicMessage>{
                    TTopicMessage(batch, "data"),
                })));
            const auto data = runtime.GrabEdgeEvent<TEvWorker::TEvData>(edge);
            UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data->Get()->Records.front().GetOffset(), batch);
            const auto read = runtime.GrabEdgeEvent<TEvYdbProxy::TEvReadTopicRequest>(topicSession);
            UNIT_ASSERT_VALUES_EQUAL(read->Sender, reader);
            failReader(reader);
        }

        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            failReader(createReader());
        }

        TAutoPtr<IEventHandle> ev;
        runtime.GrabEdgeEvents<TEvYdbProxy::TEvCreateTopicReaderRequest, TEvWorker::TEvGone>(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvWorker::TEvGone::EventType);
        UNIT_ASSERT_VALUES_EQUAL(ev->Sender, worker);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvWorker::TEvGone>()->Status, TEvWorker::TEvGone::UNAVAILABLE);
        UNIT_ASSERT_C(ev->Get<TEvWorker::TEvGone>()->ErrorDescription.Contains(issue),
            ev->Get<TEvWorker::TEvGone>()->ErrorDescription);
    }

    Y_UNIT_TEST(ReaderRetryLimitAfterHandshake) {
        CheckReaderRetryLimit(0);
    }

    Y_UNIT_TEST(ReaderRetryBudgetResetsAfterData) {
        CheckReaderRetryLimit(3);
    }
}

}
