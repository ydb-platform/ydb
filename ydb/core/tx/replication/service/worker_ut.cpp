#include "table_writer.h"
#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(Worker) {
    using namespace NTestHelpers;

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
