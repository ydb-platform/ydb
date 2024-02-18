#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(RemoteTopicReader) {
    template <typename Env>
    TActorId CreateReader(Env& env, const TEvYdbProxy::TTopicReaderSettings& settings) {
        auto reader = env.GetRuntime().Register(CreateRemoteTopicReader(env.GetYdbProxy(), settings));
        env.SendAsync(reader, new TEvWorker::TEvHandshake());

        while (true) {
            TAutoPtr<IEventHandle> handle;
            auto result = env.GetRuntime().template GrabEdgeEventsRethrow<TEvWorker::TEvHandshake, TEvWorker::TEvGone>(handle);
            if (handle->Sender != reader) {
                continue;
            }

            if (auto* ev = std::get<TEvWorker::TEvHandshake*>(result)) {
                return reader;
            } else if (std::get<TEvWorker::TEvGone*>(result)) {
                reader = env.GetRuntime().Register(CreateRemoteTopicReader(env.GetYdbProxy(), settings));
                env.SendAsync(reader, new TEvWorker::TEvHandshake());
                continue;
            } else {
                UNIT_ASSERT("Unexpected event");
            }
        }
    }

    template <typename Env>
    auto ReadData(Env& env, TActorId& reader, const TEvYdbProxy::TTopicReaderSettings& settings) {
        reader = CreateReader(env, settings);
        env.SendAsync(reader, new TEvWorker::TEvPoll());

        while (true) {
            TAutoPtr<IEventHandle> handle;
            auto result = env.GetRuntime().template GrabEdgeEventsRethrow<TEvWorker::TEvData, TEvWorker::TEvGone>(handle);
            if (handle->Sender != reader) {
                continue;
            }

            if (auto* ev = std::get<TEvWorker::TEvData*>(result)) {
                return ev->Records;
            } else if (std::get<TEvWorker::TEvGone*>(result)) {
                reader = CreateReader(env, settings);
                env.SendAsync(reader, new TEvWorker::TEvPoll());
                continue;
            }
        }
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

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
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
            UNIT_ASSERT_VALUES_EQUAL(record.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(record.Data, "message-1");
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
            UNIT_ASSERT_VALUES_EQUAL(record.Offset, 1);
            UNIT_ASSERT_VALUES_EQUAL(record.Data, "message-2");
        }
    }
}

}
