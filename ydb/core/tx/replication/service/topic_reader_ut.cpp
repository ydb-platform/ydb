#include "topic_reader.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/testlib/helpers.h>
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
}

}
