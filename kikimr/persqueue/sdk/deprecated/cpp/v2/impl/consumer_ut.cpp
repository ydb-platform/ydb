#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NPersQueue {
Y_UNIT_TEST_SUITE(TConsumerTest) {
    Y_UNIT_TEST(NotStartedConsumerCanBeDestructed) {
        // Test that consumer doesn't hang on till shutdown
        TPQLib lib;
        TConsumerSettings settings;
        settings.Server = TServerSetting{"localhost"};
        settings.Topics.push_back("topic");
        settings.ClientId = "client";
        lib.CreateConsumer(settings, {}, false);
    }

    Y_UNIT_TEST(StartTimeout) {
        if (GrpcV1EnabledByDefault()) {
            return;
        }

        TTestServer testServer;
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic", 1);
        testServer.WaitInit("topic");
        // Test that consumer doesn't hang on till shutdown
        TConsumerSettings settings;
        settings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        settings.Topics.push_back("topic");
        settings.ClientId = "client";
        auto consumer = testServer.PQLib->CreateConsumer(settings, {}, false);
        auto startResult = consumer->Start(TInstant::Now());
        UNIT_ASSERT_EQUAL_C(startResult.GetValueSync().Response.GetError().GetCode(), NErrorCode::CREATE_TIMEOUT, startResult.GetValueSync().Response);

        DestroyAndWait(consumer);
    }

    TProducerSettings MakeProducerSettings(const TTestServer& testServer) {
        TProducerSettings producerSettings;
        producerSettings.ReconnectOnFailure = false;
        producerSettings.Topic = "topic1";
        producerSettings.SourceId = "123";
        producerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        producerSettings.Codec = ECodec::LZOP;
        return producerSettings;
    }

    TConsumerSettings MakeConsumerSettings(const TTestServer& testServer) {
        TConsumerSettings settings;
        settings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        settings.Topics.emplace_back("topic1");
        settings.ClientId = "user";
        return settings;
    }

    Y_UNIT_TEST(CancelsOperationsAfterPQLibDeath) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);
        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);

        const size_t partitions = 1;
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);
        testServer.WaitInit("topic1");


        auto consumer = testServer.PQLib->CreateConsumer(MakeConsumerSettings(testServer), testServer.PQLibSettings.DefaultLogger, false);
        UNIT_ASSERT(!consumer->Start().GetValueSync().Response.HasError());
        auto isDead = consumer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        auto msg1 = consumer->GetNextMessage();
        auto msg2 = consumer->GetNextMessage();

        testServer.PQLib = nullptr;
        Cerr << "PQLib destroyed" << Endl;

        UNIT_ASSERT(msg1.HasValue());
        UNIT_ASSERT(msg2.HasValue());

        UNIT_ASSERT(msg1.GetValue().Response.HasError());
        UNIT_ASSERT(msg2.GetValue().Response.HasError());

        auto msg3 = consumer->GetNextMessage();
        UNIT_ASSERT(msg3.HasValue());
        UNIT_ASSERT(msg3.GetValue().Response.HasError());
    }
}
} // namespace NPersQueue
