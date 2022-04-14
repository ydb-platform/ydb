#include "multicluster_producer.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>

#include <atomic>

namespace NPersQueue {

Y_UNIT_TEST_SUITE(TWeightIndexTest) {
    Y_UNIT_TEST(Enables) {
        TWeightIndex wi(3);
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 0);

        wi.SetWeight(0, 1);
        wi.SetWeight(1, 2);
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 0);

        wi.Disable(1);
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 0);

        wi.Enable(0);
        wi.Enable(0); // no changes
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 1);

        wi.Enable(1);
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 3);

        wi.Disable(1);
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 1);

        wi.Disable(1); // no changes
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 1);
    }

    Y_UNIT_TEST(Chooses) {
        const size_t weightsCount = 3;
        TWeightIndex wi(weightsCount);
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), 0);
        unsigned weights[weightsCount] = {1, 2, 3};
        for (size_t i = 0; i < weightsCount; ++i) {
            wi.SetWeight(i, weights[i]);
            wi.Enable(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(wi.EnabledCount(), weightsCount);
        UNIT_ASSERT_VALUES_EQUAL(wi.WeightsSum(), 6);
        unsigned cnt[weightsCount] = {};
        for (size_t i = 0; i < wi.WeightsSum(); ++i) {
            const size_t choice = wi.Choose(i);
            UNIT_ASSERT(choice < weightsCount); // it is index
            ++cnt[choice];
        }
        for (size_t i = 0; i < weightsCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(weights[i], cnt[i], "i: " << i);
        }
    }
}

Y_UNIT_TEST_SUITE(TMultiClusterProducerTest) {
    Y_UNIT_TEST(NotStartedProducerCanBeDestructed) {
        // Test that producer doesn't hang on till shutdown
        TPQLib lib;
        TMultiClusterProducerSettings settings;
        settings.SourceIdPrefix = "prefix";
        settings.ServerWeights.resize(2);

        {
            auto& w = settings.ServerWeights[0];
            w.Weight = 100500;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost"};
            s.Topic = "topic1";
            s.MaxAttempts = 10;
        }

        {
            auto& w = settings.ServerWeights[1];
            w.Weight = 1;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost"};
            s.Topic = "topic2";
            s.MaxAttempts = 10;
        }
        lib.CreateMultiClusterProducer(settings, {}, false);
    }

    static void InitServer(TTestServer& testServer) {
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);

        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", 2);

        testServer.WaitInit("topic1");
    }

    static void ConnectsToMinOnlineDcs(size_t min) {
        return;  // Test is ignored. FIX: KIKIMR-7886

        TTestServer testServer(false);
        InitServer(testServer);

        TMultiClusterProducerSettings settings;
        settings.SourceIdPrefix = "prefix";
        settings.ServerWeights.resize(2);
        settings.MinimumWorkingDcsCount = min;

        {
            auto& w = settings.ServerWeights[0];
            w.Weight = 100500;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost", testServer.PortManager->GetPort()}; // port with nothing up
            s.Topic = "topic1";
            s.ReconnectionDelay = TDuration::MilliSeconds(1);
            s.MaxAttempts = 10;
        }

        {
            auto& w = settings.ServerWeights[1];
            w.Weight = 1;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost", testServer.GrpcPort};
            s.Topic = "topic1";
            s.MaxAttempts = 10;
        }

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLib lib;
        auto producer = lib.CreateMultiClusterProducer(settings, logger, false);
        auto start = producer->Start();

        if (min == 1) {
            UNIT_ASSERT_C(!start.GetValueSync().Response.HasError(), start.GetValueSync().Response);
        } else {
            UNIT_ASSERT_C(start.GetValueSync().Response.HasError(), start.GetValueSync().Response);
        }

        DestroyAndWait(producer);
    }

    Y_UNIT_TEST(ConnectsToMinOnlineDcs) {
        ConnectsToMinOnlineDcs(1);
    }

    Y_UNIT_TEST(FailsToConnectToMinOnlineDcs) {
        return; // Test is ignored. FIX: KIKIMR-7886

        ConnectsToMinOnlineDcs(2);
    }

    TConsumerSettings MakeConsumerSettings(const TTestServer& testServer) {
        TConsumerSettings consumerSettings;
        consumerSettings.ClientId = "user";
        consumerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        consumerSettings.Topics.push_back("topic1");
        consumerSettings.CommitsDisabled = true;
        return consumerSettings;
    }

    void AssertWriteValid(const NThreading::TFuture<TProducerCommitResponse>& respFuture) {
        const TProducerCommitResponse& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TWriteResponse::kAck, "Msg: " << resp.Response);
    }

    void AssertReadValid(const NThreading::TFuture<TConsumerMessage>& respFuture) {
        const TConsumerMessage& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kData, "Msg: " << resp.Response);
    }

    std::pair<size_t, size_t> ReadNMessagesAndDestroyConsumers(THolder<IConsumer> consumer1, THolder<IConsumer> consumer2, size_t n) {
        std::atomic<size_t> readsDone(0), reads1(0), reads2(0);
        TSystemEvent ev;
        std::pair<std::atomic<size_t>*, IConsumer*> infos[] = {
            std::make_pair(&reads1, consumer1.Get()),
            std::make_pair(&reads2, consumer2.Get())
        };
        for (size_t i = 0; i < n; ++i) {
            for (auto& info : infos) {
                auto* reads = info.first;
                info.second->GetNextMessage().Subscribe([&, reads](const NThreading::TFuture<TConsumerMessage>& respFuture) {
                    ++(*reads);
                    const size_t newReadsDone = ++readsDone;
                    if (newReadsDone == n) {
                        ev.Signal();
                    }
                    if (newReadsDone <= n) {
                        AssertReadValid(respFuture);
                    }
                });
            }
        }
        ev.Wait();
        consumer1 = nullptr;
        consumer2 = nullptr;
        return std::pair<size_t, size_t>(reads1, reads2);
    }

    Y_UNIT_TEST(WritesInSubproducers) {
        return; // Test is ignored. FIX: KIKIMR-7886

        TTestServer testServer1(false);
        InitServer(testServer1);

        TTestServer testServer2(false);
        InitServer(testServer2);

        TMultiClusterProducerSettings settings;
        settings.SourceIdPrefix = "producer";
        settings.ServerWeights.resize(2);
        settings.MinimumWorkingDcsCount = 1;

        {
            auto& w = settings.ServerWeights[0];
            w.Weight = 1;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost", testServer1.GrpcPort};
            s.Topic = "topic1";
            s.SourceId = "src0";
            s.MaxAttempts = 5;
            s.ReconnectionDelay = TDuration::MilliSeconds(30);
        }

        {
            auto& w = settings.ServerWeights[1];
            w.Weight = 1;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost", testServer2.GrpcPort};
            s.Topic = "topic1";
            s.SourceId = "src1";
            s.MaxAttempts = 2;
            s.ReconnectionDelay = TDuration::MilliSeconds(30);
        }

        TPQLibSettings pqLibSettings;
        pqLibSettings.ThreadsCount = 1;
        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLib lib(pqLibSettings);
        auto producer = lib.CreateMultiClusterProducer(settings, logger, false);
        auto startProducerResult = producer->Start();
        UNIT_ASSERT_C(!startProducerResult.GetValueSync().Response.HasError(), startProducerResult.GetValueSync().Response);

        auto consumer1 = lib.CreateConsumer(MakeConsumerSettings(testServer1), logger, false);
        UNIT_ASSERT(!consumer1->Start().GetValueSync().Response.HasError());

        auto consumer2 = lib.CreateConsumer(MakeConsumerSettings(testServer2), logger, false);
        UNIT_ASSERT(!consumer2->Start().GetValueSync().Response.HasError());

        const size_t writes = 100;
        for (size_t i = 0; i < writes; ++i) {
            auto write = producer->Write(TString(TStringBuilder() << "blob_1_" << i));
            AssertWriteValid(write);
        }

        const std::pair<size_t, size_t> msgs = ReadNMessagesAndDestroyConsumers(std::move(consumer1), std::move(consumer2), writes);
        UNIT_ASSERT_C(msgs.first > 30, "msgs.first = " << msgs.first);
        UNIT_ASSERT_C(msgs.second > 30, "msgs.second = " << msgs.second);

        consumer1 = lib.CreateConsumer(MakeConsumerSettings(testServer1), logger, false);
        UNIT_ASSERT(!consumer1->Start().GetValueSync().Response.HasError());

        testServer2.ShutdownServer();
        //testServer2.CleverServer->ShutdownGRpc();

        // Assert that all writes go in 1st DC
        for (size_t i = 0; i < writes; ++i) {
            auto write = producer->Write(TString(TStringBuilder() << "blob_2_" << i));
            auto read = consumer1->GetNextMessage();
            AssertWriteValid(write);
            AssertReadValid(read);
        }

        // Assert that all futures will be signalled after producer's death
        std::vector<NThreading::TFuture<TProducerCommitResponse>> writesFutures(writes);
        for (size_t i = 0; i < writes; ++i) {
            writesFutures[i] = producer->Write(TString(TStringBuilder() << "blob_3_" << i));
        }

        auto isDead = producer->IsDead();
        producer = nullptr;
        for (auto& f : writesFutures) {
            f.GetValueSync();
        }
        isDead.GetValueSync();

        DestroyAndWait(consumer1);
        DestroyAndWait(consumer2);
    }

    Y_UNIT_TEST(CancelsOperationsAfterPQLibDeath) {
        return;  // Test is ignored. FIX: KIKIMR-7886

        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer();

        const size_t partitions = 1;
        testServer.AnnoyingClient->InitRoot();
        testServer.AnnoyingClient->InitDCs(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);
        testServer.AnnoyingClient->InitSourceIds();

        testServer.WaitInit("topic1");

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLibSettings pqLibSettings;
        pqLibSettings.DefaultLogger = logger;
        THolder<TPQLib> PQLib = MakeHolder<TPQLib>(pqLibSettings);

        TMultiClusterProducerSettings settings;
        settings.SourceIdPrefix = "producer";
        settings.ServerWeights.resize(2);
        settings.MinimumWorkingDcsCount = 1;

        for (auto& w : settings.ServerWeights) {
            w.Weight = 1;
            auto& s = w.ProducerSettings;
            s.ReconnectOnFailure = true;
            s.Server = TServerSetting{"localhost", testServer.GrpcPort};
            s.Topic = "topic1";
            s.SourceId = "src0";
            s.MaxAttempts = 5;
            s.ReconnectionDelay = TDuration::MilliSeconds(30);
        }

        auto producer = PQLib->CreateMultiClusterProducer(settings, logger, false);
        UNIT_ASSERT(!producer->Start().GetValueSync().Response.HasError());
        auto isDead = producer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        auto write1 = producer->Write(1, TString("blob1"));
        auto write2 = producer->Write(2, TString("blob2"));

        PQLib = nullptr;

        UNIT_ASSERT(write1.HasValue());
        UNIT_ASSERT(write2.HasValue());

        auto write3 = producer->Write(3, TString("blob3"));
        UNIT_ASSERT(write3.HasValue());
        UNIT_ASSERT(write3.GetValue().Response.HasError());
    }
}
} // namespace NPersQueue
