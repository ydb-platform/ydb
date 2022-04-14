#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NThreading;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;

namespace NPersQueue {

Y_UNIT_TEST_SUITE(TRetryingProducerTest) {
    TProducerSettings FakeSettings() {
        TProducerSettings settings;
        settings.ReconnectOnFailure = true;
        settings.Server = TServerSetting{"localhost"};
        settings.Topic = "topic";
        settings.SourceId = "src";
        return settings;
    }

    Y_UNIT_TEST(NotStartedProducerCanBeDestructed) {
        // Test that producer doesn't hang on till shutdown
        TPQLib lib;
        TProducerSettings settings = FakeSettings();
        lib.CreateProducer(settings, {}, false);
    }

    Y_UNIT_TEST(StartDeadlineExpires) {
        if (std::getenv("PERSQUEUE_GRPC_API_V1_ENABLED"))
            return;
        TPQLibSettings libSettings;
        libSettings.ChannelCreationTimeout = TDuration::MilliSeconds(1);
        libSettings.DefaultLogger = new TCerrLogger(TLOG_DEBUG);
        TPQLib lib(libSettings);
        THolder<IProducer> producer;
        {
            TProducerSettings settings = FakeSettings();
            settings.ReconnectionDelay = TDuration::MilliSeconds(1);
            settings.StartSessionTimeout = TDuration::MilliSeconds(10);
            producer = lib.CreateProducer(settings, {}, false);
        }
        const TInstant beforeStart = TInstant::Now();
        auto future = producer->Start(TDuration::MilliSeconds(100));
        UNIT_ASSERT(future.GetValueSync().Response.HasError());
        UNIT_ASSERT_EQUAL_C(future.GetValueSync().Response.GetError().GetCode(), NErrorCode::CREATE_TIMEOUT,
                            "Error: " << future.GetValueSync().Response.GetError());
        const TInstant now = TInstant::Now();
        UNIT_ASSERT_C(now - beforeStart >= TDuration::MilliSeconds(100), now);

        DestroyAndWait(producer);
    }

    Y_UNIT_TEST(StartMaxAttemptsExpire) {
        TPQLibSettings libSettings;
        libSettings.ChannelCreationTimeout = TDuration::MilliSeconds(1);
        TPQLib lib(libSettings);
        THolder<IProducer> producer;
        {
            TProducerSettings settings = FakeSettings();
            settings.ReconnectionDelay = TDuration::MilliSeconds(10);
            settings.StartSessionTimeout = TDuration::MilliSeconds(10);
            settings.MaxAttempts = 3;
            producer = lib.CreateProducer(settings, {}, false);
        }
        const TInstant beforeStart = TInstant::Now();
        auto future = producer->Start();
        UNIT_ASSERT(future.GetValueSync().Response.HasError());
        const TInstant now = TInstant::Now();
        UNIT_ASSERT_C(now - beforeStart >= TDuration::MilliSeconds(30), now);

        DestroyAndWait(producer);
    }

    void AssertWriteValid(const NThreading::TFuture<TProducerCommitResponse>& respFuture) {
        const TProducerCommitResponse& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TWriteResponse::kAck, "Msg: " << resp.Response);
    }

    void AssertWriteFailed(const NThreading::TFuture<TProducerCommitResponse>& respFuture) {
        const TProducerCommitResponse& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TWriteResponse::kError, "Msg: " << resp.Response);
    }

    void AssertReadValid(const NThreading::TFuture<TConsumerMessage>& respFuture) {
        const TConsumerMessage& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kData, "Msg: " << resp.Response);
    }

    void AssertReadContinuous(const TReadResponse::TData::TMessageBatch& batch, ui64 startSeqNo) {
        for (ui32 i = 0; i < batch.MessageSize(); ++i) {
            ui64 actualSeqNo = batch.GetMessage(i).GetMeta().GetSeqNo();
            UNIT_ASSERT_EQUAL_C(actualSeqNo, startSeqNo + i, "Wrong seqNo: " << actualSeqNo << ", expected: " << startSeqNo + i);
        }
    }

    void AssertCommited(const NThreading::TFuture<TConsumerMessage>& respFuture) {
        const TConsumerMessage& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kCommit, "Msg: " << resp.Response);
    }

    void AssertReadFailed(const NThreading::TFuture<TConsumerMessage>& respFuture) {
        const TConsumerMessage& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kError, "Msg: " << resp.Response);
    }

    void AssertLock(const NThreading::TFuture<TConsumerMessage>& respFuture) {
        const TConsumerMessage& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kLock, "Msg: " << resp.Response);
    }

    TProducerSettings MakeProducerSettings(const TTestServer& testServer) {
        TProducerSettings producerSettings;
        producerSettings.ReconnectOnFailure = true;
        producerSettings.Topic = "topic1";
        producerSettings.SourceId = "123";
        producerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        producerSettings.Codec = ECodec::LZOP;
        producerSettings.ReconnectionDelay = TDuration::MilliSeconds(10);
        return producerSettings;
    }

    TConsumerSettings MakeConsumerSettings(const TTestServer& testServer) {
        TConsumerSettings consumerSettings;
        consumerSettings.ClientId = "user";
        consumerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        consumerSettings.Topics.push_back("topic1");
        return consumerSettings;
    }

    Y_UNIT_TEST(ReconnectsToServer) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);

        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        const size_t partitions = 10;
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);
        testServer.WaitInit("topic1");

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLib PQLib;

        auto producer = PQLib.CreateProducer(MakeProducerSettings(testServer), logger, false);
        producer->Start().Wait();
        TFuture<TError> isDead = producer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        auto consumer = PQLib.CreateConsumer(MakeConsumerSettings(testServer), logger, false);
        consumer->Start().Wait();

        auto read1 = consumer->GetNextMessage();

        // write first value
        auto write1 = producer->Write(1, TString("blob1"));
        AssertWriteValid(write1);
        AssertReadValid(read1);

        // commit
        consumer->Commit({1});
        auto commitAck = consumer->GetNextMessage();
        AssertCommited(commitAck);

        auto read2 = consumer->GetNextMessage();
        testServer.ShutdownServer();
        UNIT_ASSERT(!isDead.HasValue());

        auto write2 = producer->Write(2, TString("blob2"));

        testServer.StartServer(false);

        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);

        AssertWriteValid(write2);

        AssertReadFailed(read2);
        consumer->IsDead().Wait();

        consumer = PQLib.CreateConsumer(MakeConsumerSettings(testServer), logger, false);
        consumer->Start().Wait();

        read2 = consumer->GetNextMessage();
        AssertReadValid(read2);

        UNIT_ASSERT(!isDead.HasValue());

        DestroyAndWait(producer);
        DestroyAndWait(consumer);
    }

    static void DiesOnTooManyReconnectionAttempts(bool callWrite) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);

        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", 2);
        testServer.WaitInit("topic1");

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLib PQLib;

        TProducerSettings settings = MakeProducerSettings(testServer);
        settings.MaxAttempts = 3;
        settings.ReconnectionDelay = TDuration::MilliSeconds(100);
        auto producer = PQLib.CreateProducer(settings, logger, false);
        producer->Start().Wait();
        TFuture<TError> isDead = producer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        // shutdown server
        const TInstant beforeShutdown = TInstant::Now();
        testServer.ShutdownServer();

        NThreading::TFuture<TProducerCommitResponse> write;
        if (callWrite) {
            write = producer->Write(TString("data"));
        }

        isDead.Wait();
        const TInstant afterDead = TInstant::Now();
        // 3 attempts: 100ms, 200ms and 400ms
        UNIT_ASSERT_C(afterDead - beforeShutdown >= TDuration::MilliSeconds(700), "real difference: " << (afterDead - beforeShutdown));

        if (callWrite) {
            AssertWriteFailed(write);
        }
    }

    Y_UNIT_TEST(WriteDataThatWasSentBeforeConnectToServer) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);

        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        const size_t partitions = 10;
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);

        testServer.WaitInit("topic1");

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLib PQLib;

        auto producer = PQLib.CreateProducer(MakeProducerSettings(testServer), logger, false);
        auto producerStarted = producer->Start();
        auto write1 = producer->Write(1, TString("a"));
        producerStarted.Wait();
        TVector<TFuture<TProducerCommitResponse>> acks;
        for (int i = 2; i <= 1000; ++i) {
            acks.push_back(producer->Write(i, TString("b")));
        }

        AssertWriteValid(write1);
        for (auto& ack: acks) {
            ack.Wait();
        }

        auto consumer = PQLib.CreateConsumer(MakeConsumerSettings(testServer), logger, false);
        consumer->Start().Wait();

        ui64 readMessages = 0;
        while (readMessages < 1000) {
            auto read1 = consumer->GetNextMessage();
            AssertReadValid(read1);
            auto& batch = read1.GetValueSync().Response.GetData().GetMessageBatch(0);
            AssertReadContinuous(batch, readMessages + 1);
            readMessages += batch.MessageSize();
        }

        DestroyAndWait(producer);
        DestroyAndWait(consumer);
    }

    Y_UNIT_TEST(DiesOnTooManyReconnectionAttemptsWithoutWrite) {
        // Check that we reconnect even without explicit write errors
        DiesOnTooManyReconnectionAttempts(false);
    }

    Y_UNIT_TEST(DiesOnTooManyReconnectionAttemptsWithWrite) {
        DiesOnTooManyReconnectionAttempts(true);
    }

    Y_UNIT_TEST(CancelsOperationsAfterPQLibDeath) {
        return; // Test is ignored. FIX: KIKIMR-7886
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

        auto producer = PQLib->CreateProducer(MakeProducerSettings(testServer), logger, false);
        UNIT_ASSERT(!producer->Start().GetValueSync().Response.HasError());
        TFuture<TError> isDead = producer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        testServer.ShutdownServer();
        UNIT_ASSERT(!isDead.HasValue());

        auto write1 = producer->Write(1, TString("blob1"));
        auto write2 = producer->Write(2, TString("blob2"));

        UNIT_ASSERT(!write1.HasValue());
        UNIT_ASSERT(!write2.HasValue());

        PQLib = nullptr;

        UNIT_ASSERT(write1.HasValue());
        UNIT_ASSERT(write2.HasValue());

        UNIT_ASSERT(write1.GetValue().Response.HasError());
        UNIT_ASSERT(write2.GetValue().Response.HasError());

        auto write3 = producer->Write(3, TString("blob3"));
        UNIT_ASSERT(write3.HasValue());
        UNIT_ASSERT(write3.GetValue().Response.HasError());
    }

    Y_UNIT_TEST(CancelsStartAfterPQLibDeath) {
        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLibSettings pqLibSettings;
        pqLibSettings.DefaultLogger = logger;
        THolder<TPQLib> PQLib = MakeHolder<TPQLib>(pqLibSettings);

        auto producer = PQLib->CreateProducer(FakeSettings(), logger, false);
        auto start = producer->Start();
        UNIT_ASSERT(!start.HasValue());

        PQLib = nullptr;

        UNIT_ASSERT(start.HasValue());
        UNIT_ASSERT(start.GetValue().Response.HasError());

        auto dead = producer->IsDead();
        UNIT_ASSERT(dead.HasValue());
        dead.GetValueSync();

        auto write = producer->Write(1, TString("blob1"));
        UNIT_ASSERT(write.HasValue());
        UNIT_ASSERT(write.GetValueSync().Response.HasError());
    }
}
} // namespace NPersQueue
