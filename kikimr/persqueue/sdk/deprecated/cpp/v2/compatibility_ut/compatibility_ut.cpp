#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NThreading;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;

namespace NPersQueue {
Y_UNIT_TEST_SUITE(TCompatibilityTest) {
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

    Y_UNIT_TEST(ContinuesOperationsAfterPQLibDeath) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer();

        const size_t partitions = 1;
        testServer.AnnoyingClient->FullInit();
        testServer.AnnoyingClient->CreateTopic("rt3.dc1--topic1", partitions);

        testServer.WaitInit("topic1");

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLibSettings pqLibSettings;
        pqLibSettings.DefaultLogger = logger;
        THolder<TPQLib> PQLib = MakeHolder<TPQLib>(pqLibSettings);

        auto producer = PQLib->CreateProducer(MakeProducerSettings(testServer), logger, true);
        UNIT_ASSERT(!producer->Start().GetValueSync().Response.HasError());
        auto isProducerDead = producer->IsDead();
        UNIT_ASSERT(!isProducerDead.HasValue());

        auto consumer = PQLib->CreateConsumer(MakeConsumerSettings(testServer), logger, true);

        PQLib = nullptr;

        UNIT_ASSERT(!consumer->Start().GetValueSync().Response.HasError());
        auto isConsumerDead = consumer->IsDead();
        UNIT_ASSERT(!isConsumerDead.HasValue());

        auto write1 = producer->Write(1, TString("blob1"));
        auto write2 = producer->Write(2, TString("blob2"));

        UNIT_ASSERT(!write1.GetValueSync().Response.HasError());
        UNIT_ASSERT(!write2.GetValueSync().Response.HasError());

        UNIT_ASSERT(!consumer->GetNextMessage().GetValueSync().Response.HasError());

        producer = nullptr;
        consumer = nullptr;

        isProducerDead.GetValueSync();
        isConsumerDead.GetValueSync();
    }
}
} // namespace NPersQueue
