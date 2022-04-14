#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NThreading;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;

namespace NPersQueue {
Y_UNIT_TEST_SUITE(TProcessorTest) {
    TConsumerSettings MakeConsumerSettings(const TString& clientId, const TVector<TString>& topics,
                                           const TTestServer& testServer) {
        TConsumerSettings consumerSettings;
        consumerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        consumerSettings.ClientId = clientId;
        consumerSettings.Topics = topics;
        consumerSettings.UseLockSession = false;
        return consumerSettings;
    }

    TProducerSettings MakeProducerSettings(const TString& topic, const TTestServer& testServer) {
        TProducerSettings producerSettings;
        producerSettings.ReconnectOnFailure = true;
        producerSettings.Topic = topic;
        producerSettings.SourceId = "123";
        producerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        producerSettings.Codec = ECodec::RAW;
        return producerSettings;
    }

    TProcessorSettings MakeProcessorSettings(const TTestServer& testServer) {
        TProcessorSettings processorSettings;
        processorSettings.ConsumerSettings = MakeConsumerSettings("processor", {"input"}, testServer);
        processorSettings.ProducerSettings = MakeProducerSettings("output", testServer);
        return processorSettings;
    }

    void AssertWriteValid(const NThreading::TFuture<TProducerCommitResponse>& respFuture) {
        const TProducerCommitResponse& resp = respFuture.GetValueSync();
        UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TWriteResponse::kAck, "Msg: " << resp.Response);
    }

    void ProcessMessage(TOriginData& originData) {
        for (auto& messages: originData.Messages) {
            for (auto& message: messages.second) {
                auto msg = message.Message;

                TVector<TProcessedMessage> processedMessages;
                for (int i = 0; i < 3; ++i) {
                    TStringBuilder sourceIdPrefix;
                    sourceIdPrefix << "shard" << i << "_";
                    processedMessages.push_back(TProcessedMessage {"output", ToString(msg.GetOffset()), sourceIdPrefix, 0});
                }
                message.Processed.SetValue(TProcessedData{processedMessages});
            }
        }
    }

    bool IsFinished(const TTestServer& testServer) {
        auto clientInfo = testServer.AnnoyingClient->GetClientInfo({"rt3.dc1--input"}, "processor", true);
        auto topicResult = clientInfo.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult(0).GetPartitionResult();
        for (auto& partition: topicResult) {
            if (partition.GetClientOffset() < partition.GetEndOffset()) return false;
        }
        return true;
    }

    void BaseFunctionalityTest() {
        TTestServer testServer(false);
        testServer.StartServer(false);

        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--input", 2);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--output", 4);

        auto producerSettings = MakeProducerSettings("input", testServer);
        auto producer = testServer.PQLib->CreateProducer(producerSettings, testServer.PQLibSettings.DefaultLogger, false);
        producer->Start();

        for (int i = 1; i < 4; ++i) {
            auto write1 = producer->Write(i, TString("blob"));
            AssertWriteValid(write1);
        }

        auto processorSettings = MakeProcessorSettings(testServer);
        auto processor = testServer.PQLib->CreateProcessor(processorSettings, testServer.PQLibSettings.DefaultLogger, false);

        while (!IsFinished(testServer)) {
            auto data = processor->GetNextData();
            data.Subscribe([](const auto& f) {
                auto value = f.GetValueSync();
                ProcessMessage(value);
            });
            Sleep(TDuration::MilliSeconds(100));
        }

        auto consumerSettings = MakeConsumerSettings("checker", {"output"}, testServer);
        auto consumer = testServer.PQLib->CreateConsumer(consumerSettings, nullptr, false);
        auto isStarted = consumer->Start().ExtractValueSync();
        UNIT_ASSERT_C(isStarted.Response.HasInit(), "cannot start consumer " << isStarted.Response);

        ui32 totalMessages = 9;
        ui32 currentMessages = 0;
        THashMap<TString, ui32> expectedSeqno = {
            {"shard0_rt3.dc1--input:0_output", 1}, {"shard1_rt3.dc1--input:0_output", 1}, {"shard2_rt3.dc1--input:0_output", 1},
            {"shard0_rt3.dc1--input:1_output", 1}, {"shard1_rt3.dc1--input:1_output", 1}, {"shard2_rt3.dc1--input:1_output", 1},
            {"shard0_rt3.dc1--input:2_output", 1}, {"shard1_rt3.dc1--input:2_output", 1}, {"shard2_rt3.dc1--input:2_output", 1},
            {"shard0_rt3.dc1--input:3_output", 1}, {"shard1_rt3.dc1--input:3_output", 1}, {"shard2_rt3.dc1--input:3_output", 1},
        };
        while (currentMessages < totalMessages) {
            auto f = consumer->GetNextMessage();
            for (auto& batch: f.GetValueSync().Response.GetData().GetMessageBatch()) {
                for (auto& message: batch.GetMessage()) {
                    TString sourceId = message.GetMeta().GetSourceId();
                    ui32 seqNo = message.GetMeta().GetSeqNo();
                    UNIT_ASSERT_EQUAL(ToString(seqNo - 1), message.GetData());
                    auto it = expectedSeqno.find(sourceId);
                    UNIT_ASSERT_UNEQUAL_C(it, expectedSeqno.end(), "unknown sourceId " << sourceId);
                    UNIT_ASSERT_EQUAL(it->second, seqNo);
                    it->second++;
                    currentMessages += 1;
                }
            }
        }
    }

    Y_UNIT_TEST(BasicFunctionality) {
        BaseFunctionalityTest();
    }
}
}
