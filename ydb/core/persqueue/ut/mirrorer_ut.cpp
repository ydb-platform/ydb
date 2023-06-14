#include "actor_persqueue_client_iface.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>


#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPersQueueTests {

Y_UNIT_TEST_SUITE(TPersQueueMirrorer) {
    Y_UNIT_TEST(TestBasicRemote) {
        NPersQueue::TTestServer server;
        const auto& settings = server.CleverServer->GetRuntime()->GetAppData().PQConfig.GetMirrorConfig().GetPQLibSettings();

        auto fabric = std::make_shared<NKikimr::NPQ::TPersQueueMirrorReaderFactory>();
        fabric->Initialize(server.CleverServer->GetRuntime()->GetAnyNodeActorSystem(), settings);
        for (ui32 nodeId = 0; nodeId < server.CleverServer->GetRuntime()->GetNodeCount(); ++nodeId) {
            server.CleverServer->GetRuntime()->GetAppData(nodeId).PersQueueMirrorReaderFactory = fabric.get();
        }

        ui32 partitionsCount = 2;
        TString srcTopic = "topic2";
        TString dstTopic = "topic1";
        TString srcTopicFullName = "rt3.dc1--" + srcTopic;
        TString dstTopicFullName = "rt3.dc1--" + dstTopic;

        server.AnnoyingClient->CreateTopic(srcTopicFullName, partitionsCount);

        NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
        mirrorFrom.SetEndpoint("localhost");
        mirrorFrom.SetEndpointPort(server.GrpcPort);
        mirrorFrom.SetTopic(srcTopic);
        mirrorFrom.SetConsumer("some_user");
        mirrorFrom.SetSyncWriteTime(true);
        //mirrorFrom.SetDatabase("/Root");
        //mirrorFrom.MutableCredentials()->SetOauthToken("test_user@" BUILTIN_ACL_DOMAIN);

        server.AnnoyingClient->CreateTopic(
            dstTopicFullName,
            partitionsCount,
            /*ui32 lowWatermark =*/ 8_MB,
            /*ui64 lifetimeS =*/ 86400,
            /*ui64 writeSpeed =*/ 20000000,
            /*TString user =*/ "",
            /*ui64 readSpeed =*/ 200000000,
            /*TVector<TString> rr =*/ {},
            /*TVector<TString> important =*/ {},
            mirrorFrom
        );
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_MIRRORER});

        auto driver = server.AnnoyingClient->GetDriver();

        {   // write to mirrored topic (must fail)
            auto writer = CreateSimpleWriter(*driver, dstTopic, "123");
            writer->GetInitSeqNo();

            auto res = writer->Write(TString(10, 'a'), 0);
            UNIT_ASSERT(res);
            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(!res);
        }

        // write to source topic
        TVector<ui32> messagesPerPartition(partitionsCount, 0);
        for (ui32 partition = 0; partition < partitionsCount; ++partition) {
            TString sourceId = "some_sourceid_" + ToString(partition);
            auto writer = CreateSimpleWriter(*driver, srcTopic, sourceId, partition + 1);
            ui64 seqNo = writer->GetInitSeqNo();

            for (ui32 i = 1; i <= 11; ++i) {
                auto res = writer->Write(TString(i, 'a'), ++seqNo);
                UNIT_ASSERT(res);
                ++messagesPerPartition[partition];
            }
            auto res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }
        {
            TVector<TString> base64sourceIds = {"base64:ZHNmc2Rm", "MTIzNDU=", "cXdlcnR5", "base64:bG9nYnJva2Vy", "base64:aa"};
            ui32 partition = 0;
            for (auto& sourceId : base64sourceIds) {
                auto writer = CreateSimpleWriter(*driver, srcTopic, sourceId, partition + 1);
                ui64 seqNo = writer->GetInitSeqNo();

                auto res = writer->Write("message with source id: " + sourceId, ++seqNo);
                UNIT_ASSERT(res);
                ++messagesPerPartition[partition];

                res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            }
        }

        auto createReader = [&](const TString& topic, ui32 partition) {
            return CreateReader(
                *driver,
                NYdb::NPersQueue::TReadSessionSettings()
                    .AppendTopics(
                        NYdb::NPersQueue::TTopicReadSettings(topic)
                            .AppendPartitionGroupIds(partition + 1)
                    )
                    .ConsumerName("shared/user")
                    .ReadOnlyOriginal(true)
                    .Decompress(false)
            );
        };

        for (ui32 partition = 0; partition < partitionsCount; ++partition) {
            auto srcReader = createReader(srcTopic, partition);
            auto dstReader = createReader(dstTopic, partition);

            for (ui32 i = 0; i < messagesPerPartition[partition]; ++i) {
                auto dstEvent = GetNextMessageSkipAssignment(dstReader);
                UNIT_ASSERT(dstEvent);
                Cerr << "Destination read message: " << dstEvent->DebugString() << "\n";
                auto srcEvent = GetNextMessageSkipAssignment(srcReader);
                UNIT_ASSERT(srcEvent);
                Cerr << "Source read message: " << srcEvent->DebugString() << "\n";

                ui64 dstPartition = dstEvent->GetPartitionStream()->GetPartitionId();
                ui64 srcPartition = srcEvent->GetPartitionStream()->GetPartitionId();
                UNIT_ASSERT_EQUAL(dstPartition, partition);
                UNIT_ASSERT_EQUAL(srcPartition, partition);

                const auto& dstMessages = dstEvent->GetCompressedMessages();
                const auto& srcMessages = srcEvent->GetCompressedMessages();

                UNIT_ASSERT_EQUAL(dstMessages.size(), srcMessages.size());

                for (size_t i = 0; i < dstMessages.size(); ++i) {
                    UNIT_ASSERT_EQUAL(srcMessages[i].GetBlocksCount(), 1);
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetBlocksCount(), 1);
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetData(), srcMessages[i].GetData());
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetCodec(), srcMessages[i].GetCodec());
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetOffset(0), srcMessages[i].GetOffset(0));
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetMessageGroupId(0), srcMessages[i].GetMessageGroupId(0));
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetSeqNo(0), srcMessages[i].GetSeqNo(0));
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetCreateTime(0), srcMessages[i].GetCreateTime(0));
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetWriteTime(0), srcMessages[i].GetWriteTime(0));
                    UNIT_ASSERT_EQUAL(dstMessages[i].GetIp(0), srcMessages[i].GetIp(0));

                    const auto& dstMeta = dstMessages[i].GetMeta(0)->Fields;
                    const auto& srcMeta = srcMessages[i].GetMeta(0)->Fields;
                    UNIT_ASSERT_VALUES_EQUAL(dstMeta.size(), srcMeta.size());
                    for (auto& item : srcMeta) {
                        UNIT_ASSERT(dstMeta.count(item.first));
                        UNIT_ASSERT_EQUAL(dstMeta.at(item.first), item.second);
                    }
                }
            }
        }
    }

}

} // NKikimr::NPersQueueTests
