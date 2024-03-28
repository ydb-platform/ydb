#include "actor_persqueue_client_iface.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/data_plane_helpers.h>


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
        server.CleverServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicMessageMeta(true);

        auto driver = server.AnnoyingClient->GetDriver();

        using namespace NYdb;

        {   // write to mirrored topic (must fail)
            auto writer = CreateSimpleWriter(*driver, dstTopic, "123");
            writer->GetInitSeqNo();

            auto res = writer->Write(TString(10, 'a'), 0);
            UNIT_ASSERT(res);
            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(!res);
        }

        {
            auto channel = grpc::CreateChannel(
                    "localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials()
            );
            auto topicStub = Ydb::Topic::V1::TopicService::NewStub(channel);
            grpc::ClientContext rcontext;
            auto writeSession = topicStub->StreamWrite(&rcontext);
            UNIT_ASSERT(writeSession);
            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;
            auto* init = req.mutable_init_request();
            init->set_path(srcTopic);
            init->set_producer_id("directRpcSession");
            init->set_message_group_id("directRpcSession");
            if (!writeSession->Write(req)) {
                UNIT_FAIL("Grpc write fail");
            }
            UNIT_ASSERT(writeSession->Read(&resp));
            Cerr << "Write session init response: " << resp.DebugString() << Endl;
            req = Ydb::Topic::StreamWriteMessage::FromClient{};
            auto* write = req.mutable_write_request();
            write->set_codec(1);
            auto* msg = write->add_messages();
            msg->set_seq_no(1);
            msg->set_data("data");
            msg->mutable_created_at()->set_seconds(1000);
            {
                auto* meta = msg->add_metadata_items();
                meta->set_key("meta-key");
                meta->set_value("meta-value");
            };
            {
                auto* meta = msg->add_metadata_items();
                meta->set_key("meta-key2");
                meta->set_value("meta-value2");
            };

            if (!writeSession->Write(req)) {
                UNIT_FAIL("Grpc write fail");
            }
            UNIT_ASSERT(writeSession->Read(&resp));
            Cerr << "Write session write response: " << resp.DebugString() << Endl;

        }
        auto createTopicReader = [&](const TString& topic) {
            auto settings = NTopic::TReadSessionSettings()
                    .AppendTopics(NTopic::TTopicReadSettings(topic))
                    .ConsumerName("shared/user")
                    .Decompress(false);

            return NTopic::TTopicClient(*driver).CreateReadSession(settings);
        };
        auto getMessagesFromTopic = [&](auto& reader) {
            TMaybe<NTopic::TReadSessionEvent::TDataReceivedEvent> result;
            while (true) {
                auto event = reader->GetEvent(true);
                if (!event)
                    return result;
                if (auto dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    result = *dataEvent;
                    break;
                } else if (auto *lockEv = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    lockEv->Confirm();
                } else if (auto *releaseEv = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                    releaseEv->Confirm();
                } else if (auto *closeSessionEvent = std::get_if<NTopic::TSessionClosedEvent>(&*event)) {
                    return result;
                }
            }
            return result;
        };
        auto srcReader = createTopicReader(srcTopic);
        auto dstReader = createTopicReader(dstTopic);
        auto dstEvent = getMessagesFromTopic(dstReader);
        UNIT_ASSERT(dstEvent);
        Cerr << "Destination read message: " << dstEvent->DebugString() << "\n";
        auto srcEvent = getMessagesFromTopic(srcReader);
        UNIT_ASSERT(srcEvent);
        Cerr << "Source read message: " << srcEvent->DebugString() << "\n";
        const auto& dstMessages = dstEvent->GetCompressedMessages();
        const auto& srcMessages = srcEvent->GetCompressedMessages();
        UNIT_ASSERT(srcMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(srcMessages.size(), dstMessages.size());
        for (auto i = 0u; i < srcMessages.size(); i++) {
            UNIT_ASSERT_VALUES_EQUAL(dstMessages[i].GetMessageMeta()->Fields.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(srcMessages[i].GetMessageMeta()->Fields, dstMessages[i].GetMessageMeta()->Fields);

        }

        srcReader->Close(TDuration::Zero());
        dstReader->Close(TDuration::Zero());

        // write to source topic
        TVector<ui32> messagesPerPartition(partitionsCount, 0);
        for (ui32 partition = 0; partition < partitionsCount; ++partition) {
            TString sourceId = "some_sourceid_" + ToString(partition);
            THashMap<TString, TString> sessionMeta = {
                {"some_extra_field", "some_value"},
                {"some_extra_field2", "another_value" + ToString(partition)},
                {"file", "/home/user/log" + ToString(partition)}
            };
            auto writer = CreateSimpleWriter(*driver, srcTopic, sourceId, partition + 1, std::nullopt, std::nullopt, sessionMeta);

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
                auto dstEvent = GetNextMessageSkipAssignment(dstReader, TDuration::Seconds(1));
                UNIT_ASSERT(dstEvent);
                Cerr << "Destination read message: " << dstEvent->DebugString() << "\n";
                auto srcEvent = GetNextMessageSkipAssignment(srcReader, TDuration::Seconds(1));
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
                    Cerr << "SRC " << srcMessages[i].GetMessageGroupId(0) << " DST " << dstMessages[i].GetMessageGroupId(0) << "\n";
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

    Y_UNIT_TEST(ValidStartStream) {
        using namespace NYdb::NTopic;

        NPersQueue::TTestServer server;
        TString topic = "topic1";
        TString topicFullName = "rt3.dc1--" + topic;

        server.AnnoyingClient->CreateTopic(topicFullName, 1);

        auto driver = server.AnnoyingClient->GetDriver();
        auto writer = CreateSimpleWriter(*driver, topic, "src-id-test");
        for (auto i = 0u; i < 5; i++) {
            auto res = writer->Write(TString(10, 'a'));
            UNIT_ASSERT(res);
        }

        auto createTopicReader = [&](const TString& topic) {
            auto settings = TReadSessionSettings()
                    .AppendTopics(TTopicReadSettings(topic))
                    .ConsumerName("shared/user")
                    .Decompress(false);

            return TTopicClient(*driver).CreateReadSession(settings);
        };
        auto reader = createTopicReader(topic);
        ui64 messagesGot = 0;
        while(true) {
            auto event = reader->GetEvent(true);
            UNIT_ASSERT(event);
            if (auto* dataEvent = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (auto msg : dataEvent->GetCompressedMessages()) {
                    msg.Commit();
                    messagesGot++;
                }
                if (messagesGot == 5) {
                    reader->Close();
                }
            } else if (auto* lockEv = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    lockEv->Confirm();
            } else if (auto* releaseEv = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                releaseEv->Confirm();
            } else if (auto* closeSessionEvent = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(messagesGot, 5);
                break;
            }
        }

        for (auto i = 0u; i < 5; i++) {
            auto res = writer->Write(TString(10, 'b'));
            UNIT_ASSERT(res);
        }

        auto res = writer->Close(TDuration::Seconds(10));
        UNIT_ASSERT(res);

        reader = createTopicReader(topic);
        bool gotData = false;
        while(!gotData) {
            auto event = reader->GetEvent(true);
            UNIT_ASSERT(event);
            if (auto dataEvent = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                gotData = true;
            } else if (auto* lockEv = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    lockEv->Confirm(5);
            } else if (auto* closeSessionEvent = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closeSessionEvent->DebugString());
                break;
            }
        }
    }
}
} // NKikimr::NPersQueueTests
