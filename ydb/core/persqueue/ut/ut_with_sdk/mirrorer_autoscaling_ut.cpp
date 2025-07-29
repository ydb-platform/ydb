#include "actor_persqueue_client_iface.h"
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/data_plane_helpers.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPersQueueTests {

    namespace {
        static constexpr ui64 SS = 72057594046644480ull;
        NKikimrSchemeOp::TModifyScheme CreateTransaction(const TString& parentPath, ::NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
            tx.SetWorkingDir(parentPath);
            tx.MutableAlterPersQueueGroup()->CopyFrom(scheme);
            return tx;
        }

        NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction* CreateRequest(ui64 txId, NKikimrSchemeOp::TModifyScheme&& tx) {
            auto ev = new NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction(txId, SS);
            *ev->Record.AddTransaction() = std::move(tx);
            return ev;
        }


        void PrintTopicDescription(const TString& name, TStringBuf stage, NPersQueue::TTestServer& server) {
            if (0) {
                NYdb::NTopic::TTopicClient topicClient{*server.AnnoyingClient->GetDriver()};
                const TString name0 = "/Root/PQ/" + name;
                Cerr << "DESCR " << stage << " " << LabeledOutput(name0) << Endl;
                auto descr = topicClient.DescribeTopic(name0).GetValueSync();
                Ydb::Topic::CreateTopicRequest req;
                descr.GetTopicDescription().SerializeTo(req);
                Cerr << LabeledOutput(descr.GetIssues().ToString()) << "\n";
                Cerr << LabeledOutput(req.DebugString()) << Endl;
                Cerr << LabeledOutput(name0, descr.GetTopicDescription().GetRetentionPeriod()) << Endl;
                //
            }
            if (1) {
                Cerr << "DESCR " << stage << " " << LabeledOutput(name) << Endl;
                auto descr = server.AnnoyingClient->DescribeTopic({name});
                Cerr << LabeledOutput(descr.DebugString()) << Endl;
            }
        }
    } // namespace

    Y_UNIT_TEST_SUITE(TPersQueueMirrorerWith) {
        Y_UNIT_TEST(TestBasicRemote) {
#if 0
        auto createServer = []() {
            auto settings = NKikimr::NPersQueueTests::PQSettings();
            NPersQueue::TTestServer server(settings);
            server.StartServer(true, Nothing());
            return server;
        };
        NPersQueue::TTestServer server = createServer();
#else
            NPersQueue::TTestServer server;
#endif

            const auto& settings = server.CleverServer->GetRuntime()->GetAppData().PQConfig.GetMirrorConfig().GetPQLibSettings();

            auto fabric = std::make_shared<NKikimr::NPQ::TPersQueueMirrorReaderFactory>();
            fabric->Initialize(server.CleverServer->GetRuntime()->GetAnyNodeActorSystem(), settings);
            for (ui32 nodeId = 0; nodeId < server.CleverServer->GetRuntime()->GetNodeCount(); ++nodeId) {
                server.CleverServer->GetRuntime()->GetAppData(nodeId).PersQueueMirrorReaderFactory = fabric.get();
            }

            auto* driver = server.AnnoyingClient->GetDriver();
            NYdb::NTopic::TTopicClient topicClient(*driver);

            ui32 partitionsCount = 2;
            ui32 maxPartitionsCount = 4;
            TString srcTopic = "topic2_src";
            TString dstTopic = "topic1_dst";
            TString srcTopicFullName = "rt3.dc1--" + srcTopic;
            TString dstTopicFullName = "rt3.dc1--" + dstTopic;

            NKikimrPQ::TPQTabletConfig::TPartitionStrategy partitionStrategy;
            partitionStrategy.SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
            partitionStrategy.SetMinPartitionCount(partitionsCount);
            partitionStrategy.SetMaxPartitionCount(maxPartitionsCount);
            partitionStrategy.SetScaleThresholdSeconds(2);
            partitionStrategy.SetScaleUpPartitionWriteSpeedThresholdPercent(1);
            partitionStrategy.SetScaleDownPartitionWriteSpeedThresholdPercent(1);

            const ui64 writeSpeed = 1_KB;
            const ui64 reeadSpeed = 1_KB;
            server.AnnoyingClient->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/reeadSpeed,
                /*TVector<TString> rr =*/{},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(server.GrpcPort);
            mirrorFrom.SetTopic(srcTopic);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            server.AnnoyingClient->CreateTopic(
                dstTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/reeadSpeed,
                /*TVector<TString> rr =*/{},
                /*TVector<TString> important =*/{},
                mirrorFrom,
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);
            server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_MIRRORER});
            server.CleverServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicMessageMeta(true);

            Cerr << "PRECHARGE BEGIN" << Endl;
            for (int sourceId = 0; sourceId < 4; ++sourceId) {
                const TString sourceIdTxt = TStringBuilder() << "precharge_" << sourceId;
                const std::unordered_map<std::string, std::string> sessionMeta = {
                    {"precharge", "true"},
                };
                auto writer = CreateSimpleWriter(*driver, srcTopic, sourceIdTxt, std::nullopt, std::nullopt, std::nullopt, sessionMeta);
                ui64 seqNo = writer->GetInitSeqNo();
                for (ui32 i = 0; i < 1; ++i) {
                    TString data = TString(1_MB, 'a');
                    auto res = writer->Write(data, ++seqNo);
                    UNIT_ASSERT(res);
                }
                auto res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            }
            Cerr << "PRECHARGE END" << Endl;
            Sleep(TDuration::Seconds(20));

            for (TString n0 : {srcTopicFullName, dstTopicFullName})
            {
                const TString name = "/Root/PQ/" + n0;
                Cerr << "DESCR 1 " << LabeledOutput(name) << Endl;
                auto descr = topicClient.DescribeTopic(name).GetValueSync();

                Ydb::Topic::CreateTopicRequest req;
                descr.GetTopicDescription().SerializeTo(req);
                Cerr << LabeledOutput(descr.GetIssues().ToString()) << "\n";
                Cerr << LabeledOutput(req.DebugString()) << Endl;
                Cerr << LabeledOutput(name, descr.GetTopicDescription().GetRetentionPeriod()) << Endl;
                //
            }

            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "2", server);
            }


            {
                ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
                scheme.SetName(srcTopicFullName);
                auto* split = scheme.AddSplit();
                split->SetPartition(1);
                split->SetSplitBoundary("\xC0");

                if (1)
                {
                    static constexpr ui64 txId = 1006;
                    auto& runtime = *server.CleverServer->GetRuntime();
                    Sleep(TDuration::Seconds(1));

                    Cerr << "ALTER_SCHEME: " << scheme << Endl << Flush;

                    const auto sender = runtime.AllocateEdgeActor();
                    const auto request = CreateRequest(txId, CreateTransaction("/Root/PQ", scheme));
                    runtime.Send(new IEventHandle(
                                     MakeTabletResolverID(),
                                     sender,
                                     new TEvTabletResolver::TEvForward(
                                         SS,
                                         new IEventHandle(TActorId(), sender, request),
                                         {},
                                         TEvTabletResolver::TEvForward::EActor::Tablet)),
                                 0);

                    auto subscriber = NSchemeShardUT_Private::CreateNotificationSubscriber(runtime, SS);
                    runtime.Send(new IEventHandle(subscriber, sender, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(txId)));
                    TAutoPtr<IEventHandle> handle;
                    auto event = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
                    UNIT_ASSERT(event);
                    UNIT_ASSERT_EQUAL(event->Record.GetTxId(), txId);

                    auto e = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
                    UNIT_ASSERT_EQUAL_C(e->Record.GetStatus(), NSchemeShard::TEvSchemeShard::EStatus::StatusAccepted,
                                        "Unexpected status " << NKikimrScheme::EStatus_Name(e->Record.GetStatus()) << " " << e->Record.GetReason());

                    Sleep(TDuration::Seconds(1));

                    // return e->Record.GetTxId();
                }
            }

            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }

            // write to source topic
            TVector<ui32> messagesPerPartition(partitionsCount, 0);
            for (ui32 partition = 0; partition < partitionsCount; ++partition) {
                TString sourceId = "some_sourceid_" + ToString(partition);
                std::unordered_map<std::string, std::string> sessionMeta = {
                    {"some_extra_field", "some_value"},
                    {"some_extra_field2", "another_value" + std::to_string(partition)},
                    {"file", "/home/user/log" + std::to_string(partition)}};
                auto writer = CreateSimpleWriter(*driver, srcTopic, sourceId, partition + 1, std::nullopt, std::nullopt, sessionMeta);

                ui64 seqNo = writer->GetInitSeqNo();

                for (ui32 i = 1; i <= 16 + partition; ++i) {
                    auto res = writer->Write(TStringBuilder() << "[[[" << LabeledOutput(i, partition, seqNo) << "]]]", ++seqNo);
                    UNIT_ASSERT(res);
                    ++messagesPerPartition[partition];
                }
                auto res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            }

            constexpr TDuration nextMessageTimeout = TDuration::Seconds(3);

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .ConsumerName("shared/user")
                        // .ReadOnlyOriginal(false)
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            for (ui32 partition = 0; partition < partitionsCount; ++partition) {
                auto srcReader = createReader(srcTopic, partition);
                auto dstReader = createReader(dstTopic, partition);

                for (ui32 i = 0; i < messagesPerPartition[partition]; ++i) {
                    auto dstEvent = GetNextMessageSkipAssignment(dstReader, nextMessageTimeout);
                    UNIT_ASSERT_C(dstEvent, LabeledOutput(partition, i));
                    Cerr << "Destination read message: " << dstEvent->DebugString() << "\n";
                    auto srcEvent = GetNextMessageSkipAssignment(srcReader, nextMessageTimeout);
                    UNIT_ASSERT_C(srcEvent, LabeledOutput(partition, i));
                    Cerr << "Source read message: " << srcEvent->DebugString() << "\n";

                    const auto& dstMessages = dstEvent->GetMessages();
                    const auto& srcMessages = srcEvent->GetMessages();

                    for (size_t j = 0; j < dstMessages.size(); ++j) {
                        Cerr << "DST " << LabeledOutput(i, j) << "\n";
                        Cerr << dstMessages[j].DebugString(true) << "\n";
                    }
                    for (size_t j = 0; j < srcMessages.size(); ++j) {
                        Cerr << "SRC " << LabeledOutput(i, j) << "\n";
                        Cerr << srcMessages[j].DebugString(true) << "\n";
                    }
                    Cerr << "\n";

                    UNIT_ASSERT_EQUAL(dstMessages.size(), srcMessages.size());
                    UNIT_ASSERT_VALUES_EQUAL(dstMessages.size(), 1);

                    for (size_t j = 0; j < dstMessages.size(); ++j) {
                        Cerr << "SRC " << srcMessages[j].GetMessageGroupId() << " DST " << dstMessages[j].GetMessageGroupId() << "\n";

                        UNIT_ASSERT_VALUES_EQUAL_C(dstMessages[j].GetData(), srcMessages[j].GetData(), LabeledOutput(i, j));

                        UNIT_ASSERT_VALUES_EQUAL_C(dstMessages[j].GetOffset(), srcMessages[j].GetOffset(), LabeledOutput(i, j));
                        UNIT_ASSERT_VALUES_EQUAL_C(dstMessages[j].GetMessageGroupId(), srcMessages[j].GetMessageGroupId(), LabeledOutput(i, j));
                        UNIT_ASSERT_VALUES_EQUAL_C(dstMessages[j].GetSeqNo(), srcMessages[j].GetSeqNo(), LabeledOutput(i, j));
                        UNIT_ASSERT_VALUES_EQUAL_C(dstMessages[j].GetCreateTime(), srcMessages[j].GetCreateTime(), LabeledOutput(i, j));
                        UNIT_ASSERT_VALUES_EQUAL_C(dstMessages[j].GetWriteTime(), srcMessages[j].GetWriteTime(), LabeledOutput(i, j));

                        const auto& dstMeta = dstMessages[j].GetMeta()->Fields;
                        const auto& srcMeta = srcMessages[j].GetMeta()->Fields;
                        UNIT_ASSERT_VALUES_EQUAL(dstMeta.size(), srcMeta.size());
                        for (auto& item : srcMeta) {
                            UNIT_ASSERT(dstMeta.count(item.first));
                            UNIT_ASSERT_EQUAL(dstMeta.at(item.first), item.second);
                        }
                    }
                }
            }
        }
    } // Y_UNIT_TEST_SUITE(TPersQueueMirrorerWith)

} // namespace NKikimr::NPersQueueTests
