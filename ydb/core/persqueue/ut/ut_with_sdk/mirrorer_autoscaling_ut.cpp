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

        NYdb::NTopic::TDescribeTopicResult GetTopicDescriptionSync(const TString& name, NPersQueue::TTestServer& server) {
            NYdb::NTopic::TTopicClient topicClient{
                *server.AnnoyingClient->GetDriver()};
            const TString name0 = "/Root/PQ/" + name;

            auto descr = topicClient.DescribeTopic(name0).GetValueSync();

            if (!descr.IsSuccess()) {
                Cerr << LabeledOutput(descr.GetIssues().ToString()) << "\n";
            }
            UNIT_ASSERT(descr.IsSuccess());
            return descr;
        }

        struct TPartitionsStatistics {
            int Partitions = 0;
            int Active = 0;
        };

        TPartitionsStatistics CountPartitionsByStatus(const TString& name, NPersQueue::TTestServer& server) {
            const NYdb::NTopic::TDescribeTopicResult descr = GetTopicDescriptionSync(name, server);
            TPartitionsStatistics stat{};
            for (const NYdb::NTopic::TPartitionInfo& part : descr.GetTopicDescription().GetPartitions()) {
                stat.Partitions += 1;
                stat.Active += part.GetActive();
            }
            return stat;
        }

        TString GenSourceId(ui32 sourceId) {
            return TStringBuilder() << "source_id_" << sourceId;
        }

        void PrechargeTopic(const TString& name, NYdb::TDriver* driver, size_t sourceIdsCnt, size_t messageSize, size_t messageCount) {
            Cerr << "PRECHARGE BEGIN" << Endl;
            for (size_t sourceId = 0; sourceId < sourceIdsCnt; ++sourceId) {
                const TString sourceIdTxt = GenSourceId(sourceId);
                const std::unordered_map<std::string, std::string> sessionMeta = {
                    {"precharge", "true"},
                };
                auto writer = CreateSimpleWriter(*driver, name, sourceIdTxt, std::nullopt, std::nullopt, std::nullopt, sessionMeta);
                ui64 seqNo = writer->GetInitSeqNo();

                for (size_t i = 0; i < messageCount; ++i) {
                    TString data = TStringBuilder() << "[" << LabeledOutput(sourceIdTxt, i) << "]";
                    data *= (messageSize / data.size() + 1);
                    data.resize(messageSize);
                    auto res = writer->Write(data, ++seqNo);
                    UNIT_ASSERT(res);
                }
                auto res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            }
            Cerr << "PRECHARGE END" << Endl;
        }

        void PrintTopicDescription(const TString& name, TStringBuf stage, NPersQueue::TTestServer& server) {
            if (1) {
                Cerr << "DESCR " << stage << " " << LabeledOutput(name) << Endl;
                const NYdb::NTopic::TDescribeTopicResult descr = GetTopicDescriptionSync(name, server);
                Ydb::Topic::CreateTopicRequest req;
                descr.GetTopicDescription().SerializeTo(req);
                Cerr << LabeledOutput(req.DebugString()) << Endl;
                for (const NYdb::NTopic::TPartitionInfo& part : descr.GetTopicDescription().GetPartitions()) {
                    Cerr << "PARTITION " << LabeledOutput(part.GetPartitionId(), part.GetActive()) << Endl;
                }
            }
            if (0) {
                Cerr << "DESCR " << stage << " " << LabeledOutput(name) << Endl;
                auto descr = server.AnnoyingClient->DescribeTopic({name});
                Cerr << LabeledOutput(descr.DebugString()) << Endl;
            }
        }

        ui64 SplitPartition(ui64& txId, const TString& topicName, ui32 partitionId, const TString& boundary, TTestActorRuntime& runtime) {
            ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
            scheme.SetName(topicName);
            auto* split = scheme.AddSplit();
            split->SetPartition(partitionId);
            split->SetSplitBoundary(boundary);

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

            return e->Record.GetTxId();
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TPersQueueMirrorerWith) {
        Y_UNIT_TEST(TestBasicRemote) {
            NPersQueue::TTestServer server;

            const auto& settings = server.CleverServer->GetRuntime()->GetAppData().PQConfig.GetMirrorConfig().GetPQLibSettings();

            auto fabric = std::make_shared<NKikimr::NPQ::TPersQueueMirrorReaderFactory>();
            fabric->Initialize(server.CleverServer->GetRuntime()->GetAnyNodeActorSystem(), settings);
            for (ui32 nodeId = 0; nodeId < server.CleverServer->GetRuntime()->GetNodeCount(); ++nodeId) {
                server.CleverServer->GetRuntime()->GetAppData(nodeId).PersQueueMirrorReaderFactory = fabric.get();
            }

            NYdb::TDriver* driver = server.AnnoyingClient->GetDriver();
            NYdb::NTopic::TTopicClient topicClient(*driver);

            const ui32 sourcesCount = 8;
            const ui32 partitionsCount = 2;
            const ui32 maxPartitionsCount = 4;
            const TString srcTopic = "topic2_src";
            const TString dstTopic = "topic1_dst";
            const TString srcTopicFullName = "rt3.dc1--" + srcTopic;
            const TString dstTopicFullName = "rt3.dc1--" + dstTopic;

            NKikimrPQ::TPQTabletConfig::TPartitionStrategy partitionStrategy;
            partitionStrategy.SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
            partitionStrategy.SetMinPartitionCount(partitionsCount);
            partitionStrategy.SetMaxPartitionCount(maxPartitionsCount);
            partitionStrategy.SetScaleThresholdSeconds(2);
            partitionStrategy.SetScaleUpPartitionWriteSpeedThresholdPercent(90);
            partitionStrategy.SetScaleDownPartitionWriteSpeedThresholdPercent(1);

            const ui64 writeSpeed = 1_MB;
            const ui64 reeadSpeed = 1_MB;
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

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "2", server);
            }
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "33", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId, srcTopicFullName, 1, "\xC0", *server.CleverServer->GetRuntime());
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2); // MAYBE changes

            // write to source topic
            TVector<ui32> messagesPerPartition(partitionsCount, 0);
            THashMap<TString, size_t> messagesPerSourceId;
            for (ui32 partition = 0; partition < partitionsCount; ++partition) {
                try {
                const ui32 targetPartition = partition == 0 ? 1 : 4;
                const TString sourceId = "some_sourceid_" + ToString(partition);
                const std::unordered_map<std::string, std::string> sessionMeta = {
                    {"partition", ToString(partition)},
                };
                auto writer = CreateSimpleWriter(*driver, srcTopic, sourceId, targetPartition, std::nullopt, std::nullopt, sessionMeta);

                ui64 seqNo = writer->GetInitSeqNo();

                for (ui32 i = 1; i <= 10 + partition; ++i) {
                    auto res = writer->Write(TStringBuilder() << "[[[" << LabeledOutput(i, partition, seqNo) << "]]]", ++seqNo);
                    UNIT_ASSERT(res);
                    messagesPerPartition[partition] += 1;
                    messagesPerSourceId[sourceId] += 1;
                }
                auto res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            } catch (const std::exception& e) {
                UNIT_FAIL("Unable to write to partition " << partition << ": " << CurrentExceptionMessage());
            }
        }

            constexpr TDuration nextMessageTimeout = TDuration::Seconds(2);
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

            THashMap<TString, size_t> messagesPerSourceIdRead;
            Cerr << "DST\n";
            for (size_t partition = 0; partition < 4; ++partition) {
                auto dstReader = createReader(dstTopic, partition);

                for (;;) {
                    auto dstEvent = GetNextMessageSkipAssignment(dstReader, nextMessageTimeout);
                    if (!dstEvent) {
                        break;
                    }
                    const std::vector dstMessages =  dstEvent->GetMessages();
                    for (const auto& dstMessage : dstMessages) {
                        const auto& dstMeta = dstMessage.GetMeta()->Fields;

                        Cerr << "READ " << LabeledOutput(partition, dstMessage.GetOffset(), dstMessage.GetProducerId(), dstMessage.GetMessageGroupId(), dstMessage.GetSeqNo(), dstMessage.GetData().size(), dstMeta.contains("precharge")) << "\n";
                        if (!dstMeta.contains("precharge")) {
                            messagesPerSourceIdRead[dstMessage.GetProducerId()] += 1;
                        }
                    }
                   // dstEvent->Commit();
                }
            }

            Cerr << "SRC\n";
            for (size_t partition = 0; partition < 4; ++partition) {
                auto dstReader = createReader(srcTopic, partition);

                for (;;) {
                    auto dstEvent = GetNextMessageSkipAssignment(dstReader, nextMessageTimeout);
                    if (!dstEvent) {
                        break;
                    }
                    const std::vector dstMessages =  dstEvent->GetMessages();
                    for (const auto& dstMessage : dstMessages) {
                        const auto& dstMeta = dstMessage.GetMeta()->Fields;
                        Cerr << "READ " << LabeledOutput(partition, dstMessage.GetOffset(), dstMessage.GetProducerId(), dstMessage.GetMessageGroupId(), dstMessage.GetSeqNo(), dstMessage.GetData().size(), dstMeta.contains("precharge")) << "\n";
                    }
                   // dstEvent->Commit();
                }
            }


            TSet<TString> ids;
            for (const auto& [id, count] : messagesPerSourceId) {
                ids.insert(id);
            }
            for (const auto& [id, count] : messagesPerSourceIdRead) {
                ids.insert(id);
            }

            for (const auto& id : ids) {
                TString log = TStringBuilder() << "ID '" << id << "': " << messagesPerSourceId.Value(id, 0) << " == " << messagesPerSourceIdRead.Value(id, 0) << "\n";
                Cerr << log;
            }
            for (const auto& id : ids) {
                UNIT_ASSERT_VALUES_EQUAL_C(messagesPerSourceId.Value(id, 0), messagesPerSourceIdRead.Value(id, 0), id);
            }



            return;
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
                        // Cerr << "SRC " << srcMessages[j].GetMessageGroupId() << " DST " << dstMessages[j].GetMessageGroupId() << "\n";

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
