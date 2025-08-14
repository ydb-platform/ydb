#include <ydb/core/persqueue/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/events/internal.h>

#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/data_plane_helpers.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

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

            Cerr << (TStringBuilder() << "ALTER_SCHEME: " << scheme << "\n") << Flush;

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

        std::tuple<TString> WriteToPartition(const ui32 partition, NYdb::TDriver& driver, const TString& srcTopic, const ui32 messagesCount) try {
            const ui32 targetPartitionGroup = partition + 1;
            const TString sourceId = "some_sourceid_" + ToString(partition);
            const std::unordered_map<std::string, std::string> sessionMeta = {
                {"partition", ToString(partition)},
            };
            auto writer = CreateSimpleWriter(driver, srcTopic, sourceId, targetPartitionGroup, std::nullopt, std::nullopt, sessionMeta);

            ui64 seqNo = writer->GetInitSeqNo();

            for (ui32 i = 1; i <= messagesCount; ++i) {
                auto res = writer->Write(TStringBuilder() << "[[[" << LabeledOutput(i, partition, seqNo) << "]]]", ++seqNo);
                UNIT_ASSERT(res);
            }
            auto res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
            return std::make_tuple(sourceId);
        } catch (const std::exception& e) {
            UNIT_FAIL("Unable to write to partition " << partition << ": " << CurrentExceptionMessage());
            throw;
        }

        TMaybe<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent> GetNextDataMessage(const std::shared_ptr<NYdb::NTopic::IReadSession>& reader, TInstant deadline) {
            while (true) {
                auto future = reader->WaitEvent();
                future.Wait(deadline);
                std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> event = reader->GetEvent(false, 1);
                if (!event) {
                    return {};
                }
                if (auto e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    return *e;
                } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    e->Confirm();
                } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                    e->Confirm();
                } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                    return {};
                }
            }
        }

        TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> ReadMessages(auto&& reader, auto&& passFilter, bool commit, TInstant deadline, TDuration nextMessageTimeout, TStringBuf description) {
            TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> result;
            TInstant prevMessage = TInstant::Max();
            TInstant firstMessage = TInstant::Max();
            bool noData = true;
            for (;;) {
                const TInstant eventDeadline = Max(Min(deadline, prevMessage + nextMessageTimeout), noData ? TInstant::Zero() : firstMessage + nextMessageTimeout);
                TInstant now = TInstant::Now();
                Cerr << (TStringBuilder() << "WAIT READ " << LabeledOutput(description, eventDeadline - now) << "\n") << Flush;
                auto event = GetNextDataMessage(reader, eventDeadline);
                if (!event) {
                    break;
                }
                std::vector messages = event->GetMessages();
                for (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message : messages) {
                    Cerr << (TStringBuilder() << "READ " << LabeledOutput(description, message.GetOffset(), message.GetProducerId(), message.GetMessageGroupId(), message.GetSeqNo(), message.GetData().size(), message.GetMeta()->Fields.contains("precharge")) << "\n") << Flush;
                    if (!passFilter(message)) {
                        continue;
                    }
                    prevMessage = TInstant::Now();
                    if (std::exchange(noData, false)) {
                        firstMessage = TInstant::Now();
                    }
                    result.push_back(std::move(message));
                }
                if (commit) {
                    event->Commit();
                }
            }
            return result;
        }

        bool SkipPrecharge(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            const auto& meta = message.GetMeta()->Fields;
            return !meta.contains("precharge");
        }

        void ComparePartitions(auto&& srcReader, auto&& dstReader, TStringBuf descr, TInstant deadline) {
            const TDuration nextMessageTimeout = TDuration::Seconds(3);
            TVector srcMessages = ReadMessages(srcReader, SkipPrecharge, false, deadline, nextMessageTimeout, TString::Join("srcTopic ", descr));
            TVector dstMessages = ReadMessages(dstReader, SkipPrecharge, false, deadline, nextMessageTimeout, TString::Join("dstTopic ", descr));

            for (size_t j = 0; j < Min(dstMessages.size(), srcMessages.size()); ++j) {
                const TString caseDescription = TStringBuilder() << LabeledOutput(descr, j);
                const auto& dstMessage = dstMessages[j];
                const auto& srcMessage = srcMessages[j];
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessage.GetData(), srcMessage.GetData(), caseDescription);
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessage.GetOffset(), srcMessage.GetOffset(), caseDescription);
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessage.GetMessageGroupId(), srcMessage.GetMessageGroupId(), caseDescription);
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessage.GetSeqNo(), srcMessage.GetSeqNo(), caseDescription);
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessage.GetCreateTime(), srcMessage.GetCreateTime(), caseDescription);
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessage.GetWriteTime(), srcMessage.GetWriteTime(), caseDescription);
                const auto& dstMeta = dstMessage.GetMeta()->Fields;
                const auto& srcMeta = srcMessage.GetMeta()->Fields;
                UNIT_ASSERT_VALUES_EQUAL(dstMeta.size(), srcMeta.size());
                for (auto& item : srcMeta) {
                    UNIT_ASSERT(dstMeta.count(item.first));
                    UNIT_ASSERT_EQUAL(dstMeta.at(item.first), item.second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(dstMessages.size(), srcMessages.size(), LabeledOutput(descr));
            Cerr << (TStringBuilder() << descr << " has " << dstMessages.size() << " messages\n");
        }

        struct TWriteCount {
            ui32 Partiton;
            ui32 Count;
        };

        class TTestContext {
        public:
            TTestContext()
                : Fabric(std::make_shared<NKikimr::NPQ::TPersQueueMirrorReaderFactory>())
                , PQSettings{[]() {
                    auto pqSettings = NKikimr::NPersQueueTests::PQSettings(0, 2);
                    pqSettings.PQConfig.MutableCompactionConfig()->SetBlobsCount(0);
                    return pqSettings;
                }()}
                , Server(PQSettings)
            {
                const auto& settings = Server.CleverServer->GetRuntime()->GetAppData().PQConfig.GetMirrorConfig().GetPQLibSettings();

                Fabric->Initialize(Server.CleverServer->GetRuntime()->GetAnyNodeActorSystem(), settings);
                for (ui32 nodeId = 0; nodeId < Server.CleverServer->GetRuntime()->GetNodeCount(); ++nodeId) {
                    Server.CleverServer->GetRuntime()->GetAppData(nodeId).PersQueueMirrorReaderFactory = Fabric.get();
                }

                Server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_MIRRORER});
                Server.CleverServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicMessageMeta(true);
            }

            NYdb::TDriver* Driver() {
                return Server.AnnoyingClient->GetDriver();
            }

            NKikimr::NPersQueueTests::TFlatMsgBusPQClient* AnnoyingClient() {
                return &*Server.AnnoyingClient;
            }

            auto* Runtime() {
                return Server.CleverServer->GetRuntime();
            }

        public:
            std::shared_ptr<NKikimr::NPQ::TPersQueueMirrorReaderFactory> Fabric;
            Tests::TServerSettings PQSettings;
            NPersQueue::TTestServer Server;
            // NYdb::TDriver* Driver;

            const TString srcTopic = "topic2_src";
            const TString dstTopic = "topic1_dst";
            const TString srcTopicFullName = "rt3.dc1--" + srcTopic;
            const TString dstTopicFullName = "rt3.dc1--" + dstTopic;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(TPersQueueMirrorerWithScaling) {
        Y_UNIT_TEST(TestBasicRemote) {
            TTestContext ctx;
            NYdb::TDriver* driver = ctx.Driver();
            NYdb::NTopic::TTopicClient topicClient(*driver);
            auto& server = ctx.Server;

            const ui32 sourcesCount = 8;
            const ui32 partitionsCount = 2;
            const ui32 maxPartitionsCount = 4;
            const ui32 finalPartitionsCount = 4;
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
            ctx.AnnoyingClient()->CreateTopic(
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
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic(srcTopic);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
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

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "2", server);
            }
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "3", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId, srcTopicFullName, 1, "\xC0", *ctx.Runtime());
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 3);

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, finalPartitionsCount);

            // write to source topic
            TMap<ui32, ui32> messagesPerPartition;
            TMap<TString, size_t> messagesPerSourceId;
            constexpr TWriteCount writeBatch[]{
                {0, 10},
                {2, 15},
                {3, 5},
            };
            for (const TWriteCount& wc : writeBatch) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId] = WriteToPartition(wc.Partiton, *driver, srcTopic, wc.Count);
                messagesPerPartition[wc.Partiton] += wc.Count;
                messagesPerSourceId[sourceId] += wc.Count;
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .ConsumerName("shared/user")
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const TInstant deadline = TDuration::Seconds(10).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopic, partition), createReader(dstTopic, partition), TStringBuilder() << LabeledOutput(partition), deadline);
            }
        }

        Y_UNIT_TEST(TestQuotaLimitedGrow) {
            TTestContext ctx;
            NYdb::TDriver* driver = ctx.Driver();
            NYdb::NTopic::TTopicClient topicClient(*driver);
            auto& server = ctx.Server;

            const ui32 sourcesCount = 8;
            const ui32 partitionsCount = 2;
            const ui32 maxPartitionsCount = 4;
            const ui32 finalPartitionsCount = 4;
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

            NKikimrPQ::TPQTabletConfig::TPartitionStrategy partitionStrategyLimited = partitionStrategy;
            partitionStrategyLimited.SetMaxPartitionCount(2); // test case

            const ui64 writeSpeed = 1_MB;
            const ui64 reeadSpeed = 1_MB;
            ctx.AnnoyingClient()->CreateTopic(
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
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic(srcTopic);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
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
                partitionStrategyLimited);

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "2", server);
            }
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "3", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId, srcTopicFullName, 1, "\xC0", *ctx.Runtime());
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2); // not splitted yet

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, 2); // not splitted yet

            // write to source topic
            TMap<ui32, ui32> messagesPerPartition;
            TMap<TString, size_t> messagesPerSourceId;

            constexpr TWriteCount writeBatch[]{
                {0, 10},
                {2, 15},
                {3, 5},
            };
            for (const TWriteCount& wc : writeBatch) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId] = WriteToPartition(wc.Partiton, *driver, srcTopic, wc.Count);
                messagesPerPartition[wc.Partiton] += wc.Count;
                messagesPerSourceId[sourceId] += wc.Count;
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .ConsumerName("shared/user")
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2); // not splitted yet

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, 2); // not splitted yet

            auto alterStrategy = NYdb::NTopic::EAutoPartitioningStrategy::ScaleUpAndDown;

            NYdb::NTopic::TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                .MinActivePartitions(partitionsCount)
                .MaxActivePartitions(maxPartitionsCount)
                .BeginAlterAutoPartitioningSettings()
                .DownUtilizationPercent(90)
                .UpUtilizationPercent(1)
                .StabilizationWindow(TDuration::Seconds(2))
                .Strategy(alterStrategy)
                .EndAlterAutoPartitioningSettings()
                .EndAlterTopicPartitioningSettings();

            auto alterResult = topicClient.AlterTopic("/Root/PQ/" + dstTopicFullName, alterSettings).GetValueSync();

            if (!alterResult.IsSuccess()) {
                Cerr << "ALTER RES " << LabeledOutput(alterResult.GetIssues().ToString()) << "\n";
            }

            const TInstant deadline = TDuration::Seconds(65).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopic, partition), createReader(dstTopic, partition), TStringBuilder() << "stage=2 " << LabeledOutput(partition), deadline);
            }
        }
    } // Y_UNIT_TEST_SUITE(TPersQueueMirrorerWithScaling)
} // namespace NKikimr::NPersQueueTests
