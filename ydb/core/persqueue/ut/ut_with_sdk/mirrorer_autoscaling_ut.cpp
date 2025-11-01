#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
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
            ui32 Partitions = 0;
            ui32 Active = 0;
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

        void PrechargeTopic(const TString& name, NYdb::TDriver* driver, size_t sourceIdsCnt, size_t messageSize, size_t messageCount, TVector<ui32> partitions) {
            Cerr << "PRECHARGE BEGIN" << Endl;
            auto getNextPartitionGroup = [&partitions, nextPartitionIt = partitions.begin()]() mutable -> std::optional<ui32> {
                if (nextPartitionIt == partitions.end()) {
                    return std::nullopt;
                }
                ui32 groupId = 1 + *nextPartitionIt++;
                if (nextPartitionIt == partitions.end()) {
                    nextPartitionIt = partitions.begin();
                }
                return groupId;
            };
            for (size_t sourceId = 0; sourceId < sourceIdsCnt; ++sourceId) {
                const TString sourceIdTxt = TStringBuilder() << "precharge_source_id_" << sourceId;;
                const std::unordered_map<std::string, std::string> sessionMeta = {
                    {"precharge", "true"},
                };
                auto writer = CreateSimpleWriter(*driver, name, sourceIdTxt, getNextPartitionGroup(), std::nullopt, std::nullopt, sessionMeta);
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
        }

        ui64 SplitPartition(const ui64 txId, const TString& topicName, ui32 partitionId, const TString& boundary, TTestActorRuntime& runtime, TDuration pause = TDuration::Seconds(1)) {
            ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
            scheme.SetName(topicName);
            auto* split = scheme.AddSplit();
            split->SetPartition(partitionId);
            split->SetSplitBoundary(boundary);

            for (int attemptsLest = 100; ; --attemptsLest) {
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
                const auto status = e->Record.GetStatus();
                if (status == NSchemeShard::TEvSchemeShard::EStatus::StatusMultipleModifications && attemptsLest > 0) {
                    Cerr << "Multiple modifications; Retrying" << Endl;
                    Sleep(pause);
                    continue;
                }

                UNIT_ASSERT_EQUAL_C(status, NSchemeShard::TEvSchemeShard::EStatus::StatusAccepted,
                                    "Unexpected status " << NKikimrScheme::EStatus_Name(e->Record.GetStatus()) << " " << e->Record.GetReason());
                Sleep(pause);
                return e->Record.GetTxId();
            }
        }

        std::tuple<TString, ui32> WriteToPartition(const std::optional<ui32> partition, NYdb::TDriver& driver, const TString& srcTopic, const ui32 messagesCount, TMaybe<ui32> size = {}, bool checkClose = true, std::optional<ui32> id = {}) try {
            const std::optional<ui32> targetPartitionGroup = partition.transform([](ui32 p) { return p + 1; });
            Y_VERIFY(partition.has_value() || id.has_value());
            const TString sourceId = "some_sourceid_" + ToString(id.or_else([&](){ return partition; }).value());
            const std::unordered_map<std::string, std::string> sessionMeta = {
                {"partition", ToString(partition)},
            };
            auto genData = [&](ui32 messageIndex) {
                TString data = TStringBuilder() << "[[[" << LabeledOutput(messageIndex, partition.value_or(-1)) << "]]]";
                if (size.Defined() and data.size() < *size) {
                    data.resize(*size, 'x');
                }
                return data;
            };
             const auto writer = CreateSimpleWriter(driver,
                srcTopic,
                sourceId,
                targetPartitionGroup,
                {},
                {},
                sessionMeta
            );
            for (ui32 messageIndex = 1; messageIndex <= messagesCount; ++messageIndex) {
                writer->Write(genData(messageIndex));
            }
            bool res = writer->Close(TDuration::Seconds(20));
            Cerr << (TStringBuilder() << "WRITER CLOSE RESULT " << LabeledOutput(partition.value_or(-1), res) << "\n") << Flush;
            if (checkClose) {
                UNIT_ASSERT(res);
            }
            return std::make_tuple(sourceId, messagesCount);
        } catch (const yexception& e) {
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
                } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&*event)) {
                    e->Confirm();
                    return {};
                } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&*event)) {
                    return {};
                } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                    return {};
                }
            }
        }

        TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> ReadMessages(auto&& reader, auto&& passFilter, bool commit, const TInstant deadline, TDuration nextMessageTimeout, TStringBuf description, TMaybe<ui32> expectedSize) {
            TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> result;
            TInstant prevMessage = TInstant::Max();
            TInstant firstMessage = TInstant::Now();
            bool noData = true;
            size_t filteredOut = 0;
            for (;;) {
                TInstant eventDeadline = deadline;
                if (!expectedSize.Defined()) {
                    eventDeadline = Min(deadline, prevMessage + nextMessageTimeout);
                }
                eventDeadline = Max(eventDeadline, firstMessage + nextMessageTimeout);
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
                        filteredOut += 1;
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
                if (expectedSize.Defined() && result.size() >= *expectedSize) {
                    break;
                }
            }
            Cerr << (TStringBuilder() << "READ " << LabeledOutput(result.size(), filteredOut, description) << "\n") << Flush;
            return result;
        }

        bool SkipPrecharge(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            const auto& meta = message.GetMeta()->Fields;
            return !meta.contains("precharge");
        }

        void ComparePartitions(auto&& srcReader, auto&& dstReader, TStringBuf descr, TInstant deadline, TMaybe<ui32> expectedSize, bool commit = false) {
            const TDuration nextMessageTimeout = TDuration::Seconds(10);
            TVector srcMessages = ReadMessages(srcReader, SkipPrecharge, commit, deadline, nextMessageTimeout, TString::Join("srcTopic ", descr), expectedSize);
            TVector dstMessages = ReadMessages(dstReader, SkipPrecharge, commit, deadline, nextMessageTimeout, TString::Join("dstTopic ", descr), expectedSize);

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
            Cerr << (TStringBuilder() << descr << " src has " << srcMessages.size() << " messages\n");
            Cerr << (TStringBuilder() << descr << " dst has " << dstMessages.size() << " messages\n");
            UNIT_ASSERT_VALUES_EQUAL_C(dstMessages.size(), srcMessages.size(), LabeledOutput(descr));
            if (expectedSize) {
                UNIT_ASSERT_VALUES_EQUAL_C(dstMessages.size(), *expectedSize, LabeledOutput(descr));
            }
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

                    NKikimrConfig::TFeatureFlags ff;
                    ff.SetEnableTopicSplitMerge(true);
                    ff.SetEnablePQConfigTransactionsAtSchemeShard(true);
                    ff.SetEnableTopicServiceTx(true);
                    ff.SetEnableTopicAutopartitioningForCDC(true);
                    ff.SetEnableTopicAutopartitioningForReplication(true);
                    ff.SetEnableMirroredTopicSplitMerge(true);
                    pqSettings.SetFeatureFlags(ff);
                    return pqSettings;
                }()}
                , Server(PQSettings)
            {
                const auto& settings = Server.CleverServer->GetRuntime()->GetAppData().PQConfig.GetMirrorConfig().GetPQLibSettings();

                Fabric->Initialize(Server.CleverServer->GetRuntime()->GetAnyNodeActorSystem(), settings);
                for (ui32 nodeId = 0; nodeId < Server.CleverServer->GetRuntime()->GetNodeCount(); ++nodeId) {
                    Server.CleverServer->GetRuntime()->GetAppData(nodeId).PersQueueMirrorReaderFactory = Fabric.get();
                }

                Server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PERSQUEUE_READ_BALANCER, NKikimrServices::FLAT_TX_SCHEMESHARD});
                Server.EnableLogs({NKikimrServices::PQ_MIRRORER, NKikimrServices::PQ_MIRROR_DESCRIBER}, NActors::NLog::PRI_TRACE);
                Server.AnnoyingClient->CreateConsumer("user");
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
            const ui64 readSpeed = 1_MB;
            ctx.AnnoyingClient()->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{"some_user"},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic("/Root/PQ/" + srcTopicFullName);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
                dstTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
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
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1, {});
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "3", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId++, srcTopicFullName, 1, "\xC0", *ctx.Runtime());
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 3);

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, finalPartitionsCount);

            THashMap<ui32, ui32> messagesPerPartition;
            // write to source topic
            constexpr TWriteCount writeBatch[]{
                {0, 10},
                {2, 15},
                {3, 5},
            };
            for (const TWriteCount& wc : writeBatch) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .WithoutConsumer()
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const TInstant deadline = TDuration::Seconds(60).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopicFullName, partition), createReader(dstTopicFullName, partition), TStringBuilder() << LabeledOutput(partition), deadline, messagesPerPartition.Value(partition, 0), false);
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
            const ui64 readSpeed = 1_MB;
            ctx.AnnoyingClient()->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{"some_user"},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic("/Root/PQ/" + srcTopicFullName);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
                dstTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
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
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1, {});
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "3", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId++, srcTopicFullName, 1, "\xC0", *ctx.Runtime());
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2); // not splitted yet

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, 2); // not splitted yet

            THashMap<ui32, ui32> messagesPerPartition;
            // write to source topic
            constexpr TWriteCount writeBatch[]{
                {0, 10},
                {2, 15},
                {3, 5},
            };
            for (const TWriteCount& wc : writeBatch) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }

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

            while (CountPartitionsByStatus(dstTopicFullName, server).Partitions == 2) {
                Cerr << "Waiting for partitions to be splitted\n";
                Sleep(TDuration::Seconds(1));
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .WithoutConsumer()
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const TInstant deadline = TDuration::Seconds(65).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopicFullName, partition), createReader(dstTopicFullName, partition), TStringBuilder() << "stage=2 " << LabeledOutput(partition), deadline, messagesPerPartition.Value(partition, 0), false);
            }
        }

        Y_UNIT_TEST(TestMultisplit) {
            TTestContext ctx;
            NYdb::TDriver* driver = ctx.Driver();
            NYdb::NTopic::TTopicClient topicClient(*driver);
            auto& server = ctx.Server;

            const ui32 sourcesCount = 8;
            const ui32 partitionsCount = 2;
            const ui32 maxPartitionsCount = 10;
            const ui32 finalPartitionsCount = 8;
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
            const ui64 readSpeed = 1_MB;
            ctx.AnnoyingClient()->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{"some_user"},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic("/Root/PQ/" + srcTopicFullName);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
                dstTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
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
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1, {});
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "3", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId++, srcTopicFullName, 0, "\x40", *ctx.Runtime()); // 0 -> 2, 3
            SplitPartition(txId++, srcTopicFullName, 1, "\xC0", *ctx.Runtime()); // 1 -> 4, 5

            THashMap<ui32, ui32> messagesPerPartition;
            // write to source topic
            constexpr TWriteCount writeBatch1[]{
                {2, 2},
                {3, 3},
                {4, 4},
                {5, 5},
            };
            for (const TWriteCount& wc : writeBatch1) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }
            PrechargeTopic(srcTopic, driver, sourcesCount * 2, 1, 1, {});
            SplitPartition(txId++, srcTopicFullName, 3, "\x60", *ctx.Runtime()); // 3 -> 6, 7
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 5);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 5);

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, finalPartitionsCount);

            // write to source topic
            constexpr TWriteCount writeBatch2[]{
                {2, 2},
                {4, 4},
                {5, 5},
                {6, 6},
                {7, 7},
            };
            for (const TWriteCount& wc : writeBatch2) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .WithoutConsumer()
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const TInstant deadline = TDuration::Seconds(60).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopicFullName, partition), createReader(dstTopicFullName, partition), TStringBuilder() << LabeledOutput(partition), deadline, messagesPerPartition.Value(partition, 0), false);
            }
        }

        Y_UNIT_TEST(TestRootPartitionOverlap) {
            TTestContext ctx;
            NYdb::TDriver* driver = ctx.Driver();
            NYdb::NTopic::TTopicClient topicClient(*driver);
            auto& server = ctx.Server;

            const ui32 sourcesCount = 8;
            const ui32 partitionsCount = 1;
            const ui32 maxPartitionsCount = 4;
            const ui32 finalPartitionsCount = 3;
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
            const ui64 readSpeed = 1_MB;
            ctx.AnnoyingClient()->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{"some_user"},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic("/Root/PQ/" + srcTopicFullName);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
                dstTopicFullName,
                partitionsCount + 1,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{},
                /*TVector<TString> important =*/{},
                mirrorFrom,
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 1);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "2", server);
            }
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1, {});
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 1);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "3", server);
            }


            THashMap<ui32, ui32> messagesPerPartition;
            // write to source topic
            constexpr TWriteCount writeBatch1[]{
                {0, 10},
            };
            for (const TWriteCount& wc : writeBatch1) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }
            ui64 txId = 1006;
            SplitPartition(txId++, srcTopicFullName, 0, "\x80", *ctx.Runtime());
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 3); // partition `2` created, partitions `0` and `1` keept

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, finalPartitionsCount);


            // write to source topic
            constexpr TWriteCount writeBatch2[]{
                {1, 11},
                {2, 12},
            };
            for (const TWriteCount& wc : writeBatch2) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .WithoutConsumer()
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const TInstant deadline = TDuration::Seconds(60).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopicFullName, partition), createReader(dstTopicFullName, partition), TStringBuilder() << LabeledOutput(partition), deadline, messagesPerPartition.Value(partition, 0), false);
            }
        }

        Y_UNIT_TEST(TestSplitByScaleManager) {
            TTestContext ctx;
            NYdb::TDriver* driver = ctx.Driver();
            NYdb::NTopic::TTopicClient topicClient(*driver);
            auto& server = ctx.Server;

            const ui32 sourcesCount = 50;
            const ui32 partitionsCount = 8;
            const ui32 maxPartitionsCount = 100;

            const TString srcTopic = "topic2_src";
            const TString dstTopic = "topic1_dst";
            const TString srcTopicFullName = "rt3.dc1--" + srcTopic;
            const TString dstTopicFullName = "rt3.dc1--" + dstTopic;

            NKikimrPQ::TPQTabletConfig::TPartitionStrategy partitionStrategy;
            partitionStrategy.SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
            partitionStrategy.SetMinPartitionCount(partitionsCount);
            partitionStrategy.SetMaxPartitionCount(maxPartitionsCount);
            partitionStrategy.SetScaleThresholdSeconds(2);
            partitionStrategy.SetScaleUpPartitionWriteSpeedThresholdPercent(1);
            partitionStrategy.SetScaleDownPartitionWriteSpeedThresholdPercent(1);

            const ui64 writeSpeed = 100_KB;
            const ui64 readSpeed = 100_KB;
            ctx.AnnoyingClient()->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{"some_user"},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic("/Root/PQ/" + srcTopicFullName);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
                dstTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{},
                /*TVector<TString> important =*/{},
                mirrorFrom,
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, partitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, partitionsCount);
            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "2", server);
            }
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1, {});

            for (ui32 partitionId = 0; partitionId < partitionsCount; partitionId += 2) { // write to even partitions to register sources
                WriteToPartition(partitionId, *driver, srcTopicFullName, 1);
            }
            for (int writeIteration = 0;; ++writeIteration) {
                for (ui32 partitionId = 0; partitionId < partitionsCount; partitionId += 2) { // write to even partitions
                    Cerr << (TStringBuilder() << "Write big data " << LabeledOutput(writeIteration, partitionId) << "\n") << Flush;
                    WriteToPartition(partitionId, *driver, srcTopicFullName, 10, 500_KB, false);
                }
                Sleep(TDuration::Seconds(1));
                const auto& stat = CountPartitionsByStatus(srcTopicFullName, server);
                if (stat.Active > partitionsCount) {
                    break;
                }
            }
            ui64 txId = 1006;
            for (ui32 partitionId = 1; partitionId < partitionsCount; partitionId += 2) { // split odd partitions
                ui32 start = 255 * partitionId / partitionsCount;
                ui32 end = 255 * (partitionId + 1) / partitionsCount;
                ui32 middle = ClampVal<ui32>((start + end) / 2, 0, 255);
                TString bound(1, '\0' + middle);
                SplitPartition(txId++, srcTopicFullName, partitionId, bound, *ctx.Runtime(), TDuration::Zero());
            }

            TInstant end = TDuration::Minutes(1).ToDeadLine();
            while (true) {
                const auto srcStat = CountPartitionsByStatus(srcTopicFullName, server);
                const auto dstStat = CountPartitionsByStatus(dstTopicFullName, server);

                for (TString name : {srcTopicFullName, dstTopicFullName}) {
                    PrintTopicDescription(name, "WAIT", server);
                }

                Cerr << "Wait partition count equals: " << srcStat.Partitions << " == " << dstStat.Partitions << Endl;
                if (srcStat.Partitions == dstStat.Partitions) {
                    break;
                }
                if (end < TInstant::Now()) {
                    UNIT_ASSERT_VALUES_EQUAL(srcStat.Partitions, dstStat.Partitions);
                }
                Sleep(TDuration::MilliSeconds(1000));
            }

            for (TString name : {srcTopicFullName, dstTopicFullName}) {
                PrintTopicDescription(name, "4", server);
            }

            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .WithoutConsumer()
                        .AutoPartitioningSupport(true)
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const auto srcStat = CountPartitionsByStatus(srcTopicFullName, server);
            Cerr << (TStringBuilder() << "Total partitions: " << srcStat.Partitions << "; Active: " << srcStat.Active << "\n") << Flush;
            const TInstant deadline = TDuration::Seconds(30).ToDeadLine();
            for (ui32 partition = 0; partition < srcStat.Partitions; ++partition) {
                ComparePartitions(createReader(srcTopic, partition), createReader(dstTopic, partition), TStringBuilder() << LabeledOutput(partition), deadline, {}, false);
            }
        }

        Y_UNIT_TEST(TestTargetAutoGrow) {
            TTestContext ctx;
            NYdb::TDriver* driver = ctx.Driver();
            NYdb::NTopic::TTopicClient topicClient(*driver);
            auto& server = ctx.Server;

            const ui32 sourcesCount = 12;
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
            const ui64 readSpeed = 1_MB;
            ctx.AnnoyingClient()->CreateTopic(
                srcTopicFullName,
                partitionsCount,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{"some_user"},
                /*TVector<TString> important =*/{},
                /*std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom =*/{},
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);


            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);

            for (TString name : {srcTopicFullName, }) {
                PrintTopicDescription(name, "2", server);
            }
            PrechargeTopic(srcTopic, driver, sourcesCount, 1, 1, {});
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 2);
            for (TString name : {srcTopicFullName, }) {
                PrintTopicDescription(name, "3", server);
            }

            ui64 txId = 1006;
            SplitPartition(txId++, srcTopicFullName, 1, "\xC0", *ctx.Runtime());

            NKikimrPQ::TMirrorPartitionConfig mirrorFrom;
            mirrorFrom.SetEndpoint("localhost");
            mirrorFrom.SetEndpointPort(ctx.Server.GrpcPort);
            mirrorFrom.SetTopic("/Root/PQ/" + srcTopicFullName);
            mirrorFrom.SetConsumer("some_user");
            mirrorFrom.SetSyncWriteTime(true);

            ctx.AnnoyingClient()->CreateTopic(
                dstTopicFullName,
                1,
                /*ui32 lowWatermark =*/8_MB,
                /*ui64 lifetimeS =*/86400,
                /*ui64 writeSpeed =*/writeSpeed,
                /*TString user =*/"",
                /*ui64 readSpeed =*/readSpeed,
                /*TVector<TString> rr =*/{},
                /*TVector<TString> important =*/{},
                mirrorFrom,
                /*ui64 sourceIdMaxCount =*/6000000,
                /*ui64 sourceIdLifetime =*/86400,
                partitionStrategy);

            for (TString name : {srcTopicFullName, }) {
                PrintTopicDescription(name, "4", server);
            }


            THashMap<ui32, ui32> messagesPerPartition;
            // write to source topic
            constexpr TWriteCount writeBatch[]{
                {0, 10},
                {2, 15},
                {3, 5},
            };
            for (const TWriteCount& wc : writeBatch) {
                Cerr << (TStringBuilder() << "Writing to partition " << wc.Partiton << " " << wc.Count << " messages\n");
                const auto [sourceId, written] = WriteToPartition(wc.Partiton, *driver, srcTopicFullName, wc.Count);
                messagesPerPartition[wc.Partiton] += written;
            }

            while (CountPartitionsByStatus(dstTopicFullName, server).Partitions != finalPartitionsCount) {
                // wait for grow
                Sleep(TDuration::Seconds(1));
            }

            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Active, 3);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(srcTopicFullName, server).Partitions, finalPartitionsCount);
            UNIT_ASSERT_VALUES_EQUAL(CountPartitionsByStatus(dstTopicFullName, server).Partitions, finalPartitionsCount);


            auto createReader = [&](const TString& topic, ui32 partition) {
                auto settings =
                    NYdb::NTopic::TReadSessionSettings()
                        .AppendTopics(NYdb::NTopic::TTopicReadSettings(topic)
                                          .AppendPartitionIds(partition))
                        .WithoutConsumer()
                        .Decompress(true);

                return topicClient.CreateReadSession(settings);
            };

            const TInstant deadline = TDuration::Seconds(60).ToDeadLine();
            for (ui32 partition = 0; partition < finalPartitionsCount; ++partition) {
                ComparePartitions(createReader(srcTopicFullName, partition), createReader(dstTopicFullName, partition), TStringBuilder() << LabeledOutput(partition), deadline, messagesPerPartition.Value(partition, 0), false);
            }
        }

    } // Y_UNIT_TEST_SUITE(TPersQueueMirrorerWithScaling)
} // namespace NKikimr::NPersQueueTests
