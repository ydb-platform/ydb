#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/producer.h>

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <atomic>
#include <util/digest/murmur.h>
#include <util/stream/zlib.h>

#include <thread>

using namespace std::chrono_literals;

static const bool EnableDirectRead = !std::string{std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") ? std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") : ""}.empty();


namespace NYdb::inline Dev::NTopic::NTests {

// Serialize data in the format expected by the PQ tablet (TDataChunk proto)
TString SerializeDataChunk(ui64 seqNo, const TString& payload) {
    NKikimrPQClient::TDataChunk proto;
    proto.SetSeqNo(seqNo);
    proto.SetData(payload);
    proto.SetCodec(0);  // RAW codec
    proto.SetCreateTime(TInstant::Now().MilliSeconds());
    TString result;
    Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&result);
    return result;
}

TWriteMessage CreateMessage(std::string_view payload, const std::string& key, ui64 seqNo) {
    TWriteMessage msg(payload);
    msg.SeqNo(seqNo);
    msg.Key(key);
    return msg;
}

// Write a message with binary (non-UTF8) producer ID using direct tablet communication
// This bypasses gRPC string validation by sending directly to the PQ tablet
// The SourceId field in TCmdWrite is defined as 'bytes' in protobuf, so it supports binary data
void WriteBinaryProducerIdWithDirectTabletWrite(TTopicSdkTestSetup& setup,
                                                 const TString& topicPath,
                                                 const TString& binaryProducerId,
                                                 const TString& payload) {
    auto& runtime = setup.GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();

    // Get the tablet ID for partition 0 using scheme cache navigation
    auto navigate = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";

    NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(topicPath);
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NKikimr::NSchemeCache::TSchemeCacheNavigate::OpList;

    navigate->ResultSet.push_back(std::move(entry));
    navigate->Cookie = 12345;

    runtime.Send(NKikimr::MakeSchemeCacheID(), edge,
                 new NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
                 0,
                 true);
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult>();

    UNIT_ASSERT_VALUES_EQUAL(response->Request->Cookie, 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0);

    auto& front = response->Request->ResultSet.front();
    UNIT_ASSERT(front.PQGroupInfo);
    UNIT_ASSERT_GT(front.PQGroupInfo->Description.PartitionsSize(), 0);

    ui64 tabletId = 0;
    for (size_t i = 0; i < front.PQGroupInfo->Description.PartitionsSize(); ++i) {
        auto& p = front.PQGroupInfo->Description.GetPartitions(i);
        if (p.GetPartitionId() == 0) {
            tabletId = p.GetTabletId();
            break;
        }
    }
    UNIT_ASSERT(tabletId != 0);

    // First, get ownership of the partition
    auto [ownerCookie, pipeClient] = NKikimr::NPQ::CmdSetOwner(&runtime, tabletId, edge, 0, "test-owner", true);

    // Encode the binary producer ID using the same encoding as the mirrorer
    TString encodedSourceId = NKikimr::NPQ::NSourceIdEncoding::EncodeSimple(binaryProducerId);

    // Serialize the data in TDataChunk format (as expected by the PQ tablet)
    TString serializedData = SerializeDataChunk(1, payload);

    // Build the write request manually to have full control over the SourceId field
    THolder<NKikimr::TEvPersQueue::TEvRequest> request;
    request.Reset(new NKikimr::TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(0);
    req->SetOwnerCookie(ownerCookie);
    req->SetMessageNo(0);
    req->SetCmdWriteOffset(0);

    auto write = req->AddCmdWrite();
    write->SetSourceId(encodedSourceId);
    write->SetSeqNo(1);
    write->SetData(serializedData);
    write->SetCreateTimeMS(TInstant::Now().MilliSeconds());
    write->SetDisableDeduplication(true);

    runtime.SendToPipe(tabletId, edge, request.Release(), 0, NKikimr::GetPipeConfigWithRetries());

    // Wait for the response
    TAutoPtr<NActors::IEventHandle> handle;
    auto* result = runtime.GrabEdgeEvent<NKikimr::TEvPersQueue::TEvResponse>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_C(result->Record.GetErrorCode() == ::NPersQueue::NErrorCode::OK,
                  "Write failed: " << result->Record.GetErrorReason());
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetPartitionResponse().CmdWriteResultSize(), 1);
}

static std::string FindKeyForBucket(size_t bucket, size_t bucketsCount) {
    for (size_t i = 0; i < 1'000'000; ++i) {
        std::string key = "key-" + ToString(i);
        if (MurmurHash<ui64>(key.data(), key.size()) % bucketsCount == bucket) {
            return key;
        }
    }
    UNIT_FAIL("Failed to find a key for bucket");
    return {};
}

void CreateTopicWithAutoPartitioning(TTopicClient& client) {
    TCreateTopicSettings createSettings;
        createSettings
            .BeginConfigurePartitioningSettings()
            .MinActivePartitions(2)
            .MaxActivePartitions(100)
                .BeginConfigureAutoPartitioningSettings()
                .UpUtilizationPercent(2)
                .DownUtilizationPercent(1)
                .StabilizationWindow(TDuration::Seconds(2))
                .Strategy(EAutoPartitioningStrategy::ScaleUp)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings();
    client.CreateTopic(TEST_TOPIC, createSettings).Wait();
}

void WriteAndReadToEndWithRestarts(TReadSessionSettings readSettings, TWriteSessionSettings writeSettings, const std::string& message, std::uint32_t count,
    TTopicSdkTestSetup& setup, std::shared_ptr<TManagedExecutor> decompressor, ui32 restartPeriod = 7, ui32 maxRestartsCount = 10)
{
    auto client = setup.MakeClient();
    auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

    for (std::uint32_t i = 1; i <= count; ++i) {
        bool res = session->Write(message);
        UNIT_ASSERT(res);
    }
    bool res = session->Close(TDuration::Seconds(10));
    UNIT_ASSERT(res);

    std::shared_ptr<IReadSession> ReadSession;

    TTopicClient topicClient = setup.MakeClient();

    auto WaitTasks = [&, timeout = TInstant::Now() + TDuration::Seconds(60)](auto f, size_t c) {
        while (f() < c) {
            UNIT_ASSERT(timeout > TInstant::Now());
            ReadSession->WaitEvent();
            std::this_thread::sleep_for(100ms);
        };
    };
    auto WaitPlannedTasks = [&](auto e, size_t count) {
        WaitTasks([&]() { return e->GetPlannedCount(); }, count);
    };
    auto WaitExecutedTasks = [&](auto e, size_t count) {
        WaitTasks([&]() { return e->GetExecutedCount(); }, count);
    };

    auto RunTasks = [&](auto e, const std::vector<size_t>& tasks) {
        size_t n = tasks.size();
        WaitPlannedTasks(e, n);
        size_t completed = e->GetExecutedCount();
        e->StartFuncs(tasks);
        WaitExecutedTasks(e, completed + n);
    };
    Y_UNUSED(RunTasks);

    auto PlanTasksAndRestart = [&](auto e, const std::vector<size_t>& tasks) {
        size_t n = tasks.size();
        WaitPlannedTasks(e, n);
        size_t completed = e->GetExecutedCount();

        setup.GetServer().KillTopicPqrbTablet(JoinPath({TString(setup.MakeDriverConfig().GetDatabase()), TString(setup.GetTopicPath())}));
        std::this_thread::sleep_for(100ms);

        e->StartFuncs(tasks);
        WaitExecutedTasks(e, completed + n);
    };
    Y_UNUSED(PlanTasksAndRestart);


    NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
    TAtomic lastOffset = 0u;

    auto f = checkedPromise.GetFuture();
    readSettings.EventHandlers_.SimpleDataHandlers(
        [&]
        (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
        AtomicSet(lastOffset, ev.GetMessages().back().GetOffset());
        Cerr << ">>> TEST: last offset = " << lastOffset << Endl;
    });

    ReadSession = topicClient.CreateReadSession(readSettings);

    Cerr << ">>> TEST: start reading" << Endl;

    std::uint32_t i = 0;
    ui32 restartCount = 0;
    while (AtomicGet(lastOffset) + 1 < count) {
        if (restartCount < maxRestartsCount && i % restartPeriod == 1) {
            PlanTasksAndRestart(decompressor, {i++});
            restartCount++;
        } else {
            RunTasks(decompressor, {i++});
        }
    }

    ReadSession->Close(TDuration::MilliSeconds(10));
}

Y_UNIT_TEST_SUITE(BasicUsage) {
    Y_UNIT_TEST(CreateTopicWithCustomName) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        const TString name = "test-topic-" + ToString(TInstant::Now().Seconds());
        setup.CreateTopic(name, TEST_CONSUMER, 1);
    }

    Y_UNIT_TEST(ReadBinaryProducerIdFromLowLevelWrite) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME};

        const std::string topicPath = setup.GetTopicPath();
        const TString fullTopicPath = setup.GetFullTopicPath();
        const TString binaryProducerId("\xFF\xFE\xF0", 3);
        const TString payload = "payload";

        // Use direct tablet communication to write with binary producer ID
        // This bypasses gRPC string validation - SourceId is 'bytes' type in protobuf
        WriteBinaryProducerIdWithDirectTabletWrite(setup, fullTopicPath, binaryProducerId, payload);

        auto client = setup.MakeClient();
        NThreading::TPromise<void> gotPromise = NThreading::NewPromise<void>();
        TString gotProducerId;
        TString gotMetaProducerId;
        std::atomic<bool> handled{false};

        auto readSettings = TReadSessionSettings()
            .ConsumerName(setup.GetConsumerName())
            .AppendTopics(topicPath)
            .DirectRead(EnableDirectRead);

        readSettings.EventHandlers_.SimpleDataHandlers([&](TReadSessionEvent::TDataReceivedEvent& ev) {
            if (handled.exchange(true)) {
                ev.Commit();
                return;
            }

            UNIT_ASSERT(!ev.GetMessages().empty());
            auto& msg = ev.GetMessages().front();
            gotProducerId = msg.GetProducerId();
            auto meta = msg.GetMeta();
            UNIT_ASSERT(meta);
            auto it = meta->Fields.find("_encoded_producer_id");
            UNIT_ASSERT(it != meta->Fields.end());
            gotMetaProducerId = it->second;
            UNIT_ASSERT_VALUES_EQUAL(TString{msg.GetData()}, payload);
            msg.Commit();
            gotPromise.SetValue();
        });

        auto reader = client.CreateReadSession(readSettings);
        UNIT_ASSERT(gotPromise.GetFuture().Wait(TDuration::Seconds(10)));
        reader->Close(TDuration::Seconds(5));

        const TString expectedEncoded = Base64Encode(binaryProducerId);
        UNIT_ASSERT_VALUES_EQUAL(gotProducerId, expectedEncoded);
        UNIT_ASSERT_VALUES_EQUAL(gotMetaProducerId, expectedEncoded);
        UNIT_ASSERT_VALUES_EQUAL(Base64Decode(expectedEncoded), binaryProducerId);
    }
    
    Y_UNIT_TEST(CreateTopicWithManyPartitions) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        const TString name = "test-topic-" + ToString(TInstant::Now().Seconds());
        setup.CreateTopic(name, TEST_CONSUMER, 100);

        auto describe = setup.MakeClient().DescribeTopic(name).GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 100);
    }

    Y_UNIT_TEST(CreateTopicWithStreamingConsumer) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddStreamingConsumer("consumer_name");

        auto status = client.CreateTopic("topic_name", topics).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Streaming);
    }

    Y_UNIT_TEST(CreateTopicWithSharedConsumer_MoveDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddConsumer()
                .ConsumerName("shared_consumer_name")
                .ConsumerType(EConsumerType::Shared)
                .DefaultProcessingTimeout(TDuration::Seconds(7))
                .KeepMessagesOrder(true)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(11)
                    .EndCondition()
                    .MoveAction("deadLetterQueue-topic")
                .EndDeadLetterPolicy()
            .EndAddConsumer();

        auto status = client.CreateTopic("topic_name", topics).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "deadLetterQueue-topic");
    }

    Y_UNIT_TEST(CreateTopicWithSharedConsumer_DeleteDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddSharedConsumer()
                .ConsumerName("shared_consumer_name")
                .DefaultProcessingTimeout(TDuration::Seconds(7))
                .KeepMessagesOrder(true)
                .BeginDeadLetterPolicy()
                    .Enabled(true)
                    .DeleteAction()
                    .BeginCondition()
                        .MaxProcessingAttempts(11)
                    .EndCondition()
                .EndDeadLetterPolicy()
            .EndAddConsumer();
        Cerr << ">>>>> " << topics.Consumers_[0].DeadLetterPolicy_.Action_ << Endl;

        auto status = client.CreateTopic("topic_name", topics).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Delete);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "");
    }

    Y_UNIT_TEST(CreateTopicWithSharedConsumer_DisabledDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddConsumer()
                .ConsumerName("shared_consumer_name")
                .ConsumerType(EConsumerType::Shared)
                .DefaultProcessingTimeout(TDuration::Seconds(7))
                .KeepMessagesOrder(true)
            .EndAddConsumer();

        auto status = client.CreateTopic("topic_name", topics).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), false);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Unspecified);
    }

    Y_UNIT_TEST(CreateTopicWithSharedConsumer_KeepMessagesOrder_False) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddSharedConsumer("shared_consumer_name")
                .KeepMessagesOrder(false)
            .EndAddConsumer();

        auto status = client.CreateTopic("topic_name", topics).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), false);
    }

    Y_UNIT_TEST(CreateTopicWithSharedConsumer_KeepMessagesOrder_True) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddSharedConsumer("shared_consumer_name")
                .KeepMessagesOrder(true)
            .EndAddConsumer();

        auto status = client.CreateTopic("topic_name", topics).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
    }

    Y_UNIT_TEST(AlterTopicWithSharedConsumer_MoveDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(7))
                    .KeepMessagesOrder(true)
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .MoveAction("deadLetterQueue-topic")
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .DefaultProcessingTimeout(TDuration::Seconds(13))
                    .BeginAlterDeadLetterPolicy()
                        .Enabled(true)
                        .AlterMoveAction("deadLetterQueue-topic-new")
                        .BeginCondition()
                            .MaxProcessingAttempts(17)
                        .EndCondition()
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(13));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 17);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "deadLetterQueue-topic-new");
    }

    Y_UNIT_TEST(AlterTopicWithSharedConsumer_DisableDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(7))
                    .KeepMessagesOrder(true)
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .MoveAction("deadLetterQueue-topic")
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());

            auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());
            auto& c = describe.GetTopicDescription().GetConsumers()[0];
            UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .BeginAlterDeadLetterPolicy()
                        .Enabled(false)
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), false);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "deadLetterQueue-topic");
    }

    Y_UNIT_TEST(AlterTopicWithSharedConsumer_SetDeleteDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(7))
                    .KeepMessagesOrder(true)
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .MoveAction("deadLetterQueue-topic")
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .BeginAlterDeadLetterPolicy()
                        .SetDeleteAction()
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Delete);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "");
    }

    Y_UNIT_TEST(AlterTopicWithSharedConsumer_SetMoveDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(7))
                    .KeepMessagesOrder(true)
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .DeleteAction()
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .BeginAlterDeadLetterPolicy()
                        .SetMoveAction("dlq-topic")
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "dlq-topic");
    }

    Y_UNIT_TEST(AlterTopicWithSharedConsumer_AlterMoveDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(7))
                    .KeepMessagesOrder(true)
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .MoveAction("dlq-topic")
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .BeginAlterDeadLetterPolicy()
                        .AlterMoveAction("dlq-topic-new")
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "dlq-topic-new");
    }

    Y_UNIT_TEST(AlterTopicWithSharedConsumer_DeleteDeadLetterPolicy_AlterMoveDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(7))
                    .KeepMessagesOrder(true)
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .DeleteAction()
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .BeginAlterDeadLetterPolicy()
                        .AlterMoveAction("dlq-topic-new")
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(!status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        auto describe = client.DescribeTopic("topic_name").GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToOneLineString());

        auto& d = describe.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(d.GetConsumers().size(), 1);
        auto& c = d.GetConsumers()[0];
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerName(), "shared_consumer_name");
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 11);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterAction::Delete);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "");
    }

    Y_UNIT_TEST(AlterDeadLetterPolicy_StreamingConsumer) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        {
            TCreateTopicSettings topics;
            topics.BeginAddStreamingConsumer("shared_consumer_name");

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .BeginAlterDeadLetterPolicy()
                        .AlterMoveAction("dlq-topic-new")
                    .EndAlterDeadLetterPolicy()
                .EndAlterConsumer();
            auto status = client.AlterTopic("topic_name", topics).GetValueSync();
            auto issue = status.GetIssues().ToOneLineString();
            UNIT_ASSERT_C(!status.IsSuccess(), issue);
            UNIT_ASSERT_C(issue.contains("Cannot alter consumer type"), issue);
        }
    }

    Y_UNIT_TEST(ReadWithoutConsumerWithRestarts) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = std::make_shared<TSyncExecutor>();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        TTopicReadSettings topic = setup.GetTopicPath();
        topic.AppendPartitionIds(0);
        readSettings
            .WithoutConsumer()
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(topic)
            .DirectRead(EnableDirectRead)
            ;

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath())
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(NTopic::ECodec::RAW)
            .CompressionExecutor(compressor);


        std::uint32_t count = 700;
        std::string message(2'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(ReadWithRestarts) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = std::make_shared<TSyncExecutor>();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup.GetConsumerName())
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(setup.GetTopicPath())
            // .DirectRead(EnableDirectRead)
            ;

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(ECodec::RAW)
            .CompressionExecutor(compressor);


        std::uint32_t count = 700;
        std::string message(2'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(ReadWithRestartsAndLargeData) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = std::make_shared<TSyncExecutor>();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup.GetConsumerName())
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(setup.GetTopicPath())
            // .DirectRead(EnableDirectRead)
            ;

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(ECodec::RAW)
            .CompressionExecutor(compressor);

        std::uint32_t count = 3000;
        std::string message(8'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(ConflictingWrites) {

        TTopicSdkTestSetup setup(TEST_CASE_NAME);

        TWriteSessionSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Path(setup.GetTopicPath()).ProducerId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Codec(ECodec::RAW);
        IExecutor::TPtr executor = std::make_shared<TSyncExecutor>();
        writeSettings.CompressionExecutor(executor);

        std::uint64_t count = 100u;

        auto client = setup.MakeClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        std::string messageBase = "message----";

        for (auto i = 0u; i < count; i++) {
            auto res = session->Write(messageBase);
            UNIT_ASSERT(res);
            if (i % 10 == 0) {
                setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());
            }
        }
        session->Close();

        auto describeTopicSettings = TDescribeTopicSettings().IncludeStats(true);
        auto result = client.DescribeTopic(setup.GetTopicPath(), describeTopicSettings).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto description = result.GetTopicDescription();
        UNIT_ASSERT(description.GetPartitions().size() == 1);
        auto stats = description.GetPartitions().front().GetPartitionStats();
        UNIT_ASSERT(stats.has_value());
        UNIT_ASSERT_VALUES_EQUAL(stats->GetEndOffset(), count);

    }

    Y_UNIT_TEST(Producer_UserEventHandlers) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);

        auto client = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.MaxBlock(TDuration::Seconds(30));

        std::atomic<size_t> acksCount{0};
        std::atomic<size_t> closedCount{0};

        writeSettings.EventHandlers_.HandlersExecutor(std::make_shared<TSyncExecutor>());

        writeSettings.EventHandlers_.AcksHandler(
            [&](TWriteSessionEvent::TAcksEvent& ev) {
                Y_UNUSED(ev);
                acksCount.fetch_add(1);
            });

        writeSettings.EventHandlers_.SessionClosedHandler(
            [&](const TSessionClosedEvent& ev) {
                Y_UNUSED(ev);
                closedCount.fetch_add(1);
            });

        auto session = client.CreateProducer(writeSettings);

        const ui64 messages = 5;
        for (ui64 i = 0; i < messages; ++i) {
            std::string payload = "payload";
            TWriteMessage msg(payload);
            msg.SeqNo(i + 1);
            msg.Key("key-" + ToString(i));
            UNIT_ASSERT_C(session->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        UNIT_ASSERT_C(session->Close(TDuration::Seconds(30)).IsSuccess(), "Failed to close keyed write session");
        auto acks = acksCount.load();
        UNIT_ASSERT_C(acks == messages, "AcksHandler does not work properly " + std::to_string(acks));
        UNIT_ASSERT_C(closedCount.load() > 0, "SessionClosedHandler was not called");
    }

    Y_UNIT_TEST(Producer_ProducerIdPrefixRequired) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));

        UNIT_ASSERT_EXCEPTION(setup.MakeClient().CreateProducer(writeSettings), TContractViolation);
    }

    Y_UNIT_TEST(Producer_SessionClosedDueToUserError) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);
        auto publicClient = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto session = publicClient.CreateProducer(writeSettings);

        std::string payload = "msg0";
        TWriteMessage msg(payload);
        msg.SeqNo(0);
        msg.Key("key");
        UNIT_ASSERT_C(session->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        auto flushResult = session->Flush().GetValueSync();
        UNIT_ASSERT_C(flushResult.IsClosed(), "Failed to flush producer");
        UNIT_ASSERT_C(flushResult.ClosedDescription->GetStatus() == EStatus::BAD_REQUEST, "Status is not BAD_REQUEST");
        UNIT_ASSERT_C(session->Close(TDuration::Seconds(10)).IsAlreadyClosed(), "Failed to close producer");
    }

    Y_UNIT_TEST(Producer_NoAutoPartitioning_HashPartitionChooser) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);

        // Capture partition ids in the same order as DescribeTopic returns them
        // (the keyed session uses the same DescribeTopic ordering to map hash bucket -> partition id).
        auto publicClient = setup.MakeClient();
        auto describeTopicSettings = TDescribeTopicSettings().IncludeStats(true);
        auto before = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();
        UNIT_ASSERT_C(before.IsSuccess(), before.GetIssues().ToOneLineString());
        const auto& beforePartitions = before.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(beforePartitions.size(), 2);
        const ui64 partitionId0 = beforePartitions[0].GetPartitionId();
        const ui64 partitionId1 = beforePartitions[1].GetPartitionId();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto session = publicClient.CreateProducer(writeSettings);

        const std::string key0 = FindKeyForBucket(0, 2);
        const std::string key1 = FindKeyForBucket(1, 2);

        const ui64 count0 = 7;
        const ui64 count1 = 11;

        auto seqNo = 1;
        for (ui64 i = 0; i < count0; ++i) {
            std::string payload = "msg0";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            msg.Key(key0);
            UNIT_ASSERT_C(session->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }
        for (ui64 i = 0; i < count1; ++i) {
            std::string payload = "msg1";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            msg.Key(key1);
            UNIT_ASSERT_C(session->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        UNIT_ASSERT_C(session->Close(TDuration::Seconds(10)).IsSuccess(), "Failed to close keyed write session");

        auto after = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();
        UNIT_ASSERT_C(after.IsSuccess(), after.GetIssues().ToOneLineString());
        const auto& afterPartitions = after.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(afterPartitions.size(), 2);

        std::unordered_map<ui64, ui64> endOffsets;
        for (const auto& p : afterPartitions) {
            auto stats = p.GetPartitionStats();
            UNIT_ASSERT(stats.has_value());
            endOffsets[p.GetPartitionId()] = stats->GetEndOffset();
        }

        auto it0 = endOffsets.find(partitionId0);
        auto it1 = endOffsets.find(partitionId1);
        UNIT_ASSERT(it0 != endOffsets.end());
        UNIT_ASSERT(it1 != endOffsets.end());

        const ui64 endOffset0 = it0->second;
        const ui64 endOffset1 = it1->second;

        // Partition ordering in DescribeTopic is not a part of public API contract, so allow swapping.
        UNIT_ASSERT_VALUES_EQUAL(endOffset0 + endOffset1, count0 + count1);
        UNIT_ASSERT_C(
            (endOffset0 == count0 && endOffset1 == count1) || (endOffset0 == count1 && endOffset1 == count0),
            TStringBuilder() << "Unexpected end offsets distribution: "
                             << "partitionId0=" << partitionId0 << " endOffset0=" << endOffset0 << ", "
                             << "partitionId1=" << partitionId1 << " endOffset1=" << endOffset1 << ", "
                             << "expected (" << count0 << "," << count1 << ") in any order"
        );
    }

    Y_UNIT_TEST(Producer_NoAutoPartitioning_BoundPartitionChooser) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 5, 10);

        auto publicClient = setup.MakeClient();
        auto describeTopicSettings = TDescribeTopicSettings().IncludeStats(true);
        auto before = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();

        UNIT_ASSERT_C(before.IsSuccess(), before.GetIssues().ToOneLineString());
        const auto& beforePartitions = before.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(beforePartitions.size(), 5);

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Bound);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitioningKeyHasher([](const std::string_view key) -> std::string {
            return std::string{key};
        });
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto producer = publicClient.CreateProducer(writeSettings);
        auto rawProducer = std::dynamic_pointer_cast<TProducer>(producer);
        const auto& partitions = rawProducer->GetPartitions();
        
        std::unordered_map<ui64, ui64> keysCount;
        for (const auto& p : partitions) {
            keysCount[p.PartitionId_] = 0;
        }

        for (size_t i = 0; i < 100; ++i) {
            auto key = CreateGuidAsString();
            for (const auto& p : partitions) {
                if (p.InRange(key)) {
                    keysCount[p.PartitionId_]++;
                    break;
                }
            }

            std::string payload = "msg";
            TWriteMessage msg(payload);
            msg.SeqNo(i + 1);
            msg.Key(key);
            UNIT_ASSERT_C(producer->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(10)).IsSuccess(), "Failed to close producer");

        auto after = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();
        UNIT_ASSERT_C(after.IsSuccess(), after.GetIssues().ToOneLineString());
        const auto& afterPartitions = after.GetTopicDescription().GetPartitions();

        std::unordered_map<ui64, ui64> endOffsets;
        for (const auto& p : afterPartitions) {
            auto stats = p.GetPartitionStats();
            UNIT_ASSERT(stats.has_value());
            endOffsets[p.GetPartitionId()] = stats->GetEndOffset();
        }

        for (const auto& p : partitions) {
            auto sb = TStringBuilder() << "partitionId=" << p.PartitionId_ << " endOffset=" << endOffsets[p.PartitionId_] << " keysCount=" << keysCount[p.PartitionId_];
            UNIT_ASSERT_VALUES_EQUAL_C(endOffsets[p.PartitionId_], keysCount[p.PartitionId_], sb.c_str());
        }
    }

    Y_UNIT_TEST(Producer_EventLoop_Acks) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 4);

        auto client = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(10));
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto producer = client.CreateProducer(writeSettings);

        const ui64 count = 3000;
        for (ui64 i = 1; i <= count; ++i) {
            auto key = CreateGuidAsString();
            std::string payload = "data";
            TWriteMessage msg(payload);
            msg.SeqNo(i);
            msg.Key(key);
            UNIT_ASSERT_C(producer->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        UNIT_ASSERT_C(producer->Flush().GetValueSync().IsSuccess(), "Failed to flush producer");
        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(10)).IsSuccess(), "Failed to close producer");
    }

    Y_UNIT_TEST(Producer_MultiThreadedWrite_Acks) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto producer = client.CreateProducer(writeSettings);

        constexpr ui64 threadsCount = 4;
        constexpr ui64 perThread = 25;

        std::atomic<ui64> nextSeqNo{1};
        std::vector<std::jthread> threads;
        threads.reserve(threadsCount);

        for (ui64 t = 0; t < threadsCount; ++t) {
            threads.emplace_back([&, t]() {
                auto key = TStringBuilder() << "key-" << t;
                for (ui64 i = 0; i < perThread; ++i) {
                    const ui64 seqNo = nextSeqNo.fetch_add(1);
                    std::string payload = "data";
                    TWriteMessage msg(payload);
                    msg.SeqNo(seqNo);
                    msg.Key(key);
                    auto writeResult = producer->Write(std::move(msg));
                    UNIT_ASSERT_C(
                        writeResult.IsSuccess(),
                        "Failed to write message"
                    );
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        UNIT_ASSERT_C(producer->Flush().GetValueSync().IsSuccess(), "Failed to flush producer");
        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(10)).IsSuccess(), "Failed to close producer");
    }

    Y_UNIT_TEST(Producer_IdleSessionsTimeout) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(5));
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto producer = client.CreateProducer(writeSettings);

        constexpr ui64 messages = 100;
        ui64 seqNo = 1;

        for (ui64 i = 0; i < messages; ++i) {
            auto key = CreateGuidAsString();
            std::string payload = "data";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            msg.Key(key);
            UNIT_ASSERT_C(producer->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        UNIT_ASSERT_C(producer->Flush().GetValueSync().IsSuccess(), "Failed to flush producer");
        Sleep(TDuration::Seconds(6));

        for (ui64 i = 0; i < messages; ++i) {
            auto key = CreateGuidAsString();
            std::string payload = "data";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            msg.Key(key);
            UNIT_ASSERT_C(producer->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        UNIT_ASSERT_C(producer->Flush().GetValueSync().IsSuccess(), "Failed to flush producer");
        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(10)).IsSuccess(), "Failed to close producer");
    }

    Y_UNIT_TEST(KeyedWriteSession_BoundPartitionChooser_SplitPartition_MultiThreadedAcksOrder) {
        NKikimr::NPQ::NTest::TTopicSdkTestSetup setup = NKikimr::NPQ::NTest::CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        auto client = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Bound);
        writeSettings.PartitioningKeyHasher([](const std::string_view key) -> std::string {
            return std::string{key};
        });
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto producer = client.CreateProducer(writeSettings);

        constexpr ui64 messages = 1000;

        std::jthread writer([&]() {
            for (ui64 i = 1; i <= messages; ++i) {
                auto key = CreateGuidAsString();
                std::string payload = "data";   
                TWriteMessage msg(payload);
                msg.SeqNo(i);
                msg.Key(key);
                UNIT_ASSERT_C(producer->Write(std::move(msg)).IsSuccess(), "Failed to write message");
            }
        });

        std::jthread splitter([&]() {
            Sleep(TDuration::Seconds(1));
            ui64 txId = 1006;
            NKikimr::NPQ::NTest::SplitPartition(setup, ++txId, 0, "a");
        });

        writer.join();
        splitter.join();

        UNIT_ASSERT_C(producer->Flush().GetValueSync().IsSuccess(), "Failed to flush producer");
        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(30)).IsSuccess(), "Failed to close producer");
    }

    Y_UNIT_TEST(KeyedWriteSession_CloseTimeout) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.MaxBlock(TDuration::Seconds(30));

        auto producer = client.CreateProducer(writeSettings);

        for (int i = 0; i < 1000; ++i) {
            std::string payload = "message-" + ToString(i);
            TWriteMessage msg(payload);
            msg.SeqNo(i + 1);
            msg.Key("key1");
            UNIT_ASSERT_C(producer->Write(std::move(msg)).IsSuccess(), "Failed to write message");
        }

        // Test Close timeout
        const TDuration closeTimeout = TDuration::Seconds(5);
        const TInstant startTime = TInstant::Now();
        auto result = producer->Close(closeTimeout);
        // Close may legitimately return Timeout if there are still in-flight messages
        // at the moment of deadline. For this test we only require that Close doesn't
        // block longer than the timeout and that the session eventually closes successfully.
        UNIT_ASSERT_C(
            result.IsSuccess() || result.IsTimeout(),
            TStringBuilder() << "Failed to close keyed write session, status: " << static_cast<int>(result.Status)
        );
        const TDuration actualDuration = TInstant::Now() - startTime;
        
        // Verify that Close didn't block longer than timeout (with some tolerance)
        const TDuration maxExpectedDuration = closeTimeout + TDuration::MilliSeconds(100) + closeTimeout / 10;
        UNIT_ASSERT_C(
            actualDuration <= maxExpectedDuration + maxExpectedDuration / 10,
            TStringBuilder() << "Close() took " << actualDuration << " but timeout was " << closeTimeout
        );
    }

    Y_UNIT_TEST(AutoPartitioning_Producer) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();

        CreateTopicWithAutoPartitioning(client);

        auto describe = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 2);

        TProducerSettings writeSettings1;
        writeSettings1
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings1.ProducerIdPrefix("autopartitioning_keyed_1");
        writeSettings1.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Bound);
        writeSettings1.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings1.MaxBlock(TDuration::Seconds(30));

        TProducerSettings writeSettings2 = writeSettings1;
        writeSettings2.ProducerIdPrefix("autopartitioning_keyed_2");

        auto producer1 = client.CreateProducer(writeSettings1);
        auto producer2 = client.CreateProducer(writeSettings2);
        auto msgData = TString(1_MB, 'a');

        std::vector<std::string> keys;
        for (const auto& partition : describe.GetTopicDescription().GetPartitions()) {
            keys.push_back(partition.GetFromBound().value_or(""));
        }

        auto writeMessage = [&](std::shared_ptr<IProducer> s, std::string_view payload, ui64 seqNo) {
            auto key = keys[seqNo % keys.size()];
            if (key.empty()) {
                key = "lalala";
            }
            UNIT_ASSERT_C(s->Write(CreateMessage(payload, key, seqNo)).IsSuccess(), "Failed to write message");
        };

        {
            writeMessage(producer1, msgData, 1);
            writeMessage(producer1, msgData, 2);
            Sleep(TDuration::Seconds(5));
            auto d = client.DescribeTopic(TEST_TOPIC).GetValueSync();
            UNIT_ASSERT_EQUAL(d.GetTopicDescription().GetPartitions().size(), 2);
        }

        {
            writeMessage(producer1, msgData, 3);
            writeMessage(producer1, msgData, 4);
            writeMessage(producer1, msgData, 5);
            writeMessage(producer1, msgData, 6);
            writeMessage(producer1, msgData, 7);
            writeMessage(producer2, msgData, 8);
            writeMessage(producer1, msgData, 9);
            writeMessage(producer1, msgData, 10);
            writeMessage(producer2, msgData, 11);
            writeMessage(producer1, msgData, 12);
            UNIT_ASSERT_C(producer1->Flush().GetValueSync().IsSuccess(), "Failed to flush producer1");
            UNIT_ASSERT_C(producer2->Flush().GetValueSync().IsSuccess(), "Failed to flush producer2");
            Sleep(TDuration::Seconds(5));
        }

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        auto partitionsCount = describeResult.GetTopicDescription().GetPartitions().size();
        UNIT_ASSERT_C(partitionsCount >= 4,
            TStringBuilder() << "Partitions count: " << partitionsCount << ", expected at least 4");

        writeMessage(producer1, msgData, 13);
        writeMessage(producer1, msgData, 14);
        UNIT_ASSERT_C(producer1->Flush().GetValueSync().IsSuccess(), "Failed to flush producer1");
        UNIT_ASSERT_C(producer2->Flush().GetValueSync().IsSuccess(), "Failed to flush producer2");

        TProducerSettings writeSettings3 = writeSettings1;
        writeSettings3.ProducerIdPrefix("autopartitioning_keyed_3");
        auto producer3 = client.CreateProducer(writeSettings3);

        auto partitionsMap1 = dynamic_cast<TProducer*>(producer1.get())->GetPartitionsMap();
        auto partitionsMap3 = dynamic_cast<TProducer*>(producer3.get())->GetPartitionsMap();

        for (const auto& [partitionId, partitionInfo] : partitionsMap1) {
            auto partitionInfo3 = partitionsMap3.find(partitionId);
            UNIT_ASSERT_C(partitionInfo3 != partitionsMap3.end(),
                "Partition " << partitionId << " is not in session3");
            UNIT_ASSERT_C(partitionInfo.FromBound_ == partitionInfo3->second.FromBound_,
                "From bound is not equal for partition " << partitionId);
            UNIT_ASSERT_C(partitionInfo.ToBound_ == partitionInfo3->second.ToBound_,
                "To bound is not equal for partition " << partitionId);
        }

        auto partitionsIndex1 = dynamic_cast<TProducer*>(producer1.get())->GetPartitionsIndex();
        auto partitionsIndex3 = dynamic_cast<TProducer*>(producer3.get())->GetPartitionsIndex();
        for (const auto& [key, partitionId] : partitionsIndex1) {
            auto partitionId3 = partitionsIndex3.find(key);
            UNIT_ASSERT_C(partitionId3 != partitionsIndex3.end(),
                "Key " << key << " is not in session3");
            UNIT_ASSERT_EQUAL_C(partitionId, partitionId3->second,
                "Partition id is not equal for key " << key);
        }

        auto sessionPartitions = dynamic_cast<TProducer*>(producer1.get())->GetPartitions();
        UNIT_ASSERT_EQUAL_C(sessionPartitions.size(), partitionsCount,
            "Expected exactly" << partitionsCount << " partitions, actual: " << sessionPartitions.size());

        UNIT_ASSERT(producer1->Close(TDuration::Seconds(30)).IsSuccess());
        UNIT_ASSERT(producer2->Close(TDuration::Seconds(30)).IsSuccess());
        UNIT_ASSERT(producer3->Close(TDuration::Seconds(30)).IsSuccess());
    }

    Y_UNIT_TEST(AutoPartitioning_Producer_SmallMessages) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();

        CreateTopicWithAutoPartitioning(client);

        auto describe = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 2);

        TProducerSettings writeSettings1;
        writeSettings1
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings1.ProducerIdPrefix("autopartitioning_keyed_small_1");
        writeSettings1.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Bound);
        writeSettings1.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings1.MaxBlock(TDuration::Seconds(30));

        TProducerSettings writeSettings2 = writeSettings1;
        writeSettings2.ProducerIdPrefix("autopartitioning_keyed_small_2");

        auto producer1 = client.CreateProducer(writeSettings1);
        auto producer2 = client.CreateProducer(writeSettings2);
        const size_t msgSize = 256_KB;
        auto msgData = TString(msgSize, 'a');
        const ui64 totalMessages = 44;

        std::vector<std::string> keys;
        for (const auto& partition : describe.GetTopicDescription().GetPartitions()) {
            keys.push_back(partition.GetFromBound().value_or(""));
        }

        auto writeMessage = [&](std::shared_ptr<IProducer> s, std::string_view payload, ui64 seqNo) {
            auto key = keys[seqNo % keys.size()];
            if (key.empty()) key = "a";
            UNIT_ASSERT_C(s->Write(CreateMessage(payload, key, seqNo)).IsSuccess(), "Failed to write message");
        };

        {
            writeMessage(producer1, msgData, 1);
            writeMessage(producer1, msgData, 2);
            Sleep(TDuration::Seconds(5));
            auto d = client.DescribeTopic(TEST_TOPIC).GetValueSync();
            UNIT_ASSERT_EQUAL(d.GetTopicDescription().GetPartitions().size(), 2);
        }

        {
            for (ui64 seq = 3; seq <= totalMessages - 2; ++seq) {
                auto s = (seq % 4 == 0) ? producer2 : producer1;
                writeMessage(s, msgData, seq);
            }
            Sleep(TDuration::Seconds(5));
            UNIT_ASSERT_C(producer1->Flush().GetValueSync().IsSuccess(), "Failed to flush producer1");
            UNIT_ASSERT_C(producer2->Flush().GetValueSync().IsSuccess(), "Failed to flush producer2");
        }

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        auto partitionsCount = describeResult.GetTopicDescription().GetPartitions().size();
        UNIT_ASSERT_C(partitionsCount >= 3,
            TStringBuilder() << "Partitions count: " << partitionsCount << ", expected at least 3 (auto-partitioning)");

        writeMessage(producer1, msgData, totalMessages - 1);
        writeMessage(producer1, msgData, totalMessages);
        UNIT_ASSERT_C(producer1->Flush().GetValueSync().IsSuccess(), "Failed to flush producer1");
        UNIT_ASSERT_C(producer2->Flush().GetValueSync().IsSuccess(), "Failed to flush producer2");

        auto sessionPartitions = dynamic_cast<TProducer*>(producer1.get())->GetPartitions();
        UNIT_ASSERT_EQUAL_C(sessionPartitions.size(), partitionsCount,
            "Session partitions " << sessionPartitions.size() << " != topic partitions " << partitionsCount);

        UNIT_ASSERT(producer1->Close(TDuration::Seconds(30)).IsSuccess());
        UNIT_ASSERT(producer2->Close(TDuration::Seconds(30)).IsSuccess());
    }

    Y_UNIT_TEST(Producer_BasicWrite) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        TProducerSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix("producer_basic_write");
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);

        auto producer = client.CreateProducer(writeSettings);
        auto msgData = TString(10_KB, 'a');

        for (ui64 i = 0; i < 100; ++i) {    
            UNIT_ASSERT(producer->Write(TWriteMessage(msgData)).IsSuccess());
        }

        UNIT_ASSERT(producer->Flush().GetValueSync().IsSuccess());

        auto describe = client.DescribeTopic(TEST_TOPIC, TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 10);

        ui64 messagesWritten = 0;
        for (const auto& partition : describe.GetTopicDescription().GetPartitions()) {
            auto stats = partition.GetPartitionStats();
            UNIT_ASSERT(stats);
            messagesWritten += stats->GetEndOffset() - stats->GetStartOffset();
        }
    
        UNIT_ASSERT_EQUAL(messagesWritten, 100);
        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(1)).IsSuccess(), "Failed to close producer");
    }

    Y_UNIT_TEST(TypedProducer_BasicWrite) {
        struct TExample {
            std::string Payload;
            std::string Serialize() const { return Payload; }
        };

        constexpr ui64 messageCount = 100;

        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        TProducerSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix("producer_basic_write");
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.KeyProducer([](const TWriteMessage& message) -> std::string {
            return ToString(MurmurHash<std::uint64_t>(message.Data.data(), message.Data.size()));
        });
        writeSettings.MaxBlock(TDuration::Seconds(1));

        auto producer = client.CreateTypedProducer<TExample>(writeSettings);

        std::vector<std::string> sentPayloads;
        sentPayloads.reserve(messageCount);
        for (ui64 i = 0; i < messageCount; ++i) {
            auto payload = CreateGuidAsString();
            sentPayloads.push_back(payload);
            UNIT_ASSERT(producer->Write(TExample{.Payload = payload}).IsSuccess());
        }

        UNIT_ASSERT(producer->Flush().GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(producer->GetWriteStats().MessagesWritten, messageCount);
        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(1)).IsSuccess(), "Failed to close producer");

        std::vector<std::string> receivedPayloads;
        receivedPayloads.reserve(messageCount);
        NThreading::TPromise<void> allReadPromise = NThreading::NewPromise<void>();

        auto readSettings = TReadSessionSettings()
            .ConsumerName(setup.GetConsumerName())
            .AppendTopics(setup.GetTopicPath(TEST_TOPIC));

        readSettings.EventHandlers_.SimpleDataHandlers([&](TReadSessionEvent::TDataReceivedEvent& ev) {
            for (auto& msg : ev.GetMessages()) {
                receivedPayloads.push_back(std::string(msg.GetData()));
                msg.Commit();
            }
            if (receivedPayloads.size() >= messageCount) {
                allReadPromise.SetValue();
            }
        });

        auto readSession = client.CreateReadSession(readSettings);
        UNIT_ASSERT(allReadPromise.GetFuture().Wait(TDuration::Seconds(30)));
        readSession->Close(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(receivedPayloads.size(), messageCount);

        std::sort(sentPayloads.begin(), sentPayloads.end());
        std::sort(receivedPayloads.begin(), receivedPayloads.end());
        UNIT_ASSERT_VALUES_EQUAL(sentPayloads, receivedPayloads);
    }

    Y_UNIT_TEST(Producer_SmallSessionIdleTimeout) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 5);

        TProducerSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix("producer_basic_write");
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::MilliSeconds(500));
        writeSettings.MaxBlock(TDuration::Seconds(1));

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        const auto& partitions = describeResult.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_EQUAL(partitions.size(), 5);

        auto producer = client.CreateProducer(writeSettings);
        auto producerRaw = dynamic_cast<TProducer*>(producer.get());
        auto msgData = TString(10_KB, 'a');

        for (ui64 i = 0; i < 3; ++i) {
            for (const auto& partition : partitions) {
                for (ui64 i = 0; i < 10; ++i) {
                    TWriteMessage msg(msgData);
                    msg.Partition(partition.GetPartitionId());
                    UNIT_ASSERT(producer->Write(std::move(msg)).IsSuccess());
                }
                UNIT_ASSERT(producer->Flush().GetValueSync().IsSuccess());    
                UNIT_ASSERT((producerRaw->GetIdleSessionsCount() == 1 && producerRaw->GetSessionsCount() == 1) ||
                    (producerRaw->GetIdleSessionsCount() == 0 && producerRaw->GetSessionsCount() == 0));
                Sleep(TDuration::Seconds(1));
            }
        }

        {
            auto describeResult = client.DescribeTopic(TEST_TOPIC, TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
            ui64 messagesWritten = 0;
            for (const auto& partition : describeResult.GetTopicDescription().GetPartitions()) {
                auto stats = partition.GetPartitionStats();
                UNIT_ASSERT(stats);
                messagesWritten += stats->GetEndOffset() - stats->GetStartOffset();
            }
            UNIT_ASSERT_EQUAL(messagesWritten, 150);
        }
        UNIT_ASSERT(producer->Close(TDuration::Seconds(1)).IsSuccess());
    }

    Y_UNIT_TEST(Producer_CustomKeyProducerFunction) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);

        // Capture partition ids in the same order as DescribeTopic returns them
        // (the keyed session uses the same DescribeTopic ordering to map hash bucket -> partition id).
        auto publicClient = setup.MakeClient();
        auto describeTopicSettings = TDescribeTopicSettings().IncludeStats(true);
        auto before = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();
        UNIT_ASSERT_C(before.IsSuccess(), before.GetIssues().ToOneLineString());
        const auto& beforePartitions = before.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(beforePartitions.size(), 2);
        const ui64 partitionId0 = beforePartitions[0].GetPartitionId();
        const ui64 partitionId1 = beforePartitions[1].GetPartitionId();

        constexpr auto keyAttributeName = "__key";

        TProducerSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.KeyProducer([](const TWriteMessage& message) -> std::string {
            for (const auto& [attributeName, attributeValue] : message.MessageMeta_) {
                if (attributeName == keyAttributeName) {
                    return attributeValue;
                }
            }
            return "";
        });
        writeSettings.MaxBlock(TDuration::Seconds(1));

        auto producer = publicClient.CreateProducer(writeSettings);

        const std::string key0 = FindKeyForBucket(0, 2);
        const std::string key1 = FindKeyForBucket(1, 2);

        const ui64 count0 = 7;
        const ui64 count1 = 11;

        auto seqNo = 1;
        for (ui64 i = 0; i < count0; ++i) {
            std::string payload = "msg0";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            msg.MessageMeta_.emplace_back(keyAttributeName, key0);
            UNIT_ASSERT(producer->Write(std::move(msg)).IsSuccess());
        }
        for (ui64 i = 0; i < count1; ++i) {
            std::string payload = "msg1";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            msg.MessageMeta_.emplace_back(keyAttributeName, key1);
            UNIT_ASSERT(producer->Write(std::move(msg)).IsSuccess());
        }

        UNIT_ASSERT_C(producer->Close(TDuration::Seconds(10)).IsSuccess(), "Failed to close producer");
        UNIT_ASSERT_VALUES_EQUAL(producer->GetWriteStats().MessagesWritten, count0 + count1);
        UNIT_ASSERT_VALUES_EQUAL(producer->GetWriteStats().LastWrittenSeqNo, seqNo - 1);

        auto after = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();
        UNIT_ASSERT_C(after.IsSuccess(), after.GetIssues().ToOneLineString());
        const auto& afterPartitions = after.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(afterPartitions.size(), 2);

        std::unordered_map<ui64, ui64> endOffsets;
        for (const auto& p : afterPartitions) {
            auto stats = p.GetPartitionStats();
            UNIT_ASSERT(stats.has_value());
            endOffsets[p.GetPartitionId()] = stats->GetEndOffset();
        }

        auto it0 = endOffsets.find(partitionId0);
        auto it1 = endOffsets.find(partitionId1);
        UNIT_ASSERT(it0 != endOffsets.end());
        UNIT_ASSERT(it1 != endOffsets.end());

        const ui64 endOffset0 = it0->second;
        const ui64 endOffset1 = it1->second;

        // Partition ordering in DescribeTopic is not a part of public API contract, so allow swapping.
        UNIT_ASSERT_VALUES_EQUAL(endOffset0 + endOffset1, count0 + count1);
        UNIT_ASSERT_C(
            (endOffset0 == count0 && endOffset1 == count1) || (endOffset0 == count1 && endOffset1 == count0),
            TStringBuilder() << "Unexpected end offsets distribution: "
                             << "partitionId0=" << partitionId0 << " endOffset0=" << endOffset0 << ", "
                             << "partitionId1=" << partitionId1 << " endOffset1=" << endOffset1 << ", "
                             << "expected (" << count0 << "," << count1 << ") in any order"
        );
    }

    Y_UNIT_TEST(Producer_BlockingWrite) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        TProducerSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix("simple_blocking_producer_basic_write");
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.MaxBlock(TDuration::Seconds(1));

        auto producer = client.CreateProducer(writeSettings);
        auto msgData = TString(10_KB, 'a');

        for (ui64 i = 0; i < 100; ++i) {    
            UNIT_ASSERT(producer->Write(TWriteMessage(msgData)).IsSuccess());
        }

        UNIT_ASSERT(producer->Flush().GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(producer->GetWriteStats().MessagesWritten, 100);
        UNIT_ASSERT(producer->Close(TDuration::Seconds(1)).IsSuccess());
    }

    Y_UNIT_TEST(Producer_TimeoutError) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        TProducerSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix("simple_blocking_producer_basic_write");
        writeSettings.PartitionChooserStrategy(TProducerSettings::EPartitionChooserStrategy::Hash);
        writeSettings.MaxMemoryUsage(100_KB);
        writeSettings.MaxBlock(TDuration::MilliSeconds(1));

        auto producer = client.CreateProducer(writeSettings);
        auto msgData = TString(1_MB, 'a');

        UNIT_ASSERT(producer->Write(TWriteMessage(msgData)).IsSuccess());
        UNIT_ASSERT(producer->Write(TWriteMessage(msgData)).IsTimeout());
        UNIT_ASSERT(producer->Close(TDuration::Seconds(10)).IsSuccess());
    }
} // Y_UNIT_TEST_SUITE(BasicUsage)

} // namespace
