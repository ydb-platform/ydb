#include "ut_utils/topic_sdk_test_setup.h"

#include <ut/ut_utils/event_loop.h>
#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

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

#include <atomic>
#include <condition_variable>
#include <deque>
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

void WriteAndReadToEndWithRestarts(TReadSessionSettings readSettings, TWriteSessionSettings writeSettings, const std::string& message, std::uint32_t count, TTopicSdkTestSetup& setup, std::shared_ptr<TManagedExecutor> decompressor) {
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


    auto WaitTasks = [&](auto f, size_t c) {
        while (f() < c) {
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

        setup.GetServer().KillTopicPqrbTablet(setup.GetTopicPath());
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

    std::uint32_t i = 0;
    while (AtomicGet(lastOffset) + 1 < count) {
        RunTasks(decompressor, {i++});
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

    Y_UNIT_TEST(KeyedWriteSession_UserEventHandlers) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));

        std::atomic<size_t> readyCount{0};
        std::atomic<size_t> acksCount{0};
        std::atomic<size_t> closedCount{0};
        std::atomic<size_t> commonCount{0};

        std::mutex tokensMutex;
        std::condition_variable tokensCv;
        std::deque<TContinuationToken> readyTokens;

        writeSettings.EventHandlers_.HandlersExecutor(std::make_shared<TSyncExecutor>());

        writeSettings.EventHandlers_.ReadyToAcceptHandler(
            [&](TWriteSessionEvent::TReadyToAcceptEvent& ev) {
                readyCount.fetch_add(1);
                {
                    std::lock_guard lock(tokensMutex);
                    readyTokens.emplace_back(std::move(ev.ContinuationToken));
                }
                tokensCv.notify_one();
            });

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

        writeSettings.EventHandlers_.CommonHandler(
            [&](TWriteSessionEvent::TEvent& ev) {
                Y_UNUSED(ev);
                commonCount.fetch_add(1);
            });

        auto getReadyToken = [&]() -> std::optional<TContinuationToken> {
            std::unique_lock lock(tokensMutex);
            tokensCv.wait_for(lock, std::chrono::seconds(30), [&]() { return !readyTokens.empty(); });
            if (readyTokens.empty()) {
                return std::nullopt;
            }
            auto token = std::move(readyTokens.front());
            readyTokens.pop_front();
            return token;
        };

        auto session = client.CreateKeyedWriteSession(writeSettings);

        const ui64 messages = 5;
        for (ui64 i = 0; i < messages; ++i) {
            auto token = getReadyToken();
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "payload";
            TWriteMessage msg(payload);
            msg.SeqNo(i + 1);
            session->Write(std::move(*token), "key-" + ToString(i), std::move(msg));
        }

        UNIT_ASSERT_C(session->Close(TDuration::Seconds(30)), "Failed to close keyed write session");

        UNIT_ASSERT_C(readyCount.load() > 0, "ReadyToAcceptHandler was not called");
        UNIT_ASSERT_C(acksCount.load() == messages, "AcksHandler does not work properly");
        UNIT_ASSERT_C(closedCount.load() > 0, "SessionClosedHandler was not called");
        UNIT_ASSERT_C(commonCount.load() == 0, "CommonHandler should not be called when type-specific handlers are set");
    }

    Y_UNIT_TEST(KeyedWriteSession_ProducerIdPrefixRequired) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));

        UNIT_ASSERT_EXCEPTION(setup.MakeClient().CreateKeyedWriteSession(writeSettings), TContractViolation);
    }

    Y_UNIT_TEST(KeyedWriteSession_SessionClosedDueToUserError) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);
        auto publicClient = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));

        auto session = publicClient.CreateKeyedWriteSession(writeSettings);
        TKeyedWriteSessionEventLoop eventLoop(session);
        auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));

        std::string payload = "msg0";
        TWriteMessage msg(payload);
        msg.SeqNo(0);
        session->Write(std::move(*token), "key", std::move(msg));

        auto readyToAcceptEvent = session->GetEvent(false);
        UNIT_ASSERT_C(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(*readyToAcceptEvent), "ReadyToAcceptEvent is not received");

        UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(1000)), "Timed out waiting for event");
        auto event = session->GetEvent(false);
        UNIT_ASSERT_C(event, "Event is not received");
        auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event);
        UNIT_ASSERT_C(sessionClosedEvent, "SessionClosedEvent is not received");
        UNIT_ASSERT_C(sessionClosedEvent->GetStatus() == EStatus::BAD_REQUEST, "Status is not BAD_REQUEST");
        UNIT_ASSERT(!session->Close(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(KeyedWriteSession_NoAutoPartitioning_HashPartitionChooser) {
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

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));

        auto session = publicClient.CreateKeyedWriteSession(writeSettings);

        const std::string key0 = FindKeyForBucket(0, 2);
        const std::string key1 = FindKeyForBucket(1, 2);

        const ui64 count0 = 7;
        const ui64 count1 = 11;

        TKeyedWriteSessionEventLoop eventLoop(session);

        auto seqNo = 1;
        for (ui64 i = 0; i < count0; ++i) {
            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");

            std::string payload = "msg0";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            session->Write(std::move(*token), key0, std::move(msg));
        }
        for (ui64 i = 0; i < count1; ++i) {
            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "msg1";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            session->Write(std::move(*token), key1, std::move(msg));
        }

        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));

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

    Y_UNIT_TEST(KeyedWriteSession_NoAutoPartitioning_BoundPartitionChooser) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 5, 10);

        auto publicClient = setup.MakeClient();
        auto describeTopicSettings = TDescribeTopicSettings().IncludeStats(true);
        auto before = publicClient.DescribeTopic(setup.GetTopicPath(TEST_TOPIC), describeTopicSettings).GetValueSync();

        UNIT_ASSERT_C(before.IsSuccess(), before.GetIssues().ToOneLineString());
        const auto& beforePartitions = before.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(beforePartitions.size(), 5);

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitioningKeyHasher([](const std::string_view key) -> std::string {
            return std::string{key};
        });

        auto session = publicClient.CreateKeyedWriteSession(writeSettings);
        auto keyedSession = std::dynamic_pointer_cast<TKeyedWriteSession>(session);
        const auto& partitions = keyedSession->GetPartitions();

        TKeyedWriteSessionEventLoop eventLoop(session);
        
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

            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "msg";
            TWriteMessage msg(payload);
            msg.SeqNo(i + 1);
            session->Write(std::move(*token), key, std::move(msg));
        }

        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));

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

    Y_UNIT_TEST(KeyedWriteSession_EventLoop_Acks) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 4);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(10));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);

        auto session = client.CreateKeyedWriteSession(writeSettings);
        TKeyedWriteSessionEventLoop eventLoop(session);

        const ui64 count = 3000;
        for (ui64 i = 1; i <= count; ++i) {
            auto key = CreateGuidAsString();
            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "data";
            TWriteMessage msg(payload);
            msg.SeqNo(i);
            session->Write(std::move(*token), key, std::move(msg));
        }

        UNIT_ASSERT(eventLoop.WaitForAcks(count, TDuration::Seconds(60)));
        eventLoop.CheckAcksOrder();
        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(KeyedWriteSession_MultiThreadedWrite_Acks) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);

        auto session = client.CreateKeyedWriteSession(writeSettings);

        constexpr ui64 threadsCount = 4;
        constexpr ui64 perThread = 25;
        constexpr ui64 total = threadsCount * perThread;

        std::atomic<ui64> nextSeqNo{1};
        std::vector<std::jthread> threads;
        threads.reserve(threadsCount);

        TKeyedWriteSessionEventLoop eventLoop(session);

        for (ui64 t = 0; t < threadsCount; ++t) {
            threads.emplace_back([&, t]() {
                auto key = TStringBuilder() << "key-" << t;
                for (ui64 i = 0; i < perThread; ++i) {
                    std::cout << "thread " << t << " writing message " << i << std::endl;
                    auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
                    UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
                    const ui64 seqNo = nextSeqNo.fetch_add(1);
                    std::string payload = "data";
                    TWriteMessage msg(payload);
                    msg.SeqNo(seqNo);
                    session->Write(std::move(*token), key, std::move(msg));
                }
            });
        }

        UNIT_ASSERT(eventLoop.WaitForAcks(total, TDuration::Seconds(60)));
        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(KeyedWriteSession_IdleSessionsTimeout) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(5));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);

        auto session = client.CreateKeyedWriteSession(writeSettings);

        TKeyedWriteSessionEventLoop eventLoop(session);
        constexpr ui64 messages = 100;
        ui64 seqNo = 1;

        for (ui64 i = 0; i < messages; ++i) {
            auto key = CreateGuidAsString();
            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "data";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            session->Write(std::move(*token), key, std::move(msg));
        }

        UNIT_ASSERT(eventLoop.WaitForAcks(messages, TDuration::Seconds(60)));
        eventLoop.CheckAcksOrder();
    
        Sleep(TDuration::Seconds(6));

        for (ui64 i = 0; i < messages; ++i) {
            auto key = CreateGuidAsString();
            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "data";
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            session->Write(std::move(*token), key, std::move(msg));
        }

        UNIT_ASSERT(eventLoop.WaitForAcks(messages * 2, TDuration::Seconds(60)));
        eventLoop.CheckAcksOrder();
    }

    Y_UNIT_TEST(KeyedWriteSession_BoundPartitionChooser_SplitPartition_MultiThreadedAcksOrder) {
        NKikimr::NPQ::NTest::TTopicSdkTestSetup setup = NKikimr::NPQ::NTest::CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound);
        writeSettings.PartitioningKeyHasher([](const std::string_view key) -> std::string {
            return std::string{key};
        });

        auto session = client.CreateKeyedWriteSession(writeSettings);

        constexpr ui64 messages = 1000;
        TKeyedWriteSessionEventLoop eventLoop(session);

        std::jthread writer([&]() {
            for (ui64 i = 1; i <= messages; ++i) {
                auto token = eventLoop.GetContinuationToken(TDuration::Seconds(30));
                UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
                auto key = CreateGuidAsString();
                std::string payload = "data";   
                TWriteMessage msg(payload);
                msg.SeqNo(i);
                session->Write(std::move(*token), key, std::move(msg));
            }
        });

        std::jthread splitter([&]() {
            Sleep(TDuration::Seconds(1));
            ui64 txId = 1006;
            NKikimr::NPQ::NTest::SplitPartition(setup, ++txId, 0, "a");
        });

        writer.join();
        splitter.join();

        UNIT_ASSERT(eventLoop.WaitForAcks(messages, TDuration::Seconds(60)));
        eventLoop.CheckAcksOrder();
        UNIT_ASSERT(session->Close(TDuration::Seconds(30)));
    }

    Y_UNIT_TEST(SimpleBlockingKeyedWriteSession_BasicWrite) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 5);

        auto client = setup.MakeClient();
        
        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
        
        auto session = client.CreateSimpleBlockingKeyedWriteSession(writeSettings);

        const std::string key1 = "key1";
        const std::string key2 = "key2";
        
        // Write several messages with different keys
        size_t seqNo = 1;
        for (int i = 0; i < 5; ++i) {
            std::string payload = "message1-" + ToString(i);
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            bool res = session->Write(key1, std::move(msg));
            UNIT_ASSERT(res);
        }
        
        for (int i = 0; i < 5; ++i) {
            std::string payload = "message2-" + ToString(i);
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            bool res = session->Write(key2, std::move(msg));
            UNIT_ASSERT(res);
        }
        
        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(SimpleBlockingKeyedWriteSession_NoSeqNo) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);

        auto session = client.CreateSimpleBlockingKeyedWriteSession(writeSettings);

        const ui64 messages = 10;
        for (ui64 i = 0; i < messages; ++i) {
            std::string payload = "payload-" + ToString(i);
            TWriteMessage msg(payload);
            bool res = session->Write("key-" + ToString(i % 3), std::move(msg));
            UNIT_ASSERT(res);
        }

        bool closeRes = session->Close(TDuration::Seconds(30));
        UNIT_ASSERT(closeRes);
    }

    Y_UNIT_TEST(SimpleBlockingKeyedWriteSession_ManyMessages) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 4);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);

        auto session = client.CreateSimpleBlockingKeyedWriteSession(writeSettings);

        ui64 seqNo = 1;

        for (ui64 i = 0; i < 1000; ++i) {
            auto key = CreateGuidAsString();
            std::string payload = "payload-" + ToString(seqNo);
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo++);
            bool res = session->Write(key, std::move(msg));
            UNIT_ASSERT(res);
        }

        bool closeRes = session->Close(TDuration::Seconds(60));
        UNIT_ASSERT(closeRes);
    }

    Y_UNIT_TEST(KeyedWriteSession_CloseTimeout) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 3);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings.ProducerIdPrefix(CreateGuidAsString());
        writeSettings.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash);
        writeSettings.SubSessionIdleTimeout(TDuration::Seconds(30));

        auto session = client.CreateKeyedWriteSession(writeSettings);

        TKeyedWriteSessionEventLoop eventLoop(session);

        for (int i = 0; i < 1000; ++i) {
            auto token = eventLoop.GetContinuationToken(TDuration::Seconds(10));
            UNIT_ASSERT_C(token, "Timed out waiting for ReadyToAcceptEvent");
            std::string payload = "message-" + ToString(i);
            TWriteMessage msg(payload);
            msg.SeqNo(i + 1);
            session->Write(std::move(*token), "key1", std::move(msg));
        }

        // Test Close timeout
        const TDuration closeTimeout = TDuration::Seconds(2);
        const TInstant startTime = TInstant::Now();
        session->Close(closeTimeout);
        const TDuration actualDuration = TInstant::Now() - startTime;
        
        // Verify that Close didn't block longer than timeout (with some tolerance)
        const TDuration maxExpectedDuration = closeTimeout + TDuration::MilliSeconds(100) + closeTimeout / 10;
        UNIT_ASSERT_C(
            actualDuration <= maxExpectedDuration + maxExpectedDuration / 10,
            TStringBuilder() << "Close() took " << actualDuration << " but timeout was " << closeTimeout
        );

        int attempts = 0;
        constexpr int maxAttempts = 1100;
        for (attempts = 0; attempts < maxAttempts; ++attempts) {
            auto event = session->GetEvent(false);
            if (!event) {
                break;
            }
            
            auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event);
            if (!sessionClosedEvent) {
                continue;
            }
    
            UNIT_ASSERT(sessionClosedEvent->IsSuccess());
            break;
        }

        UNIT_ASSERT(attempts < maxAttempts);
    }

    Y_UNIT_TEST(AutoPartitioning_KeyedWriteSession) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();

        std::queue<TContinuationToken> readyTokens1;
        std::queue<TContinuationToken> readyTokens2;
        std::optional<TSessionClosedEvent> sessionClosedEvent;
        std::unordered_set<ui64> ackedSeqNos;
        bool closed = false;

        auto createMessage = [](std::string_view payload, ui64 seqNo) -> TWriteMessage {
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo);
            return msg;
        };

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

        auto describe = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 2);

        TKeyedWriteSessionSettings writeSettings1;
        writeSettings1
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings1.ProducerIdPrefix("autopartitioning_keyed_1");
        writeSettings1.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound);
        writeSettings1.SubSessionIdleTimeout(TDuration::Seconds(30));

        TKeyedWriteSessionSettings writeSettings2 = writeSettings1;
        writeSettings2.ProducerIdPrefix("autopartitioning_keyed_2");

        auto session1 = client.CreateKeyedWriteSession(writeSettings1);
        auto session2 = client.CreateKeyedWriteSession(writeSettings2);
        auto msgData = TString(1_MB, 'a');

        std::vector<std::string> keys;
        for (const auto& partition : describe.GetTopicDescription().GetPartitions()) {
            keys.push_back(partition.GetFromBound().value_or(""));
        }

        auto getQueue = [&](const std::shared_ptr<IKeyedWriteSession>& s) -> std::queue<TContinuationToken>& {
            if (s == session1) {
                return readyTokens1;
            }
            if (s == session2) {
                return readyTokens2;
            }
            Y_ABORT("Unknown session pointer in AutoPartitioning_KeyedWriteSession");
        };

        auto eventLoop = [&](std::shared_ptr<IKeyedWriteSession> s) {
            while (true) {
                auto event = s->GetEvent(false);
                if (!event) {
                    break;
                }
                if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                    getQueue(s).push(std::move(ready->ContinuationToken));
                    continue;
                }
                if (auto* closedEv = std::get_if<TSessionClosedEvent>(&*event)) {
                    sessionClosedEvent = std::move(*closedEv);
                    closed = true;
                    break;
                }
                if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
                    for (const auto& ack : acks->Acks) {
                        UNIT_ASSERT_C(
                            ackedSeqNos.insert(ack.SeqNo).second,
                            "Duplicate ack for seqNo " << ack.SeqNo);
                    }
                }
            }
        };

        auto getReadyToken = [&](std::shared_ptr<IKeyedWriteSession> s) -> std::optional<TContinuationToken> {
            auto& q = getQueue(s);
            while (q.empty() && !closed) {
                s->WaitEvent().Wait(TDuration::Seconds(5));
                eventLoop(s);
            }
            if (q.empty()) {
                return std::nullopt;
            }
            auto t = std::move(q.front());
            q.pop();
            return t;
        };

        auto writeMessage = [&](std::shared_ptr<IKeyedWriteSession> s, std::string_view payload, ui64 seqNo) {
            auto token = getReadyToken(s);
            UNIT_ASSERT(token);
            auto key = keys[seqNo % keys.size()];
            if (key.empty()) {
                key = "lalala";
            }
            s->Write(std::move(*token), key, createMessage(payload, seqNo));
        };

        {
            writeMessage(session1, msgData, 1);
            writeMessage(session1, msgData, 2);
            Sleep(TDuration::Seconds(5));
            auto d = client.DescribeTopic(TEST_TOPIC).GetValueSync();
            UNIT_ASSERT_EQUAL(d.GetTopicDescription().GetPartitions().size(), 2);
        }

        {
            writeMessage(session1, msgData, 3);
            writeMessage(session1, msgData, 4);
            writeMessage(session1, msgData, 5);
            writeMessage(session1, msgData, 6);
            writeMessage(session1, msgData, 7);
            writeMessage(session2, msgData, 8);
            writeMessage(session1, msgData, 9);
            writeMessage(session1, msgData, 10);
            writeMessage(session2, msgData, 11);
            writeMessage(session1, msgData, 12);
            Sleep(TDuration::Seconds(30));
            for (int i = 0; i < 50 && ackedSeqNos.size() < 12 && !closed; ++i) {
                eventLoop(session1);
                eventLoop(session2);
                if (ackedSeqNos.size() < 12) {
                    Sleep(TDuration::MilliSeconds(200));
                }
            }
            UNIT_ASSERT_EQUAL_C(ackedSeqNos.size(), 12,
                "Expected exactly 12 distinct acks, each seqNo exactly once; got " << ackedSeqNos.size());
        }

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        auto partitionsCount = describeResult.GetTopicDescription().GetPartitions().size();
        UNIT_ASSERT_C(partitionsCount >= 4,
            TStringBuilder() << "Partitions count: " << partitionsCount << ", expected at least 4");

        writeMessage(session1, msgData, 13);
        writeMessage(session1, msgData, 14);
        Sleep(TDuration::Seconds(20));
        for (int i = 0; i < 50 && ackedSeqNos.size() < 14 && !closed; ++i) {
            eventLoop(session1);
            eventLoop(session2);
            if (ackedSeqNos.size() < 14) {
                Sleep(TDuration::MilliSeconds(200));
            }
        }

        TKeyedWriteSessionSettings writeSettings3 = writeSettings1;
        writeSettings3.ProducerIdPrefix("autopartitioning_keyed_3");
        auto session3 = client.CreateKeyedWriteSession(writeSettings3);

        auto partitionsMap1 = dynamic_cast<TKeyedWriteSession*>(session1.get())->GetPartitionsMap();
        auto partitionsMap3 = dynamic_cast<TKeyedWriteSession*>(session3.get())->GetPartitionsMap();

        for (const auto& [partitionId, partitionInfo] : partitionsMap1) {
            auto partitionInfo3 = partitionsMap3.find(partitionId);
            UNIT_ASSERT_C(partitionInfo3 != partitionsMap3.end(),
                "Partition " << partitionId << " is not in session3");
            UNIT_ASSERT_C(partitionInfo.FromBound_ == partitionInfo3->second.FromBound_,
                "From bound is not equal for partition " << partitionId);
            UNIT_ASSERT_C(partitionInfo.ToBound_ == partitionInfo3->second.ToBound_,
                "To bound is not equal for partition " << partitionId);
        }

        auto partitionsIndex1 = dynamic_cast<TKeyedWriteSession*>(session1.get())->GetPartitionsIndex();
        auto partitionsIndex3 = dynamic_cast<TKeyedWriteSession*>(session3.get())->GetPartitionsIndex();
        for (const auto& [key, partitionId] : partitionsIndex1) {
            auto partitionId3 = partitionsIndex3.find(key);
            UNIT_ASSERT_C(partitionId3 != partitionsIndex3.end(),
                "Key " << key << " is not in session3");
            UNIT_ASSERT_EQUAL_C(partitionId, partitionId3->second,
                "Partition id is not equal for key " << key);
        }

        UNIT_ASSERT_EQUAL_C(ackedSeqNos.size(), 14,
            "Expected exactly 14 distinct acks, each seqNo exactly once; got " << ackedSeqNos.size());
        auto sessionPartitions = dynamic_cast<TKeyedWriteSession*>(session1.get())->GetPartitions();
        UNIT_ASSERT_EQUAL_C(sessionPartitions.size(), partitionsCount,
            "Expected exactly" << partitionsCount << " partitions, actual: " << sessionPartitions.size());

        UNIT_ASSERT(session1->Close(TDuration::Seconds(30)));
        UNIT_ASSERT(session2->Close(TDuration::Seconds(30)));
    }

    Y_UNIT_TEST(AutoPartitioning_KeyedWriteSession_SmallMessages) {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
        TTopicSdkTestSetup setup{TEST_CASE_NAME, settings, false};
        TTopicClient client = setup.MakeClient();

        std::queue<TContinuationToken> readyTokens1;
        std::queue<TContinuationToken> readyTokens2;
        std::optional<TSessionClosedEvent> sessionClosedEvent;
        std::unordered_set<ui64> ackedSeqNos;
        bool closed = false;

        auto createMessage = [](std::string_view payload, ui64 seqNo) -> TWriteMessage {
            TWriteMessage msg(payload);
            msg.SeqNo(seqNo);
            return msg;
        };

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

        auto describe = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 2);

        TKeyedWriteSessionSettings writeSettings1;
        writeSettings1
            .Path(setup.GetTopicPath(TEST_TOPIC))
            .Codec(ECodec::RAW);
        writeSettings1.ProducerIdPrefix("autopartitioning_keyed_small_1");
        writeSettings1.PartitionChooserStrategy(TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound);
        writeSettings1.SubSessionIdleTimeout(TDuration::Seconds(30));

        TKeyedWriteSessionSettings writeSettings2 = writeSettings1;
        writeSettings2.ProducerIdPrefix("autopartitioning_keyed_small_2");

        auto session1 = client.CreateKeyedWriteSession(writeSettings1);
        auto session2 = client.CreateKeyedWriteSession(writeSettings2);
        const size_t msgSize = 256_KB;
        auto msgData = TString(msgSize, 'a');
        const ui64 totalMessages = 44;

        std::vector<std::string> keys;
        for (const auto& partition : describe.GetTopicDescription().GetPartitions()) {
            keys.push_back(partition.GetFromBound().value_or(""));
        }

        auto getQueue = [&](const std::shared_ptr<IKeyedWriteSession>& s) -> std::queue<TContinuationToken>& {
            if (s == session1) return readyTokens1;
            if (s == session2) return readyTokens2;
            Y_ABORT("Unknown session pointer in AutoPartitioning_KeyedWriteSession_SmallMessages");
        };

        auto eventLoop = [&](std::shared_ptr<IKeyedWriteSession> s) {
            while (true) {
                auto event = s->GetEvent(false);
                if (!event) break;
                if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                    getQueue(s).push(std::move(ready->ContinuationToken));
                    continue;
                }
                if (auto* closedEv = std::get_if<TSessionClosedEvent>(&*event)) {
                    sessionClosedEvent = std::move(*closedEv);
                    closed = true;
                    break;
                }
                if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
                    for (const auto& ack : acks->Acks) {
                        UNIT_ASSERT_C(ackedSeqNos.insert(ack.SeqNo).second,
                            "Duplicate ack for seqNo " << ack.SeqNo);
                    }
                }
            }
        };

        auto getReadyToken = [&](std::shared_ptr<IKeyedWriteSession> s) -> std::optional<TContinuationToken> {
            auto& q = getQueue(s);
            while (q.empty() && !closed) {
                s->WaitEvent().Wait(TDuration::Seconds(5));
                eventLoop(s);
            }
            if (q.empty()) return std::nullopt;
            auto t = std::move(q.front());
            q.pop();
            return t;
        };

        auto writeMessage = [&](std::shared_ptr<IKeyedWriteSession> s, std::string_view payload, ui64 seqNo) {
            auto token = getReadyToken(s);
            UNIT_ASSERT(token);
            auto key = keys[seqNo % keys.size()];
            if (key.empty()) key = "a";
            s->Write(std::move(*token), key, createMessage(payload, seqNo));
        };

        {
            writeMessage(session1, msgData, 1);
            writeMessage(session1, msgData, 2);
            Sleep(TDuration::Seconds(5));
            auto d = client.DescribeTopic(TEST_TOPIC).GetValueSync();
            UNIT_ASSERT_EQUAL(d.GetTopicDescription().GetPartitions().size(), 2);
        }

        {
            for (ui64 seq = 3; seq <= totalMessages - 2; ++seq) {
                auto s = (seq % 4 == 0) ? session2 : session1;
                writeMessage(s, msgData, seq);
            }
            Sleep(TDuration::Seconds(30));
            for (int i = 0; i < 80 && ackedSeqNos.size() < totalMessages - 2 && !closed; ++i) {
                eventLoop(session1);
                eventLoop(session2);
                if (ackedSeqNos.size() < totalMessages - 2) Sleep(TDuration::MilliSeconds(200));
            }
            UNIT_ASSERT_EQUAL_C(ackedSeqNos.size(), totalMessages - 2,
                "Expected " << totalMessages - 2 << " acks; got " << ackedSeqNos.size());
        }

        auto describeResult = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        auto partitionsCount = describeResult.GetTopicDescription().GetPartitions().size();
        UNIT_ASSERT_C(partitionsCount >= 3,
            TStringBuilder() << "Partitions count: " << partitionsCount << ", expected at least 3 (auto-partitioning)");

        writeMessage(session1, msgData, totalMessages - 1);
        writeMessage(session1, msgData, totalMessages);
        Sleep(TDuration::Seconds(20));
        for (int i = 0; i < 80 && ackedSeqNos.size() < totalMessages && !closed; ++i) {
            eventLoop(session1);
            eventLoop(session2);
            if (ackedSeqNos.size() < totalMessages) Sleep(TDuration::MilliSeconds(200));
        }

        UNIT_ASSERT_EQUAL_C(ackedSeqNos.size(), totalMessages,
            "Expected " << totalMessages << " acks; got " << ackedSeqNos.size());
        auto sessionPartitions = dynamic_cast<TKeyedWriteSession*>(session1.get())->GetPartitions();
        UNIT_ASSERT_EQUAL_C(sessionPartitions.size(), partitionsCount,
            "Session partitions " << sessionPartitions.size() << " != topic partitions " << partitionsCount);

        UNIT_ASSERT(session1->Close(TDuration::Seconds(30)));
        UNIT_ASSERT(session2->Close(TDuration::Seconds(30)));
    }
    
} // Y_UNIT_TEST_SUITE(BasicUsage)

} // namespace
