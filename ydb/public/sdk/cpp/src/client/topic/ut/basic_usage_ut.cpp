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

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <atomic>
#include <util/stream/zlib.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>
#include <unordered_map>
#include <unordered_set>

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
        if (std::hash<std::string>{}(key) % bucketsCount == bucket) {
            return key;
        }
    }
    UNIT_FAIL("Failed to find a key for bucket");
    return {};
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

    Y_UNIT_TEST(KeyedWriteSession_NoAutoPartitioning) {
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
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.MessageGroupId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Codec(ECodec::RAW);
        writeSettings.SessionTimeout(TDuration::Seconds(30));

        auto session = publicClient.CreateKeyedWriteSession(writeSettings);

        const std::string key0 = FindKeyForBucket(0, 2);
        const std::string key1 = FindKeyForBucket(1, 2);

        const ui64 count0 = 7;
        const ui64 count1 = 11;

        auto getReadyToken = [&](TDuration timeout) -> TContinuationToken {
            const TInstant deadline = TInstant::Now() + timeout;
            while (TInstant::Now() < deadline) {
                session->WaitEvent().Wait(TDuration::Seconds(5));
                for (auto& ev : session->GetEvents(false)) {
                    if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&ev)) {
                        return std::move(ready->ContinuationToken);
                    }
                }
            }
            UNIT_FAIL("Timed out waiting for ReadyToAcceptEvent");
            Y_ABORT("Unreachable");
        };

        for (ui64 i = 0; i < count0; ++i) {
            auto token = getReadyToken(TDuration::Seconds(30));
            session->Write(std::move(token), key0, TWriteMessage("msg0"));
        }
        for (ui64 i = 0; i < count1; ++i) {
            auto token = getReadyToken(TDuration::Seconds(30));
            session->Write(std::move(token), key1, TWriteMessage("msg1"));
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

    Y_UNIT_TEST(KeyedWriteSession_EventLoop_Acks) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.MessageGroupId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Codec(ECodec::RAW);
        writeSettings.SessionTimeout(TDuration::Seconds(30));

        auto session = client.CreateKeyedWriteSession(writeSettings);

        const std::string key = "key";
        const ui64 count = 20;

        auto getReadyToken = [&](TDuration timeout) -> TContinuationToken {
            const TInstant deadline = TInstant::Now() + timeout;
            while (TInstant::Now() < deadline) {
                session->WaitEvent().Wait(TDuration::Seconds(5));
                for (auto& ev : session->GetEvents(false)) {
                    if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&ev)) {
                        return std::move(ready->ContinuationToken);
                    }
                    // ignore acks here
                }
            }
            UNIT_FAIL("Timed out waiting for ReadyToAcceptEvent");
            Y_ABORT("Unreachable");
        };

        // Typical event-loop usage: take token from ReadyToAccept, then Write with that token.
        for (ui64 i = 1; i <= count; ++i) {
            auto token = getReadyToken(TDuration::Seconds(30));
            auto msg = TWriteMessage("data");
            msg.SeqNo(i);
            session->Write(std::move(token), key, std::move(msg));
        }

        std::unordered_set<ui64> ackedSeqNos;
        ackedSeqNos.reserve(count);
        std::vector<ui64> ackOrder;
        ackOrder.reserve(count);

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (ackedSeqNos.size() < count && TInstant::Now() < deadline) {
            session->WaitEvent().Wait(TDuration::Seconds(5));
            for (auto& ev : session->GetEvents(false)) {
                if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&ev)) {
                    for (const auto& ack : acks->Acks) {
                        if (ackedSeqNos.insert(ack.SeqNo).second) {
                            ackOrder.push_back(ack.SeqNo);
                        }
                    }
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(ackedSeqNos.size(), count);
        UNIT_ASSERT_VALUES_EQUAL(ackOrder.size(), count);
        for (ui64 i = 0; i < count; ++i) {
            UNIT_ASSERT_C(
                ackOrder[i] == i + 1,
                TStringBuilder() << "Unexpected ack order at index " << i
                                 << ": got SeqNo=" << ackOrder[i]
                                 << ", expected SeqNo=" << (i + 1)
            );
        }
        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(KeyedWriteSession_MultiThreadedWrite_Acks) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        auto client = setup.MakeClient();

        TKeyedWriteSessionSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath(TEST_TOPIC));
        writeSettings.MessageGroupId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Codec(ECodec::RAW);
        writeSettings.SessionTimeout(TDuration::Seconds(30));

        auto session = client.CreateKeyedWriteSession(writeSettings);

        constexpr ui64 threadsCount = 4;
        constexpr ui64 perThread = 25;
        constexpr ui64 total = threadsCount * perThread;

        std::unordered_set<ui64> ackedSeqNos;
        ackedSeqNos.reserve(total);

        std::deque<TContinuationToken> readyTokens;
        std::mutex tokensLock;
        std::condition_variable tokensCv;

        std::mutex ackLock;
        std::condition_variable ackCv;
        std::atomic_bool stopEventLoop{false};
        std::thread eventLoop([&]() {
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
            while (!stopEventLoop.load() && TInstant::Now() < deadline) {
                session->WaitEvent().Wait(TDuration::Seconds(5));
                for (auto& ev : session->GetEvents(false)) {
                    if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&ev)) {
                        {
                            std::lock_guard g(tokensLock);
                            readyTokens.push_back(std::move(ready->ContinuationToken));
                        }
                        tokensCv.notify_one();
                    } else if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&ev)) {
                        bool done = false;
                        {
                            std::lock_guard g(ackLock);
                            for (const auto& ack : acks->Acks) {
                                ackedSeqNos.insert(ack.SeqNo);
                            }
                            done = (ackedSeqNos.size() >= total);
                        }
                        std::cerr << "acks: " << ackedSeqNos.size() << std::endl;
                        ackCv.notify_all();
                        if (done) {
                            stopEventLoop.store(true);
                            tokensCv.notify_all();
                            return;
                        }
                    }
                }
            }
        });

        std::atomic<ui64> nextSeqNo{1};
        std::vector<std::thread> threads;
        threads.reserve(threadsCount);

        auto popToken = [&]() -> TContinuationToken {
            std::unique_lock lk(tokensLock);
            tokensCv.wait(lk, [&]() { return !readyTokens.empty() || stopEventLoop.load(); });
            UNIT_ASSERT_C(!readyTokens.empty(), "No ready tokens available");
            auto t = std::move(readyTokens.front());
            readyTokens.pop_front();
            return t;
        };

        for (ui64 t = 0; t < threadsCount; ++t) {
            threads.emplace_back([&, t]() {
                auto key = TStringBuilder() << "key-" << t;
                for (ui64 i = 0; i < perThread; ++i) {
                    std::cerr << "thread " << t << " writing message " << i << std::endl;
                    auto token = popToken();
                    const ui64 seqNo = nextSeqNo.fetch_add(1);
                    auto msg = TWriteMessage("data");
                    msg.SeqNo(seqNo);
                    session->Write(std::move(token), key, std::move(msg));
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        {
            std::unique_lock lk(ackLock);
            ackCv.wait_for(lk, std::chrono::seconds(60), [&]() { return ackedSeqNos.size() >= total; });
        }

        stopEventLoop.store(true);
        tokensCv.notify_all();
        eventLoop.join();

        UNIT_ASSERT_VALUES_EQUAL(ackedSeqNos.size(), total);
        UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
    }

} // Y_UNIT_TEST_SUITE(BasicUsage)

} // namespace
