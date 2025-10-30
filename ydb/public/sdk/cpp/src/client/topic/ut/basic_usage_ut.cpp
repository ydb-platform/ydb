#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/stream/zlib.h>


using namespace std::chrono_literals;

static const bool EnableDirectRead = !std::string{std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") ? std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") : ""}.empty();


namespace NYdb::inline Dev::NTopic::NTests {

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
                    .Enabled(true)
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
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterPolicy::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "deadLetterQueue-topic");
    }

    Y_UNIT_TEST(CreateTopicWithSharedConsumer_DeleteDeadLetterPolicy) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};

        TTopicClient client(setup.MakeDriver());

        TCreateTopicSettings topics;
        topics.BeginAddConsumer()
                .ConsumerName("shared_consumer_name")
                .ConsumerType(EConsumerType::Shared)
                .DefaultProcessingTimeout(TDuration::Seconds(7))
                .KeepMessagesOrder(true)
                .BeginDeadLetterPolicy()
                    .Enabled(true)
                    .BeginCondition()
                        .MaxProcessingAttempts(11)
                    .EndCondition()
                    .DeleteAction()
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
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterPolicy::Delete);
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
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterPolicy::Unspecified);
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
                        .BeginCondition()
                            .MaxProcessingAttempts(11)
                        .EndCondition()
                        .MoveAction("deadLetterQueue-topic")
                    .EndDeadLetterPolicy()
                .EndAddConsumer();

            auto status = client.CreateTopic("topic_name", topics).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToOneLineString());
        }

        {
            TAlterTopicSettings topics;
            topics.BeginAlterConsumer()
                    .ConsumerName("shared_consumer_name")
                    .ConsumerType(EConsumerType::Shared)
                    .DefaultProcessingTimeout(TDuration::Seconds(13))
                    .BeginDeadLetterPolicy()
                        .Enabled(true)
                        .BeginCondition()
                            .MaxProcessingAttempts(17)
                        .EndCondition()
                        .AlterMoveAction("deadLetterQueue-topic-new")
                    .EndDeadLetterPolicy()
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
        UNIT_ASSERT_VALUES_EQUAL(c.GetConsumerType(), EConsumerType::Shared);
        UNIT_ASSERT_VALUES_EQUAL(c.GetKeepMessagesOrder(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeout(), TDuration::Seconds(13));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetEnabled(), true);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetCondition().GetMaxProcessingAttempts(), 17);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetAction(), EDeadLetterPolicy::Move);
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterPolicy().GetDeadLetterQueue(), "deadLetterQueue-topic");
    }


    Y_UNIT_TEST(ReadWithoutConsumerWithRestarts) {
        if (EnableDirectRead) {
            // TODO(qyryq) Enable the test when LOGBROKER-9364 is done.
            return;
        }
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
            // .DirectRead(EnableDirectRead)
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

} // Y_UNIT_TEST_SUITE(BasicUsage)

} // namespace
