#include "ut_utils/managed_executor.h"
#include "ut_utils/topic_sdk_test_setup.h"
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

#include <future>

namespace NYdb::NTopic::NTests {

void WriteAndReadToEndWithRestarts(TReadSessionSettings readSettings, TWriteSessionSettings writeSettings, const std::string& message, ui32 count, TTopicSdkTestSetup& setup, TIntrusivePtr<TManagedExecutor> decompressor) {
    auto client = setup.MakeClient();
    auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

    for (ui32 i = 1; i <= count; ++i) {
        bool res = session->Write(message);
        UNIT_ASSERT(res);
    }
    bool res = session->Close(TDuration::Seconds(10));
    UNIT_ASSERT(res);

    std::shared_ptr<IReadSession> ReadSession;

    TTopicClient topicClient = setup.MakeClient();


    auto WaitTasks = [&](auto f, size_t c) {
        while (f() < c) {
            Sleep(TDuration::MilliSeconds(100));
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
        Sleep(TDuration::MilliSeconds(100));

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

    ui32 i = 0;
    while (AtomicGet(lastOffset) + 1 < count) {
        RunTasks(decompressor, {i++});
    }

    ReadSession->Close(TDuration::MilliSeconds(10));
}

Y_UNIT_TEST_SUITE(BasicUsage) {
    Y_UNIT_TEST(ReadWithoutConsumerWithRestarts) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = new TSyncExecutor();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        TTopicReadSettings topic = TEST_TOPIC;
        topic.AppendPartitionIds(0);
        readSettings
            .WithoutConsumer()
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(topic);

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(TEST_TOPIC)
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(NTopic::ECodec::RAW)
            .CompressionExecutor(compressor);


        ui32 count = 700;
        std::string message(2'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(ReadWithRestarts) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = new TSyncExecutor();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(TEST_CONSUMER)
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(TEST_TOPIC);

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(TEST_TOPIC).MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(NTopic::ECodec::RAW)
            .CompressionExecutor(compressor);


        ui32 count = 700;
        std::string message(2'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(ConflictingWrites) {

        TTopicSdkTestSetup setup(TEST_CASE_NAME);

        NTopic::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Path(setup.GetTopicPath()).ProducerId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Codec(NTopic::ECodec::RAW);
        NTopic::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        ui64 count = 100u;

        auto client = setup.MakeClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        TString messageBase = "message----";

        for (auto i = 0u; i < count; i++) {
            auto res = session->Write(messageBase);
            UNIT_ASSERT(res);
            if (i % 10 == 0) {
                setup.GetServer().KillTopicPqTablets(setup.GetTopicPath());
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
