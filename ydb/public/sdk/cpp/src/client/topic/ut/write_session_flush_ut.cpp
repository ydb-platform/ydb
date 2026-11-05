#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests {
namespace {

TContinuationToken WaitForWriteToken(IWriteSession& session) {
    while (true) {
        UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write token");
        for (auto& event : session.GetEvents()) {
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                return std::move(ready->ContinuationToken);
            }
            if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(WriteSessionFlush) {
    Y_UNIT_TEST(PreparedWritesUseCompressionExecutor) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        for (const bool preEncoded : {false, true}) {
            auto executor = CreateThreadPoolManagedExecutor(1);
            auto session = client.CreateWriteSession(
                TWriteSessionSettings()
                    .Path(setup.GetTopicPath())
                    .MessageGroupId(preEncoded ? "encoded-producer" : TEST_MESSAGE_GROUP_ID)
                    .Codec(preEncoded ? ECodec::GZIP : ECodec::RAW)
                    .CompressionExecutor(executor)
                    .BatchFlushInterval(TDuration::Zero()));

            const std::string_view data = "message";
            for (const uint64_t seqNo : {1, 1, 2}) {
                auto token = WaitForWriteToken(*session);
                if (preEncoded) {
                    session->WriteEncoded(std::move(token), data, ECodec::RAW, static_cast<uint32_t>(data.size()), seqNo);
                } else {
                    session->Write(std::move(token), data, seqNo);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(executor->GetPlannedCount(), 1);

            auto flushed = session->Flush();
            UNIT_ASSERT(!flushed.HasValue());
            executor->RunAllTasks();
            UNIT_ASSERT_C(flushed.Wait(TDuration::Seconds(30)), "prepared writes did not flush after running the executor");
            UNIT_ASSERT(flushed.GetValue());
            UNIT_ASSERT(session->Close(TDuration::Seconds(30)));
        }
    }

    Y_UNIT_TEST(CloseWithQueuedRawSend) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        auto executor = CreateThreadPoolManagedExecutor(1);
        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .Codec(ECodec::RAW)
                .CompressionExecutor(executor)
                .BatchFlushInterval(TDuration::Zero()));
        session->Write(WaitForWriteToken(*session), "message");
        UNIT_ASSERT_VALUES_EQUAL(executor->GetPlannedCount(), 1);

        UNIT_ASSERT(!session->Close(TDuration::Zero()));
        session.reset();

        auto drained = NThreading::NewPromise<void>();
        executor->Post([drained]() mutable {
            drained.SetValue();
        });
        executor->RunAllTasks();
        UNIT_ASSERT_C(drained.GetFuture().Wait(TDuration::Seconds(30)), "executor did not drain after session destruction");
    }

    Y_UNIT_TEST(CloseImmediatelyAfterFlush) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .Codec(ECodec::RAW)
                .BatchFlushMessageCount(10)
                .BatchFlushInterval(TDuration::Hours(1)));

        session->Write(WaitForWriteToken(*session), "message");

        auto firstFlush = session->Flush();
        auto secondFlush = session->Flush();
        auto reentrantFlushPromise = NThreading::NewPromise<bool>();
        auto reentrantFlush = reentrantFlushPromise.GetFuture();
        firstFlush.Subscribe([session, reentrantFlushPromise](const NThreading::TFuture<bool>& result) mutable {
            reentrantFlushPromise.TrySetValue(result.GetValue() && session->Flush().GetValueSync());
        });

        UNIT_ASSERT_C(firstFlush.Wait(TDuration::Seconds(30)), "first flush timed out");
        UNIT_ASSERT_C(secondFlush.Wait(TDuration::Seconds(30)), "second flush timed out");
        UNIT_ASSERT_C(reentrantFlush.Wait(TDuration::Seconds(30)), "reentrant flush timed out");
        UNIT_ASSERT(firstFlush.GetValue());
        UNIT_ASSERT(secondFlush.GetValue());
        UNIT_ASSERT(reentrantFlush.GetValue());
        UNIT_ASSERT(session->Close(TDuration::Zero()));
    }
}

} // namespace NYdb::NTopic::NTests
