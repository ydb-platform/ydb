#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/adapters/executor/executor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/thread/pool.h>

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
    Y_UNIT_TEST(RawWritesWithExternalInlineExecutor) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        auto pool = ::CreateThreadPool(1, 0);
        auto executor = NAdapters::CreateExternalThreadPoolExecutorAdapter(
            std::shared_ptr<IThreadPool>(pool.Release()));

        auto session = client.CreateSimpleBlockingWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .Codec(ECodec::RAW)
                .CompressionExecutor(executor));
        UNIT_ASSERT(session->Write("message", std::nullopt, std::nullopt, TDuration::Seconds(30)));
        UNIT_ASSERT(session->Close(TDuration::Seconds(30)));
    }

    Y_UNIT_TEST(RawWritesUseCompressionExecutor) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        for (const bool runExecutor : {false, true}) {
            auto executor = CreateThreadPoolManagedExecutor(1);
            auto session = client.CreateSimpleBlockingWriteSession(
                TWriteSessionSettings()
                    .Path(setup.GetTopicPath())
                    .MessageGroupId(runExecutor ? "running-executor" : "held-executor")
                    .Codec(ECodec::RAW)
                    .CompressionExecutor(executor));

            for (const uint64_t seqNo : {2, 2, 1, 3}) {
                UNIT_ASSERT(session->Write("message", seqNo, std::nullopt, TDuration::Seconds(30)));
            }
            UNIT_ASSERT_VALUES_EQUAL(executor->GetPlannedCount(), 1);
            if (runExecutor) {
                executor->RunAllTasks();
            }
            UNIT_ASSERT(session->Close(TDuration::Seconds(30)));
            session.reset();
            executor->RunAllTasks();
            executor->Stop();
        }
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
