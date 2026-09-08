#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <thread>

namespace NYdb::inline Dev::NTopic::NTests {
namespace {

void StopDriverOrFail(TDriver& driver, TDuration timeout = TDuration::Seconds(15)) {
    auto done = NThreading::NewPromise();
    std::thread stopper([&driver, done]() mutable {
        driver.Stop(true);
        done.SetValue();
    });
    if (!done.GetFuture().Wait(timeout)) {
        stopper.detach();
        UNIT_FAIL("TDriver::Stop(true) did not return in " << timeout);
    }
    stopper.join();
}

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

Y_UNIT_TEST_SUITE(WriteSessionConnect) {
    // After TDriver::Stop the driver scope is cancelled. DirectWriteToPartition
    // (false) makes reconnect call Connect() with a still-live ClientContext.
    // Connect must AbortImpl instead of creating children of that context.
    // Stop(true) waits for ClientContext to be destroyed; keeping it deadlocks.
    Y_UNIT_TEST(ReconnectAfterDriverStopDoesNotAbortOnNullConnectContext) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TDriver driver(setup.MakeDriverConfig());
        TTopicClient client(driver);

        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(false)
                .RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(
                    TDuration::MilliSeconds(10),
                    TDuration::MilliSeconds(10))));

        Y_UNUSED(WaitForWriteToken(*session));

        StopDriverOrFail(driver);
        session.reset();
    }

    // CreateProcessor delay is cancelled with ok=false and used to return
    // without OnConnect, so ClientContext stayed in the session and Stop(true)
    // waited for CQ forever. DirectWriteToPartition(false) keeps reconnect on
    // Connect() rather than DescribePartition.
    Y_UNIT_TEST(StopDuringReconnectDelayDoesNotDeadlock) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TDriver driver(setup.MakeDriverConfig());
        TTopicClient client(driver);

        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(false)
                .RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(
                    TDuration::Seconds(10),
                    TDuration::Seconds(10))));

        Y_UNUSED(WaitForWriteToken(*session));

        setup.GetServer().ShutdownGRpc();
        Sleep(TDuration::MilliSeconds(500));

        StopDriverOrFail(driver);
        session.reset();
    }
}

} // namespace NYdb::NTopic::NTests
