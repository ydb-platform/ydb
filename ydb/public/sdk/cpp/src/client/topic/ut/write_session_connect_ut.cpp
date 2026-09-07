#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

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

        driver.Stop(true);
        session.reset();
    }
}

} // namespace NYdb::NTopic::NTests
