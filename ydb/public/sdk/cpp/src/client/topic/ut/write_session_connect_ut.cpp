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
    // Reproduces Y_ASSERT(ConnectContext) in TWriteSessionImpl::Connect:
    // ClientContext is kept across reconnect, but after TDriver::Stop the
    // driver scope is cancelled and ClientContext->CreateContext() returns
    // nullptr. DirectWriteToPartition(false) makes reconnect call Connect()
    // directly instead of DescribePartition (which already handled nullptr).
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

        driver.Stop(false);

        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (TInstant::Now() < deadline) {
            session->WaitEvent().Wait(TDuration::MilliSeconds(200));
            for (auto& event : session->GetEvents()) {
                if (std::get_if<TSessionClosedEvent>(&event)) {
                    session->Close(TDuration::Zero());
                    return;
                }
            }
        }

        session->Close(TDuration::Zero());
    }
}

} // namespace NYdb::NTopic::NTests
