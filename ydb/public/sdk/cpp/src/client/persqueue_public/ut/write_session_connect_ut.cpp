#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NPersQueue::NTests {
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
    // After TDriver::Stop the driver scope is cancelled and
    // ClientContext->CreateContext() returns nullptr. Direct CDS is disabled so
    // reconnect calls DoConnect() and must AbortImpl instead of Y_ASSERT.
    Y_UNIT_TEST(ReconnectAfterDriverStopDoesNotAbortOnNullConnectContext) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        auto& driver = setup.GetDriver();
        auto& client = setup.GetPersQueueClient();

        auto settings = setup.GetWriteSessionSettings();
        settings
            .ClusterDiscoveryMode(EClusterDiscoveryMode::Off)
            .RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(
                TDuration::MilliSeconds(10),
                TDuration::MilliSeconds(10)));

        auto session = client.CreateWriteSession(settings);
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

} // namespace NYdb::NPersQueue::NTests
