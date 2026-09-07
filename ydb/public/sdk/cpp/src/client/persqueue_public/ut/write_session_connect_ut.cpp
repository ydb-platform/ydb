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
    // subclient->CreateContext() returns nullptr. Direct CDS is disabled so
    // reconnect calls DoConnect() and must AbortImpl (releasing ClientContext)
    // instead of keeping the established context. Stop(true) waits for that
    // context to be destroyed; keeping it deadlocks here.
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

        driver.Stop(true);
        session.reset();
    }
}

} // namespace NYdb::NPersQueue::NTests
