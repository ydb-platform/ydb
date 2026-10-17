#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <thread>

namespace NYdb::NPersQueue::NTests {
namespace {

void StopDriverOrFail(NYdb::TDriver& driver, TDuration timeout = TDuration::Seconds(15)) {
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

        StopDriverOrFail(driver);
        session.reset();
    }

    // Same hang as topic: CreateProcessor delay cancelled with ok=false used
    // to skip OnConnect, so DoConnect never ran again to AbortImpl.
    Y_UNIT_TEST(StopDuringReconnectDelayDoesNotDeadlock) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        auto& driver = setup.GetDriver();
        auto& client = setup.GetPersQueueClient();

        auto settings = setup.GetWriteSessionSettings();
        settings
            .ClusterDiscoveryMode(EClusterDiscoveryMode::Off)
            .RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(
                TDuration::Seconds(10),
                TDuration::Seconds(10)));

        auto session = client.CreateWriteSession(settings);
        Y_UNUSED(WaitForWriteToken(*session));

        setup.GetServer().ShutdownGRpc();
        Sleep(TDuration::MilliSeconds(500));

        StopDriverOrFail(driver);
        session.reset();
    }
}

} // namespace NYdb::NPersQueue::NTests
