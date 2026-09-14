#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

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

void WaitForWriteAck(IWriteSession& session) {
    while (true) {
        UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write acknowledgement");
        for (const auto& event : session.GetEvents()) {
            if (const auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                UNIT_ASSERT_VALUES_EQUAL(acks->Acks.size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(acks->Acks.front().State, TWriteSessionEvent::TWriteAck::EES_WRITTEN);
                return;
            }
            if (const auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(WriteSessionConnect) {
    Y_UNIT_TEST(DriverStopPreservesExistingAndNewWriteSessions) {
        TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
        auto& driver = setup.GetDriver();
        auto& client = setup.GetPersQueueClient();
        auto settings = setup.GetWriteSessionSettings();
        settings.ClusterDiscoveryMode(EClusterDiscoveryMode::Off);
        for (bool wait : {false, true}) {
            auto session = client.CreateWriteSession(settings);
            auto token = WaitForWriteToken(*session);
            driver.Stop(wait);
            session->Write(std::move(token), "write after driver stop");
            WaitForWriteAck(*session);
            UNIT_ASSERT(session->Close(TDuration::Seconds(30)));
        }
    }
}

} // namespace NYdb::NPersQueue::NTests
