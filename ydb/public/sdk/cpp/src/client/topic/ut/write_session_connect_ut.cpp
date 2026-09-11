#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

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
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TDriver driver(setup.MakeDriverConfig());
        TTopicClient client(driver);
        auto settings = TWriteSessionSettings()
            .Path(setup.GetTopicPath())
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .DirectWriteToPartition(false);
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

} // namespace NYdb::NTopic::NTests
