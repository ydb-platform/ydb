#include "ut_utils/topic_sdk_test_setup.h"

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

Y_UNIT_TEST_SUITE(WriteSessionFlush) {
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
        UNIT_ASSERT_C(firstFlush.Wait(TDuration::Seconds(30)), "first flush timed out");
        UNIT_ASSERT_C(secondFlush.Wait(TDuration::Seconds(30)), "second flush timed out");
        UNIT_ASSERT(firstFlush.GetValue());
        UNIT_ASSERT(secondFlush.GetValue());
        UNIT_ASSERT(session->Close(TDuration::Zero()));
    }
}

} // namespace NYdb::NTopic::NTests
