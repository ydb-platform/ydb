#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NPQ {
namespace {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

void CheckConcurrentWriteSessions(ui32 maxConcurrentInitializations, bool directWrite, size_t sessionCount, bool restartTablet = false) {
    auto serverSettings = TTopicSdkTestSetup::MakeServerSettings();
    serverSettings.PQConfig.SetMaxConcurrentWriteSessionInitializations(maxConcurrentInitializations);
    TTopicSdkTestSetup setup("WriteSessionsQuoter", serverSettings, false);

    setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);
    if (restartTablet) {
        setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());
        setup.GetServer().WaitInit(setup.GetFullTopicPath());
    }

    auto driver = setup.MakeDriver();
    TTopicClient client(driver);
    TVector<std::shared_ptr<IWriteSession>> sessions;
    TVector<NThreading::TFuture<void>> initialized;
    const auto deadline = TMonotonic::Now() + TDuration::Seconds(30);
    for (size_t i = 0; i < sessionCount; ++i) {
        const auto producer = "producer-" + std::to_string(i);
        TWriteSessionSettings settings;
        settings.Path(TEST_TOPIC)
            .PartitionId(0)
            .ProducerId(producer)
            .MessageGroupId(producer)
            .DirectWriteToPartition(directWrite)
            .Codec(ECodec::RAW)
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());
        auto session = client.CreateWriteSession(settings);
        // ReadyToAccept only describes the SDK buffer, not server initialization.
        initialized.push_back(session->GetInitSeqNo().Apply([](const auto& future) {
            UNIT_ASSERT_VALUES_EQUAL(future.GetValue(), 0);
        }));
        sessions.push_back(std::move(session));
    }

    // More sessions than slots must finish while earlier sessions stay open:
    // quota covers initialization, not the lifetime of a write session.
    for (auto& future : initialized) {
        const auto now = TMonotonic::Now();
        const auto remaining = now < deadline ? deadline - now : TDuration::Zero();
        UNIT_ASSERT_C(future.Wait(remaining), "Write session initialization timed out");
        future.GetValue();
    }

    for (auto& session : sessions) {
        UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(10)), "No write session event");
        auto event = session->GetEvent(false);
        UNIT_ASSERT(event);
        auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
        UNIT_ASSERT_C(ready, "Expected a continuation token after initialization");
        session->Write(std::move(ready->ContinuationToken), "message", 1);
        UNIT_ASSERT_C(session->Close(TDuration::Seconds(10)), "Message was not acknowledged");
    }
}

Y_UNIT_TEST_SUITE(WriteSessionsQuoterWithSDK) {
    Y_UNIT_TEST(ConcurrentDirectSessionsWithOneInitializationSlot) {
        CheckConcurrentWriteSessions(1, true, 8);
    }

    Y_UNIT_TEST(ConcurrentDirectSessionsWithThreeInitializationSlots) {
        CheckConcurrentWriteSessions(3, true, 12);
    }

    Y_UNIT_TEST(NonDirectSessionsBypassZeroQuota) {
        CheckConcurrentWriteSessions(0, false, 12);
    }

    Y_UNIT_TEST(ConcurrentDirectSessionsAfterRestartWithOneInitializationSlot) {
        CheckConcurrentWriteSessions(1, true, 8, true);
    }

    Y_UNIT_TEST(ConcurrentDirectSessionsAfterRestartWithThreeInitializationSlots) {
        CheckConcurrentWriteSessions(3, true, 12, true);
    }
}

} // namespace
} // namespace NKikimr::NPQ
