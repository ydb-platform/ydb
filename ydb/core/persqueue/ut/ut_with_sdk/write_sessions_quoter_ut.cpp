#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NPQ {
namespace {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

void CheckConcurrentWriteSessions(ui32 rps, bool directWrite, size_t sessionCount, bool restartTablet = false) {
    auto serverSettings = TTopicSdkTestSetup::MakeServerSettings();
    serverSettings.PQConfig.SetWriteSessionsInitRps(rps);
    TTopicSdkTestSetup setup("WriteSessionsQuoter", serverSettings, false);

    setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);
    if (restartTablet) {
        setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());
        setup.GetServer().WaitInit(setup.GetFullTopicPath());
    }

    auto driver = setup.MakeDriver();
    TTopicClient client(driver);
    TVector<std::shared_ptr<IWriteSession>> sessions;
    TVector<NThreading::TFuture<TMonotonic>> initialized;
    const auto started = TMonotonic::Now();
    const auto deadline = started + TDuration::Seconds(30);
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
            return TMonotonic::Now();
        }));
        sessions.push_back(std::move(session));
    }

    TVector<TMonotonic> completionTimes;
    for (auto& future : initialized) {
        UNIT_ASSERT_C(future.Wait(deadline - TMonotonic::Now()), "Write session initialization timed out");
        completionTimes.push_back(future.GetValue());
    }
    Sort(completionTimes);
    if (directWrite) {
        // The initial burst is rps; later sessions must wait for replenishment.
        for (size_t i = rps; i < completionTimes.size(); ++i) {
            const auto minimum = TDuration::MilliSeconds((i + 1 - rps) * 1000 / rps);
            UNIT_ASSERT_C(completionTimes[i] - started + TDuration::MilliSeconds(10) >= minimum,
                TStringBuilder() << "Session " << i + 1 << " initialized too early at "
                    << completionTimes[i] - started << ", limit: " << rps << " RPS");
        }
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
    Y_UNIT_TEST(ConcurrentDirectSessionsAtOneRps) {
        CheckConcurrentWriteSessions(1, true, 8);
    }

    Y_UNIT_TEST(ConcurrentDirectSessionsAtThreeRps) {
        CheckConcurrentWriteSessions(3, true, 12);
    }

    Y_UNIT_TEST(NonDirectSessionsBypassZeroQuota) {
        CheckConcurrentWriteSessions(0, false, 12);
    }

    Y_UNIT_TEST(ConcurrentDirectSessionsAfterRestartAtOneRps) {
        CheckConcurrentWriteSessions(1, true, 8, true);
    }

    Y_UNIT_TEST(ConcurrentDirectSessionsAfterRestartAtThreeRps) {
        CheckConcurrentWriteSessions(3, true, 12, true);
    }
}

} // namespace
} // namespace NKikimr::NPQ
