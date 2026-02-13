#include "quota.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ {

using namespace NYdb::NTopic::NTests;

Y_UNIT_TEST_SUITE(TWriteQuoterTests) {

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("TEST_CASE_NAME", TTopicSdkTestSetup::MakeServerSettings(), false);

    auto& runtime = setup->GetRuntime();
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

    return setup;
}

TActorId RegisterQuoter(auto& runtime, auto& edgeActor, size_t writeSpeedInBytesPerSecond, size_t deduplicationIdQuota) {
    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.MutableQuotingConfig()->SetEnableQuoting(true);
    pqConfig.SetTopicsAreFirstClassCitizen(true);

    NPersQueue::TTopicConverterPtr topicConverter;
    NKikimrPQ::TPQTabletConfig config;
    config.MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(writeSpeedInBytesPerSecond);
    config.MutablePartitionConfig()->SetWriteMessageDeduplicationIdPerSecond(deduplicationIdQuota);

    TPartitionId partitionId;
    TActorId tabletActor = edgeActor;
    ui64 tabletId = 28739;
    std::shared_ptr<TTabletCountersBase> counters = std::make_shared<TTabletCountersBase>();

    auto quoterId = runtime.Register(CreateWriteQuoter(pqConfig, topicConverter, config, partitionId, tabletActor, tabletId, counters));
    runtime.EnableScheduleForActor(quoterId);

    return quoterId;
}

void RequestQuota(auto& runtime, auto& quoterId, auto& edgeActorId) {
    TEvents::TEvWakeup* writeRequest = new TEvents::TEvWakeup();
    runtime.Send(quoterId, edgeActorId,
        new TEvPQ::TEvRequestQuota(1, new IEventHandle(quoterId, edgeActorId, writeRequest)));
}

bool WaitForQuotaApproved(TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(1)) {
    auto event = runtime.GrabEdgeEvent<TEvPQ::TEvApproveWriteQuota>(timeout);
    return !!event;
}

void ConsumeQuota(auto& runtime, auto& quoterId, auto& edgeActorId, size_t bytes, size_t deduplicationIds) {
    runtime.Send(quoterId, edgeActorId,
        new TEvPQ::TEvConsumed(bytes, deduplicationIds, 1, "client"));
}

Y_UNIT_TEST(WaitDeduplicationIdQuota) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    auto edgeActorId = runtime.AllocateEdgeActor();

    auto quoterId = RegisterQuoter(runtime, edgeActorId, 1_MB, 1);
    RequestQuota(runtime, quoterId, edgeActorId);
    UNIT_ASSERT(WaitForQuotaApproved(runtime));

    TInstant start = TInstant::Now();

    ConsumeQuota(runtime, quoterId, edgeActorId, 1_KB, 1);

    RequestQuota(runtime, quoterId, edgeActorId);
    UNIT_ASSERT(WaitForQuotaApproved(runtime));

    auto duration = TInstant::Now() - start;
    UNIT_ASSERT_GT_C(duration, TDuration::MilliSeconds(950), "duration: " << duration);
}

}

} // namespace NKikimr::NPQ
