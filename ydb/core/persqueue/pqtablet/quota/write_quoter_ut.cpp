#include "quota.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ {

using namespace NYdb::NTopic::NTests;

Y_UNIT_TEST_SUITE(TFetchRequestTests) {

Y_UNIT_TEST(HappyWay) {
    auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);

    auto& runtime = setup->GetRuntime();
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.MutableQuotingConfig()->SetEnableQuoting(true);
    pqConfig.SetTopicsAreFirstClassCitizen(true);

    NPersQueue::TTopicConverterPtr topicConverter;
    NKikimrPQ::TPQTabletConfig config;
    config.MutablePartitionConfig()->SetWriteMessageDeduplicationIdPerSecond(1);

    TPartitionId partitionId;
    TActorId tabletActor = runtime.AllocateEdgeActor();
    ui64 tabletId = 28739;
    std::shared_ptr<TTabletCountersBase> counters = std::make_shared<TTabletCountersBase>();

    auto quoterId = runtime.Register(CreateWriteQuoter(pqConfig, topicConverter, config, partitionId, tabletActor, tabletId, counters));
    runtime.EnableScheduleForActor(quoterId);

    TEvents::TEvWakeup* writeRequest = new TEvents::TEvWakeup();
    runtime.Send(quoterId, tabletActor,
        new TEvPQ::TEvRequestQuota(1, new IEventHandle(TActorId{}, TActorId{}, writeRequest)));
    
    auto event = runtime.GrabEdgeEvent<TEvPQ::TEvApproveReadQuota>();
    UNIT_ASSERT(event);


}

}

} // namespace NKikimr::NPQ
