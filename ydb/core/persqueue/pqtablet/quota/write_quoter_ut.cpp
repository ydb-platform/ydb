#include "quota.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TFetchRequestTests) {

Y_UNIT_TEST(HappyWay) {
    TTestBasicRuntime runtime;
    SetupTabletServices(runtime);
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

    NKikimrPQ::TPQConfig pqConfig;
    NPersQueue::TTopicConverterPtr topicConverter;
    NKikimrPQ::TPQTabletConfig config;
    TPartitionId partitionId;
    TActorId tabletActor = runtime.AllocateEdgeActor();
    ui64 tabletId = 28739;
    std::shared_ptr<TTabletCountersBase> counters = std::make_shared<TTabletCountersBase>();

    auto quoterId = runtime.Register(CreateWriteQuoter(pqConfig, topicConverter, config, partitionId, tabletActor, tabletId, counters));
    Y_UNUSED(quoterId);
}

}

} // namespace NKikimr::NPQ
