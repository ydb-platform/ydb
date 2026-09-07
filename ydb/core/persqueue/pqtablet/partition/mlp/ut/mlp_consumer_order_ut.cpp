#include "mlp_consumer_order.h"

#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/time_provider/time_provider.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TChildPartitionsOrderManagerTests) {

class TScopedTimeProvider {
public:
    TScopedTimeProvider()
        : Previous_(TAppData::TimeProvider)
    {
        TAppData::TimeProvider = CreateDefaultTimeProvider();
    }

    ~TScopedTimeProvider() {
        TAppData::TimeProvider = Previous_;
    }

private:
    TIntrusivePtr<ITimeProvider> Previous_;
};

Y_UNIT_TEST(SetSendFullStateByPartitionIdRetriesKnownChild) {
    TScopedTimeProvider time;
    TChildPartitionsOrderManager manager;
    manager.ChildrenPartitionWithKeepOrder[1] = TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder{
        .TabletId = 100,
        .Cookie = 2,
        .SendReasons = {.Reasons = TChildPartitionsOrderManager::ESendReasons::Initial},
    };
    manager.ChildrenPartitionWithKeepOrder[1].MarkAsSent();
    UNIT_ASSERT(!manager.ChildrenPartitionWithKeepOrder[1].NeedSendFullState());

    UNIT_ASSERT(manager.SetSendFullStateByPartitionId(1, TChildPartitionsOrderManager::ESendReasons::DeliveryProblem));
    UNIT_ASSERT(manager.ChildrenPartitionWithKeepOrder[1].NeedSendFullState());
    UNIT_ASSERT(!manager.SetSendFullStateByPartitionId(99, TChildPartitionsOrderManager::ESendReasons::DeliveryProblem));
}

Y_UNIT_TEST(SetSendFullStateByCookieRetriesKnownChild) {
    TScopedTimeProvider time;
    TChildPartitionsOrderManager manager;
    manager.ChildrenPartitionWithKeepOrder[2] = TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder{
        .TabletId = 200,
        .Cookie = 7,
        .SendReasons = {.Reasons = TChildPartitionsOrderManager::ESendReasons::Initial},
    };
    manager.ChildrenPartitionWithKeepOrder[2].MarkAsSent();
    UNIT_ASSERT(!manager.ChildrenPartitionWithKeepOrder[2].NeedSendFullState());

    UNIT_ASSERT(manager.SetSendFullStateByCookie(7, TChildPartitionsOrderManager::ESendReasons::DeliveryProblem));
    UNIT_ASSERT(manager.ChildrenPartitionWithKeepOrder[2].NeedSendFullState());
    UNIT_ASSERT(!manager.SetSendFullStateByCookie(8, TChildPartitionsOrderManager::ESendReasons::DeliveryProblem));
}

} // Y_UNIT_TEST_SUITE(TChildPartitionsOrderManagerTests)

} // namespace NKikimr::NPQ::NMLP
