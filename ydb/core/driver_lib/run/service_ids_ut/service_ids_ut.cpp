#include <ydb/core/statistics/service/service.h>
#include <ydb/services/workload_manager/service/service.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ServiceAddressProtocol) {
    Y_UNIT_TEST(PreserveNodeAndTwelveByteServiceNames) {
        // This target deliberately does not link either service implementation.
        for (ui32 node : {0u, 1u, 0xffffffffu}) {
            const auto statistics = NKikimr::NStat::MakeStatServiceID(node);
            const auto workload = NKikimr::NWorkloadManager::MakeServiceId(node);
            UNIT_ASSERT_VALUES_EQUAL(statistics, NActors::TActorId(node, TStringBuf("StatService\0", 12)));
            UNIT_ASSERT_VALUES_EQUAL(workload, NActors::TActorId(node, TStringBuf("kqp_workld\0\0", 12)));
            UNIT_ASSERT(statistics != workload);
        }
    }
}
