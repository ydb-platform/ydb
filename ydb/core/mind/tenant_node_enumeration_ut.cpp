#include "tenant_node_enumeration.h"
#include <ydb/core/testlib/tenant_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

void CheckAddTenant(TTenantTestRuntime &runtime, const TString &tenant, TEvLocal::TEvTenantStatus::EStatus status, ui64 cpu = 1, ui64 expectedCpu = 0, ui64 memory = 1, ui64 expectedMemory = 0, ui64 network = 1, ui64 expectedNetwork = 0)
{
    NKikimrTabletBase::TMetrics limit;
    limit.SetCPU(cpu);
    limit.SetMemory(memory);
    limit.SetNetwork(network);
    runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                  runtime.Sender,
                                  new TEvLocal::TEvAddTenant(tenant, limit)));
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->TenantName, tenant);
    UNIT_ASSERT_VALUES_EQUAL((int)reply->Status, (int)status);
    if (status == TEvLocal::TEvTenantStatus::STARTED) {
        if (expectedCpu)
            UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetCPU(), expectedCpu);
        else
            UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetCPU(), cpu);
        if (expectedMemory)
            UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetMemory(), expectedMemory);
        else
            UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetMemory(), memory);
        if (expectedNetwork)
            UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetNetwork(), expectedNetwork);
        else
            UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetNetwork(), network);
    }
}

}

Y_UNIT_TEST_SUITE(TEnumerationTest) {
    Y_UNIT_TEST(TestPublish) {
        return;

        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckAddTenant(runtime, TENANT1_1_NAME, TEvLocal::TEvTenantStatus::STARTED, 5);
        CheckAddTenant(runtime, TENANT1_2_NAME, TEvLocal::TEvTenantStatus::STARTED);
        CheckAddTenant(runtime, TENANT2_1_NAME, TEvLocal::TEvTenantStatus::STARTED);
        CheckAddTenant(runtime, TENANT1_U_NAME, TEvLocal::TEvTenantStatus::UNKNOWN_TENANT);
        CheckAddTenant(runtime, TENANTU_1_NAME, TEvLocal::TEvTenantStatus::UNKNOWN_TENANT);
        CheckAddTenant(runtime, TENANT1_1_NAME, TEvLocal::TEvTenantStatus::STARTED, 3, 5);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 5, 1, 1},
                                   {TENANT1_2_NAME, 1, 1, 1}}});

        runtime.Register(CreateTenantNodeEnumerationPublisher(), 0);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1000));

        TActorId sender = runtime.AllocateEdgeActor();
        runtime.Register(CreateTenantNodeEnumerationLookup(sender, "/" + DOMAIN1_NAME));

        TAutoPtr<IEventHandle> handle;
        const auto event = runtime.GrabEdgeEvent<TEvTenantNodeEnumerator::TEvLookupResult>(handle);
        UNIT_ASSERT(event->Success);
        UNIT_ASSERT(event->AssignedNodes.size() == 1 && event->AssignedNodes[0] == 1);
  }
}

} //namespace NKikimr
