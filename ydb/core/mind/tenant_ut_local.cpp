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

void CheckAlterTenant(TTenantTestRuntime &runtime, const TString &tenant, TEvLocal::TEvTenantStatus::EStatus status, ui64 cpu = 1, ui64 memory = 1, ui64 network = 1)
{
    NKikimrTabletBase::TMetrics limit;
    limit.SetCPU(cpu);
    limit.SetMemory(memory);
    limit.SetNetwork(network);
    runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                  runtime.Sender,
                                  new TEvLocal::TEvAlterTenant(tenant, limit)));
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->TenantName, tenant);
    UNIT_ASSERT(reply->Status == status);
    if (status == TEvLocal::TEvTenantStatus::STARTED) {
        UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetCPU(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetMemory(), memory);
        UNIT_ASSERT_VALUES_EQUAL(reply->ResourceLimit.GetNetwork(), network);
    }
}

}

Y_UNIT_TEST_SUITE(TLocalTests) {
    Y_UNIT_TEST(TestAddTenant) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckAddTenant(runtime, TENANT1_1_NAME, TEvLocal::TEvTenantStatus::STARTED, 5);
        CheckAddTenant(runtime, TENANT1_2_NAME, TEvLocal::TEvTenantStatus::STARTED);
        CheckAddTenant(runtime, TENANT1_U_NAME, TEvLocal::TEvTenantStatus::UNKNOWN_TENANT);
        CheckAddTenant(runtime, TENANTU_1_NAME, TEvLocal::TEvTenantStatus::UNKNOWN_TENANT);
        CheckAddTenant(runtime, TENANT1_1_NAME, TEvLocal::TEvTenantStatus::STARTED, 3, 5);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 5, 1, 1},
                                   {TENANT1_2_NAME, 1, 1, 1}}});
    }

    Y_UNIT_TEST(TestAlterTenant) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckAddTenant(runtime, TENANT1_1_NAME, TEvLocal::TEvTenantStatus::STARTED, 5, 5, 5);
        CheckAlterTenant(runtime, TENANT1_1_NAME, TEvLocal::TEvTenantStatus::STARTED, 10, 10, 10);
        CheckAlterTenant(runtime, TENANT1_2_NAME, TEvLocal::TEvTenantStatus::STOPPED);
        CheckAlterTenant(runtime, TENANT1_U_NAME, TEvLocal::TEvTenantStatus::STOPPED);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 10, 10, 10}}});
    }

    Y_UNIT_TEST(TestAddTenantWhileResolving) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);
        TAutoPtr<IEventHandle> handle;

        runtime.SendToPipe(SCHEME_SHARD1_ID, runtime.Sender, new TEvTest::TEvHoldResolve(true));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        NKikimrTabletBase::TMetrics limit;
        limit.SetCPU(1);
        limit.SetMemory(1);
        limit.SetNetwork(1);

        runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvLocal::TEvAddTenant(TENANT1_1_NAME, limit)));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvLocal::TEvAddTenant(TENANT1_1_NAME, limit)));
        runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvLocal::TEvAddTenant(TENANT1_2_NAME, limit)));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        runtime.SendToPipe(SCHEME_SHARD1_ID, runtime.Sender, new TEvTest::TEvHoldResolve(false));

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply1->TenantName, TENANT1_1_NAME);
        UNIT_ASSERT(reply1->Status == TEvLocal::TEvTenantStatus::STARTED);

        auto reply2 = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply2->TenantName, TENANT1_1_NAME);
        UNIT_ASSERT(reply2->Status == TEvLocal::TEvTenantStatus::STARTED);

        auto reply3 = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply3->TenantName, TENANT1_2_NAME);
        UNIT_ASSERT(reply3->Status == TEvLocal::TEvTenantStatus::STARTED);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 1, 1, 1}}});
    }

    Y_UNIT_TEST(TestRemoveTenantWhileResolving) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);
        TAutoPtr<IEventHandle> handle;

        runtime.SendToPipe(SCHEME_SHARD1_ID, runtime.Sender, new TEvTest::TEvHoldResolve(true));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        NKikimrTabletBase::TMetrics limit;
        limit.SetCPU(1);
        limit.SetMemory(1);
        limit.SetNetwork(1);

        runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvLocal::TEvAddTenant(TENANT1_1_NAME, limit)));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvLocal::TEvAddTenant(TENANT1_2_NAME, limit)));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        runtime.Send(new IEventHandle(MakeLocalID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvLocal::TEvRemoveTenant(TENANT1_1_NAME)));

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply1->TenantName, TENANT1_1_NAME);
        UNIT_ASSERT(reply1->Status == TEvLocal::TEvTenantStatus::STOPPED);

        auto reply2 = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply2->TenantName, TENANT1_1_NAME);
        UNIT_ASSERT(reply2->Status == TEvLocal::TEvTenantStatus::STOPPED);

        runtime.SendToPipe(SCHEME_SHARD1_ID, runtime.Sender, new TEvTest::TEvHoldResolve(false));

        auto reply3 = runtime.GrabEdgeEventRethrow<TEvLocal::TEvTenantStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply3->TenantName, TENANT1_2_NAME);
        UNIT_ASSERT(reply3->Status == TEvLocal::TEvTenantStatus::STARTED);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 1, 1, 1}}});
    }
}

} //namespace NKikimr
