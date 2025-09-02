#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(Shred) {

    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env{{}};
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(5));

        {
            const TActorId self = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerShredRequest>();
            env.Runtime->SendToPipe(env.TabletId, self, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerShredResponse>(self);
            UNIT_ASSERT(!r->Get()->Record.HasCurrentGeneration());
        }

        {
            const TActorId self = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerShredRequest>(1);
            env.Runtime->SendToPipe(env.TabletId, self, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerShredResponse>(self);
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetCurrentGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetCompleted(), false);
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetProgress10k(), 0);
        }

        {
            const TActorId self = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerShredRequest>(0);
            env.Runtime->SendToPipe(env.TabletId, self, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerShredResponse>(self);
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetCurrentGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetCompleted(), false);
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetProgress10k(), 0);
        }
    }

}
