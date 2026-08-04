#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/base/tablet_resolver.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(BSCMigration) {
    void RebootTablet(TEnvironmentSetup& env, ui64 tabletId) {
        auto& runtime = *env.Runtime;
        const TActorId sender = runtime.AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto* poison = new TEvents::TEvPoison();
        auto* nested = new IEventHandle(TActorId(), sender, poison);
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvForward(tabletId, nested, {},
                TEvTabletResolver::TEvForward::EActor::Tablet)),
            sender.NodeId());
        {
            auto fwd = env.WaitForEdgeActorEvent<TEvTabletResolver::TEvForwardResult>(sender, false);
            UNIT_ASSERT(fwd);
            UNIT_ASSERT_VALUES_EQUAL_C(fwd->Get()->Status, NKikimrProto::OK, fwd->Get()->ToString());
        }
        env.Sim(TDuration::Seconds(5));
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())),
            sender.NodeId());
        env.Sim(TDuration::Seconds(5));
        runtime.DestroyActor(sender);
    }

    Y_UNIT_TEST(RestartBeforeCompatibilityInfoUpdate) {
        TFeatureFlags ff;
        ff.SetBsControllerRestartBeforeCompatibilityInfoUpdate(true);
        TEnvironmentSetup env{{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .FeatureFlags = std::move(ff),
        }};

        auto compatibilityInfo = MakeCompatibilityInfo(TVersion{ 26, 1, 1, 0 },
                NKikimrConfig::TCompatibilityRule::BlobStorageController);

        TCompatibilityInfoTest::Reset(&compatibilityInfo);
        env.Sim(TDuration::Seconds(30));

        RebootTablet(env, env.TabletId);
    
        env.CreateBoxAndPool(1, 1);
    }

}
