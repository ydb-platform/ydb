#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(GarbageCollection) {
    Y_UNIT_TEST(EmptyGcCmd) {
        TEnvironmentSetup env({
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto info = env.GetGroupInfo(env.GetGroups().front());

        auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(1u, 1u, 1u, 0u, false, 0u, 0u, nullptr, nullptr,
            TInstant::Max(), true);
        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, ev.release());
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
    }
}
