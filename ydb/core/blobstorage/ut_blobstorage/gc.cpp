#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(GarbageCollection) {
    void RunEmptyGcCmd(TBlobStorageGroupType erasure) {
        TEnvironmentSetup env({
            .NodeCount = Max(9u, erasure.BlobSubgroupSize()),
            .Erasure = erasure,
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

    Y_UNIT_TEST(EmptyGcCmd) { RunEmptyGcCmd(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(EmptyGcCmdBlock82) { RunEmptyGcCmd(TBlobStorageGroupType::Erasure8Plus2Block); }
}
