#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(GetBlock) {
    void RunEmptyGetBlockCmd(TBlobStorageGroupType::EErasureSpecies erasure) {
        TEnvironmentSetup env({
            .NodeCount = Max<ui32>(9, TBlobStorageGroupType(erasure).BlobSubgroupSize()),
            .Erasure = erasure,
        });
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool(1, 1);
        auto info = env.GetGroupInfo(env.GetGroups().front());
        auto ev = std::make_unique<TEvBlobStorage::TEvGetBlock>(1u, TInstant::Max());
        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, ev.release());
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetBlockResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    Y_UNIT_TEST(EmptyGetBlockCmd) { RunEmptyGetBlockCmd(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(EmptyGetBlockCmdBlock82) { RunEmptyGetBlockCmd(TBlobStorageGroupType::Erasure8Plus2Block); }
}
