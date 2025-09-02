#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(GetBlock) {
    Y_UNIT_TEST(EmptyGetBlockCmd) {
        TEnvironmentSetup env({
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
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
}
