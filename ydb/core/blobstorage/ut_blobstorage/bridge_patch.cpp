#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

Y_UNIT_TEST_SUITE(BridgePatch) {

    // The parts of a bridged group write only with ForceGroupGeneration. VPatch and moved patch write the patched blob
    // on the VDisks without it, so a patch of a bridged group must keep to the naive path in every part.
    Y_UNIT_TEST(PatchWithoutDeadline) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableVPatch(true);
        TEnvironmentSetup env{{
            .NodeCount = 8 * 2,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = NBridgeTest::GetBridgedLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);

        const TString data = MakeData(1000);
        const TLogoBlobID originalId(100500, 1, 1, 0, data.size(), 0);
        env.PutBlob(groupId, originalId, data);

        TLogoBlobID patchedId(100500, 1, 2, 0, data.size(), 0);
        UNIT_ASSERT(TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalId, &patchedId, 0xff, groupId,
            groupId));
        const TString chunk(8, 'x');
        TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffs(new TEvBlobStorage::TEvPatch::TDiff[1]);
        diffs[0].Set(TRcBuf(chunk), 10);
        TString expected = data;
        memcpy(expected.Detach() + 10, chunk.data(), chunk.size());

        // no part may ask its VDisks for a moved patch or VPatch
        ui32 vdiskPatches = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvBlobStorage::EvVMovedPatch || type == TEvBlobStorage::EvVPatchStart) {
                ++vdiskPatches;
            }
            return true;
        };

        // a patch without a deadline, which the moved patch carries through to the write since #58589 is fixed
        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPatch(groupId, originalId, patchedId, 0xff,
                std::move(diffs), 1, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPatchResult>(sender, false);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(vdiskPatches, 0);

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(patchedId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        auto get = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(get->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(get->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(get->Get()->Responses[0].Buffer.ConvertToString(), expected);
    }

}
