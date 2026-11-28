#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(RollingWipe) {
    Y_UNIT_TEST(RollingWipeWithPreservedLocalState) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const auto info = env.GetGroupInfo(groupId);

        // Shutdown one disk
        ui32 unavailableOrder = 0;
        if (info->GetActorId(unavailableOrder).NodeId() == env.Settings.ControllerNodeId) {
            unavailableOrder = 1;
        }
        const ui32 firstPeerOrder = unavailableOrder + 1;
        const TVDiskID unavailableDisk = info->GetVDiskId(unavailableOrder);
        const ui32 unavailableNode = info->GetActorId(unavailableOrder).NodeId();
        env.StopNode(unavailableNode);

        // Block tablet generation
        const ui64 tabletId = 1;
        const ui32 blockedGeneration = 42;
        const TActorId blockEdge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
        env.Runtime->WrapInActorContext(blockEdge, [&] {
            SendToBSProxy(blockEdge, groupId, new TEvBlobStorage::TEvBlock(tabletId, blockedGeneration, TInstant::Max()));
        });
        const auto block = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(blockEdge);
        UNIT_ASSERT_VALUES_EQUAL(block->Get()->Status, NKikimrProto::OK);
        env.Sim(TDuration::Seconds(30));

        // Do rolling wipe
        const auto config = env.FetchBaseConfig();
        for (const auto& slot : config.GetVSlot()) {
            const auto& location = slot.GetVSlotId();
            if (slot.GetGroupId() != groupId || location.GetNodeId() == unavailableNode) {
                continue;
            }
            const TVDiskID disk(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
                slot.GetFailDomainIdx(), slot.GetVDiskIdx());
            env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), disk);
            env.Sim(TDuration::Seconds(30));
        }

        auto checkBlock = [&](const TVDiskID& disk) {
            const TActorId queue = env.CreateQueueActor(disk, NKikimrBlobStorage::GetFastRead, 0);
            const auto result = env.SyncQuery<TEvBlobStorage::TEvVGetBlockResult, TEvBlobStorage::TEvVGetBlock>(
                queue, tabletId, disk, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NKikimrProto::OK, disk.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetGeneration(), blockedGeneration, disk.ToString());
        };
        checkBlock(info->GetVDiskId(firstPeerOrder));

        env.StartNode(unavailableNode);
        env.Sim(TDuration::Minutes(1));
        checkBlock(unavailableDisk);
    }
}
