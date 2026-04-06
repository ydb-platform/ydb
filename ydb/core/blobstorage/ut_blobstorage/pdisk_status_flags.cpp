#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>

Y_UNIT_TEST_SUITE(PDiskStatusFlags) {

    Y_UNIT_TEST(SetPDiskStatusFlagsHelper) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));

        auto groups = env.GetGroups();
        UNIT_ASSERT_C(!groups.empty(), "No groups created");

        auto groupInfo = env.GetGroupInfo(groups[0]);
        UNIT_ASSERT(groupInfo);

        const TVDiskID vdiskId = groupInfo->GetVDiskId(0);
        const TActorId vdiskActorId = groupInfo->GetActorId(0);
        ui32 nodeId, pdiskId;
        std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);

        auto getStatusFlags = [&]() -> ui32 {
            ui32 flags = 0;
            env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
                const TActorId edge = env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                env.Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVStatus(vdiskId)), queueId.NodeId());
                auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge);
                flags = r->Get()->Record.GetStatusFlags();
            });
            return flags;
        };

        // before: no YELLOW flag
        ui32 flagsBefore = getStatusFlags();
        UNIT_ASSERT_C(
            !(flagsBefore & ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop)),
            "Expected no YELLOW flag before SetPDiskStatusFlags, got: " << flagsBefore);

        // set YELLOW — flag must appear in status
        env.SetPDiskStatusFlags(nodeId, pdiskId, NKikimrBlobStorage::TPDiskSpaceColor::YELLOW);
        env.Sim(TDuration::Seconds(5));

        ui32 flagsYellow = getStatusFlags();
        UNIT_ASSERT_C(
            flagsYellow & ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop),
            "Expected YELLOW flag after SetPDiskStatusFlags(YELLOW), got: " << flagsYellow);

        // set RED — status flags must reflect RED
        env.SetPDiskStatusFlags(nodeId, pdiskId, NKikimrBlobStorage::TPDiskSpaceColor::RED);
        env.Sim(TDuration::Seconds(5));

        ui32 flagsRed = getStatusFlags();
        UNIT_ASSERT_C(
            flagsRed & ui32(NKikimrBlobStorage::StatusDiskSpaceRed),
            "Expected RED flag after SetPDiskStatusFlags(RED), got: " << flagsRed);
    }

}
