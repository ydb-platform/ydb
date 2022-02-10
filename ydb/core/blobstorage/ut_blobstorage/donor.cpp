#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h> 

Y_UNIT_TEST_SUITE(Donor) {

    Y_UNIT_TEST(SlayAfterWiping) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.EnableDonorMode();
        env.CreateBoxAndPool(2, 1);
        env.CommenceReplication();
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        for (ui32 i = 0; i < 100; ++i) {
            const TString buffer = TStringBuilder() << "blob number " << i;
            TLogoBlobID id(1, 1, 1, 0, buffer.size(), 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        // wait for sync and stuff
        env.Sim(TDuration::Seconds(3));

        // move slot out from disk
        auto info = env.GetGroupInfo(groupId);
        const TVDiskID& vdiskId = info->GetVDiskId(0);
        const TActorId& vdiskActorId = info->GetActorId(0);
        env.SettlePDisk(vdiskActorId);
        env.Sim(TDuration::Seconds(30));

        // find our donor disk
        auto baseConfig = env.FetchBaseConfig();
        bool found = false;
        std::pair<ui32, ui32> donorPDiskId;
        std::tuple<ui32, ui32, ui32> acceptor;
        for (const auto& slot : baseConfig.GetVSlot()) {
            if (slot.DonorsSize()) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 1);
                const auto& donor = slot.GetDonors(0);
                const auto& id = donor.GetVSlotId();
                UNIT_ASSERT_VALUES_EQUAL(vdiskActorId, MakeBlobStorageVDiskID(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(donor.GetVDiskId()), vdiskId);
                donorPDiskId = {id.GetNodeId(), id.GetPDiskId()};
                const auto& acceptorId = slot.GetVSlotId();
                acceptor = {acceptorId.GetNodeId(), acceptorId.GetPDiskId(), acceptorId.GetVSlotId()};
                found = true;
            }
        }
        UNIT_ASSERT(found);

        // restart with formatting
        env.Cleanup();
        const size_t num = env.PDiskMockStates.erase(donorPDiskId);
        UNIT_ASSERT_VALUES_EQUAL(num, 1);
        env.Initialize();

        // wait for initialization
        env.Sim(TDuration::Seconds(30));

        // ensure it has vanished
        baseConfig = env.FetchBaseConfig();
        found = false;
        for (const auto& slot : baseConfig.GetVSlot()) {
            const auto& id = slot.GetVSlotId();
            if (std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()) == acceptor) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot.GetStatus(), "REPLICATING");
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

}
