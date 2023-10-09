#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(DonorMode) {

    Y_UNIT_TEST(BlobReplicationFromDonorDisk) {
        TEnvironmentSetup env(true, TBlobStorageGroupType::Erasure4Plus2Block);
        auto& runtime = env.Runtime;

        // enable donor mode
        env.EnableDonorMode();

        // create box and pool of groups
        env.CreateBoxAndPool();

        // commence replication for original groups and wait till it finishes
        env.CommenceReplication();

        auto groups = env.GetGroups();

        auto getBlobLocation = [&](const auto& info, const TLogoBlobID& blobId) {
            TBlobStorageGroupInfo::TVDiskIds vdiskIds;
            TBlobStorageGroupInfo::TServiceIds serviceIds;
            info->PickSubgroup(blobId.FullID().Hash(), &vdiskIds, &serviceIds);
            return std::make_pair(serviceIds[0], vdiskIds[0]); // always stored at the first disk of the subgroup
        };

        std::map<TLogoBlobID, TString> stored;
        std::vector<TActorId> donorVDiskIds;

        for (ui32 i = 0; i < 16; ++i) {
            Cerr << Endl << "*** ITERATION " << i << " ***" << Endl << Endl;

            // at some points allow replication to break the donor chain
            if (i == 3 || i == 7 || i == 15) {
                // commence replication for all disks of group
                env.CommenceReplication();

                // wait for the old disk to terminate
                auto donors = std::exchange(donorVDiskIds, {});
                for (ui32 i = 0; i < donors.size() - 1; ++i) {
                    const TActorId& vdiskActorId = donors[i];
                    const TActorId& edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
                    runtime->Send(new IEventHandle(vdiskActorId, edge, new TEvBlobStorage::TEvVStatus(),
                        IEventHandle::FlagTrackDelivery), 1);
                    auto r = runtime->WaitForEdgeActorEvent({edge});
                    runtime->DestroyActor(edge);
                    if (r->GetTypeRewrite() == TEvents::TSystem::Undelivered) {
                        break;
                    } else {
                        Y_ABORT_UNLESS(r->GetTypeRewrite() == TEvBlobStorage::EvVStatusResult);
                    }
                }
            }

            auto info = env.GetGroupInfo(groups[0]);

            // prepare new blob
            TString data = TStringBuilder() << "Hello, world! Iteration number " << i << " is on the run.";
            TDataPartSet partSet;
            info->Type.SplitData(TBlobStorageGroupType::CrcModeNone, data, partSet);
            TLogoBlobID blobId;
            for (ui32 step = 1;; ++step) {
                blobId = TLogoBlobID(1, 1 + i, step, 0, partSet.FullDataSize, 0, 1);
                const auto& [vdiskActorId, vdiskId] = getBlobLocation(info, blobId);
                if (TVDiskIdShort(vdiskId) == TVDiskIdShort(0, 0, 0)) {
                    break;
                }
            }
            TRope part = partSet.Parts[blobId.PartId() - 1].OwnedString;

            // scan through existing stored blobs and ensure they are intact
            for (const auto& [blobId, part] : stored) {
                const auto& [vdiskActorId, vdiskId] = getBlobLocation(info, blobId);
                env.CheckBlob(vdiskActorId, vdiskId, blobId, part);
            }

            // add it to stored set
            stored.emplace(blobId, part.ConvertToString());

            // get the blob location for this group
            const auto& [vdiskActorId, vdiskId] = getBlobLocation(info, blobId);

            // first, check that there is no such blob in the disk
            env.CheckBlob(vdiskActorId, vdiskId, blobId, part.ConvertToString(), NKikimrProto::NODATA);

            // put the blob to the disk
            env.PutBlob(vdiskId, blobId, part.ConvertToString());

            // check it appeared
            env.CheckBlob(vdiskActorId, vdiskId, blobId, part.ConvertToString());

            // wait for sync
            env.WaitForSync(info, blobId);

            // settle pdisk under the VDisk
            env.SettlePDisk(vdiskActorId);

            // make it donor
            donorVDiskIds.push_back(vdiskActorId);
        }
    }

    Y_UNIT_TEST(BaseReadingTest) {
        TEnvironmentSetup env(true, TBlobStorageGroupType::ErasureNone);
    }

}
