#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/lifecycle_checks.h>

Y_UNIT_TEST_SUITE(DonorMode) {

    void RunBlobReplicationFromDonorDisk(TBlobStorageGroupType erasure) {
        TEnvironmentSetup env(true, erasure);
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
            return std::make_pair(serviceIds[blobId.PartId() - 1], vdiskIds[blobId.PartId() - 1]);
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
                blobId = TLogoBlobID(1, 1 + i, step, 0, partSet.FullDataSize, 0,
                    erasure.TotalPartCount() > 8 ? erasure.TotalPartCount() - i % 2 : 1);
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
        if (erasure.TotalPartCount() > 8) {
            env.Cleanup();
            env.Initialize();
            auto info = env.GetGroupInfo(groups[0]);
            for (const auto& [blobId, part] : stored) {
                const auto& [actorId, diskId] = getBlobLocation(info, blobId);
                env.CheckBlob(actorId, diskId, blobId, part);
            }
        }
    }

    Y_UNIT_TEST(BlobReplicationFromDonorDisk) { RunBlobReplicationFromDonorDisk(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(BlobReplicationFromDonorDiskBlock82) { RunBlobReplicationFromDonorDisk(TBlobStorageGroupType::Erasure8Plus2Block); }

    Y_UNIT_TEST(BaseReadingTest) {
        TEnvironmentSetup env(true, TBlobStorageGroupType::ErasureNone);
    }

}

namespace {

void Block82Recovery(bool huge, bool compact) {
    using namespace NBlobStorageLifecycle;
    const TBlobStorageGroupType erasure(TBlobStorageGroupType::Erasure8Plus2Block);
    for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
        TEnvironmentSetup env{{
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
            .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
                config.FreshCompThresholdLogoBlobs = 64_MB;
            },
            .MinHugeBlobInBytes = 64_KB,
        }};
        env.CreateBoxAndPool(1, 1);
        const auto info = env.GetGroupInfo(env.GetGroups().front());
        const TString data = env.GenerateRandomString(huge ? 1_MB + 17 : 799);
        const TLogoBlobID id(711, 1, 1, 0, data.size(), 0, 0, crc);
        TDataPartSet parts;
        erasure.SplitData(crc, data, parts);
        env.PutBlob(info->GroupID.GetRawId(), id, data);
        // Keep explicit copies of both high parts on the two high subgroup slots.
        for (ui32 partIdx : {8u, 9u}) {
            env.PutBlob(SubgroupDisk(info, id, partIdx + 2), TLogoBlobID(id, partIdx + 1),
                parts.Parts[partIdx].OwnedString.ConvertToString());
        }
        CheckGroupBlob(env, info, id, data);
        CheckMainParts(env, info, id, data);
        if (compact) {
            for (ui32 diskIdx = 0; diskIdx < erasure.BlobSubgroupSize(); ++diskIdx) {
                env.CompactVDisk(info->GetActorId(diskIdx));
            }
        }
        for (ui32 partIdx = 0; partIdx < erasure.TotalPartCount(); ++partIdx) {
            if (huge || compact) {
                CheckHeaderlessRecords(env, info, partIdx, id, 1u << partIdx);
            } else {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                const auto disk = SubgroupDisk(info, id, partIdx);
                auto layout = env.SyncQuery<T, TEvBlobStorage::TEvCaptureVDiskLayout>(info->GetActorId(disk));
                for (const auto& item : layout->Layout) {
                    UNIT_ASSERT(item.RecordType == T::ERecordType::IndexRecord || item.BlobId.FullID() != id);
                }
            }
        }
        env.Cleanup();
        env.Initialize();
        CheckGroupBlob(env, info, id, data);
        CheckMainParts(env, info, id, data, huge || compact);
        for (ui32 partIdx : {8u, 9u}) {
            CheckPart(env, info, partIdx + 2, id, partIdx, parts.Parts[partIdx].OwnedString);
            if (huge || compact) {
                CheckHeaderlessRecords(env, info, partIdx + 2, id, 1u << partIdx);
            }
        }
        // Recover after restart with two unavailable data mains, using parity9/10.
        for (ui32 partIdx : {0u, 7u}) {
            const auto disk = SubgroupDisk(info, id, partIdx);
            env.StopNode(info->GetActorId(disk).NodeId());
        }
        // The controller may share a stopped node; issue the read on a live node.
        const auto readNode = info->GetActorId(SubgroupDisk(info, id, 8)).NodeId();
        const TActorId edge = env.Runtime->AllocateEdgeActor(readNode);
        const TInstant deadline = env.Now() + TDuration::Seconds(30);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, deadline,
                NKikimrBlobStorage::FastRead));
        });
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, true, deadline);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
}

}

Y_UNIT_TEST_SUITE(Block82Lifecycle) {
    Y_UNIT_TEST(FreshRecoveryBlock82) { Block82Recovery(false, false); }
    Y_UNIT_TEST(CompactedRecoveryBlock82) { Block82Recovery(false, true); }
    Y_UNIT_TEST(HugeRecoveryBlock82) { Block82Recovery(true, true); }
}
