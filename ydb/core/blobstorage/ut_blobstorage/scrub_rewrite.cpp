#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/lifecycle_checks.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>

namespace {

TActorId FindSkeleton(TEnvironmentSetup& env, TActorId actor) {
    // Repair is internal; observe the public layout query being forwarded.
    TActorId skeleton;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvBlobStorage::EvCaptureVDiskLayout &&
                event->GetRecipientRewrite() != actor) {
            skeleton = event->GetRecipientRewrite();
        }
        return true;
    };
    env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actor);
    env.Runtime->FilterFunction = {};
    UNIT_ASSERT(skeleton);
    return skeleton;
}

void CheckScrubRewrite(TBlobStorageGroupType erasure, bool huge) {
    using namespace NBlobStorageLifecycle;
    using TLayout = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
    TEnvironmentSetup env{{
        .NodeCount = erasure.BlobSubgroupSize(),
        .Erasure = erasure,
        .DiskType = NPDisk::DEVICE_TYPE_ROT,
        .MinHugeBlobInBytes = 64_KB,
        .UseFakeConfigDispatcher = true,
    }};
    env.CreateBoxAndPool(1, 1);
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    const TString data = env.GenerateRandomString(huge ? 100_KB * erasure.DataParts() : 799);
    const TLogoBlobID id(821, 1, 1, 0, data.size(), 0, 0, TErasureType::CrcModeWholePart);
    env.PutBlob(info->GroupID.GetRawId(), id, data);
    const ui32 partIdx = erasure.TotalPartCount() - 1;
    const auto actor = info->GetActorId(SubgroupDisk(info, id, partIdx));
    env.CompactVDisk(actor);

    const TActorId skeleton = FindSkeleton(env, actor);

    auto captureLocation = [&](const TLogoBlobID& blobId, TLayout::ERecordType kind) {
        auto layout = env.SyncQuery<TLayout, TEvBlobStorage::TEvCaptureVDiskLayout>(actor);
        TDiskPart location;
        for (const auto& item : layout->Layout) {
            if (item.Database == TLayout::EDatabase::LogoBlobs && item.BlobId.FullID() == blobId &&
                    item.RecordType != TLayout::ERecordType::IndexRecord) {
                UNIT_ASSERT_C(item.RecordType == kind, item.ToString());
                const ui32 headerSize = erasure.CanUseLegacyHeader() ? TDiskBlob::HeaderSize : 0;
                UNIT_ASSERT_VALUES_EQUAL(item.Location.Size, erasure.PartSize(TLogoBlobID(blobId, partIdx + 1)) + headerSize);
                location = item.Location;
            }
        }
        UNIT_ASSERT(location.ChunkIdx);
        return location;
    };
    const auto kind = huge ? TLayout::ERecordType::HugeBlob : TLayout::ERecordType::InplaceBlob;
    const TDiskPart oldLocation = captureLocation(id, kind);

    if (huge) {
        // A normal write of the same part size must now go inline, while the
        // damaged old Huge record must still be replaced by another Huge record.
        const TActorId edge = env.Runtime->AllocateEdgeActor(actor.NodeId());
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto* performance = request->Record.MutableConfig()->MutableBlobStorageConfig()
            ->MutableVDiskPerformanceSettings()->AddVDiskTypes();
        performance->SetPDiskType(PDiskTypeToPDiskType(env.Settings.DiskType));
        performance->SetMinHugeBlobSizeInBytes(512_KB);
        env.Runtime->Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(actor.NodeId()), edge,
            request.Release()), actor.NodeId());
        env.WaitForEdgeActorEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);

        TLogoBlobID other;
        for (ui32 step = 2; step < 10000; ++step) {
            other = TLogoBlobID(821, 1, step, 0, data.size(), 0, 0, id.CrcMode());
            if (SubgroupDisk(info, other, partIdx) == SubgroupDisk(info, id, partIdx)) {
                break;
            }
        }
        UNIT_ASSERT_EQUAL(SubgroupDisk(info, other, partIdx), SubgroupDisk(info, id, partIdx));
        TDataPartSet parts;
        erasure.SplitData(TErasureType::CrcModeWholePart, data, parts);
        env.PutBlob(SubgroupDisk(info, id, partIdx), TLogoBlobID(other, partIdx + 1),
            parts.Parts[partIdx].OwnedString.ConvertToString());
        env.CompactVDisk(actor);
        captureLocation(other, TLayout::ERecordType::InplaceBlob);
        captureLocation(id, TLayout::ERecordType::HugeBlob);
    }

    auto [node, pdisk, slot] = DecomposeVDiskServiceId(actor);
    Y_UNUSED(slot);
    env.PDiskMockStates.at({node, pdisk})->SetCorruptedArea(oldLocation.ChunkIdx,
        oldLocation.Offset, oldLocation.Offset + oldLocation.Size, true);
    ui32 rewrites = 0;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
            const auto* put = event->Get<TEvBlobStorage::TEvVPut>();
            if (put->RewriteBlob && LogoBlobIDFromLogoBlobID(put->Record.GetBlobID()) == TLogoBlobID(id, partIdx + 1)) {
                UNIT_ASSERT_VALUES_EQUAL(put->RewriteHugeBlob, huge);
                UNIT_ASSERT_VALUES_EQUAL(put->GetBuffer().size(), erasure.PartSize(TLogoBlobID(id, partIdx + 1)));
                ++rewrites;
            }
        }
        return true;
    };
    std::vector<TEvRestoreCorruptedBlob::TItem> items;
    items.emplace_back(id, NMatrix::TVectorType::MakeOneHot(partIdx, erasure.TotalPartCount()),
        erasure, oldLocation);
    const TInstant deadline = env.Now() + TDuration::Minutes(1);
    const TActorId edge = env.Runtime->AllocateEdgeActor(actor.NodeId());
    env.Runtime->Send(new IEventHandle(skeleton, edge,
        new TEvRestoreCorruptedBlob(deadline, std::move(items), true, false)), actor.NodeId());
    auto result = env.WaitForEdgeActorEvent<TEvRestoreCorruptedBlobResult>(edge, true, deadline);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Items.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Items.front().Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(rewrites, 1);
    env.Runtime->FilterFunction = {};

    env.CompactVDisk(actor);
    const TDiskPart newLocation = captureLocation(id, kind);
    UNIT_ASSERT_UNEQUAL(newLocation, oldLocation);
    CheckGroupBlob(env, info, id, data);
    CheckMainParts(env, info, id, data);
    env.Cleanup();
    env.Initialize();
    CheckGroupBlob(env, info, id, data);
    CheckMainParts(env, info, id, data);
    captureLocation(id, kind);
}

void CheckHandoffScrubRewrite(bool compactBeforeRepair) {
    using namespace NBlobStorageLifecycle;
    using TLayout = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
    const TBlobStorageGroupType erasure(TBlobStorageGroupType::Erasure8Plus2Block);
    TEnvironmentSetup env{{
        .NodeCount = erasure.BlobSubgroupSize(),
        .Erasure = erasure,
        .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
            config.BalancingEnableSend = false;
            config.BalancingEnableDelete = false;
        },
        .DiskType = NPDisk::DEVICE_TYPE_ROT,
        .MinHugeBlobInBytes = 64_KB,
        .UseFakeConfigDispatcher = true,
    }};
    env.CreateBoxAndPool(1, 1);
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    const TString data = env.GenerateRandomString(800_KB);
    const TLogoBlobID id(822, 1, 1, 0, data.size(), 0, 0, TErasureType::CrcModeWholePart);
    env.PutBlob(info->GroupID.GetRawId(), id, data);
    const auto handoff = SubgroupDisk(info, id, erasure.TotalPartCount());
    const auto actor = info->GetActorId(handoff);
    const TActorId skeleton = FindSkeleton(env, actor);
    TDataPartSet parts;
    erasure.SplitData(TErasureType::CrcModeWholePart, data, parts);
    std::map<TDiskPart, ui32> oldLocations;
    for (ui32 partIdx : {8u, 9u}) {
        env.PutBlob(handoff, TLogoBlobID(id, partIdx + 1), parts.Parts[partIdx].OwnedString.ConvertToString());
        auto layout = env.SyncQuery<TLayout, TEvBlobStorage::TEvCaptureVDiskLayout>(actor);
        bool found = false;
        for (const auto& item : layout->Layout) {
            if (item.Database == TLayout::EDatabase::LogoBlobs && item.BlobId.FullID() == id &&
                    item.RecordType == TLayout::ERecordType::HugeBlob && !oldLocations.contains(item.Location)) {
                UNIT_ASSERT_VALUES_EQUAL(item.SstId, 0);
                oldLocations.emplace(item.Location, partIdx);
                UNIT_ASSERT(!std::exchange(found, true));
            }
        }
        UNIT_ASSERT(found);
    }
    UNIT_ASSERT_VALUES_EQUAL(oldLocations.size(), 2);
    if (compactBeforeRepair) {
        env.CompactVDisk(actor);
    }
    auto layout = env.SyncQuery<TLayout, TEvBlobStorage::TEvCaptureVDiskLayout>(actor);
    std::set<ui64> sstIds;
    ui32 records = 0;
    for (const auto& item : layout->Layout) {
        if (item.Database == TLayout::EDatabase::LogoBlobs && item.BlobId.FullID() == id &&
                item.RecordType != TLayout::ERecordType::IndexRecord) {
            UNIT_ASSERT(item.RecordType == TLayout::ERecordType::HugeBlob);
            UNIT_ASSERT(oldLocations.contains(item.Location));
            UNIT_ASSERT_VALUES_EQUAL(bool(item.SstId), compactBeforeRepair);
            sstIds.insert(item.SstId);
            ++records;
        }
    }
    // A compacted SST has one index entry per blob: its two Huge extents are
    // represented by ManyHugeBlobs; before compaction both remain in Fresh.
    UNIT_ASSERT_VALUES_EQUAL(records, 2);
    UNIT_ASSERT_VALUES_EQUAL(sstIds.size(), 1);

    const TActorId configEdge = env.Runtime->AllocateEdgeActor(actor.NodeId());
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    auto* performance = request->Record.MutableConfig()->MutableBlobStorageConfig()
        ->MutableVDiskPerformanceSettings()->AddVDiskTypes();
    performance->SetPDiskType(PDiskTypeToPDiskType(env.Settings.DiskType));
    performance->SetMinHugeBlobSizeInBytes(512_KB);
    env.Runtime->Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(actor.NodeId()), configEdge,
        request.Release()), actor.NodeId());
    env.WaitForEdgeActorEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(configEdge);

    auto [node, pdisk, slot] = DecomposeVDiskServiceId(actor);
    Y_UNUSED(slot);
    std::vector<TEvRestoreCorruptedBlob::TItem> items;
    for (const auto& [location, partIdx] : oldLocations) {
        env.PDiskMockStates.at({node, pdisk})->SetCorruptedArea(location.ChunkIdx,
            location.Offset, location.Offset + location.Size, true);
        items.emplace_back(id, NMatrix::TVectorType::MakeOneHot(partIdx, erasure.TotalPartCount()),
            erasure, location);
    }
    ui32 rewrittenMask = 0;
    ui32 rewrites = 0;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
            const auto* put = event->Get<TEvBlobStorage::TEvVPut>();
            const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(put->Record.GetBlobID());
            if (put->RewriteBlob && partId.FullID() == id) {
                UNIT_ASSERT(put->RewriteHugeBlob);
                UNIT_ASSERT(partId.PartId() == 9 || partId.PartId() == 10);
                UNIT_ASSERT_EQUAL(put->GetBuffer(), parts.Parts[partId.PartId() - 1].OwnedString);
                rewrittenMask |= 1u << (partId.PartId() - 1);
                ++rewrites;
            }
        }
        return true;
    };
    const TActorId edge = env.Runtime->AllocateEdgeActor(actor.NodeId());
    const TInstant deadline = env.Now() + TDuration::Minutes(1);
    env.Runtime->Send(new IEventHandle(skeleton, edge,
        new TEvRestoreCorruptedBlob(deadline, std::move(items), true, false)), actor.NodeId());
    auto result = env.WaitForEdgeActorEvent<TEvRestoreCorruptedBlobResult>(edge, true, deadline);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Items.size(), 2);
    for (const auto& item : result->Get()->Items) {
        UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
    }
    UNIT_ASSERT_VALUES_EQUAL(rewrittenMask, 0x300);
    UNIT_ASSERT_VALUES_EQUAL(rewrites, 2);
    env.Runtime->FilterFunction = {};
    env.CompactVDisk(actor);

    auto check = [&] {
        CheckGroupBlob(env, info, id, data);
        for (ui32 partIdx : {8u, 9u}) {
            CheckPart(env, info, erasure.TotalPartCount(), id, partIdx, parts.Parts[partIdx].OwnedString);
        }
        CheckHeaderlessRecords(env, info, erasure.TotalPartCount(), id, 0x300);
        auto layout = env.SyncQuery<TLayout, TEvBlobStorage::TEvCaptureVDiskLayout>(actor);
        ui32 records = 0;
        for (const auto& item : layout->Layout) {
            if (item.Database == TLayout::EDatabase::LogoBlobs && item.BlobId.FullID() == id &&
                    item.RecordType != TLayout::ERecordType::IndexRecord) {
                UNIT_ASSERT(item.RecordType == TLayout::ERecordType::HugeBlob);
                UNIT_ASSERT(!oldLocations.contains(item.Location));
                ++records;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(records, 2);
    };
    check();
    env.Cleanup();
    env.Initialize();
    check();
}

}

Y_UNIT_TEST_SUITE(ScrubRewriteBlock82) {
    Y_UNIT_TEST(InlineRepair) {
        CheckScrubRewrite(TBlobStorageGroupType::Erasure8Plus2Block, false);
    }

    Y_UNIT_TEST(HugeRepairAfterThresholdChange) {
        CheckScrubRewrite(TBlobStorageGroupType::Erasure8Plus2Block, true);
    }

    Y_UNIT_TEST(LegacyInlineRepair) {
        CheckScrubRewrite(TBlobStorageGroupType::Erasure4Plus2Block, false);
    }

    Y_UNIT_TEST(LegacyHugeRepairAfterThresholdChange) {
        CheckScrubRewrite(TBlobStorageGroupType::Erasure4Plus2Block, true);
    }

    Y_UNIT_TEST(HandoffFreshHugeRepair) {
        CheckHandoffScrubRewrite(false);
    }

    Y_UNIT_TEST(HandoffManyHugeRepair) {
        CheckHandoffScrubRewrite(true);
    }
}
