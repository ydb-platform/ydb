#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/lifecycle_checks.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <util/system/info.h>


enum class EState {
    OK,
    FORMAT
};

struct TReplTestSettings {
    ui32 BlobSize;
    ui32 MinHugeBlobSize;
    ui32 MinHugeBlobSizeInRepl;
};

void DoTestCase(const TReplTestSettings& settings, TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block) {
    using E = EState;
    std::vector<EState> states(erasure.BlobSubgroupSize(), E::OK);
    states[1] = states[2] = E::FORMAT;
    ui32 nodeCount = states.size();
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = erasure,
        .ControllerNodeId = 1,
        // Match CreateBoxAndPool's media type so performance updates reach the VDisks.
        .DiskType = NPDisk::DEVICE_TYPE_ROT,
        .MinHugeBlobInBytes = settings.MinHugeBlobSize,
        .UseFakeConfigDispatcher = true,
    });

    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));

    auto groupId = env.GetGroups()[0];
    TString data = TString::Uninitialized(settings.BlobSize);
    memset(data.Detach(), 1, data.size());
    TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
    env.PutBlob(groupId, id, data);
    auto info = env.GetGroupInfo(groupId);
    env.WaitForSync(info, id);
    auto checkRecordKind = [&](ui32 threshold) {
        using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
        const auto expected = erasure.PartSize(TLogoBlobID(id, 1)) >= threshold
            ? T::ERecordType::HugeBlob : T::ERecordType::InplaceBlob;
        for (ui32 partIdx = erasure.DataParts(); partIdx < erasure.TotalPartCount(); ++partIdx) {
            const auto actor = info->GetActorId(NBlobStorageLifecycle::SubgroupDisk(info, id, partIdx));
            env.CompactVDisk(actor);
            auto layout = env.SyncQuery<T, TEvBlobStorage::TEvCaptureVDiskLayout>(actor);
            bool found = false;
            for (const auto& item : layout->Layout) {
                if (item.Database == T::EDatabase::LogoBlobs && item.BlobId.FullID() == id &&
                        item.RecordType != T::ERecordType::IndexRecord) {
                    UNIT_ASSERT_C(item.RecordType == expected,
                        "threshold# " << threshold << " expectedRecordType# " << int(expected) << ' ' << item.ToString());
                    found = true;
                }
            }
            UNIT_ASSERT(found);
        }
    };
    if (erasure.TotalPartCount() > 8) {
        checkRecordKind(settings.MinHugeBlobSize);
        std::fill(states.begin(), states.end(), E::OK);
        // Format the main disks carrying the two high parity parts.
        for (ui32 partIdx = erasure.DataParts(); partIdx < erasure.TotalPartCount(); ++partIdx) {
            const auto disk = NBlobStorageLifecycle::SubgroupDisk(info, id, partIdx);
            states[info->GetActorId(disk).NodeId() - 1] = E::FORMAT;
        }
    }

    auto checkBlob = [&] {
        TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
        auto *msg = res->Get();
        Y_ABORT_UNLESS(msg->ResponseSz == 1);
        if (msg->Responses[0].Status == NKikimrProto::OK) {
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Buffer.ConvertToString(), data);
        }
        return msg->Responses[0].Status;
    };

    Y_ABORT_UNLESS(checkBlob() == NKikimrProto::OK);
    env.Cleanup();

    for (auto& [key, state] : env.PDiskMockStates) {
        if (states[key.first - 1] == E::FORMAT) {
            state.Reset();
        }
    }

    env.Initialize();

    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> detainedMsgs;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplStarted) {
            detainedMsgs.emplace_back(nodeId, std::move(ev));
            return false;
        }
        return true;
    };

    env.Sim(TDuration::Minutes(10));
    Y_ABORT_UNLESS(checkBlob() == NKikimrProto::OK);
    Y_ABORT_IF(detainedMsgs.empty());

    env.Runtime->FilterFunction = {};

    // replication is about to start, updating the minHugeBlobSize in skeleton and resuming replication
    for (auto& [nodeId, detainedEv] : detainedMsgs) {
        TActorId edge = env.Runtime->AllocateEdgeActor(nodeId);

        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto perfConfig = NKikimrConfig::TBlobStorageConfig_TVDiskPerformanceConfig();
        perfConfig.SetPDiskType(PDiskTypeToPDiskType(env.Settings.DiskType));
        perfConfig.SetMinHugeBlobSizeInBytes(settings.MinHugeBlobSizeInRepl);
        auto* vdiskTypes = request->Record.MutableConfig()->MutableBlobStorageConfig()->MutableVDiskPerformanceSettings()->MutableVDiskTypes();
        vdiskTypes->Add(std::move(perfConfig));

        env.Runtime->Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(nodeId), edge, request.Release()), nodeId);
        Y_ABORT_UNLESS(env.Runtime->WaitForEdgeActorEvent({edge})->CastAsLocal<NConsole::TEvConsole::TEvConfigNotificationResponse>());

        env.Runtime->Send(detainedEv.release(), nodeId);
    }

    // waiting for replication to complete 
    env.WaitForSync(env.GetGroupInfo(groupId), id);
        
    UNIT_ASSERT_EQUAL(checkBlob(), NKikimrProto::OK);
    if (erasure.TotalPartCount() > 8) {
        // The running replication job captured its old threshold before the
        // detained TEvReplStarted. Compaction preserves Huge-only input even if
        // the new threshold would select inline storage.
        checkRecordKind(Min(settings.MinHugeBlobSize, settings.MinHugeBlobSizeInRepl));
        // Only the repaired parity disks have been compacted at this point;
        // the other inline parts can still be in Fresh after recovery.
        NBlobStorageLifecycle::CheckMainParts(env, info, id, data);
        const ui32 partSize = erasure.PartSize(TLogoBlobID(id, 1));
        if (partSize >= settings.MinHugeBlobSize && partSize < settings.MinHugeBlobSizeInRepl) {
            // Add fresh input under the new policy to exercise real Huge-to-inline
            // merging, with the same bytes on both repaired parity positions.
            TDataPartSet parts;
            erasure.SplitData(TErasureType::CrcModeNone, data, parts);
            for (ui32 partIdx = erasure.DataParts(); partIdx < erasure.TotalPartCount(); ++partIdx) {
                env.PutBlob(NBlobStorageLifecycle::SubgroupDisk(info, id, partIdx),
                    TLogoBlobID(id, partIdx + 1), parts.Parts[partIdx].OwnedString.ConvertToString());
            }
            checkRecordKind(settings.MinHugeBlobSizeInRepl);
        }
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            env.CompactVDisk(info->GetActorId(i));
        }
        NBlobStorageLifecycle::CheckMainParts(env, info, id, data, true);
        env.Cleanup();
        env.Initialize();
        NBlobStorageLifecycle::CheckGroupBlob(env, info, id, data);
        NBlobStorageLifecycle::CheckMainParts(env, info, id, data, true);
    }
}

Y_UNIT_TEST_SUITE(MinHugeChangeOnReplication) {

    Y_UNIT_TEST(MinHugeDecreased) {
        DoTestCase(TReplTestSettings{ 
            .BlobSize = 200u << 10, 
            .MinHugeBlobSize = 64u << 10,
            .MinHugeBlobSizeInRepl = 512u << 10,
        });
    }

    Y_UNIT_TEST(MinHugeIncreased) {
        DoTestCase(TReplTestSettings{ 
            .BlobSize = 200u << 10, 
            .MinHugeBlobSize = 512u << 10,
            .MinHugeBlobSizeInRepl = 10u << 10,
        });
    }

    Y_UNIT_TEST(MinHugeDecreasedBlock82) {
        DoTestCase({800u << 10, 64u << 10, 512u << 10}, TBlobStorageGroupType::Erasure8Plus2Block);
    }

    Y_UNIT_TEST(MinHugeIncreasedBlock82) {
        DoTestCase({400u << 10, 512u << 10, 10u << 10}, TBlobStorageGroupType::Erasure8Plus2Block);
    }

}
