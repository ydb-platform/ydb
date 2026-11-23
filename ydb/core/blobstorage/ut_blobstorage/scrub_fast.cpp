#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/scrub/blob_recovery.h>
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>
#include <ydb/core/blobstorage/vdisk/scrub/scrub_actor.h>
#include <library/cpp/testing/unittest/registar.h>

void Test() {
    SetRandomSeed(1);
    TEnvironmentSetup env{{
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
    }};
    auto& runtime = env.Runtime;
    env.CreateBoxAndPool();
    env.SetScrubPeriodicity(TDuration::Seconds(60));
    env.Sim(TDuration::Minutes(1));
    auto groups = env.GetGroups();
    auto info = env.GetGroupInfo(groups[0]);

    TString data = TString::Uninitialized(8_MB);
    memset(data.Detach(), 'X', data.size());

    for (ui32 step = 1; step < 100; ++step) {
        TLogoBlobID id(1, 1, step, 0, data.size(), 0);

        { // write data to group
            TActorId sender = runtime->AllocateEdgeActor(1);
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        auto checkReadable = [&] {
            TActorId sender = runtime->AllocateEdgeActor(1);
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            auto& r = res->Get()->Responses[0];
            UNIT_ASSERT_VALUES_EQUAL(r.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(r.Buffer.ConvertToString(), data);

            ui32 partsMask = 0;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TVDiskID& vdiskId = info->GetVDiskId(i);
                env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
                    const TActorId sender = runtime->AllocateEdgeActor(1);
                    auto ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead);
                    ev->AddExtremeQuery(id, 0, 0);
                    runtime->Send(new IEventHandle(queueId, sender, ev.release()), sender.NodeId());
                    auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender);
                    auto& record = reply->Get()->Record;
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                    for (const auto& result : record.GetResult()) {
                        if (result.GetStatus() == NKikimrProto::OK) {
                            const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                            UNIT_ASSERT(id.PartId());
                            const ui32 partIdx = id.PartId() - 1;
                            const ui32 mask = 1 << partIdx;
                            UNIT_ASSERT(!(partsMask & mask));
                            partsMask |= mask;
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::NODATA);
                        }
                    }
                });
            }
            UNIT_ASSERT_VALUES_EQUAL(partsMask, (1 << info->Type.TotalPartCount()) - 1);
        };

        checkReadable();

        ui32 mask = 0;

        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TActorId vdiskActorId = info->GetActorId(i);

            ui32 nodeId, pdiskId;
            std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);
            auto it = env.PDiskMockStates.find(std::make_pair(nodeId, pdiskId));
            Y_ABORT_UNLESS(it != env.PDiskMockStates.end());

            const TActorId sender = runtime->AllocateEdgeActor(vdiskActorId.NodeId());
            env.Runtime->Send(new IEventHandle(vdiskActorId, sender, new TEvBlobStorage::TEvCaptureVDiskLayout), sender.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCaptureVDiskLayoutResult>(sender);

            for (auto& item : res->Get()->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob && item.BlobId.FullID() == id) {
                    const TDiskPart& part = item.Location;
                    mask |= 1 << i;
                    it->second->SetCorruptedArea(part.ChunkIdx, part.Offset, part.Offset + 1 + RandomNumber(part.Size), true);
                    break;
                }
            }

            checkReadable();
        }

        env.Sim(TDuration::Seconds(60));

        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            if (~mask >> i & 1) {
                continue;
            }

            const TActorId vdiskActorId = info->GetActorId(i);

            ui32 nodeId, pdiskId;
            std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);
            auto it = env.PDiskMockStates.find(std::make_pair(nodeId, pdiskId));
            Y_ABORT_UNLESS(it != env.PDiskMockStates.end());

            const TActorId sender = runtime->AllocateEdgeActor(vdiskActorId.NodeId());
            env.Runtime->Send(new IEventHandle(vdiskActorId, sender, new TEvBlobStorage::TEvCaptureVDiskLayout), sender.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCaptureVDiskLayoutResult>(sender);

            bool anyPartReadable = false;

            for (auto& item : res->Get()->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob && item.BlobId.FullID() == id) {
                    const TDiskPart& part = item.Location;
                    anyPartReadable = !it->second->HasCorruptedArea(part.ChunkIdx, part.Offset, part.Offset + part.Size);
                    if (anyPartReadable) {
                        break;
                    }
                }
            }

            UNIT_ASSERT(anyPartReadable);
        }
    }
}

Y_UNIT_TEST_SUITE(ScrubFast) {
    Y_UNIT_TEST(RecoverCorruptedPartInSoloRealm) {
        using TLayout = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
        TEnvironmentSetup env{{.Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        env.CreateBoxAndPool();
        env.UpdateSettings(false, false);
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const auto initialInfo = env.GetGroupInfo(groupId);
        const TString data(1_MB, 'x');
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
        const auto& topology = initialInfo->GetTopology();
        ui32 includedRealm = 0;
        for (ui32 disk = 0; disk < initialInfo->GetTotalVDisksNum(); ++disk) {
            if (initialInfo->GetActorId(disk).NodeId() == env.Settings.ControllerNodeId) {
                includedRealm = initialInfo->GetVDiskId(disk).FailRealm;
            }
        }

        TBlobStorageGroupInfo::TOrderNums subgroup;
        topology.PickSubgroup(id.Hash(), subgroup);
        ui32 targetIdx = 0;
        while (initialInfo->GetVDiskId(subgroup[targetIdx]).FailRealm != includedRealm) {
            ++targetIdx;
        }
        const ui32 targetDisk = subgroup[targetIdx];
        const ui32 sourceDisk = subgroup[targetIdx + 3];
        const TLogoBlobID partId(id, targetIdx + 1);
        const auto target = initialInfo->GetActorId(targetDisk);
        const auto source = initialInfo->GetActorId(sourceDisk);
        env.PutBlob(initialInfo->GetVDiskId(targetDisk), partId, data);
        env.PutBlob(initialInfo->GetVDiskId(sourceDisk), partId, data);

        std::unique_ptr<IEventHandle> scrubGrant;
        bool holdGrant = true;
        bool readFromSource = false;
        std::optional<NKikimrProto::EReplyStatus> restoreStatus;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (holdGrant && type == TEvBlobStorage::EvControllerScrubStartQuantum && ev->Recipient == target) {
                scrubGrant = std::move(ev);
                return false;
            }
            if (!holdGrant && type == TEvBlobStorage::EvVGet && nodeId == source.NodeId()
                    && ev->Recipient == source && ev->Sender.NodeId() == target.NodeId()) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVGet>()->Record;
                for (const auto& query : record.GetExtremeQueries()) {
                    readFromSource |= record.GetHandleClass() == NKikimrBlobStorage::AsyncRead
                        && LogoBlobIDFromLogoBlobID(query.GetId()) == id;
                }
            }
            if (!holdGrant && type == TEvBlobStorage::EvRestoreCorruptedBlobResult && nodeId == target.NodeId()) {
                for (const auto& item : ev->Get<TEvRestoreCorruptedBlobResult>()->Items) {
                    if (item.BlobId == id) {
                        restoreStatus = item.Status;
                    }
                }
            }
            return true;
        };

        // Reserve a real scrub quantum while all realms are up. Run it after losing both peers.
        env.SetScrubPeriodicity(TDuration::Seconds(1));
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_C(scrubGrant, "The controller did not grant a scrub quantum for the target VDisk");
        for (ui32 disk = 0; disk < initialInfo->GetTotalVDisksNum(); ++disk) {
            if (initialInfo->GetVDiskId(disk).FailRealm != includedRealm) {
                env.StopNode(initialInfo->GetActorId(disk).NodeId());
            }
        }
        env.Sim(TDuration::Seconds(10));

        using TRealm = NKikimrBlobStorage::TDataCenterState;
        NKikimrBlobStorage::TConfigRequest config;
        config.SetIgnoreGroupFailModelChecks(true);
        config.SetIgnoreDegradedGroupsChecks(true);
        auto* definition = config.AddCommand()->MutableDefineGroupState()->AddGroups();
        definition->SetGroupId(groupId);
        definition->SetGroupState(NKikimrBlobStorage::TGroupState::DC_SOLO_INV2);
        for (ui32 realm = 0; realm < 3; ++realm) {
            definition->AddFailRealmStates(realm == includedRealm ? TRealm::INCLUDED : TRealm::EXCLUDED);
        }
        const auto response = env.Invoke(config);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Sim(TDuration::Seconds(10));
        const auto info = env.GetGroupInfo(groupId);
        UNIT_ASSERT_GT(info->GroupGeneration, initialInfo->GroupGeneration);
        UNIT_ASSERT_VALUES_EQUAL(info->GetMirror3dcState().State, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2);

        const auto [nodeId, pdiskId, slotId] = DecomposeVDiskServiceId(target);
        const auto& pdisk = env.PDiskMockStates.at({nodeId, pdiskId});
        const auto layout = env.SyncQuery<TLayout, TEvBlobStorage::TEvCaptureVDiskLayout>(target);
        bool corrupted = false;
        for (const auto& item : layout->Layout) {
            if (item.RecordType == TLayout::ERecordType::HugeBlob && item.BlobId.FullID() == id) {
                const auto& loc = item.Location;
                pdisk->SetCorruptedArea(loc.ChunkIdx, loc.Offset, loc.Offset + loc.Size, true);
                corrupted = true;
            }
        }
        UNIT_ASSERT(corrupted);

        holdGrant = false;
        env.Runtime->Send(scrubGrant.release(), nodeId);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_C(readFromSource, "The scrub VGet did not reach the surviving replica");
        UNIT_ASSERT_C(restoreStatus, "Scrub did not finish restoring the corrupted part");
        UNIT_ASSERT_VALUES_EQUAL(*restoreStatus, NKikimrProto::OK);
        env.Runtime->FilterFunction = {};
        env.CheckBlob(target, info->GetVDiskId(targetDisk), partId, data);
    }

    Y_UNIT_TEST(RecoverBlobInExtendedGroup) {
        TEnvironmentSetup env{{
            .NodeCount = 12,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        }};
        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(3);
        geometry.SetNumFailDomainsPerFailRealm(4);
        geometry.SetNumVDisksPerFailDomain(1);
        env.CreateBoxAndPool(1, 1, 0, NKikimrBlobStorage::EPDiskType::ROT, geometry);
        env.Sim(TDuration::Minutes(1));
        const auto info = env.GetGroupInfo(env.GetGroups().front());
        const TString data = "surviving replica";
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
        TBlobStorageGroupInfo::TOrderNums nums;
        info->GetTopology().PickSubgroup(id.Hash(), nums);
        // With replication paused, only VDisks outside the old AddBlobQuery range hold data.
        for (ui32 i = 0; i < nums.size(); ++i) {
            if (nums[i] >= info->Type.BlobSubgroupSize()) {
                env.PutBlob(info->GetVDiskId(nums[i]), TLogoBlobID(id, i % 3 + 1), data);
            }
        }

        const auto vdisk = info->GetActorId(nums[0]);
        const auto edge = env.Runtime->AllocateEdgeActor(vdisk.NodeId());
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(1);
        auto request = std::make_unique<TEvRecoverBlob>();
        request->Deadline = deadline;
        request->Items.emplace_back(id, NMatrix::TVectorType::MakeOneHot(0, 3), info->Type, TDiskPart());
        auto event = std::make_unique<IEventHandle>(vdisk, edge, request.release());
        event->Rewrite(TEvBlobStorage::EvForwardToSkeleton, vdisk);
        env.Runtime->Send(event.release(), edge.NodeId());
        const auto result = env.WaitForEdgeActorEvent<TEvRecoverBlobResult>(edge, true, deadline + TDuration::Seconds(1));
        UNIT_ASSERT_C(result, "Timed out waiting for TEvRecoverBlobResult");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Items.size(), 1);
        const auto& item = result->Get()->Items.front();
        UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(item.GetPartData(TLogoBlobID(id, 1)).ConvertToString(), data);
    }

    Y_UNIT_TEST(SingleBlob) {
        Test();
    }
}

