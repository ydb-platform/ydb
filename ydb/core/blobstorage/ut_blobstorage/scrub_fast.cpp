#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/scrub/blob_recovery.h>
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

namespace {
    struct TBlobRecoveryFixture {
        TEnvironmentSetup Env;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TString Data = TString(1024, 'x');
        const TLogoBlobID Id = TLogoBlobID(1, 1, 1, 0, Data.size(), 0);
        TDataPartSet Parts;

        TBlobRecoveryFixture(TBlobStorageGroupType type = TBlobStorageGroupType::Erasure4Plus2Block)
            : Env(TEnvironmentSetup::TSettings{
                .VDiskReplPausedAtStart = true,
                .Erasure = type,
            })
        {
            Env.CreateBoxAndPool();
            Env.Sim(TDuration::Minutes(1));
            Info = Env.GetGroupInfo(Env.GetGroups().front());
            Info->Type.SplitData(TErasureType::CrcModeNone, Data, Parts);
        }

        TVDiskID VDisk(ui32 subgroupIdx) const {
            return Info->GetVDiskInSubgroup(subgroupIdx, Id.Hash());
        }

        void Put(ui32 partId, ui32 subgroupIdx) {
            Env.PutBlob(VDisk(subgroupIdx), TLogoBlobID(Id, partId),
                Info->Type.PartSize(TLogoBlobID(Id, partId))
                    ? Parts.Parts[partId - 1].OwnedString.ConvertToString() : TString());
        }

        auto Recover(ui32 subgroupIdx, std::initializer_list<ui32> neededParts) {
            const auto vdisk = Info->GetActorId(TVDiskIdShort(VDisk(subgroupIdx)));
            const auto edge = Env.Runtime->AllocateEdgeActor(vdisk.NodeId());
            const auto deadline = Env.Runtime->GetClock() + TDuration::Seconds(5);
            auto request = std::make_unique<TEvRecoverBlob>();
            request->Deadline = deadline;
            for (const ui32 partId : neededParts) {
                request->Items.emplace_back(Id, NMatrix::TVectorType::MakeOneHot(partId - 1,
                    Info->Type.TotalPartCount()), Info->Type, TDiskPart());
            }
            auto event = std::make_unique<IEventHandle>(vdisk, edge, request.release());
            event->Rewrite(TEvBlobStorage::EvForwardToSkeleton, vdisk);
            Env.Runtime->Send(event.release(), edge.NodeId());
            auto result = Env.WaitForEdgeActorEvent<TEvRecoverBlobResult>(edge, true, deadline + TDuration::Seconds(1));
            UNIT_ASSERT_C(result, "Timed out waiting for TEvRecoverBlobResult");
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Items.size(), neededParts.size());
            return result;
        }
    };
}

Y_UNIT_TEST_SUITE(ScrubFast) {
    Y_UNIT_TEST(MultipartReplyBeforeLastRequiredPart) {
        TBlobRecoveryFixture f;
        for (ui32 partId = 1; partId <= 3; ++partId) {
            f.Put(partId, 6);
        }
        f.Put(4, 3);
        std::set<TVDiskIdShort> responded;
        std::unique_ptr<IEventHandle> delayed;
        ui32 delayedNodeId = 0;
        bool released = false;
        bool multipartSeen = false;
        f.Env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvVGetResult) {
                return true;
            }
            const auto& record = ev->Get<TEvBlobStorage::TEvVGetResult>()->Record;
            if (!record.ResultSize() || LogoBlobIDFromLogoBlobID(record.GetResult(0).GetBlobID()).FullID() != f.Id) {
                return true;
            }
            const auto vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
            if (vdisk == f.VDisk(3) && !released) {
                UNIT_ASSERT(!delayed);
                delayedNodeId = nodeId;
                delayed = std::move(ev);
            } else if (vdisk != f.VDisk(3)) {
                responded.emplace(vdisk);
                multipartSeen |= vdisk == f.VDisk(6) && record.ResultSize() == 3;
            }
            // Deliver the fourth source part after all other peers, including the multipart reply.
            if (delayed && responded.size() == 6) {
                released = true;
                f.Env.Runtime->Schedule(TDuration::MilliSeconds(1), delayed.release(), nullptr, delayedNodeId);
            }
            return bool(ev);
        };
        const auto result = f.Recover(4, {5});
        f.Env.Runtime->FilterFunction = nullptr;
        UNIT_ASSERT(multipartSeen);
        UNIT_ASSERT(released);
        const auto& item = result->Get()->Items.front();
        UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(item.GetPartData(TLogoBlobID(f.Id, 5)).ConvertToString(),
            f.Parts.Parts[4].OwnedString.ConvertToString());
    }

    Y_UNIT_TEST(CompletedItemDoesNotCompleteRequestTwice) {
        TBlobRecoveryFixture f;
        // Three single-part replies ensure the first item completes before the last two replies.
        for (const ui32 subgroupIdx : {0, 6, 7}) {
            f.Put(1, subgroupIdx);
        }
        const auto result = f.Recover(4, {1, 5});
        const auto& items = result->Get()->Items;
        UNIT_ASSERT_VALUES_EQUAL(items[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(items[0].GetPartData(TLogoBlobID(f.Id, 1)).ConvertToString(),
            f.Parts.Parts[0].OwnedString.ConvertToString());
        UNIT_ASSERT_VALUES_EQUAL(items[1].Status, NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(Mirror3of4MetadataPart) {
        TBlobRecoveryFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        f.Put(3, f.Info->Type.BlobSubgroupSize() - 1);
        const auto result = f.Recover(2, {3});
        const auto& item = result->Get()->Items.front();
        UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
        UNIT_ASSERT(item.GetPartData(TLogoBlobID(f.Id, 3)).empty());
    }

    Y_UNIT_TEST(Mirror3of4MetadataCannotRestorePayload) {
        TBlobRecoveryFixture f(TBlobStorageGroupType::ErasureMirror3of4);
        f.Put(3, f.Info->Type.BlobSubgroupSize() - 1);
        const auto result = f.Recover(0, {1});
        const auto& item = result->Get()->Items.front();
        UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(item.PartsMask, 4);
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

