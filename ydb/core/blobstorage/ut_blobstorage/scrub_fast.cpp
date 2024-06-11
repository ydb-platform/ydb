#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
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
    Y_UNIT_TEST(SingleBlob) {
        Test();
    }
}

