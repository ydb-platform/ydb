#pragma once

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr::NRestorePartTest {

    struct TBlobFixture {
        TEnvironmentSetup Env;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TString Data;
        const TLogoBlobID FullId;
        TDataPartSet Parts;

        TBlobFixture(TBlobStorageGroupType type = TBlobStorageGroupType::Erasure4Plus2Block,
                ui32 size = 1024, bool encryption = false)
            : Env(TEnvironmentSetup::TSettings{
                .NodeCount = 9,
                .VDiskReplPausedAtStart = true,
                .Erasure = type,
                .Encryption = encryption,
                .ConfigPreprocessor = [encryption](ui32, TNodeWardenConfig& config) {
                    if (encryption) {
                        const ui8 keyData[32] = "VRestorePart test key";
                        config.TenantKey = TEncryptionKey{
                            .Key = {keyData, sizeof(keyData)},
                            .Version = 1,
                            .Id = "tenant key",
                        };
                    }
                },
            })
            , Data(size, 'x')
            , FullId(1, 1, 1, 0, size, 0)
        {
            Env.CreateBoxAndPool(1, 1);
            Env.Sim(TDuration::Minutes(1));
            const auto groups = Env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            Info = Env.GetGroupInfo(groups.front());
            Info->Type.SplitData(TErasureType::CrcModeNone, Data, Parts);
        }

        TVDiskID VDisk(ui32 subgroupIdx) const {
            return Info->GetVDiskInSubgroup(subgroupIdx, FullId.Hash());
        }

        void Put(ui32 partId, ui32 subgroupIdx) {
            Env.PutBlob(VDisk(subgroupIdx), TLogoBlobID(FullId, partId),
                Parts.Parts[partId - 1].OwnedString.ConvertToString());
        }

        void PutDataParts() {
            for (ui32 i = 1; i <= Info->Type.DataParts(); ++i) {
                Put(i, i - 1);
            }
        }

        TString Read(ui32 partId, ui32 subgroupIdx, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            const TVDiskID vdiskId = VDisk(subgroupIdx);
            TString data;
            Env.WithQueueId(vdiskId, NKikimrBlobStorage::GetFastRead, [&](TActorId queueId) {
                const auto edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, {},
                    {TLogoBlobID(FullId, partId)});
                Env.Runtime->Send(new IEventHandle(queueId, edge, query.release()), edge.NodeId());
                auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                const auto& record = result->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResult(0).GetStatus(), status);
                data = result->Get()->GetBlobData(record.GetResult(0)).ConvertToString();
            });
            return data;
        }

        NKikimrBlobStorage::TEvVRestorePartResult RestorePart(ui32 subgroupIdx, const TLogoBlobID& blobId,
                TDuration timeout = TDuration::Seconds(30)) {
            const auto vdiskId = VDisk(subgroupIdx);
            const auto target = Info->GetActorId(TVDiskIdShort(vdiskId));
            const auto edge = Env.Runtime->AllocateEdgeActor(target.NodeId(), __FILE__, __LINE__);
            const auto deadline = Env.Runtime->GetClock() + timeout;
            auto request = std::make_unique<TEvVRestorePart>();
            VDiskIDFromVDiskID(vdiskId, request->Record.MutableVDiskID());
            LogoBlobIDFromLogoBlobID(blobId, request->Record.MutableBlobID());
            request->Record.SetDeadline(deadline.GetValue());
            Env.Runtime->Send(new IEventHandle(target, edge, request.release(), 0, 456), target.NodeId());
            auto result = Env.WaitForEdgeActorEvent<TEvVRestorePartResult>(edge, true, deadline + TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 456);
            UNIT_ASSERT(result->Get()->Record.HasStatus());
            UNIT_ASSERT(result->Get()->Record.HasResponseOrigin());
            return result->Get()->Record;
        }
    };

} // NKikimr::NRestorePartTest
