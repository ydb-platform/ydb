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
    TLogoBlobID id(1, 1, 1, 0, data.size(), 0);

    { // write data to group
        TActorId sender = runtime->AllocateEdgeActor(1);
        runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    auto checkReadable = [&](NKikimrProto::EReplyStatus status) {
        TActorId sender = runtime->AllocateEdgeActor(1);
        runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        auto& r = res->Get()->Responses[0];
        UNIT_ASSERT_VALUES_EQUAL(r.Status, status);
        if (status == NKikimrProto::OK) {
            UNIT_ASSERT_VALUES_EQUAL(r.Buffer.ConvertToString(), data);
        }
    };

    checkReadable(NKikimrProto::OK);

    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        const TActorId vdiskActorId = info->GetActorId(i);

        ui32 nodeId, pdiskId;
        std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);
        auto it = env.PDiskMockStates.find(std::make_pair(nodeId, pdiskId));
        Y_VERIFY(it != env.PDiskMockStates.end());

        const TActorId sender = runtime->AllocateEdgeActor(vdiskActorId.NodeId());
        env.Runtime->Send(new IEventHandle(vdiskActorId, sender, new TEvBlobStorage::TEvCaptureVDiskLayout), sender.NodeId());
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCaptureVDiskLayoutResult>(sender);

        for (auto& item : res->Get()->Layout) {
            using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
            if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob) {
                const TDiskPart& part = item.Location;
                it->second->SetCorruptedArea(part.ChunkIdx, part.Offset, part.Offset + part.Size, true);
                break;
            }
        }

        checkReadable(NKikimrProto::OK);
    }

    env.Sim(TDuration::Seconds(60));
}

Y_UNIT_TEST_SUITE(ScrubFast) {
    Y_UNIT_TEST(SingleBlob) {
        Test();
    }
}
