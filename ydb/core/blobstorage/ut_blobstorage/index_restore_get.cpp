#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

Y_UNIT_TEST_SUITE(IndexRestoreGet) {
    void RunBlobRecovery(TBlobStorageGroupType::EErasureSpecies erasure) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = TBlobStorageGroupType(erasure).BlobSubgroupSize(),
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        TLogoBlobID id;
        TString error;
        const bool success = TLogoBlobID::Parse(id, "[72075186270680851:57:3905:6:786432:4194304:0]", error);
        UNIT_ASSERT(success);

        TString data = TString(id.BlobSize(), 'X');

        TDataPartSet parts;
        info->Type.SplitData((TErasureType::ECrcMode)id.CrcMode(), data, parts);

        TBlobStorageGroupInfo::TOrderNums nums;
        info->GetTopology().PickSubgroup(id.Hash(), nums);
        const ui32 partCount = info->Type.TotalPartCount();
        const ui32 subgroupSize = info->Type.BlobSubgroupSize();
        const ui32 missingPart = info->Type.DataParts();

        for (ui32 i = 0; i < partCount; ++i) {
            const ui32 partId = i + 1;
            const ui32 orderNumber = nums[i];
            if (i != missingPart) {
                env.PutBlob(info->GetVDiskId(orderNumber), TLogoBlobID(id, partId), parts.Parts[i].OwnedString.ConvertToString());
            }
        }

        {
            const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                    1, id.Channel(), true, id.Generation(), id.Step(), new TVector<TLogoBlobID>{id}, nullptr,
                    TInstant::Max(), true, TWriteSource::Unknown, false));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        env.Sim(TDuration::Minutes(1));

        for (ui32 idx = 0; idx < subgroupSize; ++idx) {
            const ui32 orderNumber = nums[idx];
            Cerr << idx << ' ' << orderNumber << Endl;

            std::vector<ui32> v;
            if (idx < partCount) {
                v.push_back(idx + 1);
            } else {
                for (ui32 partId = 1; partId <= partCount; ++partId) {
                    v.push_back(partId);
                }
            }

            for (ui32 partId : v) {
                env.CheckBlob(info->GetActorId(orderNumber), info->GetVDiskId(orderNumber), TLogoBlobID(id, partId),
                    parts.Parts[partId - 1].OwnedString.ConvertToString(),
                    idx == missingPart || idx >= partCount ? NKikimrProto::NODATA : NKikimrProto::OK);
            }
        }

        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
//            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
//                NKikimrBlobStorage::EGetHandleClass::FastRead, true, true));
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvRange(id.TabletID(), id, id, true, TInstant::Max(), true));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), 1);
//        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
//        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
//        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
//        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);

        for (ui32 idx = 0; idx < subgroupSize; ++idx) {
            const ui32 orderNumber = nums[idx];
            Cerr << idx << ' ' << orderNumber << Endl;

            std::vector<ui32> v;
            if (idx < partCount) {
                v.push_back(idx + 1);
            } else {
                for (ui32 partId = 1; partId <= partCount; ++partId) {
                    v.push_back(partId);
                }
            }

            for (ui32 partId : v) {
                env.CheckBlob(info->GetActorId(orderNumber), info->GetVDiskId(orderNumber), TLogoBlobID(id, partId),
                    parts.Parts[partId - 1].OwnedString.ConvertToString(),
                    idx >= partCount ? NKikimrProto::NODATA : NKikimrProto::OK);
            }
        }
    }

    Y_UNIT_TEST(BlobRecovery) { RunBlobRecovery(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(BlobRecoveryBlock82) { RunBlobRecovery(TBlobStorageGroupType::Erasure8Plus2Block); }
}
