#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

Y_UNIT_TEST_SUITE(IndexRestoreGet) {
    Y_UNIT_TEST(BlobRecovery) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
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

        TBlobStorageGroupInfo::TOrderNums nums{5, 6, 7, 0, 1, 2, 3, 4};

        for (ui32 i = 0; i < 6; ++i) {
            const ui32 partId = i + 1;
            const ui32 orderNumber = nums[i];
            if (i != 4) {
                env.PutBlob(info->GetVDiskId(orderNumber), TLogoBlobID(id, partId), parts.Parts[i].OwnedString.ConvertToString());
            }
        }

        {
            const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                    1, id.Channel(), true, id.Generation(), id.Step(), new TVector<TLogoBlobID>{id}, nullptr,
                    TInstant::Max(), true, false));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        env.Sim(TDuration::Minutes(1));

        for (ui32 idx = 0; idx < 8; ++idx) {
            const ui32 orderNumber = nums[idx];
            Cerr << idx << ' ' << orderNumber << Endl;

            std::vector<ui32> v;
            if (idx < 6) {
                v.push_back(idx + 1);
            } else {
                v.push_back(1);
                v.push_back(2);
                v.push_back(3);
                v.push_back(4);
                v.push_back(5);
                v.push_back(6);
            }

            for (ui32 partId : v) {
                env.CheckBlob(info->GetActorId(orderNumber), info->GetVDiskId(orderNumber), TLogoBlobID(id, partId),
                    parts.Parts[partId - 1].OwnedString.ConvertToString(),
                    idx == 4 || idx == 6 || idx == 7 ? NKikimrProto::NODATA : NKikimrProto::OK);
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

        for (ui32 idx = 0; idx < 8; ++idx) {
            const ui32 orderNumber = nums[idx];
            Cerr << idx << ' ' << orderNumber << Endl;

            std::vector<ui32> v;
            if (idx < 6) {
                v.push_back(idx + 1);
            } else {
                v.push_back(1);
                v.push_back(2);
                v.push_back(3);
                v.push_back(4);
                v.push_back(5);
                v.push_back(6);
            }

            for (ui32 partId : v) {
                env.CheckBlob(info->GetActorId(orderNumber), info->GetVDiskId(orderNumber), TLogoBlobID(id, partId),
                    parts.Parts[partId - 1].OwnedString.ConvertToString(),
                    idx == 6 || idx == 7 ? NKikimrProto::NODATA : NKikimrProto::OK);
            }
        }
    }
}
