#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BlobDepot) {

    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env{{
            .SetupTablets = true
        }};

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(20));

        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
        cmd->SetVirtualGroupPool("vg");
        cmd->SetStoragePoolName(env.StoragePoolName);
        cmd->SetParentDir("/Root");
        auto *prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolKind(env.StoragePoolName);
        prof->SetCount(3);

        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        ui32 groupId = response.GetStatus(0).GetGroupId(0);
        Cerr << "groupId# " << groupId << Endl;
        const TActorId& proxy = MakeBlobStorageProxyID(groupId);

        auto sender = env.Runtime->AllocateEdgeActor(1);
        TString data = "hello!";
        TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
        env.Runtime->Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvPut(id, data, TInstant::Max())), 1);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender);
        Cerr << res->Get()->ToString() << Endl;
    }

}
