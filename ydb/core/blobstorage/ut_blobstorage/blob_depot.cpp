#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

using namespace NKikimr::NBlobDepot;

Y_UNIT_TEST_SUITE(BlobDepot) {

    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .BlobDepotId = MakeTabletID(1, 0, 0x10000),
            .BlobDepotChannels = 4
        }};

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(20));

        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
        cmd->SetVirtualGroupPool("vg");
        cmd->SetStoragePoolName(env.StoragePoolName);
        cmd->SetParentDir("/Root");
        cmd->SetBlobDepotId(env.Settings.BlobDepotId);
        auto *prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolKind("");
        prof->SetCount(2);
        prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolKind("");
        prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        prof->SetCount(2);

        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        {
            auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
            auto *config = ev->Record.MutableConfig();
            config->SetOperationMode(NKikimrBlobDepot::VirtualGroup);
            config->MutableChannelProfiles()->CopyFrom(cmd->GetChannelProfiles());

            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->SendToPipe(env.Settings.BlobDepotId, edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            env.WaitForEdgeActorEvent<TEvBlobDepot::TEvApplyConfigResult>(edge);
        }

        ui32 groupId = response.GetStatus(0).GetGroupId(0);
        Cerr << "groupId# " << groupId << Endl;

        auto sender = env.Runtime->AllocateEdgeActor(1);
        TString data = "hello!";
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender);
            Cerr << "TEvPutResult# " << res->Get()->ToString() << Endl;
        }

        sender = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer, data);
        }

        sender = env.Runtime->AllocateEdgeActor(2);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer, data);
        }

        env.Sim(TDuration::Seconds(20));
    }

}
