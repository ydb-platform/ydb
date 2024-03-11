#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

Y_UNIT_TEST_SUITE(Mirror3dcRestore) {

    Y_UNIT_TEST(TestRestore) {
        TEnvironmentSetup env{{.Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
        const auto& groups = env.GetGroups();
        const ui32 groupId = *groups.begin();
        auto info = env.GetGroupInfo(groupId);

        ui32 index = 1;

        for (ui32 diskMask = 1; diskMask < 512; ++diskMask) {
            TLogoBlobID id(index++, 1, 1, 0, 1, 1);
            TString data(id.BlobSize(), 'X');

            TBlobStorageGroupInfo::TVDiskIds vdiskIds;
            TBlobStorageGroupInfo::TServiceIds serviceIds;
            info->PickSubgroup(id.Hash(), &vdiskIds, &serviceIds);

            for (ui32 i = 0; i < 9; ++i) {
                if (diskMask >> i & 1) {
                    env.PutBlob(vdiskIds[i], TLogoBlobID(id, 1 + i % 3), data);
                }
            }

            const TActorId& edge = env.Runtime->AllocateEdgeActor(1);
            env.Runtime->WrapInActorContext(edge, [&] {
                auto ev = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, true /*mustRestoreFirst*/, true /*isIndexOnly*/);
                SendToBSProxy(edge, groupId, ev.release());
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);

            std::map<ui32, ui32> domainsPerRealm;

            for (ui32 i = 0; i < vdiskIds.size(); ++i) {
                env.WithQueueId(vdiskIds[i], NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
                    const TActorId& edge = env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                    env.Runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskIds[i],
                        TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                        Nothing(), {{id, 0u, ui32(data.size())}}).release()), queueId.NodeId());
                    auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
                    auto& record = r->Get()->Record;
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                    const auto& res = record.GetResult(0);
                    if (res.GetStatus() == NKikimrProto::OK) {
                        UNIT_ASSERT_VALUES_EQUAL(data, r->Get()->GetBlobData(res).ConvertToString());
                        const ui32 realm = i % 3;
                        ++domainsPerRealm[realm];
                    }
                });
            }

            ui32 realms1plus = 0, realms2plus = 0;
            for (const auto& [_, domains] : domainsPerRealm) {
                realms1plus += domains >= 1;
                realms2plus += domains >= 2;
            }

            UNIT_ASSERT(realms1plus >= 3 || realms2plus >= 2);
        }
    }

}
