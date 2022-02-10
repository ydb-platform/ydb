#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h> 
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h> 

Y_UNIT_TEST_SUITE(BlobStorageBlockRace) {
    Y_UNIT_TEST(Test) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        auto sendBlock = [&](ui32 orderNum, ui64 tabletId, ui64 guid) {
            const TVDiskID vdiskId = info->GetVDiskId(orderNum);

            NKikimrProto::EReplyStatus status;

            env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
                const TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVBlock(tabletId, 1, vdiskId,
                    TInstant::Max(), guid)), edge.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVBlockResult>(edge);
                status = res->Get()->Record.GetStatus();
            });

            return status;
        };

        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 1, 0), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 1, 0), NKikimrProto::ALREADY);
        env.Sim(TDuration::Seconds(10));
        for (ui32 orderNum = 1; orderNum < info->GetTotalVDisksNum(); ++orderNum) {
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 1, 0), NKikimrProto::ALREADY);
        }

        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 2, 1), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 2, 1), NKikimrProto::OK);
        env.Sim(TDuration::Seconds(10));
        for (ui32 orderNum = 1; orderNum < info->GetTotalVDisksNum(); ++orderNum) {
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 2, 0), NKikimrProto::ALREADY);
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 2, 1), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 2, 2), NKikimrProto::ALREADY);
        }
    }
}
