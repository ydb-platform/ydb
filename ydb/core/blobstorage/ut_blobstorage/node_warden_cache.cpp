#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(NodeWardenCache) {

    bool TryReadBlob(TEnvironmentSetup& env, ui32 groupId, const TLogoBlobID& blobId, ui32 senderNodeId) {
        const TActorId sender = env.Runtime->AllocateEdgeActor(senderNodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(blobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        if (!res) {
            return false;
        }
        auto *msg = res->Get();
        return msg->Status == NKikimrProto::OK && msg->ResponseSz == 1 && msg->Responses[0].Status == NKikimrProto::OK;
    }

    void DoTestRecoveryWhileBscDown(bool useCache, bool expectReadableAfterRestart) {
        const ui32 controllerNodeId = 3;
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = controllerNodeId,
            .Cache = useCache,
        }};

        env.CreateBoxAndPool(/*numDrivesPerNode=*/1, /*numGroups=*/1, /*numStorageNodes=*/2);
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(info->GetTotalVDisksNum(), 1);
        const ui32 vdiskNodeId = info->GetActorId(0).NodeId();
        UNIT_ASSERT_C(vdiskNodeId != controllerNodeId, "VDisk unexpectedly landed on the controller node");

        const ui32 readerNodeId = vdiskNodeId == 1 ? 2 : 1;

        const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
        env.PutBlob(groupId, blobId, "test");

        UNIT_ASSERT_C(TryReadBlob(env, groupId, blobId, readerNodeId),
            "blob must be readable before the restart");

        env.StopNode(controllerNodeId);

        env.RestartNode(vdiskNodeId);
        env.Sim(TDuration::Seconds(60));

        const bool readable = TryReadBlob(env, groupId, blobId, readerNodeId);
        UNIT_ASSERT_VALUES_EQUAL(readable, expectReadableAfterRestart);
    }

    Y_UNIT_TEST(StorageNodeRecoversFromCacheWhileBscDown) {
        DoTestRecoveryWhileBscDown(/*useCache=*/true, /*expectReadableAfterRestart=*/true);
    }

    Y_UNIT_TEST(StorageNodeStuckWithoutCacheWhileBscDown) {
        DoTestRecoveryWhileBscDown(/*useCache=*/false, /*expectReadableAfterRestart=*/false);
    }
}
