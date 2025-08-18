#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(NodeDisconnected) {
    Y_UNIT_TEST(BsQueueRetries) {
        ui32 nodeCount = 8;
        TEnvironmentSetup env{{
            .NodeCount = nodeCount,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        }};
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(60));

        std::vector<ui32> groups = env.GetGroups();
        // ui32 groupId = groups.front();
        Y_ABORT_UNLESS(groups.size() == 1);
        TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        const ui32 queuesCount = 800'000;
        ui32 orderNumber = 1;

        TVDiskID vdiskId = info->GetVDiskId(orderNumber);
        ui32 badNodeId = info->GetActorId(orderNumber).NodeId();
        ui32 goodNodeId = nodeCount + 1 - badNodeId;
        Y_VERIFY(badNodeId != goodNodeId);

        TActorId edge = env.Runtime->AllocateEdgeActor(goodNodeId);

        std::vector<TActorId> queues;

        for (ui32 i = 0; i < queuesCount; ++i) {
            queues.push_back(env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, i, goodNodeId));
        }

        ui32 ctr = 0;

        for (const TActorId actorId : queues) {
            env.Runtime->Send(new IEventHandle(actorId, edge, new TEvBSQueueResetConnection), edge.NodeId());
        }

        env.Sim(TDuration::Seconds(60));

        env.Runtime->StopNode(badNodeId);

        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Type == TEvBlobStorage::TEvVCheckReadiness::EventType) {
                ++ctr;
            }
            return true;
        };

        for (const TActorId actorId : queues) {
            env.Runtime->Send(new IEventHandle(actorId, edge, new TEvBSQueueResetConnection), edge.NodeId());
            // env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatus>(edge, false, TInstant::Max());
        }

        const ui32 simSeconds = 60;

        env.Sim(TDuration::Seconds(simSeconds));
        Cerr << "CTR " << ctr << Endl;

        UNIT_ASSERT(ctr < queuesCount * simSeconds / 10);
    }
}
