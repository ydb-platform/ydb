#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

using NBridgeTest::GetBridgedLocation;
using NBridgeTest::SwitchBridgePileState;

Y_UNIT_TEST_SUITE(BridgeRestart) {
    Y_UNIT_TEST(RegistrationSnapshotPrecedesDiskUpdates) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto& runtime = env.Runtime;
        bool restarted = false;
        std::set<ui32> registeredNodes;
        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            if (restarted && nodeId >= 9 && nodeId <= 24 && event->GetTypeRewrite() ==
                    TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                const auto& record = event->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                if (record.GetComprehensive()) {
                    registeredNodes.insert(nodeId);
                }
                UNIT_ASSERT_C(record.GetServiceSet().VDisksSize() == 0 || registeredNodes.contains(nodeId),
                    "VDisk delta arrived before registration snapshot: " << record.DebugString());
            }
            return true;
        };
        const auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        ui32 step = 0;
        auto put = [&](TString data) {
            const TLogoBlobID id(100500, 1, ++step, 0, data.size(), 0);
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data,
                    runtime->GetClock() + TDuration::Seconds(30)));
            });
            auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false,
                runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT_C(response, "Timed out waiting for Put");
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
        };
        put("before disconnect");
        put(TString(128_KB, 'a'));
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        // The root's acknowledgement can precede propagation to the local bridge proxy.
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender,
            new TEvNodeWardenQueryStorageConfig(true)), 1);
        const auto disconnectDeadline = runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            auto response = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(sender, false, disconnectDeadline);
            UNIT_ASSERT_C(response, "Timed out waiting for the local disconnected state");
            const auto& state = response->Get()->Config->GetClusterState();
            if (state.GetPerPileState(1) == NKikimrBridge::TClusterState::DISCONNECTED &&
                    state.GetPerPileState(2) == NKikimrBridge::TClusterState::DISCONNECTED) {
                break;
            }
        }
        runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
            MakeBlobStorageNodeWardenID(1), sender, nullptr, 0), 1);
        for (ui32 nodeId = 9; nodeId <= 24; ++nodeId) {
            env.StopNode(nodeId);
        }
        put("while two piles are disconnected");
        put(TString(128_KB, 'b'));
        restarted = true;
        for (ui32 nodeId = 9; nodeId <= 24; ++nodeId) {
            env.StartNode(nodeId);
        }
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        const auto deadline = runtime->GetClock() + TDuration::Seconds(30);
        while (registeredNodes.size() != 16) {
            UNIT_ASSERT_C(runtime->GetClock() < deadline, "Timed out waiting for registration snapshots");
            env.Sim(TDuration::MilliSeconds(1));
        }
    }
}
