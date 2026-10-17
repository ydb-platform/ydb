#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

namespace {

TNodeLocation GetRestartLocation(ui32 nodeId) {
    NActorsInterconnect::TNodeLocation location;
    location.SetBridgePileName(TStringBuilder() << "pile_" << (nodeId - 1) / 8 + 1);
    location.SetDataCenter("my_dc");
    location.SetRack(TStringBuilder() << "rack_" << nodeId);
    location.SetUnit(TStringBuilder() << "unit_" << nodeId);
    return TNodeLocation(location);
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeRestart) {
    Y_UNIT_TEST(RegistrationSnapshotPrecedesDiskUpdates) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetRestartLocation,
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
        auto invoke = [&](std::unique_ptr<NStorage::TEvNodeConfigInvokeOnRoot> request) {
            runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender, request.release()), 1);
            auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(sender, false,
                runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT_C(response, "Timed out waiting for a bridge configuration command");
            UNIT_ASSERT_C(response->Get()->Record.GetStatus() ==
                NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, response->Get()->Record.DebugString());
            return response->Get()->Record;
        };
        auto query = [&] {
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableQueryConfig();
            return invoke(std::move(request)).GetQueryConfig().GetConfig();
        };
        auto change = [&](ui32 index, NKikimrBridge::TClusterState::EPileState state) {
            auto config = query();
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            auto *next = request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState();
            next->CopyFrom(config.GetClusterState());
            next->SetPerPileState(index, state);
            next->SetGeneration(next->GetGeneration() + 1);
            invoke(std::move(request));
        };

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
        change(1, NKikimrBridge::TClusterState::DISCONNECTED);
        change(2, NKikimrBridge::TClusterState::DISCONNECTED);
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
        change(1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        change(2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        const auto deadline = runtime->GetClock() + TDuration::Seconds(30);
        while (registeredNodes.size() != 16) {
            UNIT_ASSERT_C(runtime->GetClock() < deadline, "Timed out waiting for registration snapshots");
            env.Sim(TDuration::MilliSeconds(1));
        }
    }
}
