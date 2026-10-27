#include "bridge_common.h"

#include <ydb/core/base/bridge.h>

using NBridgeTest::GetBridgedLocation;

Y_UNIT_TEST_SUITE(BridgeStaticSyncerLifetime) {
    Y_UNIT_TEST(OldCommitReplyDoesNotCompleteRecreatedSyncer) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        env.Sim(TDuration::Seconds(5));
        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);

        const auto config = NBridgeTest::QueryStorageConfig(env);
        ui32 staticGroupId = Max<ui32>();
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                staticGroupId = group.GetGroupID();
                break;
            }
        }
        UNIT_ASSERT(staticGroupId != Max<ui32>());

        std::unique_ptr<IEventHandle> commit;
        bool captured = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                return true;
            }
            const auto& record = event->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
            if (record.HasNotifyBridgeSyncFinished()) {
                return false;
            }
            if (!captured && record.HasUpdateBridgeGroupInfo()) {
                const auto& cmd = record.GetUpdateBridgeGroupInfo();
                const auto *pile = NBridge::GetGroupPile(cmd.GetBridgeGroupInfo().GetBridgeGroupState(),
                    TBridgePileId::FromPileIndex(1));
                if (cmd.GetGroupId() == staticGroupId && pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                    commit = std::move(event);
                    captured = true;
                    return false;
                }
            }
            return true;
        };
        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        while (!commit && env.Runtime->GetClock() < deadline) {
            env.Sim(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(commit, "Static group synchronization did not reach its final commit");

        const auto result = NBridgeTest::InvokeDistconf(env, [&](auto& request) {
            request.CopyFrom(commit->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record);
        });
        UNIT_ASSERT_C(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.DebugString());
        env.Sim(TDuration::Seconds(1));

        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(1));

        auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
        response->Record.SetErrorReason("group generation changed during retry");
        env.Runtime->Send(new IEventHandle(commit->Sender, {}, response.release(), 0, commit->Cookie),
            commit->Sender.NodeId());
        env.Sim(TDuration::Seconds(1));

        env.FetchBaseConfig();
        env.Runtime->FilterFunction = nullptr;
    }
}
