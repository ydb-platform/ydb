#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

using NBridgeTest::GetBridgedLocation;

Y_UNIT_TEST_SUITE(BridgeClusterState) {
    Y_UNIT_TEST(DisconnectReplacesSyncStateAfterAnotherPile) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const auto sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        auto invoke = [&](std::unique_ptr<NStorage::TEvNodeConfigInvokeOnRoot> request) {
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender, request.release()), 1);
            auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(sender, false);
            UNIT_ASSERT_C(response->Get()->Record.GetStatus() ==
                NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, response->Get()->Record.DebugString());
            return response->Get()->Record;
        };
        auto query = [&] {
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableQueryConfig();
            return invoke(std::move(request)).GetQueryConfig().GetConfig();
        };

        // Keep synchronization and automatic suspension completion outstanding.
        env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                return true;
            }
            const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
            return !record.HasNotifyBridgeSyncFinished() && !record.HasNotifyBridgeSuspended();
        };
        auto change = [&](ui32 pileIndex, NKikimrBridge::TClusterState::EPileState state) {
            auto config = query();
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            auto *next = request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState();
            next->CopyFrom(config.GetClusterState());
            next->SetPerPileState(pileIndex, state);
            next->SetGeneration(next->GetGeneration() + 1);
            invoke(std::move(request));
            return query();
        };

        change(1, NKikimrBridge::TClusterState::SUSPENDED);
        const auto before = change(2, NKikimrBridge::TClusterState::SUSPENDED);
        UNIT_ASSERT_VALUES_EQUAL(before.GetClusterStateDetails().PileSyncStateSize(), 2);
        const auto other = before.GetClusterStateDetails().GetPileSyncState(0);
        auto pileIndex = [](const NKikimrBridge::TClusterStateDetails::TPileSyncState& state) {
            return TBridgePileId::FromProto(&state,
                &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId).GetPileIndex();
        };
        UNIT_ASSERT_VALUES_EQUAL(pileIndex(other), 1);
        UNIT_ASSERT_VALUES_EQUAL(pileIndex(before.GetClusterStateDetails().GetPileSyncState(1)), 2);

        const auto after = change(2, NKikimrBridge::TClusterState::DISCONNECTED);
        UNIT_ASSERT_C(after.GetClusterState().GetPerPileState(2) == NKikimrBridge::TClusterState::DISCONNECTED,
            after.DebugString());
        const auto& states = after.GetClusterStateDetails().GetPileSyncState();
        UNIT_ASSERT_VALUES_EQUAL_C(states.size(), 2, after.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(states.Get(0).SerializeAsString(), other.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(pileIndex(states.Get(1)), 2);
        UNIT_ASSERT_VALUES_EQUAL(states.Get(1).GetBecameUnsyncedGeneration(), after.GetClusterState().GetGeneration());
        UNIT_ASSERT(states.Get(1).GetUnsyncedBSC());
    }
}
