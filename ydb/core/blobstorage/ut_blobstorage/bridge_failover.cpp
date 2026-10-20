#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

using namespace NBridgeTest;

namespace {

void TestTwoFailedPiles(bool primarySurvives, bool atomic) {
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
    const auto initialSender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    auto initialRequest = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
    initialRequest->Record.MutableQueryConfig();
    runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), initialSender, initialRequest.release()), 1);
    auto initial = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(initialSender, false,
        runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT(initial);
    UNIT_ASSERT_C(initial->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
        initial->Get()->Record.DebugString());
    const auto state = initial->Get()->Record.GetQueryConfig().GetConfig().GetClusterState();
    const ui32 oldPrimary = TBridgePileId::FromProto(&state,
        &NKikimrBridge::TClusterState::GetPrimaryPile).GetPileIndex();
    UNIT_ASSERT_VALUES_EQUAL(oldPrimary, 0);
    const ui32 survivor = primarySurvives ? oldPrimary : (oldPrimary + 1) % 3;
    const ui32 nodeId = survivor * 8 + 1;
    const auto sender = runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
    std::vector<std::pair<TLogoBlobID, TString>> blobs;
    auto put = [&](TString data) {
        const TLogoBlobID id(100500, 1, blobs.size() + 1, 0, data.size(), 0);
        runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data,
                runtime->GetClock() + TDuration::Seconds(30)));
        });
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false,
            runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "Timed out waiting for Put");
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
        blobs.emplace_back(id, std::move(data));
    };
    put("before failure");
    put(TString(128_KB, 'a'));

    // The test bootstrapper retries failed boots without a delay. Pause those
    // retries while distconf converges, then release them after failover.
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> pendingDeaths;
    runtime->FilterFunction = [&](ui32 destination, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvTablet::TEvTabletDead::EventType &&
                event->Get<TEvTablet::TEvTabletDead>()->TabletID == env.TabletId) {
            pendingDeaths.emplace_back(destination, std::move(event));
            return false;
        }
        return true;
    };

    // Lose both piles before issuing any state-changing command.
    for (ui32 id = 1; id <= 24; ++id) {
        if ((id - 1) / 8 != survivor) {
            env.StopNode(id);
        }
    }

    auto next = state;
    next.SetGeneration(state.GetGeneration() + 1);
    TBridgePileId::FromPileIndex(survivor).CopyToProto(&next, &NKikimrBridge::TClusterState::SetPrimaryPile);
    TBridgePileId::FromPileIndex(survivor).CopyToProto(&next, &NKikimrBridge::TClusterState::SetPromotedPile);
    // For the sequential control, disconnect the old primary first if it died.
    const ui32 firstFailed = primarySurvives ? (survivor + 1) % 3 : oldPrimary;
    for (ui32 i = 0; i != 3; ++i) {
        if (i != survivor && (atomic || i == firstFailed)) {
            next.SetPerPileState(i, NKikimrBridge::TClusterState::DISCONNECTED);
        }
    }
    // A surviving node may still be rebinding after losing its distconf parent.
    // Retry while its connected component is being collected, with a bounded deadline.
    NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult result;
    const auto failoverDeadline = runtime->GetClock() + TDuration::Minutes(2);
    for (;;) {
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(next);
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender, request.release()), nodeId);
        auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(sender, false,
            runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "Timed out waiting for emergency failover");
        result = response->Get()->Record;
        if ((!atomic && result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM) ||
                result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK ||
                (result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM &&
                 result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE) ||
                runtime->GetClock() >= failoverDeadline) {
            break;
        }
        env.Sim(TDuration::MilliSeconds(10));
    }
    if (!atomic) {
        UNIT_ASSERT_C(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM,
            result.DebugString());
        return;
    }
    UNIT_ASSERT_C(result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
        result.DebugString());

    runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), sender,
        new TEvNodeWardenQueryStorageConfig(true)), nodeId);
    const auto deadline = runtime->GetClock() + TDuration::Seconds(30);
    for (;;) {
        auto local = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(sender, false, deadline);
        UNIT_ASSERT_C(local && local->Get()->Config, "Timed out waiting for local failover configuration");
        const auto& current = local->Get()->Config->GetClusterState();
        if (current.GetGeneration() >= next.GetGeneration() &&
                current.GetPrimaryPile() == next.GetPrimaryPile() &&
                current.GetPromotedPile() == next.GetPromotedPile() &&
                current.GetPerPileState(0) == next.GetPerPileState(0) &&
                current.GetPerPileState(1) == next.GetPerPileState(1) &&
                current.GetPerPileState(2) == next.GetPerPileState(2)) {
            break;
        }
    }
    runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
        MakeBlobStorageNodeWardenID(nodeId), sender, nullptr, 0), nodeId);
    runtime->FilterFunction = {};
    for (auto& [destination, event] : pendingDeaths) {
        runtime->Send(event.release(), destination);
    }
    if (!primarySurvives) {
        // The fixture has a pinned test bootstrapper rather than Hive placement.
        // Restart the lost BSC on the new primary after the configuration is committed.
        runtime->CreateTestBootstrapper(TTestActorSystem::CreateTestTabletInfo(env.TabletId,
            TTabletTypes::BSController, env.Settings.Erasure.GetErasure(), env.GroupId, 3),
            &CreateFlatBsController, nodeId);
    }
    put("after failure");
    put(TString(128_KB, 'b'));
    for (const auto& [id, data] : blobs) {
        runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(id, 0, 0,
                runtime->GetClock() + TDuration::Seconds(30), NKikimrBlobStorage::FastRead));
        });
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false,
            runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "Timed out waiting for Get after failover");
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeFailover) {
    Y_UNIT_TEST(TwoFailedPilesPrimarySurvives) { TestTwoFailedPiles(true, true); }
    Y_UNIT_TEST(TwoFailedPilesPrimaryLost) { TestTwoFailedPiles(false, true); }
    Y_UNIT_TEST(SequentialPrimarySurvives) { TestTwoFailedPiles(true, false); }
    Y_UNIT_TEST(SequentialPrimaryLost) { TestTwoFailedPiles(false, false); }

    Y_UNIT_TEST(RejectsUnsafeMultiPileTransitions) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        auto& runtime = env.Runtime;
        const auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto invoke = [&](std::unique_ptr<NStorage::TEvNodeConfigInvokeOnRoot> request) {
            runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), sender, request.release()), 1);
            auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(sender, false,
                runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT_C(response, "Timed out waiting for configuration command");
            return response->Get()->Record;
        };
        auto query = [&] {
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableQueryConfig();
            auto response = invoke(std::move(request));
            UNIT_ASSERT_C(response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                response.DebugString());
            return response.GetQueryConfig().GetConfig().GetClusterState();
        };
        auto change = [&](NKikimrBridge::TClusterState next) {
            next.SetGeneration(next.GetGeneration() + 1);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(next);
            return invoke(std::move(request));
        };
        auto reject = [&](const NKikimrBridge::TClusterState& next, const TString& reason) {
            const auto before = query();
            auto response = change(next);
            UNIT_ASSERT_C(response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
                response.DebugString());
            UNIT_ASSERT_C(response.GetErrorReason().Contains(reason), response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(query().SerializeAsString(), before.SerializeAsString());
        };
        const auto initial = query();
        auto next = initial;
        next.SetPerPileState(1, NKikimrBridge::TClusterState::SUSPENDED);
        next.SetPerPileState(2, NKikimrBridge::TClusterState::SUSPENDED);
        reject(next, "Too many state changes");
        next.SetPerPileState(2, NKikimrBridge::TClusterState::DISCONNECTED);
        reject(next, "Too many state changes");
        next.SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        next.SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        reject(next, "Incorrect primary pile state");

        // Keep the rejoining pile unsynchronized while trying to promote it.
        runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& event) {
            return event->GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRoot::EventType ||
                !event->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBridgeSyncFinished();
        };
        next = initial;
        next.SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        auto response = change(next);
        UNIT_ASSERT_C(response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            response.DebugString());
        next = query();
        next.SetPerPileState(1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        response = change(next);
        UNIT_ASSERT_C(response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            response.DebugString());
        next = query();
        UNIT_ASSERT(next.GetPerPileState(1) != NKikimrBridge::TClusterState::SYNCHRONIZED);
        TBridgePileId::FromPileIndex(1).CopyToProto(&next, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(1).CopyToProto(&next, &NKikimrBridge::TClusterState::SetPromotedPile);
        next.SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        next.SetPerPileState(2, NKikimrBridge::TClusterState::DISCONNECTED);
        reject(next, "Incorrect primary pile state");
        // The public API represents PRIMARY as SYNCHRONIZED plus a primary id.
        // A request must not be able to forge the missing synchronization this way.
        next.SetPerPileState(1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        reject(next, "Can't switch to SYNCHRONIZED directly");
    }
}
