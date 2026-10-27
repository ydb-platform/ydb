#include "bridge_config_yaml.h"

using namespace NBridgeTest;

namespace {

using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
using TReport = TResult::TPileConnectivity;
using TState = NKikimrBridge::TClusterState;

TEnvironmentSetup::TSettings MakeSettings() {
    return {
        .NodeCount = 16,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .LocationGenerator = GetBridgedLocation,
        .SelfManagementConfig = true,
        .NumPiles = 2,
        .AutomaticBootstrap = true,
    };
}

const TReport::TPile& FindPile(const TReport& report, ui32 pileIndex) {
    for (const auto& pile : report.GetPiles()) {
        if (TBridgePileId::FromProto(&pile, &TReport::TPile::GetBridgePileId).GetPileIndex() == pileIndex) {
            return pile;
        }
    }
    UNIT_FAIL("Pile missing from connectivity report: " << report.DebugString());
    Y_UNREACHABLE();
}

// Wait for the complete expected component, not merely its surviving pile's quorum:
// a quorum alone does not prove that the dead pile has been removed from the tree.
TReport WaitForConnectivity(TEnvironmentSetup& env, ui32 nodeId, ui32 firstPileNodes, ui32 secondPileNodes,
        std::optional<bool> hasScepter = {})
{
    const auto deadline = env.Runtime->GetClock() + TDuration::Minutes(2);
    TResult response;
    for (;;) {
        UNIT_ASSERT_C(env.Runtime->GetClock() < deadline, "Connectivity did not converge: " << response.DebugString());
        response = InvokeDistconf(env, [](auto& request) {
            request.MutableQueryPileConnectivity();
        }, nodeId, Min(deadline, env.Runtime->GetClock() + TDuration::Seconds(5)));
        if (response.GetStatus() == TResult::OK) {
            UNIT_ASSERT_C(response.HasPileConnectivity(), response.DebugString());
            const auto& report = response.GetPileConnectivity();
            UNIT_ASSERT_VALUES_EQUAL(report.PilesSize(), 2);
            if (FindPile(report, 0).GetNumConnectedStaticNodes() == firstPileNodes
                    && FindPile(report, 1).GetNumConnectedStaticNodes() == secondPileNodes
                    && (!hasScepter || report.GetHasScepter() == *hasScepter)) {
                return report;
            }
        } else {
            UNIT_ASSERT_C(response.GetStatus() == TResult::RACE, response.DebugString());
        }
        UNIT_ASSERT_C(env.Runtime->GetClock() < deadline, response.DebugString());
        env.Sim(Min(TDuration::Seconds(1), deadline - env.Runtime->GetClock()));
    }
}

void CheckPile(const TReport& report, ui32 index, ui32 connected, bool quorum, bool configQuorum, bool dataQuorum) {
    const auto& pile = FindPile(report, index);
    UNIT_ASSERT_VALUES_EQUAL(pile.GetNumStaticNodes(), 8);
    UNIT_ASSERT_VALUES_EQUAL(pile.GetNumConnectedStaticNodes(), connected);
    UNIT_ASSERT_VALUES_EQUAL(pile.GetHasNodeQuorum(), quorum);
    UNIT_ASSERT_VALUES_EQUAL(pile.GetRequiresConfigQuorum(), configQuorum);
    UNIT_ASSERT_VALUES_EQUAL(pile.GetRequiresDataQuorum(), dataQuorum);
}

} // namespace

Y_UNIT_TEST_SUITE(BridgePileConnectivity) {
    Y_UNIT_TEST(HealthyClusterReportsAllPiles) {
        TEnvironmentSetup env{MakeSettings()};
        // QueryConfig waits for bootstrap before exercising the immediate read path.
        const auto config = QueryStorageConfig(env);
        auto report = WaitForConnectivity(env, 9, 8, 8);
        UNIT_ASSERT(report.GetHasScepter());
        UNIT_ASSERT(!report.GetBindingInProgress());
        UNIT_ASSERT_VALUES_EQUAL(report.GetClusterStateGeneration(), config.GetClusterState().GetGeneration());
        CheckPile(report, 0, 8, true, true, true);
        CheckPile(report, 1, 8, true, true, true);
        // Force a request from a non-root node to exercise the existing hop path.
        const ui32 nonRoot = report.GetReporterNodeId() == 9 ? 1 : 9;
        const auto forwarded = WaitForConnectivity(env, nonRoot, 8, 8);
        UNIT_ASSERT_VALUES_EQUAL(forwarded.GetReporterNodeId(), report.GetReporterNodeId());
    }

    Y_UNIT_TEST(DeadPrimaryIsReportedBySurvivors) {
        TEnvironmentSetup env{MakeSettings()};
        const auto config = QueryStorageConfig(env);
        WaitForConnectivity(env, 9, 8, 8);
        // Prevent the fixture's immediate BSC boot retries from spinning simulated time.
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            return event->GetTypeRewrite() != TEvTablet::TEvTabletDead::EventType
                || event->Get<TEvTablet::TEvTabletDead>()->TabletID != env.TabletId;
        };
        for (ui32 id = 1; id <= 8; ++id) {
            env.StopNode(id);
        }
        const auto report = WaitForConnectivity(env, 9, 0, 8);
        UNIT_ASSERT(!report.GetHasScepter());
        CheckPile(report, 0, 0, false, true, true);
        CheckPile(report, 1, 8, true, true, true);
        const auto edge = env.Runtime->AllocateEdgeActor(9, __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(9), edge,
            new TEvNodeWardenQueryStorageConfig(false)), 9);
        const auto local = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(local && local->Get()->Config);
        UNIT_ASSERT_VALUES_EQUAL(local->Get()->Config->GetClusterState().GetPrimaryPile(),
            config.GetClusterState().GetPrimaryPile());
        UNIT_ASSERT_VALUES_EQUAL(report.GetClusterStateGeneration(), config.GetClusterState().GetGeneration());
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(FailedStage2PileIsReported) {
        TEnvironmentSetup env{MakeSettings()};
        QueryStorageConfig(env);
        env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& event) {
            return event->GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRoot::EventType
                || !event->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBridgeSyncFinished();
        };
        SwitchBridgePileState(env, 1, TState::DISCONNECTED);
        SwitchBridgePileState(env, 1, TState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, TState::NOT_SYNCHRONIZED_2);
        const auto before = WaitForConnectivity(env, 1, 8, 8);
        CheckPile(before, 1, 8, true, true, true);
        for (ui32 id = 9; id <= 16; ++id) {
            env.StopNode(id);
        }
        const auto report = WaitForConnectivity(env, 1, 8, 0);
        CheckPile(report, 0, 8, true, true, true);
        CheckPile(report, 1, 0, false, true, true);
        UNIT_ASSERT(!report.GetHasScepter());
        UNIT_ASSERT_VALUES_EQUAL(report.GetClusterStateGeneration(), before.GetClusterStateGeneration());
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(Stage1PileIsDistinguishable) {
        TEnvironmentSetup env{MakeSettings()};
        QueryStorageConfig(env);
        SwitchBridgePileState(env, 1, TState::DISCONNECTED);
        // The command can reply before its configuration reaches every survivor. The old root may be
        // in the pile being stopped, so wait for the local snapshots rather than a forwarded root query.
        for (ui32 id = 1; id <= 8; ++id) {
            WaitUntil(env, [&] {
                const auto edge = env.Runtime->AllocateEdgeActor(id, __FILE__, __LINE__);
                env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(id), edge,
                    new TEvNodeWardenQueryStorageConfig(false)), id);
                const auto local = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(edge, true,
                    env.Runtime->GetClock() + TDuration::Seconds(5));
                UNIT_ASSERT(local && local->Get()->Config);
                return local->Get()->Config->GetClusterState().GetPerPileState(1) == TState::DISCONNECTED;
            }, "disconnection applied on survivor", TDuration::MilliSeconds(1), 30000);
        }
        for (ui32 id = 9; id <= 16; ++id) {
            env.StopNode(id);
        }
        WaitForConnectivity(env, 1, 8, 0, true);
        SwitchBridgePileState(env, 1, TState::NOT_SYNCHRONIZED_1);
        const auto report = WaitForConnectivity(env, 1, 8, 0, true);
        CheckPile(report, 0, 8, true, true, true);
        CheckPile(report, 1, 0, false, false, false);
        UNIT_ASSERT(report.GetHasScepter());
    }

    Y_UNIT_TEST(AnswersWhileBindingIsInProgress) {
        TEnvironmentSetup env{MakeSettings()};
        QueryStorageConfig(env);
        const auto before = WaitForConnectivity(env, 9, 8, 8);
        const ui32 nodeId = before.GetReporterNodeId() == 9 ? 10 : 9;
        bool bindingResponseHeld = false;
        env.Runtime->FilterFunction = [&](ui32 destination, std::unique_ptr<IEventHandle>& event) {
            if (destination == nodeId && event->GetTypeRewrite() == NStorage::TEvNodeConfigReversePush::EventType) {
                bindingResponseHeld = true;
                return false;
            }
            return true;
        };
        env.RestartNode(nodeId);
        WaitUntil(env, [&] { return bindingResponseHeld; }, "binding response held", TDuration::MilliSeconds(1), 30000);
        const auto response = InvokeDistconf(env, [](auto& request) {
            request.MutableQueryPileConnectivity();
        }, nodeId, env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(response.GetStatus() == TResult::OK, response.DebugString());
        UNIT_ASSERT(response.HasPileConnectivity());
        const auto& report = response.GetPileConnectivity();
        UNIT_ASSERT_VALUES_EQUAL(report.GetReporterNodeId(), nodeId);
        UNIT_ASSERT(report.GetBindingInProgress());
        UNIT_ASSERT(!report.GetHasScepter());
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(AnswersWhilePropositionIsStalled) {
        TEnvironmentSetup env{MakeSettings()};
        auto config = QueryStorageConfig(env);
        const auto before = WaitForConnectivity(env, 9, 8, 8);
        bool propositionHeld = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType
                    && event->Get<NStorage::TEvNodeConfigScatter>()->Record.HasProposeStorageConfig()) {
                propositionHeld = true;
                return false;
            }
            return true;
        };
        const ui32 root = before.GetReporterNodeId();
        const auto edge = env.Runtime->AllocateEdgeActor(root, __FILE__, __LINE__);
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto *next = request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState();
        next->CopyFrom(config.GetClusterState());
        next->SetGeneration(next->GetGeneration() + 1);
        next->SetPerPileState(1, TState::DISCONNECTED);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(root), edge, request.release()), root);
        WaitUntil(env, [&] { return propositionHeld; }, "configuration proposition held", TDuration::MilliSeconds(1), 30000);
        const auto response = InvokeDistconf(env, [](auto& query) {
            query.MutableQueryPileConnectivity();
        }, root, env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(response.GetStatus() == TResult::OK, response.DebugString());
        UNIT_ASSERT(response.HasPileConnectivity());
        UNIT_ASSERT_VALUES_EQUAL(response.GetPileConnectivity().GetReporterNodeId(), root);
        const auto pending = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false,
            env.Runtime->GetClock() + TDuration::MilliSeconds(1));
        UNIT_ASSERT_C(!pending, "Proposition should still be waiting for the held scatter");
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(SuspendedPileRequiresOnlyConfigQuorum) {
        TEnvironmentSetup env{MakeSettings()};
        QueryStorageConfig(env);
        env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& event) {
            return event->GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRoot::EventType
                || !event->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBridgeSuspended();
        };
        SwitchBridgePileState(env, 1, TState::SUSPENDED);
        const auto report = WaitForConnectivity(env, 1, 8, 8);
        CheckPile(report, 1, 8, true, true, false);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(InactiveAndFreeSlotsAreReportedCorrectly) {
        TEnvironmentSetup env{MakeExpandableSettings()};
        QueryStorageConfig(env);
        // Registry changes require capability reports from every configured static node, not just a quorum.
        WaitForConnectivity(env, 1, 8, 8, true);
        auto added = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        UNIT_ASSERT_C(added.GetStatus() == TResult::OK, added.DebugString());
        auto query = [&] {
            const auto response = InvokeDistconf(env, [](auto& request) {
                request.MutableQueryPileConnectivity();
            }, 1, env.Runtime->GetClock() + TDuration::Seconds(5));
            UNIT_ASSERT_C(response.GetStatus() == TResult::OK, response.DebugString());
            UNIT_ASSERT(response.HasPileConnectivity());
            return response.GetPileConnectivity();
        };
        auto report = query();
        UNIT_ASSERT_VALUES_EQUAL(report.PilesSize(), 3);
        CheckPile(report, 2, 0, false, false, false);
        UNIT_ASSERT_VALUES_EQUAL(FindPile(report, 2).GetPileName(), "pile_3");
        const auto removed = DropPileFromConfig(env, "pile_3");
        UNIT_ASSERT_C(removed.GetStatus() == TResult::OK, removed.DebugString());
        report = query();
        UNIT_ASSERT_VALUES_EQUAL(report.PilesSize(), 2);
        CheckPile(report, 0, 8, true, true, true);
        CheckPile(report, 1, 8, true, true, true);
    }

}
