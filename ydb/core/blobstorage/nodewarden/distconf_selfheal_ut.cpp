#include "distconf_selfheal.h"

#include <ydb/core/base/nodestate.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

namespace NKikimr::NStorage {
namespace {

void CheckReassignCancellation(ui32 phase) {
    TTestActorSystem runtime(1);
    runtime.Start();
    Y_DEFER { runtime.Stop(); };

    const TActorId edge = runtime.AllocateEdgeActor(1);
    runtime.RegisterService(MakeBlobStorageNodeWardenID(1), edge);
    const TActorId actor = runtime.Register(new TStateStorageReassignNodeSelfhealActor(
        edge, 123, TDuration::Minutes(1), 1, 2, true, true, true), 1);
    auto disable = runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(edge, false);
    UNIT_ASSERT(disable->Get()->Record.GetReassignStateStorageNode().GetDisableRing());

    auto acknowledge = [&] {
        auto reply = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        reply->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        runtime.Send(new IEventHandle(actor, edge, reply.release()), 1);
    };
    if (phase >= 1) {
        acknowledge();
    }
    if (phase >= 2) {
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvWakeup), 1);
        auto replace = runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(edge, false);
        UNIT_ASSERT(!replace->Get()->Record.GetReassignStateStorageNode().GetDisableRing());
    }
    if (phase >= 3) {
        acknowledge();
    }

    // UnbecomeRoot and a superseding selfheal request cancel this actor with
    // Poison. It must die quietly, including after applying the replacement
    // while its final timer is still pending.
    runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPoison), 1);
    runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPing,
        IEventHandle::FlagTrackDelivery), 1);
    auto undelivered = runtime.WaitForEdgeActorEvent<TEvents::TEvUndelivered>(edge);
    UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->SourceType, TEvents::TEvPing::EventType);
    UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
}

} // namespace

Y_UNIT_TEST_SUITE(TStateStorageSelfhealRecovery) {
    void AddGroup(NKikimrConfig::TStateStorageConfig& config, ui32 pileId, ui32 firstNode, ui32 rings) {
        auto *group = config.AddRingGroups();
        group->SetBridgePileId(pileId);
        group->SetNToSelect(rings);
        group->SetRingGroupActorIdOffset(firstNode);
        for (ui32 node : xrange(firstNode, firstNode + rings)) {
            group->AddRing()->AddNode(node);
        }
    }

    std::unordered_map<ui32, ui32> HealthyNodes(ui32 count) {
        std::unordered_map<ui32, ui32> states;
        for (ui32 node : xrange(1u, count + 1)) {
            states[node] = ENodeState::GOOD;
        }
        return states;
    }

    Y_UNIT_TEST(KeepReadableQuorumAcrossDifferentGeometries) {
        for (bool reverse : {false, true}) {
            NKikimrConfig::TStateStorageConfig config;
            AddGroup(config, 1, 1, 9);
            AddGroup(config, 1, 10, 3);
            auto states = HealthyNodes(12);
            for (ui32 node : {1u, 2u, 3u, 4u, 10u, 11u, 12u}) {
                states[node] = ENodeState::BAD;
            }
            if (reverse) {
                config.MutableRingGroups()->SwapElements(0, 1);
            }
            const auto selected = ChooseStateStorageSelfHealSource(config, 1, states);
            UNIT_ASSERT(selected);
            UNIT_ASSERT_VALUES_EQUAL(config.GetRingGroups(*selected).RingSize(), 9);
        }
    }

    Y_UNIT_TEST(CountUnhealthyRingsInsteadOfReplicas) {
        NKikimrConfig::TStateStorageConfig config;
        AddGroup(config, 1, 1, 3);
        AddGroup(config, 1, 4, 3);
        config.MutableRingGroups(0)->MutableRing(0)->AddNode(7);
        config.MutableRingGroups(0)->MutableRing(0)->AddNode(8);
        auto states = HealthyNodes(8);
        // Three failed replicas in one ring leave the first group a quorum;
        // two failed replicas in different rings leave the second without it.
        for (ui32 node : {1u, 7u, 8u, 4u, 5u}) {
            states[node] = ENodeState::BAD;
        }
        const auto selected = ChooseStateStorageSelfHealSource(config, 1, states);
        UNIT_ASSERT(selected);
        UNIT_ASSERT_VALUES_EQUAL(*selected, 0);
    }

    Y_UNIT_TEST(NodeShorthandPreservesReadableQuorum) {
        NKikimrConfig::TStateStorageConfig config;
        AddGroup(config, 1, 1, 3);
        AddGroup(config, 1, 4, 9);
        for (auto& group : *config.MutableRingGroups()) {
            for (const auto& ring : group.GetRing()) {
                group.AddNode(ring.GetNode(0));
            }
            group.ClearRing();
        }
        auto states = HealthyNodes(12);
        for (ui32 node : {1u, 2u, 3u, 4u, 5u, 6u, 7u}) {
            states[node] = ENodeState::BAD;
        }
        const auto selected = ChooseStateStorageSelfHealSource(config, 1, states);
        UNIT_ASSERT(selected);
        UNIT_ASSERT_VALUES_EQUAL(*selected, 1);
    }

    Y_UNIT_TEST(KeepPossibleQuorumWhenNoGroupHasGuaranteedQuorum) {
        NKikimrConfig::TStateStorageConfig config;
        AddGroup(config, 1, 1, 3);
        AddGroup(config, 1, 4, 9);
        config.MutableRingGroups(1)->SetNToSelect(5);
        auto states = HealthyNodes(12);
        for (ui32 node : {1u, 2u, 4u, 5u, 6u, 7u}) {
            states[node] = ENodeState::BAD;
        }
        const auto selected = ChooseStateStorageSelfHealSource(config, 1, states);
        UNIT_ASSERT(selected);
        UNIT_ASSERT_VALUES_EQUAL(*selected, 1);
    }

    Y_UNIT_TEST(DisabledRingsCannotProvideQuorum) {
        NKikimrConfig::TStateStorageConfig config;
        AddGroup(config, 1, 1, 3);
        AddGroup(config, 1, 4, 3);
        config.MutableRingGroups(0)->MutableRing(0)->SetIsDisabled(true);
        config.MutableRingGroups(0)->MutableRing(1)->SetIsDisabled(true);
        const auto selected = ChooseStateStorageSelfHealSource(config, 1, HealthyNodes(6));
        UNIT_ASSERT(selected);
        UNIT_ASSERT_VALUES_EQUAL(*selected, 1);
    }

    Y_UNIT_TEST(CompetingSourcesRequireKnownNodeStates) {
        NKikimrConfig::TStateStorageConfig config;
        AddGroup(config, 1, 1, 3);
        AddGroup(config, 1, 4, 3);
        UNIT_ASSERT(!ChooseStateStorageSelfHealSource(config, 1, {}));
        auto states = HealthyNodes(6);
        states.erase(6);
        UNIT_ASSERT(!ChooseStateStorageSelfHealSource(config, 1, states));
        states[6] = ENodeState::UNKNOWN;
        UNIT_ASSERT(!ChooseStateStorageSelfHealSource(config, 1, states));
    }

    Y_UNIT_TEST(WriteOnlyAndOtherPilesAreNotSources) {
        NKikimrConfig::TStateStorageConfig config;
        AddGroup(config, 2, 1, 3);
        AddGroup(config, 1, 4, 3);
        AddGroup(config, 1, 7, 3);
        config.MutableRingGroups(1)->SetWriteOnly(true);
        const auto selected = ChooseStateStorageSelfHealSource(config, 1, {});
        UNIT_ASSERT(selected);
        UNIT_ASSERT_VALUES_EQUAL(*selected, 2);
    }

    Y_UNIT_TEST(PrepareRetiresSurplusBeforeIntroducingTarget) {
        TTestActorSystem runtime(1);
        runtime.Start();
        Y_DEFER { runtime.Stop(); };
        const TActorId edge = runtime.AllocateEdgeActor(1);
        runtime.RegisterService(MakeBlobStorageNodeWardenID(1), edge);
        NKikimrBlobStorage::TStateStorageConfig current;
        AddGroup(*current.MutableStateStorageBoardConfig(), 1, 1, 9);
        AddGroup(*current.MutableStateStorageBoardConfig(), 1, 10, 3);
        current.MutableStateStorageBoardConfig()->MutableRingGroups(1)->SetWriteOnly(true);
        NKikimrBlobStorage::TStateStorageConfig target;
        AddGroup(*target.MutableStateStorageBoardConfig(), 1, 13, 3);
        const TActorId actor = runtime.Register(new TStateStorageSelfhealActor(edge, 123, TDuration::Minutes(1),
            std::move(current), std::move(target), true), 1);
        auto request = runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(edge, false);
        const auto& prepared = request->Get()->Record.GetReconfigStateStorage().GetStateStorageBoardConfig();
        UNIT_ASSERT_VALUES_EQUAL(prepared.RingGroupsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(prepared.GetRingGroups(0).GetRingGroupActorIdOffset(), 1);
        UNIT_ASSERT(!prepared.GetRingGroups(0).GetWriteOnly());
        UNIT_ASSERT_VALUES_EQUAL(prepared.GetRingGroups(1).GetRingGroupActorIdOffset(), 10);
        UNIT_ASSERT(prepared.GetRingGroups(1).GetWriteOnly());

        auto reply = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        reply->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        runtime.Send(new IEventHandle(actor, edge, reply.release()), 1);
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvWakeup), 1);
        request = runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(edge, false);
        const auto& introduced = request->Get()->Record.GetReconfigStateStorage().GetStateStorageBoardConfig();
        UNIT_ASSERT_VALUES_EQUAL(introduced.RingGroupsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(introduced.GetRingGroups(0).GetRingGroupActorIdOffset(), 1);
        UNIT_ASSERT(!introduced.GetRingGroups(0).GetWriteOnly());
        UNIT_ASSERT_VALUES_EQUAL(introduced.GetRingGroups(1).GetRingGroupActorIdOffset(), 13);
        UNIT_ASSERT(introduced.GetRingGroups(1).GetWriteOnly());
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPoison), 1);
    }
}

Y_UNIT_TEST_SUITE(TStateStorageReassignSelfhealCancellation) {
    Y_UNIT_TEST(BeforeDisableCompletes) {
        CheckReassignCancellation(0);
    }

    Y_UNIT_TEST(WhileWaitingToReplace) {
        CheckReassignCancellation(1);
    }

    Y_UNIT_TEST(BeforeReplaceCompletes) {
        CheckReassignCancellation(2);
    }

    Y_UNIT_TEST(AfterReplaceCompletes) {
        CheckReassignCancellation(3);
    }
}

Y_UNIT_TEST_SUITE(TDistconfStateStorageSelfhealTest) {
    struct TSetup {
        TTestActorSystem Runtime{1};
        TActorId Client;
        TActorId NodeWarden;
        TActorId Selfheal;

        TSetup() {
            Runtime.Start();
            Client = Runtime.AllocateEdgeActor(1);
            NodeWarden = Runtime.AllocateEdgeActor(1);
            Runtime.RegisterService(MakeBlobStorageNodeWardenID(1), NodeWarden);
            Selfheal = Runtime.Register(new TStateStorageReassignNodeSelfhealActor(
                Client, 42, TDuration::Seconds(1), 10, 11, true, false, true), 1);
        }

        ~TSetup() {
            Runtime.Stop();
        }

        void ExpectRequest(bool disableRing) {
            auto request = Runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(NodeWarden, false);
            UNIT_ASSERT_VALUES_EQUAL(request->Sender, Selfheal);
            UNIT_ASSERT(request->Get()->Record.HasReassignStateStorageNode());
            const auto& command = request->Get()->Record.GetReassignStateStorageNode();
            UNIT_ASSERT_VALUES_EQUAL(command.GetFrom(), 10);
            UNIT_ASSERT_VALUES_EQUAL(command.GetTo(), 11);
            UNIT_ASSERT(command.GetStateStorage());
            UNIT_ASSERT(!command.GetStateStorageBoard());
            UNIT_ASSERT(command.GetSchemeBoard());
            UNIT_ASSERT_VALUES_EQUAL(command.GetDisableRing(), disableRing);
        }

        void ReplyOK() {
            auto result = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            result->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
            Runtime.Send(new IEventHandle(Selfheal, NodeWarden, result.release()), 1);
            Runtime.Sim([&] { return Runtime.HasImmediateEvents(); });
        }
    };

    Y_UNIT_TEST(ReassignmentCompletesWithoutCancellation) {
        TSetup setup;
        setup.ExpectRequest(true);
        setup.ReplyOK();
        setup.ExpectRequest(false);
        setup.ReplyOK();
        const auto result = setup.Runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRootResult>(setup.Client);
        UNIT_ASSERT_C(result->Get()->Record.GetStatus() ==
            NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, result->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 42);
    }

    Y_UNIT_TEST(CancelledReassignmentDoesNotResume) {
        TSetup setup;
        setup.ExpectRequest(true);
        setup.ReplyOK();
        setup.Runtime.Send(new IEventHandle(setup.Selfheal, setup.Client, new TEvents::TEvPoison), 1);
        setup.Runtime.Schedule(TDuration::Seconds(3),
            new IEventHandle(setup.NodeWarden, setup.Client, new TEvents::TEvWakeup), nullptr, 1);
        setup.Runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(setup.NodeWarden);
        setup.Runtime.Send(new IEventHandle(setup.Client, {}, new TEvents::TEvWakeup), 1);
        setup.Runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(setup.Client);
    }
}

} // namespace NKikimr::NStorage
