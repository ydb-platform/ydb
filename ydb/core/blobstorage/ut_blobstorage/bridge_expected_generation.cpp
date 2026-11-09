#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

#include <util/generic/scope.h>

#include <optional>

using NBridgeTest::GetBridgedLocation;

namespace {

using TInvokeResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
using TClusterState = NKikimrBridge::TClusterState;

// 3 piles, the state stays put: synchronization and automatic suspension completion are kept outstanding
struct TFixture {
    TEnvironmentSetup Env;

    TFixture()
        : Env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }}
    {
        Env.CreatePool();
        Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                return true;
            }
            const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
            return !record.HasNotifyBridgeSyncFinished() && !record.HasNotifyBridgeSuspended();
        };
    }

    TClusterState Query() {
        return NBridgeTest::QueryStorageConfig(Env).GetClusterState();
    }

    // state: the base for the overlay; stamp: NewClusterState.Generation
    TInvokeResult Suspend2(const TClusterState& state, ui64 stamp, std::optional<ui64> expected, ui32 nodeId = 1) {
        return NBridgeTest::InvokeDistconf(Env, [&](auto& record) {
            auto *cmd = record.MutableSwitchBridgeClusterState();
            auto *next = cmd->MutableNewClusterState();
            next->CopyFrom(state);
            next->SetPerPileState(2, TClusterState::SUSPENDED);
            next->SetGeneration(stamp);
            if (expected) {
                cmd->SetExpectedGeneration(*expected);
            }
        }, nodeId);
    }

    // a change made by someone else after the decision was taken at generation g0
    void Intervene(const TClusterState& state, ui64 g0) {
        const auto result = NBridgeTest::InvokeDistconf(Env, [&](auto& record) {
            auto *next = record.MutableSwitchBridgeClusterState()->MutableNewClusterState();
            next->CopyFrom(state);
            next->SetPerPileState(1, TClusterState::SUSPENDED);
            next->SetGeneration(g0 + 1);
        });
        UNIT_ASSERT_C(result.GetStatus() == TInvokeResult::OK, result.DebugString());
    }

    void CheckState(ui64 generation, TClusterState::EPileState pile2) {
        const auto state = Query();
        UNIT_ASSERT_VALUES_EQUAL_C(state.GetGeneration(), generation, state.ShortDebugString());
        UNIT_ASSERT_C(state.GetPerPileState(2) == pile2, state.ShortDebugString());
    }
};

void TestLostConcurrentReply(bool expectedGeneration) {
    TFixture f;
    auto& env = f.Env;
    auto& runtime = env.Runtime;
    const auto initial = NBridgeTest::InvokeDistconf(env, [](auto& record) {
        record.MutableQueryConfig();
    });
    UNIT_ASSERT_C(initial.GetStatus() == TInvokeResult::OK && initial.HasScepter(), initial.DebugString());
    const ui32 root = initial.GetScepter().GetNodeId();
    const ui32 nodeId = root == 1 ? 2 : 1; // a forwarding node in the surviving primary pile
    const auto state = initial.GetQueryConfig().GetConfig().GetClusterState();
    auto next = state;
    next.SetGeneration(state.GetGeneration() + 1);
    next.SetPerPileState(2, TClusterState::DISCONNECTED);

    const auto edge = runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
    auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
    auto *cmd = request->Record.MutableSwitchBridgeClusterState();
    cmd->MutableNewClusterState()->CopyFrom(next);
    if (expectedGeneration) {
        cmd->SetExpectedGeneration(state.GetGeneration());
    }

    std::unique_ptr<IEventHandle> delayed;
    TActorId forwarder;
    THashMap<TActorId, TActorId> sessions;
    ui32 hop = 0;
    bool replyLost = false;
    auto previousFilter = std::move(runtime->FilterFunction);
    Y_DEFER { runtime->FilterFunction = std::move(previousFilter); };
    runtime->FilterFunction = [&](ui32 destination, std::unique_ptr<IEventHandle>& ev) {
        if (!previousFilter(destination, ev)) {
            return false;
        }
        if (destination == nodeId && ev->GetTypeRewrite() == TEvInterconnect::TEvNodeConnected::EventType) {
            sessions[ev->Recipient] = ev->Sender;
        }
        if (!forwarder && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType
                && ev->Sender.NodeId() == nodeId && destination != nodeId
                && ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasSwitchBridgeClusterState()) {
            forwarder = ev->Sender;
            hop = destination;
            delayed = std::move(ev);
            return false;
        }
        if (ev->Recipient == forwarder) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& result = ev->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                UNIT_ASSERT_C(result.GetStatus() == (expectedGeneration
                    ? TInvokeResult::GENERATION_MISMATCH : TInvokeResult::ERROR), result.DebugString());
                replyLost = true;
                return false;
            }
        }
        return true;
    };
    runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge, request.release()), nodeId);
    NBridgeTest::WaitUntil(env, [&] { return delayed && sessions.contains(forwarder); }, "delayed forwarding request",
        TDuration::MilliSeconds(1), 30000);

    // The competing CAS commits exactly the same state while the first request is still in flight.
    const auto winner = NBridgeTest::InvokeDistconf(env, [&](auto& record) {
        auto *change = record.MutableSwitchBridgeClusterState();
        change->MutableNewClusterState()->CopyFrom(next);
        change->SetExpectedGeneration(state.GetGeneration());
    }, root, runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT_C(winner.GetStatus() == TInvokeResult::OK, winner.DebugString());
    const auto committed = f.Query();
    auto expected = next;
    expected.SetGuid(committed.GetGuid()); // distconf assigns a new GUID when proposing the state
    UNIT_ASSERT_VALUES_EQUAL(committed.SerializeAsString(), expected.SerializeAsString());

    // Wait until the forwarding node has received the winner's configuration, which enables the fallback.
    const auto localEdge = runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
    runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), localEdge,
        new TEvNodeWardenQueryStorageConfig(true)), nodeId);
    const auto deadline = runtime->GetClock() + TDuration::Seconds(30);
    for (;;) {
        auto local = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(localEdge, false, deadline);
        UNIT_ASSERT_C(local && local->Get()->Config, "Timed out waiting for the winner's local configuration");
        if (local->Get()->Config->GetClusterState().SerializeAsString() == committed.SerializeAsString()) {
            break;
        }
    }
    runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
        MakeBlobStorageNodeWardenID(nodeId), localEdge, nullptr, 0), nodeId);

    // Let the losing request reach the root, but drop its rejection before the forwarding actor sees it.
    runtime->Send(delayed.release(), hop);
    NBridgeTest::WaitUntil(env, [&] { return replyLost; }, "lost losing-request reply",
        TDuration::MilliSeconds(1), 30000);
    runtime->Send(new IEventHandle(forwarder, sessions.at(forwarder),
        new TEvInterconnect::TEvNodeDisconnected(hop)), nodeId);
    auto result = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false,
        runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT_C(result, "Timed out waiting for forwarding failure");
    const auto& record = result->Get()->Record;
    UNIT_ASSERT_C(record.GetStatus() == (expectedGeneration ? TInvokeResult::RACE : TInvokeResult::OK),
        record.DebugString());
    if (expectedGeneration) {
        UNIT_ASSERT_STRING_CONTAINS(record.GetErrorReason(), "Hop node disconnected");
    }
    UNIT_ASSERT_VALUES_EQUAL(f.Query().SerializeAsString(), committed.SerializeAsString());
}

}

Y_UNIT_TEST_SUITE(BridgeExpectedGeneration) {
    Y_UNIT_TEST(LostConcurrentCasReplyIsUnknown) {
        TestLostConcurrentReply(true);
    }

    Y_UNIT_TEST(LostConcurrentLegacyReplyKeepsSuccess) {
        TestLostConcurrentReply(false);
    }

    Y_UNIT_TEST(StaleRebasedRequestIsNotApplied) {
        TFixture f;
        const auto s0 = f.Query();
        const ui64 g0 = s0.GetGeneration();
        f.Intervene(s0, g0);
        f.CheckState(g0 + 1, TClusterState::SYNCHRONIZED); // a spurious automatic transition is a setup failure

        // The delayed request takes its snapshot after the intervening change: the overlay is built on the newer
        // state, while the decision named g0.
        for (const ui32 nodeId : {1, 9, 17}) {
            const auto current = f.Query();
            const auto result = f.Suspend2(current, current.GetGeneration() + 1, g0, nodeId);
            UNIT_ASSERT_C(result.GetStatus() == TInvokeResult::GENERATION_MISMATCH,
                "node# " << nodeId << " " << result.DebugString());
            f.CheckState(g0 + 1, TClusterState::SYNCHRONIZED);
        }
    }

    Y_UNIT_TEST(StaleStampedRequestReportsMismatch) {
        TFixture f;
        const auto s0 = f.Query();
        const ui64 g0 = s0.GetGeneration();
        f.Intervene(s0, g0);
        f.CheckState(g0 + 1, TClusterState::SYNCHRONIZED);

        // the snapshot of the decision time arriving late
        const auto result = f.Suspend2(s0, g0 + 1, g0);
        UNIT_ASSERT_C(result.GetStatus() == TInvokeResult::GENERATION_MISMATCH, result.DebugString());
        f.CheckState(g0 + 1, TClusterState::SYNCHRONIZED);
    }

    Y_UNIT_TEST(MatchingGenerationCommits) {
        TFixture f;
        const auto s0 = f.Query();
        const ui64 g0 = s0.GetGeneration();
        const auto result = f.Suspend2(s0, g0 + 1, g0);
        UNIT_ASSERT_C(result.GetStatus() == TInvokeResult::OK, result.DebugString());
        f.CheckState(g0 + 1, TClusterState::SUSPENDED);
    }

    Y_UNIT_TEST(InconsistentStampIsRejected) {
        TFixture f;
        const auto s0 = f.Query();
        const ui64 g0 = s0.GetGeneration();
        const auto result = f.Suspend2(s0, g0 + 2, g0);
        UNIT_ASSERT_C(result.GetStatus() == TInvokeResult::ERROR, result.DebugString());
        f.CheckState(g0, TClusterState::SYNCHRONIZED);
    }
}
