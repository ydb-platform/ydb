#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

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

}

Y_UNIT_TEST_SUITE(BridgeExpectedGeneration) {
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
