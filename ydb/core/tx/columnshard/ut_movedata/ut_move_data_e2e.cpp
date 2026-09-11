#include "tablet_info_helper.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>

namespace NKikimr {

using namespace NTxUT;
using namespace NColumnShard;

namespace {

using NTestMoveData::MakeTabletInfo;
using EBackground = NYDBTest::ICSController::EBackground;

constexpr ui32 OldGroup = 2181038080;
constexpr ui32 NewGroup = 2181038081;
constexpr ui64 TabletId = TTestTxConfig::TxTablet0;
constexpr ui64 TableId = 1;

TActorId BootTablet(TTestBasicRuntime& runtime, const TIntrusivePtr<TTabletStorageInfo>& info, const TActorId& launcher = {}) {
    auto setupInfo = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
    const TActorId actorId = runtime.Register(CreateTablet(launcher, info.Get(), setupInfo.Get(), 0), 0);
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);
    // EvBoot only starts boot: the shard stays in StateInit until its normalizers finish.
    runtime.DispatchEvents({}, TDuration::Seconds(1));
    return actorId;
}

// Portion data lives on channels 2+; the executor's vacuum leg moves the log and local DB.
std::vector<TLogoBlobID> LivePortionBlobs(const NFake::TProxyDS& proxy, const ui64 tabletId) {
    std::vector<TLogoBlobID> result;
    for (const auto& [id, blob] : proxy.AllMyBlobs()) {
        if (id.TabletID() == tabletId && id.Channel() >= 2 && !blob.DoNotKeep) {
            result.push_back(id);
        }
    }
    return result;
}

// One shard whose portion data sits in OldGroup, driven through a MoveData session by manual wakeups.
class TMoveDataFixture {
public:
    TTestBasicRuntime Runtime;
    TIntrusivePtr<NFake::TProxyDS> OldGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
    TIntrusivePtr<NFake::TProxyDS> NewGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));
    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> Controller;
    TActorId Sender;

    explicit TMoveDataFixture(const bool moveDataEnabled = true)
        : Controller(SetupRuntime(moveDataEnabled))
    {
        // Without a real mediator the rewrite plan-step never ages, so set staleness to zero.
        Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        TabletActorId = BootTablet(Runtime, MakeTabletInfo(TabletId, { { 0, OldGroup } }));
        Sender = Runtime.AllocateEdgeActor();
        ReadStep = SetupSchema(Runtime, Sender, TableId, Table);
    }

    // The write id doubles as the tx id.
    void Write(const ui64 txId, const ui64 fromRow, const ui64 toRow) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(
            WriteData(Runtime, Sender, TabletId, txId, TableId, MakeTestBlob({ fromRow, toRow }, Table.Schema), Table.Schema, &writeIds));
        ReadStep = ProposeCommit(Runtime, Sender, TabletId, txId, writeIds);
        PlanCommit(Runtime, Sender, TabletId, ReadStep, TSet<ui64>{ txId });
    }

    // Reassign past everything written so far: those portions stay behind in OldGroup.
    size_t ReassignPastWrittenData() {
        const std::vector<TLogoBlobID> before = LivePortionBlobs(*OldGroupProxy, TabletId);
        UNIT_ASSERT_C(before.size(), "nothing was written into OldGroup - the test would pass vacuously");
        ui32 reassignedFrom = 0;
        for (const auto& id : before) {
            reassignedFrom = Max(reassignedFrom, id.Generation() + 1);
        }
        Runtime.Send(new IEventHandle(TabletActorId, TabletActorId, new TKikimrEvents::TEvPoisonPill));
        TabletActorId = BootTablet(Runtime, MakeTabletInfo(TabletId, { { 0, OldGroup }, { reassignedFrom, NewGroup } }));
        UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*NewGroupProxy, TabletId).size(), 0u, "no portion data may exist in the target group yet");
        return before.size();
    }

    void StartMove() {
        Runtime.SendToPipe(TabletId, Sender, new TEvTablet::TEvMoveData(std::vector<ui32>{ OldGroup }), 0, GetPipeConfigWithRetries());
    }

    // Each step is a wakeup, which reruns the background work and the MoveData gate.
    TEvTablet::TEvMoveDataResponse::TPtr DriveGate(
        const ui32 steps, const std::function<void(ui32)>& onStep = {}, const std::function<bool()>& stopWhen = {}) {
        TEvTablet::TEvMoveDataResponse::TPtr response;
        for (ui32 i = 0; i < steps && !response && !(stopWhen && stopWhen()); ++i) {
            Wakeup(Runtime, Sender, TabletId);
            Runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            if (onStep) {
                onStep(i);
            }
            response = Runtime.GrabEdgeEventIf<TEvTablet::TEvMoveDataResponse>(Sender, [](const TEvTablet::TEvMoveDataResponse::TPtr&) {
                return true;
            }, TDuration::MilliSeconds(100));
        }
        return response;
    }

    // Success promises the old group holds no portion data, not merely that the queues drained.
    void AssertDrainedSuccess(const TEvTablet::TEvMoveDataResponse::TPtr& response) const {
        UNIT_ASSERT_VALUES_EQUAL((int)response->Get()->Record.GetStatus(), (int)NKikimrTabletBase::TEvMoveDataResponse::Success);
        UNIT_ASSERT_VALUES_EQUAL_C(
            LivePortionBlobs(*OldGroupProxy, TabletId).size(), 0u, "answered Success with portion data still live in the old group");
    }

    // Reads at (ReadStep, 1), which skips a write committed in the latest plan step.
    ui64 ReadRows() {
        return ReadAllAsBatch(Runtime, TableId, NOlap::TSnapshot(ReadStep.Val(), 1), Table.Schema)->num_rows();
    }

private:
    TActorId TabletActorId;
    TestTableDescription Table;
    TPlanStep ReadStep;

    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> SetupRuntime(const bool moveDataEnabled) {
        Runtime.SetScheduledLimit(10'000);
        TTester::Setup(Runtime, { new NFake::TProxyDS(TGroupId::FromValue(0)), OldGroupProxy, NewGroupProxy,
                                    new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardGroupDecommission(moveDataEnabled);
        return NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    }
};

void RunMoveDataToCompletion(const bool ttlBackgroundDisabled, const bool moveDataEnabled = true) {
    TMoveDataFixture f(moveDataEnabled);
    if (ttlBackgroundDisabled) {
        f.Controller->DisableBackground(EBackground::TTL);
    }
    f.Write(1, 0, 1000);
    f.Controller->WaitCompactions(TDuration::Seconds(10));
    const size_t oldBlobs = f.ReassignPastWrittenData();

    f.StartMove();
    // At step 25 commit an extra write to advance minSnapshotForNewReads.
    const auto response = f.DriveGate(150, [&](const ui32 i) {
        if (i == 25) {
            f.Write(2, 1000, 1001);
        }
    });
    UNIT_ASSERT_C(response, "no TEvMoveDataResponse: the move never drained OldGroup");
    UNIT_ASSERT_VALUES_EQUAL((int)response->Get()->Record.GetStatus(), (int)NKikimrTabletBase::TEvMoveDataResponse::Success);

    // Success must mean rewritten, not merely empty queues: only the move puts data in NewGroup.
    const size_t movedBlobs = LivePortionBlobs(*f.NewGroupProxy, TabletId).size();
    if (moveDataEnabled) {
        UNIT_ASSERT_C(movedBlobs, "answered Success without rewriting any of the " << oldBlobs << " portion blobs out of the old group");
        f.AssertDrainedSuccess(response);
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(movedBlobs, 0u, "the disabled feature flag still rewrote portions");
    }
    UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1000);
}

}   // namespace

// Whole chain: TEvMoveData -> selection -> accessor metadata -> rewrite -> response.
Y_UNIT_TEST_SUITE(TColumnShardMoveDataE2E) {
    Y_UNIT_TEST(MoveDataRewritesPortionsAndAnswersHive) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/false);
    }

    // Rewrites come from the loop TTL uses: TTL off must not stop the move.
    Y_UNIT_TEST(MoveDataCompletesWithTtlDisabled) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/true);
    }

    // Flag off: TEvMoveData goes straight to the executor, which leaves the portions alone.
    Y_UNIT_TEST(MoveDataDisabledLeavesPortionsInPlace) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/false, /*moveDataEnabled=*/false);
    }

    // A running cleanup has taken its portions out of CleanupPortions but not yet queued their blobs for GC.
    Y_UNIT_TEST(SuccessWaitsForTheRunningCleanup) {
        TMoveDataFixture f;
        // With TTL off, every TTL change is a MoveData rewrite.
        f.Controller->DisableBackground(EBackground::TTL);
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        THashSet<ui64> rewritten;
        std::vector<TAutoPtr<IEventHandle>> heldCleanups;
        bool holdCleanups = true;
        // Private event ids repeat across components, so the type id alone does not identify TEvWriteIndex.
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            const auto* writeIndex = ev->GetTypeRewrite() == TEvPrivate::TEvWriteIndex::EventType && ev->HasEvent()
                                         ? dynamic_cast<const TEvPrivate::TEvWriteIndex*>(ev->GetBase())
                                         : nullptr;
            if (!writeIndex) {
                return;
            }
            const auto& changes = writeIndex->IndexChanges;
            if (const auto rewrite = std::dynamic_pointer_cast<NOlap::TTTLColumnEngineChanges>(changes)) {
                const THashSet<ui64> ids = rewrite->GetPortionsToRemove().GetPortionIds();
                rewritten.insert(ids.begin(), ids.end());
                return;
            }
            const auto cleanup = std::dynamic_pointer_cast<NOlap::TCleanupPortionsColumnEngineChanges>(changes);
            if (holdCleanups && cleanup && AnyOf(cleanup->GetPortionsToDrop(), [&](const NOlap::TPortionInfo::TConstPtr& portion) {
                    return rewritten.contains(portion->GetPortionId());
                })) {
                heldCleanups.emplace_back(ev.Release());
            }
        });

        f.StartMove();
        auto response = f.DriveGate(
            150,
            [&](const ui32 i) {
                if (i == 25) {
                    f.Write(2, 1000, 1001);
                }
            },
            [&] {
                return !heldCleanups.empty();
            });
        UNIT_ASSERT_C(!response, "answered before any cleanup took the rewritten portions");
        UNIT_ASSERT_C(!heldCleanups.empty(), "no cleanup took the rewritten portions");
        // Over two gate cadences with that cleanup still running.
        UNIT_ASSERT_C(!f.DriveGate(120), "answered Success while the cleanup of the rewritten portions was still running");

        holdCleanups = false;
        for (auto& ev : heldCleanups) {
            f.Runtime.Send(ev.Release());
        }
        response = f.DriveGate(150);
        UNIT_ASSERT_C(response, "no TEvMoveDataResponse after the cleanup finished");
        f.AssertDrainedSuccess(response);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1000);
    }

    // Compaction, not MoveData, retires the target portions; their cleanup must still hold the answer back.
    Y_UNIT_TEST(SuccessWaitsForPortionsCompactionRetired) {
        TMoveDataFixture f;
        f.Controller->DisableBackground(EBackground::TTL);
        f.Controller->DisableBackground(EBackground::Compaction);
        // Two overlapping writes leave two portions for compaction to merge.
        f.Write(1, 0, 1000);
        f.Write(2, 0, 1000);
        f.ReassignPastWrittenData();

        f.Controller->DisableBackground(EBackground::MoveData);
        f.Controller->DisableBackground(EBackground::Cleanup);
        f.StartMove();
        UNIT_ASSERT_C(!f.DriveGate(60), "answered Success while the target portions were still live");

        f.Controller->EnableBackground(EBackground::Compaction);
        UNIT_ASSERT_C(!f.DriveGate(150, {}, [&] {
            return !LivePortionBlobs(*f.NewGroupProxy, TabletId).empty();
        }), "answered Success before compaction retired the target portions");
        UNIT_ASSERT_C(!LivePortionBlobs(*f.NewGroupProxy, TabletId).empty(), "compaction never rewrote the target portions");
        UNIT_ASSERT_C(!f.DriveGate(120), "answered Success while the portions compaction retired still awaited cleanup");

        f.Controller->EnableBackground(EBackground::Cleanup);
        // Advance minSnapshotForNewReads past the compaction, so cleanup can take the retired portions.
        f.Write(3, 1000, 1001);
        const auto response = f.DriveGate(150);
        UNIT_ASSERT_C(response, "no TEvMoveDataResponse after cleanup was re-enabled");
        f.AssertDrainedSuccess(response);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1000);
    }
}

}   // namespace NKikimr
