#include "tablet_info_helper.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

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

// TCSCounters registers under module_id=CS, and GetDeriviative prefixes the name with "Deriviative/".
i64 GateBlockedByFirstGCRound(TTestBasicRuntime& runtime) {
    const auto subgroup =
        GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id", "CS");
    return subgroup->GetCounter("Deriviative/MoveData/GateBlocked/FirstGCRound/Count", true)->Val();
}

// Private event ids repeat across components, so the type id alone does not identify TEvWriteIndex.
const TEvPrivate::TEvWriteIndex* AsWriteIndex(IEventHandle::TPtr& ev) {
    if (ev->GetTypeRewrite() != TEvPrivate::TEvWriteIndex::EventType || !ev->HasEvent()) {
        return nullptr;
    }
    return dynamic_cast<const TEvPrivate::TEvWriteIndex*>(ev->GetBase());
}

// One shard whose portion data sits in OldGroup, driven through a MoveData session by manual wakeups.
class TMoveDataFixture {
public:
    TTestBasicRuntime Runtime;
    TIntrusivePtr<NFake::TProxyDS> OldGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
    TIntrusivePtr<NFake::TProxyDS> NewGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));
    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> Controller;
    TActorId Sender;
    // The cutter sends TEvCutTabletHistory here, so it must be a real actor.
    TActorId Launcher;

    explicit TMoveDataFixture(const bool moveDataEnabled = true)
        : Controller(SetupRuntime(moveDataEnabled))
    {
        // Without a real mediator the rewrite plan-step never ages, so set staleness to zero.
        Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        Launcher = Runtime.AllocateEdgeActor();
        TabletActorId = BootTablet(Runtime, MakeTabletInfo(TabletId, { { 0, OldGroup } }), Launcher);
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

    // A write whose commit never arrives, as when the tablet restarts before its result reaches the client.
    std::vector<ui64> WriteUncommitted(const ui64 writeId, const ui64 fromRow, const ui64 toRow, const ui64 lockId) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(Runtime, Sender, TabletId, writeId, TableId, MakeTestBlob({ fromRow, toRow }, Table.Schema), Table.Schema,
            &writeIds, NEvWrite::EModificationType::Upsert, lockId));
        return writeIds;
    }

    void CommitLock(const ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId) {
        ReadStep = ProposeCommit(Runtime, Sender, TabletId, txId, writeIds, lockId);
        PlanCommit(Runtime, Sender, TabletId, ReadStep, TSet<ui64>{ txId });
    }

    // What the lock service answers once the lock's owner is gone: the shard aborts the writes under it.
    void ReportLockGone(const ui64 lockId) {
        Runtime.SendToPipe(TabletId, Sender, new NLongTxService::TEvLongTxService::TEvLockStatus(lockId, /*lockNode=*/1,
                                                 NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND), 0, GetPipeConfigWithRetries());
    }

    // Reassign past everything written so far: those portions stay behind in OldGroup.
    size_t ReassignPastWrittenData() {
        const std::vector<TLogoBlobID> before = LivePortionBlobs(*OldGroupProxy, TabletId);
        UNIT_ASSERT_C(before.size(), "nothing was written into OldGroup - the test would pass vacuously");
        for (const auto& id : before) {
            ReassignedFrom = Max(ReassignedFrom, id.Generation() + 1);
        }
        Restart();
        UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*NewGroupProxy, TabletId).size(), 0u, "no portion data may exist in the target group yet");
        return before.size();
    }

    // Boots the next generation with the current channel history, as Hive does after MoveData.
    void Restart() {
        Runtime.Send(new IEventHandle(TabletActorId, TabletActorId, new TKikimrEvents::TEvPoisonPill));
        TabletActorId = BootTablet(Runtime, MakeTabletInfo(TabletId, { { 0, OldGroup }, { ReassignedFrom, NewGroup } }), Launcher);
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
    // First generation in NewGroup, once ReassignPastWrittenData has run.
    ui32 ReassignedFrom = 0;

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

    // An uncommitted write cannot be rewritten, yet its blobs sit in the old group until it commits and moves.
    Y_UNIT_TEST(SuccessWaitsForUncommittedWriteToCommit) {
        TMoveDataFixture f;
        f.Write(1, 0, 1000);
        const auto writeIds = f.WriteUncommitted(100, 5000, 5010, 7);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        f.StartMove();
        UNIT_ASSERT_C(!f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(2, 1000, 1001);
            }
        }), "answered Success while an uncommitted write held blobs in the old group");

        f.CommitLock(3, writeIds, 7);
        const auto response = f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(4, 1001, 1002);
            }
        });
        UNIT_ASSERT_C(response, "no TEvMoveDataResponse after the write committed");
        f.AssertDrainedSuccess(response);
        // Write 4 lands in the plan step ReadRows skips.
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1011);
    }

    // An aborted write leaves its blobs in the old group until cleanup deletes them.
    Y_UNIT_TEST(SuccessWaitsForAbortedWriteToBeCleanedUp) {
        TMoveDataFixture f;
        f.Write(1, 0, 1000);
        f.WriteUncommitted(100, 5000, 5010, 7);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        f.StartMove();
        UNIT_ASSERT_C(!f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(2, 1000, 1001);
            }
        }), "answered Success while an uncommitted write held blobs in the old group");

        f.ReportLockGone(7);
        const auto response = f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(3, 1001, 1002);
            }
        });
        UNIT_ASSERT_C(response, "no TEvMoveDataResponse after the write aborted");
        f.AssertDrainedSuccess(response);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1001);
    }

    // An uncommitted write whose blobs are already in the new group must not hold the move back.
    Y_UNIT_TEST(UncommittedWriteInTheNewGroupDoesNotHoldSuccess) {
        TMoveDataFixture f;
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();
        f.WriteUncommitted(100, 5000, 5010, 7);

        f.StartMove();
        const auto response = f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(2, 1000, 1001);
            }
        });
        UNIT_ASSERT_C(response, "an uncommitted write outside the moved group held the answer back");
        f.AssertDrainedSuccess(response);
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
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            const auto* writeIndex = AsWriteIndex(ev);
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

    // Regression: cleanup that has DoStart-ed (portions already out of CleanupPortions) before the first empty-queue gate check must still block the gate.
    Y_UNIT_TEST(GateStaysClosedWhenCleanupStartedBeforeWatermarkFreezes) {
        TMoveDataFixture f;
        f.Controller->DisableBackground(EBackground::TTL);
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        THashSet<ui64> rewritten;
        std::vector<TAutoPtr<IEventHandle>> heldCleanups;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            const auto* writeIndex = AsWriteIndex(ev);
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
            if (!rewritten.empty() && cleanup && AnyOf(cleanup->GetPortionsToDrop(), [&](const NOlap::TPortionInfo::TConstPtr& p) {
                    return rewritten.contains(p->GetPortionId());
                })) {
                heldCleanups.emplace_back(ev.Release());
            }
        });

        f.StartMove();
        f.DriveGate(
            150,
            [&](const ui32 i) {
                if (i == 25) {
                    f.Write(2, 1000, 1001);
                }
            },
            [&] {
                return !heldCleanups.empty();
            });
        UNIT_ASSERT_C(!heldCleanups.empty(), "cleanup of rewritten portions never started");
        // Cleanup has DoStart-ed (CleanupPortions emptied) but not yet committed; the gate must stay closed when the watermark is first frozen in this window.
        UNIT_ASSERT_C(!f.DriveGate(120), "gate opened while the in-flight cleanup still held target blobs");

        for (auto& ev : heldCleanups) {
            f.Runtime.Send(ev.Release());
        }
        const auto response = f.DriveGate(150);
        UNIT_ASSERT_C(response, "no Success after cleanup committed");
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

    // Cleanup of portions retired after the queues drained holds no target data, so it must not hold the answer back.
    Y_UNIT_TEST(SuccessIgnoresCleanupOfLaterRetiredPortions) {
        TMoveDataFixture f;
        f.Controller->DisableBackground(EBackground::TTL);
        // No portion written after the move starts is adopted, so the watermark stays frozen once the queues drain.
        f.Controller->SetOverrideMoveDataAdmissionWindow(TDuration::Zero());
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        THashSet<ui64> rewritten;
        THashSet<ui64> laterRetired;
        bool watermarkFrozen = false;
        bool targetCleanupSeen = false;
        std::vector<TAutoPtr<IEventHandle>> heldCleanups;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            const auto* writeIndex = AsWriteIndex(ev);
            if (!writeIndex) {
                return;
            }
            const auto& changes = writeIndex->IndexChanges;
            if (const auto rewrite = std::dynamic_pointer_cast<NOlap::TTTLColumnEngineChanges>(changes)) {
                const THashSet<ui64> ids = rewrite->GetPortionsToRemove().GetPortionIds();
                rewritten.insert(ids.begin(), ids.end());
            } else if (const auto cleanup = std::dynamic_pointer_cast<NOlap::TCleanupPortionsColumnEngineChanges>(changes)) {
                const bool target = AnyOf(cleanup->GetPortionsToDrop(), [&](const NOlap::TPortionInfo::TConstPtr& portion) {
                    return rewritten.contains(portion->GetPortionId());
                });
                targetCleanupSeen |= target;
                const bool onlyLater = !cleanup->GetPortionsToDrop().empty() &&
                                       AllOf(cleanup->GetPortionsToDrop(), [&](const NOlap::TPortionInfo::TConstPtr& portion) {
                                           return laterRetired.contains(portion->GetPortionId());
                                       });
                if (targetCleanupSeen && onlyLater) {
                    heldCleanups.emplace_back(ev.Release());
                }
            } else if (const auto merge = std::dynamic_pointer_cast<NOlap::TChangesWithAppend>(changes); merge && watermarkFrozen) {
                const THashSet<ui64> ids = merge->GetPortionsToRemove().GetPortionIds();
                laterRetired.insert(ids.begin(), ids.end());
            }
        });

        // Cleanup and GC stay off so the answer waits for the later retirements; compaction off keeps earlier ones out.
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->DisableBackground(EBackground::Cleanup);
        f.Controller->DisableBackground(EBackground::GC);
        f.StartMove();
        UNIT_ASSERT_C(!f.DriveGate(150, {}, [&] {
            return !rewritten.empty();
        }), "answered before MoveData rewrote the target portions");
        UNIT_ASSERT_C(!rewritten.empty(), "MoveData never rewrote the target portions");
        // A gate check after the drain freezes the watermark; the write makes the target retirements cleanable.
        f.Write(2, 1000, 1001);
        UNIT_ASSERT(!f.DriveGate(60));

        // Two overlapping writes give compaction portions to retire after the watermark froze.
        watermarkFrozen = true;
        f.Write(3, 5000, 5100);
        f.Write(4, 5000, 5100);
        f.Controller->EnableBackground(EBackground::Compaction);
        UNIT_ASSERT(!f.DriveGate(150, {}, [&] {
            return !laterRetired.empty();
        }));
        UNIT_ASSERT_C(!laterRetired.empty(), "compaction never retired a portion after the drain");

        f.Controller->EnableBackground(EBackground::Cleanup);
        UNIT_ASSERT(!f.DriveGate(150, {}, [&] {
            return targetCleanupSeen;
        }));
        UNIT_ASSERT_C(targetCleanupSeen, "the target portions were never cleaned up");

        // The next write makes the later retirements cleanable; their cleanup stays running from here on.
        f.Write(5, 6000, 6001);
        UNIT_ASSERT(!f.DriveGate(150, {}, [&] {
            return !heldCleanups.empty();
        }));
        UNIT_ASSERT_C(!heldCleanups.empty(), "no cleanup of the later retirements started");

        f.Controller->EnableBackground(EBackground::GC);
        const auto response = f.DriveGate(150);
        UNIT_ASSERT_C(response, "a cleanup of portions retired after the drain held the answer back");
        f.AssertDrainedSuccess(response);
        for (auto& ev : heldCleanups) {
            f.Runtime.Send(ev.Release());
        }
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1101);
    }

    // Drained queues are not barrier coverage: a fresh incarnation has collected nothing yet, and its empty queues say nothing about the old generations.
    Y_UNIT_TEST(SuccessWaitsForTheFirstGCRoundOfTheIncarnation) {
        TMoveDataFixture f;
        f.Controller->DisableBackground(EBackground::TTL);
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        f.StartMove();
        auto response = f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(2, 1000, 1001);
            }
        });
        UNIT_ASSERT_C(response, "the first move never drained OldGroup");
        f.AssertDrainedSuccess(response);

        // The queues come back empty from local DB, but LastCollectedGenStep predates this generation.
        f.Controller->DisableBackground(EBackground::GC);
        f.Restart();
        const i64 blockedBefore = GateBlockedByFirstGCRound(f.Runtime);
        f.StartMove();
        // The writes let cleanup drain, so nothing but the missing barrier can hold the gate.
        UNIT_ASSERT_C(!f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(3, 2000, 2001);
            }
        }), "answered Success before this incarnation committed a GC barrier");
        UNIT_ASSERT_C(GateBlockedByFirstGCRound(f.Runtime) > blockedBefore, "the gate was held by something other than the missing GC round");

        f.Controller->EnableBackground(EBackground::GC);
        response = f.DriveGate(200, [&](const ui32 i) {
            if (i == 25) {
                f.Write(4, 3000, 3001);
            }
        });
        UNIT_ASSERT_C(response, "no Success after the first GC round of the incarnation committed");
        f.AssertDrainedSuccess(response);
    }

    // MoveData must rewrite index blobs (InheritPortionStorage=false) left in BlobStorage by a tiered portion.
    Y_UNIT_TEST(MoveDataMovesIndexBlobsOfTieredPortion) {
        // TWaitCompactionController must be registered before the runtime is set up.
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->DisableBackground(EBackground::TTL);
        csController->SetSkipSpecialCheckForEvict(true);
        csController->SetOverrideMaxReadStaleness(TDuration::Zero());

        TIntrusivePtr<NFake::TProxyDS> oldProxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
        TIntrusivePtr<NFake::TProxyDS> newProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));

        TTestBasicRuntime runtime;
        runtime.SetScheduledLimit(10'000);
        TTester::Setup(
            runtime, { new NFake::TProxyDS(TGroupId::FromValue(0)), oldProxy, newProxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
        runtime.GetAppData().FeatureFlags.SetEnableColumnshardGroupDecommission(true);

        TActorId sender = runtime.AllocateEdgeActor();
        TActorId tabletActorId = BootTablet(runtime, MakeTabletInfo(TabletId, { { 0, OldGroup } }));

        // Tier "warm" with a MAX index (DefaultStorageId=__DEFAULT) so its blobs stay in BlobStorage after tiering.
        static constexpr ui32 kBsIndexId = 3000;
        TTestSchema::TTableSpecials specials;
        {
            TTestSchema::TStorageTier warm("warm");
            warm.EvictAfter = TDuration::Zero();
            specials.Tiers.push_back(warm);
        }

        TPlanStep readStep;
        {
            NKikimrTxColumnShard::TSchemaTxBody tx;
            auto* initShard = tx.MutableInitShard();
            initShard->SetOwnerPath("/Root/olap");
            initShard->SetOwnerPathId(TableId);
            auto* tableProto = initShard->AddTables();
            TSchemeShardLocalPathId::FromRawValue(TableId).ToProto(*tableProto);
            auto* schemaProto = tableProto->MutableSchema();
            TTestSchema::InitSchema(TTestSchema::YdbSchema(), TTestSchema::YdbPkSchema(), specials, schemaProto);
            // MAX index on timestamp (column 1), __DEFAULT storage, InheritPortionStorage=false.
            *schemaProto->AddIndexes() = NOlap::NIndexes::TIndexMetaContainer(
                std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(
                    kBsIndexId, "ts_max_bs", NOlap::IStoragesManager::DefaultStorageId, /*inheritPortionStorage=*/false, /*columnId=*/1))
                                             .SerializeToProto();
            TTestSchema::InitTiersAndTtl(specials, tableProto->MutableTtlSettings());
            TString txBody;
            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
            readStep = SetupSchema(runtime, sender, txBody, 11);
        }

        // Write 1000 rows and wait for compaction to produce a single compacted portion.
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, TabletId, 1, TableId, MakeTestBlob({ 0, 1000 }, TTestSchema::YdbSchema()),
                TTestSchema::YdbSchema(), &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TabletId, 1, writeIds);
            PlanCommit(runtime, sender, TabletId, planStep, TSet<ui64>{ 1 });
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        // Apply tier config and enable TTL so the compacted portion is tiered to S3.
        csController->OverrideTierConfigs(runtime, sender, TTestSchema::BuildSnapshot(specials));
        csController->EnableBackground(EBackground::TTL);

        // Kick the tablet so tiering actualization runs immediately.
        ForwardToTablet(runtime, TabletId, sender, new TEvPrivate::TEvPeriodicWakeup());

        // Wait for TTL to start and then finish (export + BlobStorage delete-intent commit).
        {
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
            while (csController->GetTTLStartedCounter().Val() == 0 && TInstant::Now() < deadline) {
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            UNIT_ASSERT_C(csController->GetTTLStartedCounter().Val() > 0, "TTL never started");
            while (csController->GetTTLFinishedCounter().Val() < csController->GetTTLStartedCounter().Val() && TInstant::Now() < deadline) {
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            UNIT_ASSERT_C(
                csController->GetTTLFinishedCounter().Val() == csController->GetTTLStartedCounter().Val(), "TTL started but never finished");
        }

        // Allow GC to set the DoNotKeep flags on the exported column blobs.
        runtime.SimulateSleep(TDuration::Seconds(3));
        csController->WaitCompactions(TDuration::Seconds(5));

        // (a) Column blobs are in S3; the MAX index blob (DefaultStorage) remains live in OldGroup.
        const size_t indexBlobsInOld = LivePortionBlobs(*oldProxy, TabletId).size();
        UNIT_ASSERT_C(indexBlobsInOld > 0, "expected index blobs to remain in OldGroup after tiering, but found none");

        // Reassign channel history: new writes go to NewGroup; old blobs stay in OldGroup.
        ui32 reassignFrom = 0;
        for (const auto& id : LivePortionBlobs(*oldProxy, TabletId)) {
            reassignFrom = Max(reassignFrom, id.Generation() + 1);
        }
        runtime.Send(new IEventHandle(tabletActorId, tabletActorId, new TKikimrEvents::TEvPoisonPill));
        tabletActorId = BootTablet(runtime, MakeTabletInfo(TabletId, { { 0, OldGroup }, { reassignFrom, NewGroup } }));
        UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*newProxy, TabletId).size(), 0u, "no portion data may exist in NewGroup before MoveData");

        // Start MoveData for OldGroup.
        runtime.SendToPipe(TabletId, sender, new TEvTablet::TEvMoveData(std::vector<ui32>{ OldGroup }), 0, GetPipeConfigWithRetries());

        // Drive the gate; write one extra row at step 25 to advance minSnapshotForNewReads.
        TEvTablet::TEvMoveDataResponse::TPtr response;
        TPlanStep lastWriteStep = readStep;
        for (ui32 i = 0; i < 200 && !response; ++i) {
            Wakeup(runtime, sender, TabletId);
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            if (i == 25) {
                std::vector<ui64> wids;
                UNIT_ASSERT(WriteData(runtime, sender, TabletId, 2, TableId, MakeTestBlob({ 1000, 1001 }, TTestSchema::YdbSchema()),
                    TTestSchema::YdbSchema(), &wids));
                lastWriteStep = ProposeCommit(runtime, sender, TabletId, 2, wids);
                PlanCommit(runtime, sender, TabletId, lastWriteStep, TSet<ui64>{ 2 });
            }
            response = runtime.GrabEdgeEventIf<TEvTablet::TEvMoveDataResponse>(sender, [](const TEvTablet::TEvMoveDataResponse::TPtr&) {
                return true;
            }, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(response, "no TEvMoveDataResponse: MoveData never completed");

        // (b) and (d): all tablet blobs have been rewritten out of OldGroup.
        UNIT_ASSERT_VALUES_EQUAL_C(
            LivePortionBlobs(*oldProxy, TabletId).size(), 0u, "blobs remain live in OldGroup after MoveData answered Success");
        UNIT_ASSERT_VALUES_EQUAL((int)response->Get()->Record.GetStatus(), (int)NKikimrTabletBase::TEvMoveDataResponse::Success);

        // (c) The table is still readable after the move.
        UNIT_ASSERT_C(ReadAllAsBatch(runtime, TableId, NOlap::TSnapshot(lastWriteStep.Val(), 1), TTestSchema::YdbSchema())->num_rows() > 0,
            "table is empty after MoveData");

        (void)tabletActorId;
    }

    // The row is what Success promises; drained queues alone must never produce one.
    Y_UNIT_TEST(NoRowIsPersistedWhileTheGateIsClosed) {
        TMoveDataFixture f;
        f.Controller->DisableBackground(EBackground::TTL);
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        f.StartMove();
        auto response = f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(2, 1000, 1001);
            }
        });
        UNIT_ASSERT_C(response, "the first move never drained OldGroup");
        f.AssertDrainedSuccess(response);
        UNIT_ASSERT_C(!f.Controller->GetMoveDataRows().empty(), "answered Success without persisting a row");

        // A fresh incarnation reloads empty queues, yet it has collected nothing: that is no proof of an empty range.
        f.Controller->DisableBackground(EBackground::GC);
        f.Restart();
        f.Controller->ClearMoveDataRows();
        f.StartMove();
        UNIT_ASSERT_C(!f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(3, 2000, 2001);
            }
        }), "answered Success before this incarnation committed a GC barrier");
        UNIT_ASSERT_C(f.Controller->GetMoveDataRows().empty(), "persisted a row from drained queues alone");

        f.Controller->EnableBackground(EBackground::GC);
        response = f.DriveGate(200, [&](const ui32 i) {
            if (i == 25) {
                f.Write(4, 3000, 3001);
            }
        });
        UNIT_ASSERT_C(response, "no Success after the first GC round of the incarnation committed");
        UNIT_ASSERT_C(!f.Controller->GetMoveDataRows().empty(), "answered Success without a row for the drained interval");
    }

    // A target Hive adds while the proof is committing is not covered by it, so Success must wait.
    Y_UNIT_TEST(SuccessWaitsForTargetsAddedDuringTheProof) {
        TMoveDataFixture f;
        f.Controller->DisableBackground(EBackground::TTL);
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        f.ReassignPastWrittenData();

        // The widening lands between Execute and Complete: the log commit costs a round trip the tablet keeps serving.
        bool widened = false;
        const TActorId sender = f.Sender;
        f.Controller->SetOnMoveDataRowsWritten([&] {
            if (widened) {
                return;
            }
            widened = true;
            const auto* shard = f.Controller->GetTheOnlyShard();
            NActors::TActivationContext::Send(
                new IEventHandle(shard->SelfId(), sender, new TEvTablet::TEvMoveData(std::vector<ui32>{ OldGroup, NewGroup })));
        });

        f.StartMove();
        const auto response = f.DriveGate(150, [&](const ui32 i) {
            if (i == 25) {
                f.Write(2, 1000, 1001);
            }
        });
        UNIT_ASSERT_C(widened, "the proof never committed, so the widening was never exercised");
        UNIT_ASSERT_C(!response, "answered Success for a target added after the proof was captured");
    }
}

}   // namespace NKikimr
