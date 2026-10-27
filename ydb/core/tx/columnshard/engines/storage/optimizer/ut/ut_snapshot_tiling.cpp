#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/engines/changes/counters/general.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/snapshot_pp/planner.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

namespace {

struct TTestPortion {
    using TPtr = std::shared_ptr<TTestPortion>;
    using TConstPtr = std::shared_ptr<const TTestPortion>;

    ui64 PortionId;
    ui64 Start;
    ui64 Finish;
    ui64 BlobBytes;
    ui64 Snapshot;
    NPortion::EProduced Produced;
    ui32 CompactionLevel;

    TTestPortion(const ui64 portionId, const ui64 start, const ui64 finish, const ui64 blobBytes, const ui64 snapshot,
        const NPortion::EProduced produced = NPortion::INSERTED, const ui32 compactionLevel = 0)
        : PortionId(portionId)
        , Start(start)
        , Finish(finish)
        , BlobBytes(blobBytes)
        , Snapshot(snapshot)
        , Produced(produced)
        , CompactionLevel(compactionLevel)
    {
    }

    ui64 GetPortionId() const {
        return PortionId;
    }

    ui64 IndexKeyStart() const {
        return Start;
    }

    ui64 IndexKeyEnd() const {
        return Finish;
    }

    ui64 GetTotalBlobBytes() const {
        return BlobBytes;
    }

    ui64 RecordSnapshotMin() const {
        return Snapshot;
    }

    NPortion::EProduced GetProduced() const {
        return Produced;
    }

    ui32 GetCompactionLevel() const {
        return CompactionLevel;
    }
};

using TCore = TSnapshotTiling<ui64, TTestPortion, ui64>;

constexpr ui64 SMALL = 100;
constexpr ui64 LARGE = 4'000;

TSnapshotTilingSettings TestSettings() {
    TSnapshotTilingSettings settings;
    settings.AccumulatorPortionSizeLimit = 1'000;
    settings.PortionExpectedSize = 4'000;
    settings.TaskBytes = 64'000;
    // Test clocks are small integers read as seconds; the delay source stays out of the way unless a test asks for it.
    settings.DelayTrigger = TDuration::Seconds(100'000);
    settings.DelayOverload = TDuration::Seconds(200'000);
    return settings;
}

TTestPortion::TPtr Portion(const ui64 id, const ui64 start, const ui64 finish, const ui64 snapshot, const ui64 bytes = LARGE) {
    return std::make_shared<TTestPortion>(id, start, finish, bytes, snapshot);
}

TTestPortion::TPtr LinePortion(const ui64 id, const ui64 start, const ui64 finish, const ui64 snapshot) {
    return std::make_shared<TTestPortion>(id, start, finish, LARGE, snapshot, NPortion::SPLIT_COMPACTED, LINE_LEVEL);
}

TTestPortion::TPtr EvictedPortion(const ui64 id, const ui64 start, const ui64 finish, const ui64 snapshot) {
    return std::make_shared<TTestPortion>(id, start, finish, LARGE, snapshot, NPortion::EVICTED);
}

struct TFixture {
    NTiling::TCounters Counters;
    TCore Core;
    /// Inputs of running tasks, as the engine's locks manager would report them.
    THashSet<ui64> Locked;

    explicit TFixture(TSnapshotTilingSettings settings = TestSettings())
        : Core(std::move(settings), Counters)
    {
        Core.UpdateClock({ .Now = 1'000, .SettledBefore = 990 });
    }

    /// First batch: the bootstrap load.
    void Load(std::vector<TTestPortion::TPtr> portions) {
        Core.ModifyPortions(portions, {});
        Check();
    }

    void Add(const TTestPortion::TPtr& p) {
        Core.ModifyPortions({ p }, {});
        Check();
    }

    void Remove(const TTestPortion::TPtr& p) {
        Core.ModifyPortions({}, { p });
        Check();
    }

    void Check() const {
        const auto status = Core.CheckInvariants();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetErrorMessage());
    }

    ui8 LevelOf(const ui64 id) const {
        const auto level = Core.GetLevelOf(id);
        UNIT_ASSERT_C(level, "portion " << id << " is not tracked");
        return *level;
    }

    static bool NeverLocked(const TTestPortion::TConstPtr&) {
        return false;
    }

    bool IsLocked(const TTestPortion::TConstPtr& p) const {
        return Locked.contains(p->GetPortionId());
    }

    /// A task request as the engine makes it: the inputs of running tasks are locked, the task taken gets locked.
    std::optional<TCore::TTask> Take() {
        auto task = Core.TakeNextTask([this](const TTestPortion::TConstPtr& p) { return IsLocked(p); });
        if (task) {
            Lock(*task);
        }
        Check();
        return task;
    }

    std::optional<TCore::TTask> TakeAdvance() {
        auto task = Core.TakeAdvanceTask([this](const TTestPortion::TConstPtr& p) { return IsLocked(p); });
        if (task) {
            Lock(*task);
        }
        Check();
        return task;
    }

    void Lock(const TCore::TTask& task) {
        for (const auto& p : task.Portions) {
            Locked.insert(p->GetPortionId());
        }
    }

    /// The engine released the task's locks, as it does on success and on abort alike.
    void ReleaseLocks(const TCore::TTask& task) {
        for (const auto& p : task.Portions) {
            Locked.erase(p->GetPortionId());
        }
    }

    /// The engine finished the task: its inputs are removed, the given outputs are added.
    void Finish(const TCore::TTask& task, std::vector<TTestPortion::TPtr> outputs = {}) {
        ReleaseLocks(task);
        Core.ModifyPortions(outputs, task.Portions);
        Check();
    }

    /// The task aborted: its locks are gone, and the next task request's first step puts the inputs back.
    void Abort(const TCore::TTask& task) {
        ReleaseLocks(task);
        Core.Resync([this](const TTestPortion::TConstPtr& p) { return IsLocked(p); });
        Check();
    }

    static std::vector<ui64> Ids(const TCore::TTask& task) {
        std::vector<ui64> ids;
        for (const auto& p : task.Portions) {
            ids.push_back(p->GetPortionId());
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    static TString Cuts(const TCore::TTask& task) {
        TStringBuilder out;
        for (const auto& cut : task.CheckPoints) {
            out << (cut.KeyBelongsToLeft ? "]" : "[") << cut.Key << " ";
        }
        return out;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(SnapshotTilingSettings) {
    Y_UNIT_TEST(DefaultsAreValid) {
        const auto status = TSnapshotTilingSettings().Validate();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetErrorMessage());
    }

    Y_UNIT_TEST(BudgetMustHoldSixteenExpectedPortions) {
        TSnapshotTilingSettings settings;
        settings.PortionExpectedSize = 8ULL * 1024 * 1024;
        UNIT_ASSERT(!settings.Validate().IsSuccess());
        settings.TaskBytes = 128ULL * 1024 * 1024;
        UNIT_ASSERT(settings.Validate().IsSuccess());
    }

    Y_UNIT_TEST(TriggerBelowOverload) {
        TSnapshotTilingSettings settings;
        settings.OverloadHeight = 9;
        UNIT_ASSERT(!settings.Validate().IsSuccess());
        settings.OverloadHeight = settings.TriggerHeight;
        UNIT_ASSERT(!settings.Validate().IsSuccess());
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingPlacement) {
    Y_UNIT_TEST(SmallPortionGoesToAccumulator) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 100, 109, 20, SMALL));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), ACCUMULATOR_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetAccumulatorSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 20);
    }

    Y_UNIT_TEST(OldFreePortionGoesToLine) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        UNIT_ASSERT(f.Core.GetPlacementThreshold());
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 10);
        f.Add(Portion(2, 20, 29, 10));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetLineSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetOutsideSize(), 0);
    }

    Y_UNIT_TEST(FreshFreePortionWaitsOnLevelTwo) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 20, 29, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetSchedulingCompactedSnapshot(), 11);
    }

    Y_UNIT_TEST(IntersectingOldPortionStaysOutsideLine) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 5, 14, 10));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        f.Add(Portion(3, 9, 20, 10));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 2);
    }

    Y_UNIT_TEST(EqualBordersIntersect) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 9, 9, 10));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        f.Add(Portion(3, 10, 10, 10));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), LINE_LEVEL);
    }

    Y_UNIT_TEST(PersistedLineLevelIsHonouredWhileFree) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(LinePortion(2, 20, 29, 50));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), LINE_LEVEL);
    }

    Y_UNIT_TEST(PersistedLineLevelYieldsWhenDestinationIsTaken) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(LinePortion(2, 5, 29, 50));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
    }

    Y_UNIT_TEST(RaisingThresholdAdmitsLaterArrivals) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 20, 29, 15));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        f.Core.RaisePlacementThreshold(15);
        f.Add(Portion(3, 40, 49, 15));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), LINE_LEVEL);
        f.Core.RaisePlacementThreshold(12);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 15);
    }

    Y_UNIT_TEST(EvictedPortionsAreIgnored) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10), EvictedPortion(2, 0, 9, 1) });
        UNIT_ASSERT(!f.Core.GetLevelOf(2));
        f.Add(EvictedPortion(3, 100, 109, 1));
        UNIT_ASSERT(!f.Core.GetLevelOf(3));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetRegistrySize(), 1);
        f.Remove(EvictedPortion(3, 100, 109, 1));
    }

    Y_UNIT_TEST(RemovalUpdatesEverySet) {
        TFixture f;
        const auto line = Portion(1, 0, 9, 10);
        const auto tree = Portion(2, 5, 14, 11);
        const auto small = Portion(3, 100, 109, 12, SMALL);
        f.Load({ line });
        f.Add(tree);
        f.Add(small);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
        f.Remove(tree);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 0);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 12);
        f.Remove(small);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetOutsideSize(), 0);
        UNIT_ASSERT(!f.Core.GetForcedCompactedSnapshot());
        f.Remove(line);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetRegistrySize(), 0);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingBootstrap) {
    Y_UNIT_TEST(SweepsLineInSnapshotOrderAndStopsAtFirstFailure) {
        TFixture f;
        f.Load({
            Portion(4, 40, 49, 4),
            Portion(3, 5, 9, 3),
            Portion(1, 0, 9, 1),
            Portion(2, 20, 29, 2),
        });
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(1), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), FIRST_TREE_LEVEL);
        // Free, but after the first failure: demoted, comes back through the advance (accepted risk).
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(4), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 2);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 3);
    }

    Y_UNIT_TEST(DerivesSnapshotFromAccumulatorToo) {
        TFixture f;
        f.Load({
            Portion(1, 100, 101, 10, SMALL),
            Portion(2, 0, 9, 20),
            Portion(3, 5, 9, 30),
        });
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(1), ACCUMULATOR_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 20);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 10);
    }

    Y_UNIT_TEST(EmptyTableHasNoSnapshot) {
        TFixture f;
        f.Load({});
        UNIT_ASSERT(!f.Core.GetPlacementThreshold());
        // Nothing to count: the reader supplies the fallback, the shard its outdated step minus L for a forced request,
        // the adapter the node's clock minus L for the queue key (decision 6).
        UNIT_ASSERT(!f.Core.GetForcedCompactedSnapshot());
        UNIT_ASSERT(!f.Core.GetSchedulingCompactedSnapshot());
        f.Core.UpdateClock({ .Now = 2'000, .SettledBefore = 1'990 });
        UNIT_ASSERT(!f.Core.GetForcedCompactedSnapshot());
    }

    Y_UNIT_TEST(EverythingFitsGivesNoSnapshot) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 1), Portion(2, 20, 29, 2), Portion(3, 40, 49, 3) });
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetLineSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 3);
        UNIT_ASSERT(!f.Core.GetForcedCompactedSnapshot());
    }

    Y_UNIT_TEST(AllSmallTableHasNoThreshold) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 5, SMALL), Portion(2, 20, 29, 6, SMALL) });
        UNIT_ASSERT(!f.Core.GetPlacementThreshold());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetAccumulatorSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 5);
    }

    Y_UNIT_TEST(PersistedLevelsAreIgnoredAtBootstrap) {
        TFixture f;
        f.Load({ LinePortion(1, 0, 9, 10), LinePortion(2, 5, 14, 5) });
        // Snapshot order admits portion 2 first; portion 1 intersects it and is demoted despite its persisted level.
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(1), FIRST_TREE_LEVEL);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingWidth) {
    Y_UNIT_TEST(CountsMaximumDisjointSubset) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 1, 11));
        f.Add(Portion(3, 2, 3, 11));
        f.Add(Portion(4, 4, 5, 11));
        f.Add(Portion(5, 0, 5, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 4);
        const TTestPortion probe(999, 0, 9, LARGE, 11);
        ui64 examined = 0;
        UNIT_ASSERT_VALUES_EQUAL(f.Core.MeasureWidth(probe, FIRST_TREE_LEVEL, &examined), 3);
        UNIT_ASSERT_VALUES_EQUAL(examined, 4);
    }

    Y_UNIT_TEST(StopsAtThresholdPlusOne) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 20; ++i) {
            f.Add(Portion(10 + i, 2 * i, 2 * i + 1, 11));
        }
        const TTestPortion probe(999, 0, 100, LARGE, 11);
        ui64 examined = 0;
        UNIT_ASSERT_VALUES_EQUAL(f.Core.MeasureWidth(probe, FIRST_TREE_LEVEL, &examined), 11);
        UNIT_ASSERT_VALUES_EQUAL(examined, 11);
    }

    Y_UNIT_TEST(ExaminedCountIsBoundedByHeight) {
        auto settings = TestSettings();
        settings.WidthThreshold = 2;
        TFixture f(settings);
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 1; i <= 6; ++i) {
            f.Add(Portion(10 + i, 0, i, 11));
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 6);
        const TTestPortion probe(999, 0, 10, LARGE, 11);
        ui64 examined = 0;
        UNIT_ASSERT_VALUES_EQUAL(f.Core.MeasureWidth(probe, FIRST_TREE_LEVEL, &examined), 1);
        UNIT_ASSERT_VALUES_EQUAL(examined, 6);
        UNIT_ASSERT(examined <= (2 + 1) * (6 + 1) + 6);
    }

    Y_UNIT_TEST(CountsTheStackAtTheEndKey) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 100, 11));
        const TTestPortion probe(999, 50, 60, LARGE, 11);
        ui64 examined = 0;
        UNIT_ASSERT_VALUES_EQUAL(f.Core.MeasureWidth(probe, FIRST_TREE_LEVEL, &examined), 1);
        UNIT_ASSERT_VALUES_EQUAL(examined, 1);
        const TTestPortion outside(998, 200, 210, LARGE, 11);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.MeasureWidth(outside, FIRST_TREE_LEVEL, &examined), 0);
        UNIT_ASSERT_VALUES_EQUAL(examined, 0);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingDescent) {
    Y_UNIT_TEST(WidePortionStaysAboveNarrowLevel) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 11; ++i) {
            f.Add(Portion(10 + i, 10 * i, 10 * i + 9, 11));
        }
        f.Add(Portion(30, 0, 109, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(30), FIRST_TREE_LEVEL + 1);
        f.Add(Portion(31, 0, 109, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(31), FIRST_TREE_LEVEL + 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL + 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 1);
    }

    Y_UNIT_TEST(NarrowPortionDescendsToLevelTwo) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 11; ++i) {
            f.Add(Portion(10 + i, 10 * i, 10 * i + 9, 11));
        }
        f.Add(Portion(30, 0, 109, 11));
        f.Add(Portion(40, 50, 55, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(40), FIRST_TREE_LEVEL);
    }

    Y_UNIT_TEST(HeightCountsOverlap) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 10, 11));
        f.Add(Portion(3, 5, 15, 11));
        f.Add(Portion(4, 8, 20, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 3);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingBootstrapLevels) {
    Y_UNIT_TEST(SeedAndFillMeasuresAgainstTheSeededLevel) {
        auto settings = TestSettings();
        settings.WidthThreshold = 2;
        TFixture f(settings);
        f.Load({
            Portion(1, 0, 9, 0),
            Portion(2, 0, 4, 1),
            Portion(3, 0, 9, 2),
            Portion(4, 5, 6, 3),
            Portion(5, 7, 8, 4),
        });
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(1), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(4), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(5), FIRST_TREE_LEVEL);
        // Measured against the seeded level, the old wide portion spans three and goes up (design.md, decision 10).
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), FIRST_TREE_LEVEL + 1);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 0);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 1);
    }

    Y_UNIT_TEST(LeftoversStayOnTheTopLevel) {
        auto settings = TestSettings();
        settings.WidthThreshold = 2;
        settings.TreeLevelsCount = 1;
        TFixture f(settings);
        f.Load({
            Portion(1, 0, 99, 0),
            Portion(2, 0, 1, 1),
            Portion(3, 2, 3, 2),
            Portion(4, 4, 5, 3),
            Portion(5, 0, 5, 4),
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetSettings().TopLevel(), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(5), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 4);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 2);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingTasks) {
    Y_UNIT_TEST(LevelTaskTakesTheStackAndHandsItOut) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 9; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11 + i));
            UNIT_ASSERT(!f.Core.GetTaskPriority());
            UNIT_ASSERT(!f.Take());
        }
        f.Add(Portion(19, 0, 10, 20));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 10);
        UNIT_ASSERT(f.Core.GetTaskPriority());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTaskPriority()->GetLevel(), 1);
        UNIT_ASSERT(!f.Core.IsOverloaded());

        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 10);
        UNIT_ASSERT(task->Kind == ETaskKind::Merge);
        UNIT_ASSERT(task->Reason == ETaskReason::Level);
        UNIT_ASSERT_VALUES_EQUAL(task->SourceLevel, FIRST_TREE_LEVEL);
        // Oldest first.
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.front()->GetPortionId(), 10);

        // Handed out: gone from the unlocked view, still in the registry and in the forced reading of S.
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetOutsideSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
        UNIT_ASSERT(!f.Core.GetSchedulingCompactedSnapshot());
        UNIT_ASSERT(!f.Core.GetTaskPriority());
        UNIT_ASSERT(!f.Take());

        // Success: the engine removes the inputs and adds the output.
        f.Finish(*task, { Portion(30, 0, 10, 11) });
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetRegistrySize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(30), FIRST_TREE_LEVEL);
    }

    Y_UNIT_TEST(AbortedInputsComeBackOnTheNextTaskRequest) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 10; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11));
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 0);
        // The level reports nothing, but the planner remembers the task's priority until its inputs are back or gone,
        // so the scheduling pass after an abort still asks the queue (decision 14).
        UNIT_ASSERT(!f.Core.GetTaskPriority());
        UNIT_ASSERT(f.Core.GetSchedulingPriority());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetSchedulingPriority()->GetLevel(), 1);
        // The task aborted and the engine released its locks: the next request restores the inputs and builds the task again.
        f.ReleaseLocks(*task);
        const auto again = f.Take();
        UNIT_ASSERT(again);
        UNIT_ASSERT_VALUES_EQUAL(again->Portions.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 0);
        UNIT_ASSERT(!f.Core.GetSchedulingCompactedSnapshot());
        f.Finish(*again, { Portion(30, 0, 10, 11) });
        UNIT_ASSERT(!f.Core.GetSchedulingPriority());
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetSchedulingCompactedSnapshot(), 11);
    }

    Y_UNIT_TEST(AbortedInputsComeBackOnResync) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 10; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11));
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        f.Abort(*task);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 10);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetSchedulingCompactedSnapshot(), 11);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetSchedulingPriority()->GetLevel(), 1);
        UNIT_ASSERT(f.Take());
    }

    Y_UNIT_TEST(RunningTaskInputsStayLockedThroughTheNextRequest) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 10; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11));
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        // While the task runs, a request finds its inputs locked: nothing comes back and no task is built.
        UNIT_ASSERT(!f.Take());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetSchedulingPriority()->GetLevel(), 1);
    }

    Y_UNIT_TEST(LevelTaskIsCappedAndOverloadRecomputed) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 20; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11 + i));
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 20);
        UNIT_ASSERT(f.Core.IsOverloaded());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTaskPriority()->GetLevel(), 10);

        const auto task = f.Take();
        UNIT_ASSERT(task);
        // 64 000 bytes of budget, 4 000 per portion: sixteen.
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(task->Bytes, 64'000);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHeight(FIRST_TREE_LEVEL), 4);
        UNIT_ASSERT(!f.Core.IsOverloaded());
        UNIT_ASSERT(!f.Core.GetTaskPriority());
    }

    Y_UNIT_TEST(OverloadIsPublishedToTheNodeBuckets) {
        TFixture f;
        const auto nodeMax = [&f]() {
            return f.Counters.GetTilingCounters().Portions->GetMaxOverloadLevel();
        };
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        UNIT_ASSERT_VALUES_EQUAL(nodeMax(), 0);
        for (ui64 i = 0; i < 9; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11 + i));
        }
        UNIT_ASSERT_VALUES_EQUAL(nodeMax(), 0);
        f.Add(Portion(19, 0, 10, 20));
        // Height 10 is the trigger: level 1.
        UNIT_ASSERT_VALUES_EQUAL(nodeMax(), 1);
        for (ui64 i = 0; i < 5; ++i) {
            f.Add(Portion(20 + i, 0, 10, 21 + i));
        }
        // Height 15 is the overload edge: critical.
        UNIT_ASSERT(f.Core.IsOverloaded());
        UNIT_ASSERT_VALUES_EQUAL(nodeMax(), 10);
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 15);
        // The whole stack is running: nothing is overloaded on the unlocked view (decision 25).
        UNIT_ASSERT(!f.Core.IsOverloaded());
        UNIT_ASSERT_VALUES_EQUAL(nodeMax(), 0);
        f.Abort(*task);
        UNIT_ASSERT_VALUES_EQUAL(nodeMax(), 10);
    }

    Y_UNIT_TEST(TwoPortionsAlwaysFit) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 10; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11, 40'000));
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(task->Bytes, 80'000);
    }

    Y_UNIT_TEST(LockedPortionsAreSkippedButKeepCounting) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 10; ++i) {
            f.Add(Portion(10 + i, 0, 10, 11));
        }
        const auto lockedElsewhere = [](const TTestPortion::TConstPtr& p) {
            return p->GetPortionId() == 15;
        };
        auto task = f.Core.TakeNextTask(lockedElsewhere);
        f.Check();
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(15), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 1);
    }

    Y_UNIT_TEST(TopUpTakesContainedPortionsOfOtherLevels) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        for (ui64 i = 0; i < 11; ++i) {
            f.Add(Portion(10 + i, 10 * i, 10 * i + 9, 11));
        }
        for (ui64 i = 0; i < 10; ++i) {
            f.Add(Portion(30 + i, 0, 109, 11));
            UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(30 + i), FIRST_TREE_LEVEL + 1);
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->SourceLevel, FIRST_TREE_LEVEL + 1);
        // Ten wide portions, 40 000 bytes, then six contained narrow ones fill the 64 000 budget.
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 5);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL + 1), 0);
    }

    Y_UNIT_TEST(AccumulatorTaskAtTrigger) {
        auto settings = TestSettings();
        settings.AccumulatorTriggerPortions = 3;
        settings.AccumulatorOverloadPortions = 10;
        TFixture f(settings);
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 1, 11, SMALL));
        f.Add(Portion(3, 50, 51, 11, SMALL));
        UNIT_ASSERT(!f.Take());
        f.Add(Portion(4, 20, 21, 11, SMALL));
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::Accumulator);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 3);
        // End-key order, so that neighbours meet.
        UNIT_ASSERT_VALUES_EQUAL(task->Portions[1]->GetPortionId(), 4);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetAccumulatorSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
        f.Finish(*task, { Portion(5, 0, 51, 11) });
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(5), FIRST_TREE_LEVEL);
    }

    Y_UNIT_TEST(HigherPriorityUnitWins) {
        auto settings = TestSettings();
        settings.AccumulatorTriggerPortions = 2;
        settings.AccumulatorOverloadPortions = 100;
        TFixture f(settings);
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 1, 11, SMALL));
        f.Add(Portion(3, 2, 3, 11, SMALL));
        for (ui64 i = 0; i < 15; ++i) {
            f.Add(Portion(10 + i, 100, 110, 11));
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::Level);
        const auto next = f.Take();
        UNIT_ASSERT(next);
        UNIT_ASSERT(next->Reason == ETaskReason::Accumulator);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingDelay) {
    TSnapshotTilingSettings DelaySettings() {
        auto settings = TestSettings();
        settings.DelayTrigger = TDuration::Seconds(100);
        settings.DelayOverload = TDuration::Seconds(200);
        return settings;
    }

    Y_UNIT_TEST(BelowTheTriggerNothingIsCompacted) {
        TFixture f(DelaySettings());
        f.Load({ Portion(1, 0, 9, 10) });
        // S is 950 against a clock of 1000: a lag of 50, below the trigger of 100. No source, no task, no overload.
        f.Add(Portion(2, 5, 14, 950));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetDelaySeconds(), 50);
        UNIT_ASSERT(!f.Core.GetTaskPriority());
        UNIT_ASSERT(!f.Core.GetSchedulingPriority());
        UNIT_ASSERT(!f.Take());
        UNIT_ASSERT(!f.Core.IsOverloaded());
        // The entry point that ignores the trigger, for a forced request in PR 2, still composes the task.
        UNIT_ASSERT(f.Core.HasAdvanceTarget());
        UNIT_ASSERT(f.TakeAdvance());
    }

    Y_UNIT_TEST(DelayPriorityGrowsLinearlyToCritical) {
        TFixture f(DelaySettings());
        f.Load({ Portion(1, 0, 9, 10) });
        // A lag of exactly the trigger: level 1.
        f.Add(Portion(2, 5, 14, 900));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetDelaySeconds(), 100);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTaskPriority()->GetLevel(), 1);
        // Halfway to the edge: level 5.
        f.Core.UpdateClock({ .Now = 1'050, .SettledBefore = 1'040 });
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetDelaySeconds(), 150);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTaskPriority()->GetLevel(), 5);
        UNIT_ASSERT(!f.Core.IsOverloaded());
        // At the edge: critical, overloaded, and the node's buckets follow the clock, not only portion changes.
        f.Core.UpdateClock({ .Now = 1'100, .SettledBefore = 1'090 });
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTaskPriority()->GetLevel(), 10);
        UNIT_ASSERT(f.Core.IsOverloaded());
        UNIT_ASSERT_VALUES_EQUAL(f.Counters.GetTilingCounters().Portions->GetMaxOverloadLevel(), 10);
        // The advance is taken as a prioritized task: the target with its line portion, at the delay's priority.
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMerge);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2 }));
        UNIT_ASSERT_VALUES_EQUAL(task->Priority.GetLevel(), 10);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 900);
        // Handed out, the target counts for nothing on the unlocked view: no lag, no overload, the priority is remembered.
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetDelaySeconds(), 0);
        UNIT_ASSERT(!f.Core.IsOverloaded());
        UNIT_ASSERT(!f.Core.GetTaskPriority());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetSchedulingPriority()->GetLevel(), 10);
    }

    Y_UNIT_TEST(LevelTaskStillWinsOverALowerDelayPriority) {
        TFixture f(DelaySettings());
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        // Height 15 is critical; the lag of 100 is only level 1.
        for (ui64 i = 0; i < 15; ++i) {
            f.Add(Portion(10 + i, 0, 10, 900));
        }
        const auto task = f.Take();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::Level);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 15);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingCounters) {
    Y_UNIT_TEST(OldestSnapshotOverPlanners) {
        NTiling::TCounters counters;
        auto& snapshots = counters.GetCompactedSnapshotCounters();
        snapshots.Set(1, 1'000, 1'100);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetOldestSeconds(), 1'000);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetCurrentSeconds(), 1'100);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 100);
        snapshots.Set(2, 900, 1'100);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetOldestSeconds(), 900);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 200);
        // The older planner leaves: the other one's value is the oldest again.
        snapshots.Remove(2, 1'200);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetOldestSeconds(), 1'000);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 200);
        // No planner left: the oldest snapshot is the clock and the delay is zero.
        snapshots.Remove(1, 1'300);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetOldestSeconds(), 1'300);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetCurrentSeconds(), 1'300);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 0);
    }

    Y_UNIT_TEST(SettledPlannerKeepsItsDelayBetweenReports) {
        NTiling::TCounters counters;
        auto& snapshots = counters.GetCompactedSnapshotCounters();
        // A settled planner reports the clock minus L = 10 and then goes quiet; a planner with data outside reports once too.
        snapshots.Set(1, 1'090, 1'100, /*settled=*/true);
        snapshots.Set(2, 1'000, 1'100, /*settled=*/false);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 100);
        // A minute later another planner publishes: the settled one still shows a delay of L, the other one has aged.
        snapshots.Set(3, 1'150, 1'160, /*settled=*/true);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetOldestSeconds(), 1'000);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 160);
        snapshots.Remove(2, 1'160);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetOldestSeconds(), 1'150);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetMaxDelaySeconds(), 10);
    }

    Y_UNIT_TEST(TaskCountersPerReason) {
        using NChanges::TGeneralCompactionCounters;
        const auto& move = TGeneralCompactionCounters::ForReason("snapshot_move");
        const auto& level = TGeneralCompactionCounters::ForReason("level");
        UNIT_ASSERT(&move != &level);
        UNIT_ASSERT(&move == &TGeneralCompactionCounters::ForReason("snapshot_move"));
        const auto startedBefore = move.GetTasksStarted();
        move.OnStart(1, 0, TDuration::MilliSeconds(5));
        move.OnWritten(0, 0);
        move.OnFinish(true, TDuration::MilliSeconds(20));
        UNIT_ASSERT_VALUES_EQUAL(move.GetTasksStarted(), startedBefore + 1);
        UNIT_ASSERT_VALUES_EQUAL(move.GetTasksFinished(), startedBefore + 1);
        UNIT_ASSERT_VALUES_EQUAL(level.GetTasksStarted(), 0);
    }
}

Y_UNIT_TEST_SUITE(SnapshotTilingAdvance) {
    Y_UNIT_TEST(FreeTargetIsMovedByMetadata) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 20, 29, 11));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        UNIT_ASSERT(f.Core.HasAdvanceTarget());
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Kind == ETaskKind::MetadataMove);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMove);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT(task->CheckPoints.empty());
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetPlacementThreshold(), 11);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
        UNIT_ASSERT(!f.Core.GetSchedulingCompactedSnapshot());
        UNIT_ASSERT(!f.Core.HasAdvanceTarget());
        // The engine commits the move: the same portion comes back with level 1 persisted.
        f.Core.ModifyPortions({ LinePortion(2, 20, 29, 11) }, { task->Portions.front() });
        f.Check();
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), LINE_LEVEL);
        UNIT_ASSERT(!f.Core.GetForcedCompactedSnapshot());
    }

    Y_UNIT_TEST(MoveWhoseDestinationWasTakenIsPlacedByTheArrivalRule) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 20, 29, 11));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task && task->Kind == ETaskKind::MetadataMove);
        // An old free arrival takes the destination while the move runs.
        f.Add(Portion(3, 25, 35, 10));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(3), LINE_LEVEL);
        f.Core.ModifyPortions({ LinePortion(2, 20, 29, 11) }, { task->Portions.front() });
        f.Check();
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
    }

    Y_UNIT_TEST(AbortedAdvanceKeepsAFloorPriority) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 20, 29, 11));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task && task->Kind == ETaskKind::MetadataMove);
        UNIT_ASSERT(!f.Core.GetTaskPriority());
        UNIT_ASSERT(!f.Core.HasAdvanceTarget());
        // Level 0 with weight 1 passes the engine's zero filter, so the pass after an abort still asks the queue.
        UNIT_ASSERT(f.Core.GetSchedulingPriority());
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetSchedulingPriority()->GetLevel(), 0);
        UNIT_ASSERT(!f.Core.GetSchedulingPriority()->IsZero());
        // The move aborted: the next advance request restores the target and takes it again.
        f.ReleaseLocks(*task);
        const auto again = f.TakeAdvance();
        UNIT_ASSERT(again && again->Kind == ETaskKind::MetadataMove);
        UNIT_ASSERT_VALUES_EQUAL(again->Portions.front()->GetPortionId(), 2);
        UNIT_ASSERT(f.Core.GetSchedulingPriority());
    }

    Y_UNIT_TEST(TargetInsideTheLineRunNeedsNoCut) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10), Portion(2, 20, 29, 10) });
        f.Add(Portion(3, 5, 25, 11));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Kind == ETaskKind::Merge);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMerge);
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2, 3 }));
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetLineSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 3);
        f.Finish(*task, { LinePortion(4, 0, 29, 10) });
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(4), LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetOutsideSize(), 0);
    }

    Y_UNIT_TEST(TargetBeyondTheRunEntersTheLineWhole) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10), Portion(2, 20, 29, 10) });
        f.Add(Portion(3, 5, 40, 11));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMerge);
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        // The window grows to the target's end, where the line has nothing: the whole output enters the line.
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
    }

    Y_UNIT_TEST(PrefixWhenTheLineRunExceedsTheBudget) {
        TFixture f;
        std::vector<TTestPortion::TPtr> line;
        for (ui64 i = 0; i < 20; ++i) {
            line.push_back(Portion(10 + i, 10 * i, 10 * i + 9, 10));
        }
        f.Load(line);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetLineSize(), 20);
        f.Add(Portion(1, 0, 199, 11));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvancePrefix);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(task->Bytes, 64'000);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "[150 ");
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetHandedOutSize(), 16);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetLineSize(), 20);
        // The memory shortage: the uncut window would have taken the target and all twenty line portions.
        UNIT_ASSERT_VALUES_EQUAL(task->IdealBytes, 84'000);
    }

    Y_UNIT_TEST(EveryIntersectingPortionOfTheLevelJoins) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 100, 11));
        f.Add(Portion(3, 10, 20, 12));
        f.Add(Portion(4, 40, 50, 12));
        f.Add(Portion(5, 80, 90, 12));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 4);
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Kind == ETaskKind::Merge);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMerge);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 2, 3, 4, 5 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 0);
    }

    Y_UNIT_TEST(WiderNeighbourGrowsTheWindow) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 10, 11));
        f.Add(Portion(3, 5, 100, 12));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 2, 3 }));
        // The neighbour is taken whole and the window follows it to 100: no cut, no remainder.
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
    }

    Y_UNIT_TEST(EveryLevelAndTheLineJoinTheWindow) {
        TFixture f;
        f.Load({ Portion(1, 0, 109, 10) });
        for (ui64 i = 0; i < 11; ++i) {
            f.Add(Portion(10 + i, 10 * i, 10 * i + 9, 11));
        }
        f.Add(Portion(30, 0, 109, 12));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(30), FIRST_TREE_LEVEL + 1);
        // The target is the narrow portion with the smallest end key; the line portion grows the window over everything.
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMerge);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 13);
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
        UNIT_ASSERT_VALUES_EQUAL(task->IdealBytes, task->Bytes);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL + 1), 0);
        f.Finish(*task, { LinePortion(40, 0, 109, 11) });
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetOutsideSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetLineSize(), 1);
    }

    Y_UNIT_TEST(OverlappingNeighboursChainUntilAGap) {
        TFixture f;
        f.Load({ Portion(1, 100, 109, 10) });
        f.Add(Portion(2, 0, 10, 11));
        f.Add(Portion(3, 8, 30, 12));
        f.Add(Portion(4, 28, 50, 12));
        f.Add(Portion(5, 60, 70, 12));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 2, 3, 4 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, FIRST_TREE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 1);
    }

    Y_UNIT_TEST(BudgetCutsTheWindowAtTheFirstPortionThatDoesNotFit) {
        TFixture f;
        f.Load({ Portion(1, 1'000, 1'009, 10) });
        f.Add(Portion(2, 0, 200, 11));
        for (ui64 i = 0; i < 20; ++i) {
            f.Add(Portion(10 + i, 10 * i, 10 * i + 9, 12));
        }
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvancePrefix);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(task->Bytes, 64'000);
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, FIRST_TREE_LEVEL);
        // The sixteenth sibling starts at 150: the slice below it is absorbed whole, the rest stays for the next advance.
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "[150 ");
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 5);
        // The uncut window: the target and all twenty siblings.
        UNIT_ASSERT_VALUES_EQUAL(task->IdealBytes, 84'000);
    }

    Y_UNIT_TEST(HeldLinePortionBeyondTheLeftBorderCutsTheWindow) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10), Portion(2, 20, 29, 10) });
        f.Add(Portion(4, 22, 23, 11));
        const auto first = f.TakeAdvance();
        UNIT_ASSERT(first);
        UNIT_ASSERT_EQUAL(f.Ids(*first), (std::vector<ui64>{ 2, 4 }));
        f.Add(Portion(3, 5, 25, 12));
        const auto second = f.TakeAdvance();
        UNIT_ASSERT(second);
        UNIT_ASSERT(second->Reason == ETaskReason::AdvancePrefix);
        UNIT_ASSERT_EQUAL(f.Ids(*second), (std::vector<ui64>{ 1, 3 }));
        UNIT_ASSERT_VALUES_EQUAL(second->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*second), "[20 ");
    }

    Y_UNIT_TEST(PortionsCoveringTheLeftBorderBeyondTheBudgetCloseAtTheLinePortion) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 0, 100, 11));
        for (ui64 i = 0; i < 16; ++i) {
            f.Add(Portion(10 + i, 0, 100 + i, 12));
        }
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Reason == ETaskReason::AdvancePrefix);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        // Every sibling covers key 0, so no cut at a start key works; the window closes at the line portion's end.
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "]9 ");
        UNIT_ASSERT_VALUES_EQUAL(task->IdealBytes, 72'000);
    }

    Y_UNIT_TEST(TargetAboveAnEmptiedLevelReachesTheLine) {
        TFixture f;
        f.Load({ Portion(1, 0, 109, 10) });
        std::vector<TTestPortion::TConstPtr> narrow;
        for (ui64 i = 0; i < 11; ++i) {
            const auto p = Portion(10 + i, 10 * i, 10 * i + 9, 11);
            f.Add(p);
            narrow.push_back(p);
        }
        f.Add(Portion(30, 0, 109, 12));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(30), FIRST_TREE_LEVEL + 1);
        f.Core.ModifyPortions({}, narrow);
        f.Check();
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetTreeLevelSize(FIRST_TREE_LEVEL), 0);
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 30 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
    }

    Y_UNIT_TEST(SmallTargetMergesWithTheIntersectingLinePortion) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 5, 5, 11, SMALL));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
    }

    Y_UNIT_TEST(SmallTargetTakesTheIntersectingTreePortionsToo) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 5, 5, 11, SMALL));
        f.Add(Portion(3, 3, 30, 12));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2, 3 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Cuts(*task), "");
    }

    Y_UNIT_TEST(SmallTargetTakesTheNearestLinePortionWithoutACut) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10), Portion(2, 100, 109, 10) });
        f.Add(Portion(3, 50, 51, 11, SMALL));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 2, 3 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, LINE_LEVEL);
        UNIT_ASSERT(task->CheckPoints.empty());
    }

    Y_UNIT_TEST(SmallTargetWithAnEmptyLineTakesTheNearestTreePortion) {
        TFixture f;
        f.Load({});
        f.Add(Portion(1, 0, 1, 11, SMALL));
        f.Add(Portion(2, 100, 109, 12));
        UNIT_ASSERT_VALUES_EQUAL(f.LevelOf(2), FIRST_TREE_LEVEL);
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2 }));
        UNIT_ASSERT(task->CheckPoints.empty());
    }

    Y_UNIT_TEST(AllSmallTableCoalescesItsNeighbours) {
        TFixture f;
        f.Load({});
        f.Add(Portion(1, 0, 1, 11, SMALL));
        f.Add(Portion(2, 10, 11, 11, SMALL));
        f.Add(Portion(3, 20, 21, 11, SMALL));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT(task->Kind == ETaskKind::Merge);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2, 3 }));
        UNIT_ASSERT_VALUES_EQUAL(task->TargetLevel, ACCUMULATOR_LEVEL);
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetAccumulatorSize(), 0);
    }

    Y_UNIT_TEST(LoneSmallPortionYieldsNoTask) {
        TFixture f;
        f.Load({});
        f.Add(Portion(1, 0, 1, 11, SMALL));
        UNIT_ASSERT(f.Core.HasAdvanceTarget());
        UNIT_ASSERT(!f.TakeAdvance());
        UNIT_ASSERT_VALUES_EQUAL(*f.Core.GetForcedCompactedSnapshot(), 11);
    }

    Y_UNIT_TEST(BlockedTargetIsSkippedAndRetried) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 5, 6, 11));
        f.Add(Portion(3, 100, 109, 12));
        const auto first = f.TakeAdvance();
        UNIT_ASSERT(first);
        UNIT_ASSERT_EQUAL(f.Ids(*first), (std::vector<ui64>{ 1, 2 }));
        // The free target behind it is not blocked.
        const auto second = f.TakeAdvance();
        UNIT_ASSERT(second && second->Kind == ETaskKind::MetadataMove);
        // A target that needs the line portion held by the first task waits.
        f.Add(Portion(4, 7, 8, 13));
        UNIT_ASSERT(!f.TakeAdvance());
        f.Finish(*first, { LinePortion(5, 0, 9, 10) });
        const auto third = f.TakeAdvance();
        UNIT_ASSERT(third);
        UNIT_ASSERT_EQUAL(f.Ids(*third), (std::vector<ui64>{ 4, 5 }));
    }

    Y_UNIT_TEST(YoungTargetsWaitForTheSettleTime) {
        TFixture f;
        f.Load({ Portion(1, 0, 9, 10) });
        f.Add(Portion(2, 5, 6, 995));
        UNIT_ASSERT(!f.Core.HasAdvanceTarget());
        UNIT_ASSERT(!f.TakeAdvance());
        f.Core.UpdateClock({ .Now = 1'100, .SettledBefore = 1'090 });
        UNIT_ASSERT(f.Core.HasAdvanceTarget());
        UNIT_ASSERT(f.TakeAdvance());
    }

    Y_UNIT_TEST(AccumulatorPortionsInsideTheSpanJoin) {
        TFixture f;
        f.Load({ Portion(1, 0, 99, 10) });
        f.Add(Portion(2, 10, 11, 11, SMALL));
        f.Add(Portion(3, 50, 51, 12, SMALL));
        f.Add(Portion(4, 500, 501, 12, SMALL));
        const auto task = f.TakeAdvance();
        UNIT_ASSERT(task);
        UNIT_ASSERT_EQUAL(f.Ids(*task), (std::vector<ui64>{ 1, 2, 3 }));
        UNIT_ASSERT_VALUES_EQUAL(f.Core.GetAccumulatorSize(), 1);
    }
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling

#include <ydb/core/formats/arrow/rows/view.h>

#include <util/system/hp_timer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling {

namespace {

/// A portion with the engine's key representation: serialized primary keys rebuilt as NArrow::TSimpleRow on every access.
struct TRowPortion {
    using TPtr = std::shared_ptr<TRowPortion>;
    using TConstPtr = std::shared_ptr<const TRowPortion>;

    ui64 PortionId;
    NArrow::TSimpleRow Start;
    NArrow::TSimpleRow End;
    ui64 BlobBytes;
    ui64 Snapshot;
    ui32 CompactionLevel;

    ui64 GetPortionId() const {
        return PortionId;
    }
    NArrow::TSimpleRow IndexKeyStart() const {
        return Start;
    }
    NArrow::TSimpleRow IndexKeyEnd() const {
        return End;
    }
    ui64 GetTotalBlobBytes() const {
        return BlobBytes;
    }
    ui64 RecordSnapshotMin() const {
        return Snapshot;
    }
    NPortion::EProduced GetProduced() const {
        return NPortion::INSERTED;
    }
    ui32 GetCompactionLevel() const {
        return CompactionLevel;
    }
};

using TRowCore = TSnapshotTiling<NArrow::TSimpleRow, TRowPortion, ui64>;

std::shared_ptr<arrow::Schema> BenchSchema() {
    static std::shared_ptr<arrow::Schema> schema = arrow::schema({ arrow::field("resource_id", arrow::uint64()),
        arrow::field("timestamp", arrow::uint64()), arrow::field("uid", arrow::utf8()) });
    return schema;
}

NArrow::TSimpleRow BenchKey(const ui64 resource, const ui64 ts, const TString& uid) {
    NArrow::TSimpleRowViewV0::TWriter writer(64);
    writer.Append<ui64>(resource);
    writer.Append<ui64>(ts);
    writer.Append<ui8>(1, false);
    writer.Append<ui32>(uid.size(), false);
    writer.Append(uid.data(), uid.size(), false);
    return NArrow::TSimpleRow(writer.Finish(), BenchSchema());
}

TRowPortion::TPtr RowPortion(const ui64 id, const ui64 tsStart, const ui64 tsEnd, const ui64 snapshot, const ui64 bytes, const ui32 level = 0) {
    return std::make_shared<TRowPortion>(TRowPortion{ .PortionId = id, .Start = BenchKey(7, tsStart, "aaaaaaaa-start"),
        .End = BenchKey(7, tsEnd, "zzzzzzzz-end"), .BlobBytes = bytes, .Snapshot = snapshot, .CompactionLevel = level });
}

double Micros(const THPTimer& timer, const ui64 ops) {
    return timer.Passed() * 1e6 / ops;
}

}   // namespace

Y_UNIT_TEST_SUITE(SnapshotTilingBench) {
    /// Per-call costs of the core on a shard-sized structure with the engine's key type; prints, asserts nothing tight.
    Y_UNIT_TEST(PortionChangeCostWithSimpleRowKeys) {
        NTiling::TCounters counters;
        auto settings = TestSettings();
        // The advance is live from the start: everything loaded is far older than one second.
        settings.DelayTrigger = TDuration::Seconds(1);
        TRowCore core(std::move(settings), counters);
        core.UpdateClock({ .Now = 1'000, .SettledBefore = 990 });
        ui64 nextId = 1;
        std::vector<TRowPortion::TPtr> load;
        // The line: 2000 disjoint portions over the key space.
        for (ui64 i = 0; i < 2'000; ++i) {
            load.push_back(RowPortion(nextId++, i * 1'000, i * 1'000 + 999, 10, LARGE, LINE_LEVEL));
        }
        // Level 2: 400 normal-size portions in stacks of 9 below the level trigger, each spanning about 5 line portions.
        for (ui64 i = 0; i < 400; ++i) {
            const ui64 base = 1'500'000 + (i / 9) * 6'000;
            load.push_back(RowPortion(nextId++, base + (i % 9) * 100, base + 4'900 + (i % 9) * 100, 11, LARGE));
        }
        // The accumulator: 5600 small portions near the frontier.
        for (ui64 i = 0; i < 5'600; ++i) {
            const ui64 ts = 1'900'000 + (i * 7'919) % 100'000;
            load.push_back(RowPortion(nextId++, ts, ts + 3, 12, SMALL));
        }
        {
            THPTimer timer;
            core.ModifyPortions(load, {});
            Cerr << "bootstrap of " << load.size() << " portions: " << timer.Passed() * 1e3 << " ms" << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(core.GetLineSize(), 2'000);
        UNIT_ASSERT_VALUES_EQUAL(core.GetAccumulatorSize(), 5'600);
        THashSet<ui64> locked;
        const auto isLocked = [&locked](const TRowPortion::TConstPtr& p) { return locked.contains(p->GetPortionId()); };
        const auto take = [&]() {
            auto task = core.TakeNextTask(isLocked);
            if (task) {
                for (const auto& p : task->Portions) {
                    locked.insert(p->GetPortionId());
                }
            }
            return task;
        };

        const auto measure = [&](const TString& name) {
            std::vector<TRowPortion::TPtr> added;
            {
                THPTimer timer;
                for (ui64 i = 0; i < 1'000; ++i) {
                    const ui64 ts = 1'950'000 + (i * 7'919) % 100'000;
                    added.push_back(RowPortion(nextId++, ts, ts + 3, 12, SMALL));
                    core.ModifyPortions({ added.back() }, {});
                }
                Cerr << name << ": add small portion " << Micros(timer, 1'000) << " us";
            }
            {
                THPTimer timer;
                for (const auto& p : added) {
                    core.ModifyPortions({}, { p });
                }
                Cerr << ", remove small " << Micros(timer, 1'000) << " us";
            }
            {
                std::vector<TRowPortion::TPtr> normal;
                THPTimer timer;
                for (ui64 i = 0; i < 200; ++i) {
                    const ui64 base = 1'600'000 + i * 400;
                    normal.push_back(RowPortion(nextId++, base, base + 60'000, 12, LARGE));
                    core.ModifyPortions({ normal.back() }, {});
                }
                Cerr << ", add normal-size " << Micros(timer, 200) << " us";
                timer.Reset();
                for (const auto& p : normal) {
                    core.ModifyPortions({}, { p });
                }
                Cerr << ", remove normal-size " << Micros(timer, 200) << " us";
            }
            {
                THPTimer timer;
                ui64 sum = 0;
                for (ui64 i = 0; i < 1'000; ++i) {
                    core.UpdateClock({ .Now = 1'000 + i, .SettledBefore = 990 + i });
                    sum += core.GetSchedulingPriority().has_value();
                    sum += core.GetDelaySeconds();
                }
                Cerr << ", clock + priority " << Micros(timer, 1'000) << " us (" << sum << ")" << Endl;
            }
        };

        measure("idle");
        {
            THPTimer timer;
            ui64 accumulatorTasks = 0;
            ui64 inputs = 0;
            while (true) {
                const auto task = take();
                if (!task || task->Reason != ETaskReason::Accumulator) {
                    break;
                }
                ++accumulatorTasks;
                inputs += task->Portions.size();
            }
            Cerr << "TakeNextTask: " << accumulatorTasks << " accumulator tasks of " << inputs / std::max<ui64>(1, accumulatorTasks)
                 << " portions, " << Micros(timer, accumulatorTasks + 1) << " us each" << Endl;
        }
        measure("accumulator handed out");
        {
            THPTimer timer;
            ui64 advances = 0;
            ui64 inputs = 0;
            for (ui64 i = 0; i < 50; ++i) {
                const auto task = take();
                if (!task) {
                    break;
                }
                UNIT_ASSERT(task->Reason == ETaskReason::AdvanceMerge || task->Reason == ETaskReason::AdvancePrefix ||
                            task->Reason == ETaskReason::AdvanceMove);
                ++advances;
                inputs += task->Portions.size();
            }
            Cerr << "TakeNextTask: " << advances << " advance tasks of " << inputs / std::max<ui64>(1, advances) << " portions, "
                 << Micros(timer, std::max<ui64>(1, advances)) << " us each" << Endl;
        }
        measure("advances handed out at the front of the set");
        {
            THPTimer timer;
            ui64 found = 0;
            for (ui64 i = 0; i < 100; ++i) {
                found += take().has_value();
            }
            Cerr << "TakeNextTask x100 more: " << Micros(timer, 100) << " us each, found " << found << Endl;
        }
    }
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSnapshotTiling
