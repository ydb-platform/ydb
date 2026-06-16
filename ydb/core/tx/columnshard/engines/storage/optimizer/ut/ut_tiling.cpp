#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling_pp/tiling.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <random>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

namespace {

struct TTestPortion {
    using TPtr = std::shared_ptr<TTestPortion>;
    using TConstPtr = std::shared_ptr<const TTestPortion>;

    ui64 PortionId;
    ui64 Start;
    ui64 Finish;
    ui64 BlobBytes;
    ui64 RawBytes;
    ui32 RecordsCount;
    NPortion::EProduced Produced;
    ui32 CompactionLevel;

    TTestPortion(const ui64 portionId, const ui64 start, const ui64 finish, const ui64 blobBytes, const ui32 recordsCount = 1,
        const NPortion::EProduced produced = NPortion::INSERTED, const ui32 compactionLevel = 0)
        : PortionId(portionId)
        , Start(start)
        , Finish(finish)
        , BlobBytes(blobBytes)
        , RawBytes(blobBytes)
        , RecordsCount(recordsCount)
        , Produced(produced)
        , CompactionLevel(compactionLevel)
    {
    }

    ui64 IndexKeyStart() const {
        return Start;
    }

    ui64 IndexKeyEnd() const {
        return Finish;
    }

    ui64 GetPortionId() const {
        return PortionId;
    }

    ui32 GetRecordsCount() const {
        return RecordsCount;
    }

    ui64 GetTotalBlobBytes() const {
        return BlobBytes;
    }

    ui64 GetTotalRawBytes() const {
        return RawBytes;
    }

    NPortion::EProduced GetProduced() const {
        return Produced;
    }

    ui32 GetCompactionLevel() const {
        return CompactionLevel;
    }

    void AddRuntimeFeature(const TPortionInfo::ERuntimeFeature /*feature*/) {};
};

using TTestAccumulator = Accumulator<ui64, TTestPortion>;
using TTestLastLevel = LastLevel<ui64, TTestPortion>;
using TTestMiddleLevel = MiddleLevel<ui64, TTestPortion>;
using TTestTiling = Tiling<ui64, TTestPortion>;

TTestPortion::TPtr MakePortion(const ui64 id, const ui64 start, const ui64 finish, const ui64 blobBytes) {
    return std::make_shared<TTestPortion>(id, start, finish, blobBytes);
}

const auto& NeverLocked() {
    static const auto fn = [](TTestPortion::TConstPtr) {
        return false;
    };
    return fn;
}

std::shared_ptr<IOptimizerPlannerConstructor> MakeTilingPlusPlusConstructor() {
    auto ctor = IOptimizerPlannerConstructor::BuildDefault("tiling++");
    UNIT_ASSERT(ctor);
    return ctor;
}

}   // namespace

Y_UNIT_TEST_SUITE(TilingCoreUnits) {
    Y_UNIT_TEST(AccumulatorReturnsSimpleCompactionTask) {
        TAccumulatorSettings settings;
        settings.Trigger.Bytes = 100;
        settings.Trigger.Portions = 100;
        settings.Compaction.Bytes = 150;
        settings.Compaction.Portions = 10;
        settings.OverloadPortions = 1000;

        TCounters counters;
        TTestAccumulator accumulator(settings, counters);
        const auto p1 = MakePortion(1, 10, 20, 60);
        const auto p2 = MakePortion(2, 30, 40, 60);
        const auto p3 = MakePortion(3, 50, 60, 60);

        accumulator.AddPortion(p1);
        accumulator.AddPortion(p2);
        accumulator.AddPortion(p3);

        const auto nextTask = accumulator.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(nextTask);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->Portions.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->Portions[0]->GetPortionId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->Portions[1]->GetPortionId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->Portions[2]->GetPortionId(), 3);
    }

    Y_UNIT_TEST(MiddleLevelReturnsMaxIntersectionRange) {
        TMiddleLevelSettings settings;
        settings.TriggerHeight = 2;
        settings.OverloadHeight = 4;

        TCounters counters;
        TTestMiddleLevel middle(settings, 2, counters);
        middle.RegisterRoutingWidth(1, 1);
        middle.AddPortion(MakePortion(1, 0, 10, 1000));
        middle.RegisterRoutingWidth(2, 1);
        middle.AddPortion(MakePortion(2, 5, 15, 1000));
        middle.RegisterRoutingWidth(3, 1);
        middle.AddPortion(MakePortion(3, 7, 12, 1000));
        middle.RegisterRoutingWidth(4, 1);
        middle.AddPortion(MakePortion(4, 20, 30, 1000));

        const auto task = middle.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 3);

        TVector<ui64> ids;
        for (const auto& p : task->Portions) {
            ids.push_back(p->GetPortionId());
        }
        Sort(ids.begin(), ids.end());
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ids[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(ids[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(ids[2], 3);
    }

    Y_UNIT_TEST(LastLevelReturnsCandidateWithIntersectedStablePortions) {
        TLastLevelSettings settings;
        settings.Compaction.Bytes = 1000;
        settings.Compaction.Portions = 10;
        settings.CandidatePortionsOverload = 3;

        TCounters counters;
        TTestLastLevel lastLevel(settings, counters);
        lastLevel.AddPortion(MakePortion(1, 0, 10, 100));
        lastLevel.AddPortion(MakePortion(2, 20, 30, 100));
        lastLevel.AddPortion(MakePortion(3, 5, 25, 100));

        const auto task = lastLevel.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions[0]->GetPortionId(), 3);

        TVector<ui64> ids;
        for (const auto& p : task->Portions) {
            ids.push_back(p->GetPortionId());
        }
        Sort(ids.begin(), ids.end());
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ids[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(ids[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(ids[2], 3);
    }

    Y_UNIT_TEST(TilingReturnsAccumulatorTask) {
        TTestTiling::TilingSettings settings;
        settings.AccumulatorSettings.Trigger.Bytes = 100;
        settings.AccumulatorSettings.Trigger.Portions = 100;
        settings.AccumulatorSettings.Compaction.Bytes = 150;
        settings.AccumulatorSettings.Compaction.Portions = 10;

        TCounters counters;
        TTestTiling tiling(settings, counters);
        tiling.AddPortion(MakePortion(1, 0, 1, 60));
        tiling.AddPortion(MakePortion(2, 2, 3, 60));
        tiling.AddPortion(MakePortion(3, 4, 5, 60));

        const auto nextTask = tiling.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(nextTask);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->TargetLevel, 0);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->Portions.size(), 3);
    }

    Y_UNIT_TEST(TilingChoosesAccumulatorMiddleAndLastLevelsIndependently) {
        TTestTiling::TilingSettings settings;
        settings.AccumulatorPortionSizeLimit = 100;
        settings.K = 10;
        settings.MiddleLevelCount = TILING_LAYERS_COUNT;
        settings.AccumulatorSettings.Trigger.Bytes = 100;
        settings.AccumulatorSettings.Trigger.Portions = 100;
        settings.AccumulatorSettings.Compaction.Bytes = 150;
        settings.AccumulatorSettings.Compaction.Portions = 10;
        settings.AccumulatorSettings.OverloadPortions = 1000;
        settings.LastLevelSettings.Compaction.Bytes = 10000;
        settings.LastLevelSettings.Compaction.Portions = 10;
        settings.LastLevelSettings.CandidatePortionsOverload = 100;
        settings.MiddleLevelSettings.TriggerHeight = 2;
        settings.MiddleLevelSettings.OverloadHeight = 4;

        TCounters counters;
        TTestTiling tiling(settings, counters);

        tiling.AddPortion(MakePortion(1, 0, 1, 60));
        tiling.AddPortion(MakePortion(2, 2, 3, 60));
        tiling.AddPortion(MakePortion(3, 4, 5, 60));

        auto task = tiling.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 3);
        {
            TVector<ui64> ids;
            for (const auto& p : task->Portions) {
                ids.push_back(p->GetPortionId());
            }
            Sort(ids.begin(), ids.end());
            UNIT_ASSERT_VALUES_EQUAL(ids[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(ids[1], 2);
            UNIT_ASSERT_VALUES_EQUAL(ids[2], 3);
        }

        tiling.RemovePortion(MakePortion(1, 0, 1, 60));
        tiling.RemovePortion(MakePortion(2, 2, 3, 60));
        tiling.RemovePortion(MakePortion(3, 4, 5, 60));

        tiling.AddPortion(MakePortion(10, 0, 1000, 1000));
        tiling.AddPortion(MakePortion(11, 100, 1100, 1000));
        tiling.AddPortion(MakePortion(12, 200, 1200, 1000));

        task = tiling.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 2);
        {
            TVector<ui64> ids;
            for (const auto& p : task->Portions) {
                ids.push_back(p->GetPortionId());
            }
            Sort(ids.begin(), ids.end());
            UNIT_ASSERT_VALUES_EQUAL(ids.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(ids[0], 10);
            UNIT_ASSERT_VALUES_EQUAL(ids[1], 11);
        }

        tiling.RemovePortion(MakePortion(10, 0, 1000, 1000));
        tiling.RemovePortion(MakePortion(11, 100, 1100, 1000));
        tiling.RemovePortion(MakePortion(12, 200, 1200, 1000));

        tiling.AddPortion(MakePortion(20, 0, 9, 1000));
        tiling.AddPortion(MakePortion(21, 20, 29, 1000));
        tiling.AddPortion(MakePortion(22, 5, 25, 1000));

        task = tiling.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(task);
        UNIT_ASSERT_VALUES_EQUAL(task->Portions.size(), 3);
        {
            TVector<ui64> ids;
            for (const auto& p : task->Portions) {
                ids.push_back(p->GetPortionId());
            }
            Sort(ids.begin(), ids.end());
            UNIT_ASSERT_VALUES_EQUAL(ids[0], 20);
            UNIT_ASSERT_VALUES_EQUAL(ids[1], 21);
            UNIT_ASSERT_VALUES_EQUAL(ids[2], 22);
        }
    }

    Y_UNIT_TEST(TilingInitialLoadKeepsThinPortionsOnLastLevelAfterShuffle) {
        TTestTiling::TilingSettings settings;
        settings.AccumulatorPortionSizeLimit = 100;
        settings.K = 2;
        settings.MiddleLevelCount = TILING_LAYERS_COUNT;
        settings.AccumulatorSettings.Trigger.Bytes = 1'000'000;
        settings.AccumulatorSettings.Trigger.Portions = 1'000'000;
        settings.AccumulatorSettings.Compaction.Bytes = 1'000'000;
        settings.AccumulatorSettings.Compaction.Portions = 1'000'000;
        settings.AccumulatorSettings.OverloadPortions = 1'000'000;
        settings.LastLevelSettings.Compaction.Bytes = 1'000'000;
        settings.LastLevelSettings.Compaction.Portions = 1'000'000;
        settings.LastLevelSettings.CandidatePortionsOverload = 1'000'000;
        settings.MiddleLevelSettings.TriggerHeight = 1'000'000;
        settings.MiddleLevelSettings.OverloadHeight = 1'000'000;

        TCounters counters;
        TTestTiling tiling(settings, counters);

        TVector<TTestPortion::TPtr> portions;
        portions.reserve(122);

        ui64 nextId = 1000;
        for (ui64 i = 0; i < 100; ++i) {
            const ui64 start = i * 20;
            portions.emplace_back(MakePortion(nextId++, start, start + 9, 1000));
        }

        for (ui64 i = 0; i < 20; ++i) {
            const ui64 startThinIndex = i * 5;
            const ui64 start = startThinIndex * 20;
            const ui64 finish = start + 5 * 20 + 9;
            portions.emplace_back(MakePortion(nextId++, start, finish, 1000));
        }

        portions.emplace_back(MakePortion(nextId++, 10'000, 10'009, 50));
        portions.emplace_back(MakePortion(nextId++, 10'020, 10'029, 70));

        std::mt19937_64 rng(42);
        std::shuffle(portions.begin(), portions.end(), rng);

        tiling.ModifyPortions(portions, {});

        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Portions.size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.WidthByPortionId.size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(tiling.Accumulator.Portions.size(), 2);

        ui64 widePortionsOutsideLastLevel = 0;
        ui64 accumulatorPortions = 0;
        for (const auto& [portionId, placement] : tiling.InternalLevel) {
            if (placement.Level == 0) {
                ++accumulatorPortions;
            } else if (placement.Level != 1) {
                ++widePortionsOutsideLastLevel;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(accumulatorPortions, 2);
        UNIT_ASSERT_VALUES_EQUAL(widePortionsOutsideLastLevel, 20);
    }

    Y_UNIT_TEST(TilingAgingPromotesPortionDownLevelByLevel) {
        TTestTiling::TilingSettings settings;
        // Disable accumulator routing entirely (no portion has < 0 bytes).
        settings.AccumulatorPortionSizeLimit = 0;
        settings.K = 2;
        settings.MiddleLevelCount = 5;   // allowed middle levels: 2, 3, 4
        settings.AccumulatorSettings.Trigger.Bytes = 1'000'000;
        settings.AccumulatorSettings.Trigger.Portions = 1'000'000;
        settings.AccumulatorSettings.Compaction.Bytes = 1'000'000;
        settings.AccumulatorSettings.Compaction.Portions = 1'000'000;
        settings.AccumulatorSettings.OverloadPortions = 1'000'000;
        settings.LastLevelSettings.Compaction.Bytes = 1'000'000;
        settings.LastLevelSettings.Compaction.Portions = 1'000'000;
        settings.LastLevelSettings.CandidatePortionsOverload = 1'000'000;
        settings.MiddleLevelSettings.TriggerHeight = 1'000'000;
        settings.MiddleLevelSettings.OverloadHeight = 1'000'000;
        settings.AgingSettings.Enabled = true;
        settings.AgingSettings.PromoteTime = TDuration::Seconds(60);
        settings.AgingSettings.MaxPortionPromotion = 100;

        TCounters counters;
        TTestTiling tiling(settings, counters);

        // 4 non-overlapping baseline portions land on LastLevel.Portions (measure=0).
        tiling.AddPortion(MakePortion(1, 0, 9, 1000));
        tiling.AddPortion(MakePortion(2, 100, 109, 1000));
        tiling.AddPortion(MakePortion(3, 200, 209, 1000));
        tiling.AddPortion(MakePortion(4, 300, 309, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Portions.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Candidates.size(), 0);

        // Keep one last-level compaction candidate pending so the useful metric stays non-zero-level and
        // the planner remains REGULAR. Without it the planner goes BORED (no work to do) and promotes
        // portions ignoring their timers, which would break the "no movement before timer" checks below.
        // It overlaps a single baseline (measure 1) and is removed before the removal-cleanup section.
        tiling.AddPortion(MakePortion(200, 0, 9, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(200).Level, 1);
        UNIT_ASSERT(tiling.LastLevel.CandidateIds.contains(200));

        // Wide portion overlaps all 4 baselines → measure=4.
        // With K=2: 1*2<=4 → L2, 2*2<=4 → L3, 4*2<=4? no. So measuredLevel=3.
        tiling.AddPortion(MakePortion(100, 0, 309, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(3).PortionById.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(3).WidthByPortionId.at(100), 4);
        UNIT_ASSERT(!tiling.MiddleLevels.at(2).PortionById.contains(100));
        UNIT_ASSERT(!tiling.MiddleLevels.at(4).PortionById.contains(100));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 3);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Width, 4);
        UNIT_ASSERT(tiling.InsertTimeByPortionId.contains(100));
        UNIT_ASSERT_VALUES_EQUAL(tiling.PortionsByTime.size(), 1);

        const TInstant insertTime = tiling.InsertTimeByPortionId.at(100);

        // Tick before expiry: nothing happens.
        tiling.PromoteExpiredPortions(insertTime + TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(3).PortionById.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 3);

        // Tick after expiry: demote L3 → L2 with fresh timer.
        const TInstant tick1 = insertTime + TDuration::Seconds(120);
        tiling.PromoteExpiredPortions(tick1);
        UNIT_ASSERT(!tiling.MiddleLevels.at(3).PortionById.contains(100));
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(3).WidthByPortionId.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(2).PortionById.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(2).WidthByPortionId.at(100), 4);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 2);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Width, 4);
        UNIT_ASSERT(tiling.InsertTimeByPortionId.contains(100));
        UNIT_ASSERT(tiling.InsertTimeByPortionId.at(100) >= tick1);
        UNIT_ASSERT_VALUES_EQUAL(tiling.PortionsByTime.size(), 1);

        // Tick again, before the L2 timer expires — no movement.
        tiling.PromoteExpiredPortions(tick1 + TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 2);

        // Tick after L2 timer expires: demote to L1 (LastLevel). L1 has no aging timer.
        const TInstant tick2 = tiling.InsertTimeByPortionId.at(100) + TDuration::Seconds(120);
        tiling.PromoteExpiredPortions(tick2);
        UNIT_ASSERT(!tiling.MiddleLevels.at(2).PortionById.contains(100));
        UNIT_ASSERT_VALUES_EQUAL(tiling.MiddleLevels.at(2).WidthByPortionId.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 1);
        UNIT_ASSERT(tiling.LastLevel.CandidateIds.contains(100));
        // Width on LastLevel must equal current measure of the portion (overlaps 4 baselines).
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.WidthByPortionId.at(100), 4);
        // Wide portion has measure=4 → enters Candidates, not Portions; candidate 200 (kept to keep the
        // planner busy) is the second candidate.
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Candidates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Portions.size(), 4);
        // No timer entry left for L1.
        UNIT_ASSERT(!tiling.InsertTimeByPortionId.contains(100));
        UNIT_ASSERT_VALUES_EQUAL(tiling.PortionsByTime.size(), 0);

        // Drop the busy-keeping candidate; the remaining checks exercise removal bookkeeping for 100.
        tiling.RemovePortion(MakePortion(200, 0, 9, 1000));
        UNIT_ASSERT(!tiling.LastLevel.CandidateIds.contains(200));

        // Further ticks are no-ops.
        tiling.PromoteExpiredPortions(tick2 + TDuration::Seconds(600));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 1);

        // Removing the portion cleans up its own bookkeeping (baselines remain).
        tiling.RemovePortion(MakePortion(100, 0, 309, 1000));
        UNIT_ASSERT(!tiling.InternalLevel.contains(100));
        UNIT_ASSERT(!tiling.PortionRegistry.contains(100));
        UNIT_ASSERT(!tiling.LastLevel.WidthByPortionId.contains(100));
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.WidthByPortionId.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Portions.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(tiling.LastLevel.Candidates.size(), 0);
    }

    Y_UNIT_TEST(TilingAccumulatorTaskTargetsLevelZero) {
        TTestTiling::TilingSettings settings;
        settings.AccumulatorPortionSizeLimit = 100;
        settings.AccumulatorSettings.Trigger.Bytes = 1;
        settings.AccumulatorSettings.Trigger.Portions = 1;
        settings.AccumulatorSettings.Compaction.Bytes = 1000;
        settings.AccumulatorSettings.Compaction.Portions = 100;

        TCounters counters;
        TTestTiling tiling(settings, counters);
        tiling.AddPortion(MakePortion(1, 0, 1, 50));
        tiling.AddPortion(MakePortion(2, 2, 3, 50));
        tiling.AddPortion(MakePortion(3, 4, 5, 50));

        const auto nextTask = tiling.GetNextOptimizationTask(NeverLocked());
        UNIT_ASSERT(nextTask);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->TargetLevel, 0);
        UNIT_ASSERT_VALUES_EQUAL(nextTask->Portions.size(), 3);
    }
}

Y_UNIT_TEST_SUITE(TilingCompactionState) {
    // Accumulator-only settings where the useful-metric level maps 1:1 to the accumulator portion
    // count: Normalize(1, 10, count) gives level == count for count in [1, 10], and level 10
    // (critical) once count >= 10.
    static TTestTiling::TilingSettings MakeCountPrioritySettings(const bool compatibilityMode) {
        TTestTiling::TilingSettings settings;
        settings.EnableCompatibilityMode = compatibilityMode;
        settings.AccumulatorPortionSizeLimit = 1'000'000;   // small portions land in the accumulator
        settings.K = 10;
        settings.AccumulatorSettings.Trigger.Portions = 1;
        settings.AccumulatorSettings.OverloadPortions = 10;
        settings.AccumulatorSettings.Trigger.Bytes = 1'000'000'000;
        settings.AccumulatorSettings.Compaction.Portions = 1'000'000;
        settings.AccumulatorSettings.Compaction.Bytes = 1'000'000'000;
        settings.LastLevelSettings.CandidatePortionsOverload = 1'000'000;
        settings.MiddleLevelSettings.TriggerHeight = 1'000'000;
        settings.MiddleLevelSettings.OverloadHeight = 2'000'000;
        settings.AgingSettings.Enabled = false;
        return settings;
    }

    // Accumulator-only settings with a wide overload band, so a large backlog maps to a high
    // useful-level and IncPercent(10) yields a non-truncated, proportional headroom.
    // Normalize(1, 901, count): range = 900, level = 1 + (count - 1) * 9 / 900.
    static TTestTiling::TilingSettings MakeWideBandSettings() {
        auto settings = MakeCountPrioritySettings(/*compatibilityMode=*/true);
        settings.AccumulatorSettings.Trigger.Portions = 1;
        settings.AccumulatorSettings.OverloadPortions = 901;
        return settings;
    }

    static TVector<TTestPortion::TPtr> MakeAccumulatorPortions(const ui64 count) {
        TVector<TTestPortion::TPtr> portions;
        for (ui64 i = 0; i < count; ++i) {
            portions.push_back(MakePortion(i + 1, i * 10, i * 10 + 1, 10));
        }
        return portions;
    }

    // 1) Compatibility state entry conditions.
    Y_UNIT_TEST(CompatibilityEntryRequiresFlagAndOverload) {
        // Flag on + overloaded from start -> compatibility, overload suppressed.
        {
            TCounters counters;
            TTestTiling tiling(MakeCountPrioritySettings(/*compatibilityMode=*/true), counters);
            tiling.ModifyPortions(MakeAccumulatorPortions(10), {});
            UNIT_ASSERT(tiling.DoGetUsefulMetric().IsCritical());
            UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);
            UNIT_ASSERT(!tiling.IsOverloaded());
        }
        // Flag off + overloaded from start -> stays regular and reports overloaded.
        {
            TCounters counters;
            TTestTiling tiling(MakeCountPrioritySettings(/*compatibilityMode=*/false), counters);
            tiling.ModifyPortions(MakeAccumulatorPortions(10), {});
            UNIT_ASSERT(tiling.DoGetUsefulMetric().IsCritical());
            UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
            UNIT_ASSERT(tiling.IsOverloaded());
        }
        // Flag on + not overloaded from start -> regular, not overloaded.
        {
            TCounters counters;
            TTestTiling tiling(MakeCountPrioritySettings(/*compatibilityMode=*/true), counters);
            tiling.ModifyPortions(MakeAccumulatorPortions(2), {});
            UNIT_ASSERT(!tiling.DoGetUsefulMetric().IsCritical());
            UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
            UNIT_ASSERT(!tiling.IsOverloaded());
        }
    }

    // 2) Compatibility recovery: the overload ceiling ratchets down with the load (IncPercent(10)
    //    headroom above current useful, lowered only); a spike back above the ratcheted ceiling
    //    re-reports overload; once the load drops far enough that the ceiling falls below critical
    //    we return to regular. Uses the wide overload band so the ceiling stays critical across the
    //    ratchet/spike cycle (the tiny-band scale truncates IncPercent to a no-op).
    Y_UNIT_TEST(CompatibilityRecoveryRatchetsAndDetectsSpike) {
        TCounters counters;
        TTestTiling tiling(MakeWideBandSettings(), counters);

        // 9000 portions -> useful level 90, ceiling IncPercent(10) = (99, 9900).
        auto portions = MakeAccumulatorPortions(9000);
        tiling.ModifyPortions(portions, {});
        UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);
        UNIT_ASSERT(!tiling.IsOverloaded());

        const TInstant now = TInstant::Now();

        // Load drops to ~5000 (level 50): ceiling ratchets down to (55, 5500), still critical ->
        // stays compatibility, not overloaded.
        for (ui64 i = 5000; i < 9000; ++i) {
            tiling.RemovePortion(portions[i]);
        }
        tiling.DoActualize(now);
        UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);
        UNIT_ASSERT(!tiling.IsOverloaded());

        // Spike back to 9000 (level 90, above the ratcheted ceiling level 55) -> overloaded again.
        for (ui64 i = 5000; i < 9000; ++i) {
            tiling.AddPortion(portions[i]);
        }
        UNIT_ASSERT(tiling.DoGetUsefulMetric().IsCritical());
        UNIT_ASSERT(tiling.IsOverloaded());
        UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);

        // Recover well below the overload band (to 500, level 5): ceiling drops below critical ->
        // back to regular.
        for (ui64 i = 500; i < 9000; ++i) {
            tiling.RemovePortion(portions[i]);
        }
        tiling.DoActualize(now);
        UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
        UNIT_ASSERT(!tiling.IsOverloaded());
    }

    // 3) Compatibility ceiling gives ~10% headroom (IncPercent(10)), not the near-zero margin that
    //    Inc() produced once the level stopped tracking single-portion deltas. Reproduces the cluster
    //    case where a 65k-portion backlog re-reported overload after only a handful of extra writes.
    Y_UNIT_TEST(CompatibilityCeilingToleratesProportionalGrowth) {
        TCounters counters;
        TTestTiling tiling(MakeWideBandSettings(), counters);

        // Enter compatibility with a large backlog: level 90 -> ceiling IncPercent(10) = (99, 9900).
        auto portions = MakeAccumulatorPortions(9990);
        tiling.ModifyPortions(TVector<TTestPortion::TPtr>(portions.begin(), portions.begin() + 9000), {});
        UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);
        UNIT_ASSERT(tiling.DoGetUsefulMetric().IsCritical());
        UNIT_ASSERT(!tiling.IsOverloaded());

        // Grow +5% (to 9450): still under the ceiling level -> not overloaded. With the old Inc(),
        // the stale-weight ceiling would already have tripped here.
        for (ui64 i = 9000; i < 9450; ++i) {
            tiling.AddPortion(portions[i]);
        }
        UNIT_ASSERT(!tiling.IsOverloaded());
        UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);

        // Grow past +10% (to 9990, level 100 > ceiling level 99) -> overload re-reported.
        for (ui64 i = 9450; i < 9990; ++i) {
            tiling.AddPortion(portions[i]);
        }
        UNIT_ASSERT(tiling.IsOverloaded());
        UNIT_ASSERT(tiling.State == TTestTiling::EState::COMPATIBILITY);
    }

    // Settings for the aging/promotion tests: accumulator disabled, routing purely by measure.
    static TTestTiling::TilingSettings MakeAgingSettings() {
        TTestTiling::TilingSettings settings;
        settings.AccumulatorPortionSizeLimit = 0;
        settings.K = 2;
        settings.MiddleLevelCount = 5;
        settings.AccumulatorSettings.Trigger.Portions = 1'000'000;
        settings.AccumulatorSettings.OverloadPortions = 2'000'000;
        settings.AccumulatorSettings.Compaction.Portions = 1'000'000;
        settings.AccumulatorSettings.Compaction.Bytes = 1'000'000'000;
        settings.LastLevelSettings.Compaction.Portions = 1'000'000;
        settings.LastLevelSettings.Compaction.Bytes = 1'000'000'000;
        settings.LastLevelSettings.CandidatePortionsOverload = 1'000'000;
        settings.MiddleLevelSettings.TriggerHeight = 1'000'000;
        settings.MiddleLevelSettings.OverloadHeight = 2'000'000;
        settings.AgingSettings.Enabled = true;
        settings.AgingSettings.PromoteTime = TDuration::Seconds(60);
        settings.AgingSettings.MaxPortionPromotion = 100;
        return settings;
    }

    // 3) Bored mode: with no optimization task the planner goes bored and pushes portions down even
    //    before their promote timer expires; a returning task switches it back to regular.
    Y_UNIT_TEST(BoredModePromotesPortionsIgnoringTimer) {
        TCounters counters;
        TTestTiling tiling(MakeAgingSettings(), counters);

        // 4 non-overlapping baselines on the last level (no timer).
        tiling.AddPortion(MakePortion(1, 0, 9, 1000));
        tiling.AddPortion(MakePortion(2, 100, 109, 1000));
        tiling.AddPortion(MakePortion(3, 200, 209, 1000));
        tiling.AddPortion(MakePortion(4, 300, 309, 1000));
        // Wide portion overlaps all 4 -> measure 4 -> middle level 3 (K=2), with a timer.
        tiling.AddPortion(MakePortion(100, 0, 309, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 3);
        const TInstant insertTime = tiling.InsertTimeByPortionId.at(100);

        // Nothing to compact -> bored.
        UNIT_ASSERT(!tiling.GetNextOptimizationTask(NeverLocked()));
        tiling.DoActualize(insertTime);
        UNIT_ASSERT(tiling.State == TTestTiling::EState::BORED);

        // Bored actualize promotes the wide portion despite the timer not being expired.
        tiling.DoActualize(insertTime + TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 2);
        UNIT_ASSERT(tiling.State == TTestTiling::EState::BORED);

        // A last-level candidate (overlaps exactly one baseline) gives the planner work again.
        tiling.AddPortion(MakePortion(200, 0, 9, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(200).Level, 1);
        UNIT_ASSERT(tiling.GetNextOptimizationTask(NeverLocked()));
        tiling.DoActualize(insertTime);
        UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
    }

    // 4) Regular mode promotes portions down only after the promote timer expires.
    Y_UNIT_TEST(RegularModePromotesOnlyAfterPromoteTime) {
        TCounters counters;
        TTestTiling tiling(MakeAgingSettings(), counters);

        tiling.AddPortion(MakePortion(1, 0, 9, 1000));
        tiling.AddPortion(MakePortion(2, 100, 109, 1000));
        tiling.AddPortion(MakePortion(3, 200, 209, 1000));
        tiling.AddPortion(MakePortion(4, 300, 309, 1000));
        tiling.AddPortion(MakePortion(100, 0, 309, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 3);
        UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
        const TInstant insertTime = tiling.InsertTimeByPortionId.at(100);

        // Keep a pending last-level compaction so the planner never goes bored (which would promote the
        // wide portion immediately, ignoring its timer).
        tiling.AddPortion(MakePortion(200, 0, 9, 1000));
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(200).Level, 1);
        UNIT_ASSERT(tiling.GetNextOptimizationTask(NeverLocked()));

        // Before the promote time elapses: no movement.
        tiling.DoActualize(insertTime + TDuration::Seconds(30));
        UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 3);

        // After the promote time: demote one level.
        tiling.DoActualize(insertTime + TDuration::Seconds(90));
        UNIT_ASSERT(tiling.State == TTestTiling::EState::REGULAR);
        UNIT_ASSERT_VALUES_EQUAL(tiling.InternalLevel.at(100).Level, 2);
    }
}

Y_UNIT_TEST_SUITE(TilingPlusPlusParallelCompaction) {
    Y_UNIT_TEST(CompactionThreadsRejectsZero) {
        auto ctor = MakeTilingPlusPlusConstructor();
        NJson::TJsonValue json;
        json["compaction_threads"] = 0;
        const auto status = ctor->DeserializeFromJson(json);
        UNIT_ASSERT(status.IsFail());
    }

    Y_UNIT_TEST(CompactionThreadsRoundTripInProtoJson) {
        auto ctor = MakeTilingPlusPlusConstructor();
        NJson::TJsonValue json;
        json["compaction_threads"] = 3;
        json["k"] = 10;
        UNIT_ASSERT_C(ctor->DeserializeFromJson(json).IsSuccess(), "deserialize");

        NKikimrSchemeOp::TCompactionPlannerConstructorContainer proto;
        ctor->SerializeToProto(proto);
        UNIT_ASSERT(proto.HasTiling());

        NJson::TJsonValue restoredJson;
        UNIT_ASSERT(NJson::ReadJsonFastTree(proto.GetTiling().GetJson(), &restoredJson));
        UNIT_ASSERT(restoredJson.Has("compaction_threads"));
        UNIT_ASSERT_VALUES_EQUAL(restoredJson["compaction_threads"].GetUInteger(), 3);
    }

    Y_UNIT_TEST(CompactionThreadsDefaultOmittedInProtoJson) {
        auto ctor = MakeTilingPlusPlusConstructor();
        UNIT_ASSERT_C(ctor->DeserializeFromJson(NJson::TJsonValue(NJson::JSON_MAP)).IsSuccess(), "deserialize");

        NKikimrSchemeOp::TCompactionPlannerConstructorContainer proto;
        ctor->SerializeToProto(proto);
        UNIT_ASSERT(proto.HasTiling());

        NJson::TJsonValue restoredJson;
        UNIT_ASSERT(NJson::ReadJsonFastTree(proto.GetTiling().GetJson(), &restoredJson));
        UNIT_ASSERT(restoredJson.Has("compaction_threads"));
        UNIT_ASSERT_VALUES_EQUAL(restoredJson["compaction_threads"].GetUInteger(), 2);
    }
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
