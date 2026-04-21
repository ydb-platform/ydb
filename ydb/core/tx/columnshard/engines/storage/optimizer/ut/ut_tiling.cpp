#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/tiling.h>

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

    TTestPortion(
        const ui64 portionId,
        const ui64 start,
        const ui64 finish,
        const ui64 blobBytes,
        const ui32 recordsCount = 1,
        const NPortion::EProduced produced = NPortion::INSERTED)
        : PortionId(portionId)
        , Start(start)
        , Finish(finish)
        , BlobBytes(blobBytes)
        , RawBytes(blobBytes)
        , RecordsCount(recordsCount)
        , Produced(produced) {
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
};

using TTestAccumulator = Accumulator<ui64, TTestPortion>;
using TTestLastLevel = LastLevel<ui64, TTestPortion>;
using TTestMiddleLevel = MiddleLevel<ui64, TTestPortion>;
using TTestTiling = Tiling<ui64, TTestPortion>;

TTestPortion::TConstPtr MakePortion(const ui64 id, const ui64 start, const ui64 finish, const ui64 blobBytes) {
    return std::make_shared<TTestPortion>(id, start, finish, blobBytes);
}

const auto& NeverLocked() {
    static const auto fn = [](TTestPortion::TConstPtr) {
        return false;
    };
    return fn;
}

} // namespace

Y_UNIT_TEST_SUITE(TilingCoreUnits) {

    Y_UNIT_TEST(AccumulatorReturnsSimpleCompactionTask) {
        TTestAccumulator::AccumulatorSettings settings;
        settings.Trigger.Bytes = 100;
        settings.Trigger.Portions = 100;
        settings.Compaction.Bytes = 150;
        settings.Compaction.Portions = 10;
        settings.Overload.Bytes = 1000;
        settings.Overload.Portions = 1000;

        TCounters counters;
        TTestAccumulator accumulator(settings, counters);
        const auto p1 = MakePortion(1, 10, 20, 60);
        const auto p2 = MakePortion(2, 30, 40, 60);
        const auto p3 = MakePortion(3, 50, 60, 60);

        accumulator.AddPortion(p1);
        accumulator.AddPortion(p2);
        accumulator.AddPortion(p3);

        const auto tasks = accumulator.GetOptimizationTasks(NeverLocked());
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions[0]->GetPortionId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions[1]->GetPortionId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions[2]->GetPortionId(), 3);
    }

    Y_UNIT_TEST(MiddleLevelReturnsMaxIntersectionRange) {
        TTestMiddleLevel::MiddleLevelSettings settings;
        settings.TriggerHight = 2;
        settings.OverloadHight = 4;

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

        const auto tasks = middle.GetOptimizationTasks(NeverLocked());
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions.size(), 3);

        TVector<ui64> ids;
        for (const auto& p : tasks[0].Portions) {
            ids.push_back(p->GetPortionId());
        }
        Sort(ids.begin(), ids.end());
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ids[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(ids[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(ids[2], 3);
    }

    Y_UNIT_TEST(LastLevelReturnsCandidateWithIntersectedStablePortions) {
        TTestLastLevel::LastLevelSettings settings;
        settings.Compaction.Bytes = 1000;
        settings.Compaction.Portions = 10;
        settings.CandidatePortionsOverload = 3;

        TCounters counters;
        TTestLastLevel lastLevel(settings, counters);
        lastLevel.AddPortion(MakePortion(1, 0, 10, 100));
        lastLevel.AddPortion(MakePortion(2, 20, 30, 100));
        lastLevel.AddPortion(MakePortion(3, 5, 25, 100));

        const auto tasks = lastLevel.GetOptimizationTasks(NeverLocked());
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions[0]->GetPortionId(), 3);

        TVector<ui64> ids;
        for (const auto& p : tasks[0].Portions) {
            ids.push_back(p->GetPortionId());
        }
        Sort(ids.begin(), ids.end());
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ids[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(ids[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(ids[2], 3);
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
        settings.AccumulatorSettings.Overload.Bytes = 1000;
        settings.AccumulatorSettings.Overload.Portions = 1000;
        settings.LastLevelSettings.Compaction.Bytes = 1000;
        settings.LastLevelSettings.Compaction.Portions = 10;
        settings.LastLevelSettings.CandidatePortionsOverload = 100;
        settings.MiddleLevelSettings.TriggerHight = 2;
        settings.MiddleLevelSettings.OverloadHight = 4;

        TTestTiling tiling(settings);

        tiling.AddPortion(MakePortion(1, 0, 1, 60));
        tiling.AddPortion(MakePortion(2, 2, 3, 60));
        tiling.AddPortion(MakePortion(3, 4, 5, 60));

        auto tasks = tiling.GetOptimizationTasks(NeverLocked());
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions.size(), 3);
        {
            TVector<ui64> ids;
            for (const auto& p : tasks[0].Portions) {
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

        tasks = tiling.GetOptimizationTasks(NeverLocked());
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions.size(), 3);
        {
            TVector<ui64> ids;
            for (const auto& p : tasks[0].Portions) {
                ids.push_back(p->GetPortionId());
            }
            Sort(ids.begin(), ids.end());
            UNIT_ASSERT_VALUES_EQUAL(ids[0], 10);
            UNIT_ASSERT_VALUES_EQUAL(ids[1], 11);
            UNIT_ASSERT_VALUES_EQUAL(ids[2], 12);
        }

        tiling.RemovePortion(MakePortion(10, 0, 1000, 1000));
        tiling.RemovePortion(MakePortion(11, 100, 1100, 1000));
        tiling.RemovePortion(MakePortion(12, 200, 1200, 1000));

        tiling.AddPortion(MakePortion(20, 0, 9, 1000));
        tiling.AddPortion(MakePortion(21, 20, 29, 1000));
        tiling.AddPortion(MakePortion(22, 5, 25, 1000));

        tasks = tiling.GetOptimizationTasks(NeverLocked());
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks[0].Portions.size(), 3);
        {
            TVector<ui64> ids;
            for (const auto& p : tasks[0].Portions) {
                ids.push_back(p->GetPortionId());
            }
            Sort(ids.begin(), ids.end());
            UNIT_ASSERT_VALUES_EQUAL(ids[0], 20);
            UNIT_ASSERT_VALUES_EQUAL(ids[1], 21);
            UNIT_ASSERT_VALUES_EQUAL(ids[2], 22);
        }
    }
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
