#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>

#include <reader/common_reader/iterator/fetch_steps.h>
#include <reader/common_reader/iterator/fetching.h>
#include <reader/simple_reader/iterator/fetching.h>
#include <scheme/versions/snapshot_scheme.h>

#include <barrier>
#include <thread>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

Y_UNIT_TEST_SUITE(TestScriptStats) {
    std::shared_ptr<ISnapshotSchema> MakeTestSchema(THashMap<ui32, NTable::TColumn> columns, const std::vector<ui32> pkIds = { 0 }) {
        for (ui64 i = 0; i < pkIds.size(); ++i) {
            TValidator::CheckNotNull(columns.FindPtr(pkIds[i]))->KeyOrder = i;
        }

        auto cache = std::make_shared<TSchemaObjectsCache>();
        TIndexInfo info = TIndexInfo::BuildDefault(1, TTestStoragesManager::GetInstance(), columns, pkIds);
        return std::make_shared<TSnapshotSchema>(cache->GetOrInsertIndexInfo(std::move(info)), TSnapshot(1, 1));
    }

    Y_UNIT_TEST(StepStatisticsUseMergedScript) {
        using EStage = NArrow::NSSA::IMemoryCalculationPolicy::EStage;
        auto builder = NCommon::TFetchingScriptBuilder::MakeForTests(
            MakeTestSchema({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 1, NTable::TColumn("c1", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 2, NTable::TColumn("c2", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } }));
        builder.AddFetchingStep(std::vector<ui32>{ 1 }, EStage::Fetching);
        builder.AddFetchingStep(std::vector<ui32>{ 1, 2 }, EStage::Fetching);
        builder.AddAssembleStep(std::vector<ui32>{ 1, 2 }, "", EStage::Fetching, false);
        builder.AddStep(std::make_shared<NSimple::TDeletionFilter>());
        builder.AddStep(std::make_shared<NSimple::TDeletionFilter>());

        const NKikimr::NColumnShard::TConcreteScanCounters counters(NKikimr::NColumnShard::TScanCounters("TestScript"), nullptr);
        auto script = std::move(builder).Build(&counters);
        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 5);
        for (ui32 i = 0; i < stats.size(); ++i) {
            const auto step = script->GetStep(i);
            UNIT_ASSERT_VALUES_EQUAL(stats[i].Name, step->GetName());
            UNIT_ASSERT(step->GetStepStatsHandle());
        }
        const auto& fetchedColumns = std::get<NKikimr::NColumnShard::TFetchColumnsStepDetails>(stats[1].Details).Columns;
        UNIT_ASSERT_VALUES_EQUAL(fetchedColumns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fetchedColumns[0].Id, 1);
        UNIT_ASSERT_VALUES_EQUAL(fetchedColumns[1].Id, 2);

        const auto& assembledColumns = std::get<NKikimr::NColumnShard::TAssembleStepDetails>(stats[2].Details).Columns;
        const std::vector<NKikimr::NColumnShard::TStepColumn> expectedColumns = { { 1, "c1" }, { 2, "c2" } };
        UNIT_ASSERT(assembledColumns == expectedColumns);

        const auto handle = script->GetStep(4)->GetStepStatsHandle();
        counters.RecordStepExecution(*handle, TDuration::MicroSeconds(1), 10, 5, std::nullopt, std::nullopt);
        const auto updated = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(updated[3].Statistics.Calls, 0);
        UNIT_ASSERT_VALUES_EQUAL(updated[4].Statistics.Calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(updated[4].Statistics.OutputRows.Sum, 5);
    }

    Y_UNIT_TEST(StepDetailsMergeAcrossScripts) {
        using namespace NKikimr::NColumnShard;
        const TConcreteScanCounters counters(TScanCounters("TestScript"), nullptr);
        counters.RegisterSteps({ { "step", TAssembleStepDetails{ .Columns = { { 1, "c1" }, { 2, "c2" } } } } });
        const auto before = counters.GetStepStats();
        counters.RegisterSteps({ { "step", TAssembleStepDetails{ .Columns = { { 2, "c2" }, { 3, "c3" } } } } });

        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(before.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TAssembleStepDetails>(before.front().Details).Columns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.front().Name, "step");
        const std::vector<TStepColumn> assembledColumns = { { 1, "c1" }, { 2, "c2" }, { 3, "c3" } };
        UNIT_ASSERT(std::get<TAssembleStepDetails>(stats.front().Details).Columns == assembledColumns);
    }

    Y_UNIT_TEST(StepDetailsMergeResultBounds) {
        using namespace NKikimr::NColumnShard;
        const TConcreteScanCounters counters(TScanCounters("TestScript"), nullptr);
        counters.RegisterSteps({ { "result", TBuildResultStepDetails{} } });
        counters.RegisterSteps({ { "result", TBuildResultStepDetails{ .PageBounds = TBuildResultStepDetails::TPageBounds{ 1, 1, 5, 5 } } } });
        counters.RegisterSteps({ { "result", TBuildResultStepDetails{ .PageBounds = TBuildResultStepDetails::TPageBounds{ 3, 3, 2, 2 } } } });
        counters.RegisterSteps({ { "result", TBuildResultStepDetails{} } });

        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 1);
        const auto& bounds = std::get<TBuildResultStepDetails>(stats.front().Details).PageBounds;
        UNIT_ASSERT(bounds);
        UNIT_ASSERT_VALUES_EQUAL(bounds->MinStartIndex, 1);
        UNIT_ASSERT_VALUES_EQUAL(bounds->MaxStartIndex, 3);
        UNIT_ASSERT_VALUES_EQUAL(bounds->MinRecordsCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(bounds->MaxRecordsCount, 5);
    }

    Y_UNIT_TEST(StepDetailsReserveMemory) {
        using namespace NKikimr::NColumnShard;
        using EStage = NArrow::NSSA::IMemoryCalculationPolicy::EStage;
        auto builder = NCommon::TFetchingScriptBuilder::MakeForTests(
            MakeTestSchema({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } }));
        const NCommon::TColumnsSetIds columns(std::vector<ui32>{ 0 });
        auto reserve = std::make_shared<NCommon::TAllocateMemoryStep>(columns, NCommon::EMemType::Blob, EStage::Filter);
        reserve->AddAllocation(columns, NCommon::EMemType::Raw);
        reserve->AddAllocation(columns, NCommon::EMemType::RawSequential);
        builder.AddStep(reserve);

        const TConcreteScanCounters counters(TScanCounters("TestScript"), nullptr);
        const auto script = std::move(builder).Build(&counters);
        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 1);
        auto details = std::get<TReserveMemoryStepDetails>(stats.front().Details);
        UNIT_ASSERT_VALUES_EQUAL(details.Stages.size(), 1);
        const std::vector<TString> expectedTypes = { "Blob", "Raw", "RawSequential" };
        UNIT_ASSERT(details.Stages.front().MemoryTypes == expectedTypes);
        UNIT_ASSERT(!details.RequestedBytes);

        TReserveMemoryStepDetails measured;
        measured.RequestedBytes = 0;
        details.Merge(measured);
        UNIT_ASSERT(details.RequestedBytes);
        UNIT_ASSERT_VALUES_EQUAL(*details.RequestedBytes, 0);
        measured.RequestedBytes = 7;
        details.Merge(measured);
        measured.RequestedBytes = 3;
        details.Merge(measured);
        details.Merge(TReserveMemoryStepDetails{});
        UNIT_ASSERT_VALUES_EQUAL(*details.RequestedBytes, 10);
        UNIT_ASSERT(details.Stages.front().MemoryTypes == expectedTypes);
    }

    Y_UNIT_TEST(StepDetailsConcurrentUpdates) {
        using namespace NKikimr::NColumnShard;
        const TConcreteScanCounters counters(TScanCounters("TestScript"), nullptr);
        const auto handles = counters.RegisterSteps({ { "reserve", TReserveMemoryStepDetails{} } });
        const auto unmeasured = counters.GetStepStats();
        const auto recordBytes = [&](ui64 bytes) {
            counters.UpdateStepDetails<TReserveMemoryStepDetails>(handles.front(), [bytes](TReserveMemoryStepDetails& details) {
                details.RequestedBytes = details.RequestedBytes.value_or(0) + bytes;
            });
        };
        recordBytes(0);
        const auto measuredZero = counters.GetStepStats();

        constexpr ui64 Iterations = 100;
        const auto recordRequests = [&](ui64 bytes) {
            for (ui64 i = 0; i < Iterations; ++i) {
                recordBytes(bytes);
            }
        };
        std::thread worker(recordRequests, 3);
        recordRequests(2);
        worker.join();

        const auto stats = counters.GetStepStats();
        UNIT_ASSERT(!std::get<TReserveMemoryStepDetails>(unmeasured.front().Details).RequestedBytes);
        const auto zero = std::get<TReserveMemoryStepDetails>(measuredZero.front().Details).RequestedBytes;
        UNIT_ASSERT(zero);
        UNIT_ASSERT_VALUES_EQUAL(*zero, 0);
        const auto total = std::get<TReserveMemoryStepDetails>(stats.front().Details).RequestedBytes;
        UNIT_ASSERT(total);
        UNIT_ASSERT_VALUES_EQUAL(*total, 5 * Iterations);
    }

    Y_UNIT_TEST(StepStatisticsAggregateAcrossCopies) {
        const NKikimr::NColumnShard::TConcreteScanCounters counters(NKikimr::NColumnShard::TScanCounters("TestScript"), nullptr);
        const auto copy = counters;
        const auto first = counters.RegisterSteps({ { "DELETION" }, { "DELETION" } });
        const auto second = copy.RegisterSteps({ { "DELETION" }, { "DELETION" } });
        UNIT_ASSERT_VALUES_EQUAL(first.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(second.size(), 2);

        counters.RecordStepExecution(first[0], TDuration::MicroSeconds(2), 0, std::nullopt, 30, 10);
        counters.RecordStepExecution(first[1], TDuration::MicroSeconds(3), std::nullopt, 0, std::nullopt, std::nullopt);
        const auto before = counters.GetStepStats();
        copy.RecordStepExecution(second[0], TDuration::MicroSeconds(4), std::nullopt, std::nullopt, 5, 0);
        copy.AddStepWait(second[0], TDuration::MicroSeconds(11));
        copy.AddStepReadBytes(second[1], 13);

        counters.CountersForStep("DELETION").RawBytesRead->Add(17);
        UNIT_ASSERT_VALUES_EQUAL(counters.ReadStepsCounters().at("DELETION").RawBytesRead, 17);

        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].Name, "DELETION");
        UNIT_ASSERT_VALUES_EQUAL(stats[1].Name, "DELETION");
        const auto& firstStats = stats[0].Statistics;
        UNIT_ASSERT_VALUES_EQUAL(firstStats.Calls, 2);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.ActiveTime.MicroSeconds(), 6);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.WaitTime.MicroSeconds(), 11);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.InputRows.Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.InputRows.Sum, 0);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.OutputRows.Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.InputBytes.Count, 2);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.InputBytes.Sum, 35);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.OutputBytes.Count, 2);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.OutputBytes.Sum, 10);
        UNIT_ASSERT_VALUES_EQUAL(firstStats.ReadBytes.Count, 0);
        const auto& secondStats = stats[1].Statistics;
        UNIT_ASSERT_VALUES_EQUAL(secondStats.Calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(secondStats.OutputRows.Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(secondStats.OutputRows.Sum, 0);
        UNIT_ASSERT_VALUES_EQUAL(secondStats.ReadBytes.Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(secondStats.ReadBytes.Sum, 13);

        UNIT_ASSERT_VALUES_EQUAL(before.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(before[0].Statistics.Calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(before[0].Statistics.WaitTime.MicroSeconds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(before[1].Statistics.ReadBytes.Count, 0);
    }

    Y_UNIT_TEST(StepStatisticsPreserveOrderAndOldHandles) {
        const NKikimr::NColumnShard::TConcreteScanCounters counters(NKikimr::NColumnShard::TScanCounters("TestScript"), nullptr);
        UNIT_ASSERT(counters.RegisterSteps({}).empty());
        const auto first = counters.RegisterSteps({ { "DELETION" }, { "SNAPSHOT" } });
        UNIT_ASSERT_VALUES_EQUAL(first.size(), 2);
        const auto reverse = counters.RegisterSteps({ { "SNAPSHOT" }, { "DELETION" } });
        UNIT_ASSERT_VALUES_EQUAL(reverse.size(), 2);
        counters.AddStepReadBytes(first[0], 1);
        counters.AddStepReadBytes(first[1], 2);
        counters.AddStepReadBytes(reverse[0], 4);
        counters.AddStepReadBytes(reverse[1], 8);

        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 3);
        std::vector<TString> firstOrder;
        std::vector<TString> reverseOrder;
        for (const auto& step : stats) {
            const ui64 bytes = step.Statistics.ReadBytes.Sum;
            if (bytes & 3) {
                firstOrder.push_back(step.Name);
            }
            if (bytes & 12) {
                reverseOrder.push_back(step.Name);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(firstOrder, (std::vector<TString>{ "DELETION", "SNAPSHOT" }));
        UNIT_ASSERT_VALUES_EQUAL(reverseOrder, (std::vector<TString>{ "SNAPSHOT", "DELETION" }));

        const auto subsequence = counters.RegisterSteps({ { "SNAPSHOT" }, { "DELETION" } });
        UNIT_ASSERT_VALUES_EQUAL(subsequence.size(), 2);
        counters.AddStepReadBytes(subsequence[1], 16);
        const auto after = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(after.size(), 3);
        for (size_t i = 0; i < stats.size(); ++i) {
            const ui64 bytes = stats[i].Statistics.ReadBytes.Sum;
            UNIT_ASSERT_VALUES_EQUAL(after[i].Statistics.ReadBytes.Sum, bytes + ((bytes & 8) ? 16 : 0));
        }
    }

    Y_UNIT_TEST(StepStatisticsConcurrentRegistrationAndUpdates) {
        constexpr size_t iterations = 100;
        const NKikimr::NColumnShard::TConcreteScanCounters counters(NKikimr::NColumnShard::TScanCounters("TestScript"), nullptr);
        const auto copy = counters;
        std::barrier start(2);
        const auto record = [&](const auto& scanCounters, bool& consistent) {
            start.arrive_and_wait();
            for (size_t i = 0; i < iterations; ++i) {
        const auto handles = scanCounters.RegisterSteps({ { "DELETION" } });
                if (handles.size() != 1) {
                    consistent = false;
                    return;
                }
                scanCounters.RecordStepExecution(handles.front(), TDuration::MicroSeconds(1), 1, std::nullopt, std::nullopt, std::nullopt);
                const auto stats = scanCounters.GetStepStats();
                if (stats.size() != 1 || stats.front().Statistics.Calls != stats.front().Statistics.InputRows.Count) {
                    consistent = false;
                    return;
                }
            }
        };
        bool firstConsistent = true;
        bool secondConsistent = true;
        std::thread worker([&] { record(copy, secondConsistent); });
        record(counters, firstConsistent);
        worker.join();
        UNIT_ASSERT(firstConsistent);
        UNIT_ASSERT(secondConsistent);
        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.front().Statistics.Calls, 2 * iterations);
        UNIT_ASSERT_VALUES_EQUAL(stats.front().Statistics.ActiveTime.MicroSeconds(), 2 * iterations);
        UNIT_ASSERT_VALUES_EQUAL(stats.front().Statistics.InputRows.Sum, 2 * iterations);
    }
}
