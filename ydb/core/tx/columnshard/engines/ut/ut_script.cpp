#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <reader/common_reader/iterator/fetching.h>
#include <reader/simple_reader/iterator/fetching.h>
#include <scheme/versions/snapshot_scheme.h>

#include <barrier>
#include <thread>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

Y_UNIT_TEST_SUITE(TestScript) {
    std::shared_ptr<ISnapshotSchema> MakeTestSchema(THashMap<ui32, NTable::TColumn> columns, const std::vector<ui32> pkIds = { 0 }) {
        for (ui64 i = 0; i < pkIds.size(); ++i) {
            TValidator::CheckNotNull(columns.FindPtr(pkIds[i]))->KeyOrder = i;
        }

        auto cache = std::make_shared<TSchemaObjectsCache>();
        TIndexInfo info = TIndexInfo::BuildDefault(1, TTestStoragesManager::GetInstance(), columns, pkIds);
        return std::make_shared<TSnapshotSchema>(cache->GetOrInsertIndexInfo(std::move(info)), TSnapshot(1, 1));
    }

    Y_UNIT_TEST(StepMerging) {
        NCommon::TFetchingScriptBuilder acc = NCommon::TFetchingScriptBuilder::MakeForTests(
            MakeTestSchema({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 1, NTable::TColumn("c1", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") },
                { 2, NTable::TColumn("c2", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } }));

        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddAssembleStep(std::vector<ui32>({ 0 }), "", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
        acc.AddStep(std::make_shared<NSimple::TDeletionFilter>());
        acc.AddFetchingStep(std::vector<ui32>({ 0, 1 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddFetchingStep(std::vector<ui32>({ 1, 2 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        acc.AddAssembleStep(std::vector<ui32>({ 0, 1, 2 }), "", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, false);
        acc.AddStep(std::make_shared<NSimple::TDeletionFilter>());
        acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Merge);

        const NKikimr::NColumnShard::TConcreteScanCounters counters(NKikimr::NColumnShard::TScanCounters("TestScript"), nullptr);
        auto script = std::move(acc).Build(&counters);
        UNIT_ASSERT_STRINGS_EQUAL(script->DebugString(),
            "{branch:UNDEFINED;steps:["
            "{name=ALLOCATE_MEMORY::FILTER;details={stage=FILTER;column_ids=[Blob:0,Raw:0];};};"
            "{name=FETCHING_COLUMNS;details={columns=0;};};"
            "{name=ASSEMBLER;details={columns=(column_ids=0;column_names=c0;);;};};"
            "{name=DELETION;details={};};"
            "{name=ALLOCATE_MEMORY::FILTER;details={stage=FILTER;column_ids=[Blob:1];};};"
            "{name=ALLOCATE_MEMORY::FETCHING;details={stage=FETCHING;column_ids=[Blob:2,Raw:1,Raw:2];};};"
            "{name=FETCHING_COLUMNS;details={columns=1,2;};};"
            "{name=ASSEMBLER;details={columns=(column_ids=1,2;column_names=c1,c2;);;};};"
            "{name=DELETION;details={};};]}");

        const auto stats = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 9);
        for (ui32 i = 0; i < stats.size(); ++i) {
            const auto step = script->GetStep(i);
            UNIT_ASSERT_VALUES_EQUAL(stats[i].Name, step->GetName());
            UNIT_ASSERT(step->GetStepStatsHandle());
        }
        const auto& fetchedColumns = std::get<NKikimr::NColumnShard::TFetchColumnsStepDetails>(stats[6].Details).Columns;
        UNIT_ASSERT_VALUES_EQUAL(fetchedColumns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fetchedColumns[0].Id, 1);
        UNIT_ASSERT_VALUES_EQUAL(fetchedColumns[1].Id, 2);

        const auto& assembledColumns = std::get<NKikimr::NColumnShard::TAssembleStepDetails>(stats[7].Details).Columns;
        const std::vector<NKikimr::NColumnShard::TStepColumn> expectedColumns = { { 1, "c1" }, { 2, "c2" } };
        UNIT_ASSERT(assembledColumns == expectedColumns);

        const auto handle = script->GetStep(8)->GetStepStatsHandle();
        counters.RecordStepExecution(*handle, TDuration::MicroSeconds(1), 10, 5, std::nullopt, std::nullopt);
        const auto updated = counters.GetStepStats();
        UNIT_ASSERT_VALUES_EQUAL(updated[3].Statistics.Calls, 0);
        UNIT_ASSERT_VALUES_EQUAL(updated[8].Statistics.Calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(updated[8].Statistics.OutputRows.Sum, 5);
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

    // regression for #47943: reading the owner from another thread while the script is being
    // published must not observe a torn shared_ptr
    Y_UNIT_TEST(OwnerConcurrentInitialization) {
        static constexpr ui32 RacesCount = 5000;
        static constexpr ui32 ReaderAttemptsCount = 100000;
        auto schema = MakeTestSchema({ { 0, NTable::TColumn("c0", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Int32), "") } });
        for (ui32 i = 0; i < RacesCount; ++i) {
            NCommon::TFetchingScriptBuilder acc = NCommon::TFetchingScriptBuilder::MakeForTests(schema);
            acc.AddFetchingStep(std::vector<ui32>({ 0 }), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
            acc.AddAssembleStep(std::vector<ui32>({ 0 }), "", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
            auto script = std::move(acc).Build();

            const NCommon::TFetchingScript* published = script.get();
            NCommon::TFetchingScriptOwner owner;
            std::atomic<bool> readerStarted = false;
            std::atomic<const NCommon::TFetchingScript*> observed = nullptr;
            std::thread reader([&]() {
                readerStarted = true;
                for (ui32 j = 0; j < ReaderAttemptsCount; ++j) {
                    if (owner.HasScript()) {
                        observed = owner.GetScriptVerified().get();
                        break;
                    }
                }
            });
            while (!readerStarted) {
            }
            auto guard = owner.StartInitialization();
            UNIT_ASSERT(guard);
            guard->InitializationFinished(std::move(script));
            reader.join();
            UNIT_ASSERT(!owner.NeedInitialization());
            if (observed) {
                UNIT_ASSERT_VALUES_EQUAL((const void*)observed.load(), (const void*)published);
            }
            UNIT_ASSERT_VALUES_EQUAL((const void*)owner.GetScriptVerified().get(), (const void*)published);
            UNIT_ASSERT(!owner.GetScriptVerified()->IsFinished(0));
        }
    }
}
