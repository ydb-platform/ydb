#include "processor_database_metrics_aggregator.h"
#include "node_database_metrics_aggregator.h"

#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using NTabletFlatExecutor::TExecutorCounters;

namespace {

    const TString DATABASE_PATH = "/Root/db";
    const TString RELATIVE_TABLE_PATH = "Table";
    const TString TABLE_PATH = DATABASE_PATH + "/" + RELATIVE_TABLE_PATH;
    const TInstant NOW = TInstant::Seconds(100);
    constexpr auto TABLET_TYPE = TTabletTypes::DataShard;
    constexpr auto DB_UNIQUE_ROWS_TOTAL = TExecutorCounters::DB_UNIQUE_ROWS_TOTAL;
    constexpr auto CONSUMED_CPU = TExecutorCounters::CONSUMED_CPU;

    // Use the production positional layouts for both source categories.
    struct TFakeTablet {
        const ui64 TabletId;
        const ui32 FollowerId;
        TExecutorCounters Executor;
        THolder<TTabletCountersBase> App = CreateAppCountersByTabletType(TABLET_TYPE);
        TTabletCountersBase ExecutorBaseline;
        TTabletCountersBase AppBaseline;

        TFakeTablet(ui64 tabletId, ui32 followerId)
            : TabletId(tabletId)
            , FollowerId(followerId)
        {
        }

        TFakeTablet& SetSimple(ui32 counter, ui64 value) {
            Executor.Simple()[counter] = value;
            return *this;
        }

        TFakeTablet& AddCumulative(ui32 counter, ui64 value) {
            Executor.Cumulative()[counter] += value;
            return *this;
        }

        TFakeTablet& AddAppCumulative(ui32 counter, ui64 value) {
            App->Cumulative()[counter] += value;
            return *this;
        }

        void Report(const TNodeDatabaseMetricsAggregatorPtr& node, EDetailedMetricsLevel level,
                    TInstant now, const TString& path = TABLE_PATH)
        {
            auto executorDiff = Executor.MakeDiffForAggr(ExecutorBaseline);
            auto appDiff = App->MakeDiffForAggr(AppBaseline);
            node->AddCounters(path, level, TabletId, FollowerId, TABLET_TYPE, *executorDiff, *appDiff, now);
            Executor.RememberCurrentStateAsBaseline(ExecutorBaseline);
            App->RememberCurrentStateAsBaseline(AppBaseline);
        }
    };

    struct TSimulatedNode {
        NMonitoring::TDynamicCounterPtr Root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TNodeDatabaseMetricsAggregatorPtr Leaders = CreateNodeDatabaseMetricsAggregator(Root, DATABASE_PATH, false);
        TNodeDatabaseMetricsAggregatorPtr Followers = CreateNodeDatabaseMetricsAggregator(Root, DATABASE_PATH, true);
    };

    struct TProcessorFixture {
        NMonitoring::TDynamicCounterPtr RawRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NMonitoring::TDynamicCounterPtr PublicRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TProcessorDatabaseMetricsAggregatorPtr Processor = CreateProcessorDatabaseMetricsAggregator(
            RawRoot, PublicRoot, DATABASE_PATH, MakeHolder<TExecutorCounters>());

        void ApplyNode(ui32 nodeId, TSimulatedNode& node) {
            NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> tables;
            node.Leaders->Pack(tables);
            Processor->ApplyFromNode(nodeId, false, tables);
            tables.Clear();
            node.Followers->Pack(tables);
            Processor->ApplyFromNode(nodeId, true, tables);
        }
    };

    ui64 GetMappedCounterValue(NMonitoring::TDynamicCounterPtr group, const TString& name) {
        UNIT_ASSERT_C(group, "no counter group for " << name);
        auto counter = group->FindNamedCounter("name", name);
        UNIT_ASSERT_C(counter, "no mapped counter " << name);
        return counter->Val();
    }

    NMonitoring::TDynamicCounterPtr FindPublicTableGroup(
        NMonitoring::TDynamicCounterPtr publicRoot,
        const TString& relativeTablePath = RELATIVE_TABLE_PATH) {
        return publicRoot->FindSubgroup("table", relativeTablePath);
    }

    NMonitoring::TDynamicCounterPtr FindPublicLeafGroup(
        NMonitoring::TDynamicCounterPtr publicRoot,
        ui64 tabletId,
        ui32 followerId,
        const TString& relativeTablePath = RELATIVE_TABLE_PATH) {
        auto tableGroup = FindPublicTableGroup(publicRoot, relativeTablePath);
        if (!tableGroup) {
            return nullptr;
        }
        auto tabletGroup = tableGroup->FindSubgroup("tablet_id", ToString(tabletId));
        if (!tabletGroup) {
            return nullptr;
        }
        return tabletGroup->FindSubgroup("follower_id", ToString(followerId));
    }

    NMonitoring::TDynamicCounterPtr FindRawTableGroup(
        NMonitoring::TDynamicCounterPtr rawRoot,
        const TString& relativeTablePath = RELATIVE_TABLE_PATH) {
        return rawRoot->FindSubgroup("table", relativeTablePath);
    }

    NMonitoring::TDynamicCounterPtr FindRawExecutorCountersGroup(NMonitoring::TDynamicCounterPtr bucketGroup) {
        if (!bucketGroup) {
            return nullptr;
        }
        auto typeGroup = bucketGroup->FindSubgroup("type", TTabletTypes::TypeToStr(TABLET_TYPE));
        if (!typeGroup) {
            return nullptr;
        }
        return typeGroup->FindSubgroup("category", "executor");
    }

    NMonitoring::TDynamicCounterPtr FindRawLeafExecutorCounters(
        NMonitoring::TDynamicCounterPtr rawRoot,
        ui64 tabletId,
        ui32 followerId,
        const TString& relativeTablePath = RELATIVE_TABLE_PATH) {
        auto tableGroup = FindRawTableGroup(rawRoot, relativeTablePath);
        if (!tableGroup) {
            return nullptr;
        }
        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        if (!perPartitionGroup) {
            return nullptr;
        }
        auto tabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(tabletId));
        if (!tabletGroup) {
            return nullptr;
        }
        return FindRawExecutorCountersGroup(tabletGroup->FindSubgroup("follower_id", ToString(followerId)));
    }

    ui64 GetCounterValue(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name) {
        UNIT_ASSERT_C(countersGroup, "no counter group for the counter " << name);
        auto counter = countersGroup->FindNamedCounter("sensor", name);
        UNIT_ASSERT_C(counter, "no counter " << name);
        return counter->Val();
    }

    ui64 GetHistogramTotal(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name,
                           const TString& label = "sensor")
    {
        UNIT_ASSERT_C(countersGroup, "no counter group for the histogram " << name);
        auto histogram = countersGroup->FindNamedHistogram(label, name);
        UNIT_ASSERT_C(histogram, "no histogram " << name);

        auto snapshot = histogram->Snapshot();
        ui64 total = 0;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            total += snapshot->Value(i);
        }
        return total;
    }

    void AssertCpuHistogram(NMonitoring::TDynamicCounterPtr rawExecutor,
                            NMonitoring::TDynamicCounterPtr publicGroup, ui64 expectedTotal)
    {
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(rawExecutor, "HIST(ConsumedCPU)"), expectedTotal);
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(publicGroup, "table.datashard.used_core_percents", "name"), expectedTotal);
    }

    void AssertNoWrappedBuckets(NMonitoring::TDynamicCounterPtr rawExecutor, const TString& histogramName = "HIST(ConsumedCPU)") {
        auto histogram = rawExecutor->FindNamedHistogram("sensor", histogramName);
        UNIT_ASSERT_C(histogram, "histogram not found: " << histogramName);
        auto snapshot = histogram->Snapshot();
        const ui64 maxValue = Max<ui64>();
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            UNIT_ASSERT_C(snapshot->Value(i) != maxValue,
                "bucket " << i << " has wrapped value Max<ui64>()");
        }
    }

    TString DumpBuckets(NMonitoring::IHistogramSnapshot* snapshot) {
        TStringBuilder result;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            if (i > 0) result << ",";
            result << "b" << i << "=" << snapshot->Value(i);
        }
        return result;
    }

} // namespace

Y_UNIT_TEST_SUITE(TProcessorDatabaseMetricsAggregatorTest) {

    Y_UNIT_TEST(PartitionLevelUnionsLeavesLeaderOnlyMetricNotInflatedThenDropNodeShrinks) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;
        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 4).AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 6).Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 6).AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 5).AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 7).Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        TFakeTablet follower1(1000, 1);
        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999).AddCumulative(CONSUMED_CPU, 7).AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 999).AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 11).Report(node2.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u + 6u + 7u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.write.rows"), 9);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.read.rows"), 24);
        UNIT_ASSERT(!tableGroup->FindSubgroup("follower_id", "replicas_only"));
        auto tabletGroup1000 = tableGroup->FindSubgroup("tablet_id", "1000");
        UNIT_ASSERT(tabletGroup1000);
        UNIT_ASSERT(!tabletGroup1000->FindNamedCounter("name", "table.datashard.row_count"));

        fixture.Processor->ApplyFromNode(2, true, {});
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 30);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 18);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.write.rows"), 9);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.read.rows"), 24);
        fixture.Processor->DropNode(2);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 18);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.write.rows"), 9);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.read.rows"), 24);
        auto rawTableGroup = fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH);
        UNIT_ASSERT(rawTableGroup);
        auto rawPerPartition = rawTableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(rawPerPartition);
        UNIT_ASSERT(rawPerPartition->FindSubgroup("tablet_id", "1000"));
        UNIT_ASSERT(!rawPerPartition->FindSubgroup("tablet_id", "2000"));
    }

    Y_UNIT_TEST(TableLevelSumsPartialsAndRejectsFollowerPartial) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 6).Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u + 6u);
        UNIT_ASSERT(!tableGroup->FindSubgroup("tablet_id", "1000"));
        UNIT_ASSERT(!tableGroup->FindSubgroup("follower_id", "replicas_only"));
        TSimulatedNode rejectedNode;
        TFakeTablet rejectedTablet(3000, 0);
        rejectedTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999999).AddCumulative(CONSUMED_CPU, 999).Report(rejectedNode.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);
        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> rejected;
        rejectedNode.Leaders->Pack(rejected);

        fixture.Processor->ApplyFromNode(3, true, rejected);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u + 6u);
    }

    Y_UNIT_TEST(GaugeDropToZeroReflectedAfterRecalculate) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 42)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 42u);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 0)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 0u);
    }

    Y_UNIT_TEST(FinalReportPrecedesTableEvictionAndRecreationResetsHistory) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        auto oldTable = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(oldTable);
        UNIT_ASSERT(fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));
        leader1.AddCumulative(CONSUMED_CPU, 3)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(5));
        node1.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == oldTable);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(oldTable, "table.datashard.row_count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(oldTable, "table.datashard.consumed_cpu_us"), 8);
        AssertCpuHistogram(FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot)), oldTable, 0);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));

        TFakeTablet recreated(1000, 0);
        recreated.SetSimple(DB_UNIQUE_ROWS_TOTAL, 4).AddCumulative(CONSUMED_CPU, 2).Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(10));
        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        auto newTable = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(newTable != oldTable);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.row_count"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.consumed_cpu_us"), 2);
        auto rawExecutor = FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot));
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(rawExecutor, "ConsumedCPU"), 2);
        AssertCpuHistogram(rawExecutor, newTable, 1);
    }

    Y_UNIT_TEST(MaxCumulativeCounterRoundTripsThroughPackAndUnpack) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.AddCumulative(CONSUMED_CPU, 50)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        auto leafExecutorCounters = FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafExecutorCounters, "MAX(ConsumedCPU)"), 0u);

        leader1.AddCumulative(CONSUMED_CPU, 100)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafExecutorCounters, "MAX(ConsumedCPU)"), 100u / 5u);
    }

    Y_UNIT_TEST(MultiGenerationCumulativeAccumulatesEveryDeltaFromEveryNode) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);

        ui64 expectedTotal = 0;
        TInstant now = NOW;

        for (ui64 generation = 1; generation <= 3; ++generation) {
            const ui64 delta1 = 10 * generation;
            const ui64 delta2 = 7 * generation;

            leader1.AddCumulative(CONSUMED_CPU, delta1);
            leader2.AddCumulative(CONSUMED_CPU, delta2);
            leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
            leader2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);

            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            expectedTotal += delta1 + delta2;
            now += TDuration::Seconds(5);
        }

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), expectedTotal);
    }

    Y_UNIT_TEST(HistogramAggregateRoundTripsThroughPackAndUnpack) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);

        leader1.AddCumulative(CONSUMED_CPU, 50)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        leader1.AddCumulative(CONSUMED_CPU, 80)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));
        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        auto leafExecutorCounters = FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leafExecutorCounters, "HIST(ConsumedCPU)"), 1u);
        auto rawSnapshot = leafExecutorCounters->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(rawSnapshot->Value(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(rawSnapshot->Value(1), 1);

        // The same tablet moves from the zero-rate bucket to the first positive bucket.
        for (const auto& group : {FindPublicLeafGroup(fixture.PublicRoot, 1000, 0), FindPublicTableGroup(fixture.PublicRoot)}) {
            UNIT_ASSERT(group);
            auto histogram = group->FindNamedHistogram("name", "table.datashard.used_core_percents");
            UNIT_ASSERT(histogram);
            auto snapshot = histogram->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Value(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Value(1), 1);
        }
    }

    Y_UNIT_TEST(PartitionMoveTransientDoesNotDoubleAGauge) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leaderOnNode1(1000, 0);
        leaderOnNode1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 55)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet leaderOnNode2(1000, 0);
        leaderOnNode2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 55)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 55u);
        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 55u);
    }

    Y_UNIT_TEST(LeaflessPartitionMessageLeavesNoGroupBehind) {
        TProcessorFixture fixture;

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> leafless;
        auto* entry = leafless.Add();
        entry->SetTablePath(TABLE_PATH);
        entry->SetLevel(TDetailedMetricsSettings::MetricsLevelPartition);

        fixture.Processor->ApplyFromNode(1, false, leafless);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));
    }

    Y_UNIT_TEST(MixedShapesContributeToOneTableRollup) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 77)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 77u + 999u);
        UNIT_ASSERT(tableGroup->FindSubgroup("tablet_id", "2000"));
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 90)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(5));
        leader2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 90u + 999u);
    }

    Y_UNIT_TEST(GradualLevelChangesKeepOneTableAndAcceptEveryReport) {
        for (auto initial : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const auto next = initial == TDetailedMetricsSettings::MetricsLevelTable
                                  ? TDetailedMetricsSettings::MetricsLevelPartition
                                  : TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(2000, 0);
            first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).Report(node.Leaders, initial, NOW);
            second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 7).Report(node.Leaders, initial, NOW);
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            auto table = FindPublicTableGroup(fixture.PublicRoot);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);

            first.AddCumulative(CONSUMED_CPU, 3).Report(node.Leaders, next, NOW + TDuration::Seconds(5));
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == table);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 30);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);

            second.AddCumulative(CONSUMED_CPU, 4).Report(node.Leaders, next, NOW + TDuration::Seconds(10));
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == table);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 30);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 19);

            // The retired shape disappears only after its final report has been sent.
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == table);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 30);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 19);
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 2);
            UNIT_ASSERT_VALUES_EQUAL(bool(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0)),
                                     next == TDetailedMetricsSettings::MetricsLevelPartition);
            fixture.Processor->ApplyFromNode(1, false, {});
            UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
            UNIT_ASSERT(!FindRawTableGroup(fixture.RawRoot));
        }
    }

    Y_UNIT_TEST(NodeRemovalDropsLiveHistogramsAndRetainsCumulativeHistory) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(tableLevel ? 2000 : 1000, 0);
            first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).Report(node1.Leaders, level, NOW);
            second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 7).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();
            auto table = FindPublicTableGroup(fixture.PublicRoot);
            auto rawExecutor = tableLevel
                                   ? FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot))
                                   : FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            AssertCpuHistogram(rawExecutor, publicBucket, 2);

            // An unchanged report has no histogram deltas, but still owns its live observation.
            fixture.ApplyNode(1, node1);
            fixture.Processor->DropNode(1);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 20);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
            AssertCpuHistogram(rawExecutor, publicBucket, 1);
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);

            second.AddCumulative(CONSUMED_CPU, 3)
                .Report(node2.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);
            AssertCpuHistogram(rawExecutor, publicBucket, 1);
            auto rawSnapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(rawSnapshot->Value(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(rawSnapshot->Value(1), 1);

            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();
            AssertCpuHistogram(rawExecutor, publicBucket, 1);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);
        }
    }

    Y_UNIT_TEST(NodeRemovalHandlesExtraHistogramBucketsFromSender) {
        TSimulatedNode node1, node2;
        TProcessorFixture fixture;
        TFakeTablet first(1000, 0), second(2000, 0);
        first.AddCumulative(CONSUMED_CPU, 5)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);
        second.AddCumulative(CONSUMED_CPU, 7)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> tables;
        node1.Leaders->Pack(tables);
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        auto* histogram = tables.Mutable(0)->MutableTableCounters()->MutableExecutorCounters()->MutableHistogram(TExecutorCounters::TX_PERCENTILE_CONSUMED_CPU);
        const auto knownBucketCount = histogram->GetBucketsCount();
        histogram->SetBucketsCount(knownBucketCount + 1);
        histogram->AddBuckets(knownBucketCount);
        histogram->AddBuckets(1);

        fixture.Processor->ApplyFromNode(1, false, tables);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        auto table = FindPublicTableGroup(fixture.PublicRoot);
        auto rawExecutor = FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot));
        AssertCpuHistogram(rawExecutor, table, 2);

        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();
        AssertCpuHistogram(rawExecutor, table, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
    }

    Y_UNIT_TEST(FinalHistogramCancellationIsNotRepeatedOnNodeAbsence) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(tableLevel ? 2000 : 1000, 0);
            first.AddCumulative(CONSUMED_CPU, 5).Report(node1.Leaders, level, NOW);
            second.AddCumulative(CONSUMED_CPU, 7).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();
            auto table = FindPublicTableGroup(fixture.PublicRoot);
            auto rawExecutor = tableLevel
                                   ? FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot))
                                   : FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            AssertCpuHistogram(rawExecutor, publicBucket, 2);

            node1.Leaders->ForgetTablet(first.TabletId, first.FollowerId);
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();
            AssertCpuHistogram(rawExecutor, publicBucket, 1);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);

            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();
            AssertCpuHistogram(rawExecutor, publicBucket, 1);
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
        }
    }

    Y_UNIT_TEST(RetiredPartitionKeepsFinalIncrementsBeforeRecalculation) {
        TSimulatedNode node;
        TProcessorFixture fixture;
        TFakeTablet first(1000, 0), second(2000, 0);
        first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).Report(node.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 7).Report(node.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, node);
        fixture.Processor->RecalculateAllCounters();
        auto table = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);

        first.AddCumulative(CONSUMED_CPU, 3)
            .Report(node.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));
        node.Leaders->ForgetTablet(first.TabletId, first.FollowerId);
        fixture.ApplyNode(1, node);
        // Retire the leaf before publishing its final increment.
        fixture.ApplyNode(1, node);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0));
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 20);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);

        fixture.Processor->DropNode(1);
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!FindRawTableGroup(fixture.RawRoot));
        TSimulatedNode replacementNode;
        TFakeTablet replacement(1000, 0);
        replacement.AddCumulative(CONSUMED_CPU, 2)
            .Report(replacementNode.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, replacementNode);
        fixture.Processor->RecalculateAllCounters();
        auto recreatedTable = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(recreatedTable != table);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(recreatedTable, "table.datashard.consumed_cpu_us"), 2);
        AssertCpuHistogram(FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0), recreatedTable, 1);
    }

    Y_UNIT_TEST(RetiredFollowerHistoryPreservesLeaderOnlyFiltering) {
        TSimulatedNode node;
        TProcessorFixture fixture;
        TFakeTablet leader(1000, 0), follower(1000, 1);
        leader.AddCumulative(CONSUMED_CPU, 5)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 4)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 6)
            .Report(node.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        follower.AddCumulative(CONSUMED_CPU, 7)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 999)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 11)
            .Report(node.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, node);
        node.Followers->ForgetTablet(follower.TabletId, follower.FollowerId);
        fixture.ApplyNode(1, node);
        fixture.ApplyNode(1, node);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        auto table = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.write.rows"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.read.rows"), 17);
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);
    }

    Y_UNIT_TEST(RenameRetiresOnlyTheReportingNodesOldPath) {
        TSimulatedNode node1, node2;
        TProcessorFixture fixture;
        TFakeTablet first(1000, 0), second(1000, 0);
        first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        first.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition,
                     NOW + TDuration::Seconds(5), DATABASE_PATH + "/Renamed");
        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0, "Renamed"));

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(FindPublicTableGroup(fixture.PublicRoot),
                                                       "table.datashard.row_count"), 10);

        second.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition,
                      NOW + TDuration::Seconds(5), DATABASE_PATH + "/Renamed");
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        auto retiredTable = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(retiredTable, "table.datashard.row_count"), 0);
        AssertCpuHistogram(FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0), retiredTable, 0);

        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!FindRawTableGroup(fixture.RawRoot));
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(FindPublicTableGroup(fixture.PublicRoot, "Renamed"),
                                                       "table.datashard.row_count"), 10);
    }

    Y_UNIT_TEST(FollowerOnlyReportRetireLeaderContributionViaEmptyFallback) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        // Node1: both leader and follower
        TFakeTablet node1Leader(1000, 0);
        node1Leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 30)
            .AddCumulative(CONSUMED_CPU, 10)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet node1Follower(1000, 1);
        node1Follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 15)
            .AddCumulative(CONSUMED_CPU, 5)
            .Report(node1.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // Node2: leader only
        TFakeTablet node2Leader(2000, 0);
        node2Leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 40)
            .AddCumulative(CONSUMED_CPU, 8)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // Initial report with both nodes contributing both roles
        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));
        // row_count is leader-only, so node1's follower 15 never reaches it: 30 + 40.
        // consumed_cpu_us is cumulative and does count followers: 10 + 5 + 8, and it
        // stays put when a contribution is retired below.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 70u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 23u);

        // Node1 now reports follower only; leader role gets empty fallback
        node1Leader.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));
        node1.Leaders->ForgetTablet(node1Leader.TabletId, node1Leader.FollowerId);

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> followerTables;
        node1.Followers->Pack(followerTables);
        fixture.Processor->ApplyFromNode(1, false, {});  // Leader role fallback: empty
        fixture.Processor->ApplyFromNode(1, true, followerTables);  // Follower role: has data

        fixture.Processor->RecalculateAllCounters();

        // Node1's leader contribution is retired, but follower survives
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));
        // Only node2's leader 40 is left feeding row_count.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 40u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 23u);
    }

    Y_UNIT_TEST(LeaderOnlyReportRetireFollowerContributionViaEmptyFallback) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        // Node1: both leader and follower
        TFakeTablet node1Leader(1000, 0);
        node1Leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 25)
            .AddCumulative(CONSUMED_CPU, 9)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet node1Follower(1000, 1);
        node1Follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20)
            .AddCumulative(CONSUMED_CPU, 6)
            .Report(node1.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // Node2: follower only
        TFakeTablet node2Follower(2000, 1);
        node2Follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 35)
            .AddCumulative(CONSUMED_CPU, 7)
            .Report(node2.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // Initial report with both nodes contributing both roles
        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 1));
        // row_count is leader-only, and node1's leader is the only leader here: 25.
        // consumed_cpu_us counts both roles on both nodes: 9 + 6 + 7.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 25u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 22u);

        // Node1 now reports leader only; follower role gets empty fallback
        node1Follower.Report(node1.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));
        node1.Followers->ForgetTablet(node1Follower.TabletId, node1Follower.FollowerId);

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> leaderTables;
        node1.Leaders->Pack(leaderTables);
        fixture.Processor->ApplyFromNode(1, false, leaderTables);  // Leader role: has data
        fixture.Processor->ApplyFromNode(1, true, {});  // Follower role fallback: empty

        fixture.Processor->RecalculateAllCounters();

        // Node1's follower contribution is retired, but leader survives
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 1));
        // Retiring a follower leaves row_count untouched - it never counted it. The
        // retire is observable in the leaf group above, not in this gauge.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 25u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 22u);
    }

    Y_UNIT_TEST(PartialReportThenDropNodeRetiresDependingOnRole) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        // Node1: both leader and follower
        TFakeTablet node1Leader(1000, 0);
        node1Leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20)
            .AddCumulative(CONSUMED_CPU, 7)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet node1Follower(1000, 1);
        node1Follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 12)
            .AddCumulative(CONSUMED_CPU, 4)
            .Report(node1.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // Node2: both leader and follower
        TFakeTablet node2Leader(2000, 0);
        node2Leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 30)
            .AddCumulative(CONSUMED_CPU, 6)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet node2Follower(2000, 1);
        node2Follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 18)
            .AddCumulative(CONSUMED_CPU, 5)
            .Report(node2.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 1));
        // row_count takes the two leaders only: 20 + 30. consumed_cpu_us takes all
        // four contributions: 7 + 4 + 6 + 5.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 50u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 22u);

        // Node1 now reports follower only; leader role gets empty fallback
        node1Leader.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));
        node1.Leaders->ForgetTablet(node1Leader.TabletId, node1Leader.FollowerId);

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> node1FollowerTables;
        node1.Followers->Pack(node1FollowerTables);
        fixture.Processor->ApplyFromNode(1, false, {});  // Leader role fallback: empty
        fixture.Processor->ApplyFromNode(1, true, node1FollowerTables);  // Follower role: has data

        fixture.Processor->RecalculateAllCounters();

        // Node1's leader contribution is retired, but follower and node2 both survive
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 1));
        // Node1's leader is gone, so only node2's leader 30 feeds row_count now.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 30u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 22u);

        // Now drop node1 completely
        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();

        // Node1 completely gone, node2 has both
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 1));
        // Node1's leader was already retired above, so row_count is unchanged at 30 -
        // what DropNode removes here is node1's follower leaf, asserted above.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 30u);
        // DropNode retires the live gauges but keeps the cumulative history, matching
        // NodeRemovalDropsLiveHistogramsAndRetainsCumulativeHistory.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 22u);
    }

    Y_UNIT_TEST(SingleNodeResumeAfterDropDoesNotWrapLiveHistogram) {
        // Histogram buckets on per-second rate between consecutive reports, not cumulative values.
        // First report parks tablet in bucket 0 (rate 0 on diff==0). Move needs a second report.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0);

            // NOW: First report (rate 0 -> bucket 0).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // NOW+1s: Second report (rate 50000 -> bucket 1). Assert tablet moved to bucket 1.
            first.AddCumulative(CONSUMED_CPU, 50000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            auto rawExecutor = tableLevel
                                   ? FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot))
                                   : FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
            auto snapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Value(1), 1, DumpBuckets(snapshot.Get()));

            // Drop the node. This erases the bucket and the entire TTableEntry,
            // destroying all group pointers (the counters are detached from the tree).
            fixture.Processor->DropNode(1);

            // NOW+2s: Third report (rate 150000 -> bucket 2). The sender kept its baseline,
            // so it emits a phantom decrement from bucket 1. The recreated processor's Total
            // starts at 0, so without clamping the decrement lands as Max<ui64>() in bucket 1.
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Re-find the groups after the recreation (previous groups were destroyed).
            auto table = FindPublicTableGroup(fixture.PublicRoot);
            rawExecutor = tableLevel
                              ? FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot))
                              : FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);

            // The recreated bucket has only node1's new observation. Bucket 1 should be 0
            // (clamped phantom decrement prevents Max<ui64>()). Bucket 2 has the tablet.
            snapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Value(1), 0, DumpBuckets(snapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Value(2), 1, DumpBuckets(snapshot.Get()));
            AssertNoWrappedBuckets(rawExecutor);
            // The histogram total is 1 (only the new observation in bucket 2).
            AssertCpuHistogram(rawExecutor, table, 1);
            // The cumulative total is 150000: the recreated entry sees only the new delta.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 150000);
        }
    }

    Y_UNIT_TEST(LiveHistogramMultiNodeIsolationPreventsTotalClamp) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(tableLevel ? 2000 : 1000, 0);

            // NOW: First report for both (rate 0 -> bucket 0).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            // NOW+1s: Second report for both (rate 50000, 70000 -> bucket 1 for both).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            auto table = FindPublicTableGroup(fixture.PublicRoot);
            auto rawExecutor = tableLevel
                                   ? FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot))
                                   : FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
            auto rawSnapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(rawSnapshot->Value(1), 2, DumpBuckets(rawSnapshot.Get()));
            AssertCpuHistogram(rawExecutor, table, 2);

            // Drop node1. The bucket survives on node2, so the group pointers stay valid.
            fixture.Processor->DropNode(1);

            // NOW+2s: Node1 moves to bucket 2 (rate 150000). Node2 does not report.
            // Per-node clamping should not affect node2's contribution to bucket 1.
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Node1 moved from bucket 1 to bucket 2, node2 still in bucket 1.
            rawSnapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(rawSnapshot->Value(1), 1, DumpBuckets(rawSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(rawSnapshot->Value(2), 1, DumpBuckets(rawSnapshot.Get()));
            AssertNoWrappedBuckets(rawExecutor);
            AssertCpuHistogram(rawExecutor, table, 2);
            // Cumulative counters are append-only and the bucket survived the drop (node2
            // still holds it), so the total is every diff ever applied:
            // node1 50000 + 50000 + 150000, node2 70000 + 70000 = 390000.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 390000);
        }
    }

    Y_UNIT_TEST(LiveHistogramClampingDoesNotAffectCumulativeHistory) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(1001, 0);
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            second.AddCumulative(CONSUMED_CPU, 30000).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            auto table = FindPublicTableGroup(fixture.PublicRoot);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 80000);

            // Drop node1, then resume it with a bucket move to bucket 2.
            // The cumulative total must not change: clamp affects only live histograms,
            // not cumulative counters (which are append-only).
            fixture.Processor->DropNode(1);
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Cumulative counters are append-only. The total is 50000 + 150000 + 30000 = 230000,
            // never affected by the clamp of the live-histogram bucket counts.
            // At partition level, DropNode removes node1's leaf source from the rollup via source
            // removal and re-registration. The rollup's RetainOnSourceRemoval history plus the
            // re-added source correctly recovers the cumulative (confirmed).
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 230000);
        }
    }

    Y_UNIT_TEST(ProcessorRestartEquivalentWithoutNodeBaseline) {
        // When the processor is restarted with an empty in-memory aggregator, but the node
        // keeps its baseline, reports sent after the restart trigger phantom decrements that
        // must be clamped. Node2 never reports to the new processor, so its observation is lost.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(tableLevel ? 2000 : 1000, 0);

            // NOW: First report for both (rate 0 -> bucket 0).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            // NOW+1s: Second report for both (rate 50000, 70000 -> bucket 1 for both).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            // Simulate processor restart: new fixture, node baseline unchanged.
            // The new processor will see node1's move to bucket 2 as a phantom decrement from bucket 1.
            // Node2 does not report to the new processor at all.
            TProcessorFixture newFixture;

            // NOW+2s: Node1 moves to bucket 2 (rate 150000). Apply to the new processor only.
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            newFixture.ApplyNode(1, node1);
            newFixture.Processor->RecalculateAllCounters();

            // Bucket 1 is 0: node1's phantom decrement is clamped (unfixed code would be Max<ui64>()).
            // Bucket 2 is 1: node1's new observation. Bucket 1 being 0 is two things at once: the
            // clamped phantom decrement, and node2's missing observation (it never reported to
            // the new processor — the accepted residual undercount).
            auto newTable = FindPublicTableGroup(newFixture.PublicRoot);
            auto newRawExecutor = tableLevel
                                      ? FindRawExecutorCountersGroup(FindRawTableGroup(newFixture.RawRoot))
                                      : FindRawLeafExecutorCounters(newFixture.RawRoot, 1000, 0);
            auto newRawSnapshot = newRawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(newRawSnapshot->Value(1), 0, DumpBuckets(newRawSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(newRawSnapshot->Value(2), 1, DumpBuckets(newRawSnapshot.Get()));
            AssertNoWrappedBuckets(newRawExecutor);
            // The cumulative total is 150000 (only node1's delta). Node2's 70000 is lost
            // until node2's cumulative moves buckets and sends a delta to the new processor.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.consumed_cpu_us"), 150000);
        }
    }

    Y_UNIT_TEST(LiveHistogramClampingKnownGapUnchangedObservation) {
        // After dropping a node and resuming it with the same rate (no rate change),
        // no histogram delta is emitted and the observation stays missing (known limit).
        // Full repair would require baseline resync from the sender, which is deliberately not implemented.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(tableLevel ? 2000 : 1000, 0);

            // NOW: First report for both (rate 0 -> bucket 0).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            // NOW+1s: Second report for both (rate 50000, 70000 -> bucket 1 for both).
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            auto table = FindPublicTableGroup(fixture.PublicRoot);
            auto rawExecutor = tableLevel
                                   ? FindRawExecutorCountersGroup(FindRawTableGroup(fixture.RawRoot))
                                   : FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
            auto rawSnapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(rawSnapshot->Value(1), 2, DumpBuckets(rawSnapshot.Get()));
            AssertCpuHistogram(rawExecutor, table, 2);

            // Drop node1. The bucket survives on node2, so the group pointers stay valid.
            fixture.Processor->DropNode(1);

            // NOW+2s: Node1 reports again with the SAME cumulative value (50000). Same rate,
            // so the histogram bucket stays the same and no histogram delta is emitted.
            first.AddCumulative(CONSUMED_CPU, 50000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Node1's observation is lost and cannot be recovered without baseline resync.
            // This is a deliberate limit: the processor has no way to know the original
            // bucket placement after it has forgotten the baseline. Same rate = no delta sent.
            // Node1 no longer contributes to the histogram. Node2 is still in bucket 1.
            rawSnapshot = rawExecutor->FindHistogram("HIST(ConsumedCPU)")->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(rawSnapshot->Value(1), 1, DumpBuckets(rawSnapshot.Get()));
            // The histogram total is 1 (only node2), not 2 (node1 is missing).
            AssertCpuHistogram(rawExecutor, table, 1);
            AssertNoWrappedBuckets(rawExecutor);
            // Cumulative history is preserved even though the live observation is lost.
            // Node1's third report carries its cumulative delta even though its rate is
            // unchanged and no histogram delta is emitted, and the bucket survived the drop
            // on node2, so nothing was unwound.
            // Total: node1 3 x 50000 + node2 2 x 70000 = 290000.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 290000);
        }
    }
} // Y_UNIT_TEST_SUITE(TProcessorDatabaseMetricsAggregatorTest)
