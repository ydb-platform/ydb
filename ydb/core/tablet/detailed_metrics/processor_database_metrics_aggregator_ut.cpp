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
        : TabletId(tabletId), FollowerId(followerId)
    {}

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
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    return publicRoot->FindSubgroup("table", relativeTablePath);
}

NMonitoring::TDynamicCounterPtr FindPublicLeafGroup(
    NMonitoring::TDynamicCounterPtr publicRoot,
    ui64 tabletId,
    ui32 followerId,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
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
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
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
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
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

ui64 GetHistogramTotal(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name) {
    UNIT_ASSERT_C(countersGroup, "no counter group for the histogram " << name);
    auto histogram = countersGroup->FindHistogram(name);
    UNIT_ASSERT_C(histogram, "no histogram " << name);

    auto snapshot = histogram->Snapshot();
    ui64 total = 0;
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        total += snapshot->Value(i);
    }
    return total;
}

} // namespace

Y_UNIT_TEST_SUITE(TProcessorDatabaseMetricsAggregatorTest) {

    Y_UNIT_TEST(PartitionLevelUnionsLeavesLeaderOnlyMetricNotInflatedThenDropNodeShrinks) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;
        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5)
               .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 4)
               .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 6)
               .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 6)
               .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 5)
               .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 7)
               .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        TFakeTablet follower1(1000, 1);
        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999).AddCumulative(CONSUMED_CPU, 7)
               .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 999)
               .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 11)
               .Report(node2.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

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
        fixture.Processor->DropNode(2);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u);
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
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5)
               .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 6)
               .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u + 6u);
        UNIT_ASSERT(!tableGroup->FindSubgroup("tablet_id", "1000"));
        UNIT_ASSERT(!tableGroup->FindSubgroup("follower_id", "replicas_only"));
        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> rejected;
        auto* bogus = rejected.Add();
        bogus->SetTablePath(TABLE_PATH);
        bogus->SetLevel(TDetailedMetricsSettings::MetricsLevelTable);
        bogus->MutableTableCounters()->SetType(TTabletTypes::DataShard);
        bogus->MutableTableCounters()->MutableExecutorCounters()->AddSimple(999999);

        fixture.Processor->ApplyFromNode(3, true , rejected);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
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

    Y_UNIT_TEST(NodeStoppingToMentionATableEvictsItFromBothTrees) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10)
               .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));
        node1.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));
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

        fixture.Processor->ApplyFromNode(1, false , leafless);
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
                ? TDetailedMetricsSettings::MetricsLevelPartition : TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(2000, 0);
            first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5)
                .Report(node.Leaders, initial, NOW);
            second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 7)
                .Report(node.Leaders, initial, NOW);
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            auto table = FindPublicTableGroup(fixture.PublicRoot);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);

            first.AddCumulative(CONSUMED_CPU, 3).Report(node.Leaders, next, NOW + TDuration::Seconds(5));
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == table);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 30);
            const ui64 mixedTotal = initial == TDetailedMetricsSettings::MetricsLevelTable ? 15 : 10;
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), mixedTotal);

            second.AddCumulative(CONSUMED_CPU, 4).Report(node.Leaders, next, NOW + TDuration::Seconds(10));
            fixture.ApplyNode(1, node);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == table);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 30);
            // Representation changes may reset history; the new shape retains every new increment.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 7);
            UNIT_ASSERT_VALUES_EQUAL(bool(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0)),
                next == TDetailedMetricsSettings::MetricsLevelPartition);
            fixture.Processor->ApplyFromNode(1, false, {});
            UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
            UNIT_ASSERT(!FindRawTableGroup(fixture.RawRoot));
        }
    }

    Y_UNIT_TEST(TableCumulativeHistorySurvivesOneNodeLeaving) {
        TSimulatedNode node1, node2;
        TProcessorFixture fixture;
        TFakeTablet first(1000, 0), second(2000, 0);
        first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);
        second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 7)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);
        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();
        auto table = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 20);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
        second.AddCumulative(CONSUMED_CPU, 3)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(5));
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);
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

        second.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition,
            NOW + TDuration::Seconds(5), DATABASE_PATH + "/Renamed");
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!FindRawTableGroup(fixture.RawRoot));
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(FindPublicTableGroup(fixture.PublicRoot, "Renamed"),
            "table.datashard.row_count"), 10);
    }
}
