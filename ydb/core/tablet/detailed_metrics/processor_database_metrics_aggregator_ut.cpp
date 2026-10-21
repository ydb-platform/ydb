#include "processor_database_metrics_aggregator.h"
#include "node_database_metrics_aggregator.h"
#include "ut_helpers.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>

using namespace NKikimr;
using NTabletFlatExecutor::TExecutorCounters;

namespace {

    const TString DATABASE_PATH = "/Root/db";
    const TString RELATIVE_TABLE_PATH = "Table";
    const TString TABLE_PATH = DATABASE_PATH + "/" + RELATIVE_TABLE_PATH;
    const TString RELATIVE_OTHER_TABLE_PATH = "Other";
    const TString OTHER_TABLE_PATH = DATABASE_PATH + "/" + RELATIVE_OTHER_TABLE_PATH;
    const TInstant NOW = TInstant::Seconds(100);
    constexpr auto TABLET_TYPE = TTabletTypes::DataShard;
    constexpr auto DB_UNIQUE_ROWS_TOTAL = TExecutorCounters::DB_UNIQUE_ROWS_TOTAL;
    constexpr auto CONSUMED_CPU = TExecutorCounters::CONSUMED_CPU;

    constexpr ui32 PUBLIC_ROW_COUNT = NDataShard::COUNTER_DATASHARD_ROW_COUNT;
    constexpr ui32 PUBLIC_WRITE_ROWS = NDataShard::COUNTER_DATASHARD_WRITE_ROWS;
    constexpr ui32 PUBLIC_READ_ROWS = NDataShard::COUNTER_DATASHARD_READ_ROWS;
    constexpr ui32 PUBLIC_CONSUMED_CPU_US = NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS;
    constexpr ui32 PUBLIC_USED_CORE_PERCENTS = NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS;

    using TTables = NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>;

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

    const TDetailedMetricsDescriptor& GetDataShardDescriptor() {
        const auto* descriptor = GetDetailedMetricsDescriptor(TABLET_TYPE);
        UNIT_ASSERT(descriptor);
        return *descriptor;
    }

    // The public metric values of one DataShard bucket, shaped as a node packs them
    class TPublicValues {
    public:
        TPublicValues() {
            const auto& descriptor = GetDataShardDescriptor();
            Values.MutableSimple()->Resize(static_cast<int>(descriptor.Gauges.size()), 0);
            Values.SetCumulativeCount(descriptor.Rates.size());
            for (const auto& spec : descriptor.Histograms) {
                auto* histogram = Values.AddHistogram();
                histogram->SetBucketsCount(spec.BucketCount());
                if (spec.NonDerivative) {
                    histogram->SetNonDerivative(true);
                }
            }
        }

        TPublicValues& Gauge(ui32 metric, ui64 value) {
            Values.SetSimple(metric, value);
            return *this;
        }

        TPublicValues& Rate(ui32 metric, ui64 delta) {
            Values.AddCumulative(metric);
            Values.AddCumulative(delta);
            return *this;
        }

        TPublicValues& Bucket(ui64 bucket, ui64 count, ui32 metric = PUBLIC_USED_CORE_PERCENTS) {
            auto* histogram = Values.MutableHistogram(metric);
            histogram->AddBuckets(bucket);
            histogram->AddBuckets(count);
            return *this;
        }

        NKikimrSysView::TDbCounters& Mutable() {
            return Values;
        }

        const NKikimrSysView::TDbCounters& Get() const {
            return Values;
        }

    private:
        NKikimrSysView::TDbCounters Values;
    };

    // A hand-built report of a node on the detailed wire
    class TPublicReport {
    public:
        TPublicReport& Table(const TPublicValues& values, const TString& path = TABLE_PATH,
                             TTabletTypes::EType type = TABLET_TYPE) {
            auto* entry = AddEntry(path, TDetailedMetricsSettings::MetricsLevelTable, type);
            *entry->MutableTableMetrics() = values.Get();
            return *this;
        }

        TPublicReport& Leaf(ui64 tabletId, ui32 followerId, const TPublicValues& values,
                            const TString& path = TABLE_PATH, TTabletTypes::EType type = TABLET_TYPE) {
            auto* leaf = GetOrAddPartitionEntry(path, type)->AddLeaves();
            leaf->SetTabletId(tabletId);
            leaf->SetFollowerId(followerId);
            *leaf->MutableMetrics() = values.Get();
            return *this;
        }

        const TTables& Get() const {
            return Tables;
        }

    private:
        NKikimrSysView::TDetailedTableCounters* AddEntry(
            const TString& path, TDetailedMetricsSettings::EMetricsLevel level, TTabletTypes::EType type) {
            auto* entry = Tables.Add();
            entry->SetTablePath(path);
            entry->SetLevel(level);
            entry->SetTabletType(type);
            return entry;
        }

        NKikimrSysView::TDetailedTableCounters* GetOrAddPartitionEntry(const TString& path, TTabletTypes::EType type) {
            for (auto& entry : Tables) {
                if (entry.GetTablePath() == path
                    && entry.GetLevel() == TDetailedMetricsSettings::MetricsLevelPartition
                    && entry.GetTabletType() == type)
                {
                    return &entry;
                }
            }
            return AddEntry(path, TDetailedMetricsSettings::MetricsLevelPartition, type);
        }

        TTables Tables;
    };

    // used_core_percents made a derivative histogram: the DataShard rollup still finds every target
    const TDetailedMetricsDescriptor* GetDerivativeHistogramDescriptor(TTabletTypes::EType type) {
        static const TDetailedMetricsDescriptor descriptor = [] {
            auto result = GetDataShardDescriptor();
            result.Histograms[PUBLIC_USED_CORE_PERCENTS].NonDerivative = false;
            return result;
        }();
        return type == TABLET_TYPE ? &descriptor : nullptr;
    }

    struct TProcessorFixture {
        explicit TProcessorFixture(TDetailedMetricsDescriptorGetter getDescriptor = &GetDetailedMetricsDescriptor)
            : Processor(CreateProcessorDatabaseMetricsAggregator(PublicRoot, DATABASE_PATH, getDescriptor))
        {
        }

        NMonitoring::TDynamicCounterPtr PublicRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TProcessorDatabaseMetricsAggregatorPtr Processor;

        void ApplyNode(ui32 nodeId, TSimulatedNode& node) {
            NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> tables;
            node.Leaders->Pack(tables);
            Processor->ApplyFromNode(nodeId, false, tables);
            tables.Clear();
            node.Followers->Pack(tables);
            Processor->ApplyFromNode(nodeId, true, tables);
        }

        void ApplyPublicNode(ui32 nodeId, const TPublicReport& leaders, const TPublicReport& followers = TPublicReport()) {
            Processor->ApplyFromNode(nodeId, false, leaders.Get());
            Processor->ApplyFromNode(nodeId, true, followers.Get());
        }

        TString DumpPublicSeries() const {
            return NDetailedMetricsTests::NormalizeJson(NMonitoring::ToJson(*PublicRoot));
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

    NMonitoring::IHistogramSnapshotPtr GetPublicHistogramSnapshot(
        NMonitoring::TDynamicCounterPtr publicGroup,
        const TString& publicName = "table.datashard.used_core_percents")
    {
        UNIT_ASSERT_C(publicGroup, "no counter group for the histogram " << publicName);
        auto histogram = publicGroup->FindNamedHistogram("name", publicName);
        UNIT_ASSERT_C(histogram, "no histogram " << publicName);
        return histogram->Snapshot();
    }

    ui64 GetHistogramTotal(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name,
                           const TString& label = "name")
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

    void AssertCpuHistogram(NMonitoring::TDynamicCounterPtr publicGroup, ui64 expectedTotal,
                            const TString& publicName = "table.datashard.used_core_percents")
    {
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(publicGroup, publicName, "name"), expectedTotal);
    }

    void AssertNoWrappedBuckets(NMonitoring::TDynamicCounterPtr publicGroup,
                                const TString& histogramName = "table.datashard.used_core_percents") {
        auto histogram = publicGroup->FindNamedHistogram("name", histogramName);
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

    TString DumpNonEmptyBuckets(NMonitoring::TDynamicCounterPtr publicGroup,
                                const TString& publicName = "table.datashard.used_core_percents") {
        auto snapshot = GetPublicHistogramSnapshot(publicGroup, publicName);
        TStringBuilder result;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            if (snapshot->Value(i)) {
                if (!result.empty()) {
                    result << ",";
                }
                result << i << ":" << snapshot->Value(i);
            }
        }
        return result;
    }

    // Counts the series of a group without subgroups
    size_t CountSeries(NMonitoring::TDynamicCounterPtr group) {
        UNIT_ASSERT(group);
        return group->ReadSnapshot().size();
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

        // Leader leaf (1000,0) has partition-scoped names and LeaderOnly metrics
        auto leaf1000_0 = FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(leaf1000_0, "table.datashard.partition.row_count"), 10u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(leaf1000_0, "table.datashard.partition.read.rows"), 6u);

        // Follower leaf (1000,1) has partition-scoped names but no LeaderOnly metrics
        auto leaf1000_1 = FindPublicLeafGroup(fixture.PublicRoot, 1000, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(leaf1000_1, "table.datashard.partition.read.rows"), 11u);
        UNIT_ASSERT(!leaf1000_1->FindNamedCounter("name", "table.datashard.partition.row_count"));

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
        leader1.AddCumulative(CONSUMED_CPU, 3)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(5));
        node1.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot) == oldTable);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(oldTable, "table.datashard.row_count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(oldTable, "table.datashard.consumed_cpu_us"), 8);
        AssertCpuHistogram(oldTable, 0);

        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));

        TFakeTablet recreated(1000, 0);
        recreated.SetSimple(DB_UNIQUE_ROWS_TOTAL, 4).AddCumulative(CONSUMED_CPU, 2).Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(10));
        fixture.ApplyNode(1, node1);
        fixture.Processor->RecalculateAllCounters();
        auto newTable = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(newTable != oldTable);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.row_count"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.consumed_cpu_us"), 2);
        AssertCpuHistogram(newTable, 1);
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

        // The same tablet moves from the zero-rate bucket to the first positive bucket.
        // Leaf uses partition-scoped name, table uses aggregate name.
        auto leafGroup = FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(leafGroup);
        UNIT_ASSERT(tableGroup);
        AssertCpuHistogram(leafGroup, 1, "table.datashard.partition.used_core_percents");
        auto leafHistogram = leafGroup->FindNamedHistogram("name", "table.datashard.partition.used_core_percents");
        UNIT_ASSERT(leafHistogram);
        auto leafSnapshot = leafHistogram->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(leafSnapshot->Value(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(leafSnapshot->Value(1), 1);

        auto tableHistogram = tableGroup->FindNamedHistogram("name", "table.datashard.used_core_percents");
        UNIT_ASSERT(tableHistogram);
        auto tableSnapshot = tableHistogram->Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(tableSnapshot->Value(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(tableSnapshot->Value(1), 1);
    }

    Y_UNIT_TEST(PartitionMoveTransientDoesNotDoubleAGauge) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leaderOnNode1(1000, 0);
        leaderOnNode1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 55)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet leaderOnNode2(1000, 0);
        leaderOnNode2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 50)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 55u);
        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 50u);
    }

    Y_UNIT_TEST(LeaflessPartitionMessageLeavesNoGroupBehind) {
        TProcessorFixture fixture;

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> leafless;
        auto* entry = leafless.Add();
        entry->SetTablePath(TABLE_PATH);
        entry->SetTabletType(TABLET_TYPE);
        entry->SetLevel(TDetailedMetricsSettings::MetricsLevelPartition);

        fixture.Processor->ApplyFromNode(1, false, leafless);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
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
        }
    }

    Y_UNIT_TEST(NodeRemovalDropsNonDerivativeHistogramsAndRetainsCumulativeHistory) {
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
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            AssertCpuHistogram(publicBucket, 2, histogramName);

            // An unchanged report repeats the whole non-derivative histogram of the node, which replaces
            // the previous one and still holds the tablet's observation.
            fixture.ApplyNode(1, node1);
            fixture.Processor->DropNode(1);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 20);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
            AssertCpuHistogram(publicBucket, 1, histogramName);
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);

            second.AddCumulative(CONSUMED_CPU, 3)
                .Report(node2.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);
            AssertCpuHistogram(publicBucket, 1, histogramName);
            auto publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL(publicSnapshot->Value(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(publicSnapshot->Value(1), 1);

            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();
            AssertCpuHistogram(publicBucket, 1, histogramName);
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
        auto* histogram = tables.Mutable(0)->MutableTableMetrics()->MutableHistogram(PUBLIC_USED_CORE_PERCENTS);
        const auto knownBucketCount = histogram->GetBucketsCount();
        histogram->SetBucketsCount(knownBucketCount + 1);
        histogram->AddBuckets(knownBucketCount);
        histogram->AddBuckets(1);

        fixture.Processor->ApplyFromNode(1, false, tables);
        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        auto table = FindPublicTableGroup(fixture.PublicRoot);
        AssertCpuHistogram(table, 2);

        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();
        AssertCpuHistogram(table, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);
    }

    Y_UNIT_TEST(RetiredTabletFinalSnapshotEmptiesHistogramAndStaysGoneOnNodeAbsence) {
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
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            AssertCpuHistogram(publicBucket, 2, histogramName);

            // The final report of the retired tablet is an empty histogram, which replaces its
            // observation.
            node1.Leaders->ForgetTablet(first.TabletId, first.FollowerId);
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();
            AssertCpuHistogram(publicBucket, 1, histogramName);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 12);

            // The node reports nothing for the retired tablet afterwards: nothing is repeated.
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();
            AssertCpuHistogram(publicBucket, 1, histogramName);
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
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 20);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 15);
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);

        fixture.Processor->DropNode(1);
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        TSimulatedNode replacementNode;
        TFakeTablet replacement(1000, 0);
        replacement.AddCumulative(CONSUMED_CPU, 2)
            .Report(replacementNode.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, replacementNode);
        fixture.Processor->RecalculateAllCounters();
        auto recreatedTable = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(recreatedTable != table);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(recreatedTable, "table.datashard.consumed_cpu_us"), 2);
        AssertCpuHistogram(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0), 1, "table.datashard.partition.used_core_percents");
        AssertCpuHistogram(recreatedTable, 1);
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
        AssertCpuHistogram(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0), 0, "table.datashard.partition.used_core_percents");
        AssertCpuHistogram(retiredTable, 0);

        fixture.ApplyNode(2, node2);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
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
        // DropNode retires the gauges and non-derivative histograms but keeps the cumulative history, matching
        // NodeRemovalDropsNonDerivativeHistogramsAndRetainsCumulativeHistory.
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 22u);
    }

    Y_UNIT_TEST(SingleNodeResumeAfterDropRebuildsNonDerivativeHistogram) {
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

            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            auto publicBucket = tableLevel
                                    ? FindPublicTableGroup(fixture.PublicRoot)
                                    : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto snapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Value(1), 1, DumpBuckets(snapshot.Get()));

            // Drop the node. This erases the bucket and the entire TTableEntry,
            // destroying all group pointers (the counters are detached from the tree).
            fixture.Processor->DropNode(1);

            // NOW+2s: Third report (rate 150000 -> bucket 2). The sender kept its baseline, but
            // its report carries the whole non-derivative histogram, not a change against the baseline.
            // The recreated processor state therefore starts from exactly the sender's state.
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Re-find the groups after the recreation (previous groups were destroyed).
            auto table = FindPublicTableGroup(fixture.PublicRoot);
            publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);

            // The recreated bucket has only node1's current observation: bucket 2 holds the
            // tablet, bucket 1 is empty.
            snapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Value(1), 0, DumpBuckets(snapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Value(2), 1, DumpBuckets(snapshot.Get()));
            AssertNoWrappedBuckets(publicBucket, histogramName);
            // The histogram total is 1 (only the new observation in bucket 2).
            AssertCpuHistogram(table, 1);
            // The cumulative total is 150000: the recreated entry sees only the new delta.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 150000);
        }
    }

    Y_UNIT_TEST(NonDerivativeHistogramsAreIsolatedPerNode) {
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
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            auto publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 2, DumpBuckets(publicSnapshot.Get()));
            AssertCpuHistogram(table, 2);

            // Drop node1. The bucket survives on node2, so the group pointers stay valid.
            fixture.Processor->DropNode(1);
            fixture.Processor->RecalculateAllCounters();
            publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 1, DumpBuckets(publicSnapshot.Get()));

            // NOW+2s: Node1 moves to bucket 2 (rate 150000). Node2 does not report.
            // Node1's report replaces only node1's own counts, node2's stay in bucket 1.
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Node1 moved from bucket 1 to bucket 2, node2 still in bucket 1.
            publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 1, DumpBuckets(publicSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(2), 1, DumpBuckets(publicSnapshot.Get()));
            AssertNoWrappedBuckets(publicBucket, histogramName);
            AssertCpuHistogram(table, 2);
            // Cumulative counters are append-only and the bucket survived the drop (node2
            // still holds it), so the total is every diff ever applied:
            // node1 50000 + 50000 + 150000, node2 70000 + 70000 = 390000.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 390000);
        }
    }

    Y_UNIT_TEST(NonDerivativeReplacementDoesNotAffectCumulativeHistory) {
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
            // The cumulative total must not change: the replacement affects only non-derivative
            // histograms, not cumulative counters (which are append-only).
            fixture.Processor->DropNode(1);
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // Cumulative counters are append-only. The total is 50000 + 150000 + 30000 = 230000,
            // never affected by the replacement of the non-derivative histogram bucket counts.
            // At partition level, DropNode removes node1's leaf source from the rollup via source
            // removal and re-registration. The rollup's RetainOnSourceRemoval history plus the
            // re-added source correctly recovers the cumulative (confirmed).
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 230000);
            // The non-derivative histogram holds node2's idle tablet (bucket 0) and node1's resumed one.
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 2);
        }
    }

    Y_UNIT_TEST(ProcessorRestartRestoresNonDerivativeHistogramsFromNextReport) {
        // When the processor is restarted with an empty in-memory aggregator, but the nodes
        // keep their baselines, every report still carries the whole non-derivative histogram of its node.
        // A node's observation is therefore back as soon as the node reports to the new
        // processor, whether or not the tablet changed its bucket.
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

            // Simulate processor restart: new fixture, node baselines unchanged.
            TProcessorFixture newFixture;

            // NOW+2s: Node1 moves to bucket 2 (rate 150000). Apply to the new processor only.
            first.AddCumulative(CONSUMED_CPU, 150000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            newFixture.ApplyNode(1, node1);
            newFixture.Processor->RecalculateAllCounters();

            // Node1's observation is in bucket 2, whatever the lost state was. Node2 has not
            // reported to the new processor yet, so bucket 1 is empty for now.
            auto newTable = FindPublicTableGroup(newFixture.PublicRoot);
            auto newPublicBucket = tableLevel ? newTable : FindPublicLeafGroup(newFixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            auto newPublicSnapshot = GetPublicHistogramSnapshot(newPublicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(newPublicSnapshot->Value(1), 0, DumpBuckets(newPublicSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(newPublicSnapshot->Value(2), 1, DumpBuckets(newPublicSnapshot.Get()));
            AssertNoWrappedBuckets(newPublicBucket, histogramName);
            AssertCpuHistogram(newTable, 1);
            // The cumulative total is 150000 (only node1's delta): cumulative history is
            // delta based and is not recovered from before the restart.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.consumed_cpu_us"), 150000);

            // NOW+2s: Node2 reports to the new processor with the SAME rate as before (70000),
            // so its tablet does not change the bucket. Its observation is restored regardless.
            second.AddCumulative(CONSUMED_CPU, 70000)
                .Report(node2.Leaders, level, NOW + TDuration::Seconds(2));
            newFixture.ApplyNode(2, node2);
            newFixture.Processor->RecalculateAllCounters();

            newPublicSnapshot = GetPublicHistogramSnapshot(newPublicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(newPublicSnapshot->Value(1), 1, DumpBuckets(newPublicSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(newPublicSnapshot->Value(2), 1, DumpBuckets(newPublicSnapshot.Get()));
            AssertNoWrappedBuckets(newPublicBucket, histogramName);
            AssertCpuHistogram(newTable, 2);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(newTable, "table.datashard.consumed_cpu_us"), 220000);
        }
    }

    Y_UNIT_TEST(NonDerivativeUnchangedObservationRestoredAfterNodeResume) {
        // After dropping a node and resuming it with the same rate (no rate change), the node
        // still reports its whole non-derivative histogram, so its observation is counted again.
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
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            auto publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 2, DumpBuckets(publicSnapshot.Get()));
            AssertCpuHistogram(table, 2);

            // Drop node1. The bucket survives on node2, so the group pointers stay valid.
            fixture.Processor->DropNode(1);

            // NOW+2s: Node1 reports again with the same rate (50000 over 1s), so its tablet stays
            // in bucket 1 and nothing changes on the sender side.
            first.AddCumulative(CONSUMED_CPU, 50000)
                .Report(node1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, node1);
            fixture.Processor->RecalculateAllCounters();

            // The report carries node1's whole non-derivative histogram, so its observation is back
            // in bucket 1 next to node2's.
            publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 2, DumpBuckets(publicSnapshot.Get()));
            AssertCpuHistogram(table, 2);
            AssertNoWrappedBuckets(publicBucket, histogramName);
            // Cumulative history is preserved as well: node1's third report carries its
            // cumulative delta, and the bucket survived the drop on node2, so nothing was unwound.
            // Total: node1 3 x 50000 + node2 2 x 70000 = 290000.
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 290000);
        }
    }

    Y_UNIT_TEST(RedeliveredReportDoesNotDoubleNonDerivativeHistogram) {
        // A retried or duplicated report holds the same full non-derivative histogram, which
        // replaces the one applied before. Cumulative counters are deltas and would double,
        // so only the non-derivative histogram is asserted.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0);

            // Rate 50000 on the second report -> bucket 1.
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW + TDuration::Seconds(1));

            NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> tables;
            node1.Leaders->Pack(tables);
            fixture.Processor->ApplyFromNode(1, false, tables);
            fixture.Processor->ApplyFromNode(1, false, tables);
            fixture.Processor->RecalculateAllCounters();

            auto table = FindPublicTableGroup(fixture.PublicRoot);
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            auto publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(0), 0, DumpBuckets(publicSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 1, DumpBuckets(publicSnapshot.Get()));
            AssertCpuHistogram(publicBucket, 1, histogramName);
        }
    }

    Y_UNIT_TEST(NodeRestartReplacesNonDerivativeHistogramOfTheLostNode) {
        // A restarted node starts from an empty baseline, while the processor still holds the
        // node's previous observations. The first report of the restarted node replaces them.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(tableLevel ? 2000 : 1000, 0);

            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW + TDuration::Seconds(1));
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            auto table = FindPublicTableGroup(fixture.PublicRoot);
            auto publicBucket = tableLevel ? table : FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
            auto histogramName = tableLevel ? "table.datashard.used_core_percents" : "table.datashard.partition.used_core_percents";
            auto publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(0), 1, DumpBuckets(publicSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 1, DumpBuckets(publicSnapshot.Get()));
            AssertCpuHistogram(publicBucket, 2, histogramName);

            // Node1 restarts: a fresh aggregator, the tablet has its first report (rate 0 -> bucket 0).
            // The processor has not dropped node1 in between.
            TSimulatedNode restartedNode1;
            TFakeTablet restarted(1000, 0);
            restarted.AddCumulative(CONSUMED_CPU, 10000)
                .Report(restartedNode1.Leaders, level, NOW + TDuration::Seconds(2));
            fixture.ApplyNode(1, restartedNode1);
            fixture.Processor->RecalculateAllCounters();

            // The old bucket 1 observation of node1 is gone rather than kept next to the new one.
            publicSnapshot = GetPublicHistogramSnapshot(publicBucket, histogramName);
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(0), 2, DumpBuckets(publicSnapshot.Get()));
            UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(1), 0, DumpBuckets(publicSnapshot.Get()));
            AssertCpuHistogram(publicBucket, 2, histogramName);
        }
    }

    Y_UNIT_TEST(UnmarkedNonDerivativeHistogramIsIgnored) {
        // A non-derivative histogram that is not marked NonDerivative is a delta, which cannot
        // be applied without the baseline it was taken against: the node contributes nothing
        // to it, but the rest of its report is applied as usual.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(2000, 0);
            first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5).Report(node1.Leaders, level, NOW);
            second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 7).Report(node2.Leaders, level, NOW);

            NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> tables;
            node1.Leaders->Pack(tables);
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            auto* values = tableLevel
                               ? tables.Mutable(0)->MutableTableMetrics()
                               : tables.Mutable(0)->MutableLeaves(0)->MutableMetrics();
            auto* histogram = values->MutableHistogram(PUBLIC_USED_CORE_PERCENTS);
            UNIT_ASSERT(histogram->GetNonDerivative());
            histogram->ClearNonDerivative();

            fixture.Processor->ApplyFromNode(1, false, tables);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            auto table = FindPublicTableGroup(fixture.PublicRoot);
            UNIT_ASSERT(table);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 10u + 20u);
            UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 5u + 7u);
            // Only node2 contributes to the histogram.
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(table, "table.datashard.used_core_percents", "name"), 1);
            if (tableLevel) {
                AssertCpuHistogram(table, 1);
            } else {
                AssertCpuHistogram(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0), 0, "table.datashard.partition.used_core_percents");
                AssertCpuHistogram(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0), 1, "table.datashard.partition.used_core_percents");
            }
        }
    }

    Y_UNIT_TEST(ProcessorRestartRestoresIdleTabletsFromNextReports) {
        // Idle tablets never change their bucket, so a delta encoding would never report them
        // again. Every report carries the whole non-derivative histogram of the node instead.
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TSimulatedNode node1, node2;
            TProcessorFixture fixture;
            TFakeTablet first(1000, 0), second(2000, 0);

            // Rate 0 -> bucket 0 for both, and they stay there.
            first.AddCumulative(CONSUMED_CPU, 50000).Report(node1.Leaders, level, NOW);
            second.AddCumulative(CONSUMED_CPU, 70000).Report(node2.Leaders, level, NOW);
            fixture.ApplyNode(1, node1);
            fixture.ApplyNode(2, node2);
            fixture.Processor->RecalculateAllCounters();

            // Simulate processor restart: new fixture, node baselines unchanged, nothing reported
            // since the tablets are idle.
            TProcessorFixture newFixture;
            newFixture.ApplyNode(1, node1);
            newFixture.ApplyNode(2, node2);
            newFixture.Processor->RecalculateAllCounters();

            auto newTable = FindPublicTableGroup(newFixture.PublicRoot);
            UNIT_ASSERT(newTable);
            UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(newTable, "table.datashard.used_core_percents", "name"), 2);
            if (tableLevel) {
                AssertCpuHistogram(newTable, 2);
                auto publicSnapshot = GetPublicHistogramSnapshot(newTable);
                UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(0), 2, DumpBuckets(publicSnapshot.Get()));
            } else {
                for (ui64 tabletId : {1000, 2000}) {
                    auto leafGroup = FindPublicLeafGroup(newFixture.PublicRoot, tabletId, 0);
                    AssertCpuHistogram(leafGroup, 1, "table.datashard.partition.used_core_percents");
                    auto publicSnapshot = GetPublicHistogramSnapshot(leafGroup, "table.datashard.partition.used_core_percents");
                    UNIT_ASSERT_VALUES_EQUAL_C(publicSnapshot->Value(0), 1, DumpBuckets(publicSnapshot.Get()));
                }
            }
        }
    }

    Y_UNIT_TEST(PublicNonDerivativeHistogramReplacesTheHistogramOfTheNode) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool tableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            TProcessorFixture fixture;
            const auto report = [tableLevel](const TPublicValues& values) {
                return tableLevel ? TPublicReport().Table(values) : TPublicReport().Leaf(1000, 0, values);
            };
            const auto dumpHistogram = [&]() {
                fixture.Processor->RecalculateAllCounters();
                return tableLevel
                    ? DumpNonEmptyBuckets(FindPublicTableGroup(fixture.PublicRoot))
                    : DumpNonEmptyBuckets(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0),
                                          "table.datashard.partition.used_core_percents");
            };

            fixture.ApplyPublicNode(1, report(TPublicValues().Bucket(1, 2)));
            fixture.ApplyPublicNode(2, report(TPublicValues().Bucket(0, 1)));
            UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "0:1,1:2");

            fixture.ApplyPublicNode(1, report(TPublicValues().Bucket(3, 2)));
            UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "0:1,3:2");

            // An unmarked non-derivative histogram is not applied and leaves the histogram of the node empty
            TPublicValues unmarked;
            unmarked.Bucket(5, 9).Mutable().MutableHistogram(PUBLIC_USED_CORE_PERCENTS)->ClearNonDerivative();
            fixture.ApplyPublicNode(1, report(unmarked));
            UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "0:1");

            fixture.ApplyPublicNode(1, report(TPublicValues().Bucket(3, 2)));
            UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "0:1,3:2");

            fixture.Processor->DropNode(2);
            UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "3:2");

            TPublicValues missing;
            missing.Mutable().ClearHistogram();
            fixture.ApplyPublicNode(1, report(missing));
            UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "");
        }
    }

    Y_UNIT_TEST(PublicValuesOfFollowerLeafHaveNoLeaderOnlySeries) {
        TProcessorFixture fixture;
        fixture.ApplyPublicNode(1,
            TPublicReport().Leaf(1000, 0, TPublicValues()
                .Gauge(PUBLIC_ROW_COUNT, 10).Rate(PUBLIC_WRITE_ROWS, 4).Rate(PUBLIC_READ_ROWS, 6).Bucket(0, 1)),
            TPublicReport().Leaf(1000, 1, TPublicValues()
                .Gauge(PUBLIC_ROW_COUNT, 999).Rate(PUBLIC_WRITE_ROWS, 999).Rate(PUBLIC_READ_ROWS, 11).Bucket(1, 1)));
        fixture.Processor->RecalculateAllCounters();

        auto leader = FindPublicLeafGroup(fixture.PublicRoot, 1000, 0);
        auto follower = FindPublicLeafGroup(fixture.PublicRoot, 1000, 1);
        UNIT_ASSERT(leader);
        UNIT_ASSERT(follower);
        // DataShard has 2 gauges, 13 rates and 1 histogram, 8 of them (both gauges and 6 rates) LeaderOnly
        UNIT_ASSERT_VALUES_EQUAL(CountSeries(leader), 16);
        UNIT_ASSERT_VALUES_EQUAL(CountSeries(follower), 8);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(leader, "table.datashard.partition.row_count"), 10);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(leader, "table.datashard.partition.write.rows"), 4);
        UNIT_ASSERT(!follower->FindNamedCounter("name", "table.datashard.partition.row_count"));
        UNIT_ASSERT(!follower->FindNamedCounter("name", "table.datashard.partition.write.rows"));
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(follower, "table.datashard.partition.read.rows"), 11);
        UNIT_ASSERT_VALUES_EQUAL(DumpNonEmptyBuckets(follower, "table.datashard.partition.used_core_percents"), "1:1");

        auto table = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 10);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.write.rows"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.read.rows"), 17);
        UNIT_ASSERT_VALUES_EQUAL(DumpNonEmptyBuckets(table), "0:1,1:1");

        // A follower leaf in the report of the leader role is ignored
        fixture.Processor->ApplyFromNode(2, false, TPublicReport().Leaf(2000, 1, TPublicValues()
            .Gauge(PUBLIC_ROW_COUNT, 999)).Get());
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 2000, 1));
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 10);
    }

    Y_UNIT_TEST(MalformedPublicValuesAreClampedWithoutAbort) {
        TProcessorFixture fixture;

        TPublicValues values;
        auto& counters = values.Mutable();
        // A short Simple: the missing gauges are zero
        counters.ClearSimple();
        counters.AddSimple(7);
        // Unknown rates and an odd tail are ignored, CumulativeCount sizes nothing
        counters.SetCumulativeCount(Max<ui64>());
        for (ui64 value : {ui64(PUBLIC_CONSUMED_CPU_US), ui64(5), ui64(1000), ui64(9), Max<ui64>(), ui64(1),
                           ui64(PUBLIC_CONSUMED_CPU_US)}) {
            counters.AddCumulative(value);
        }
        // A huge BucketsCount is clamped to the public bucket count; a bucket beyond it, an odd tail
        // and an extra histogram entry are ignored
        auto* histogram = counters.MutableHistogram(PUBLIC_USED_CORE_PERCENTS);
        histogram->SetBucketsCount(Max<ui64>());
        for (ui64 value : {ui64(1000000), ui64(1), ui64(2), ui64(3), Max<ui64>()}) {
            histogram->AddBuckets(value);
        }
        auto* extra = counters.AddHistogram();
        extra->SetBucketsCount(Max<ui64>());
        extra->SetNonDerivative(true);
        extra->AddBuckets(0);
        extra->AddBuckets(5);

        fixture.ApplyPublicNode(1, TPublicReport().Table(values));
        fixture.Processor->RecalculateAllCounters();

        auto table = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(table);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 7);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.size_bytes"), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.consumed_cpu_us"), 5);
        UNIT_ASSERT_VALUES_EQUAL(DumpNonEmptyBuckets(table), "2:3");

        // A long Simple: the extra slots are ignored, and so is a bucket beyond the BucketsCount of the payload
        TPublicValues longValues;
        longValues.Mutable().ClearSimple();
        for (ui64 value : {1, 2, 3, 4}) {
            longValues.Mutable().AddSimple(value);
        }
        auto* shortHistogram = longValues.Mutable().MutableHistogram(PUBLIC_USED_CORE_PERCENTS);
        shortHistogram->SetBucketsCount(2);
        for (ui64 value : {1, 4, 5, 6}) {
            shortHistogram->AddBuckets(value);
        }
        fixture.ApplyPublicNode(2, TPublicReport().Table(longValues));
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.row_count"), 8);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(table, "table.datashard.size_bytes"), 2);
        UNIT_ASSERT_VALUES_EQUAL(DumpNonEmptyBuckets(table), "1:4,2:3");

        TPublicValues empty;
        empty.Mutable().Clear();
        fixture.ApplyPublicNode(3, TPublicReport().Leaf(2000, 0, empty, OTHER_TABLE_PATH));
        fixture.Processor->RecalculateAllCounters();
        auto emptyLeaf = FindPublicLeafGroup(fixture.PublicRoot, 2000, 0, RELATIVE_OTHER_TABLE_PATH);
        UNIT_ASSERT(emptyLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(emptyLeaf, "table.datashard.partition.row_count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(emptyLeaf, "table.datashard.partition.consumed_cpu_us"), 0);
        UNIT_ASSERT_VALUES_EQUAL(DumpNonEmptyBuckets(emptyLeaf, "table.datashard.partition.used_core_percents"), "");
    }

    Y_UNIT_TEST(EntriesWithoutTabletTypeAreIgnored) {
        TProcessorFixture fixture;

        TTables tables = TPublicReport()
            .Table(TPublicValues().Gauge(PUBLIC_ROW_COUNT, 10).Rate(PUBLIC_CONSUMED_CPU_US, 5).Bucket(0, 1))
            .Leaf(2000, 0, TPublicValues().Gauge(PUBLIC_ROW_COUNT, 20).Rate(PUBLIC_CONSUMED_CPU_US, 7).Bucket(2, 1),
                  OTHER_TABLE_PATH)
            .Get();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 2);
        tables.Mutable(0)->ClearTabletType();

        fixture.Processor->ApplyFromNode(1, false, tables);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        auto other = FindPublicTableGroup(fixture.PublicRoot, RELATIVE_OTHER_TABLE_PATH);
        auto leaf = FindPublicLeafGroup(fixture.PublicRoot, 2000, 0, RELATIVE_OTHER_TABLE_PATH);
        UNIT_ASSERT(other);
        UNIT_ASSERT(leaf);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(other, "table.datashard.row_count"), 20);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(other, "table.datashard.consumed_cpu_us"), 7);
        UNIT_ASSERT_VALUES_EQUAL(DumpNonEmptyBuckets(leaf, "table.datashard.partition.used_core_percents"), "2:1");

        // Entries without the tablet type count as unreported, so the buckets of the node retire
        for (auto& entry : tables) {
            entry.ClearTabletType();
        }
        fixture.Processor->ApplyFromNode(1, false, tables);
        fixture.Processor->RecalculateAllCounters();
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot, RELATIVE_OTHER_TABLE_PATH));
    }

    Y_UNIT_TEST(PackedAndHandBuiltPayloadsPublishTheSameSeries) {
        TSimulatedNode node1, node2;
        TProcessorFixture packed, published;
        TFakeTablet leader(1000, 0), follower(1000, 1), first(2000, 0), second(2001, 0);

        const auto assertSameSeries = [&]() {
            packed.Processor->RecalculateAllCounters();
            published.Processor->RecalculateAllCounters();
            UNIT_ASSERT_VALUES_EQUAL(published.DumpPublicSeries(), packed.DumpPublicSeries());
        };

        // First reports: zero rates put every tablet in bucket 0
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10)
            .AddCumulative(CONSUMED_CPU, 50000)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 4)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 6)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        follower.AddCumulative(CONSUMED_CPU, 20000)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, 999)
            .AddAppCumulative(NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW, 11)
            .Report(node1.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        first.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 70000)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW, OTHER_TABLE_PATH);
        second.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5).AddCumulative(CONSUMED_CPU, 1000)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW, OTHER_TABLE_PATH);
        packed.ApplyNode(1, node1);
        packed.ApplyNode(2, node2);

        published.ApplyPublicNode(1,
            TPublicReport().Leaf(1000, 0, TPublicValues()
                .Gauge(PUBLIC_ROW_COUNT, 10)
                .Rate(PUBLIC_WRITE_ROWS, 4).Rate(PUBLIC_READ_ROWS, 6).Rate(PUBLIC_CONSUMED_CPU_US, 50000)
                .Bucket(0, 1)),
            TPublicReport().Leaf(1000, 1, TPublicValues()
                .Rate(PUBLIC_WRITE_ROWS, 999).Rate(PUBLIC_READ_ROWS, 11).Rate(PUBLIC_CONSUMED_CPU_US, 20000)
                .Bucket(0, 1)));
        published.ApplyPublicNode(2, TPublicReport().Table(TPublicValues()
            .Gauge(PUBLIC_ROW_COUNT, 25).Rate(PUBLIC_CONSUMED_CPU_US, 71000).Bucket(0, 2), OTHER_TABLE_PATH));
        assertSameSeries();
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(FindPublicTableGroup(published.PublicRoot), "table.datashard.read.rows"), 17);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(FindPublicTableGroup(published.PublicRoot, RELATIVE_OTHER_TABLE_PATH),
                                                       "table.datashard.row_count"), 25);

        // The tablets move to the buckets of their rates. The retiring follower's final report has
        // its last deltas and an empty non-derivative histogram
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 12).AddCumulative(CONSUMED_CPU, 50000)
            .Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(1));
        follower.AddCumulative(CONSUMED_CPU, 150000)
            .Report(node1.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(1));
        node1.Followers->ForgetTablet(follower.TabletId, follower.FollowerId);
        first.AddCumulative(CONSUMED_CPU, 70000)
            .Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(1), OTHER_TABLE_PATH);
        second.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(1), OTHER_TABLE_PATH);
        packed.ApplyNode(1, node1);
        packed.ApplyNode(2, node2);

        published.ApplyPublicNode(1,
            TPublicReport().Leaf(1000, 0, TPublicValues()
                .Gauge(PUBLIC_ROW_COUNT, 12).Rate(PUBLIC_CONSUMED_CPU_US, 50000).Bucket(1, 1)),
            TPublicReport().Leaf(1000, 1, TPublicValues()
                .Rate(PUBLIC_CONSUMED_CPU_US, 150000)));
        published.ApplyPublicNode(2, TPublicReport().Table(TPublicValues()
            .Gauge(PUBLIC_ROW_COUNT, 25).Rate(PUBLIC_CONSUMED_CPU_US, 70000).Bucket(0, 1).Bucket(1, 1), OTHER_TABLE_PATH));
        assertSameSeries();

        // Node 1 repeats its leader leaf (gauges and the full non-derivative histogram, no deltas) without
        // the retired follower leaf
        packed.Processor->DropNode(2);
        published.Processor->DropNode(2);
        packed.ApplyNode(1, node1);
        published.ApplyPublicNode(1, TPublicReport().Leaf(1000, 0, TPublicValues()
            .Gauge(PUBLIC_ROW_COUNT, 12).Bucket(1, 1)));
        assertSameSeries();
        UNIT_ASSERT(FindPublicLeafGroup(published.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindPublicLeafGroup(published.PublicRoot, 1000, 1));
        UNIT_ASSERT(!FindPublicTableGroup(published.PublicRoot, RELATIVE_OTHER_TABLE_PATH));
    }
    Y_UNIT_TEST(PublicLeafSeriesMatchTheClassicMapper) {
        TProcessorFixture fixture;
        fixture.ApplyPublicNode(1, TPublicReport().Leaf(1000, 0, TPublicValues()), TPublicReport().Leaf(1000, 1, TPublicValues()));

        for (ui32 followerId : {0, 1}) {
            auto classic = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto mapper = CreateYdbMetricsMapperByTabletType(TABLET_TYPE, classic, MakeIntrusive<NMonitoring::TDynamicCounters>(),
                EYdbMetricNameScope::Partition, followerId != 0);
            auto leaf = FindPublicLeafGroup(fixture.PublicRoot, 1000, followerId);
            UNIT_ASSERT(leaf);
            UNIT_ASSERT_VALUES_EQUAL_C(
                NDetailedMetricsTests::NormalizeJson(NMonitoring::ToJson(*leaf)),
                NDetailedMetricsTests::NormalizeJson(NMonitoring::ToJson(*classic)),
                "follower " << followerId);
        }
    }

    Y_UNIT_TEST(DerivativeHistogramAccumulatesOverNodesAndIgnoresNonDerivative) {
        TProcessorFixture fixture(&GetDerivativeHistogramDescriptor);
        const auto report = [](ui64 bucket, ui64 count) {
            TPublicValues values;
            values.Bucket(bucket, count).Mutable().MutableHistogram(PUBLIC_USED_CORE_PERCENTS)->ClearNonDerivative();
            return TPublicReport().Leaf(1000, 0, values);
        };
        const auto dumpHistogram = [&]() {
            fixture.Processor->RecalculateAllCounters();
            return DumpNonEmptyBuckets(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0), "table.datashard.partition.used_core_percents");
        };

        fixture.ApplyPublicNode(1, report(1, 2));
        fixture.ApplyPublicNode(2, report(1, 3));
        fixture.ApplyPublicNode(1, report(4, 1));
        UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "1:5,4:1");

        // The deltas of a removed node stay while another node reports the bucket
        fixture.Processor->DropNode(1);
        UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "1:5,4:1");

        // A histogram marked NonDerivative (as TPublicValues does) is ignored
        fixture.ApplyPublicNode(2, TPublicReport().Leaf(1000, 0, TPublicValues().Bucket(2, 7)));
        UNIT_ASSERT_VALUES_EQUAL(dumpHistogram(), "1:5,4:1");
    }
} // Y_UNIT_TEST_SUITE(TProcessorDatabaseMetricsAggregatorTest)
