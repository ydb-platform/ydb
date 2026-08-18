#include "processor_database_metrics_aggregator.h"

#include "node_database_metrics_aggregator.h"
#include "ut_helpers.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;

namespace {

const TString DATABASE_PATH = "/Root/db";

const TInstant NOW = TInstant::Seconds(100);

/**
 * One simulated node: a private raw root with the leader/follower Tablet
 * Counters Aggregator pair sharing it, exactly the shape a real node builds
 * (step 06/08) — the pair TFakeTablet::Report() is fed into, and Pack()ed
 * out of, per role.
 */
struct TSimulatedNode {
    NMonitoring::TDynamicCounterPtr Root = MakeIntrusive<NMonitoring::TDynamicCounters>();

    TNodeDatabaseMetricsAggregatorPtr Leaders = CreateNodeDatabaseMetricsAggregator(
        Root, DATABASE_PATH, false /* isFollowerRole */
    );
    TNodeDatabaseMetricsAggregatorPtr Followers = CreateNodeDatabaseMetricsAggregator(
        Root, DATABASE_PATH, true /* isFollowerRole */
    );
};

/**
 * The processor under test, fed the way the real wire transport (step
 * 11/12, not built yet) would: one Pack()ed message per role per node per
 * tick.
 *
 * @note The Executor category is initialized off MakeFakeExecutorCountersTemplate()
 *       (TFakeTablet's OWN layout), not the real NTabletFlatExecutor::
 *       TExecutorCounters: the wire format is positional, so the processor
 *       must Initialize() off the very same counter set TFakeTablet packed
 *       with (see that helper's doc comment in ut_helpers.h). The App
 *       category has no such injection point (TProcessorDatabaseMetricsAggregator
 *       always initializes it off the real CreateAppCountersByTabletType()),
 *       so this suite only asserts on Executor-sourced public metrics —
 *       table.datashard.row_count/size_bytes (Simple) and
 *       table.datashard.consumed_cpu_us/cache_*.bytes (Cumulative). That
 *       covers both a LeaderOnly metric (row_count) and an all-role one
 *       (consumed_cpu_us), which is everything the Cases below need.
 */
struct TProcessorFixture {
    NMonitoring::TDynamicCounterPtr RawRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
    NMonitoring::TDynamicCounterPtr PublicRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();

    TProcessorDatabaseMetricsAggregatorPtr Processor = CreateProcessorDatabaseMetricsAggregator(
        RawRoot,
        PublicRoot,
        DATABASE_PATH,
        MakeFakeExecutorCountersTemplate()
    );

    /**
     * Pack and apply BOTH role streams of one node for the given generation.
     */
    void ApplyNode(ui32 nodeId, TSimulatedNode& node, ui64 generation) {
        Processor->ApplyFromNode(nodeId, false /* isFollowerRole */, PackOnce(node.Leaders, generation));
        Processor->ApplyFromNode(nodeId, true /* isFollowerRole */, PackOnce(node.Followers, generation));
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

void DumpCounters(const TString& title, NMonitoring::TDynamicCounterPtr rootGroup) {
    Cerr << "TEST " << title << ":" << Endl
         << NormalizeJson(NMonitoring::ToJson(*rootGroup)) << Endl;
}

} // namespace

/**
 * Unit tests for the SysView Processor side database metrics aggregator
 * (TProcessorDatabaseMetricsAggregator, step 09).
 */
Y_UNIT_TEST_SUITE(TProcessorDatabaseMetricsAggregatorTest) {
    /**
     * Two nodes report different partitions of one PARTITION table (plus a
     * follower of one of them, on the second node): both leaves land in the
     * public tree; at table level a leader-only metric (row_count) is the
     * sum over follower_id == 0 leaves ONLY — the regression test against
     * 2x inflation by the replication factor — while an all-role metric
     * (consumed_cpu_us) sums every leaf, followers included. Dropping a
     * node then removes its leaves (and their groups, raw and public) and
     * shrinks the table level to what is left.
     */
    Y_UNIT_TEST(PartitionLevelUnionsLeavesLeaderOnlyMetricNotInflatedThenDropNodeShrinks) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        // node1 hosts the leader of partition 1000
        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // node2 hosts the leader of partition 2000...
        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 6);
        leader2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        // ... and ALSO a follower of partition 1000, with an inflated row count:
        // if the table level summed every leaf regardless of role, this alone
        // would push row_count to 10 + 20 + 999 instead of 10 + 20
        TFakeTablet follower1(1000, 1);
        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999).AddCumulative(CONSUMED_CPU, 7);
        follower1.Report(node2.Followers, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.ApplyNode(2, node2, 1);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree, 2 nodes, 3 leaves", fixture.PublicRoot);

        // All three leaves are present, addressed by (tablet_id, follower_id) alone
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u + 6u + 7u);

        // No replicas_only aggregate, and no partition (follower_id dropped)
        // rollup: the tablet_id= node itself carries no counters of its own
        UNIT_ASSERT(!tableGroup->FindSubgroup("follower_id", "replicas_only"));
        auto tabletGroup1000 = tableGroup->FindSubgroup("tablet_id", "1000");
        UNIT_ASSERT(tabletGroup1000);
        UNIT_ASSERT(!tabletGroup1000->FindNamedCounter("name", "table.datashard.row_count"));

        // Drop node2: its leaves (the second partition's leader AND the first
        // partition's follower) disappear, and the table level shrinks to what
        // node1 alone still contributes
        fixture.Processor->DropNode(2);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree after dropping node2", fixture.PublicRoot);

        UNIT_ASSERT(FindPublicLeafGroup(fixture.PublicRoot, 1000, 0));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 1000, 1));
        UNIT_ASSERT(!FindPublicLeafGroup(fixture.PublicRoot, 2000, 0));

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u);

        // The raw tree loses the very same leaves
        auto rawTableGroup = fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH);
        UNIT_ASSERT(rawTableGroup);
        auto rawPerPartition = rawTableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(rawPerPartition);
        UNIT_ASSERT(rawPerPartition->FindSubgroup("tablet_id", "1000"));
        UNIT_ASSERT(!rawPerPartition->FindSubgroup("tablet_id", "2000"));
    }

    /**
     * Two nodes report TABLE partials of one TABLE-level table: the table
     * level is their sum. A TABLE-level partial arriving on the follower
     * stream is rejected, not added — exercised on the release path (a
     * hand built message, since a real node's follower instance structurally
     * never packs a TABLE bucket, S1'').
     */
    Y_UNIT_TEST(TableLevelSumsPartialsAndRejectsFollowerPartial) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 5);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 6);
        leader2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.ApplyNode(2, node2, 1);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), 5u + 6u);

        // No per-partition leaves, and no replicas_only aggregate, for a
        // TABLE-level table
        UNIT_ASSERT(!tableGroup->FindSubgroup("tablet_id", "1000"));
        UNIT_ASSERT(!tableGroup->FindSubgroup("follower_id", "replicas_only"));

        // A TABLE-level partial on the follower stream: hand built, since
        // TNodeDatabaseMetricsAggregator's follower instance never packs one
        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> rejected;
        auto* bogus = rejected.Add();
        bogus->SetTablePath(TABLE_PATH);
        bogus->SetLevel(TDetailedMetricsSettings::MetricsLevelTable);
        bogus->MutableTableCounters()->SetType(TTabletTypes::DataShard);
        bogus->MutableTableCounters()->MutableExecutorCounters()->AddSimple(999999);

        fixture.Processor->ApplyFromNode(3, true /* isFollowerRole */, rejected);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree after the rejected follower TABLE partial", fixture.PublicRoot);

        // Dropped, not summed: the table level is exactly what it was before
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 10u + 20u);
    }

    /**
     * A gauge (Simple/GAUGE, absolute stateful, C2) dropping to 0 on one
     * node is reflected at the table level after the next
     * RecalculateAllCounters(): the per-node state is replaced, not merged,
     * on every ApplyFromNode() (the same trick the node's own AddCounters
     * relies on, carried one level up).
     */
    Y_UNIT_TEST(GaugeDropToZeroReflectedAfterRecalculate) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 42);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 42u);

        // The same tablet, on the same node, reports the gauge back down to 0
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 0);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));

        fixture.ApplyNode(1, node1, 2);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree after the gauge dropped to 0", fixture.PublicRoot);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 0u);
    }

    /**
     * A node that stops mentioning a table in a later message loses its
     * slot in that table's contributor set (the WHOLE per-message list is
     * compared against what the SAME role stream reported last time); when
     * the last contributor goes, the table's groups are removed from BOTH
     * the raw and the public tree.
     */
    Y_UNIT_TEST(NodeStoppingToMentionATableEvictsItFromBothTrees) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT(FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));

        // node1 stops mentioning the table (its Tablet Counters Aggregator forgot
        // the tablet: partition move / table drop / node loss). The NEXT Pack()
        // carries no entry for it at all
        node1.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        fixture.ApplyNode(1, node1, 2);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree after node1 stopped mentioning the table", fixture.PublicRoot);

        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));
    }
}
