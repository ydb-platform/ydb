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

////////////////////////////////////////////////////////////////////////////////
// RAW tree helpers, modelled on FindTableGroup/FindLeafGroup/FindCategoryCountersGroup/
// GetCounterValue/GetHistogramTotal in node_database_metrics_aggregator_ut.cpp: the
// processor's raw tree has the same shape below its own root (no database= label,
// the caller already scoped RawRoot to the role-agnostic private group), so these
// are the same helpers minus that one label.

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

    /**
     * Pins the P0: the node's Pack() diffs the Max pair against an EMPTY
     * baseline (detailed_counters_diff.cpp), i.e. it comes out DENSE-turned-
     * sparse exactly like the sum pair, with CumulativeCount set. Before the
     * fix, the processor's TAggregateCumulative<true> decoded it as if it
     * were still dense (bounded by CumulativeCount == 0 for an all-zero
     * baseline), so every cross-node MAX(<cumulative>) silently decoded to 0
     * no matter what the node sent. A single generation gives
     * MAX(ConsumedCPU) == 0 by construction (the aggregate is a per-second
     * RATE: TAggregatedTabletCounters::Apply needs a PREVIOUS report of the
     * SAME tablet to compute one), so this needs a second generation, 5 s
     * later, with its own delta, to produce the non-zero rate the whole
     * defect hinges on.
     */
    Y_UNIT_TEST(MaxCumulativeCounterRoundTripsThroughPackAndUnpack) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.AddCumulative(CONSUMED_CPU, 50);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.Processor->RecalculateAllCounters();

        auto leafExecutorCounters = FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafExecutorCounters, "MAX(ConsumedCPU)"), 0u);

        leader1.AddCumulative(CONSUMED_CPU, 100);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));

        fixture.ApplyNode(1, node1, 2);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Raw tree after the second generation", fixture.RawRoot);

        // 100 over the 5 s gap: non-zero only if the encode (dense-turned-sparse,
        // node side) and the decode (sparse, processor side) finally agree
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafExecutorCounters, "MAX(ConsumedCPU)"), 100u / 5u);
    }

    /**
     * No existing test applies more than one generation with a Cumulative
     * assertion: this pins that a Cumulative/HIST-sourced public metric
     * (table.datashard.consumed_cpu_us) is the running total of EVERY
     * generation's delta from EVERY node (AggregateIncrementalTabletCounters
     * sums straight into TCrossNodeEntry::Accumulator on every ApplyDelta,
     * never resets it), not just the latest generation's snapshot the way a
     * Simple/MAX gauge behaves.
     */
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

            fixture.ApplyNode(1, node1, generation);
            fixture.ApplyNode(2, node2, generation);
            fixture.Processor->RecalculateAllCounters();

            expectedTotal += delta1 + delta2;
            now += TDuration::Seconds(5);
        }

        DumpCounters("Public tree after 3 generations from 2 nodes", fixture.PublicRoot);

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.consumed_cpu_us"), expectedTotal);
    }

    /**
     * Plan 09 names "map-after-transport keeps HIST/MAX correct" as the
     * reason for its step ordering, but nothing exercised it: this asserts
     * the percentile aggregate (HIST(ConsumedCPU), one observation per
     * Report()) actually survives a pack/unpack round trip. Each of the two
     * generations' diff (CalculateCountersDiff's histogram branch) carries
     * exactly one new observation, and TAggregateCumulative<false> sums
     * those diffs into the raw leaf bucket across generations, so the total
     * must be 2, not 1 (the second generation's diff dropped) or 0.
     */
    Y_UNIT_TEST(HistogramAggregateRoundTripsThroughPackAndUnpack) {
        TSimulatedNode node1;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);

        leader1.AddCumulative(CONSUMED_CPU, 50);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);
        fixture.ApplyNode(1, node1, 1);
        fixture.Processor->RecalculateAllCounters();

        leader1.AddCumulative(CONSUMED_CPU, 80);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));
        fixture.ApplyNode(1, node1, 2);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Raw tree after 2 generations, HIST(ConsumedCPU)", fixture.RawRoot);

        auto leafExecutorCounters = FindRawLeafExecutorCounters(fixture.RawRoot, 1000, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leafExecutorCounters, "HIST(ConsumedCPU)"), 2u);
    }

    /**
     * A PARTITION leaf is single-owner (F1): the tablet behind (tablet_id,
     * follower_id) has exactly one legitimate owner at a time, so two nodes
     * reporting the very same leaf at once is only ever the transient window
     * of a partition move (old and new owner both still reporting, up to one
     * 5 s tick). TCrossNodeEntry::Recalculate takes the MAX across nodes for
     * a single-owner bucket's Simple/gauge part rather than the SUM used for
     * a many-owner TABLE collapse bucket, so table.datashard.row_count must
     * come out as ONE copy of the value here, not two. Dropping either owner
     * afterwards must not change it: the survivor's own copy is already the
     * whole story.
     */
    Y_UNIT_TEST(PartitionMoveTransientDoesNotDoubleAGauge) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leaderOnNode1(1000, 0);
        leaderOnNode1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 55);
        leaderOnNode1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        TFakeTablet leaderOnNode2(1000, 0);
        leaderOnNode2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 55);
        leaderOnNode2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.ApplyNode(2, node2, 1);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree, the same leaf reported by both owners in one tick", fixture.PublicRoot);

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 55u);

        // Drop the old owner: the new owner's own copy is unchanged, so the
        // public value must stay exactly what it already was
        fixture.Processor->DropNode(1);
        fixture.Processor->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 55u);
    }

    /**
     * Before the fix, a PARTITION message's TTableEntry* was obtained (and,
     * on an older code path, created) BEFORE the leaf loop ran, so a message
     * whose Leaves field is empty still left a group behind: nothing in the
     * (never entered) loop body could ever reach it again to clean it up,
     * leaking both the raw and the public table= group forever. Hand-built,
     * the way TableLevelSumsPartialsAndRejectsFollowerPartial hand-builds
     * its rejected TABLE partial: PARTITION level, a TablePath, and
     * deliberately no Leaves at all.
     */
    Y_UNIT_TEST(LeaflessPartitionMessageLeavesNoGroupBehind) {
        TProcessorFixture fixture;

        NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> leafless;
        auto* entry = leafless.Add();
        entry->SetTablePath(TABLE_PATH);
        entry->SetLevel(TDetailedMetricsSettings::MetricsLevelPartition);
        // Leaves deliberately left empty

        fixture.Processor->ApplyFromNode(1, false /* isFollowerRole */, leafless);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree after a leafless PARTITION message", fixture.PublicRoot);

        UNIT_ASSERT(!FindPublicTableGroup(fixture.PublicRoot));
        UNIT_ASSERT(!fixture.RawRoot->FindSubgroup("table", RELATIVE_TABLE_PATH));
    }

    /**
     * A METRICS_LEVEL flip reaches nodes asynchronously: node1 still reports
     * TABLE, node2 already reports PARTITION, for the SAME path, in the SAME
     * tick — and again the next tick. Before the fix, every disagreeing
     * message tore the shared TTableEntry down and rebuilt both its counter
     * groups from scratch, so the surviving (TABLE) shape's own live value
     * would come back wrong (0, until some LATER tick's message happened to
     * repopulate it) on every single tick a disagreeing node kept reporting.
     * The disagreeing message itself is simply skipped (LevelMatches),
     * leaving normal absence detection (ReconcileStream) to retire the old
     * shape once its last real contributor stops feeding it — which never
     * happens here, since node1 keeps reporting TABLE every tick.
     */
    Y_UNIT_TEST(LevelDisagreementDoesNotResetTheSurvivingShapeEveryTick) {
        TSimulatedNode node1;
        TSimulatedNode node2;
        TProcessorFixture fixture;

        TFakeTablet leader1(1000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 77);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW);

        TFakeTablet leader2(2000, 0);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999);
        leader2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW);

        fixture.ApplyNode(1, node1, 1);
        fixture.ApplyNode(2, node2, 1);
        fixture.Processor->RecalculateAllCounters();

        auto tableGroup = FindPublicTableGroup(fixture.PublicRoot);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 77u);
        // The disagreeing message left no per-partition leaf either: the TABLE shape wins
        UNIT_ASSERT(!tableGroup->FindSubgroup("tablet_id", "2000"));

        // TICK 2: node1 reports a FRESH value (proving the shape is still live,
        // not frozen), node2 keeps disagreeing at PARTITION level as before
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 90);
        leader1.Report(node1.Leaders, TDetailedMetricsSettings::MetricsLevelTable, NOW + TDuration::Seconds(5));
        leader2.Report(node2.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, NOW + TDuration::Seconds(5));

        fixture.ApplyNode(1, node1, 2);
        fixture.ApplyNode(2, node2, 2);
        fixture.Processor->RecalculateAllCounters();

        DumpCounters("Public tree after the second tick's disagreeing message", fixture.PublicRoot);

        UNIT_ASSERT_VALUES_EQUAL(GetMappedCounterValue(tableGroup, "table.datashard.row_count"), 90u);
    }
}
