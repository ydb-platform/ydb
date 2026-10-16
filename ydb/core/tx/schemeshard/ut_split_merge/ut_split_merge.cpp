#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/util_fmt_cell.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/schemeshard_counters.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers_flags_n.h>  // for Y_UNIT_TEST_FLAGS_N
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// defined in ut_table_partitions_format.cpp
TPathId GetPathId(TTestActorRuntime& runtime, const TString& path);

namespace {

constexpr ui32 MAX_SPLIT_PROTOCOL_VERSION = 3;

ui32 GetSplitProtocolVersion(TTestActorRuntime& runtime) {
    if (runtime.GetAppData().FeatureFlags.GetEnableDataShardSplitHistogramOmission()) {
        return 3;
    } else if (runtime.GetAppData().FeatureFlags.GetEnableDataShardSplitKeySelection()) {
        return 2;
    } else if (runtime.GetAppData().FeatureFlags.GetEnableDataShardSplitHistogramSorting()) {
        return 1;
    }
    return 0;
}

void WaitForTableSplit(TTestActorRuntime& runtime, const TString& path, size_t requiredPartitionCount = 10) {
    while (true) {
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvGetTableStatsResult::EventType);

        WaitForSuppressed(runtime, suppressed, 1, prevObserver);
        for (auto &msg : suppressed) {
            runtime.Send(msg.Release());
        }
        suppressed.clear();

        const auto result = DescribePath(runtime, path, true);
        if (result.GetPathDescription().TablePartitionsSize() >= requiredPartitionCount)
            return;
    }
}

}  // namespace anonymous

Y_UNIT_TEST_SUITE(TSchemeShardSplitBySizeTest) {
    Y_UNIT_TEST(Test) {
    }

    Y_UNIT_TEST(ConcurrentSplitOneShard) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "Key"       Type: "Utf8"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["Key", "Value"]
                            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({""})});

        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvHive::TEvCreateTablet::EventType);

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "A" } }
                                }
                            })");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                        SourceTabletId: 72075186233409546
                        SplitBoundary {
                            KeyPrefix {
                                Tuple { Optional { Text: "A" } }
                            }
                        })",
                       {NKikimrScheme::StatusMultipleModifications});

        WaitForSuppressed(runtime, suppressed, 4, prevObserver);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        env.TestWaitNotification(runtime, {txId-1, txId});
        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets); //delete src

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"A", ""})});

    }

    Y_UNIT_TEST(ConcurrentSplitOneToOne) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "Key"       Type: "Utf8"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["Key", "Value"]
                            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({""})});

        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvHive::TEvCreateTablet::EventType);

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            AllowOneToOneSplitMerge: true
                            )");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                        SourceTabletId: 72075186233409546
                        AllowOneToOneSplitMerge: true
                        )",
                       {NKikimrScheme::StatusMultipleModifications});

        WaitForSuppressed(runtime, suppressed, 2, prevObserver);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        env.TestWaitNotification(runtime, {txId-1, txId});
        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets); //delete src

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({""})});
    }

    void Split10Shards(ui32 splitProtocolVersion) {
        TTestEnvOptions opts;
        if (splitProtocolVersion == 1) {
            opts.EnableDataShardSplitHistogramSorting(true);
        } else if (splitProtocolVersion == 2) {
            opts.EnableDataShardSplitKeySelection(true);
        } else if (splitProtocolVersion == 3) {
            opts.EnableDataShardSplitHistogramOmission(true);
        }
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(1);
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, opts);

        UNIT_ASSERT_VALUES_EQUAL(splitProtocolVersion, GetSplitProtocolVersion(runtime));

        ui64 txId = 100;

        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        //runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_CRIT);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_CRIT);


        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 1
                        )");
        env.TestWaitNotification(runtime, txId);

        auto fnWriteRow = [&] (ui64 tabletId, ui64 key) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('key (Uint64 '%lu)) ) )
                    (let value '('('value (Utf8 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA)) ) )
                    (return (AsList (UpdateRow '__user__Table key value) ))
                )
            )", key);;
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };
        for (ui64 key = 0; key < 1000; ++key) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, key* 1'000'000);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 100
                                MaxPartitionsCount: 100
                                SizeToSplit: 1
                                FastSplitSettings {
                                    SizeThreshold: 10
                                    RowCountThreshold: 10
                                }
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        WaitForTableSplit(runtime, "/MyRoot/Table");
    }
    struct TTestRegistrationSplit10Shards {
        TTestRegistrationSplit10Shards() {
            static std::vector<TString> TestNames;

            for (const auto& i : xrange(MAX_SPLIT_PROTOCOL_VERSION + 1)) {
                TestNames.emplace_back(TStringBuilder() << "Split10Shards-protocol" << i);

                TCurrentTest::AddTest(
                    TestNames.back().c_str(),
                    std::bind(std::bind(Split10Shards, i), std::placeholders::_1),
                    /*forceFork*/ false
                );
            }
        }
    };
    static TTestRegistrationSplit10Shards testRegistrationSplit10Shards;

    Y_UNIT_TEST(SplitShardsWithDecimalKey) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.EnableParameterizedDecimal(true);
        opts.DataShardStatsReportIntervalSeconds(1);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_ERROR);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"_(
                        Name: "Table"
                        Columns { Name: "key"  Type: "Decimal(35, 10)"}
                        Columns { Name: "decimal_value" Type: "Decimal(2, 1)"}
                        Columns { Name: "string_value" Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        )_");
        env.TestWaitNotification(runtime, txId);

        const std::pair<ui64, ui64> decimalValue = NYql::NDecimal::MakePair(
            NYql::NDecimal::FromString("32.1", 2, 1));
        TString stringValue(1000, 'A');

        for (ui64 key = 0; key < 1000; ++key) {
            const std::pair<ui64, ui64> decimalKey = NYql::NDecimal::MakePair(
                NYql::NDecimal::FromString(Sprintf("%d.123456789", key * 1'000'000), 35, 10));
            UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2, 3},
                {TCell::Make<std::pair<ui64, ui64>>(decimalKey)},
                {TCell::Make<std::pair<ui64, ui64>>(decimalValue), TCell(stringValue)});
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 100
                                MaxPartitionsCount: 100
                                SizeToSplit: 1
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        WaitForTableSplit(runtime, "/MyRoot/Table");
    }

    Y_UNIT_TEST(SplitShardsWithPgKey) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.EnableTablePgTypes(true);
        opts.DataShardStatsReportIntervalSeconds(1);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_CRIT);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "pgint8"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 1
                        )");
        env.TestWaitNotification(runtime, txId);

        TString valueString(1000, 'A');;
        for (ui64 key = 0; key < 1000; ++key) {
            auto pgKey = NPg::PgNativeBinaryFromNativeText(ToString(key * 1'000'000), NPg::TypeDescFromPgTypeName("pgint8")).Str;
            UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2}, {TCell(pgKey)}, {TCell(valueString)});
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 100
                                MaxPartitionsCount: 100
                                SizeToSplit: 1
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        WaitForTableSplit(runtime, "/MyRoot/Table");
    }

    Y_UNIT_TEST(Merge1KShards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DisableStatsBatching(true);
        opts.DataShardStatsReportIntervalSeconds(0);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;
        runtime.SetDispatchedEventsLimit(10'000'000);

        //runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_CRIT);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_CRIT);


        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 1000
                        )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            WaitForSuppressed(runtime, suppressed, 1000, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1000)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 1
                                SizeToSplit: 100500
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            WaitForSuppressed(runtime, suppressed, 5*1000, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1000)});


        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1000));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1)});
    }

    Y_UNIT_TEST(Merge111Shards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 111
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(111)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 1
                                SizeToSplit: 100500
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        WaitForSuppressed(runtime, suppressed, suppressed.size(), prevObserver);
        for (auto &msg : suppressed) {
            runtime.Send(msg.Release());
        }
        suppressed.clear();

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+111));
        // test requires more txids than cached at start
    }

    Y_UNIT_TEST(MergeNonFirstPartitions) {
        // Merging non-first partitions (positions 1 and 2) with partial persistence
        // disabled (splitStartIdx=0).  ApplySplitMerge uses srcFirstIdx=1 for the
        // src range; partition 0 must be unchanged.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        // 3 partitions: (-inf,"A"), ["A","B"), ["B",+inf)
        // shards: FakeHiveTablets+0, +1, +2
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } } } }
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 100500
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"A", "B", ""})});

        // With the flag off, splitStartIdx=0 for persistence; ApplySplitMerge
        // still uses srcFirstIdx=1 as the src-shard position.
        runtime.GetAppData().FeatureFlags.SetEnableSplitMergePartialPersistence(false);

        // Merge partitions 1 and 2 (the non-first ones).
        TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
            SourceTabletId: %lu
            SourceTabletId: %lu
        )", TTestTxConfig::FakeHiveTablets + 1, TTestTxConfig::FakeHiveTablets + 2));
        env.TestWaitNotification(runtime, txId);

        // Partition 0 must be unchanged; partitions 1+2 merged into one.
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"A", ""})});
    }

    Y_UNIT_TEST_FLAG(UnchangedPartitionStatsKeptAfterSplit, EnableSplitMergePartialPersistence) {
        // Stats for partitions at positions >= splitStartIdx survive a schemeshard
        // restart.  Exercised with both partial-persistence on and off.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DisableStatsBatching(true);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableSplitMergePartialPersistence(EnableSplitMergePartialPersistence);
        // EnablePersistentPartitionStats is checked at call time, so setting it
        // before any stats arrive is sufficient — no tablet restart needed.
        runtime.GetAppData().FeatureFlags.SetEnablePersistentPartitionStats(true);

        // 3 partitions: (-inf,100)→F+0, [100,200)→F+1, [200,+inf)→F+2 (C).
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            PartitionConfig {
                PartitioningPolicy {
                    SizeToSplit: 100500
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Block stats events.  Patch DataSize for shard C (F+2) to a sentinel so
        // we can distinguish "stats row present" from "stats missing → default 0".
        constexpr ui64 kShardCDataSize = 99999;
        const ui64 shardCTabletId = TTestTxConfig::FakeHiveTablets + 2;
        // Only capture events addressed to schemeshard, so a same-typed forward to an aux
        // actor (which changes Recipient) sails through untouched.
        const TActorId schemeShardActorId = ResolveTablet(runtime, TTestTxConfig::SchemeShard);
        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> statsBlocker(runtime,
            [schemeShardActorId](const auto& ev) {
                return ev->GetRecipientRewrite() == schemeShardActorId;
            }
        );

        runtime.WaitFor("stats from all 3 shards", [&]{ return statsBlocker.size() >= 3; });

        for (auto& ev : statsBlocker) {
            auto* msg = ev->Get();
            if (msg->Record.GetDatashardId() == shardCTabletId) {
                msg->Record.MutableTableStats()->SetDataSize(kShardCDataSize);
            }
        }
        statsBlocker.Unblock(3);
        // The 3 unblocked events are now ahead of the split proposal in the
        // dispatch queue, so TestSplitTable's internal dispatch processes them
        // first.  All further stats remain blocked so schemeshard cannot
        // refresh C's stats from a fresh datashard report.

        // Split F+1 (position 1) at key 150 → B1(pos=1), B2(pos=2).
        // C shifts from position 2 to position 3.
        TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
            SourceTabletId: %lu
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 150 } } } }
        )", TTestTxConfig::FakeHiveTablets + 1));
        env.TestWaitNotification(runtime, txId);

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Stats are still blocked: schemeshard has only what it loaded from local DB.
        NKikimrSchemeOp::TDescribeOptions descOpts;
        descOpts.SetReturnPartitioningInfo(true);
        descOpts.SetReturnPartitionStats(true);
        auto describe = DescribePath(runtime, "/MyRoot/Table", descOpts);
        const auto& partStats = describe.GetPathDescription().GetTablePartitionStats();
        UNIT_ASSERT_VALUES_EQUAL((size_t)partStats.size(), 4u);
        // C is now at position 3; its stats row must be present in the DB.
        UNIT_ASSERT_VALUES_EQUAL(partStats[3].GetDataSize(), kShardCDataSize);
    }

    Y_UNIT_TEST(MergeIndexTableShards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> statsBlocker(runtime);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                }
                IndexDescription {
                    Name: "ByValue"
                    KeyColumnNames: ["value"]
                    IndexImplTableDescriptions {
                        SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } } } }
                        SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } } } }
                        SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } } } }
                    }
                }
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/ByValue/indexImplTable", true),
            { NLs::PartitionCount(4) }
        );

        statsBlocker.Stop().Unblock();

        TVector<ui64> indexShards;
        auto shardCollector = [&indexShards](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);
            const auto& partitions = record.GetPathDescription().GetTablePartitions();
            indexShards.clear();
            indexShards.reserve(partitions.size());
            for (const auto& partition : partitions) {
                indexShards.emplace_back(partition.GetDatashardId());
            }
        };

        // wait until all index impl table shards are merged into one
        while (true) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/ByValue/indexImplTable", true), {
                shardCollector
            });
            if (indexShards.size() > 1) {
                // If a merge happens, old shards are deleted and replaced with a new one.
                // That is why we need to wait for * all * the shards to be deleted.
                env.TestWaitTabletDeletion(runtime, indexShards);
            } else {
                break;
            }
        }
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AutoMergeInOne, 2, 1, false) {
        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;
        t.EnvOpts.EnableBackgroundCompaction(false);
        t.EnvOpts.DataShardStatsReportIntervalSeconds(0);
        t.EnvOpts.EnableRealSystemViewPaths(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", ""})});

                TControlBoard::SetValue(1, runtime.GetAppData().Icb->SchemeShardControls.MergeByLoadMinUptimeSec);
                TControlBoard::SetValue(10, runtime.GetAppData().Icb->SchemeShardControls.MergeByLoadMinLowLoadDurationSec);
            }

            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            {
                TInactiveZone inactive(activeZone);
                TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 1
                                        SizeToSplit: 100500
                                    }
                                }
                            )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {NLs::PartitionKeys({"A", ""})});

            WaitForSuppressed(runtime, suppressed, 1, prevObserver);

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1));

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {NLs::PartitionKeys({""})});

        }, true);
    }

    void TryMergeWithInflyLimit(TTestActorRuntime &runtime, TTestEnv &env, const ui64 mergeNum, const ui64 remainMergeNum, const ui64 acceptedMergeNum, ui64 &txId) {
        const ui64 shardsNum = mergeNum * 2;
        const ui64 startMergePart = mergeNum - remainMergeNum;
        TSet<ui64> txIds;
        ui64 startTxId = txId;
        for (ui64 i = startMergePart * 2; i < shardsNum; i += 2) {
            AsyncSplitTable(runtime, txId, "/MyRoot/Table",
                                Sprintf(R"(
                                    SourceTabletId: %lu
                                    SourceTabletId: %lu
                                )", TTestTxConfig::FakeHiveTablets + i, TTestTxConfig::FakeHiveTablets + i + 1));
            txIds.insert(txId++);
        }

        for (ui64 i = startTxId; i < startTxId + acceptedMergeNum ; i++)
            TestModificationResult(runtime, i, NKikimrScheme::StatusAccepted);
        for (ui64 i = startTxId + acceptedMergeNum; i < txId; i++)
            TestModificationResult(runtime, i, NKikimrScheme::StatusResourceExhausted);

        env.TestWaitNotification(runtime, txIds);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                            NLs::ShardsInsideDomain(mergeNum + remainMergeNum - acceptedMergeNum)
                        });
    };

    void AsyncMergeWithInflyLimit(const ui64 mergeNum, const ui64 mergeLimit) {
        const ui64 shardsNum = mergeNum * 2;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;
        auto& appData = runtime.GetAppData();

        // set batching only by timeout
        NKikimrConfig::TSchemeShardConfig_TInFlightCounterConfig *inFlightCounter = appData.SchemeShardConfig.AddInFlightCounterConfig();
        inFlightCounter->SetType(NKikimr::NSchemeShard::ESimpleCounters::COUNTER_IN_FLIGHT_OPS_TxSplitTablePartition);
        inFlightCounter->SetInFlightLimit(mergeLimit);
        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateTable(runtime, txId++, "/MyRoot", Sprintf(R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: %lu
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 0
                            }
                        })", shardsNum));

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::IsTable,
                            NLs::ShardsInsideDomain(shardsNum)});
        ui64 remainMergeNum = mergeNum;

        while (remainMergeNum > 0)
        {
            ui64 acceptedMergeNum = mergeLimit == 0
                ? remainMergeNum
                : std::min(remainMergeNum, mergeLimit);
            TryMergeWithInflyLimit(runtime, env, mergeNum, remainMergeNum, acceptedMergeNum, txId);
            remainMergeNum -= acceptedMergeNum;
        }
    }

    Y_UNIT_TEST(Make11MergeOperationsWithInflyLimit10) {
        AsyncMergeWithInflyLimit(11, 10);
    }

    Y_UNIT_TEST(Make20MergeOperationsWithInflyLimit5) {
        AsyncMergeWithInflyLimit(20, 5);
    }

    Y_UNIT_TEST(Make20MergeOperationsWithoutLimit) {
        AsyncMergeWithInflyLimit(20, 0);
    }

    Y_UNIT_TEST(SequentialSplitFirstShard) {
        // Split the first (leftmost) shard 19 times to produce 20 partitions.
        // Each split inserts a new shard at position 0, incrementing Position
        // of all existing shards by 1.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 100500
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Each iteration splits shard 0 at "key%02u" for i = 19, 18, ..., 01.
        // Because "key01" < "key02" < ... < "key19" lexicographically, the new
        // boundary is always less than all previous ones, keeping shard 0 as
        // the leftmost shard throughout.
        for (ui32 i = 19; i >= 1; --i) {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            const auto& parts = describe.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_VALUES_EQUAL((ui32)parts.size(), 20 - i);

            const ui64 firstTabletId = parts[0].GetDatashardId();
            TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "key%02u" } } } }
            )", firstTabletId, i));
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(20)});
    }

    Y_UNIT_TEST(SplitRestartRoundTrip) {
        // Split a table into 3 partitions, restart schemeshard, then verify
        // that the partition count, shard identities, and boundaries survive
        // TTxInit loading from TablePartitionsByShardIdx (ShardIdx format).
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 100500
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Split into 2.
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            const ui64 shard0 = describe.GetPathDescription().GetTablePartitions(0).GetDatashardId();
            TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "M" } } } }
            )", shard0));
            env.TestWaitNotification(runtime, txId);
        }

        // Split into 3.
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            const ui64 shard1 = describe.GetPathDescription().GetTablePartitions(1).GetDatashardId();
            TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "T" } } } }
            )", shard1));
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"M", "T", ""})});

        // Record shard IDs before restart.
        TVector<ui64> preShardIds;
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            for (const auto& p : describe.GetPathDescription().GetTablePartitions()) {
                preShardIds.push_back(p.GetDatashardId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(preShardIds.size(), 3u);

        // Restart schemeshard — forces TTxInit to re-load from persistent tables.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"M", "T", ""})});

        TVector<ui64> postShardIds;
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            for (const auto& p : describe.GetPathDescription().GetTablePartitions()) {
                postShardIds.push_back(p.GetDatashardId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(preShardIds, postShardIds);
    }

    Y_UNIT_TEST(ShardIdxFormatMigration) {
        // Start in positional format, do one split, enable ShardIdx format,
        // do another split (triggers migration to ShardIdx format), then restart
        // schemeshard and verify the partition state survives the round-trip.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(false);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 100500
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // First split in positional format.
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            const ui64 shard0 = describe.GetPathDescription().GetTablePartitions(0).GetDatashardId();
            TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "M" } } } }
            )", shard0));
            env.TestWaitNotification(runtime, txId);
        }
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"M", ""})});

        // Enable ShardIdx format.  The next split migrates from positional rows
        // to ShardIdx rows and sets PartitionsInShardIdxFormat.
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            const ui64 shard1 = describe.GetPathDescription().GetTablePartitions(1).GetDatashardId();
            TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "T" } } } }
            )", shard1));
            env.TestWaitNotification(runtime, txId);
        }
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"M", "T", ""})});

        // Record shard IDs before restart.
        TVector<ui64> preShardIds;
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            for (const auto& p : describe.GetPathDescription().GetTablePartitions()) {
                preShardIds.push_back(p.GetDatashardId());
            }
        }

        // Restart schemeshard: forces TTxInit to load from ShardIdx rows.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Verify boundaries and shard identities survived the round-trip.
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"M", "T", ""})});
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table", true);
            TVector<ui64> postShardIds;
            for (const auto& p : describe.GetPathDescription().GetTablePartitions()) {
                postShardIds.push_back(p.GetDatashardId());
            }
            UNIT_ASSERT_VALUES_EQUAL(preShardIds, postShardIds);
        }
    }

    Y_UNIT_TEST(DropTableShardIdxFormatNoOrphanRows) {
        // PersistRemoveTable should delete from TablePartitionsByShardIdx.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "M" } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "T" } } } }
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 3
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathId = GetPathId(runtime, "/MyRoot/Table");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"M", "T", ""})}
        );

        // Switch table to shardidx format
        auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);

        // Counts all rows in TablePartitionsByShardIdx via a full composite-key range scan.
        // ReadLocalTableRecords() only works for single-column keys, so we issue the
        // MiniKQL query directly, specifying all four key columns in the range.
        auto countShardIdxRows = [&]() -> ui64 {
            const auto result = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, R"(
                (
                    (let range '(
                        '('OwnerPathId  (Null) (Void))
                        '('LocalPathId  (Null) (Void))
                        '('OwnerShardIdx (Null) (Void))
                        '('LocalShardIdx (Null) (Void))
                    ))
                    (let fields '('OwnerPathId))
                    (return (AsList
                        (SetResult 'Result (SelectRange 'TablePartitionsByShardIdx range fields '()))
                    ))
                )
            )");
            return NKikimr::NClient::TValue::Create(result)[0]["List"].Size();
        };

        // After switching to shardidx format, TablePartitionStatsByShardIdx should have exactly 3 rows
        UNIT_ASSERT_VALUES_EQUAL(countShardIdxRows(), 3u);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"M", "T", ""})}
        );

        // Drop the table — PersistRemoveTable must delete those rows.
        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        // TablePartitionsByShardIdx must now be empty.
        UNIT_ASSERT_VALUES_EQUAL(countShardIdxRows(), 0u);

        // Restart schemeshard — before the fix, TTxInit crashed here because
        // orphaned TablePartitionsByShardIdx rows referenced the dropped pathId.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
            {NLs::PathNotExist}
        );
    }

    Y_UNIT_TEST(FormatDowngradeNoOrphanStatsRows) {
        // PersistTablePartitioningDeletion should delete from
        // TablePartitionStatsByShardIdx when PartitionsInShardIdxFormat=true.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnablePersistentPartitionStats(true);

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "M" } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "T" } } } }
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 3
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathId = GetPathId(runtime, "/MyRoot/Table");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"M", "T", ""})}
        );

        // Switch table to shardidx format
        auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);

        auto countStatsShardIdxRows = [&]() -> ui64 {
            const auto result = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, R"(
                (
                    (let range '(
                        '('OwnerPathId  (Null) (Void))
                        '('LocalPathId  (Null) (Void))
                        '('OwnerShardIdx (Null) (Void))
                        '('LocalShardIdx (Null) (Void))
                    ))
                    (let fields '('OwnerPathId))
                    (return (AsList
                        (SetResult 'Result (SelectRange 'TablePartitionStatsByShardIdx range fields '()))
                    ))
                )
            )");
            return NKikimr::NClient::TValue::Create(result)[0]["List"].Size();
        };

        // After switching to shardidx format, TablePartitionStatsByShardIdx should have exactly 3 rows
        UNIT_ASSERT_VALUES_EQUAL(countStatsShardIdxRows(), 3u);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"M", "T", ""})}
        );

        // Switch table back to position format
        r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "position");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);

        // After switching to position format, TablePartitionStatsByShardIdx must be empty, no orphaned rows
        UNIT_ASSERT_VALUES_EQUAL(countStatsShardIdxRows(), 0u);

        // Restart to verify no crash loading orphaned rows
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"M", "T", ""})}
        );
    }

}

namespace {

using NFmt::TPrintableTypedCells;

TString ToSerialized(ui64 key) {
    const auto cell = TCell::Make(key);
    const TSerializedCellVec saved(TArrayRef<const TCell>(&cell, 1));
    return TString(saved.GetBuffer());
}

ui64 FromSerialized(const TString& buf) {
    TSerializedCellVec saved(buf);
    // Cerr << "TEST FromSerialized, " << TPrintableTypedCells(saved.GetCells(), {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)}) << Endl;
    auto& cell = saved.GetCells()[0];
    // Cerr << "TEST FromSerialized, cell " << cell.IsInline() << ", " << cell.IsNull() << ", " << cell.Size() << Endl;
    return cell.IsNull() ? 0 : cell.AsValue<ui64>();
}

void HistogramAddBucket(NKikimrTableStats::THistogram& hist, ui64 key, ui64 value) {
    auto bucket = hist.AddBuckets();
    bucket->SetKey(ToSerialized(key));
    bucket->SetValue(value);
};

constexpr ui64 CpuLoadMicroseconds(const ui64 percent) {
    return percent * 1000000 / 100;
}

// const ui64 CpuLoadPercent(const ui64 microseconds) {
//     return microseconds * 100 / 1000000;
// }

/**
 * The strategy for sending duplicate EvGetTableStatsResult messages
 * to induce various boundary conditions related to split transactions.
 */
enum class ESendDuplicateTableStatsStrategy {
    /**
     * Do not send duplicate EvGetTableStatsResult messages.
     *
     * @note This strategy does not modify the regular behavior of the split process.
     */
    None,

    /**
     * Send a duplicate EvGetTableStatsResult message immediately.
     *
     * @note This strategy should result in a concurrent split transaction.
     */
    Immediately,

    /**
     * Send a duplicate EvGetTableStatsResult message after intercepting the EvSplitAck message.
     *
     * @note This strategy should result in a duplicate split transaction.
     */
    AfterSplitAck,
};

// Quick and dirty simulator for cpu overload and key range splitting of datashards.
// Should be used in test runtime EventObservers.
//
// Assumed index configuration: 1 initial datashard, Uint64 key.
//
struct TLoadAndSplitSimulator {
    std::map<ui32, NKikimrTabletBase::TMetrics> MetricsPatchByFollowerIdPeriodic;
    std::map<ui32, NKikimrTabletBase::TMetrics> MetricsPatchByFollowerIdStats;
    NKikimrTableStats::THistogram KeyAccessHistogramPatch;
    ui64 TableLocalPathId;
    ui64 TableOwnerId;
    bool ShouldSendReadRequests;
    ESendDuplicateTableStatsStrategy SendDuplicateTableStats;
    std::map<ui64, std::unique_ptr<IEventHandle>> DuplicateTableStatsByDatashardId;
    ui32 SplitProtocolVersion = 0;

    TTestActorRuntime* TestRuntime;
    TActorId SenderActorId;

    std::map<ui64, std::pair<ui64, ui64>> DatashardsKeyRanges;
    TInstant LastSplitAckTime;
    ui64 SplitAckCount = 0;
    ui64 PeriodicTableStatsCount = 0;
    ui64 KeyAccessSampleReqCount = 0;
    ui64 SplitReqCount = 0;
    ui64 ReadRequestCount = 0;

    /**
     * An empty actor, which is used only as a sender for sending messages to other actors.
     */
    class TDummyActor : public TActor<TDummyActor> {
    public:
        TDummyActor()
            : TActor(&TThis::StateWork)
        {
        }

        STFUNC(StateWork) {
            Y_UNUSED(ev);
        }
    };

    TLoadAndSplitSimulator(
        ui64 tableLocalPathId,
        ui64 tableOwnerId,
        ui64 initialDatashardId,
        bool shouldSendReadRequests,
        ESendDuplicateTableStatsStrategy sendDuplicateTableStats,
        const std::map<ui32, i32>& targetCpuLoadByFollowerIdPeriodic,
        const std::map<ui32, i32>& targetCpuLoadByFollowerIdStats,
        TTestActorRuntime& testRuntime
    ) : TableLocalPathId(tableLocalPathId)
        , TableOwnerId(tableOwnerId)
        , ShouldSendReadRequests(shouldSendReadRequests)
        , SendDuplicateTableStats(sendDuplicateTableStats)
        , TestRuntime(&testRuntime)
    {
        for (const auto& [followerId, targetCpuLoad] : targetCpuLoadByFollowerIdPeriodic) {
            MetricsPatchByFollowerIdPeriodic[followerId].SetCPU(CpuLoadMicroseconds(targetCpuLoad));
        }

        for (const auto& [followerId, targetCpuLoad] : targetCpuLoadByFollowerIdStats) {
            if (targetCpuLoad >= 0) {
                MetricsPatchByFollowerIdStats[followerId].SetCPU(CpuLoadMicroseconds(targetCpuLoad));
            } else {
                MetricsPatchByFollowerIdStats[followerId].ClearCPU();
            }
        }

        // NOTE: histogram must have at least 3 buckets with different keys to be able to produce split key
        // (see ydb/core/split/key_access.cpp, FindSplitKeyPrefix() and SelectShortestMedianKeyPrefix())
        HistogramAddBucket(KeyAccessHistogramPatch, 999998, 1000);
        HistogramAddBucket(KeyAccessHistogramPatch, 999999, 1000);
        HistogramAddBucket(KeyAccessHistogramPatch, 1000000, 1000);

        SplitProtocolVersion = GetSplitProtocolVersion(testRuntime);

        DatashardsKeyRanges[initialDatashardId] = std::make_pair(0, 1000000);

        // Use a dummy actor as a sender for events instead of an edge actor
        // to avoid an infinite loop between the edge actor and the observer
        if (ShouldSendReadRequests) {
            SenderActorId = TestRuntime->Register(new TDummyActor());
        }

        for (const auto& [followerId, targetCpuLoad] : targetCpuLoadByFollowerIdPeriodic) {
            Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                << ", target CPU load (EvPeriodicTableStats) for followerId " << followerId
                << " is " << targetCpuLoad
                << "%"
                << Endl;
        }

        for (const auto& [followerId, targetCpuLoad] : targetCpuLoadByFollowerIdStats) {
            if (targetCpuLoad >= 0) {
                Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                    << ", target CPU load (EvGetTableStatsResult) for followerId " << followerId
                    << " is " << targetCpuLoad
                    << "%"
                    << Endl;
            } else {
                Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                    << ", target CPU load (EvGetTableStatsResult) for followerId " << followerId
                    << " is UNSET"
                    << Endl;
            }
        }
    }

    /**
     * Create a simple EvRead request, which contains a query for the first key.
     *
     * @return The corresponding EvRead request
     */
    std::unique_ptr<TEvDataShard::TEvRead> MakeSimpleReadRequest() {
        std::unique_ptr<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        record.SetReadId(++ReadRequestCount);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        record.MutableTableId()->SetTableId(TableLocalPathId);
        record.MutableTableId()->SetOwnerId(TableOwnerId);

        record.AddColumns(1);
        record.AddColumns(2);

        request->Ranges.emplace_back(
            TSerializedCellVec::Serialize({TCell::Make(Min<ui64>())}),
            TSerializedCellVec::Serialize({TCell::Make(Min<ui64>())}),
            true /* fromInclusive */,
            true /* toInclusive */
        );

        return request;
    }

    void ChangeEvent(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvTablet::TEvTabletActive::EventType:
                {
                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvTabletActive from sender " << ev->Sender
                        << " to recipient " << ev->GetRecipientRewrite()
                        << Endl;

                    // Followers send EvTabletActive when they are ready to process requests,
                    // this is the right moment to send a dummy read request to this follower
                    // to make it start sending periodic stats updates
                    //
                    // NOTE: This read request is assumed to be simple and to return no data.
                    //       In other words, it is assumed to produce only a single final response,
                    //       which does not need to be acknowledged. Thus, there is no need to block
                    //       here and wait for the response to come back.
                    if (ShouldSendReadRequests) {
                        // Send back to the actor, which sent EvTabletActive
                        const TActorId readRequestTarget = ev->Sender;

                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << ", sending EvRead request to " << readRequestTarget
                            << Endl;

                        TestRuntime->SendAsync(
                            new IEventHandle(
                                readRequestTarget,
                                SenderActorId,
                                MakeSimpleReadRequest().release()
                            )
                        );
                    }
                }
                break;

            case TEvDataShard::EvRead:
                {
                    const auto msg = ev->Get<TEvDataShard::TEvRead>();

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvRead from sender " << ev->Sender
                        << " to recipient " << ev->GetRecipientRewrite()
                        << ", readId " << msg->Record.GetReadId()
                        << ", tableId " << msg->Record.GetTableId().GetTableId()
                        << Endl;
                }
                break;

            case TEvDataShard::EvReadResult:
                {
                    const auto msg = ev->Get<TEvDataShard::TEvReadResult>();

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvReadResult from sender " << ev->Sender
                        << " to recipient " << ev->GetRecipientRewrite()
                        << ", readId " << msg->Record.GetReadId()
                        << ", sequence number " << msg->Record.GetSeqNo()
                        << ", row count " << msg->Record.GetRowCount()
                        << ", status " << msg->Record.GetStatus()
                        << ", finished " << msg->Record.GetFinished()
                        << Endl;
                }
                break;

            case TEvDataShard::EvPeriodicTableStats:
                // replace real stats with the simulated ones
                {
                    const auto msg = ev->Get<TEvDataShard::TEvPeriodicTableStats>();

                    if (msg->Record.GetTableLocalId() != TableLocalPathId) {
                        return;
                    }

                    const auto itTargetCpuForFollower = MetricsPatchByFollowerIdPeriodic.find(msg->Record.GetFollowerId());

                    if (itTargetCpuForFollower != MetricsPatchByFollowerIdPeriodic.end()) {
                        const auto prevCPU = msg->Record.GetTabletMetrics().GetCPU();
                        msg->Record.MutableTabletMetrics()->MergeFrom(itTargetCpuForFollower->second);
                        const auto newCPU = msg->Record.GetTabletMetrics().GetCPU();

                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << ", intercept EvPeriodicTableStats, from datashard " << msg->Record.GetDatashardId()
                            << ", from followerId " << msg->Record.GetFollowerId()
                            << ", patched CPU: " << prevCPU << "->" << newCPU
                            << Endl;
                    } else {
                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << ", intercept EvPeriodicTableStats, from datashard " << msg->Record.GetDatashardId()
                            << ", from followerId " << msg->Record.GetFollowerId()
                            << ", unpatched CPU: " << msg->Record.GetTabletMetrics().GetCPU()
                            << Endl;
                    }

                    ++PeriodicTableStatsCount;
                }
                break;
            case TEvDataShard::EvGetTableStats:
                // count requests for key access samples, as they indicate consideration of performing a split
                {
                    const auto msg = ev->Get<TEvDataShard::TEvGetTableStats>();

                    if (msg->Record.GetTableId() != TableLocalPathId) {
                        return;
                    }

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                         << ", intercept EvGetTableStats, collectKeySample " << msg->Record.GetCollectKeySample()
                         << Endl;

                    if (msg->Record.GetCollectKeySample()) {
                        ++KeyAccessSampleReqCount;
                    }
                }
                break;
            case TEvDataShard::EvGetTableStatsResult:
                // replace real key access samples with the simulated ones
                {
                    const auto msg = ev->Get<TEvDataShard::TEvGetTableStatsResult>();

                    if (msg->Record.GetTableLocalId() != TableLocalPathId) {
                        return;
                    }

                    // Duplicate EvGetTableStatsResult messages will come here too,
                    // they need to be excluded from the duplication logic explicitly
                    // to avoid infinite duplication. A special magic cookie is used
                    // to mark duplicated messages and to exclude them from the duplication
                    const ui64 MagicDuplicateMessageCookie = 123456789ul;

                    if (ev->Cookie == MagicDuplicateMessageCookie) {
                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << ", intercept EvGetTableStatsResult (induced duplicate), "
                            << "from datashard " << msg->Record.GetDatashardId()
                            << Endl;

                        break;
                    }

                    const auto itTargetCpuForFollower = MetricsPatchByFollowerIdStats.find(msg->Record.GetFollowerId());

                    if (itTargetCpuForFollower != MetricsPatchByFollowerIdStats.end()) {
                        const auto prevCPU = msg->Record.GetTabletMetrics().GetCPU();

                        if (itTargetCpuForFollower->second.HasCPU()) {
                            msg->Record.MutableTabletMetrics()->MergeFrom(itTargetCpuForFollower->second);
                            const auto newCPU = msg->Record.GetTabletMetrics().GetCPU();

                            Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                                << ", intercept EvGetTableStatsResult, from datashard " << msg->Record.GetDatashardId()
                                << ", from followerId " << msg->Record.GetFollowerId()
                                << ", patched CPU: " << prevCPU << "->" << newCPU
                                << Endl;
                        } else {
                            msg->Record.MutableTabletMetrics()->ClearCPU();

                            Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                                << ", intercept EvGetTableStatsResult, from datashard " << msg->Record.GetDatashardId()
                                << ", from followerId " << msg->Record.GetFollowerId()
                                << ", patched CPU: " << prevCPU << "->UNSET"
                                << Endl;
                        }
                    } else {
                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << ", intercept EvGetTableStatsResult, from datashard " << msg->Record.GetDatashardId()
                            << ", from followerId " << msg->Record.GetFollowerId()
                            << ", unpatched CPU: " << msg->Record.GetTabletMetrics().GetCPU()
                            << Endl;
                    }

                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->CopyFrom(KeyAccessHistogramPatch);

                    auto [start, end] = DatashardsKeyRanges[msg->Record.GetDatashardId()];
                    // NOTE: zero end means infinity -- this is a final shard
                    if (end == 0) {
                        end = 1000000;
                    }
                    const ui64 splitPoint = (end + start) / 2;

                    // Emulate split protocol versions, see GetSplitBoundaryByLoad().
                    switch (SplitProtocolVersion) {
                        case 0: {  // unsorted array, no key
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(0)->SetKey(ToSerialized(splitPoint + 1));
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(1)->SetKey(ToSerialized(splitPoint - 1));
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(2)->SetKey(ToSerialized(splitPoint));
                            }
                            break;
                        case 1: {  // sorted array, no key
                                msg->Record.MutableTableStats()->SetSplitProtocolVersion(SplitProtocolVersion);
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(0)->SetKey(ToSerialized(splitPoint - 1));
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(1)->SetKey(ToSerialized(splitPoint));
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(2)->SetKey(ToSerialized(splitPoint + 1));
                            }
                            break;
                        case 2: {  // sorted array, with key
                                msg->Record.MutableTableStats()->SetSplitProtocolVersion(SplitProtocolVersion);
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(0)->SetKey(ToSerialized(splitPoint - 1));
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(1)->SetKey(ToSerialized(splitPoint));
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(2)->SetKey(ToSerialized(splitPoint + 1));
                                msg->Record.MutableTableStats()->SetSplitByLoadSuggestedKey(ToSerialized(splitPoint));
                            }
                            break;
                        case 3: {  // no array, with key
                                msg->Record.MutableTableStats()->SetSplitProtocolVersion(SplitProtocolVersion);
                                msg->Record.MutableTableStats()->MutableKeyAccessSample()->Clear();
                                msg->Record.MutableTableStats()->SetSplitByLoadSuggestedKey(ToSerialized(splitPoint));
                            }
                            break;
                        default:
                            UNIT_ASSERT_C(false, TStringBuilder() << "Unsupported protocol version " << SplitProtocolVersion << ". Consider to support it in a simulation?");
                    };

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvGetTableStatsResult, from datashard " << msg->Record.GetDatashardId()
                        << ", from followerId " << msg->Record.GetFollowerId()
                        << ", patch KeyAccessSample: split point " << splitPoint
                        << " (start=" << start
                        << ", end=" << end
                        << ")"
                        << " " << msg->Record.GetTableStats().DebugString()
                        << Endl;

                    if (SendDuplicateTableStats != ESendDuplicateTableStatsStrategy::None) {
                        Y_ASSERT(!ev->Cookie);

                        std::unique_ptr<TEvDataShard::TEvGetTableStatsResult> msg_copy(
                            new TEvDataShard::TEvGetTableStatsResult()
                        );

                        msg_copy->Record.CopyFrom(msg->Record);

                        std::unique_ptr<IEventHandle> msg_copy_handle(
                            new IEventHandle(
                                ev->GetRecipientRewrite(),
                                ev->Sender,
                                msg_copy.release(),
                                0 /* flags */,
                                MagicDuplicateMessageCookie
                            )
                        );

                        switch (SendDuplicateTableStats) {
                            case ESendDuplicateTableStatsStrategy::None:
                                break;

                            case ESendDuplicateTableStatsStrategy::Immediately:
                                {
                                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                                        << ", sending a duplicate EvGetTableStatsResult "
                                        << "from datashard " << msg->Record.GetDatashardId()
                                        << Endl;

                                    TestRuntime->SendAsync(msg_copy_handle.release());
                                }
                                break;

                            case ESendDuplicateTableStatsStrategy::AfterSplitAck:
                                {
                                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                                        << ", saving a duplicate EvGetTableStatsResult "
                                        << "from datashard " << msg->Record.GetDatashardId()
                                        << " (will be sent after EvSplitAck)"
                                        << Endl;

                                    DuplicateTableStatsByDatashardId[msg->Record.GetDatashardId()] =
                                        std::move(msg_copy_handle);
                                }
                                break;
                        }
                    }
                }
                break;
            case TEvDataShard::EvSplit:
                // save key ranges of the new datashards
                {
                    const auto msg = ev->Get<TEvDataShard::TEvSplit>();

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvSplit, to datashard " << msg->Record.GetSplitDescription().GetSourceRanges(0).GetTabletID()
                        << ", event:"
                        << Endl
                        << msg->Record.DebugString()
                        << Endl;

                    // remove info for the source shard(s) that will be splitted
                    // (split will have a single source range, merge - multiple)
                    const TString splitMergeVerb =
                        (msg->Record.GetSplitDescription().GetSourceRanges().size() > 1)
                            ? "\n... were merged"
                            : " was splitted";

                    auto sourceShardsBuilder = TStringBuilder();
                    TString datashardName =
                        (msg->Record.GetSplitDescription().GetSourceRanges().size() > 1)
                            ? "\n... datashard "
                            : ", datashard ";

                    for (const auto& i : msg->Record.GetSplitDescription().GetSourceRanges()) {
                        DatashardsKeyRanges.erase(i.GetTabletID());

                        const ui64 start = FromSerialized(i.GetKeyRangeBegin());
                        // NOTE: empty KeyRangeEnd means infinity
                        const auto keyRangeEnd = i.GetKeyRangeEnd();
                        const ui64 end = (keyRangeEnd.size() > 0) ? FromSerialized(keyRangeEnd) : 0;

                        sourceShardsBuilder << datashardName << i.GetTabletID()
                            << " (start="  << start
                            << ", end=" << ((end != 0) ? end : 1000000)
                            << ")";
                    }

                    // add info for destination shards
                    for (const auto& i : msg->Record.GetSplitDescription().GetDestinationRanges()) {
                        const ui64 start = FromSerialized(i.GetKeyRangeBegin());
                        // NOTE: empty KeyRangeEnd means infinity
                        const auto keyRangeEnd = i.GetKeyRangeEnd();
                        const ui64 end = (keyRangeEnd.size() > 0) ? FromSerialized(keyRangeEnd) : 0;

                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << sourceShardsBuilder
                            << splitMergeVerb
                            << " into datashard " << i.GetTabletID()
                            << " (start="  << start
                            << ", end=" << ((end != 0) ? end : 1000000)
                            << ")"
                            << Endl;

                        DatashardsKeyRanges[i.GetTabletID()] = std::make_pair(start, end);
                    }

                    ++SplitReqCount;
                }
                break;
            case TEvDataShard::EvSplitAck:
                // count splits
                {
                    const auto msg = ev->Get<TEvDataShard::TEvSplitAck>();

                    const auto now = TestRuntime->GetCurrentTime();
                    const auto elapsed = now - LastSplitAckTime;
                    LastSplitAckTime = now;

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvSplitAck, from datashard " << msg->Record.GetTabletId()
                        << ", " << elapsed << " since last split ack"
                        << Endl;

                    ++SplitAckCount;

                    // Check, if need to send a duplicate EvGetTableStatsResult to the same shard
                    if (SendDuplicateTableStats == ESendDuplicateTableStatsStrategy::AfterSplitAck) {
                        const auto it_stats = DuplicateTableStatsByDatashardId.find(msg->Record.GetTabletId());

                        if (it_stats != DuplicateTableStatsByDatashardId.end()) {
                            Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                                << ", sending a duplicate EvGetTableStatsResult "
                                << "from datashard " << msg->Record.GetTabletId()
                                << Endl;

                            TestRuntime->SendAsync(it_stats->second.release());
                            DuplicateTableStatsByDatashardId.erase(it_stats);
                        }
                    }
                }
                break;
        }
    };
};

TTestEnv SetupEnv(TTestBasicRuntime &runtime, TTestEnvOptions& opts) {
    opts.EnableBackgroundCompaction(false);
    opts.DataShardStatsReportIntervalSeconds(0);

    TTestEnv env(runtime, opts);

    NDataShard::gDbStatsDataSizeResolution = 10;
    NDataShard::gDbStatsRowCountResolution = 10;

    {
        auto& appData = runtime.GetAppData();

        appData.FeatureFlags.SetEnablePersistentPartitionStats(true);

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);
    }

    runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

    // apply config changes to schemeshard via reboot
    //FIXME: make it possible to set config before initial boot
    GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

    return env;
}
TTestEnv SetupEnv(TTestBasicRuntime &runtime) {
    TTestEnvOptions opts;
    return SetupEnv(runtime, opts);
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardSplitByLoad) {

    /**
     * Execute a test on the given table, which simulates high CPU load on the leader and/or followers
     * and verifies that the table is split into the correct number of partitions.
     *
     * @param[in] runtime The test runtime
     * @param[in] tablePath The table to use for the test
     * @param[in] targetCpuLoadByFollowerIdPeriodic The map from the follower ID (0 == leader)
     *                                              to the corresponding induced CPU load (as percent)
     *                                              (for EvPeriodicTableStats)
     * @param[in] targetCpuLoadByFollowerIdStats The map from the follower ID (0 == leader)
     *                                           to the corresponding induced CPU load (as percent)
     *                                           (for EvGetTableStatsResult)
     *                                           (any negative value == unset the CPU usage value)
     * @param[in] shouldSendReadRequests If true, send EvRead requests to all followers
     * @param[in] sendDuplicateTableStats Determines if/when to send duplicate EvGetTableStatsResult messages
     * @param[in] expectTableToBeSplitted If true, expect the table to be splitted
     */
    void SplitByLoad(
        TTestActorRuntime& runtime,
        const TString& tablePath,
        const std::map<ui32, i32>& targetCpuLoadByFollowerIdPeriodic,
        const std::map<ui32, i32>& targetCpuLoadByFollowerIdStats,
        bool shouldSendReadRequests = false,
        ESendDuplicateTableStatsStrategy sendDuplicateTableStats = ESendDuplicateTableStatsStrategy::None,
        bool expectTableToBeSplitted = true
    ) {
        auto tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table initial state:" << Endl << tableInfo.DebugString() << Endl;

        const ui64 tableLocalPathId = tableInfo.GetPathDescription().GetSelf().GetPathId();
        const ui64 tableOwnerId = tableInfo.GetPathDescription().GetSelf().GetSchemeshardId();
        const ui64 initialDatashardId = tableInfo.GetPathDescription().GetTablePartitions(0).GetDatashardId();

        TLoadAndSplitSimulator simulator(
            tableLocalPathId,
            tableOwnerId,
            initialDatashardId,
            shouldSendReadRequests,
            sendDuplicateTableStats,
            targetCpuLoadByFollowerIdPeriodic,
            targetCpuLoadByFollowerIdStats,
            runtime
        );

        auto observerHolder = runtime.AddObserver(
            [&simulator](IEventHandle::TPtr& event) {
                simulator.ChangeEvent(event);
            }
        );

        if (expectTableToBeSplitted) {
            runtime.WaitFor(
                "the table to be splitted",
                [&simulator, &runtime]() -> bool {
                    auto now = runtime.GetCurrentTime();
                    return (simulator.SplitAckCount > 0) && ((now - simulator.LastSplitAckTime) > TDuration::Seconds(15));
                },
                TDuration::Seconds(60)
            );
        } else {
            runtime.WaitFor(
                "the confirmation that the table is not splitting",
                [&simulator]() -> bool {
                    return (simulator.PeriodicTableStatsCount > 10) && (simulator.SplitReqCount == 0);
                },
                TDuration::Seconds(60)
            );
        }

        Cerr << "TEST SplitByLoad, splitted " << simulator.SplitAckCount << " times"
            << ", datashard count " << simulator.DatashardsKeyRanges.size()
            << Endl;
        // Cerr << "TEST SplitByLoad, PeriodicTableStats " << simulator.PeriodicTableStatsCount << Endl;
        // Cerr << "TEST SplitByLoad, KeyAccessSampleReq " << simulator.KeyAccessSampleReqCount << Endl;
        // Cerr << "TEST SplitByLoad, SplitReq " << simulator.SplitReqCount << Endl;
    }

    void NoSplitByLoad(TTestActorRuntime& runtime, const TString &tablePath, ui32 targetCpuLoadPercent) {
        auto tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table initial state:" << Endl << tableInfo.DebugString() << Endl;

        const ui64 tableLocalPathId = tableInfo.GetPathDescription().GetSelf().GetPathId();
        const ui64 tableOwnerId = tableInfo.GetPathDescription().GetSelf().GetSchemeshardId();
        const ui64 initialDatashardId = tableInfo.GetPathDescription().GetTablePartitions(0).GetDatashardId();

        TLoadAndSplitSimulator simulator(
            tableLocalPathId,
            tableOwnerId,
            initialDatashardId,
            false /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::None,
            {{0, targetCpuLoadPercent}}, // Target CPU load for the leader only
            {{0, targetCpuLoadPercent}}, // Target CPU load for the leader only
            runtime
        );

        auto observerHolder = runtime.AddObserver(
            [&simulator](IEventHandle::TPtr& event) {
                simulator.ChangeEvent(event);
            }
        );

        runtime.WaitFor(
            "the confirmation that the table is not splitting",
            [&simulator]() -> bool {
                return (simulator.PeriodicTableStatsCount > 10) && (simulator.KeyAccessSampleReqCount == 0);
            },
            TDuration::Seconds(60)
        );

        Cerr << "TEST NoSplitByLoad, splitted " << simulator.SplitAckCount << " times"
            << ", datashard count " << simulator.DatashardsKeyRanges.size()
            << Endl;
        // Cerr << "TEST SplitByLoad, PeriodicTableStats " << simulator.PeriodicTableStatsCount << Endl;
        // Cerr << "TEST SplitByLoad, KeyAccessSampleReq " << simulator.KeyAccessSampleReqCount << Endl;
        // Cerr << "TEST SplitByLoad, SplitReq " << simulator.SplitReqCount << Endl;
    }

    static void TableSplitsUpToMaxPartitionsCount(ui32 splitProtocolVersion) {
        TTestEnvOptions opts;
        if (splitProtocolVersion == 1) {
            opts.EnableDataShardSplitHistogramSorting(true);
        } else if (splitProtocolVersion == 2) {
            opts.EnableDataShardSplitKeySelection(true);
        } else if (splitProtocolVersion == 3) {
            opts.EnableDataShardSplitHistogramOmission(true);
        }
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime, opts);

        UNIT_ASSERT_VALUES_EQUAL(splitProtocolVersion, GetSplitProtocolVersion(runtime));

        const ui32 expectedPartitionCount = 5;
        const ui32 cpuLoadThreshold = 1;    // percents
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"       Type: "Uint64"}
                Columns { Name: "value"     Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {

                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true

                            CpuPercentageThreshold: %d  # replacement field for cpu load split threshold, percents

                        }
                    }
                }
            )",
            expectedPartitionCount,
            cpuLoadThreshold
        );

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, cpuLoadSimulated}} // Target CPU load for the leader only
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }
    struct TTestRegistrationTableSplitsUpToMaxPartitionsCount {
        TTestRegistrationTableSplitsUpToMaxPartitionsCount() {
            static std::vector<TString> TestNames;

            constexpr ui32 MAX_SPLIT_PROTOCOL_VERSION = 3;

            for (const auto& i : xrange(MAX_SPLIT_PROTOCOL_VERSION + 1)) {
                TestNames.emplace_back(TStringBuilder() << "TableSplitsUpToMaxPartitionsCount-protocol" << i);

                TCurrentTest::AddTest(
                    TestNames.back().c_str(),
                    std::bind(std::bind(TableSplitsUpToMaxPartitionsCount, i), std::placeholders::_1),
                    /*forceFork*/ false
                );
            }
        }
    };
    static TTestRegistrationTableSplitsUpToMaxPartitionsCount testRegistrationTableSplitsUpToMaxPartitionsCount;

    Y_UNIT_TEST(IndexTableSplitsUpToMainTableCurrentPartitionCount) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui32 cpuLoadThreshold = 1;    // percents
        const ui64 cpuLoadSimulated = 100;  // percents

        // NOTE: The main table for the index should start with the expected number
        //       of partitions (see UniformPartitionsCount below) to make sure
        //       the index table splits up to this limit. The overall limit
        //       on the number of partitions (see MaxPartitionsCount below)
        //       is irrelevant, because the main table is not going to be split
        //       by this test (only the index table will be).
        const auto mainTableScheme = Sprintf(
            R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key"       Type: "Uint64"}
                    Columns { Name: "value"     Type: "Uint64"}
                    KeyColumnNames: ["key"]

                    UniformPartitionsCount: %d  # replacement field for required number of partitions

                    PartitionConfig {
                        PartitioningPolicy {
                            MaxPartitionsCount: 10
                            SplitByLoadSettings: {
                                Enabled: true

                                CpuPercentageThreshold: %d  # replacement field for cpu load split threshold, percents

                            }
                        }
                    }
                }
                IndexDescription {
                    Name: "by-value"
                    KeyColumnNames: ["value"]
                }
            )",
            expectedPartitionCount,
            cpuLoadThreshold
        );

        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", mainTableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table/by-value/indexImplTable",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, cpuLoadSimulated}} // Target CPU load for the leader only
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table/by-value/indexImplTable", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    Y_UNIT_TEST(IndexTableDoesNotSplitsIfDisabledByMainTable) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 cpuLoadThreshold = 1;    // percents
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto mainTableScheme = Sprintf(
            R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key"       Type: "Uint64"}
                    Columns { Name: "value"     Type: "Uint64"}
                    KeyColumnNames: ["key"]

                    UniformPartitionsCount: 5

                    PartitionConfig {
                        PartitioningPolicy {
                            MaxPartitionsCount: 10
                            SplitByLoadSettings: {
                                Enabled: false

                                CpuPercentageThreshold: %d  # replacement field for cpu load split threshold, percents

                            }
                        }
                    }
                }
                IndexDescription {
                    Name: "by-value"
                    KeyColumnNames: ["value"]
                }
            )",
            cpuLoadThreshold
        );

        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", mainTableScheme);
        env.TestWaitNotification(runtime, txId);

        NoSplitByLoad(runtime, "/MyRoot/Table/by-value/indexImplTable", cpuLoadSimulated);

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table/by-value/indexImplTable", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(1)});
    }

    /**
     * Verify that if the EvGetTableStatsResult message comes while an ongoing
     * split transaction is in progress, the message is ignored and the second split
     * transaction is not started concurrently with the first one.
     */
    Y_UNIT_TEST(ConcurrentSplitTransactionIgnored) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 1
                        }
                    }

                    FollowerCount: 3
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            false /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::Immediately // Trigger concurrent split transactions
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    /**
     * Verify that if the EvGetTableStatsResult message comes again after the current
     * split transaction completes, the message is ignored and the second split
     * transaction is not started after the first one.
     */
    Y_UNIT_TEST(DuplicateSplitTransactionIgnored) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 1
                        }
                    }

                    FollowerCount: 3
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            false /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::AfterSplitAck // Trigger duplicate split transactions
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    /**
     * Verify that a shard is split automatically, when some of the followers
     * become overloaded with requests.
     */
    Y_UNIT_TEST_FLAGS_N(TableSplitsByFollowerLoad, bool DataShardSplitHistogramSorting, bool DataShardSplitKeySelection, bool DataShardSplitHistogramOmission) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime, TTestEnvOptions()
            .EnableDataShardSplitHistogramSorting(DataShardSplitHistogramSorting)
            .EnableDataShardSplitKeySelection(DataShardSplitKeySelection)
            .EnableDataShardSplitHistogramOmission(DataShardSplitHistogramOmission)
        );

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 1
                        }
                    }

                    FollowerCount: 3
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {
                // No simulated CPU load for the leader, only for one of the followers
                {0, 0},
                {1, 0},
                {2, cpuLoadSimulated},
                {3, 0},
            },
            {
                // No simulated CPU load for the leader, only for one of the followers
                {0, 0},
                {1, 0},
                {2, cpuLoadSimulated},
                {3, 0},
            },
            true /* shouldSendReadRequests */
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    /**
     * Verify that a shard does not split, if the CPU load jumps to a high value,
     * when the EvPeriodicTableStats message is generated, but drops to a low value,
     * when the EvGetTableStats message is processed and the EvGetTableStatsResult
     * response is generated. This is for the CPU load on the leader only.
     */
    Y_UNIT_TEST(NoSplitWithLeaderSpikeLoad) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"       Type: "Uint64"}
                Columns { Name: "value"     Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 1
                        }
                    }
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, 0}}, // The CPU load drops to 0% for EvGetTableStatsResult
            false /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::None,
            false /* expectTableToBeSplitted */
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(1)});
    }

    /**
     * Verify that a shard does not split, if the CPU load jumps to a high value,
     * when the EvPeriodicTableStats message is generated, but drops to a low value,
     * when the EvGetTableStats message is processed and the EvGetTableStatsResult
     * response is generated. This is for the CPU load on the leader only.
     */
    Y_UNIT_TEST(NoSplitWithFollowerSpikeLoad) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 1
                        }
                    }

                    FollowerCount: 3
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {
                // No simulated CPU load for the leader, only for one of the followers
                {0, 0},
                {1, 0},
                {2, cpuLoadSimulated},
                {3, 0},
            },
            {
                // The CPU load drops to 0% for EvGetTableStatsResult
                {0, 0},
                {1, 0},
                {2, 0},
                {3, 0},
            },
            true /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::None,
            false /* expectTableToBeSplitted */
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(1)});
    }

    /**
     * Verify that a shard splits correctly, even if the CPU usage value
     * in the EvGetTableStatsResult response is not populated.
     */
    Y_UNIT_TEST(SplitWithoutCpuUsageInStatsResponse) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"       Type: "Uint64"}
                Columns { Name: "value"     Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 1
                        }
                    }
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, -100}} // Explicitly clear the CPU value from EvGetTableStatsResult
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    /**
     * Verify that the split-by-load logic uses the correct default value
     * for the splitting threshold (50%) when the load is only on the leader.
     */
    Y_UNIT_TEST(CorrectSplitThresholdDefaultValueLeader) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 51;  // percents

        // NOTE: No split threshold settings here, use only the default value
        //       (CpuPercentageThreshold = 50).
        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"       Type: "Uint64"}
                Columns { Name: "value"     Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                        }
                    }
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {{0, cpuLoadSimulated}}, // Target CPU load for the leader only
            {{0, cpuLoadSimulated}}  // Target CPU load for the leader only
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    /**
     * Verify that the split-by-load logic uses the correct default value
     * for the splitting threshold (50%) when the load is only on the followers.
     */
    Y_UNIT_TEST(CorrectSplitThresholdDefaultValueFollower) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui64 cpuLoadSimulated = 51;  // percents

        // NOTE: No split threshold settings here, use only the default value
        //       (CpuPercentageThreshold = 50).
        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"       Type: "Uint64"}
                Columns { Name: "value"     Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true
                        }
                    }

                    FollowerCount: 3
                }
            )",
            expectedPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(
            runtime,
            "/MyRoot/Table",
            {
                // No simulated CPU load for the leader, only for one of the followers
                {0, 0},
                {1, 0},
                {2, cpuLoadSimulated},
                {3, 0},
            },
            {
                // No simulated CPU load for the leader, only for one of the followers
                {0, 0},
                {1, 0},
                {2, cpuLoadSimulated},
                {3, 0},
            },
            true /* shouldSendReadRequests */
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }
}

/**
 * The set of tests, which verify the merge-by-load logic in the SchemeShard class.
 */
Y_UNIT_TEST_SUITE(TSchemeShardMergeByLoad) {
    /**
     * Execute a test on the given table, which simulates high CPU load on the leader and/or followers
     * to split it and then drops the CPU load to allow the table to be merged back.
     *
     * @param[in] runtime The test runtime
     * @param[in] tablePath The table to use for the test
     * @param[in] maxPartitionCount The expected number of partitions (after splitting)
     * @param[in] finalPartitionCount The expected number of partitions (after merging)
     * @param[in] splitCpuLoadByFollowerId The map from the follower ID (0 == leader)
     *                                     to the corresponding induced CPU load (as percent),
     *                                     which will be used for splitting the table
     * @param[in] mergeCpuLoadByFollowerId The map from the follower ID (0 == leader)
     *                                     to the corresponding induced CPU load (as percent),
     *                                     which will be used for merging the table
     * @param[in] shouldSendReadRequests If true, send EvRead requests to all followers
     * @param[in] expectTableToBeMerged If true, expect the table to be merged
     */
    void MergeByLoad(
        TTestActorRuntime& runtime,
        const TString& tablePath,
        ui32 maxPartitionCount,
        ui32 finalPartitionCount,
        const std::map<ui32, i32>& splitCpuLoadByFollowerId,
        const std::map<ui32, i32>& mergeCpuLoadByFollowerId,
        bool shouldSendReadRequests,
        bool expectTableToBeMerged
    ) {
        auto tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table initial state:" << Endl << tableInfo.DebugString() << Endl;

        TestDescribeResult(tableInfo, {NLs::PartitionCount(1)});

        const ui64 tableLocalPathId = tableInfo.GetPathDescription().GetSelf().GetPathId();
        const ui64 tableOwnerId = tableInfo.GetPathDescription().GetSelf().GetSchemeshardId();
        const ui64 initialDatashardId = tableInfo.GetPathDescription().GetTablePartitions(0).GetDatashardId();

        TLoadAndSplitSimulator simulatorSplit(
            tableLocalPathId,
            tableOwnerId,
            initialDatashardId,
            shouldSendReadRequests,
            ESendDuplicateTableStatsStrategy::None,
            splitCpuLoadByFollowerId,
            splitCpuLoadByFollowerId,
            runtime
        );

        auto observerHolderSplit = runtime.AddObserver(
            [&simulatorSplit](IEventHandle::TPtr& event) {
                simulatorSplit.ChangeEvent(event);
            }
        );

        // Wait for the table to be fully splitted
        runtime.WaitFor(
            "the table to be splitted",
            [&simulatorSplit, &runtime]() -> bool {
                auto now = runtime.GetCurrentTime();
                return (simulatorSplit.SplitAckCount > 0)
                    && ((now - simulatorSplit.LastSplitAckTime) > TDuration::Seconds(15));
            }
        );

        Cerr << "TEST MergeByLoad, splitted " << simulatorSplit.SplitAckCount << " times"
            << ", datashard count " << simulatorSplit.DatashardsKeyRanges.size()
            << Endl;

        tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table state after splitting:" << Endl << tableInfo.DebugString() << Endl;

        TestDescribeResult(tableInfo, {NLs::PartitionCount(maxPartitionCount)});

        // Start a new simulator, which will handle the merge back
        TLoadAndSplitSimulator simulatorMerge(
            tableLocalPathId,
            tableOwnerId,
            initialDatashardId,
            shouldSendReadRequests,
            ESendDuplicateTableStatsStrategy::None,
            mergeCpuLoadByFollowerId,
            mergeCpuLoadByFollowerId,
            runtime
        );

        // The simulator for the merge should use the final shards from splitting
        simulatorMerge.DatashardsKeyRanges = simulatorSplit.DatashardsKeyRanges;

        observerHolderSplit.Remove();

        auto observerHolderMerge = runtime.AddObserver(
            [&simulatorMerge](IEventHandle::TPtr& event) {
                simulatorMerge.ChangeEvent(event);
            }
        );

        // NOTE: To force splitting, the simulator induces very high CPU load
        //       for all EvPeriodicTableStats events. To force merging, the simulator
        //       induces medium CPU load for all EvPeriodicTableStats events.
        //       The problem is that the merge-by-load code takes into account
        //       the peak CPU usage over a certain time period. To make sure
        //       the two CPU loads (for splitting and for merging) do not interfere
        //       with each other, the test needs to wait for some time to make sure
        //       there is enough gap between the high and the medium CPU loads
        //       for all shards.
        Cerr << "TEST waiting for the CPU load data to settle..." << Endl;
        runtime.SimulateSleep(TDuration::Seconds(10));
        Cerr << "TEST finished waiting for the CPU load data to settle..." << Endl;

        // Before forcing the table to be merged, reduce the thresholds
        // for partition merging to make the test execute faster
        TControlBoard::SetValue(
            1,
            runtime.GetAppData().Icb->SchemeShardControls.MergeByLoadMinUptimeSec
        );

        TControlBoard::SetValue(
            10,
            runtime.GetAppData().Icb->SchemeShardControls.MergeByLoadMinLowLoadDurationSec
        );

        // Wait for the table to be fully merged back
        if (expectTableToBeMerged) {
            runtime.WaitFor(
                "the table to be merged",
                [&simulatorMerge, &runtime]() -> bool {
                    auto now = runtime.GetCurrentTime();
                    return (simulatorMerge.SplitAckCount > 0)
                        && ((now - simulatorMerge.LastSplitAckTime) > TDuration::Seconds(15));
                }
            );
        } else {
            runtime.WaitFor(
                "the confirmation that the table is not merging",
                [&simulatorMerge]() -> bool {
                    return (simulatorMerge.PeriodicTableStatsCount > 50)
                        && (simulatorMerge.SplitAckCount == 0);
                },
                TDuration::Seconds(60)
            );
        }

        Cerr << "TEST MergeByLoad, merged " << simulatorMerge.SplitAckCount << " times"
            << ", datashard count " << simulatorMerge.DatashardsKeyRanges.size()
            << Endl;

        tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table state after merging:" << Endl << tableInfo.DebugString() << Endl;

        TestDescribeResult(tableInfo, {NLs::PartitionCount(finalPartitionCount)});
    }

    /**
     * Verify that a shard merges all partitions when there is no CPU load
     * neither on the leader nor on the followers.
     */
    Y_UNIT_TEST(MergeWithoutLeaderOrFollowerLoad) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 maxPartitionCount = 5;

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        MaxPartitionsCount: %d

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 70
                        }
                    }

                    FollowerCount: 3
                }
            )",
            maxPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        MergeByLoad(
            runtime,
            "/MyRoot/Table",
            maxPartitionCount,
            1 /* finalPartitionCount */,
            {
                // The initial CPU load only on the leaders to force the split
                {0, 100},
                {1, 0},
                {2, 0},
                {3, 0},
            },
            {
                // Drop the CPU load to 0% both on the leader and all the followers
                // to force the table to be merged back to a single partition
                {0, 0},
                {1, 0},
                {2, 0},
                {3, 0},
            },
            true /* shouldSendReadRequests */,
            true /* expectTableToBeMerged */
        );
    }

    /**
     * Verify that a shard does not merge partitions when there is CPU load
     * on the leader.
     */
    Y_UNIT_TEST(NoMergeWithLeaderLoad) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 maxPartitionCount = 5;

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        MaxPartitionsCount: %d

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 70
                        }
                    }

                    FollowerCount: 3
                }
            )",
            maxPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        MergeByLoad(
            runtime,
            "/MyRoot/Table",
            maxPartitionCount,
            maxPartitionCount /* finalPartitionCount */,
            {
                // The initial CPU load only on the leaders to force the split
                {0, 100},
                {1, 0},
                {2, 0},
                {3, 0},
            },
            {
                // Keep the CPU load on the leader to prevent the table from merging
                //
                // WARNING: The CPU percentage here must be below the threshold
                //          for splitting (70%), but above the threshold for merging
                //          (70% of the splitting threshold == 50%)
                {0, 60},
                {1, 0},
                {2, 0},
                {3, 0},
            },
            true /* shouldSendReadRequests */,
            false /* expectTableToBeMerged */
        );
    }

    /**
     * Verify that a shard does not merge partitions when there is CPU load
     * on the followers.
     */
    Y_UNIT_TEST(NoMergeWithFollowerLoad) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 maxPartitionCount = 5;

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
                KeyColumnNames: ["key"]
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        MaxPartitionsCount: %d

                        SplitByLoadSettings: {
                            Enabled: true
                            CpuPercentageThreshold: 70
                        }
                    }

                    FollowerCount: 3
                }
            )",
            maxPartitionCount
        );

        ui64 txId = 100;
        TestCreateTable(runtime, txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        MergeByLoad(
            runtime,
            "/MyRoot/Table",
            maxPartitionCount,
            5 /* finalPartitionCount */,
            {
                // The initial CPU load only on the leaders to force the split
                {0, 100},
                {1, 0},
                {2, 0},
                {3, 0},
            },
            {
                // Drop the CPU load to 0% on the leader, but raise the load
                // on all the followers to prevent the table from merging
                //
                // WARNING: The CPU percentage here must be below the threshold
                //          for splitting (70%), but above the threshold for merging
                //          (70% of the splitting threshold == 50%)
                {0, 0},
                {1, 60},
                {2, 60},
                {3, 60},
            },
            true /* shouldSendReadRequests */,
            false /* expectTableToBeMerged */
        );
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSplitMergeValidation) {
    Y_UNIT_TEST(SplitWithDuplicateSourceTabletId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "Key" Type: "Uint64"}
            Columns { Name: "Value" Type: "Utf8"}
            KeyColumnNames: ["Key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        // Try to split with duplicate SourceTabletId - should fail with StatusInvalidParameter
        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
            SourceTabletId: 72075186233409546
            SourceTabletId: 72075186233409546
        )", {{NKikimrScheme::StatusInvalidParameter, "Duplicate SourceTabletId"}});
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardConsistencyCheckCounter) {
    // COUNTER_TABLE_PARTITIONS_CONSISTENCY_CHECK_TIME_NS accumulates the wall-clock
    // nanoseconds spent in TTableInfo::VerifyConsistency(), gated by the
    // EnableTablePartitionsConsistencyCheck feature flag (default on).
    static constexpr const char* CounterName = "SchemeShard/TablePartitionsConsistencyCheckTimeNs";

    Y_UNIT_TEST(ZeroWhenFlagOff) {
        // With the check disabled, VerifyConsistency() returns early and resets
        // LastVerifyConsistencyTime to 0, so every report site increments by 0
        // and the cumulative counter stays exactly 0.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsConsistencyCheck(false);

        // Many partitions: a verify here would be measurable if it ran at all.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 200
        )");
        env.TestWaitNotification(runtime, txId);

        // Exercise ApplySplitMerge as well (the first shard of a uniform table).
        TestSplitTable(runtime, ++txId, "/MyRoot/Table", Sprintf(R"(
            SourceTabletId: %lu
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 1 } } } }
        )", TTestTxConfig::FakeHiveTablets + 0));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetCumulativeCounter(runtime, CounterName), 0u);
    }

    Y_UNIT_TEST(ReportedWhenFlagOn) {
        // With the check enabled (default), the cumulative counter accumulates a
        // non-zero time across the partitioning report sites. A 200-partition
        // verify reliably exceeds 1us, so create/copy/move alone guarantee > 0;
        // split and merge additionally cover the ApplySplitMerge report sites and
        // assert the verification invariants hold on real operation results.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsConsistencyCheck(true);

        // Small table with explicit, predictable shard ids for split/merge.
        // Partitions: (-inf,"A") -> F+0, ["A","B") -> F+1, ["B",+inf) -> F+2.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "SplitMe"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } } } }
            PartitionConfig {
                PartitioningPolicy { MinPartitionsCount: 1 SizeToSplit: 100500 }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Split the first partition -> ApplySplitMerge (split).
        TestSplitTable(runtime, ++txId, "/MyRoot/SplitMe", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "0" } } } }
            )",
            TTestTxConfig::FakeHiveTablets + 0
        ));
        env.TestWaitNotification(runtime, txId);

        // Merge the two non-first partitions F+1, F+2 -> ApplySplitMerge (merge).
        TestSplitTable(runtime, ++txId, "/MyRoot/SplitMe", Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )",
            TTestTxConfig::FakeHiveTablets + 1,
            TTestTxConfig::FakeHiveTablets + 2
        ));
        env.TestWaitNotification(runtime, txId);

        // Big table to force a measurable verify time.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "BigTable"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 200
        )");
        env.TestWaitNotification(runtime, txId);

        // Copy -> SetPartitioning on the new table.
        TestCopyTable(runtime, ++txId, "/MyRoot", "BigCopy", "/MyRoot/BigTable");
        env.TestWaitNotification(runtime, txId);

        // Move -> MovePartitioning + the move_table report site.
        TestMoveTable(runtime, ++txId, "/MyRoot/BigTable", "/MyRoot/BigMoved");
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_GT(GetCumulativeCounter(runtime, CounterName), 0u);
    }
}

// Integration + fairness tests for the split/merge candidacy memory (PR1).
// Flag EnableSplitMergeDemandTracking + the EnableSplitMergeFairScheduling immediate control.
Y_UNIT_TEST_SUITE(TSchemeShardSplitMergeHistory) {

    // Mirrors the anonymous-namespace SetupEnv, but enables the history feature and (optionally)
    // installs a split in-flight limit before the schemeshard boots so deferrals are forced.
    TTestEnv SetupHistoryEnv(TTestBasicRuntime& runtime, bool enableHistory, bool enableFairScheduler,
            ui64 splitInFlightLimit = 0) {
        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(0);
        opts.EnableSplitMergeDemandTracking(enableHistory);

        TTestEnv env(runtime, opts);

        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        {
            auto& appData = runtime.GetAppData();
            appData.FeatureFlags.SetEnablePersistentPartitionStats(true);
            appData.FeatureFlags.SetEnableSplitMergeDemandTracking(enableHistory);
            appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
            appData.SchemeShardConfig.SetStatsMaxBatchSize(0);
            if (splitInFlightLimit) {
                auto* counter = appData.SchemeShardConfig.AddInFlightCounterConfig();
                counter->SetType(NKikimr::NSchemeShard::ESimpleCounters::COUNTER_IN_FLIGHT_OPS_TxSplitTablePartition);
                counter->SetInFlightLimit(splitInFlightLimit);
            }
        }

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);

        // Apply the config + flag via reboot (the schemeshard reads them at init).
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        if (enableFairScheduler) {
            TControlBoard::SetValue(1, runtime.GetAppData().Icb->SchemeShardControls.EnableSplitMergeFairScheduling);
        }
        return env;
    }

    TString HotTableScheme(const TString& name, ui32 maxPartitions, ui32 cpuThresholdPercent,
            ui32 uniformPartitions = 1) {
        return Sprintf(R"(
                Name: "%s"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: %d
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: %d
                        SplitByLoadSettings: { Enabled: true CpuPercentageThreshold: %d }
                    }
                }
            )", name.c_str(), uniformPartitions, maxPartitions, cpuThresholdPercent);
    }

    THolder<TLoadAndSplitSimulator> MakeHotSimulator(TTestActorRuntime& runtime, const TString& path) {
        auto desc = DescribePrivatePath(runtime, path, true, true);
        const ui64 localId = desc.GetPathDescription().GetSelf().GetPathId();
        const ui64 ownerId = desc.GetPathDescription().GetSelf().GetSchemeshardId();
        const ui64 ds0 = desc.GetPathDescription().GetTablePartitions(0).GetDatashardId();
        return MakeHolder<TLoadAndSplitSimulator>(
            localId, ownerId, ds0,
            false /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::None,
            std::map<ui32, i32>{{0, 100}},  // 100% CPU on the leader (periodic stats)
            std::map<ui32, i32>{{0, 100}},  // 100% CPU on the leader (get-stats result)
            runtime);
    }

    ui32 PartitionCountOf(TTestActorRuntime& runtime, const TString& path) {
        auto desc = DescribePrivatePath(runtime, path, true, true);
        return desc.GetPathDescription().TablePartitionsSize();
    }

    // Flag OFF: a split-by-load table still splits normally, and the memory counters never move.
    Y_UNIT_TEST(FlagOff_NoMemory) {
        TTestBasicRuntime runtime;
        auto env = SetupHistoryEnv(runtime, /* enableHistory */ false, /* enableFairScheduler */ false);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", HotTableScheme("Table", /* maxPartitions */ 4, /* cpu */ 1));
        env.TestWaitNotification(runtime, txId);

        auto simulator = MakeHotSimulator(runtime, "/MyRoot/Table");
        auto observer = runtime.AddObserver([&simulator](IEventHandle::TPtr& ev) { simulator->ChangeEvent(ev); });

        runtime.WaitFor("the table to split", [&simulator, &runtime]() -> bool {
            return (simulator->SplitAckCount > 0)
                && ((runtime.GetCurrentTime() - simulator->LastSplitAckTime) > TDuration::Seconds(15));
        }, TDuration::Seconds(60));

        // Behavior unchanged: the table split. Memory disabled: the deferred gauges stay at zero.
        UNIT_ASSERT_GT(PartitionCountOf(runtime, "/MyRoot/Table"), 1u);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/PartitionsWithDeferredSplitMerge"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesWithDeferredSplitMerge"), 0u);
        // Always-on observability: demand was detected every stats cycle even with the flag off,
        // and no slot-limit deferral happened (no in-flight limit is configured).
        UNIT_ASSERT_GT(GetCumulativeCounter(runtime, "SchemeShard/SplitDemandDetected"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(GetCumulativeCounter(runtime, "SchemeShard/MergeDemandDetected"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(GetCumulativeCounter(runtime, "SchemeShard/SplitMergeDeferrals"), 0u);
    }

    // Flag ON, split slot limit 1: while one split is held in flight, other over-threshold shards
    // become deferred candidates and are recorded (counters move above zero).
    Y_UNIT_TEST(RecordsDeferredSplit) {
        TTestBasicRuntime runtime;
        auto env = SetupHistoryEnv(runtime, /* enableHistory */ true, /* enableFairScheduler */ false,
            /* splitInFlightLimit */ 1);

        ui64 txId = 100;
        // Start with several partitions so that, with one split slot busy, the rest defer.
        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 4
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: 100
                        SplitByLoadSettings: { Enabled: true CpuPercentageThreshold: 1 }
                    }
                }
            )"));
        env.TestWaitNotification(runtime, txId);

        // Hold split operations near completion so the single slot stays occupied and the other
        // hot shards keep hitting the in-flight limit (the deferral recording site).
        TBlockEvents<TEvDataShard::TEvSplitPartitioningChangedAck> splitBlocker(runtime);

        auto simulator = MakeHotSimulator(runtime, "/MyRoot/Table");
        auto observer = runtime.AddObserver([&simulator](IEventHandle::TPtr& ev) { simulator->ChangeEvent(ev); });

        // Drive load until a deferral is recorded (or give up after a bounded wait).
        bool deferredRecorded = false;
        for (ui32 i = 0; i < 40 && !deferredRecorded; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            deferredRecorded = GetSimpleCounter(runtime, "SchemeShard/PartitionsWithDeferredSplitMerge") > 0;
        }

        UNIT_ASSERT_C(deferredRecorded, "expected at least one deferred split to be recorded");
        UNIT_ASSERT_GT(GetSimpleCounter(runtime, "SchemeShard/TablesWithDeferredSplitMerge"), 0u);
        // Always-on observability: demand detection and slot-limit deferrals are counted.
        UNIT_ASSERT_GT(GetCumulativeCounter(runtime, "SchemeShard/SplitDemandDetected"), 0u);
        UNIT_ASSERT_GT(GetCumulativeCounter(runtime, "SchemeShard/SplitMergeDeferrals"), 0u);

        splitBlocker.Stop().Unblock();
    }

    // Fairness: two equally-hot tables, one split slot. With the fair scheduler on, neither table
    // is starved -- both eventually split (round-robin hands the freed slot across tables).
    Y_UNIT_TEST(FairScheduler_NoStarvationAcrossTables) {
        TTestBasicRuntime runtime;
        auto env = SetupHistoryEnv(runtime, /* enableHistory */ true, /* enableFairScheduler */ true,
            /* splitInFlightLimit */ 1);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", HotTableScheme("TableA", /* maxPartitions */ 3, /* cpu */ 1));
        env.TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, "/MyRoot", HotTableScheme("TableB", /* maxPartitions */ 3, /* cpu */ 1));
        env.TestWaitNotification(runtime, txId);

        auto simA = MakeHotSimulator(runtime, "/MyRoot/TableA");
        auto simB = MakeHotSimulator(runtime, "/MyRoot/TableB");
        auto observerA = runtime.AddObserver([&simA](IEventHandle::TPtr& ev) { simA->ChangeEvent(ev); });
        auto observerB = runtime.AddObserver([&simB](IEventHandle::TPtr& ev) { simB->ChangeEvent(ev); });

        // Both tables must split -- neither starves the other for the single split slot.
        // NOTE: assert on real partition counts (via Describe), not the simulators' SplitAckCount:
        // EvSplitAck is not table-scoped, so two concurrent simulators cross-count each other's acks.
        bool bothSplit = false;
        for (ui32 i = 0; i < 90 && !bothSplit; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            bothSplit = (PartitionCountOf(runtime, "/MyRoot/TableA") > 1)
                && (PartitionCountOf(runtime, "/MyRoot/TableB") > 1);
        }

        UNIT_ASSERT_GT_C(PartitionCountOf(runtime, "/MyRoot/TableA"), 1u, "TableA was starved");
        UNIT_ASSERT_GT_C(PartitionCountOf(runtime, "/MyRoot/TableB"), 1u, "TableB was starved");
    }

    // Control off: the scheduler is inert (stats-arrival behavior), but the memory still records.
    // The table still splits to its limit, and deferrals are recorded while the slot is busy.
    Y_UNIT_TEST(FairScheduler_Off_FallsBackToArrivalOrder) {
        TTestBasicRuntime runtime;
        auto env = SetupHistoryEnv(runtime, /* enableHistory */ true, /* enableFairScheduler */ false,
            /* splitInFlightLimit */ 1);

        ui64 txId = 100;
        // Start with two partitions and allow four: while the single split slot is held
        // by one shard's split, the sibling hot shard is a Ready shard whose demand passes
        // the shard-count check (ExpectedPartitionCount=3 < 4) and then hits the in-flight
        // limit (the deferral recording site). With maxPartitions=3 the in-flight split
        // already pushes ExpectedPartitionCount to the limit, so the sibling's demand is
        // rejected with ConditionsNotMet before the slot check and no deferral is recorded.
        TestCreateTable(runtime, ++txId, "/MyRoot",
            HotTableScheme("Table", /* maxPartitions */ 4, /* cpu */ 1, /* uniformPartitions */ 2));
        env.TestWaitNotification(runtime, txId);

        auto simulator = MakeHotSimulator(runtime, "/MyRoot/Table");
        auto observer = runtime.AddObserver([&simulator](IEventHandle::TPtr& ev) { simulator->ChangeEvent(ev); });

        // Hold the split op in flight: block the partitioning-change event the schemeshard
        // sends to the datashard (the op completes independently of the Ack, so blocking the
        // Ack does not keep the slot busy). While the single slot is occupied, the sibling
        // hot shard's stats hit the in-flight limit and the deferral is recorded.
        TBlockEvents<TEvDataShard::TEvSplitPartitioningChanged> splitBlocker(runtime);

        // Memory records even with the scheduler off: a deferral is seen while a split holds the slot.
        bool deferredSeen = false;
        for (ui32 i = 0; i < 40 && !deferredSeen; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            deferredSeen = GetSimpleCounter(runtime, "SchemeShard/PartitionsWithDeferredSplitMerge") > 0;
        }

        // Release the slot: the held split completes (2 -> 3 partitions).
        splitBlocker.Stop().Unblock();

        // Baseline arrival-order behavior preserved: the table still grows past the
        // pre-block partition count (the held split completes, 2 -> 3 partitions).
        bool reachedLimit = false;
        for (ui32 i = 0; i < 90 && !reachedLimit; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            reachedLimit = PartitionCountOf(runtime, "/MyRoot/Table") >= 3;
        }

        UNIT_ASSERT_C(reachedLimit, "table did not split to its partition limit with the scheduler off");
        UNIT_ASSERT_C(deferredSeen, "memory did not record a deferral with the scheduler off");
    }

    // Churn: a high-demand table (4 hot shards) competes with a quiet one (1 hot shard) for a single
    // split slot. With the fair scheduler on, round-robin still services the quiet table -- it splits.
    Y_UNIT_TEST(Fairness_NoStarvationUnderChurn) {
        TTestBasicRuntime runtime;
        auto env = SetupHistoryEnv(runtime, /* enableHistory */ true, /* enableFairScheduler */ true,
            /* splitInFlightLimit */ 1);

        ui64 txId = 100;
        // Churny table: starts with 4 partitions, all hot -> constant split demand.
        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Churny"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 4
                PartitionConfig {
                    PartitioningPolicy {
                        MaxPartitionsCount: 20
                        SplitByLoadSettings: { Enabled: true CpuPercentageThreshold: 1 }
                    }
                }
            )"));
        env.TestWaitNotification(runtime, txId);
        // Quiet table: one partition, equally hot.
        TestCreateTable(runtime, ++txId, "/MyRoot", HotTableScheme("Quiet", /* maxPartitions */ 3, /* cpu */ 1));
        env.TestWaitNotification(runtime, txId);

        auto simChurny = MakeHotSimulator(runtime, "/MyRoot/Churny");
        auto simQuiet = MakeHotSimulator(runtime, "/MyRoot/Quiet");
        auto obsChurny = runtime.AddObserver([&simChurny](IEventHandle::TPtr& ev) { simChurny->ChangeEvent(ev); });
        auto obsQuiet = runtime.AddObserver([&simQuiet](IEventHandle::TPtr& ev) { simQuiet->ChangeEvent(ev); });

        // The quiet table must not be starved by the churny one's constant demand.
        bool quietSplit = false;
        for (ui32 i = 0; i < 120 && !quietSplit; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            quietSplit = PartitionCountOf(runtime, "/MyRoot/Quiet") > 1;
        }

        UNIT_ASSERT_C(quietSplit, "quiet table was starved by the churny table");
    }

    // Cross-direction fairness: a merge wave (a table with many tiny shards merging down to 1)
    // races a hot single-shard table's split demand for the one shared split/merge in-flight
    // slot. Merges are inline + decisive in the revisit wave, while splits only re-request
    // stats and claim the slot later, so a merge cascade can in principle consume every freed
    // slot before the split's stats round-trip wins one (the documented slot-level race, see
    // plans/split_merge_memory_review.md, "Recommendation 4 (3a): proposed design").
    // Empirically (first run of this test) the starvation does NOT reproduce here: the
    // revisit wave round-robins tables, the split demand is recorded before the merge wave
    // exists, and the split's stats round-trip wins a slot while the merge backlog is still
    // draining. The test therefore asserts the interleaving property as a regression guard
    // for the wave-level cross-direction fairness.
    Y_UNIT_TEST(Fairness_SplitNotStarvedByMergeWave) {
        TTestBasicRuntime runtime;
        auto env = SetupHistoryEnv(runtime, /* enableHistory */ true, /* enableFairScheduler */ true,
            /* splitInFlightLimit */ 1);

        ui64 txId = 100;
        // Split-demand table: one hot shard wanting a split, competing for the same single slot.
        TestCreateTable(runtime, ++txId, "/MyRoot", HotTableScheme("SplitHot", /* maxPartitions */ 3, /* cpu */ 1));
        env.TestWaitNotification(runtime, txId);

        auto simSplit = MakeHotSimulator(runtime, "/MyRoot/SplitHot");
        auto obsSplit = runtime.AddObserver([&simSplit](IEventHandle::TPtr& ev) { simSplit->ChangeEvent(ev); });

        // Start split demand FIRST and wait until it is recorded -- guarantees the merge
        // backlog exists while split demand is already queued (no vacuous pass).
        bool splitDemandSeen = false;
        for (ui32 i = 0; i < 60 && !splitDemandSeen; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            splitDemandSeen = GetCumulativeCounter(runtime, "SchemeShard/SplitDemandDetected") > 0;
        }
        UNIT_ASSERT_C(splitDemandSeen, "no split demand was detected on SplitHot");

        // Merge-wave table: 8 tiny uniform shards merging down to MinPartitionsCount=1.
        // Merge-by-size requires an explicit SizeToSplit and MinPartitionsCount in the policy
        // (IsMergeBySizeEnabled), and each shard must own at least one data part
        // (TryAddShardToMerge rejects shards with empty PartOwners) -- hence one tiny row
        // per shard, far below the split threshold.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "MergeWave"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 8
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        SizeToSplit: 1000000000
                    }
                }
            )");
        env.TestWaitNotification(runtime, txId);

        // Write one tiny row into each of the 8 shards (key ranges: [i*125000, (i+1)*125000)).
        {
            auto desc = DescribePrivatePath(runtime, "/MyRoot/MergeWave", true, true);
            const auto& parts = desc.GetPathDescription().GetTablePartitions();
            for (int i = 0; i < parts.size(); ++i) {
                TString writeQuery = Sprintf(R"(
                    (
                        (let key '( '('key (Uint64 '%lu)) ) )
                        (let value '('('value (Uint64 '1)) ) )
                        (return (AsList (UpdateRow '__user__MergeWave key value) ))
                    )
                )", i * 125000 + 1);
                NKikimrMiniKQL::TResult result;
                TString err;
                NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, parts.Get(i).GetDatashardId(), writeQuery, result, err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
            }
        }

        // Cold simulator for MergeWave: no CPU patches (empty maps), only drives the stats
        // pipeline for this table (same construction as MakeHotSimulator minus the patches).
        auto desc = DescribePrivatePath(runtime, "/MyRoot/MergeWave", true, true);
        const ui64 mwLocalId = desc.GetPathDescription().GetSelf().GetPathId();
        const ui64 mwOwnerId = desc.GetPathDescription().GetSelf().GetSchemeshardId();
        const ui64 mwDs0 = desc.GetPathDescription().GetTablePartitions(0).GetDatashardId();
        auto simMerge = MakeHolder<TLoadAndSplitSimulator>(
            mwLocalId, mwOwnerId, mwDs0,
            false /* shouldSendReadRequests */,
            ESendDuplicateTableStatsStrategy::None,
            std::map<ui32, i32>{},  // no CPU patch (periodic stats): cold shards
            std::map<ui32, i32>{},  // no CPU patch (get-stats result): cold shards
            runtime);
        auto obsMerge = runtime.AddObserver([&simMerge](IEventHandle::TPtr& ev) { simMerge->ChangeEvent(ev); });

        // Concurrent phase: poll each second; the primary property is checked per iteration.
        bool mergeDemandSeen = false;
        bool deferralSeen = false;
        bool splitFiredWhileDraining = false;
        bool mergeDrained = false;
        for (ui32 i = 0; i < 120 && !mergeDrained; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            mergeDemandSeen = mergeDemandSeen
                || GetCumulativeCounter(runtime, "SchemeShard/MergeDemandDetected") > 0;
            deferralSeen = deferralSeen
                || GetCumulativeCounter(runtime, "SchemeShard/SplitMergeDeferrals") > 0;
            const ui32 splitCount = PartitionCountOf(runtime, "/MyRoot/SplitHot");
            const ui32 mergeCount = PartitionCountOf(runtime, "/MyRoot/MergeWave");
            if (splitCount > 1 && mergeCount > 1) {
                splitFiredWhileDraining = true;  // split landed while the merge backlog remains
            }
            mergeDrained = mergeCount <= 1;
        }

        // Guards against a vacuous pass: demand was real on both sides and a deferral occurred.
        UNIT_ASSERT_C(mergeDemandSeen, "no merge demand was detected on MergeWave");
        UNIT_ASSERT_C(deferralSeen, "no split/merge deferral was recorded under slot contention");

        // Primary (the cross-direction-fairness property): the split fired while the merge
        // wave was still draining. The revisit wave round-robins tables and the split demand
        // is recorded before the merge wave exists, so the split's stats round-trip wins a
        // slot before the merge backlog drains (see plans/split_merge_memory_review.md,
        // "Recommendation 4 (3a): proposed design" for the direction-aware revisit wave +
        // soft split reservation that would restore fairness if this ever regresses).
        UNIT_ASSERT_C(splitFiredWhileDraining,
            "SplitHot was starved by the merge wave: the split did not fire while MergeWave was still draining");

        // The fix must not simply block merges: the wave still completes within the bounded wait.
        UNIT_ASSERT_C(mergeDrained, "MergeWave did not drain to 1 partition");
    }
}
