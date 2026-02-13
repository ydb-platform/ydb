#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/util_fmt_cell.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

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
}

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

    Y_UNIT_TEST(Split10Shards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(1);

        TTestEnv env(runtime, opts);

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
        // (see ydb/core/tx/schemeshard/schemeshard__table_stats_histogram.cpp, DoFindSplitKey() and ChooseSplitKeyByKeySample())
        HistogramAddBucket(KeyAccessHistogramPatch, 999998, 1000);
        HistogramAddBucket(KeyAccessHistogramPatch, 999999, 1000);
        HistogramAddBucket(KeyAccessHistogramPatch, 1000000, 1000);

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
                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(0)->SetKey(ToSerialized(splitPoint - 1));
                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(1)->SetKey(ToSerialized(splitPoint));
                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(2)->SetKey(ToSerialized(splitPoint + 1));

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvGetTableStatsResult, from datashard " << msg->Record.GetDatashardId()
                        << ", from followerId " << msg->Record.GetFollowerId()
                        << ", patch KeyAccessSample: split point " << splitPoint
                        << " (start=" << start
                        << ", end=" << end
                        << ")"
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

TTestEnv SetupEnv(TTestBasicRuntime &runtime) {
    TTestEnvOptions opts;
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
                "the table to be slitted",
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

    Y_UNIT_TEST(TableSplitsUpToMaxPartitionsCount) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

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
    Y_UNIT_TEST(TableSplitsByFollowerLoad) {
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
            "the table to be slitted",
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
