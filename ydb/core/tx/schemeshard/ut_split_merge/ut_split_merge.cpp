#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/util_fmt_cell.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

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

    Y_UNIT_TEST(AutoMergeInOne) {
        TTestWithReboots t;
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
enum class SendDuplicateTableStatsStrategy {
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
    std::map<ui32, NKikimrTabletBase::TMetrics> MetricsPatchByFollowerId;
    NKikimrTableStats::THistogram KeyAccessHistogramPatch;
    ui64 TableLocalPathId;
    ui64 TableOwnerId;
    bool ShouldSendReadRequests;
    SendDuplicateTableStatsStrategy SendDuplicateTableStats;
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
        SendDuplicateTableStatsStrategy sendDuplicateTableStats,
        const std::map<ui32, ui32>& targetCpuLoadByFolowerId,
        TTestActorRuntime& testRuntime
    ) : TableLocalPathId(tableLocalPathId)
        , TableOwnerId(tableOwnerId)
        , ShouldSendReadRequests(shouldSendReadRequests)
        , SendDuplicateTableStats(sendDuplicateTableStats)
        , TestRuntime(&testRuntime)
    {
        for (const auto& [followerId, targetCpuLoad] : targetCpuLoadByFolowerId) {
            MetricsPatchByFollowerId[followerId].SetCPU(CpuLoadMicroseconds(targetCpuLoad));
        }

        //NOTE: histogram must have at least 3 buckets with different keys to be able to produce split key
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

        for (const auto& [followerId, targetCpuLoad] : targetCpuLoadByFolowerId) {
            Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                << ", target CPU load for followerId " << followerId
                << " is " << targetCpuLoad
                << "%"
                << Endl;
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

                    const auto itTargetCpuForFollower = MetricsPatchByFollowerId.find(msg->Record.GetFollowerId());

                    if (itTargetCpuForFollower != MetricsPatchByFollowerId.end()) {
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

                    // Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                    //     << ", intercept EvGetTableStats" << Endl;

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

                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->CopyFrom(KeyAccessHistogramPatch);

                    auto [start, end] = DatashardsKeyRanges[msg->Record.GetDatashardId()];
                    //NOTE: zero end means infinity -- this is a final shard
                    if (end == 0) {
                        end = 1000000;
                    }
                    const ui64 splitPoint = (end + start) / 2;
                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(0)->SetKey(ToSerialized(splitPoint - 1));
                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(1)->SetKey(ToSerialized(splitPoint));
                    msg->Record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(2)->SetKey(ToSerialized(splitPoint + 1));

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvGetTableStatsResult, from datashard " << msg->Record.GetDatashardId()
                        << ", patch KeyAccessSample: split point " << splitPoint
                        << " (start=" << start
                        << ", end=" << end
                        << ")"
                        << Endl;

                    if (SendDuplicateTableStats != SendDuplicateTableStatsStrategy::None) {
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
                            case SendDuplicateTableStatsStrategy::None:
                                break;

                            case SendDuplicateTableStatsStrategy::Immediately:
                                {
                                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                                        << ", sending a duplicate EvGetTableStatsResult "
                                        << "from datashard " << msg->Record.GetDatashardId()
                                        << Endl;

                                    TestRuntime->SendAsync(msg_copy_handle.release());
                                }
                                break;

                            case SendDuplicateTableStatsStrategy::AfterSplitAck:
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
                    // (split will have single source range)
                    for (const auto& i : msg->Record.GetSplitDescription().GetSourceRanges()) {
                        DatashardsKeyRanges.erase(i.GetTabletID());
                    }
                    // add info for destination shards
                    for (const auto& i : msg->Record.GetSplitDescription().GetDestinationRanges()) {
                        auto& [start, end] = DatashardsKeyRanges[i.GetTabletID()];
                        start = FromSerialized(i.GetKeyRangeBegin());
                        //NOTE: empty KeyRangeEnd means infinity
                        const auto keyRangeEnd = i.GetKeyRangeEnd();
                        end = (keyRangeEnd.size() > 0) ? FromSerialized(keyRangeEnd) : 0;

                        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                            << ", datashard " << msg->Record.GetSplitDescription().GetSourceRanges(0).GetTabletID()
                            << " split into datashard " << i.GetTabletID()
                            << " (start="  << start
                            << ", end=" << ((end != 0) ? end : 1000000)
                            << ")"
                            << Endl;
                    }

                    ++SplitReqCount;
                }
                break;
            case TEvDataShard::EvSplitAck:
                // count splits
                {
                    const auto msg = ev->Get<TEvDataShard::TEvSplitAck>();

                    const auto now = TInstant::Now();
                    const auto elapsed = now - LastSplitAckTime;
                    LastSplitAckTime = now;

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvSplitAck, from datashard " << msg->Record.GetTabletId()
                        << ", " << elapsed << " since last split ack"
                        << Endl;

                    ++SplitAckCount;

                    // Check, if need to send a duplicate EvGetTableStatsResult to the same shard
                    if (SendDuplicateTableStats == SendDuplicateTableStatsStrategy::AfterSplitAck) {
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
     * @param[in] targetCpuLoadByFolowerId The map from the follower ID (0 == leader)
     *                                     to the corresponding induced CPU load (as percent)
     * @param[in] shouldSendReadRequests If true, send EvRead requests to all followers
     * @param[in] sendDuplicateTableStats Determines if/when to send duplicate EvGetTableStatsResult messages
     * @param[in] maxTestDuration The maximum duration of the test
     */
    void SplitByLoad(
        TTestActorRuntime& runtime,
        const TString& tablePath,
        const std::map<ui32, ui32>& targetCpuLoadByFolowerId,
        bool shouldSendReadRequests = false,
        SendDuplicateTableStatsStrategy sendDuplicateTableStats = SendDuplicateTableStatsStrategy::None,
        const TDuration& maxTestDuration = TDuration::Max()
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
            targetCpuLoadByFolowerId,
            runtime
        );

        runtime.SetObserverFunc([&simulator](TAutoPtr<IEventHandle>& event) {
            simulator.ChangeEvent(event);
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        {
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&simulator]() {
                auto now = TInstant::Now();
                // Cerr << "TEST SplitByLoad, CustomFinalCondition, SplitAckCount " << simulator.SplitAckCount << ", " << (now - simulator.LastSplitAckTime) << " since last split" << Endl;
                return simulator.SplitAckCount > 0 && (now - simulator.LastSplitAckTime) > TDuration::Seconds(3);
            };
            runtime.DispatchEvents(opts, maxTestDuration);
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
            SendDuplicateTableStatsStrategy::None,
            {{0, targetCpuLoadPercent}}, // Target CPU load for the leader only
            runtime
        );

        runtime.SetObserverFunc([&simulator](TAutoPtr<IEventHandle>& event) {
            simulator.ChangeEvent(event);
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        {
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&simulator]() {
                // Cerr << "TEST SplitByLoad, PeriodicTableStats " << simulator.PeriodicTableStatsCount << ", KeyAccessSampleReq " << simulator.KeyAccessSampleReqCount << Endl;
                return simulator.PeriodicTableStatsCount > 10 && simulator.KeyAccessSampleReqCount == 0;
            };
            runtime.DispatchEvents(opts, TDuration::Seconds(60));
        }
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
            false /* shouldSendReadRequests */,
            SendDuplicateTableStatsStrategy::Immediately // Trigger concurrent split transactions
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
            false /* shouldSendReadRequests */,
            SendDuplicateTableStatsStrategy::AfterSplitAck // Trigger duplicate split transactions
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    /**
     * Verify that a shard is split automatically, when some of the followers
     * become overloaded with requests.
     *
     * @todo Once splitting by the follower load is implemented,
     *       this test should be adjusted to match the splitting logic.
     *       For now this test is essentially reversed - it verifies that
     *       no splitting is triggered, when one of the followers is overloaded.
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
                // No simulated CPU load for the leader, only for the followers
                {1, cpuLoadSimulated},
                {2, cpuLoadSimulated},
                {3, cpuLoadSimulated},
            },
            true /* shouldSendReadRequests */,
            SendDuplicateTableStatsStrategy::None,
            /// @todo Remove the time limit once splitting by the replica load is implemented
            TDuration::Seconds(30) // Only 15 seconds real time
        );

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;

        /// @todo Use expectedPartitionCount instead of 1 once splitting by the follower load is implemented
        TestDescribeResult(tableInfo, {NLs::PartitionCount(1)});
    }
}
