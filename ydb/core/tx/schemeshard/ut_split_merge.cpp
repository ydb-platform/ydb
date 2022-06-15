#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/library/yql/minikql/mkql_node.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardSplitTest) {
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
                            SourceTabletId: 9437194
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "A" } }
                                }
                            })");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                        SourceTabletId: 9437194
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

    Y_UNIT_TEST(Split10Shards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(1);
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

        while (true) {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvGetTableStatsResult::EventType);

            WaitForSuppressed(runtime, suppressed, 1, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();

            bool itIsEnough = false;

            NLs::TCheckFunc checkPartitionCount = [&] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                if (record.GetPathDescription().TablePartitionsSize() >= 10) {
                    itIsEnough = true;
                }
            };

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {checkPartitionCount});

            if (itIsEnough) {
                return;
            }
        }
    }

    Y_UNIT_TEST(Merge1KShards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DisableStatsBatching(true);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;
        runtime.SetDispatchedEventsLimit(10'000'000);

        NDataShard::gDbStatsReportInterval = TDuration::MilliSeconds(1);

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
        // test requeres more txids than cached at start
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

}
