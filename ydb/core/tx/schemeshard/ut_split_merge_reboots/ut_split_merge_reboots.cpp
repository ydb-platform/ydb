#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/library/yql/minikql/mkql_node.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardSplitTestReboots) {

    void CheckTablePartitions(TTestActorRuntime& runtime, const TString& tablePath, const TVector<TString>& partitionPrefixes) {
        TString tableDescr = TestDescribe(runtime, tablePath);
        NKikimrScheme::TEvDescribeSchemeResult pbDescr;
        UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(tableDescr, &pbDescr));
        UNIT_ASSERT_VALUES_EQUAL(partitionPrefixes.size(), pbDescr.GetPathDescription().TablePartitionsSize());
        TVector<ui64> datashards;
        for (size_t i = 0; i < partitionPrefixes.size(); ++i) {
            datashards.push_back(pbDescr.GetPathDescription().GetTablePartitions(i).GetDatashardId());
            UNIT_ASSERT_STRING_CONTAINS(pbDescr.GetPathDescription().GetTablePartitions(i).GetEndOfRangeKeyPrefix(),
                                        partitionPrefixes[i]);
        }
        for (ui64 tabletId : datashards) {
            UNIT_ASSERT_VALUES_EQUAL(GetDatashardState(runtime, tabletId), (ui64)NKikimrTxDataShard::Ready);
        }
    }

    Y_UNIT_TEST(ReTryMerge) { //+
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
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", ""})});
            }

            auto defObserver = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                return TTestActorRuntime::DefaultObserverFunc(ev);
            };

            auto prevObserver = runtime.SetObserverFunc(defObserver);

            TVector<THolder<IEventHandle>> suppressed;
            auto suppressEvent  = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                if (ev->GetTypeRewrite() == TEvDataShard::TEvStateChanged::EventType) {
                    auto *msg = ev->Get<TEvDataShard::TEvStateChanged>();
                    auto state = msg->Record.GetState();
                    Cerr << "TEvStateChanged has happened " << state << Endl;

                    if (state == NDataShard::TShardState::Offline) {
                        Cerr << "suppressEvent has happened" << Endl;
                        suppressed.push_back(std::move(ev));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return prevObserver(ev);
            };

            runtime.SetObserverFunc(suppressEvent);

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )",
                           {NKikimrScheme::StatusInvalidParameter});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.SetObserverFunc(prevObserver);
            for (auto& ev : suppressed) {
                runtime.Send(ev.Release(), 0, /* via actor system */ true);
            }
            suppressed.clear();

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::CheckColumns("Table", {"key1", "key2", "Value", "add_1", "add_2"}, {}, {"key1", "key2"}),
                                    NLs::PartitionKeys({""}),
                                    ShardsIsReady(runtime)});
            }

            {
                TInactiveZone inactive(activeZone);
                // make sure that merge has finished
                TestCopyTable(runtime, ++t.TxId, "/MyRoot", "Copy", "/MyRoot/Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        }, true);
    }

    Y_UNIT_TEST(MergeMergeAlterParallel) { //+
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
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                    {NLs::PartitionKeys({"A", "B", "C", ""}),
                                     ShardsIsReady(runtime)});
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )");
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409548
                            SourceTabletId: 72075186233409549
                            )");

            AsyncAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "add_1"  Type: "Uint32"}
                    Columns { Name: "add_2"  Type: "Uint64"}
                )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId -2, t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+4));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::CheckColumns("Table", {"key1", "key2", "Value", "add_1", "add_2"}, {}, {"key1", "key2"}),
                                    NLs::PartitionKeys({"B", ""})});
            }
        }, true);
    }

    auto MergeMergeCopyParallelScenario = [] (TTestWithReboots& t) {
        return [&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                    Name: "Table"
                                    Columns { Name: "key1"       Type: "Utf8"}
                                    Columns { Name: "key2"       Type: "Uint32"}
                                    Columns { Name: "Value"      Type: "Utf8"}
                                    KeyColumnNames: ["key1", "key2"]
                                    SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                    SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } }}}
                                    SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                    PartitionConfig {
                                        PartitioningPolicy {
                                            MinPartitionsCount: 0
                                        }
                                    }
                                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", "B", "C", ""})});
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                SourceTabletId: 72075186233409547
                                )");
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409548
                                SourceTabletId: 72075186233409549
                                )");
            t.TestEnv->ReliablePropose(runtime, CopyTableRequest(++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});

            AsyncCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId -2, t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::CheckColumns("Table", {"key1", "key2", "Value", "add_1", "add_2"}, {}, {"key1", "key2"}),
                                    NLs::PartitionKeys({"B", ""})});
            }
        };
    };

    Y_UNIT_TEST(MergeMergeCopyParallelReboots) { //+
        TTestWithReboots t;
        t.RunWithTabletReboots(MergeMergeCopyParallelScenario(t));
    }

    Y_UNIT_TEST(MergeMergeCopyParallelPipeResets) { //+
        TTestWithReboots t;
        t.RunWithPipeResets(MergeMergeCopyParallelScenario(t));
    }

    Y_UNIT_TEST(MergeMergeDropParallel) { //+
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
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", "B", "C", ""})});
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )");
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409548
                            SourceTabletId: 72075186233409549
                            )");
            AsyncDropTable(runtime, ++t.TxId, "/MyRoot", "Table");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId -2, t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+6));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        }, true);
    }

    Y_UNIT_TEST(SplitSameShardTwice) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                    Name: "Table"
                                    Columns { Name: "key1"       Type: "Utf8"}
                                    Columns { Name: "key2"       Type: "Uint32"}
                                    Columns { Name: "Value"      Type: "Utf8"}
                                    KeyColumnNames: ["key1", "key2"]
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Text: "Jack" } }
                                        }
                                    })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", ""})});
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409547
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Marla" } }
                                    }
                                }
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Robert" } }
                                    }
                                })");

            // Second split request must be rejected
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409547
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Marla Singer" } }
                                    }
                                }
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Robert Paulson" } }
                                    }
                                })");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            t.TestEnv->TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets+1); //delete src

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", "Marla", "Robert", ""})});
            }
        });
    }


    Y_UNIT_TEST(SplitTableWithReboots) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                    Name: "Table"
                                    Columns { Name: "key1"       Type: "Utf8"}
                                    Columns { Name: "key2"       Type: "Uint32"}
                                    Columns { Name: "Value"      Type: "Utf8"}
                                    KeyColumnNames: ["key1", "key2"]
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Text: "Jack" } }
                                        }
                                    })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", ""})});

                TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                    SourceTabletId: 72075186233409547
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Text: "Marla" } }
                                        }
                                    }
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Text: "Robert" } }
                                        }
                                    })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", "Marla", "Robert", ""})});
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409549
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Marla Singer" } }
                                        Tuple { Optional { Uint32: 20 } }
                                    }
                                })");
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409550
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "Robert Paulson" } }
                                    Tuple { Optional { Uint32: 300 } }
                                }
                            }
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "Tyler Durden" } }
                                }
                            })");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, {72075186233409549, 72075186233409550}); //delete src

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", "Marla", "Marla Singer", "Robert", "Robert Paulson", "Tyler Durden", ""})});
            }
        });
    }

    Y_UNIT_TEST(SplitTableOneToOneWithReboots) {
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                    Name: "Table"
                                    Columns { Name: "key1"       Type: "Utf8"}
                                    Columns { Name: "key2"       Type: "Uint32"}
                                    Columns { Name: "Value"      Type: "Utf8"}
                                    KeyColumnNames: ["key1", "key2"]
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Text: "Jack" } }
                                        }
                                    })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", ""})});
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                AllowOneToOneSplitMerge: true
                                )");
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409547
                                AllowOneToOneSplitMerge: true
                                )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, {72075186233409546, 72075186233409547}); //delete src

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", ""})});
            }
        });
    }

    Y_UNIT_TEST(MergeTableWithReboots) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "Jack" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", ""})});
            }

            // Merge 2 partitions into 1
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table",
                            R"(
                                SourceTabletId: 72075186233409546
                                SourceTabletId: 72075186233409547
                            )");

            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {72075186233409546, 72075186233409547}); //delete src

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({""})});
            }
        });
    }

    Y_UNIT_TEST(MergeTableWithRebootsAndDropAfter) { //+
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"B", "C", ""})});

                {
                    // Write some data to the user table
                    auto fnWriteRow = [&] (ui64 tabletId, TString key) {
                        TString writeQuery = Sprintf( R"(
                            (
                                (let key '( '('key1 (Utf8 '%s)) '('key2 (Uint32 '0)) ) )
                                (let value '('('Value (Utf8 '281474980010683)) ) )
                                (return (AsList (UpdateRow '__user__Table key value) ))
                            )
                        )", key.c_str());
                        NKikimrMiniKQL::TResult result;
                        TString err;
                        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                        UNIT_ASSERT_VALUES_EQUAL(err, "");
                        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
                    };

                    //fnWriteRow(TTestTxConfig::FakeHiveTablets, "AAA"); //we need to make a mix of shards which have shared blobs and don't
                    fnWriteRow(TTestTxConfig::FakeHiveTablets + 1, "BBB");
                    fnWriteRow(TTestTxConfig::FakeHiveTablets + 2, "CCC");
                }


                TestCopyTable(runtime, ++t.TxId, "/MyRoot", "Copy", "/MyRoot/Table");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"B", "C", ""})});
            }

            // Merge 2 partitions into 1
            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table",
                           R"(
                                SourceTabletId: 72075186233409546
                                SourceTabletId: 72075186233409547
                            )");

            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {NLs::PartitionKeys({"C", ""})});

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Copy");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+10));
            }
        }, true);
    }

    Y_UNIT_TEST(MergeSplitParallel) { //+
        TTestWithReboots t(true);
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
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "B" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", "B", ""})});

                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+1, false);
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                SourceTabletId: 72075186233409547
                            )");
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409548
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "C" } }
                                }
                            })");

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+3));
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"B", "C", ""})});
            }
        }, true);
    }

    Y_UNIT_TEST(SplitAlterParallel) { //+
        TTestWithReboots t(true);
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

                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+1, false);
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409547
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "D" } }
                                }
                            })");

            AsyncAlterTable(runtime, ++t.TxId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "add_1"  Type: "Uint32"}
                    Columns { Name: "add_2"  Type: "Uint64"}
                )");

            t.TestEnv->TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets+1);
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::CheckColumns("Table", {"key1", "key2", "Value", "add_1", "add_2"}, {}, {"key1", "key2"}),
                                    NLs::PartitionKeys({"A", "D", ""})});
            }
        });
    }

    Y_UNIT_TEST(MergeAlterSplitParallel) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                                 {NLs::PathVersionEqual(3)});
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )");
            t.TestEnv->ReliablePropose(runtime, AlterTableRequest(++t.TxId, "/MyRoot", R"(
                                                        Name: "Table"
                                                        Columns { Name: "add_1"  Type: "Uint32"}
                                                        Columns { Name: "add_2"  Type: "Uint64"}
                                                        )", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                             {NLs::PathVersionEqual(5)});
            }

            t.TestEnv->ReliablePropose(runtime, SplitTableRequest(++t.TxId, "/MyRoot/Table", R"(
                                                                SourceTabletId: 72075186233409548
                                                                SplitBoundary {
                                                                    KeyPrefix {
                                                                        Tuple { Optional { Text: "B" } }
                                                                    }
                                                                })", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionCount(2),
                                    NLs::CheckColumns("Table", {"key1", "key2", "Value", "add_1", "add_2"}, {}, {"key1", "key2"}),
                                    NLs::PartitionKeys({"B", ""})});
            }
        }, true);
    }

    Y_UNIT_TEST(MergeAlterAlterParallel) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                                 {NLs::PathVersionEqual(3),
                                                  NLs::PartitionKeys({"A", ""})});
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )");
            t.TestEnv->ReliablePropose(runtime, AlterTableRequest(++t.TxId, "/MyRoot",
                                                        R"(Name: "Table"
                                                            Columns { Name: "add_1"  Type: "Uint32"}
                                                            Columns { Name: "add_2"  Type: "Uint64"}
                                                        )"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                                 {NLs::PathVersionEqual(5)});
            }

            t.TestEnv->ReliablePropose(runtime, AlterTableRequest(++t.TxId, "/MyRoot", R"(
                                                            Name: "Table"
                                                            Columns { Name: "add_3"  Type: "String"}
                                                    )", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::CheckColumns("Table", {"key1", "key2", "Value", "add_1", "add_2", "add_3"}, {}, {"key1", "key2"}),
                                    NLs::PartitionKeys({""})});
            }
        }, true);
    }

    Y_UNIT_TEST(SplitDropParallel) { //+
        TTestWithReboots t(true);
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

                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+1, false);
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table",
                            R"(
                            SourceTabletId: 72075186233409547
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "D" } }
                                }
                            }
                            )");

            AsyncDropTable(runtime, ++t.TxId, "/MyRoot", "Table");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+4));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        }, true);
    }

    Y_UNIT_TEST(MergeCopyParallelWithChannelsBindings) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, t.TxId, "/MyRoot/DirA", //1001
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\""
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-1\""
                                    "  Kind: \"storage-pool-number-1\""
                                    "}"
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-2\""
                                    "  Kind: \"storage-pool-number-2\""
                                    "}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0/Table", true),
                                                 {NLs::PathVersionEqual(3),
                                                  NLs::PartitionKeys({"A", ""})});

                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+2, false);
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/Table",
                           R"(
                            SourceTabletId: 72075186233409548
                            SourceTabletId: 72075186233409549
                            )");

            t.TestEnv->ReliablePropose(runtime, CopyTableRequest(++t.TxId, "/MyRoot/DirA/USER_0", "TableCopy", "/MyRoot/DirA/USER_0/Table",
                                                                 {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0/Table", true),
                                                                 {NLs::PartitionKeys({""})});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0/TableCopy", true),
                                                                 {NLs::PartitionKeys({""})});
            }
        }, false);
    }

    Y_UNIT_TEST(ForceDropAndCopyInParallelAllPathsAreLocked) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets, false);
            }

            AsyncCopyTable(runtime, ++t.TxId, "/MyRoot/DirA", "TableCopy", "/MyRoot/DirA/Table");

            AsyncForceDropUnsafe(runtime, ++t.TxId, 2);

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/DirA/Table", R"(
                            SourceTabletId: 72075186233409546
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "AA" } }
                                }
                            }
                            )");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

            t.TestEnv->TestWaitTabletDeletion(runtime,
                                              {TTestTxConfig::FakeHiveTablets     //Ds
                                                  , TTestTxConfig::FakeHiveTablets+1 //CopyDS
                                                  , TTestTxConfig::FakeHiveTablets+2 //FirstSplit
                                                  , TTestTxConfig::FakeHiveTablets+3 //FirstSplit
                                              });

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::NoChildren});
            }
        });
    }

    Y_UNIT_TEST(MergeCopyParallelAndSplitCopyAfter) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets, false);
                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+1, false);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", ""})});

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot", true),
                                                 {NLs::Finished,
                                                  NLs::ShardsInsideDomain(2)});
            }


            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table",
                           R"(
                            SourceTabletId: 72075186233409546
                            SourceTabletId: 72075186233409547
                            )");

            t.TestEnv->ReliablePropose(runtime, CopyTableRequest(++t.TxId, "/MyRoot", "TableCopy", "/MyRoot/Table", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});


            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true), {NLs::PartitionKeys({""})});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/TableCopy", true), {NLs::PartitionKeys({""})});

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                                 {NLs::Finished,
                                                  NLs::ShardsInsideDomainOneOf({2, 3, 4})});
            }

            t.TestEnv->ReliablePropose(runtime, SplitTableRequest(++t.TxId, "/MyRoot/TableCopy", R"(
                                                            SourceTabletId: 72075186233409549
                                                            SplitBoundary {
                                                                KeyPrefix {
                                                                    Tuple { Optional { Text: "E" } }
                                                                }
                                                            }
                                                            )", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});

            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true), {NLs::PartitionKeys({""})});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/TableCopy", true), {NLs::PartitionKeys({"E", ""})});

                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::Finished,
                                    NLs::ShardsInsideDomainOneOf({2, 3, 4, 5})});
            }
        }, true);
    }


    Y_UNIT_TEST(SplitThenMerge) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                SetAllowLogBatching(runtime, t.SchemeShardTabletId, false);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "AAAA" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "BBBB" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "CCCC" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "FFFF" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "GGGG" } }}}
                                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"AAAA", "BBBB", "CCCC", "FFFF", "GGGG", ""})});

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                                 {NLs::PathVersionEqual(3)});
            }

            t.TestEnv->ReliablePropose(runtime, SplitTableRequest(++t.TxId, "/MyRoot/Table", R"(
                                            SourceTabletId: 72075186233409549
                                            SplitBoundary {
                                                KeyPrefix {
                                                    Tuple { Optional { Text: "DDDD" } }
                                                }
                                            }
                                            )", {pathVersion}), // 72075186233409549 ->  72075186233409552, 72075186233409553
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                                 {NLs::PathVersionEqual(4)});
            }

            t.TestEnv->ReliablePropose(runtime, SplitTableRequest(++t.TxId, "/MyRoot/Table", R"(
                                            SourceTabletId: 72075186233409548
                                            SourceTabletId: 72075186233409552
                                            )", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"AAAA", "BBBB", "DDDD", "FFFF", "GGGG", ""})});

            }
        }, true);
    }

    Y_UNIT_TEST(MergeThenSplit) {
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                SetAllowLogBatching(runtime, t.SchemeShardTabletId, false);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "AAAA" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "CCCC" } }}}
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "DDDD" } }}}
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 0
                                    }
                                }
                                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                                 {NLs::PathVersionEqual(3),
                                                  NLs::PartitionKeys({"AAAA", "CCCC", "DDDD", ""})});
            }

            t.TestEnv->ReliablePropose(runtime, SplitTableRequest(++t.TxId, "/MyRoot/Table", R"(
                                                    SourceTabletId: 72075186233409547
                                                    SourceTabletId: 72075186233409548
                                            )", {pathVersion}), // -> 72075186233409550
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                                 {NLs::PathVersionEqual(4),
                                                  NLs::PartitionCount(3)});
            }

            t.TestEnv->ReliablePropose(runtime, SplitTableRequest(++t.TxId, "/MyRoot/Table", R"(
                                                SourceTabletId: 72075186233409550
                                                SplitBoundary {
                                                    KeyPrefix {
                                                        Tuple { Optional { Text: "BBBBB" } }
                                                    }
                                                }
                                            )", {pathVersion}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"AAAA", "BBBB", "DDDD", ""})});
            }
        }, true);
    }

    Y_UNIT_TEST(SplitWithTxInFlightWithReboots) { //+
        TTestWithReboots t(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "Jack" } }}}
                                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", ""})});
            }


            ui64 dataTxId = 20000;

            // Propose cross-shard write Tx1
            ++dataTxId;
            TFakeDataReq req1(runtime, dataTxId, "/MyRoot/Table",
                                R"(
                                (
                                    (let row1 '('('key1 (Utf8 'AAA)) '('key2 (Uint32 '111))))
                                    (let row2 '('('key1 (Utf8 'KKK)) '('key2 (Uint32 '222))))
                                    (let myUpd '())
                                    (let ret (AsList
                                        (UpdateRow '/MyRoot/Table row1 myUpd)
                                        (UpdateRow '/MyRoot/Table row2 myUpd)
                                    ))
                                    (return ret)
                                )
                                )");
            IEngineFlat::EStatus status1 = req1.Propose(false, activeZone);
            UNIT_ASSERT_VALUES_EQUAL_C(status1, IEngineFlat::EStatus::Unknown, "This Tx should be accepted and wait for Plan");
            UNIT_ASSERT(req1.GetErrors().empty());

            // Split partition #2 into 2
            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table",
                            R"(
                                SourceTabletId: 72075186233409547
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Marla" } }
                                    }
                                }
                            )");

            // Wait for split to reach src DS
            int retries = 3;
            while (retries--) {
                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back(TEvDataShard::EvSplit);
                    runtime.DispatchEvents(opts);
                }

                ++dataTxId;
                TFakeDataReq req2(runtime, dataTxId, "/MyRoot/Table",
                                R"(
                                (
                                    (let row1 '('('key1 (Utf8 'AAA)) '('key2 (Uint32 '333))))
                                    (let row2 '('('key1 (Utf8 'KKK)) '('key2 (Uint32 '444))))
                                    (let myUpd '())
                                    (let ret (AsList
                                        (UpdateRow '/MyRoot/Table row1 myUpd)
                                        (UpdateRow '/MyRoot/Table row2 myUpd)
                                    ))
                                    (return ret)
                                )
                                )");

                IEngineFlat::EStatus status2 = req2.Propose(false, activeZone);

                if (status2 == IEngineFlat::EStatus::Unknown) {
                    req2.Plan(t.CoordinatorTabletId);
                    continue;
                }

                UNIT_ASSERT_VALUES_EQUAL_C(status2, IEngineFlat::EStatus::Error, "Write Tx should be rejected while split is pending");
                break;
            }
            UNIT_ASSERT_C(retries >= 0, "New Tx wasn't rejected by splitting datashard");

            // Plan Tx1
            {
                TInactiveZone inactive(activeZone);
                req1.Plan(t.CoordinatorTabletId);
                // Split should now proceed
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"Jack", "Marla", ""})});
            }
        });
    }

}
