#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/base/path.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TAsyncIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
              Type: EIndexTypeGlobalAsync
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex"),{
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalAsync),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"indexed"}),
        });
    }

    Y_UNIT_TEST(OnlineBuild) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "indexed" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "UserDefinedIndex", NKikimrSchemeOp::EIndexTypeGlobalAsync, {"indexed"}, {}, {}
        });
        env.TestWaitNotification(runtime, txId);
    }

    THolder<TEvDataShard::TEvUploadRowsRequest> MakeUploadRows(ui64 tableId,
            const TVector<ui32>& keyTags, const TVector<ui32>& valueTags,
            TVector<std::pair<TString, TString>>&& serializedRows)
    {
        auto ev = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        ev->Record.SetTableId(tableId);

        auto& scheme = *ev->Record.MutableRowScheme();
        for (ui32 tag : keyTags) {
            scheme.AddKeyColumnIds(tag);
        }
        for (ui32 tag : valueTags) {
            scheme.AddValueColumnIds(tag);
        }

        for (const auto& [k, v] : serializedRows) {
            auto& row = *ev->Record.AddRows();
            row.SetKeyColumns(k);
            row.SetValueColumns(v);
        }

        return ev;
    }

    template <typename C>
    TVector<ui64> MakeTabletIds(const C& partitions) {
        TVector<ui64> tabletIds;
        for (const auto& x : partitions) {
            tabletIds.push_back(x.GetDatashardId());
        }
        return tabletIds;
    }

    TVector<ui64> Prepare(TTestActorRuntime& runtime, const TString& mainTablePath, const TVector<ui32>& recordIds, bool block = false) {
        ui64 mainTableId = 0;
        TVector<ui64> mainTabletIds;
        TVector<std::pair<TString, TString>> rows;

        {
            auto tableDesc = DescribePath(runtime, mainTablePath, true, true);
            mainTableId = tableDesc.GetPathId();
            mainTabletIds = MakeTabletIds(tableDesc.GetPathDescription().GetTablePartitions());
        }

        for (ui32 i : recordIds) {
            auto key = TVector<TCell>{TCell::Make(i)};
            auto value = TVector<TCell>{TCell::Make(i)};
            rows.emplace_back(TSerializedCellVec::Serialize(key), TSerializedCellVec::Serialize(value));
        }

        const auto sender = runtime.AllocateEdgeActor();
        auto ev = MakeUploadRows(mainTableId, {1}, {2}, std::move(rows));
        ForwardToTablet(runtime, mainTabletIds[0], sender, ev.Release());

        if (block) {
            runtime.GrabEdgeEvent<TEvDataShard::TEvUploadRowsResponse>(sender);
        }

        return mainTabletIds;
    }

    struct TTableTraits {
        TString Path;
        TVector<TString> Key;
        TVector<TString> Columns;
        ui32 ExpectedRecords;
    };

    bool CheckWrittenToIndex(TTestActorRuntime& runtime, const TTableTraits& mainTable, const TTableTraits& indexTable) {
        auto mainTableRows = CountRows(runtime, mainTable.Path);
        bool writtenToMainTable = (mainTable.ExpectedRecords == mainTableRows);

        if (writtenToMainTable) {
            int i = 0;
            while (++i < 10) {
                runtime.SimulateSleep(TDuration::Seconds(1));
                if (indexTable.ExpectedRecords == CountRows(runtime, indexTable.Path)) {
                    break;
                }
            }

            UNIT_ASSERT(i < 10);
        }

        return writtenToMainTable;
    }

    enum ESplitOp {
        SPLIT_OP_MAIN = 0x01,
        SPLIT_OP_INDEX = 0x02,
        SPLIT_OP_BOTH = 0x03,
    };

    void SplitWithReboots(TTestWithReboots& t, ESplitOp op,
            const std::function<void(TTestActorRuntime&)>& init,
            const std::function<ui64(TTestActorRuntime&, const TString&, const TVector<ui64>&)>& split)
    {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<ui64> mainTabletIds;
            TVector<ui64> indexTabletIds;

            {
                TInactiveZone inactive(activeZone);
                runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NActors::NLog::PRI_DEBUG);

                init(runtime);

                auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex/indexImplTable", true, true);
                indexTabletIds = MakeTabletIds(indexDesc.GetPathDescription().GetTablePartitions());
                mainTabletIds = Prepare(runtime, "/MyRoot/Table", {1, 10, 100});
            }

            TVector<ui64> txIds;
            if (op & SPLIT_OP_MAIN) {
                txIds.push_back(split(runtime, "/MyRoot/Table", mainTabletIds));
            }
            if (op & SPLIT_OP_INDEX) {
                txIds.push_back(split(runtime, "/MyRoot/Table/UserDefinedIndex/indexImplTable", indexTabletIds));
            }
            t.TestEnv->TestWaitNotification(runtime, txIds);

            {
                TInactiveZone inactive(activeZone);

                bool written = CheckWrittenToIndex(runtime,
                    {"/MyRoot/Table", {"key"}, {"key", "indexed"}, 3},
                    {"/MyRoot/Table/UserDefinedIndex/indexImplTable", {"indexed", "key"}, {"key", "indexed"}, 3});

                if (written) {
                    Prepare(runtime, "/MyRoot/Table", {2, 20, 200}, true);

                    written = CheckWrittenToIndex(runtime,
                        {"/MyRoot/Table", {"key"}, {"key", "indexed"}, 6},
                        {"/MyRoot/Table/UserDefinedIndex/indexImplTable", {"indexed", "key"}, {"key", "indexed"}, 6});
                    UNIT_ASSERT(written);
                }
            }
        });
    }

    void SplitWithReboots(TTestWithReboots& t, ESplitOp op, const std::function<void(TTestActorRuntime&)>& init) {
        SplitWithReboots(t, op, init, [&](TTestActorRuntime& runtime, const TString& path, const TVector<ui64>& tablets) {
            UNIT_ASSERT_VALUES_EQUAL(tablets.size(), 1);
            TestSplitTable(runtime, ++t.TxId, path, Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary {
                  KeyPrefix {
                    Tuple { Optional { Uint32: 50 } }
                  }
                }
            )", tablets[0]));
            return t.TxId;
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SplitMainWithReboots, 2, 1, false) {
        SplitWithReboots(t, SPLIT_OP_MAIN, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SplitIndexWithReboots, 2, 1, false) {
        SplitWithReboots(t, SPLIT_OP_INDEX, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SplitBothWithReboots, 2, 1, false) {
        SplitWithReboots(t, SPLIT_OP_BOTH, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CdcAndSplitWithReboots, 2, 1, false) {
        SplitWithReboots(t, SPLIT_OP_MAIN, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    void MergeWithReboots(TTestWithReboots& t, ESplitOp op, const std::function<void(TTestActorRuntime&)>& init) {
        SplitWithReboots(t, op, init, [&](TTestActorRuntime& runtime, const TString& path, const TVector<ui64>& tablets) {
            UNIT_ASSERT_VALUES_EQUAL(tablets.size(), 2);
            TestSplitTable(runtime, ++t.TxId, path, Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", tablets[0], tablets[1]));
            return t.TxId;
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MergeMainWithReboots, 2, 1, false) {
        MergeWithReboots(t, SPLIT_OP_MAIN, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                  SplitBoundary {
                    KeyPrefix {
                      Tuple { Optional { Uint32: 50 } }
                    }
                  }
                  PartitionConfig {
                    PartitioningPolicy {
                      MinPartitionsCount: 1
                    }
                  }
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MergeIndexWithReboots, 2, 1, false) {
        MergeWithReboots(t, SPLIT_OP_INDEX, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                  IndexImplTableDescriptions: [ {
                    SplitBoundary {
                      KeyPrefix {
                        Tuple { Optional { Uint32: 50 } }
                      }
                    }
                    PartitionConfig {
                      PartitioningPolicy {
                        MinPartitionsCount: 1
                      }
                    }
                  } ]
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MergeBothWithReboots, 2, 1, false) {
        MergeWithReboots(t, SPLIT_OP_BOTH, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                  SplitBoundary {
                    KeyPrefix {
                      Tuple { Optional { Uint32: 50 } }
                    }
                  }
                  PartitionConfig {
                    PartitioningPolicy {
                      MinPartitionsCount: 1
                    }
                  }
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                  IndexImplTableDescriptions: [ {
                    SplitBoundary {
                      KeyPrefix {
                        Tuple { Optional { Uint32: 50 } }
                      }
                    }
                    PartitionConfig {
                      PartitioningPolicy {
                        MinPartitionsCount: 1
                      }
                    }
                  } ]
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CdcAndMergeWithReboots, 2, 1, false) {
        MergeWithReboots(t, SPLIT_OP_MAIN, [&](TTestActorRuntime& runtime) {
            TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint32" }
                  Columns { Name: "indexed" Type: "Uint32" }
                  KeyColumnNames: ["key"]
                  SplitBoundary {
                    KeyPrefix {
                      Tuple { Optional { Uint32: 50 } }
                    }
                  }
                  PartitionConfig {
                    PartitioningPolicy {
                      MinPartitionsCount: 1
                    }
                  }
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                  Type: EIndexTypeGlobalAsync
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropTableWithInflightChanges, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            auto origObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

            TVector<THolder<IEventHandle>> enqueued;
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchange::EvEnqueueRecords) {
                    enqueued.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return origObserver(ev);
            });

            {
                TInactiveZone inactive(activeZone);
                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                    TableDescription {
                      Name: "Table"
                      Columns { Name: "key" Type: "Uint32" }
                      Columns { Name: "indexed" Type: "Uint32" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndex"
                      KeyColumnNames: ["indexed"]
                      Type: EIndexTypeGlobalAsync
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                Prepare(runtime, "/MyRoot/Table", {1, 10, 100}, true);
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");

            runtime.SetObserverFunc(origObserver);
            for (auto& ev : std::exchange(enqueued, {})) {
                runtime.Send(ev.Release(), 0, true);
            }

            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {
                TTestTxConfig::FakeHiveTablets,
                TTestTxConfig::FakeHiveTablets + 1,
            });
        });
    }

    Y_UNIT_TEST(Decimal) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableParameterizedDecimal(true));
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"_(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Decimal(35,9)" }
              Columns { Name: "indexed" Type: "Decimal(35,9)" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
              Type: EIndexTypeGlobalAsync
            }
        )_");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalAsync),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"indexed"}),
        });
    }
}
