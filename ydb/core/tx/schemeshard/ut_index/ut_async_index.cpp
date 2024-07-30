#include <ydb/core/base/path.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

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

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex"),
            {NLs::PathExist,
             NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalAsync),
             NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
             NLs::IndexKeys({"indexed"})});
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
            "UserDefinedIndex", NKikimrSchemeOp::EIndexTypeGlobalAsync, {"indexed"}, {}
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

    NKikimrMiniKQL::TResult ReadTable(TTestActorRuntime& runtime, ui64 tabletId,
            const TString& table, const TVector<TString>& pk, const TVector<TString>& columns)
    {
        TStringBuilder keyFmt;
        for (const auto& k : pk) {
            keyFmt << "'('" << k << " (Null) (Void)) ";
        }
        const auto columnsFmt = "'" + JoinSeq(" '", columns);

        NKikimrMiniKQL::TResult result;
        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"((
            (let range '(%s))
            (let columns '(%s))
            (let result (SelectRange '__user__%s range columns '()))
            (return (AsList (SetResult 'Result result) ))
        ))", keyFmt.data(), columnsFmt.data(), table.data()), result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);

        return result;
    }

    struct TTableTraits {
        TString Path;
        TVector<TString> Key;
        TVector<TString> Columns;
        ui32 ExpectedRecords;
    };

    template <typename C>
    ui32 CountRows(TTestActorRuntime& runtime, const TTableTraits& table, const C& partitions) {
        ui32 rows = 0;

        for (const auto& x : partitions) {
            auto result = ReadTable(runtime, x.GetDatashardId(), SplitPath(table.Path).back(), table.Key, table.Columns);
            auto value = NClient::TValue::Create(result);
            rows += value["Result"]["List"].Size();
        }

        return rows;
    }

    bool CheckWrittenToIndex(TTestActorRuntime& runtime, const TTableTraits& mainTable, const TTableTraits& indexTable) {
        bool writtenToMainTable = false;
        {
            auto tableDesc = DescribePath(runtime, mainTable.Path, true, true);
            const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT(!tablePartitions.empty());
            writtenToMainTable = mainTable.ExpectedRecords == CountRows(runtime, mainTable, tablePartitions);
        }

        if (writtenToMainTable) {
            auto tableDesc = DescribePrivatePath(runtime, indexTable.Path, true, true);
            const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT(!tablePartitions.empty());

            int i = 0;
            while (++i < 10) {
                runtime.SimulateSleep(TDuration::Seconds(1));
                if (indexTable.ExpectedRecords == CountRows(runtime, indexTable, tablePartitions)) {
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

    template <typename T>
    void SplitWithReboots(ESplitOp op,
            const std::function<void(T&, TTestActorRuntime&)>& init,
            const std::function<ui64(T&, TTestActorRuntime&, const TString&, const TVector<ui64>&)>& split)
    {
        T t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<ui64> mainTabletIds;
            TVector<ui64> indexTabletIds;

            {
                TInactiveZone inactive(activeZone);
                runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NActors::NLog::PRI_DEBUG);

                init(t, runtime);

                auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex/indexImplTable", true, true);
                indexTabletIds = MakeTabletIds(indexDesc.GetPathDescription().GetTablePartitions());
                mainTabletIds = Prepare(runtime, "/MyRoot/Table", {1, 10, 100});
            }

            TVector<ui64> txIds;
            if (op & SPLIT_OP_MAIN) {
                txIds.push_back(split(t, runtime, "/MyRoot/Table", mainTabletIds));
            }
            if (op & SPLIT_OP_INDEX) {
                txIds.push_back(split(t, runtime, "/MyRoot/Table/UserDefinedIndex/indexImplTable", indexTabletIds));
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

    template <typename T>
    void SplitWithReboots(ESplitOp op, const std::function<void(T&, TTestActorRuntime&)>& init) {
        SplitWithReboots<T>(op, init, [](T& t, TTestActorRuntime& runtime, const TString& path, const TVector<ui64>& tablets) {
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

    Y_UNIT_TEST_WITH_REBOOTS(SplitMainWithReboots) {
        SplitWithReboots<T>(SPLIT_OP_MAIN, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(SplitIndexWithReboots) {
        SplitWithReboots<T>(SPLIT_OP_INDEX, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(SplitBothWithReboots) {
        SplitWithReboots<T>(SPLIT_OP_BOTH, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(CdcAndSplitWithReboots) {
        SplitWithReboots<T>(SPLIT_OP_MAIN, [](T& t, TTestActorRuntime& runtime) {
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

    template <typename T>
    void MergeWithReboots(ESplitOp op, const std::function<void(T&, TTestActorRuntime&)>& init) {
        SplitWithReboots<T>(op, init, [](T& t, TTestActorRuntime& runtime, const TString& path, const TVector<ui64>& tablets) {
            UNIT_ASSERT_VALUES_EQUAL(tablets.size(), 2);
            TestSplitTable(runtime, ++t.TxId, path, Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", tablets[0], tablets[1]));
            return t.TxId;
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MergeMainWithReboots) {
        MergeWithReboots<T>(SPLIT_OP_MAIN, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(MergeIndexWithReboots) {
        MergeWithReboots<T>(SPLIT_OP_INDEX, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(MergeBothWithReboots) {
        MergeWithReboots<T>(SPLIT_OP_BOTH, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(CdcAndMergeWithReboots) {
        MergeWithReboots<T>(SPLIT_OP_MAIN, [](T& t, TTestActorRuntime& runtime) {
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

    Y_UNIT_TEST_WITH_REBOOTS(DropTableWithInflightChanges) {
        T t;
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
}
