#include <ydb/core/base/path.h>
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

    TVector<ui64> Prepare(TTestActorRuntime& runtime, const TString& mainTablePath) {
        ui64 mainTableId = 0;
        TVector<ui64> mainTabletIds;
        TVector<std::pair<TString, TString>> rows;

        {
            auto tableDesc = DescribePath(runtime, mainTablePath, true, true);
            const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();

            mainTableId = tableDesc.GetPathId();
            for (const auto& partition : tablePartitions) {
                mainTabletIds.push_back(partition.GetDatashardId());
            }
        }

        for (ui32 i = 0; i < 3; ++i) {
            auto key = TVector<TCell>{TCell::Make(1 << i)};
            auto value = TVector<TCell>{TCell::Make(i)};
            rows.emplace_back(TSerializedCellVec::Serialize(key), TSerializedCellVec::Serialize(value));
        }

        auto ev = MakeUploadRows(mainTableId, {1}, {2}, std::move(rows));
        ForwardToTablet(runtime, mainTabletIds[0], runtime.AllocateEdgeActor(), ev.Release());

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
    };

    void CheckWrittenToIndex(TTestActorRuntime& runtime, const TTableTraits& mainTable, const TTableTraits& indexTable) {
        bool writtenToMainTable = false;
        {
            auto tableDesc = DescribePath(runtime, mainTable.Path, true, true);
            const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT(!tablePartitions.empty());

            auto result = ReadTable(runtime, tablePartitions[0].GetDatashardId(), SplitPath(mainTable.Path).back(),
                mainTable.Key, mainTable.Columns);
            auto value = NClient::TValue::Create(result);
            writtenToMainTable = value["Result"]["List"].Size() == 3;
        }

        if (writtenToMainTable) {
            auto tableDesc = DescribePrivatePath(runtime, indexTable.Path, true, true);
            const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT(!tablePartitions.empty());

            int i = 0;
            while (++i < 10) {
                runtime.SimulateSleep(TDuration::Seconds(1));

                auto result = ReadTable(runtime, tablePartitions[0].GetDatashardId(), SplitPath(indexTable.Path).back(),
                    indexTable.Key, indexTable.Columns);
                auto value = NClient::TValue::Create(result);
                if (value["Result"]["List"].Size() == 3) {
                    break;
                }
            }

            UNIT_ASSERT(i < 10);
        }
    }

    Y_UNIT_TEST_WITH_REBOOTS(SplitWithReboots) {
        T t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<ui64> mainTabletIds;

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

                mainTabletIds = Prepare(runtime, "/MyRoot/Table");
                UNIT_ASSERT_VALUES_EQUAL(mainTabletIds.size(), 1);
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint32: 100 } }
                    }
                }
            )", mainTabletIds[0]));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                CheckWrittenToIndex(runtime,
                    {"/MyRoot/Table", {"key"}, {"key", "indexed"}},
                    {"/MyRoot/Table/UserDefinedIndex/indexImplTable", {"indexed", "key"}, {"key", "indexed"}});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MergeWithReboots) {
        T t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<ui64> mainTabletIds;

            {
                TInactiveZone inactive(activeZone);

                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                    TableDescription {
                      Name: "Table"
                      Columns { Name: "key" Type: "Uint32" }
                      Columns { Name: "indexed" Type: "Uint32" }
                      KeyColumnNames: ["key"]
                      UniformPartitionsCount: 2
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

                mainTabletIds = Prepare(runtime, "/MyRoot/Table");
                UNIT_ASSERT_VALUES_EQUAL(mainTabletIds.size(), 2);
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", mainTabletIds[0], mainTabletIds[1]));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                CheckWrittenToIndex(runtime,
                    {"/MyRoot/Table", {"key"}, {"key", "indexed"}},
                    {"/MyRoot/Table/UserDefinedIndex/indexImplTable", {"indexed", "key"}, {"key", "indexed"}});
            }
        });
    }
}
