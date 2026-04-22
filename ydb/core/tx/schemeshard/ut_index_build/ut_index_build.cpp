#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/base/table_index.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

static void WriteRows(TTestActorRuntime& runtime, ui64 tabletId, ui32 key, ui32 index) {
    TString writeQuery = Sprintf(R"(
        (
            (let keyNull   '( '('key   (Null) ) ) )
            (let row0   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key0   '( '('key   (Uint32 '%u ) ) ) )
            (let row0   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key1   '( '('key   (Uint32 '%u ) ) ) )
            (let row1   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key2   '( '('key   (Uint32 '%u ) ) ) )
            (let row2   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key3   '( '('key   (Uint32 '%u ) ) ) )
            (let row3   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key4   '( '('key   (Uint32 '%u ) ) ) )
            (let row4   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key5   '( '('key   (Uint32 '%u ) ) ) )
            (let row5   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key6   '( '('key   (Uint32 '%u ) ) ) )
            (let row6   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key7   '( '('key   (Uint32 '%u ) ) ) )
            (let row7   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key8   '( '('key   (Uint32 '%u ) ) ) )
            (let row8   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key9   '( '('key   (Uint32 '%u ) ) ) )
            (let row9   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )

            (return (AsList
                        (UpdateRow '__user__Table keyNull row0)
                        (UpdateRow '__user__Table key0 row0)
                        (UpdateRow '__user__Table key1 row1)
                        (UpdateRow '__user__Table key2 row2)
                        (UpdateRow '__user__Table key3 row3)
                        (UpdateRow '__user__Table key4 row4)
                        (UpdateRow '__user__Table key5 row5)
                        (UpdateRow '__user__Table key6 row6)
                        (UpdateRow '__user__Table key7 row7)
                        (UpdateRow '__user__Table key8 row8)
                        (UpdateRow '__user__Table key9 row9)
                     )
            )
        )
    )",
         1000*index + 0,
         1000*key + 0, 1000*index + 0,
         1000*key + 1, 1000*index + 1,
         1000*key + 2, 1000*index + 2,
         1000*key + 3, 1000*index + 3,
         1000*key + 4, 1000*index + 4,
         1000*key + 5, 1000*index + 5,
         1000*key + 6, 1000*index + 6,
         1000*key + 7, 1000*index + 7,
         1000*key + 8, 1000*index + 8,
         1000*key + 9, 1000*index + 9);

    NKikimrMiniKQL::TResult result;
    TString err;
    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
    UNIT_ASSERT_VALUES_EQUAL(err, "");
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
}

Y_UNIT_TEST_SUITE(IndexBuildTest) {
    Y_UNIT_TEST(ShadowDataNotAllowedByDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "key"     Type: "Uint32" }
              KeyColumnNames: ["index", "key"]
              PartitionConfig {
                  CompactionPolicy {
                      KeepEraseMarkers: true
                  }
                  ShadowData: true
              }
        )", {NKikimrScheme::StatusInvalidParameter});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "key"     Type: "Uint32" }
              KeyColumnNames: ["index", "key"]
              PartitionConfig {
                  CompactionPolicy {
                      KeepEraseMarkers: true
                  }
                  ShadowData: false
              }
        )", {NKikimrScheme::StatusInvalidParameter});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "key"     Type: "Uint32" }
              KeyColumnNames: ["index", "key"]
              PartitionConfig {
                  CompactionPolicy {
                      KeepEraseMarkers: true
                  }
              }
        )");
        env.TestWaitNotification(runtime, txId);
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              PartitionConfig {
                  ShadowData: false
              }
        )", {NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              PartitionConfig {
                  ShadowData: true
              }
        )", {NKikimrScheme::StatusInvalidParameter});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "CopyTable"
              CopyFromTable: "/MyRoot/IndexTable"
              PartitionConfig {
                  ShadowData: true
              }
        )", {NKikimrScheme::StatusInvalidParameter});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "CopyTable"
              CopyFromTable: "/MyRoot/IndexTable"
              PartitionConfig {
                  ShadowData: false
              }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(ShadowDataEdgeCases) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Allow manipulating shadow data using normal schemeshard operations
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "key"     Type: "Uint32" }
              KeyColumnNames: ["index", "key"]
              PartitionConfig {
                  CompactionPolicy {
                      KeepEraseMarkers: true
                  }
                  ShadowData: true
              }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "CopyTable"
              CopyFromTable: "/MyRoot/IndexTable"
        )", {NKikimrScheme::StatusPreconditionFailed});

        // This is basically a no-op alter, not filtered at the moment
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              PartitionConfig {
                  ShadowData: true
              }
        )");
        env.TestWaitNotification(runtime, txId);

        // This removes shadow data, should be allowed
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              PartitionConfig {
                  ShadowData: false
              }
        )");
        env.TestWaitNotification(runtime, txId);

        // Shadow data cannot be re-enabled
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              PartitionConfig {
                  ShadowData: true
              }
        )", {NKikimrScheme::StatusInvalidParameter});

        // This is basically a no-op alter, not filtered at the moment
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "IndexTable"
              PartitionConfig {
                  ShadowData: false
              }
        )");
        env.TestWaitNotification(runtime, txId);

        // Copy should work after shadow data is disabled
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "CopyTable1"
              CopyFromTable: "/MyRoot/IndexTable"
        )");
        env.TestWaitNotification(runtime, txId);

        // Should we prohibit creation of shadow data in a copy?
        // Technically it's safe, even if not backwards compatible.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "CopyTable2"
              CopyFromTable: "/MyRoot/IndexTable"
              PartitionConfig {
                  ShadowData: true
              }
        )");
        env.TestWaitNotification(runtime, txId);

        // This should remove shadow data correctly
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "CopyTable2"
              PartitionConfig {
                  ShadowData: false
              }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(Metering_Documentation_Formula) {
        for (ui64 dataSizeMB : {1500, 500, 100, 0}) {
            const ui64 rowCount = 500'000;

            TMeteringStats stats;
            stats.SetReadRows(rowCount);
            stats.SetReadBytes(dataSizeMB * 1_MB);
            stats.SetUploadRows(rowCount);
            stats.SetUploadBytes(dataSizeMB * 1_MB);

            TString explain;
            const ui64 result = TRUCalculator::Calculate(stats, explain);

            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#secondary-index
            UNIT_ASSERT_VALUES_EQUAL_C(result, Max<ui64>(dataSizeMB * 640, dataSizeMB * 128 + rowCount * 0.5), explain);
        }
    }

    void BaseCase(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        // Just create main table
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            // What is __user__?
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };
        for (ui32 delta = 0; delta < 101; ++delta) {
            // What is TTestTxConfig::FakeHiveTablets + 6?
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        TStringBuilder meteringMessages;
        auto grabMeteringMessage = [&meteringMessages](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->Type == NMetering::TEvMetering::TEvWriteMeteringJson::EventType) {
                auto *msg = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>();
                Cerr << "grabMeteringMessage has happened" << Endl;
                meteringMessages << msg->MeteringJson;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(grabMeteringMessage);

        TestBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}});
        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
        // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#secondary-index
        const TString meteringId = indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique ? "106-72075186233409549-2-0-0-0-0-101-202-1818-3030" : "106-72075186233409549-2-0-0-0-0-101-101-1818-1818";
        const TString expectedMetering = Sprintf(R"({"usage":{"start":0,"quantity":179,"finish":0,"unit":"request_unit","type":"delta"},"tags":{},"id":"%s","cloud_id":"CLOUD_ID_VAL","source_wt":0,"source_id":"sless-docapi-ydb-ss","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.requests.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})",
            meteringId.c_str()
        );
        MeteringDataEqual(meteringMessages, expectedMetering);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1", true, true, true),
                           {NLs::PathExist,
                            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

        TestForgetBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 0);

        TestDropTableIndex(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            TableName: "Table"
            IndexName: "index1"
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(8)});

        // Test that index build succeeds on recreated columns

        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              DropColumns { Name: "index" }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "index"   Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{"index2", indexType, {"index"}, {}, {}});
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // CommonDB
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"CommonDB\"");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "StoragePools { "
                              "  Name: \"pool-3\" "
                              "  Kind: \"pool-kind-3\" "
                              "} "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              "Name: \"CommonDB\"");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CommonDB"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("CommonDB"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/CommonDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        for (ui32 delta = 0; delta < 101; ++delta) {
            // What is TTestTxConfig::FakeHiveTablets + 12?
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 12, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TVector<TString> billRecords;
        TVector<bool> shadowData;
        TVector<bool> keepEraseMarkers;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Type == NMetering::TEvMetering::TEvWriteMeteringJson::EventType) {
                auto* msg = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>();
                billRecords.push_back(msg->MeteringJson);
            } else if (ev->Type == TSchemeBoardEvents::TEvNotifyUpdate::EventType) {
                auto* msg = ev->Get<TSchemeBoardEvents::TEvNotifyUpdate>();
                if (msg->Path.EndsWith(NTableIndex::ImplTable)) {
                    auto& desc = msg->DescribeSchemeResult.GetPathDescription().GetTable().GetPartitionConfig();
                    shadowData.push_back(desc.GetShadowData());
                    keepEraseMarkers.push_back(desc.GetCompactionPolicy().GetKeepEraseMarkers());
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/CommonDB", "/MyRoot/CommonDB/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}});
        buildIndexId = txId;

        listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB", txId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        UNIT_ASSERT_VALUES_EQUAL(billRecords.size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(shadowData.size(), indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobal ? 2 : 3);
        UNIT_ASSERT_VALUES_EQUAL(shadowData[0], true);
        UNIT_ASSERT_VALUES_EQUAL(shadowData[1], false);
        UNIT_ASSERT(shadowData.size() < 3 || shadowData[2] == false);

        UNIT_ASSERT_VALUES_EQUAL(keepEraseMarkers.size(), indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobal ? 2 : 3);
        UNIT_ASSERT_VALUES_EQUAL(keepEraseMarkers[0], true);
        UNIT_ASSERT_VALUES_EQUAL(keepEraseMarkers[1], false);
        UNIT_ASSERT(keepEraseMarkers.size() < 3 || keepEraseMarkers[2] == false);
    }

    Y_UNIT_TEST(BaseCase) {
        BaseCase(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(BaseCaseUniq) {
        BaseCase(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void CancellationNotEnoughRetries(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetSplitMergePartCountLimit(&runtime, -1);

        // Just create main table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              Columns { Name: "index"   Type: "Uint32" }
              KeyColumnNames: ["key", "value"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto fnWriteRow = [&](ui64 tabletId, ui32 key, ui32 index, const TString& value, const char *table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) '('value (Utf8 '%s) ) ) )
                    (let row   '( '('index (Uint32 '%u ) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, value.c_str(), index, table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };
        TVector<char> longStrData(100000, 'a');
        TString longString(longStrData.begin(), longStrData.end());
        for (ui32 delta = 0; delta < 1000; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, 1000 + delta, longString, "Table");
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        // Force stats reporting without delays
        runtime.GetAppData().DataShardConfig.SetStatsReportIntervalSeconds(0);
        NDataShard::gDbStatsDataSizeResolution = 80000;

        auto upgradeEvent = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->Type == TEvSchemeShard::EvModifySchemeTransaction) {
                auto *msg = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>();
                if (msg->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild) {
                    auto& tx = *msg->Record.MutableTransaction(0);
                    auto& config = *tx.MutableInitiateIndexBuild();
                    NKikimrSchemeOp::TIndexCreationConfig& indexConfig = *config.MutableIndex();
                    NKikimrSchemeOp::TTableDescription& indexTableDescr = indexConfig.MutableIndexImplTableDescriptions()->at(0);

                    indexTableDescr.MutablePartitionConfig()->MutablePartitioningPolicy()->SetSizeToSplit(10);
                    indexTableDescr.MutablePartitionConfig()->MutablePartitioningPolicy()->SetMaxPartitionsCount(10);

                    Cerr << "upgradeEvent has happened" << Endl;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(upgradeEvent);

        {
            NKikimrIndexBuilder::TIndexBuildSettings settings;
            settings.set_source_path("/MyRoot/Table");
            settings.MutableScanSettings()->SetMaxBatchRows(0); // row by row
            settings.MutableScanSettings()->SetMaxBatchBytes(1<<10);
            settings.MutableScanSettings()->SetMaxBatchRetries(0);
            settings.set_max_shards_in_flight(1);

            Ydb::Table::TableIndex& index = *settings.mutable_index();
            index.set_name("index1");
            index.add_index_columns("index");
            if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                *index.mutable_global_unique_index() = Ydb::Table::GlobalUniqueIndex();
            } else {
                *index.mutable_global_index() = Ydb::Table::GlobalIndex();
            }

            auto request = new TEvIndexBuilder::TEvCreateRequest(++txId, "/MyRoot", std::move(settings));
            auto sender = runtime.AllocateEdgeActor();

            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);

            TAutoPtr<IEventHandle> handle;
            TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
            UNIT_ASSERT(event);

            Cerr << "BUILDINDEX RESPONSE CREATE: " << event->ToString() << Endl;
            UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), Ydb::StatusIds::SUCCESS,
                                "status mismatch"
                                        << " got " << Ydb::StatusIds::StatusCode_Name(event->Record.GetStatus())
                                        << " expected "  << Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS));
        }
        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, txId);

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId);
        UNIT_ASSERT_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table/index1", true, true, true),
                           {NLs::PathNotExist});

        TestForgetBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
        listing = TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 0);
    }

    Y_UNIT_TEST(CancellationNotEnoughRetries) {
        CancellationNotEnoughRetries(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(CancellationNotEnoughRetriesUniq) {
        CancellationNotEnoughRetries(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void CancellationNoTable(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        NKikimrIndexBuilder::TEvListResponse listing = TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 0);
    }

    Y_UNIT_TEST(CancellationNoTable) {
        CancellationNoTable(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(CancellationNoTableUniq) {
        CancellationNoTable(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void WithFollowers(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "WithFollowers"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value0" Type: "Utf8" }
            Columns { Name: "value1" Type: "Utf8" }
            Columns { Name: "valueFloat" Type: "Float" }
            KeyColumnNames: ["key"]
            PartitionConfig {
              FollowerGroups {
                FollowerCount: 1
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/WithFollowers", TBuildIndexConfig{"UserDefinedIndexByValue0", indexType, {"value0"}, {}, {}});
        env.TestWaitNotification(runtime, txId);

        ui64 buildId = txId;

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
        UNIT_ASSERT(descr.GetIndexBuild().HasStartTime());
        UNIT_ASSERT(descr.GetIndexBuild().HasEndTime());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/WithFollowers"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/WithFollowers/UserDefinedIndexByValue0", true, true, true),
                           {NLs::PathExist,
                            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

        TestForgetBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
        auto listing = TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 0);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "WithFollowers"
            IndexName: "UserDefinedIndexByValue0"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/WithFollowers"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(8)});
    }

    Y_UNIT_TEST(WithFollowers) {
        WithFollowers(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(WithFollowersUniq) {
        WithFollowers(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void RejectsCreate(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/NotExist", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        TestMkDir(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", "DIR");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/DIR", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              Columns { Name: "valueFloat" Type: "Float" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              Type: %s
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              Type: %s
              KeyColumnNames: ["value1"]
            }
        )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str(), NKikimrSchemeOp::EIndexType_Name(indexType).c_str()));
        env.TestWaitNotification(runtime, txId);

        // should not affect index limits
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"UserDefinedIndexByValue0", indexType, {"value0"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"UserDefinedIndexByValue0", indexType, {"value1"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"NotExist"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"valueFloat"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        TSchemeLimits lowLimits;

        lowLimits.ExtraPathSymbolsAllowed = "_-.";
        lowLimits.MaxTableIndices = 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"!name!", indexType, {"value0"}, {}, {}}, Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxTableIndices = 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"value0", "value1"}, {}, {}}, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxTableIndices = 3;
        lowLimits.MaxChildrenInDir = 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"value0", "value1"}, {}, {}}, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxTableIndices = 3;
        lowLimits.MaxChildrenInDir = 4;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"value0", "value1"}, {}, {}}, Ydb::StatusIds::SUCCESS);
        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"value0", "value1"}, {}, {}}, Ydb::StatusIds::OVERLOADED);
        env.TestWaitNotification(runtime, {txId, txId - 1});
    }

    Y_UNIT_TEST(RejectsCreate) {
        RejectsCreate(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(RejectsCreateUniq) {
        RejectsCreate(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void CheckLimitWithDroppedIndex(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSchemeLimits lowLimits;
        lowLimits.MaxTableIndices = 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"Index1", indexType, {"value"}, {}, {}}, Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "Index1"
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"Index2", indexType, {"value"}, {}, {}}, Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CheckLimitWithDroppedIndex) {
        CheckLimitWithDroppedIndex(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(CheckLimitWithDroppedIndexUniq) {
        CheckLimitWithDroppedIndex(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void Lock(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Just create main table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TWaitForFirstEvent<TEvSchemeShard::TEvModifySchemeTransaction> lockWaiter(runtime,
            [](const auto& ev) {
                const auto& record = ev->Get()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock && record.GetTransaction(0).GetWorkingDir() == "/MyRoot" && record.GetTransaction(0).GetLockConfig().GetName() == "Table") {
                    return true;
                }
                return false;
            }
        );

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> dropLockBlocker(runtime, [](const auto& ev) {
            const auto& modifyScheme = ev->Get()->Record.GetTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock;
        });

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"nameOK", indexType, {"index"}, {}, {}});
        ui64 buildIndexId = txId;

        lockWaiter.Wait();

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                DropColumns { Name: "index" }
                           )",
                       {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "nameOK"
        )", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table",
                      {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, txId);

        dropLockBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table/nameOK", true, true, true),
                           {NLs::PathExist,
                            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

        NKikimrIndexBuilder::TEvGetResponse descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

//        KIKIMR-9945
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                DropColumns { Name: "index" }
                           )",
                       {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(Lock) {
        Lock(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(LockUniq) {
        Lock(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void MergeIndexTableShardsOnlyWhenReady(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DisableStatsBatching(true);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::GlobalIndexSettings settings;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            partition_at_keys {
                split_points {
                    type { tuple_type { elements { optional_type { item { type_id: UINT64 } } } } }
                    value { items { uint64_value: 10 } }
                }
                split_points {
                    type { tuple_type { elements { optional_type { item { type_id: UINT64 } } } } }
                    value { items { uint64_value: 20 } }
                }
                split_points {
                    type { tuple_type { elements { optional_type { item { type_id: UINT64 } } } } }
                    value { items { uint64_value: 30 } }
                }
            }
        )", &settings));

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> indexApplicationBlocker(runtime, [](const auto& ev) {
            const auto& modifyScheme = ev->Get()->Record.GetTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild;
        });

        ui64 indexInitializationTx = 0;
        TWaitForFirstEvent<TEvSchemeShard::TEvModifySchemeTransaction> indexInitializationWaiter(runtime,
            [&indexInitializationTx](const auto& ev){
                const auto& record = ev->Get()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexBuild) {
                    indexInitializationTx = record.GetTxId();
                    return true;
                }
                return false;
            }
        );

        // we lock index impl table in case of unique index
        TWaitForFirstEvent<TEvSchemeShard::TEvModifySchemeTransaction> indexImplTableLockWaiter(runtime,
            [](const auto& ev) {
                const auto& record = ev->Get()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock && record.GetTransaction(0).GetWorkingDir() == "/MyRoot/Table/ByValue" && record.GetTransaction(0).GetLockConfig().GetName() == "indexImplTable") {
                    return true;
                }
                return false;
            }
        );

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime,  buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "ByValue", indexType, { "value" }, {},
            { NYdb::NTable::TGlobalIndexSettings::FromProto(settings) }
        });

        indexInitializationWaiter.Wait();
        UNIT_ASSERT_VALUES_UNEQUAL(indexInitializationTx, 0);
        env.TestWaitNotification(runtime, indexInitializationTx);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/ByValue"), {
            NLs::PathExist,
            NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly)
        });

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
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/ByValue/indexImplTable", true), {
            NLs::PathExist,
            NLs::PartitionCount(4),
            shardCollector
        });
        UNIT_ASSERT_VALUES_EQUAL(indexShards.size(), 4);

        {
            // make sure no shards are merged
            TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> mergeBlocker(runtime, [](const auto& ev) {
                const auto& modifyScheme = ev->Get()->Record.GetTransaction(0);
                return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions;
            });

            {
                // wait for all index shards to send statistics
                THashSet<ui64> shardsWithStats;
                using TEvType = TEvDataShard::TEvPeriodicTableStats;
                auto statsObserver = runtime.AddObserver<TEvType>([&shardsWithStats](const TEvType::TPtr& ev) {
                    shardsWithStats.emplace(ev->Get()->Record.GetDatashardId());
                });

                runtime.WaitFor("all index shards to send statistics", [&]{
                    return AllOf(indexShards, [&shardsWithStats](ui64 indexShard) {
                        return shardsWithStats.contains(indexShard);
                    });
                });
            }

            // we expect to not have observed any attempts to merge
            UNIT_ASSERT(mergeBlocker.empty());

            // wait for 1 minute to ensure that no merges have been started by SchemeShard
            env.SimulateSleep(runtime, TDuration::Minutes(1));
            UNIT_ASSERT(mergeBlocker.empty());
        }

        TVector<TExpectedResult> expectedSplitTableResult;
        if (indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique) {
            // unique index locks index table before validating
            expectedSplitTableResult.emplace_back(NKikimrScheme::StatusMultipleModifications);

            indexImplTableLockWaiter.Wait();
            // We don't need to wait for transaction result explicitly, because the Scheme Shard will order them according to it's queue
        } else {
            // splits are allowed even if the index is not ready
            expectedSplitTableResult.emplace_back(NKikimrScheme::StatusAccepted);
        }

        auto runSplit = [&](const TVector<TExpectedResult>& expectedResults) {
            TestSplitTable(runtime, ++txId, "/MyRoot/Table/ByValue/indexImplTable", Sprintf(R"(
                        SourceTabletId: %lu
                        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 5 } } } }
                    )",
                    indexShards.front()
                ),
                expectedResults
            );
            env.TestWaitNotification(runtime, txId);
        };

        runSplit(expectedSplitTableResult);

        indexApplicationBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexTx);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/ByValue"), {
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)
        });

        if (indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique) {
            runSplit({{NKikimrScheme::StatusAccepted}});
        }

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

    Y_UNIT_TEST(MergeIndexTableShardsOnlyWhenReady) {
        MergeIndexTableShardsOnlyWhenReady(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(MergeIndexTableShardsOnlyWhenReadyUniq) {
        MergeIndexTableShardsOnlyWhenReady(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void IndexPartitioningIsPersisted(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: [ "key" ]
        )");
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::GlobalIndexSettings settings;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            partition_at_keys {
                split_points {
                    type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                    value { items { text_value: "alice" } }
                }
                split_points {
                    type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                    value { items { text_value: "bob" } }
                }
            }
            partitioning_settings {
                min_partitions_count: 3
                max_partitions_count: 3
            }
        )", &settings));

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> indexCreationBlocker(runtime, [](const auto& ev) {
            const auto& modifyScheme = ev->Get()->Record.GetTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexBuild;
        });

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "Index", indexType, { "value" }, {},
            { NYdb::NTable::TGlobalIndexSettings::FromProto(settings) }
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        indexCreationBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexTx);

        auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            buildIndexOperation.DebugString()
        );

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::IsTable,
            NLs::IndexesCount(1)
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index"), {
            NLs::PathExist,
            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable", true, true), {
            NLs::IsTable,
            NLs::PartitionCount(3),
            NLs::MinPartitionsCountEqual(3),
            NLs::MaxPartitionsCountEqual(3),
            NLs::PartitionKeys({"alice", "bob", ""})
        });
    }

    Y_UNIT_TEST(IndexPartitioningIsPersisted) {
        IndexPartitioningIsPersisted(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(IndexPartitioningIsPersistedUniq) {
        IndexPartitioningIsPersisted(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void DropIndex(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              Type: %s
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              Type: %s
              KeyColumnNames: ["value1"]
            }
        )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str(), NKikimrSchemeOp::EIndexType_Name(indexType).c_str()));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(2)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                           {NLs::Finished,
                            NLs::IndexType(indexType),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "UserDefinedIndexByValue0"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::IndexesCount(1)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::PathNotExist});

        TestCopyTable(runtime, ++txId, "/MyRoot", "Copy", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::IndexesCount(1)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue1"),
                           {NLs::PathExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue1/indexImplTable"),
                           {NLs::PathExist});


        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Copy");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DropIndex) {
        DropIndex(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(DropIndexUniq) {
        DropIndex(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void RejectsDropIndex(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              Type: %s
              KeyColumnNames: ["value0"]
            }
        )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str()));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(1)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                           {NLs::Finished,
                            NLs::IndexType(indexType),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});

        TestDropTableIndex(runtime, ++txId, "/MyRoot/NotExist", R"(
            TableName: "Table"
            IndexName: "UserDefinedIndexByValue0"
        )", {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "NotExist"
            IndexName: "UserDefinedIndexByValue0"
        )", {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "NotExist"
        )", {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "UserDefinedIndexByValue0"
        )");
        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "UserDefinedIndexByValue0"
        )", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::IndexesCount(0)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::PathNotExist});

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RejectsDropIndex) {
        RejectsDropIndex(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(RejectsDropIndexUniq) {
        RejectsDropIndex(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void CancelBuild(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        // Just create main table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };
        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}});
        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);

        env.TestWaitNotification(runtime, buildIndexId);

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);
        UNIT_ASSERT(descr.GetIndexBuild().HasStartTime());
        UNIT_ASSERT(descr.GetIndexBuild().HasEndTime());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table/index1", true, true, true),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CancelBuild) {
        CancelBuild(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(CancelBuildUniq) {
        CancelBuild(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void RejectsCancel(NKikimrSchemeOp::EIndexType indexType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        // Just create main table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        };
        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}});
        ui64 buildIndexId = txId;

        {
            auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
            UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_PREPARING);
            UNIT_ASSERT(descr.GetIndexBuild().HasStartTime());
            UNIT_ASSERT(!descr.GetIndexBuild().HasEndTime());
        }

        runtime.AdvanceCurrentTime(TDuration::Seconds(30)); // building index

        //
        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId + 1, TVector<Ydb::StatusIds::StatusCode>{Ydb::StatusIds::NOT_FOUND});
        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot/DirNoExist", buildIndexId, TVector<Ydb::StatusIds::StatusCode>{Ydb::StatusIds::NOT_FOUND});

        env.TestWaitNotification(runtime, buildIndexId);

        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId, TVector<Ydb::StatusIds::StatusCode>{Ydb::StatusIds::PRECONDITION_FAILED});

        {
            auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
            UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            UNIT_ASSERT(descr.GetIndexBuild().HasStartTime());
            UNIT_ASSERT(descr.GetIndexBuild().HasEndTime());
            UNIT_ASSERT_LT(descr.GetIndexBuild().GetStartTime().seconds(), descr.GetIndexBuild().GetEndTime().seconds());
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table/index1", true, true, true),
                           {NLs::PathExist});
    }

    Y_UNIT_TEST(RejectsCancel) {
        RejectsCancel(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(RejectsCancelUniq) {
        RejectsCancel(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
    }

    void FillUniqueData(TTestBasicRuntime& runtime, ui64 schemeshardId, const TString& tablePath, ui64& txId, bool makeDuplicates) {
        const ui32 rows = 100;
        const ui32 duplicatedRows = makeDuplicates;
        const ui32 rowsWithNulls = 8;
        const ui32 totalRows = rows + rowsWithNulls + duplicatedRows;
        const ui32 cols = 4;
        TVector<TCell> cells;
        cells.reserve(totalRows * cols);
        TDeque<TString> storage; // appending values to deque does not invalidate references
        auto addRow = [&](ui32 key, std::optional<ui32> index1, std::optional<ui32> index2, const TString& value) {
            cells.emplace_back(TCell::Make<ui32>(key));

            if (index1) {
                cells.emplace_back(TCell::Make<ui32>(*index1));
            } else {
                cells.emplace_back(TCell());
            }

            if (index2) {
                cells.emplace_back(TCell::Make<ui32>(*index2));
            } else {
                cells.emplace_back(TCell());
            }

            auto& storedValue = storage.emplace_back(value);
            cells.emplace_back(TCell(storedValue.c_str(), storedValue.size()));

            UNIT_ASSERT_VALUES_EQUAL(cells.size() % cols, 0);
        };
        ui32 i = 0;
        for (; i < rows; ++i) {
            addRow(i, i * 10, i * 10, ToString(i) * 50);
        }
        addRow(i, std::nullopt, i * 10, ToString(i) * 50); ++i;
        addRow(i, std::nullopt, i * 10, ToString(i) * 50); ++i;
        addRow(i, std::nullopt, 500, ToString(i) * 50); ++i; // intercepts with existing index2 col, but index1 is null
        addRow(i, i * 10, std::nullopt, ToString(i) * 50); ++i;
        addRow(i, i * 10, std::nullopt, ToString(i) * 50); ++i;
        addRow(i, 500, std::nullopt, ToString(i) * 50); ++i; // intercepts with existing index1 col, but index2 is null
        addRow(i, std::nullopt, std::nullopt, ToString(i) * 50); ++i;
        addRow(i, std::nullopt, std::nullopt, ToString(i) * 50); ++i;
        if (makeDuplicates) {
            addRow(42, 500, 500, "duplicated index value"); // duplicates row (50, 500, 500, "...")
        }
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), totalRows * cols);
        TSerializedCellMatrix matrix(cells, totalRows, cols);
        std::vector<ui32> colIds = {1, 2, 3, 4};
        WriteOp(runtime, schemeshardId, txId, tablePath, 0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, colIds, std::move(matrix), true);
    }

    void RejectsOnDuplicatesUniq(bool crossShardDuplicates) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        // Just create main table
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index1"   Type: "Uint32" }
              Columns { Name: "index2"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        FillUniqueData(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table", ++txId, true);

        TVector<NYdb::NTable::TGlobalIndexSettings> globalIndexSettings;
        if (crossShardDuplicates) {
            // Set explicit partitionting of index table between duplicated keys:
            // (42, 500, 500, "...")
            // (50, 500, 500, "...")
            // index impl tables has primary key: (index1, index2, key), set split point at (index1=500, index2=500, key=45)
            Ydb::Table::GlobalIndexSettings settings;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
                partition_at_keys {
                    split_points {
                        type { tuple_type {
                            elements { optional_type { item { type_id: UINT32 } } }
                            elements { optional_type { item { type_id: UINT32 } } }
                            elements { optional_type { item { type_id: UINT32 } } }
                        } }
                        value {
                            items { uint32_value: 500 }
                            items { uint32_value: 500 }
                            items { uint32_value: 45 }
                        }
                    }
                }
            )", &settings));
            globalIndexSettings.emplace_back(NYdb::NTable::TGlobalIndexSettings::FromProto(settings));
        }

        TestBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{"test_index", NKikimrSchemeOp::EIndexTypeGlobalUnique, {"index1", "index2"}, {}, globalIndexSettings});
        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6)});

        TestForgetBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 0);

        // Build a non-unique index to check that the table is correctly unlocked
        TestBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{"test_index", NKikimrSchemeOp::EIndexTypeGlobal, {"index1", "index2"}, {}, globalIndexSettings});
        auto buildIndexId2 = txId;

        env.TestWaitNotification(runtime, buildIndexId2, tenantSchemeShard);

        descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId2);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
    }

    Y_UNIT_TEST(RejectsOnDuplicatesUniq) {
        RejectsOnDuplicatesUniq(false);
    }

    Y_UNIT_TEST(RejectsOnCrossShardDuplicatesUniq) {
        RejectsOnDuplicatesUniq(true);
    }

    // Test that two nulls are not considered the same (as in SQL) in unique index validation
    Y_UNIT_TEST(NullsAreUniq) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        // Just create main table
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index1"   Type: "Uint32" }
              Columns { Name: "index2"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        FillUniqueData(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table", ++txId, false);

        TestBuildUniqIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "test_index", {"index1", "index2"});
        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});
    }
}


// Tests for issue #35458: hung index build scans should be restarted after datashard reboot.
// The datashard persists TEvBuildIndexCreateRequest on receipt; on reboot it sends ABORTED
// back to schemeshard which then re-sends TEvBuildIndexCreateRequest to restart the scan.
Y_UNIT_TEST_SUITE(IndexBuildHungScanRestartTests) {

    // When a datashard reboots during an index build scan, the persisted scan request
    // is found on startup and ABORTED is sent to schemeshard. Schemeshard then retries.
    Y_UNIT_TEST(HungScanIsRestarted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "index" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        WriteRows(runtime, TTestTxConfig::FakeHiveTablets, 1, 100);

        // Count how many scan create requests are sent to detect restarts.
        ui32 scanCreateRequestCount = 0;
        auto createRequestObserver = runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>(
            [&](TEvDataShard::TEvBuildIndexCreateRequest::TPtr&) {
                ++scanCreateRequestCount;
            });

        AsyncBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table",
            TBuildIndexConfig{"index1", NKikimrSchemeOp::EIndexTypeGlobal, {"index"}, {}, {}});
        const ui64 buildIndexId = txId;

        // Wait for the initial scan request to arrive at datashard.
        runtime.WaitFor("initial scan request", [&] { return scanCreateRequestCount >= 1; });
        UNIT_ASSERT_GE(scanCreateRequestCount, 1);

        // Reboot the datashard to simulate a crash mid-scan.
        // On reboot, the datashard finds the persisted scan request and sends ABORTED
        // to schemeshard, which then retries by sending a new TEvBuildIndexCreateRequest.
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::FakeHiveTablets, sender);

        // After reboot, schemeshard should have sent a second TEvBuildIndexCreateRequest.
        runtime.WaitFor("scan restart after reboot", [&] { return scanCreateRequestCount >= 2; });
        UNIT_ASSERT_C(scanCreateRequestCount >= 2,
            "Schemeshard did not restart the scan after datashard reboot (got "
            << scanCreateRequestCount << " scan create requests, expected at least 2). "
            "This demonstrates the bug from issue #35458: scans after reboot are never restarted.");

        env.TestWaitNotification(runtime, buildIndexId);

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
        UNIT_ASSERT_VALUES_EQUAL((ui64)descr.GetIndexBuild().GetState(),
            (ui64)Ydb::Table::IndexBuildState::STATE_DONE);
    }

    // Same as above but with two shards: only one reboots, the other finishes normally.
    // This verifies the reboot-restart mechanism works correctly in a multi-shard scenario.
    Y_UNIT_TEST(HungScanIsRestartedAfterDatashardReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "index" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8"   }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        for (ui32 delta = 0; delta < 2; ++delta) {
            WriteRows(runtime, TTestTxConfig::FakeHiveTablets + delta, 1 + delta, 100 + delta);
        }

        ui32 restartCount = 0;
        auto createRequestObserver = runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>(
            [&](TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev) {
                if (ev->Get()->Record.GetTabletId() == TTestTxConfig::FakeHiveTablets) {
                    ++restartCount;
                }
            });

        AsyncBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table",
            TBuildIndexConfig{"index1", NKikimrSchemeOp::EIndexTypeGlobal, {"index"}, {}, {}});
        const ui64 buildIndexId = txId;

        // Wait for the scan request to reach the first datashard.
        runtime.WaitFor("initial scan request to first shard", [&] { return restartCount >= 1; });

        // Reboot the first datashard mid-scan.
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::FakeHiveTablets, sender);

        // After reboot, schemeshard should retry the first shard.
        runtime.WaitFor("scan restart after reboot", [&] { return restartCount >= 2; });
        UNIT_ASSERT_C(restartCount >= 2,
            "Schemeshard did not restart the scan on shard "
            << TTestTxConfig::FakeHiveTablets << " after reboot (got "
            << restartCount << " requests, expected at least 2). "
            "This demonstrates the bug from issue #35458.");

        env.TestWaitNotification(runtime, buildIndexId);

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
        UNIT_ASSERT_VALUES_EQUAL((ui64)descr.GetIndexBuild().GetState(),
            (ui64)Ydb::Table::IndexBuildState::STATE_DONE);
    }
}
