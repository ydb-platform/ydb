#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/metering/metering.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE (VectorIndexBuildTest) {
    Y_UNIT_TEST (BaseCase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot",
                               "Name: \"ResourceDB\"");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot",
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"pool-kind-1\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"pool-2\" "
                              "  Kind: \"pool-kind-2\" "
                              "} "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              "Name: \"ResourceDB\"");
        env.TestWaitNotification(runtime, txId);

        const auto attrs = AlterUserAttrs({
            {"cloud_id", "CLOUD_ID_VAL"},
            {"folder_id", "FOLDER_ID_VAL"},
            {"database_id", "DATABASE_ID_VAL"},
        });

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "ServerLessDB"
            ResourcesDomainKey {
                SchemeShard: %lu
                PathId: 2
            }
        )", TTestTxConfig::SchemeShard), attrs);
        env.TestWaitNotification(runtime, txId);

        TString alterData = TStringBuilder()
                            << "PlanResolution: 50 "
                            << "Coordinators: 1 "
                            << "Mediators: 1 "
                            << "TimeCastBucketsPerMediator: 2 "
                            << "ExternalSchemeShard: true "
                            << "ExternalHive: false "
                            << "Name: \"ServerLessDB\" "
                            << "StoragePools { "
                            << "  Name: \"pool-1\" "
                            << "  Kind: \"pool-kind-1\" "
                            << "} ";
        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", alterData);
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLessDB"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("ServerLessDB"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        // Just create main table
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"       Type: "Uint32" }
              Columns { Name: "embedding" Type: "String" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&](ui64 tabletId, ui32 key, TString embedding, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key       (Uint32 '%u ) ) ) )
                    (let row   '( '('embedding (String '%s ) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, embedding.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        };
        for (ui32 key = 0; key < 200; ++key) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, key, std::to_string(key), "Table");
        }

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        TStringBuilder meteringMessages;
        auto observerHolder = runtime.AddObserver<NMetering::TEvMetering::TEvWriteMeteringJson>([&](NMetering::TEvMetering::TEvWriteMeteringJson::TPtr& event) {
            Cerr << "grabMeteringMessage has happened" << Endl;
            meteringMessages << event->Get()->MeteringJson;
        });

        TestBuildVectorIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "index1", "embedding");
        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        const TString meteringData = R"({"usage":{"start":0,"quantity":128,"finish":0,"unit":"request_unit","type":"delta"},"tags":{},"id":"106-72075186233409549-2-0-0-0-0-200-0-1290-0","cloud_id":"CLOUD_ID_VAL","source_wt":0,"source_id":"sless-docapi-ydb-ss","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.requests.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})""\n";

        UNIT_ASSERT_NO_DIFF(meteringMessages, meteringData);

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
              DropColumns { Name: "embedding" }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "embedding"   Type: "String" }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestBuildVectorIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "index2", "embedding");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // CommonDB
        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot",
                               "Name: \"CommonDB\"");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot",
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
              Columns { Name: "key"       Type: "Uint32" }
              Columns { Name: "embedding" Type: "String" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        for (ui32 key = 100; key < 300; ++key) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, key, std::to_string(key), "Table");
        }

        TVector<TString> billRecords;
        observerHolder = runtime.AddObserver<NMetering::TEvMetering::TEvWriteMeteringJson>([&](NMetering::TEvMetering::TEvWriteMeteringJson::TPtr& event) {
            billRecords.push_back(event->Get()->MeteringJson);
        });

        TestBuildVectorIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/CommonDB", "/MyRoot/CommonDB/Table", "index1", "embedding");
        buildIndexId = txId;

        listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB");
        UNIT_ASSERT_VALUES_EQUAL(listing.EntriesSize(), 1);

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB", txId);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        UNIT_ASSERT_VALUES_EQUAL(billRecords.size(), 0);
    }

    Y_UNIT_TEST(VectorIndexDescriptionIsPersisted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "covered" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        NYdb::NTable::TGlobalIndexSettings globalIndexSettings;
        {
            Ydb::Table::GlobalIndexSettings proto;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
                partition_at_keys {
                    split_points {
                        type { tuple_type { elements { optional_type { item { type_id: UINT64 } } } } }
                        value { items { uint64_value: 12345 } }
                    }
                    split_points {
                        type { tuple_type { elements { optional_type { item { type_id: UINT64 } } } } }
                        value { items { uint64_value: 54321 } }
                    }
                }
                partitioning_settings {
                    min_partitions_count: 3
                    max_partitions_count: 3
                }
            )", &proto));
            globalIndexSettings = NYdb::NTable::TGlobalIndexSettings::FromProto(proto);
        }

        std::unique_ptr<NYdb::NTable::TKMeansTreeSettings> kmeansTreeSettings;
        {
            Ydb::Table::KMeansTreeSettings proto;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
                settings {
                    metric: DISTANCE_COSINE
                    vector_type: VECTOR_TYPE_FLOAT
                    vector_dimension: 1024
                }
                levels: 5
                clusters: 4
            )", &proto));
            using T = NYdb::NTable::TKMeansTreeSettings;
            kmeansTreeSettings = std::make_unique<T>(T::FromProto(proto));
        }

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> indexCreationBlocker(runtime, [](const auto& ev) {
            const auto& modifyScheme = ev->Get()->Record.GetTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexBuild;
        });

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", TBuildIndexConfig{
            "by_embedding", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, { "embedding" }, { "covered" },
            { globalIndexSettings, globalIndexSettings }, std::move(kmeansTreeSettings)
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        indexCreationBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexTx);

        auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            buildIndexOperation.DebugString()
        );

        using namespace NKikimr::NTableIndex::NTableVectorKmeansTreeIndex;
        TestDescribeResult(DescribePrivatePath(runtime, JoinFsPaths("/MyRoot/vectors/by_embedding", LevelTable), true, true), {
            NLs::IsTable,
            NLs::PartitionCount(3),
            NLs::MinPartitionsCountEqual(3),
            NLs::MaxPartitionsCountEqual(3),
            NLs::SplitBoundaries<ui64>({12345, 54321})
        });
        TestDescribeResult(DescribePrivatePath(runtime, JoinFsPaths("/MyRoot/vectors/by_embedding", PostingTable), true, true), {
            NLs::IsTable,
            NLs::PartitionCount(3),
            NLs::MinPartitionsCountEqual(3),
            NLs::MaxPartitionsCountEqual(3),
            NLs::SplitBoundaries<ui64>({12345, 54321})
        });

        for (size_t i = 0; i != 3; ++i) {
            if (i != 0) {
                // check that specialized index description persisted even after reboot
                RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            }
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/by_embedding"), {
                NLs::PathExist,
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
                NLs::IndexKeys({"embedding"}),
                NLs::IndexDataColumns({"covered"}),
                NLs::KMeansTreeDescription(
                    Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                    Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                    1024,
                    4,
                    5
                )
            });
        }
    }
}
