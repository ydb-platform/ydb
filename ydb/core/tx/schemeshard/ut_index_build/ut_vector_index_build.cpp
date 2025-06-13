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
    // TODO: this is awful why do we check everything here
    Y_UNIT_TEST(PleaseSplitMeOnMultipleTestsAndDoNotUseServerless) {
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
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", false, 0, 0, 50);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", false, 1, 50, 150);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", false, 2, 150, 200);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        TBlockEvents<TEvDataShard::TEvReshuffleKMeansRequest> reshuffleBlocker(runtime, [&](const auto& ) {
            return true;
        });

        ui64 buildIndexTx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "index1", "embedding");

        // Wait for the first "reshuffle" request (samples will be already collected on the first level)
        // and reboot the scheme shard to verify that its intermediate state is persisted correctly.
        // The bug checked here: Sample.Probability was not persisted (#18236).
        runtime.WaitFor("ReshuffleKMeansRequest", [&]{ return reshuffleBlocker.size(); });
        Cerr << "... rebooting scheme shard" << Endl;
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        // Now wait for the 1st level to be finalized
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> level1Blocker(runtime, [&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable) {
                return true;
            }
            return false;
        });
        reshuffleBlocker.Stop().Unblock();

        // Reshard the first level table (0build)
        // First bug checked here: after restarting the schemeshard during reshuffle it
        //   generates more clusters than requested and dies with VERIFY on shard boundaries (#18278).
        // Second bug checked here: posting table doesn't contain all rows from the main table
        //   when the build table is resharded during build (#18355).
        {
            auto indexDesc = DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build", true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 4);
            ui64 cluster = 1;
            for (const auto & x: parts) {
                TestSplitTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build", Sprintf(R"(
                    SourceTabletId: %lu
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: %lu } } Tuple { Optional { Uint32: 50 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: %lu } } Tuple { Optional { Uint32: 150 } } } }
                )", x.GetDatashardId(), cluster, cluster));
                env.TestWaitNotification(runtime, txId);
                cluster++;
            }
        }

        level1Blocker.Stop().Unblock();

        // Now wait for the index build
        {
            auto expectedStatus = Ydb::StatusIds::SUCCESS;
            TAutoPtr<IEventHandle> handle;
            TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
            UNIT_ASSERT(event);

            Cerr << "BUILDINDEX RESPONSE CREATE: " << event->ToString() << Endl;
            UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), expectedStatus,
                                "status mismatch"
                                    << " got " << Ydb::StatusIds::StatusCode_Name(event->Record.GetStatus())
                                    << " expected "  << Ydb::StatusIds::StatusCode_Name(expectedStatus)
                                    << " issues was " << event->Record.GetIssues());
        }

        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);

        // Check row count in the posting table
        {
            auto rows = CountRows(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable");
            Cerr << "... posting table contains " << rows << " rows" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(rows, 200);
        }

        auto buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 1);

        auto buildIndexOperation = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL(buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1", true, true, true),
                           {NLs::PathExist,
                            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

        TestForgetBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexTx);
        buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 0);

        TestDropTableIndex(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            TableName: "Table"
            IndexName: "index1"
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        Cerr << "... rebooting scheme shard" << Endl;
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
    }

    Y_UNIT_TEST(CommonDB) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", "Name: \"CommonDB\"");
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

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/CommonDB"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("CommonDB"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/CommonDB", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/CommonDB/Table", false, 0, 100, 300);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/CommonDB/Table"), {
            NLs::PathExist,
            NLs::IndexesCount(0),
            NLs::PathVersionEqual(3)});

        TBlockEvents<NMetering::TEvMetering::TEvWriteMeteringJson> meteringBlocker(runtime, [&](const auto& ev) {
            Cerr << "TEvWriteMeteringJson " << ev->Get()->MeteringJson << Endl;
            return true;
        });

        // Initiate index build:
        ui64 buildIndexTx = ++txId;
        TestBuildVectorIndex(runtime, buildIndexTx, tenantSchemeShard, "/MyRoot/CommonDB", "/MyRoot/CommonDB/Table", "index1", "embedding");
        {
            auto buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB");
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 1);
            auto buildIndexOperation = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_PREPARING);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/CommonDB/Table"), {
                NLs::PathExist,
                NLs::IndexesCount(0),
                NLs::PathVersionEqual(3)});
        }

        // Wait and check Filling state:
        TBlockEvents<TEvDataShard::TEvSampleKResponse> sampleKBlocker(runtime, [&](const auto&) {
            return true;
        });
        runtime.WaitFor("sampleK", [&]{ return sampleKBlocker.size(); });
        sampleKBlocker.Stop().Unblock();
        {
            auto buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB");
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 1);
            auto buildIndexOperation = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/CommonDB/Table"), {
                NLs::PathExist,
                NLs::IndexesCount(1),
                NLs::PathVersionEqual(4)});
            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/CommonDB/Table/index1", true, true, true), {
                NLs::PathExist,
                NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateWriteOnly)});
        }

        // Wait Done state:
        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);
        {
            auto buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB");
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 1);
            auto buildIndexOperation = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/CommonDB", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/CommonDB/Table"), {
                NLs::PathExist,
                NLs::IndexesCount(1),
                NLs::PathVersionEqual(6)});
            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/CommonDB/Table/index1", true, true, true), {
                NLs::PathExist,
                NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});
        }

        UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 0);
    }

    Y_UNIT_TEST_FLAG(ServerLessDB, smallScanBuffer) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", false, 0, 0, 50);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", false, 1, 50, 150);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", false, 2, 150, 200);

        TBlockEvents<NMetering::TEvMetering::TEvWriteMeteringJson> meteringBlocker(runtime, [&](const auto& ev) {
            Cerr << "TEvWriteMeteringJson " << ev->Get()->MeteringJson << Endl;
            return true;
        });

        ui64 uploadRows = 0, uploadBytes = 0, readRows = 0, readBytes = 0;
        auto logBillingStats = [&]() {
            Cerr << "BillingStats:"
                << " uploadRows: " << uploadRows << " uploadBytes: " << uploadBytes
                << " readRows: " << readRows << " readBytes: " << readBytes
                << Endl;
        };

        TBlockEvents<TEvDataShard::TEvSampleKResponse> sampleKBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            readRows += response.GetReadRows();
            readBytes += response.GetReadBytes();
            return true;
        });

        TBlockEvents<TEvIndexBuilder::TEvUploadSampleKResponse> uploadSampleKBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            uploadRows += response.GetUploadRows();
            uploadBytes += response.GetUploadBytes();
            return true;
        });

        TBlockEvents<TEvDataShard::TEvReshuffleKMeansResponse> reshuffleBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            uploadRows += response.GetUploadRows();
            uploadBytes += response.GetUploadBytes();
            readRows += response.GetReadRows();
            readBytes += response.GetReadBytes();
            return true;
        });

        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> localKMeansBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            uploadRows += response.GetUploadRows();
            uploadBytes += response.GetUploadBytes();
            readRows += response.GetReadRows();
            readBytes += response.GetReadBytes();
            return true;
        });

        // Build vector index with max_shards_in_flight(1) to guarantee deterministic metering data
        ui64 buildIndexTx = ++txId;
        {
            auto sender = runtime.AllocateEdgeActor();
            auto request = CreateBuildIndexRequest(buildIndexTx, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{
                "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}
            });
            auto settings = request->Record.MutableSettings();
            settings->set_max_shards_in_flight(1);
            if (smallScanBuffer) {
                settings->MutableScanSettings()->SetMaxBatchRows(1);
            } else {
                settings->MutableScanSettings()->ClearMaxBatchRows();
            }
            auto kmeansSettings = request->Record.MutableSettings()->mutable_index()->Mutableglobal_vector_kmeans_tree_index();
            kmeansSettings->Mutablevector_settings()->Setlevels(2);
            kmeansSettings->Mutablevector_settings()->Setclusters(4);
            ForwardToTablet(runtime, tenantSchemeShard, sender, request);
        }

        for (ui32 shard = 0; shard < 3; shard++) {
            runtime.WaitFor("sampleK", [&]{ return sampleKBlocker.size(); });
            sampleKBlocker.Unblock();
        }
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(uploadRows, 0);
        UNIT_ASSERT_VALUES_EQUAL(uploadBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 200);
        UNIT_ASSERT_VALUES_EQUAL(readBytes, 1800);

        runtime.WaitFor("uploadSampleK", [&]{ return uploadSampleKBlocker.size(); });
        uploadSampleKBlocker.Unblock();
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(uploadRows, 4);
        UNIT_ASSERT_VALUES_EQUAL(uploadBytes, 148);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 200);
        UNIT_ASSERT_VALUES_EQUAL(readBytes, 1800);
        
        runtime.WaitFor("metering", [&]{ return meteringBlocker.size(); });
        {
            // id format: [billed uploadRows-readRows-uploadBytes-readBytes] [processed uploadRows-readRows-uploadBytes-readBytes]
            auto expectedBill = TBillRecord()
                .Id("109-72075186233409549-2-0-0-0-0-4-200-148-1800")
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(10))
                .Usage(TBillRecord::RequestUnits(130, TInstant::Seconds(0), TInstant::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
        }
        meteringBlocker.Unblock();

        for (ui32 shard = 0; shard < 3; shard++) {
            runtime.WaitFor("reshuffle", [&]{ return reshuffleBlocker.size(); });
            reshuffleBlocker.Unblock();
        }
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(uploadRows, 204);
        UNIT_ASSERT_VALUES_EQUAL(uploadBytes, 6748);
        UNIT_ASSERT_VALUES_EQUAL(readRows, 400);
        UNIT_ASSERT_VALUES_EQUAL(readBytes, 3600);

        for (ui32 shard = 0; shard < 4; shard++) {
            runtime.WaitFor("localKMeans", [&]{ return localKMeansBlocker.size(); });
            localKMeansBlocker.Unblock();
        }
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(uploadRows, 420);
        UNIT_ASSERT_VALUES_EQUAL(uploadBytes, 11740);
        if (smallScanBuffer) {
            UNIT_ASSERT_VALUES_EQUAL(readRows, 1400); // SAMPLE + KMEANS * 3 + UPLOAD = 5 scans
            UNIT_ASSERT_VALUES_EQUAL(readBytes, 20600);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(readRows, 600);
            UNIT_ASSERT_VALUES_EQUAL(readBytes, 7000);
        }

        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);
        {
            auto buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 1);
            auto buildIndexOperation = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL(buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"), {
                NLs::PathExist,
                NLs::IndexesCount(1),
                NLs::PathVersionEqual(6)});
            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1", true, true, true), {
                NLs::PathExist,
                NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});
        }

        runtime.WaitFor("metering", [&]{ return meteringBlocker.size(); });
        {
            // id format: [billed uploadRows-readRows-uploadBytes-readBytes] [processed uploadRows-readRows-uploadBytes-readBytes]
            auto expectedBill = TBillRecord()
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(10))
                .Usage(TBillRecord::RequestUnits(336, TInstant::Seconds(10), TInstant::Seconds(10)));
            if (smallScanBuffer) {
                expectedBill.Id("109-72075186233409549-2-4-200-148-1800-420-1400-11740-20600");
            } else {
                expectedBill.Id("109-72075186233409549-2-4-200-148-1800-420-600-11740-7000");
            }
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
        }
        meteringBlocker.Stop().Unblock();
    }

    Y_UNIT_TEST_FLAG(DescriptionIsPersisted, prefixed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix" Type: "Uint64" }
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
        const TVector<TString> indexColumns = prefixed
            ? TVector<TString>{"prefix", "embedding"}
            : TVector<TString>{"embedding"};
        const TVector<TString> dataColumns = { "covered" };
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", TBuildIndexConfig{
            "by_embedding", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, indexColumns, dataColumns,
            { globalIndexSettings, globalIndexSettings, globalIndexSettings }, std::move(kmeansTreeSettings)
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
        if (prefixed) {
        TestDescribeResult(DescribePrivatePath(runtime, JoinFsPaths("/MyRoot/vectors/by_embedding", PrefixTable), true, true), {
            NLs::IsTable,
            NLs::PartitionCount(3),
            NLs::MinPartitionsCountEqual(3),
            NLs::MaxPartitionsCountEqual(3),
            NLs::SplitBoundaries<ui64>({12345, 54321})
        });
        }

        for (size_t i = 0; i != 3; ++i) {
            if (i != 0) {
                // check that specialized index description persisted even after reboot
                RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            }
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/by_embedding"), {
                NLs::PathExist,
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
                NLs::IndexKeys(indexColumns),
                NLs::IndexDataColumns(dataColumns),
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

    Y_UNIT_TEST(TTxReply_DoExecute_Throws) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        NYdb::NTable::TGlobalIndexSettings globalIndexSettings;

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

        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> blocked(runtime, [&](auto& ev) {
            ev->Get()->Record.SetRequestSeqNoRound(999);
            return true;
        });

        const ui64 buildIndexTx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", "embedding");

        runtime.WaitFor("block", [&]{ return blocked.size(); });
        blocked.Stop().Unblock();

        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Condition violated: `actualSeqNo > recordSeqNo");
        }

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Unhandled exception");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Condition violated: `actualSeqNo > recordSeqNo");
        }
    }

    Y_UNIT_TEST(TTxProgress_Throws) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        NYdb::NTable::TGlobalIndexSettings globalIndexSettings;

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

        const ui64 buildIndexTx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", "embedding");

        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

        { // set 'Invalid' state
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('Id (Uint64 '%lu)) ) )
                    (let value '('('State (Uint32 '0)) ) )
                    (return (AsList (UpdateRow 'IndexBuild key value) ))
                )
            )", buildIndexTx);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        }

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_UNSPECIFIED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Unhandled exception");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Unreachable");
        }
    }

    Y_UNIT_TEST(TTxInit_Throws) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        NYdb::NTable::TGlobalIndexSettings globalIndexSettings;

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

        const ui64 buildIndexTx = ++txId;
        const TVector<TString> dataColumns;
        const TVector<TString> indexColumns{"embedding"};
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", TBuildIndexConfig{
            "by_embedding", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, indexColumns, dataColumns,
            { globalIndexSettings, globalIndexSettings, globalIndexSettings }, std::move(kmeansTreeSettings)
        });

        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

        {
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('Id (Uint64 '%lu)) ) )
                    (let value '('('CreationConfig (String 'aaaaaaaa)) ) )
                    (return (AsList (UpdateRow 'IndexBuild key value) ))
                )
            )", buildIndexTx);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        }

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Init IndexBuild unhandled exception");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Condition violated: `creationConfig.ParseFromString");
        }
    }

    Y_UNIT_TEST(Shard_Build_Error) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        NYdb::NTable::TGlobalIndexSettings globalIndexSettings;

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

        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> blocked(runtime, [&](auto& ev) {
            ev->Get()->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
            auto issue = ev->Get()->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message("Datashard test fail");
            return true;
        });

        const ui64 buildIndexTx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", "embedding");

        runtime.WaitFor("block", [&]{ return blocked.size(); });
        blocked.Stop().Unblock();

        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            Cout << "BuildIndex 1 " << buildIndexOperation.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "One of the shards report BUILD_ERROR");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Error: Datashard test fail");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Processed: { upload rows: 0, upload bytes: 0, read rows: 0, read bytes: 0 } }");
        }

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            Cout << "BuildIndex 2 " << buildIndexOperation.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "One of the shards report BUILD_ERROR");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Error: Datashard test fail");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Processed: { upload rows: 0, upload bytes: 0, read rows: 0, read bytes: 0 } }");
        }
    }
}
