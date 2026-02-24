#include <ydb/public/lib/deprecated/kicli/kicli.h>
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

namespace {
    // to check cpu converted to request units it should be big enough
    const ui64 CpuTimeUsMultiplier = 150;

    template<class TEvType>
    bool MakeCpuMeteringDeterministic(const TEvType& ev) {
        auto stats = ev->Get()->Record.MutableMeteringStats();
        UNIT_ASSERT(stats->HasCpuTimeUs());
        stats->SetCpuTimeUs((stats->GetReadRows() + stats->GetUploadRows()) * CpuTimeUsMultiplier);
        return false;
    }

    auto MakeCpuMeteringDeterministic(TTestBasicRuntime& runtime) {
        return std::make_tuple(
            MakeHolder<TBlockEvents<TEvDataShard::TEvSampleKResponse>>(runtime, [&](const auto& ev) {
                return MakeCpuMeteringDeterministic(ev);
            }),
            MakeHolder<TBlockEvents<TEvIndexBuilder::TEvUploadSampleKResponse>>(runtime, [&](const auto& ev) {
                // special internal Scheme Shard event, no cpu, but AddRead/AddUpload helpers will fix it
                auto stats = ev->Get()->Record.MutableMeteringStats();
                UNIT_ASSERT(!stats->HasCpuTimeUs() || stats->GetCpuTimeUs() == (stats->GetReadRows() + stats->GetUploadRows()) * CpuTimeUsMultiplier);
                stats->SetCpuTimeUs((stats->GetReadRows() + stats->GetUploadRows()) * CpuTimeUsMultiplier);
                return false;
            }),
            MakeHolder<TBlockEvents<TEvDataShard::TEvRecomputeKMeansResponse>>(runtime, [&](const auto& ev) {
                return MakeCpuMeteringDeterministic(ev);
            }),
            MakeHolder<TBlockEvents<TEvDataShard::TEvReshuffleKMeansResponse>>(runtime, [&](const auto& ev) {
                return MakeCpuMeteringDeterministic(ev);
            }),
            MakeHolder<TBlockEvents<TEvDataShard::TEvLocalKMeansResponse>>(runtime, [&](const auto& ev) {
                return MakeCpuMeteringDeterministic(ev);
            })
        );
    }

    auto ZeroMeteringCpuTimeUs(TTestBasicRuntime& runtime) {
        return std::make_tuple(
            MakeHolder<TBlockEvents<TEvDataShard::TEvSampleKResponse>>(runtime, [&](const auto& ev) {
                ev->Get()->Record.MutableMeteringStats()->SetCpuTimeUs(0);
                return false;
            }),
            MakeHolder<TBlockEvents<TEvIndexBuilder::TEvUploadSampleKResponse>>(runtime, [&](const auto& ev) {
                ev->Get()->Record.MutableMeteringStats()->SetCpuTimeUs(0);
                return false;
            }),
            MakeHolder<TBlockEvents<TEvDataShard::TEvRecomputeKMeansResponse>>(runtime, [&](const auto& ev) {
                ev->Get()->Record.MutableMeteringStats()->SetCpuTimeUs(0);
                return false;
            }),
            MakeHolder<TBlockEvents<TEvDataShard::TEvReshuffleKMeansResponse>>(runtime, [&](const auto& ev) {
                ev->Get()->Record.MutableMeteringStats()->SetCpuTimeUs(0);
                return false;
            }),
            MakeHolder<TBlockEvents<TEvDataShard::TEvLocalKMeansResponse>>(runtime, [&](const auto& ev) {
                ev->Get()->Record.MutableMeteringStats()->SetCpuTimeUs(0);
                return false;
            })
        );
    }

    void AddRead(TMeteringStats& stats, ui64 rows, ui64 bytes) {
        stats.SetReadRows(stats.GetReadRows() + rows);
        stats.SetReadBytes(stats.GetReadBytes() + bytes);
        stats.SetCpuTimeUs(stats.GetCpuTimeUs() + rows * CpuTimeUsMultiplier); // see MakeCpuMeteringDeterministic
    }

    void AddUpload(TMeteringStats& stats, ui64 rows, ui64 bytes) {
        stats.SetUploadRows(stats.GetUploadRows() + rows);
        stats.SetUploadBytes(stats.GetUploadBytes() + bytes);
        stats.SetCpuTimeUs(stats.GetCpuTimeUs() + rows * CpuTimeUsMultiplier); // see MakeCpuMeteringDeterministic
    }
}

Y_UNIT_TEST_SUITE(VectorIndexBuildTest) {
    Y_UNIT_TEST(CreateAndDrop) {
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
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 200);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(0), NLs::PathVersionEqual(3)});

        ui64 buildIndexTx = ++txId;
        TestBuildVectorIndex(runtime, buildIndexTx, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "index1", {"embedding"});
        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);

        auto buildIndexOperations = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL(buildIndexOperations.EntriesSize(), 1);

        auto buildIndexOperation = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL(buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(1), NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1", true, true, true),
            {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

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
            {NLs::PathExist, NLs::IndexesCount(0), NLs::PathVersionEqual(8)});
    }

    Y_UNIT_TEST(RecreatedColumns) {
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
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

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

        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 200, {1, 5, 3, 4});

        TestBuildVectorIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "index2", {"embedding"});
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(1), NLs::PathVersionEqual(8)});
    }

    Y_UNIT_TEST_FLAG(SimpleDuplicates, Overlap) {
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
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 50);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 1, 50, 150);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 2, 150, 200);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(0), NLs::PathVersionEqual(3)});

        std::unique_ptr<TBlockEvents<TEvDataShard::TEvReshuffleKMeansRequest>> reshuffleBlocker;
        std::unique_ptr<TBlockEvents<TEvDataShard::TEvFilterKMeansRequest>> filterBlocker;
        if (Overlap) {
            filterBlocker = std::make_unique<TBlockEvents<TEvDataShard::TEvFilterKMeansRequest>>(runtime, [&](const auto& ) {
                return true;
            });
        } else {
            reshuffleBlocker = std::make_unique<TBlockEvents<TEvDataShard::TEvReshuffleKMeansRequest>>(runtime, [&](const auto& ) {
                return true;
            });
        }

        ui64 buildIndexTx = ++txId;
        auto sender = runtime.AllocateEdgeActor();
        auto request = CreateBuildIndexRequest(buildIndexTx, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{
            "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {}
        });
        if (Overlap) {
            auto kmeansSettings = request->Record.MutableSettings()->mutable_index()->mutable_global_vector_kmeans_tree_index();
            kmeansSettings->mutable_vector_settings()->set_overlap_clusters(2);
        }
        ForwardToTablet(runtime, tenantSchemeShard, sender, request);

        if (Overlap) {
            runtime.WaitFor("FilterKMeansRequest", [&]{ return filterBlocker->size(); });
        } else {
            // Wait for the first "reshuffle" request (samples will be already collected on the first level)
            // and reboot the scheme shard to verify that its intermediate state is persisted correctly.
            // The bug checked here: Sample.Probability was not persisted (#18236).
            runtime.WaitFor("ReshuffleKMeansRequest", [&]{ return reshuffleBlocker->size(); });
            Cerr << "... rebooting scheme shard" << Endl;
            RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        }

        // Now wait for the 1st level to be finalized
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> level1Blocker(runtime, [&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable) {
                return true;
            }
            return false;
        });
        if (Overlap) {
            filterBlocker->Stop().Unblock();
        } else {
            reshuffleBlocker->Stop().Unblock();
        }

        // Reshard the first level table (0build)
        // First bug checked here: after restarting the schemeshard during reshuffle it
        //   generates more clusters than requested and dies with VERIFY on shard boundaries (#18278).
        // Second bug checked here: posting table doesn't contain all rows from the main table
        //   if the build table was resharded during build (#18355).
        // Third bug checked here: build with overlap was skipping sample collection on levels > 1
        //   and 0 clusters were generated if the build table was resharded.
        {
            const char *buildTable = Overlap
                ? "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable1build"
                : "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build";
            auto indexDesc = DescribePath(runtime, tenantSchemeShard, buildTable, true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 4);
            ui64 cluster = 1;
            for (const auto & x: parts) {
                TestSplitTable(runtime, tenantSchemeShard, ++txId, buildTable, Sprintf(R"(
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
        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(1), NLs::PathVersionEqual(6)});

        // Check row count in the posting table
        {
            auto rows = CountRows(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable");
            Cerr << "... posting table contains " << rows << " rows" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(rows, (Overlap ? 400 : 200));
        }
    }

    Y_UNIT_TEST(PrefixedDuplicates) {
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
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 50);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 1, 50, 150);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 2, 150, 200);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(0), NLs::PathVersionEqual(3)});

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> lockBlocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock &&
                tx.GetLockConfig().GetName() == "indexImplPostingTable0build") {
                return true;
            }
            return false;
        });

        // Build vector index with max_shards_in_flight > 1 to guarantee double upload of the same shard
        const ui64 buildIndexId = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", "index1", {"prefix", "embedding"});

        // Wait for the "lock" request
        runtime.WaitFor("LockBuildRequest", [&]{ return lockBlocker.size(); });
        lockBlocker.Stop();

        // Reshard the first level secondary-index-like prefix table (0build)
        // Force out-of-order shard indexes (1, 3, 2)
        {
            auto indexDesc = DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build", true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 1);
            TestSplitTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 10 } } Tuple { Optional { Uint32: 100 } } } }
            )", parts[0].GetDatashardId()));
            env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        }
        {
            auto indexDesc = DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build", true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 2);
            TestSplitTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable0build", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 5 } } Tuple { Optional { Uint32: 100 } } } }
            )", parts[0].GetDatashardId()));
            env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        }

        int prefixSeen = 0;
        TBlockEvents<TEvDataShard::TEvPrefixKMeansRequest> prefixBlocker(runtime, [&](const auto& ) {
            return (++prefixSeen) == 2;
        });
        TBlockEvents<TEvDataShard::TEvPrefixKMeansResponse> prefixResponseBlocker(runtime, [&](const auto& ) {
            return true;
        });

        lockBlocker.Unblock(lockBlocker.size());

        // Wait for the first scan to finish to prevent it from aborting on split
        // Wait for the second PrefixKMeansRequest and reboot the scheme shard
        runtime.WaitFor("Second PrefixKMeansRequest", [&]{ return prefixBlocker.size() && prefixResponseBlocker.size(); });
        Cerr << "... rebooting scheme shard" << Endl;
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        prefixResponseBlocker.Stop();
        prefixResponseBlocker.Unblock(prefixResponseBlocker.size());
        prefixBlocker.Stop();
        prefixBlocker.Unblock(prefixBlocker.size());

        // Now wait for the index build
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(1), NLs::PathVersionEqual(6)});

        // Check row count in the posting table
        {
            auto rows = CountRows(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable");
            Cerr << "... posting table contains " << rows << " rows" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(rows, 200);
        }
    }

    Y_UNIT_TEST(Metering_Documentation_Formula) {
        for (ui32 levels : xrange(1, 10)) {
            for (ui64 dataSizeMB : {1500, 500, 100, 0}) {
                const ui64 rowCount = 500'000;

                TMeteringStats stats;
                stats.SetReadRows(5 * levels * rowCount);
                stats.SetReadBytes(5 * levels * dataSizeMB * 1_MB);
                stats.SetUploadRows(levels * rowCount);
                stats.SetUploadBytes(levels * dataSizeMB * 1_MB);

                TString explain;
                const ui64 result = TRUCalculator::Calculate(stats, explain);

                // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
                // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
                UNIT_ASSERT_VALUES_EQUAL_C(result, levels * Max<ui64>(dataSizeMB * 1152, dataSizeMB * 640 + rowCount * 0.5), explain);
            }
        }
    }

    Y_UNIT_TEST_FLAG(Metering_Documentation_Formula_Build, smallRows) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);
        auto zeroMeteringCpuTimeUs = ZeroMeteringCpuTimeUs(runtime);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        const ui32 levels = 3;
        const ui32 tableRows = 1500;
        const ui32 tableRowDimension = smallRows ? 500 : 5000;
        const ui64 tableRowBytes = 4 + (tableRowDimension + 1); // key:Uint32 (4 bytes), embedding:String (vector_dimension + 1 bytes)
        const ui64 tableBytes = tableRows * tableRowBytes;
        for (ui32 rowId = 0; rowId < tableRows; rowId += 100) { // batching
            WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, rowId, rowId + 100, {}, tableRowDimension);
        }

        // Build vector index with max_shards_in_flight(1) to guarantee deterministic metering data
        ui64 buildIndexTx = ++txId;
        {
            auto sender = runtime.AllocateEdgeActor();
            auto request = CreateBuildIndexRequest(buildIndexTx, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{
                "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {}
            });
            auto settings = request->Record.MutableSettings();
            settings->set_max_shards_in_flight(1);
            settings->MutableScanSettings()->SetMaxBatchRows(1); // the worst case with small buffer size
            auto kmeansSettings = request->Record.MutableSettings()->mutable_index()->Mutableglobal_vector_kmeans_tree_index();
            kmeansSettings->Mutablevector_settings()->Setlevels(levels);
            kmeansSettings->Mutablevector_settings()->mutable_settings()->set_vector_dimension(tableRowDimension);
            ForwardToTablet(runtime, tenantSchemeShard, sender, request);
        }

        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);

        {
            ui64 dataSizeMB = (tableBytes + 1_MB - 1) / 1_MB;
            ui32 rowCount = tableRows; // alias
            ui64 formulaRequestUnitsApproximation = levels * Max<ui64>(dataSizeMB * 1152, dataSizeMB * 640 + rowCount * 0.5);

            ui64 actualRequestUnits = smallRows ? 3415 : 18552; // TODO: cut from html
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex " << buildIndexHtml << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: " << actualRequestUnits << " ");

            Cerr
                << "dataSizeMB = " << dataSizeMB
                << " rowCount = " << rowCount
                << " expectedRequestUnits = " << formulaRequestUnitsApproximation
                << " actualRequestUnits = " << actualRequestUnits
                << Endl;

            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
            // or ensure manually that the difference is not really big
            UNIT_ASSERT_LE(actualRequestUnits, formulaRequestUnitsApproximation);
            if (smallRows) {
                // here `formulaRequestUnitsApproximation` may be even a bit less than `actualRequestUnits`
                // because we do not count `__ydb_parent` and `id` columns
                UNIT_ASSERT_VALUES_EQUAL(actualRequestUnits, 3415);
                UNIT_ASSERT_VALUES_EQUAL(formulaRequestUnitsApproximation, 4170);
            } else {
                // here `formulaRequestUnitsApproximation` much less than `actualRequestUnits`
                // because we do less than 5 kmeans iterations or buffer some rows
                UNIT_ASSERT_VALUES_EQUAL(actualRequestUnits, 18552);
                UNIT_ASSERT_VALUES_EQUAL(formulaRequestUnitsApproximation, 27648);
            }
        }
    }

    Y_UNIT_TEST(Metering_CommonDB) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);
        auto deterministicMetering = MakeCpuMeteringDeterministic(runtime);

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
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/CommonDB/Table", 0, 100, 300);

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
        TestBuildVectorIndex(runtime, buildIndexTx, tenantSchemeShard, "/MyRoot/CommonDB", "/MyRoot/CommonDB/Table", "index1", {"embedding"});
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
        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> localKBlocker(runtime, [&](const auto&) {
            return true;
        });
        runtime.WaitFor("localK", [&]{ return localKBlocker.size(); });
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
        localKBlocker.Stop().Unblock();

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

    Y_UNIT_TEST_FLAG(Metering_ServerLessDB, smallScanBuffer) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);
        auto deterministicMetering = MakeCpuMeteringDeterministic(runtime);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        const ui32 K = 4;
        const ui32 tableRows = 200;
        const ui64 tableRowBytes = 9; // key:Uint32 (4 bytes), embedding:String (5 bytes)
        const ui64 tableBytes = tableRows * tableRowBytes;
        const ui64 buildRowBytes = 17; // parent:Uint64 (8 bytes), key:Uint32 (4 bytes), embedding:String (5 bytes)
        const ui64 buildBytes = tableRows * buildRowBytes;
        const ui64 postingRowBytes = 12; // parent:Uint64 (8 bytes), key:Uint32 (4 bytes)
        const ui64 postingBytes = tableRows * postingRowBytes;
        const ui64 levelRowBytes = 21; // parent:Uint64 (8 bytes), id:Uint64 (8 bytes), embedding:String (5 bytes)
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 50);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 1, 50, 150);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 2, 150, 200);

        TBlockEvents<NMetering::TEvMetering::TEvWriteMeteringJson> meteringBlocker(runtime, [&](const auto& ev) {
            Cerr << "TEvWriteMeteringJson " << ev->Get()->MeteringJson << Endl;
            return true;
        });
        TString previousBillId = "0-0-0-0";

        TMeteringStats billingStats = TMeteringStatsHelper::ZeroValue();
        TMeteringStats expectedBillingStats = TMeteringStatsHelper::ZeroValue();
        auto logBillingStats = [&]() {
            Cerr << "BillingStats: " << billingStats << Endl;
        };

        TBlockEvents<TEvDataShard::TEvSampleKResponse> sampleKBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            billingStats += response.GetMeteringStats();
            return true;
        });

        TBlockEvents<TEvDataShard::TEvRecomputeKMeansResponse> recomputeKBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            billingStats += response.GetMeteringStats();
            return true;
        });

        TBlockEvents<TEvIndexBuilder::TEvUploadSampleKResponse> uploadSampleKBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            billingStats += response.GetMeteringStats();
            return true;
        });

        TBlockEvents<TEvDataShard::TEvReshuffleKMeansResponse> reshuffleBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            billingStats += response.GetMeteringStats();
            return true;
        });

        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> localKMeansBlocker(runtime, [&](const auto& ev) {
            auto response = ev->Get()->Record;
            billingStats += response.GetMeteringStats();
            return true;
        });

        // Build vector index with max_shards_in_flight(1) to guarantee deterministic metering data
        ui64 buildIndexTx = ++txId;
        {
            auto sender = runtime.AllocateEdgeActor();
            auto request = CreateBuildIndexRequest(buildIndexTx, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{
                "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {}
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
            kmeansSettings->Mutablevector_settings()->Setclusters(K);
            ForwardToTablet(runtime, tenantSchemeShard, sender, request);
        }

        for (ui32 shard = 0; shard < 3; shard++) {
            runtime.WaitFor("sampleK", [&]{ return sampleKBlocker.size(); });
            sampleKBlocker.Unblock();
        }
        // SAMPLE reads table once, no writes:
        AddRead(expectedBillingStats, tableRows, tableBytes);
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(billingStats.ShortDebugString(), expectedBillingStats.ShortDebugString());

        // every RECOMPUTE round reads table once, no writes; there are 3 recompute rounds:
        for (ui32 round = 0; round < 3; round++) {
            for (ui32 shard = 0; shard < 3; shard++) {
                runtime.WaitFor("recomputeK", [&]{ return recomputeKBlocker.size(); });
                recomputeKBlocker.Unblock();
            }
            AddRead(expectedBillingStats, tableRows, tableBytes);
            logBillingStats();
            UNIT_ASSERT_VALUES_EQUAL(billingStats.ShortDebugString(), expectedBillingStats.ShortDebugString());
        }

        runtime.WaitFor("uploadSampleK", [&]{ return uploadSampleKBlocker.size(); });
        // upload SAMPLE writes K level rows, no reads:
        AddUpload(expectedBillingStats, K, K * levelRowBytes);
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(billingStats.ShortDebugString(), expectedBillingStats.ShortDebugString());
        uploadSampleKBlocker.Unblock();

        runtime.WaitFor("metering", [&]{ return meteringBlocker.size(); });
        {
            auto newBillId = TStringBuilder()
                << expectedBillingStats.GetUploadRows() << "-" << expectedBillingStats.GetReadRows() << "-"
                << expectedBillingStats.GetUploadBytes() << "-" << expectedBillingStats.GetReadBytes();
            auto expectedId = TStringBuilder()
                << "109-72075186233409549-2-" << previousBillId << "-" << newBillId;
            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
            auto expectedBill = TBillRecord()
                .Id(expectedId)
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(10))
                .Usage(TBillRecord::RequestUnits(130, TInstant::Seconds(0), TInstant::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            MeteringDataEqual(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
            previousBillId = newBillId;
            meteringBlocker.Unblock();
        }

        for (ui32 shard = 0; shard < 3; shard++) {
            runtime.WaitFor("reshuffle", [&]{ return reshuffleBlocker.size(); });
            reshuffleBlocker.Unblock();
        }
        // RESHUFFLE reads and writes table once:
        AddUpload(expectedBillingStats, tableRows, buildBytes);
        AddRead(expectedBillingStats, tableRows, tableBytes);
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(billingStats.ShortDebugString(), expectedBillingStats.ShortDebugString());

        for (ui32 shard = 0; shard < 4; shard++) {
            runtime.WaitFor("localKMeans", [&]{ return localKMeansBlocker.size(); });
            localKMeansBlocker.Unblock();
        }
        // KMEANS writes build table once and forms at least K, at most K * K level rows
        // (depending on clustering uniformity; it's not so good on test data)
        AddUpload(expectedBillingStats, tableRows, postingBytes);
        UNIT_ASSERT(billingStats.GetUploadRows() >= expectedBillingStats.GetUploadRows() + K);
        const ui64 level2clusters = billingStats.GetUploadRows() - expectedBillingStats.GetUploadRows();
        AddUpload(expectedBillingStats, level2clusters, level2clusters * levelRowBytes);
        if (smallScanBuffer) {
            // KMEANS reads build table 5 times (SAMPLE + KMEANS * 3 + UPLOAD):
            AddRead(expectedBillingStats, tableRows * 5, buildBytes * 5);
        } else {
            // KMEANS reads build table once:
            AddRead(expectedBillingStats, tableRows, buildBytes);
        }
        logBillingStats();
        UNIT_ASSERT_VALUES_EQUAL(billingStats.ShortDebugString(), expectedBillingStats.ShortDebugString());

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
            auto newBillId = TStringBuilder()
                << expectedBillingStats.GetUploadRows() << "-" << expectedBillingStats.GetReadRows() << "-"
                << expectedBillingStats.GetUploadBytes() << "-" << expectedBillingStats.GetReadBytes();
            auto expectedId = TStringBuilder()
                << "109-72075186233409549-2-" << previousBillId << "-" << newBillId;
            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
            auto expectedBill = TBillRecord()
                .Id(expectedId)
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(10))
                .Usage(TBillRecord::RequestUnits(336, TInstant::Seconds(10), TInstant::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            MeteringDataEqual(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
            previousBillId = newBillId;
            meteringBlocker.Stop().Unblock();
        }
    }

    Y_UNIT_TEST_FLAG(Metering_ServerLessDB_Restarts, doRestarts) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);
        auto deterministicMetering = MakeCpuMeteringDeterministic(runtime);

        ui64 tenantSchemeShard = 0;
        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write data directly into shards
        const ui32 K = 4;
        const ui32 tableRows = 200;
        const ui64 tableRowBytes = 9; // key:Uint32 (4 bytes), embedding:String (5 bytes)
        const ui64 tableBytes = tableRows * tableRowBytes;
        const ui64 shardRows = 50;
        const ui64 tableShardBytes = shardRows * tableRowBytes;
        const ui64 buildRowBytes = 17; // parent:Uint64 (8 bytes), key:Uint32 (4 bytes), embedding:String (5 bytes)
        const ui64 buildBytes = tableRows * buildRowBytes;
        const ui64 buildShardBytes = shardRows * buildRowBytes;
        const ui64 postingRowBytes = 12; // parent:Uint64 (8 bytes), key:Uint32 (4 bytes)
        const ui64 postingBytes = tableRows * postingRowBytes;
        const ui64 levelRowBytes = 21; // parent:Uint64 (8 bytes), id:Uint64 (8 bytes), embedding:String (5 bytes)
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 50);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 1, 50, 150);
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 2, 150, 200);

        TBlockEvents<NMetering::TEvMetering::TEvWriteMeteringJson> meteringBlocker(runtime, [&](const auto& ev) {
            Cerr << "TEvWriteMeteringJson " << ev->Get()->MeteringJson << Endl;
            return true;
        });
        TString previousBillId = "0-0-0-0";
        TMeteringStats billedStats = TMeteringStatsHelper::ZeroValue();
        TMeteringStats expectedBillingStats = TMeteringStatsHelper::ZeroValue();

        TBlockEvents<TEvDataShard::TEvReshuffleKMeansResponse> reshuffleBlocker(runtime, [&](const auto&) {
            return true;
        });

        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> localKMeansBlocker(runtime, [&](const auto&) {
            return doRestarts;
        });

        // Build vector index with max_shards_in_flight(1) to guarantee deterministic metering data
        ui64 buildIndexTx = ++txId;
        {
            auto sender = runtime.AllocateEdgeActor();
            auto request = CreateBuildIndexRequest(buildIndexTx, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{
                "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {}
            });
            auto settings = request->Record.MutableSettings();
            settings->set_max_shards_in_flight(1);
            settings->MutableScanSettings()->SetMaxBatchRows(1);
            auto kmeansSettings = request->Record.MutableSettings()->mutable_index()->Mutableglobal_vector_kmeans_tree_index();
            kmeansSettings->Mutablevector_settings()->Setlevels(2);
            kmeansSettings->Mutablevector_settings()->Setclusters(K);
            ForwardToTablet(runtime, tenantSchemeShard, sender, request);
        }

        runtime.WaitFor("reshuffle", [&]{ return reshuffleBlocker.size(); });
        // SAMPLE reads table once, no writes:
        AddRead(expectedBillingStats, tableRows, tableBytes);
        // every RECOMPUTE round reads table once, no writes; there are 3 recompute rounds:
        AddRead(expectedBillingStats, tableRows * 3, tableBytes * 3);
        // upload SAMPLE writes K level rows, no reads:
        AddUpload(expectedBillingStats, K, K * levelRowBytes);
        {
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex 1 " << buildIndexHtml << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Processed: " + expectedBillingStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: 130 (ReadTable: 128, BulkUpsert: 2, "
                << "CPU: " << expectedBillingStats.GetCpuTimeUs() / 1500 << ")");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Billed: " + billedStats.ShortDebugString());
        }
        runtime.WaitFor("metering", [&]{ return meteringBlocker.size(); });
        {
            auto newBillId = TStringBuilder()
                << expectedBillingStats.GetUploadRows() << "-" << expectedBillingStats.GetReadRows() << "-"
                << expectedBillingStats.GetUploadBytes() << "-" << expectedBillingStats.GetReadBytes();
            auto expectedId = TStringBuilder()
                << "109-72075186233409549-2-" << previousBillId << "-" << newBillId;
            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
            auto expectedBill = TBillRecord()
                .Id(expectedId)
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(10))
                .Usage(TBillRecord::RequestUnits(130, TInstant::Seconds(0), TInstant::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            MeteringDataEqual(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
            previousBillId = newBillId;
            billedStats = expectedBillingStats;
            meteringBlocker.Unblock();
        }
        if (doRestarts) {
            RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        }
        {
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex 2 " << buildIndexHtml << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Processed: " + expectedBillingStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: 130 (ReadTable: 128, BulkUpsert: 2, "
                << "CPU: " << expectedBillingStats.GetCpuTimeUs() / 1500 << ")");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Billed: " + billedStats.ShortDebugString());
        }

        reshuffleBlocker.Unblock();
        runtime.WaitFor("reshuffle", [&]{ return reshuffleBlocker.size(); });
        // shard RESHUFFLE reads and writes once:
        TMeteringStats shardReshuffleBillingStats = TMeteringStatsHelper::ZeroValue();
        AddUpload(shardReshuffleBillingStats, shardRows, buildShardBytes);
        AddRead(shardReshuffleBillingStats, shardRows, tableShardBytes);
        expectedBillingStats += shardReshuffleBillingStats;
        {
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex 3 " << buildIndexHtml << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Processed: " + expectedBillingStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: 155 (ReadTable: 128, BulkUpsert: 27, "
                << "CPU: " << expectedBillingStats.GetCpuTimeUs() / 1500 << ")");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Billed: " + billedStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, "<td>" + shardReshuffleBillingStats.ShortDebugString());
        }
        runtime.WaitFor("metering", [&]{ return meteringBlocker.size(); });
        {
            auto newBillId = TStringBuilder()
                << expectedBillingStats.GetUploadRows() << "-" << expectedBillingStats.GetReadRows() << "-"
                << expectedBillingStats.GetUploadBytes() << "-" << expectedBillingStats.GetReadBytes();
            auto expectedId = TStringBuilder()
                << "109-72075186233409549-2-" << previousBillId << "-" << newBillId;
            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
            auto expectedBill = TBillRecord()
                .Id(expectedId)
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(20))
                .Usage(TBillRecord::RequestUnits(153, TInstant::Seconds(10), TInstant::Seconds(20)));
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            MeteringDataEqual(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
            previousBillId = newBillId;
            billedStats = expectedBillingStats;
            meteringBlocker.Unblock();
        }
        if (doRestarts) {
            RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        }
        {
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex 4 " << buildIndexHtml << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Processed: " + expectedBillingStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: 155 (ReadTable: 128, BulkUpsert: 27, "
                << "CPU: " << expectedBillingStats.GetCpuTimeUs() / 1500 << ")");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Billed: " + billedStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, "<td>" + shardReshuffleBillingStats.ShortDebugString());
        }

        reshuffleBlocker.Stop().Unblock();
        // RESHUFFLE reads and writes table once:
        expectedBillingStats -= shardReshuffleBillingStats; // already added
        AddUpload(expectedBillingStats, tableRows, buildBytes);
        AddRead(expectedBillingStats, tableRows, tableBytes);

        if (doRestarts) {
            runtime.WaitFor("localKMeans", [&]{ return localKMeansBlocker.size(); });
            RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
            localKMeansBlocker.Stop().Unblock();
        }

        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);
        // KMEANS writes build table once and forms K * K level rows:
        AddUpload(expectedBillingStats, tableRows + K * K, postingBytes + K * K * levelRowBytes);
        // KMEANS reads build table 5 times (SAMPLE + KMEANS * 3 + UPLOAD):
        AddRead(expectedBillingStats, tableRows * 5, buildBytes * 5);
        {
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex 5 " << buildIndexHtml << Endl;
            Cout << expectedBillingStats.ShortDebugString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Processed: " + expectedBillingStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: 338 (ReadTable: 128, BulkUpsert: 210, "
                << "CPU: " << expectedBillingStats.GetCpuTimeUs() / 1500 << ")");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Billed: " + expectedBillingStats.ShortDebugString());
        }
        runtime.WaitFor("metering", [&]{ return meteringBlocker.size(); });
        {
            auto newBillId = TStringBuilder()
                << expectedBillingStats.GetUploadRows() << "-" << expectedBillingStats.GetReadRows() << "-"
                << expectedBillingStats.GetUploadBytes() << "-" << expectedBillingStats.GetReadBytes();
            auto expectedId = TStringBuilder()
                << "109-72075186233409549-2-" << previousBillId << "-" << newBillId;
            // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
            // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#vector-index
            auto expectedBill = TBillRecord()
                .Id(expectedId)
                .CloudId("CLOUD_ID_VAL").FolderId("FOLDER_ID_VAL").ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::Seconds(20))
                .Usage(TBillRecord::RequestUnits(311, TInstant::Seconds(20), TInstant::Seconds(20)));
            UNIT_ASSERT_VALUES_EQUAL(meteringBlocker.size(), 1);
            MeteringDataEqual(meteringBlocker[0]->Get()->MeteringJson, expectedBill.ToString());
            previousBillId = newBillId;
            billedStats = expectedBillingStats;
            meteringBlocker.Unblock();
        }
        if (doRestarts) {
            RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        }
        {
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, tenantSchemeShard, buildIndexTx);
            Cout << "BuildIndex 6 " << buildIndexHtml << Endl;
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Processed: " + expectedBillingStats.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, TStringBuilder() << "Request Units: 338 (ReadTable: 128, BulkUpsert: 210, "
                << "CPU: " << expectedBillingStats.GetCpuTimeUs() / 1500 << ")");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml,  "Billed: " + expectedBillingStats.ShortDebugString());
        }
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
                overlap_clusters: 3
                overlap_ratio: 1.2
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
        const TVector<NYdb::NTable::TGlobalIndexSettings> partitionSettings = prefixed
            ? TVector<NYdb::NTable::TGlobalIndexSettings>{ globalIndexSettings, globalIndexSettings, globalIndexSettings }
            : TVector<NYdb::NTable::TGlobalIndexSettings>{ globalIndexSettings, globalIndexSettings };
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", TBuildIndexConfig{
            "by_embedding", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, indexColumns, dataColumns,
            partitionSettings, std::move(kmeansTreeSettings)
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        indexCreationBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexTx);

        auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            buildIndexOperation.DebugString()
        );

        using namespace NKikimr::NTableIndex::NKMeans;
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
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});

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
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});

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
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});

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
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Processed: UploadRows: 0 UploadBytes: 0 ReadRows: 0 ReadBytes: 0");
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
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Processed: UploadRows: 0 UploadBytes: 0 ReadRows: 0 ReadBytes: 0");
        }
    }

    ui64 DoCreateBrokenIndex(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
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

        TBlockEvents<TEvDataShard::TEvLocalKMeansRequest> kmeansBlocker(runtime, [&](const auto& ) {
            return true;
        });

        const ui64 buildIndexTx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});

        runtime.WaitFor("LocalKMeansRequest", [&]{ return kmeansBlocker.size(); });

        {
            // set unknown State value
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('Id (Uint64 '%lu)) ) )
                    (let value '('('State (Uint32 '999999)) ) )
                    (return (AsList (UpdateRow 'IndexBuild key value) ))
                )
            )", buildIndexTx);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        }

        Cerr << "... rebooting scheme shard" << Endl;
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        kmeansBlocker.Stop().Unblock();

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, TTestTxConfig::SchemeShard, buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_UNSPECIFIED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Unknown build state");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, "IsBroken: YES");
        }

        return buildIndexTx;
    }

    Y_UNIT_TEST(UnknownState) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        const ui64 buildIndexTx = DoCreateBrokenIndex(runtime, env, txId);

        {
            // set a known State but unknown SubState
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('Id (Uint64 '%lu)) ) )
                    (let value '('('State (Uint32 '40)) '('SubState (Uint32 '999999)) ) )
                    (return (AsList (UpdateRow 'IndexBuild key value) ))
                )
            )", buildIndexTx);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        }

        Cerr << "... rebooting scheme shard" << Endl;
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, TTestTxConfig::SchemeShard, buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Unknown build sub-state");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, "IsBroken: YES");
        }

        {
            // set a known SubState but unknown BuildKind
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('Id (Uint64 '%lu)) ) )
                    (let value '('('SubState (Uint32 '0)) '('BuildKind (Uint32 '999999)) ) )
                    (return (AsList (UpdateRow 'IndexBuild key value) ))
                )
            )", buildIndexTx);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        }

        Cerr << "... rebooting scheme shard" << Endl;
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            auto buildIndexHtml = TestGetBuildIndexHtml(runtime, TTestTxConfig::SchemeShard, buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Unknown build kind");
            UNIT_ASSERT_STRING_CONTAINS(buildIndexHtml, "IsBroken: YES");
        }
    }

    Y_UNIT_TEST(CancelBroken) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        const ui64 buildIndexTx = DoCreateBrokenIndex(runtime, env, txId);

        const ui64 cancelTxId = ++txId;
        TestCancelBuildIndex(runtime, cancelTxId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        env.TestWaitNotification(runtime, buildIndexTx);

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        Y_ASSERT(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_CANCELLED);

        // Check that another index is built successfully (i.e. the table is not left in a locked state)
        const ui64 buildIndex2Tx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndex2Tx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});
        env.TestWaitNotification(runtime, buildIndex2Tx);
    }

    Y_UNIT_TEST(CreateBuildProposeReject) {
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

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable) {
                auto& op = *modifyScheme.MutableCreateTable();
                // specify invalid shard count to fail the operation
                op.SetUniformPartitionsCount(0);
            }
            return false;
        });

        const ui64 buildIndexTx = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});

        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            Cout << "BuildIndex 1 " << buildIndexOperation.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED,
                buildIndexOperation.DebugString()
            );
            UNIT_ASSERT_STRING_CONTAINS(buildIndexOperation.DebugString(), "Invalid table partition count specified");
        }

        blocker.Stop().Unblock();

        {
            auto result = ReadSystemTable(runtime, TTestTxConfig::SchemeShard, "SnapshotTables", {"Id", "TableOwnerId", "TableLocalId"}, {"Id"});
            auto value = NClient::TValue::Create(result);
            auto rowCount = value["Result"]["List"].Size();
            UNIT_ASSERT_VALUES_EQUAL_C(rowCount, 0, "Snapshot is not removed after rejecting index build");
        }

        // The next index build should succeed

        const ui64 buildIndexTx2 = ++txId;
        AsyncBuildVectorIndex(runtime, buildIndexTx2, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", "index1", {"embedding"});
        env.TestWaitNotification(runtime, buildIndexTx2);

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx2);
            Cout << "BuildIndex 2 " << buildIndexOperation.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

    }

    Y_UNIT_TEST(BuildTableWithEmptyShard) {
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
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // Write only 10 rows
        WriteVectorTableRows(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB/Table", 0, 0, 10);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(0), NLs::PathVersionEqual(3)});

        TBlockEvents<TEvDataShard::TEvFilterKMeansRequest> filterBlocker(runtime, [&](const auto& ) {
            return true;
        });

        ui64 buildIndexTx = ++txId;
        auto sender = runtime.AllocateEdgeActor();
        auto request = CreateBuildIndexRequest(buildIndexTx, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", TBuildIndexConfig{
            "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {}
        });
        auto kmeansSettings = request->Record.MutableSettings()->mutable_index()->mutable_global_vector_kmeans_tree_index();
        // 20 clusters with 10 rows will be merged into 1 cluster
        kmeansSettings->mutable_vector_settings()->set_clusters(20);
        kmeansSettings->mutable_vector_settings()->set_levels(2);
        kmeansSettings->mutable_vector_settings()->set_overlap_clusters(2);
        ForwardToTablet(runtime, tenantSchemeShard, sender, request);

        runtime.WaitFor("FilterKMeansRequest", [&]{ return filterBlocker.size(); });

        // Now wait for the 1st level to be finalized
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> level1Blocker(runtime, [&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable) {
                return true;
            }
            return false;
        });
        filterBlocker.Stop().Unblock();

        // Reshard the first level table (1build) so that one of the shards becomes empty.
        // (pretend that it's really luckily presharded to contain the empty child cluster range)
        {
            const char* buildTable = "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable1build";
            auto indexDesc = DescribePath(runtime, tenantSchemeShard, buildTable, true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 1);
            // There should be only 1 cluster because there are too little rows. So no clusters with ID > 1.
            TestSplitTable(runtime, tenantSchemeShard, ++txId, buildTable, Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 2 } } } }
            )", parts[0].GetDatashardId()));
            env.TestWaitNotification(runtime, txId);
        }
        level1Blocker.Stop().Unblock();

        // Now wait for the index build
        env.TestWaitNotification(runtime, buildIndexTx, tenantSchemeShard);
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
            {NLs::PathExist, NLs::IndexesCount(1), NLs::PathVersionEqual(6)});

        // Check row count in the posting table
        {
            auto rows = CountRows(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1/indexImplPostingTable");
            Cerr << "... posting table contains " << rows << " rows" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(rows, 10);
        }
    }

    Y_UNIT_TEST(GlobalLimit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NKikimrScheme::StatusSuccess, "Unexpected status: " << describe.GetStatus());
        auto curShards = describe.GetPathDescription().GetDomainDescription().GetShardsInside();

        TSchemeLimits lowLimits;

        lowLimits.MaxPaths = 3;
        lowLimits.MaxShards = curShards + 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildVectorIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "idx_global", {"embedding"}, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 4;
        lowLimits.MaxShards = curShards + 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildVectorIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "idx_global", {"embedding"}, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        Ydb::Table::GlobalIndexSettings shardingProto;
        shardingProto.set_uniform_partitions(2);
        auto sharding = NYdb::NTable::TGlobalIndexSettings::FromProto(shardingProto);
        lowLimits.MaxPaths = 4;
        lowLimits.MaxShards = curShards + 4;
        lowLimits.MaxShardsInPath = 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "idx_global", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {sharding, sharding}
        }, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxShardsInPath = 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "idx_global", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {sharding, sharding}
        });
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(PrefixedLimit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NKikimrScheme::StatusSuccess, "Unexpected status: " << describe.GetStatus());
        auto curShards = describe.GetPathDescription().GetDomainDescription().GetShardsInside();

        TSchemeLimits lowLimits;

        lowLimits.MaxPaths = 5;
        lowLimits.MaxShards = curShards + 4;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildVectorIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "idx_global", {"prefix", "embedding"}, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 6;
        lowLimits.MaxShards = curShards + 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildVectorIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "idx_global", {"prefix", "embedding"}, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 6;
        lowLimits.MaxShards = curShards + 4;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildVectorIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "idx_global", {"prefix", "embedding"});
        env.TestWaitNotification(runtime, txId);
    }

}
