#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(ReadIteratorExternalBlobs) {

    struct ReadIteratorCounter {
        int Reads = 0;
        int Continues = 0;
        int EvGets = 0;
        int BlobsRequested = 0;
    };

    std::unique_ptr<ReadIteratorCounter> SetupReadIteratorObserver(TTestActorRuntime& runtime) {
        std::unique_ptr<ReadIteratorCounter> iteratorCounter = std::make_unique<ReadIteratorCounter>();

        auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvDataShard::EvRead: {
                    iteratorCounter->Reads++;
                    break;
                }
                case TEvDataShard::EvReadContinue: {
                    iteratorCounter->Continues++;
                    break;
                }
                case TEvBlobStorage::EvGet: {
                    auto* msg = event->Get<TEvBlobStorage::TEvGet>();
                    iteratorCounter->EvGets++;
                    iteratorCounter->BlobsRequested += msg->QuerySize;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        return iteratorCounter;
    }

    struct Node {
        TPortManager Pm;
        TServerSettings ServerSettings;
        TServer::TPtr Server;
        ui64 Shard;
        TTableId TableId;
        TActorId Sender;
        TTestActorRuntime* Runtime;

        Node(bool useExternalBlobs, int externalBlobColumns = 1) : ServerSettings(Pm.GetPort(2134)) {
            ServerSettings.SetDomainName("Root")
                .SetUseRealThreads(false)
                .AddStoragePool("ssd")
                .AddStoragePool("hdd")
                .AddStoragePool("ext")
                .SetEnableUuidAsPrimaryKey(true);

            Server = new TServer(ServerSettings);
            
            Runtime = Server->GetRuntime();

            Sender = Runtime->AllocateEdgeActor();
        
            InitRoot(Server, Sender);
            
            TShardedTableOptions::TFamily fam;
            
            if (useExternalBlobs) {
                fam = {.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd", 
                        .ExternalPoolKind = "ext", .DataThreshold = 100u, .ExternalThreshold = 512_KB};
            } else {
                fam = {.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd", .DataThreshold = 100u};
            }
            
            TVector<TShardedTableOptions::TColumn> columns = {
                    {"blob_id", "Uuid", true, false}, 
                    {"chunk_num", "Int32", true, false}
            };

            for (int i = 0; i < externalBlobColumns; i++) {
                columns.push_back({"data" + ToString(i), "String", false, false});
            }

            auto opts = TShardedTableOptions()
                .Columns(columns)
                .Families({fam});

            CreateShardedTable(Server, Sender, "/Root", "table-1", opts);

            Shard = GetTableShards(Server, Sender, "/Root/table-1").at(0);
            TableId = ResolveTableId(Server, Sender, "/Root/table-1");
        }
    };

    void ValidateReadResult(TTestActorRuntime& runtime, NThreading::TFuture<Ydb::Table::ExecuteDataQueryResponse> readFuture, int rowsCount, int firstBlobChunkNum = 0, int extBlobColumnCount = 1) {
        Ydb::Table::ExecuteDataQueryResponse res = AwaitResponse(runtime, std::move(readFuture));
        auto& operation = res.Getoperation();
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        operation.result().UnpackTo(&result);
        UNIT_ASSERT_EQUAL(result.result_sets().size(), 1);
        auto& resultSet = result.result_sets()[0];
        UNIT_ASSERT_EQUAL(resultSet.rows_size(), rowsCount);

        for (int i = 0; i < resultSet.rows_size(); i++) {
            auto& row = resultSet.get_idx_rows(i);

            UNIT_ASSERT_EQUAL(row.items_size(), 2 + extBlobColumnCount);

            auto& chunkNumValue = row.get_idx_items(1);

            UNIT_ASSERT(chunkNumValue.has_int32_value());
            UNIT_ASSERT_EQUAL(chunkNumValue.Getint32_value(), firstBlobChunkNum + i);
            
            for (int j = 0; j < extBlobColumnCount; j++) {
                auto& dataValue = row.get_idx_items(2 + j);
                UNIT_ASSERT(dataValue.has_bytes_value());
                UNIT_ASSERT_EQUAL(dataValue.bytes_value().size(), 1_MB);
            }
        }
    }
    
    template <ui8 resultSize>
    void ValidateReadResult(TTestActorRuntime& runtime, NThreading::TFuture<Ydb::Table::ExecuteDataQueryResponse> readFuture, std::array<i32, resultSize> expectedResult) {
        Ydb::Table::ExecuteDataQueryResponse res = AwaitResponse(runtime, std::move(readFuture));
        auto& operation = res.Getoperation();
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        operation.result().UnpackTo(&result);
        UNIT_ASSERT_EQUAL(result.result_sets().size(), 1);
        auto& resultSet = result.result_sets()[0];
        UNIT_ASSERT_EQUAL(resultSet.rows_size(), resultSize);

        for (int i = 0; i < resultSet.rows_size(); i++) {
            auto& row = resultSet.get_idx_rows(i);

            UNIT_ASSERT_EQUAL(row.items_size(), 3);

            auto& chunkNumValue = row.get_idx_items(1);

            UNIT_ASSERT(chunkNumValue.has_int32_value());
            UNIT_ASSERT_EQUAL(chunkNumValue.Getint32_value(), expectedResult[i]);
            
            auto& dataValue = row.get_idx_items(2);
            UNIT_ASSERT(dataValue.has_bytes_value());
            UNIT_ASSERT_EQUAL(dataValue.bytes_value().size(), 1_MB);
        }
    }

    Y_UNIT_TEST(ExtBlobs) {
        Node node(true);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                    (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
            
            ExecSQL(server, sender, query);    
        }

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1);
        }

        auto iteratorCounter = SetupReadIteratorObserver(runtime);

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        ValidateReadResult(runtime, std::move(readFuture), 10);

        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Reads, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Continues, 2);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->EvGets, 2);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->BlobsRequested, 10);
    }

    Y_UNIT_TEST(ExtBlobsWithDeletesInTheBeginning) {
        Node node(true);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                    (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
            
            ExecSQL(server, sender, query);    
        }

        CompactTable(runtime, shard1, tableId1, false);

        for (int i = 0; i < 7; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                DELETE FROM `/Root/table-1` WHERE blob_id=Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") and chunk_num=)___"
                + chunkNum + ";";
            
            ExecSQL(server, sender, query);    
        }

        auto iteratorCounter = SetupReadIteratorObserver(runtime);

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        ValidateReadResult(runtime, std::move(readFuture), 3, 7);

        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Reads, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Continues, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->EvGets, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->BlobsRequested, 3);
    }

    Y_UNIT_TEST(ExtBlobsWithDeletesInTheEnd) {
        Node node(true);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                    (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
            
            ExecSQL(server, sender, query);    
        }
        
        CompactTable(runtime, shard1, tableId1, false);

        for (int i = 3; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                DELETE FROM `/Root/table-1` WHERE blob_id=Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") and chunk_num=)___"
                + chunkNum + ";";
            
            ExecSQL(server, sender, query);    
        }

        auto iteratorCounter = SetupReadIteratorObserver(runtime);

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        ValidateReadResult(runtime, std::move(readFuture), 3);

        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Reads, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Continues, 0);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->EvGets, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->BlobsRequested, 3);
    }

    Y_UNIT_TEST(ExtBlobsWithDeletesInTheMiddle) {
        Node node(true);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                    (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
            
            ExecSQL(server, sender, query);    
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            TString query = R"___(
                DELETE FROM `/Root/table-1` WHERE blob_id=Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") and chunk_num=0;)___";
            
            ExecSQL(server, sender, query);    
        }

        for (int i = 2; i < 5; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                DELETE FROM `/Root/table-1` WHERE blob_id=Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") and chunk_num=)___"
                + chunkNum + ";";
            
            ExecSQL(server, sender, query);    
        }

        auto iteratorCounter = SetupReadIteratorObserver(runtime);

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        ValidateReadResult<6>(runtime, std::move(readFuture), {1, 5, 6, 7, 8, 9});

        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Reads, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Continues, 2);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->EvGets, 2);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->BlobsRequested, 6);
    }

    Y_UNIT_TEST(ExtBlobsMultipleColumns) {
        Node node(true, 2);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0, data1) VALUES
                    (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___"
                     + chunkNum + ", \"" + largeValue + "\", \"" + largeValue + "\");";
            
            ExecSQL(server, sender, query);    
        }

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1);
        }

        auto iteratorCounter = SetupReadIteratorObserver(runtime);

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0, data1
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        ValidateReadResult(runtime, std::move(readFuture), 10, 0, 2);

        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Reads, 1);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Continues, 3);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->EvGets, 4);
        UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->BlobsRequested, 20);
    }

    Y_UNIT_TEST(ExtBlobsWithCompactingMiddleRows) {
        std::unordered_map<int, ReadIteratorCounter> expectedResults;
        expectedResults[1] = {1, 7, 7, 18};
        expectedResults[2] = {1, 8, 7, 16};
        expectedResults[3] = {1, 7, 6, 14};
        expectedResults[4] = {1, 8, 6, 12};
        expectedResults[5] = {1, 4, 2, 10};

        // We write 20 rows, some of them are compacted, then we write some more rows "before" and "after" and read all of them
        // The quantity of rows before, in the middle and after is different for each test. For example the first one is
        // 1 row before, 18 rows in the middle and 1 row after.
        for (int test = 1; test < 6; test++) {
            int compactedPart = 20 - (test * 2);

            Node node(true);

            auto server = node.Server;
            auto& runtime = *node.Runtime;
            auto& sender = node.Sender;
            auto shard1 = node.Shard;
            auto tableId1 = node.TableId;

            TString largeValue(1_MB, 'L');

            for (int i = 0; i < compactedPart; i++) {
                TString chunkNum = ToString(test + i);

                TString query = R"___(
                    UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                        (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
                
                ExecSQL(server, sender, query);    
            }

            {
                Cerr << "... waiting for stats after upsert" << Endl;
                auto stats = WaitTableStats(runtime, shard1);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), compactedPart);
            }

            CompactTable(runtime, shard1, tableId1, false);

            {
                Cerr << "... waiting for stats after compaction" << Endl;
                auto stats = WaitTableStats(runtime, shard1, 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), compactedPart);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1);
            }

            for (int i = 0; i < test; i++) {
                TString chunkNum = ToString(i);

                TString query = R"___(
                    UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                        (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
                
                ExecSQL(server, sender, query);    
            }

            for (int i = compactedPart + test; i < 20; i++) {
                TString chunkNum = ToString(i);

                TString query = R"___(
                    UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                        (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
                
                ExecSQL(server, sender, query);    
            }

            auto iteratorCounter = SetupReadIteratorObserver(runtime);

            auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                    FROM `/Root/table-1`
                    WHERE
                        blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                        chunk_num >= 0
                    ORDER BY blob_id, chunk_num ASC
                    LIMIT 100;)");

            ValidateReadResult(runtime, std::move(readFuture), 20);

            auto& expectedResult = expectedResults[test];

            UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->Reads, expectedResult.Reads);
            UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->EvGets, expectedResult.EvGets);
            UNIT_ASSERT_VALUES_EQUAL(iteratorCounter->BlobsRequested, expectedResult.BlobsRequested);
        }
    }

    Y_UNIT_TEST(ExtBlobsEmptyTable) {
        Node node(true);

        auto& runtime = *node.Runtime;

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        ValidateReadResult(runtime, std::move(readFuture), 0);
    }

    Y_UNIT_TEST(NotExtBlobs) {
        Node node(false);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data0) VALUES
                    (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), )___" + chunkNum + ", \"" + largeValue + "\");";
            
            ExecSQL(server, sender, query);    
        }

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1);
        }

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data0
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");
        
        ValidateReadResult(runtime, std::move(readFuture), 10);
    }

}

} // namespace NKikimr
