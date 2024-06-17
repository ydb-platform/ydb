#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"
#include "read_iterator.h"

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

#include <algorithm>
#include <map>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(ReadIteratorExternalBlobs) {

    Y_UNIT_TEST(ExtBlobs) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .AddStoragePool("ssd")
            .AddStoragePool("hdd")
            .AddStoragePool("ext")
            .SetEnableUuidAsPrimaryKey(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Columns({
                {"blob_id", "Uuid", true, false}, 
                {"chunk_num", "Int32", true, false}, 
                {"data", "String", false, false}})
            .Families({
                {.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd", 
                    .ExternalPoolKind = "ext", .DataThreshold = 100u, .ExternalThreshold = 512_KB}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        TString smallValue(150, 'S');
        TString largeValue(1_MB, 'L');

        //;
        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data) VALUES
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

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        Ydb::Table::ExecuteDataQueryResponse res = AwaitResponse(runtime, std::move(readFuture));
        auto& operation = res.Getoperation();
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        operation.result().UnpackTo(&result);
        UNIT_ASSERT_EQUAL(result.result_sets().size(), 1);
        auto& resultSet = result.result_sets()[0];
        UNIT_ASSERT_EQUAL(resultSet.rows().size(), 10);
    }

    Y_UNIT_TEST(ExtBlobsEmptyTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .AddStoragePool("ssd")
            .AddStoragePool("hdd")
            .AddStoragePool("ext")
            .SetEnableUuidAsPrimaryKey(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Columns({
                {"blob_id", "Uuid", true, false}, 
                {"chunk_num", "Int32", true, false}, 
                {"data", "String", false, false}})
            .Families({
                {.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd", 
                    .ExternalPoolKind = "ext", .DataThreshold = 100u, .ExternalThreshold = 512_KB}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            "");
    }

    Y_UNIT_TEST(NotExtBlobs) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .AddStoragePool("ssd")
            .AddStoragePool("hdd")
            .SetEnableUuidAsPrimaryKey(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Columns({
                {"blob_id", "Uuid", true, false}, 
                {"chunk_num", "Int32", true, false}, 
                {"data", "String", false, false}})
            .Families({
                {.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd"}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        TString smallValue(150, 'S');
        TString largeValue(1_MB, 'L');

        for (int i = 0; i < 10; i++) {
            TString chunkNum = ToString(i);
            TString query = R"___(
                UPSERT INTO `/Root/table-1` (blob_id, chunk_num, data) VALUES
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

        auto readFuture = KqpSimpleSend(runtime, R"(SELECT blob_id, chunk_num, data
                FROM `/Root/table-1`
                WHERE
                    blob_id = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c") AND
                    chunk_num >= 0
                ORDER BY blob_id, chunk_num ASC
                LIMIT 100;)");

        Ydb::Table::ExecuteDataQueryResponse res = AwaitResponse(runtime, std::move(readFuture));
        auto& operation = res.Getoperation();
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        operation.result().UnpackTo(&result);
        UNIT_ASSERT_EQUAL(result.result_sets().size(), 1);
        auto& resultSet = result.result_sets()[0];
        UNIT_ASSERT_EQUAL(resultSet.rows().size(), 10);
    }

}

} // namespace NKikimr
