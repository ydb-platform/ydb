#include "datashard.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardFulltext) {

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(TPortManager& pm, std::optional<TServerSettings> serverSettings = {}) {
        if (!serverSettings) {
            serverSettings.emplace(pm.GetPort(2134));
            serverSettings->SetDomainName("Root").SetUseRealThreads(false);
        }

        Tests::TServer::TPtr server = new TServer(serverSettings.value());
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    Y_UNIT_TEST(Insert) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);
        serverSettings.FeatureFlags.SetEnableAccessToIndexImplTables(true);
        auto [runtime, server, sender] = TestCreateServer(pm, serverSettings);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (pk uint64, text string, PRIMARY KEY (pk));
            )"),
            "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                ALTER TABLE `/Root/table` ADD INDEX ft_idx GLOBAL USING fulltext_compact ON (text)
                WITH (tokenizer=standard, use_filter_lowercase=true);
            )", "/Root"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table/ft_idx/indexImplTable");
        const auto shards = GetTableShards(server, sender, "/Root/table/ft_idx/indexImplTable");
        ui64 txId = 100;

        Cout << "========= Initial writes =========\n";
        {
            // 1, 2, 3
            // 100, 200, 201, 203
            // 10, 20, 30
            TVector<TCell> cells(15);
            cells[0] = TCell("red", 3);
            cells[1] = TCell::Make((ui64)0);
            cells[2] = TCell::Make((ui64)0);
            cells[3] = TCell::Make(true);
            cells[4] = TCell("\x01\x01\x01", 3);
            cells[5] = TCell("red", 3);
            cells[6] = TCell::Make((ui64)0);
            cells[7] = TCell::Make((ui64)0);
            cells[8] = TCell::Make(true);
            cells[9] = TCell("dd\x01\x02", 4);
            cells[10] = TCell("red", 3);
            cells[11] = TCell::Make((ui64)0);
            cells[12] = TCell::Make((ui64)0);
            cells[13] = TCell::Make(true);
            cells[14] = TCell("\x0A\x0A\x0A", 3);
            Write(runtime, sender, shards[0], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            Cerr << "Data: " << tableState << "\n";
            TString expectedState = "__ydb_token = red, __ydb_max_id = 18446744073709551615, __ydb_generation = 18446744073709551600, __ydb_added = true, __ydb_segment = \\n\\n\\n\n"
                "__ydb_token = red, __ydb_max_id = 18446744073709551615, __ydb_generation = 18446744073709551607, __ydb_added = true, __ydb_segment = dd\\1\\2\n"
                "__ydb_token = red, __ydb_max_id = 18446744073709551615, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\1\\1\\1\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }

        Cout << "========= Writes with compaction =========\n";
        {
            gFulltextMaxDelta = 10;
            gFulltextMaxSegment = 3;

            // del 100, 200
            // list becomes: 1 2 3 10 20 30 201 203
            // -> split into 1 2 3 + 10 20 30 + 201 203
            // add 50, 60
            // added as a new segment
            // -> 1 2 3 [max=9] + 10 20 30 [max=200] + 50 60 [max=200] + 201 203 [max=UINT64_MAX]
            TVector<TCell> cells(10);
            cells[0] = TCell("red", 3);
            cells[1] = TCell::Make((ui64)0);
            cells[2] = TCell::Make((ui64)0);
            cells[3] = TCell::Make(false);
            cells[4] = TCell("dd", 2);
            cells[5] = TCell("red", 3);
            cells[6] = TCell::Make((ui64)0);
            cells[7] = TCell::Make((ui64)0);
            cells[8] = TCell::Make(true);
            cells[9] = TCell("2\x0A", 2);
            txId++;
            Write(runtime, sender, shards[0], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

            gFulltextMaxDelta = 10000;
            gFulltextMaxSegment = 10000;
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            Cerr << "Data: " << tableState << "\n";
            TString expectedState = "__ydb_token = red, __ydb_max_id = 9, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\1\\1\\1\n"
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = 18446744073709551609, __ydb_added = true, __ydb_segment = 2\\n\n"
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\n\\n\\n\n"
                "__ydb_token = red, __ydb_max_id = 18446744073709551615, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\xC9\\1\\2\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardFulltext)
} // namespace NKikimr
