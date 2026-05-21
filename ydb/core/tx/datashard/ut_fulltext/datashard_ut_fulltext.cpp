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

        Cout << "========= Initial writes =========\n";
        {
            // 1, 2, 3
            // 100, 200, 201, 203
            // 10, 20, 30
            ExecSQL(server, sender, Q_(R"(
                INSERT INTO `/Root/table/ft_idx/indexImplTable`
                (__ydb_token, __ydb_max_id, __ydb_generation, __ydb_added, __ydb_segment) VALUES
                ("red", 0, 0, true, "\x01\x01\x01"),
                ("red", 0, 0, true, "dd\x01\x02"),
                ("red", 0, 0, true, "\x0A\x0A\x0A");
            )"));
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            Cerr << "Data: " << tableState << "\n";
            TString expectedState = "__ydb_token = red, __ydb_max_id = 3, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\1\\1\\1\n"
                "__ydb_token = red, __ydb_max_id = 203, __ydb_generation = 18446744073709551608, __ydb_added = true, __ydb_segment = \\n\\n\\n\n"
                "__ydb_token = red, __ydb_max_id = 203, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = dd\\1\\2\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }

        Cout << "========= Writes with compaction =========\n";
        {
            gFulltextMaxDelta = 10;
            gFulltextMaxSegment = 3;

            // del 100, 200
            // add 50, 60
            // max_id 203 should become: 10 20 30 50 60 201 203 -> split in 3 parts
            ExecSQL(server, sender, Q_(R"(
                INSERT INTO `/Root/table/ft_idx/indexImplTable`
                (__ydb_token, __ydb_max_id, __ydb_generation, __ydb_added, __ydb_segment) VALUES
                ("red", 0, 0, false, "dd"),
                ("red", 0, 0, true, "2\x0A");
            )"));

            gFulltextMaxDelta = 10000;
            gFulltextMaxSegment = 10000;
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            Cerr << "Data: " << tableState << "\n";
            TString expectedState = "__ydb_token = red, __ydb_max_id = 3, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\1\\1\\1\n"
                "__ydb_token = red, __ydb_max_id = 30, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\n\\n\\n\n"
                "__ydb_token = red, __ydb_max_id = 201, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = 2\\n\\x8D\\1\n"
                "__ydb_token = red, __ydb_max_id = 203, __ydb_generation = 18446744073709551615, __ydb_added = true, __ydb_segment = \\xCB\\1\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardFulltext)
} // namespace NKikimr
