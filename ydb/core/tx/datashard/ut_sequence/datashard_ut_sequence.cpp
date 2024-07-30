#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(TSequence) {
    Y_UNIT_TEST(CreateTableWithDefaultFromSequence) {
        TPortManager pm;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions()
                .Sequences(true)
                .Columns({
                    {"key", "Int64", true, false, "default", "myseq"},
                    {"value", "Uint32", true, false},
                }));

        {
            TString result = KqpSimpleExec(runtime, "UPSERT INTO `/Root/table-1` (value) VALUES (1), (2), (3);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "INSERT INTO `/Root/table-1` (value) VALUES (4), (5), (6);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "REPLACE INTO `/Root/table-1` (value) VALUES (7), (8), (9);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "SELECT * FROM `/Root/table-1`;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { int64_value: 1 } items { uint32_value: 1 } }, "
                "{ items { int64_value: 2 } items { uint32_value: 2 } }, "
                "{ items { int64_value: 3 } items { uint32_value: 3 } }, "
                "{ items { int64_value: 4 } items { uint32_value: 4 } }, "
                "{ items { int64_value: 5 } items { uint32_value: 5 } }, "
                "{ items { int64_value: 6 } items { uint32_value: 6 } }, "
                "{ items { int64_value: 7 } items { uint32_value: 7 } }, "
                "{ items { int64_value: 8 } items { uint32_value: 8 } }, "
                "{ items { int64_value: 9 } items { uint32_value: 9 } }");
        }
    }

    Y_UNIT_TEST(SequencesIndex) {
        TPortManager pm;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-5",
            TShardedTableOptions()
                .Sequences(true)
                .Columns({
                    {"key", "Int64", true, false, "default", "myseq"},
                    {"value", "Uint32", true, false},
                })
                .Indexes({
                    {"by_i1value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobal},
                }));

        {
            TString result = KqpSimpleExec(runtime, "UPSERT INTO `/Root/table-5` (value) VALUES (1), (2), (3);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "INSERT INTO `/Root/table-5` (value) VALUES (4), (5), (6);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "REPLACE INTO `/Root/table-5` (value) VALUES (7), (8), (9);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "SELECT * FROM `/Root/table-5`;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { int64_value: 1 } items { uint32_value: 1 } }, "
                "{ items { int64_value: 2 } items { uint32_value: 2 } }, "
                "{ items { int64_value: 3 } items { uint32_value: 3 } }, "
                "{ items { int64_value: 4 } items { uint32_value: 4 } }, "
                "{ items { int64_value: 5 } items { uint32_value: 5 } }, "
                "{ items { int64_value: 6 } items { uint32_value: 6 } }, "
                "{ items { int64_value: 7 } items { uint32_value: 7 } }, "
                "{ items { int64_value: 8 } items { uint32_value: 8 } }, "
                "{ items { int64_value: 9 } items { uint32_value: 9 } }");
        }  
        
        {
            TString result = KqpSimpleExec(runtime, "SELECT * FROM `/Root/table-5` view by_i1value;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { int64_value: 1 } items { uint32_value: 1 } }, "
                "{ items { int64_value: 2 } items { uint32_value: 2 } }, "
                "{ items { int64_value: 3 } items { uint32_value: 3 } }, "
                "{ items { int64_value: 4 } items { uint32_value: 4 } }, "
                "{ items { int64_value: 5 } items { uint32_value: 5 } }, "
                "{ items { int64_value: 6 } items { uint32_value: 6 } }, "
                "{ items { int64_value: 7 } items { uint32_value: 7 } }, "
                "{ items { int64_value: 8 } items { uint32_value: 8 } }, "
                "{ items { int64_value: 9 } items { uint32_value: 9 } }");
        }            
    }

    Y_UNIT_TEST(CreateTableWithDefaultFromSequenceFromSelect) {
        TPortManager pm;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-4",
            TShardedTableOptions()
                .Sequences(true)
                .Columns({
                    {"key", "Int64", true, false, "default", "myseq"},
                    {"value", "Uint32", true, false},
                }));

        {
            TString result = KqpSimpleExec(runtime, "UPSERT INTO `/Root/table-4` (value) VALUES (303);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table-4`;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { int64_value: 1 } items { uint32_value: 303 } }");
        }

        {
            TString result = KqpSimpleExec(runtime, "UPSERT INTO `/Root/table-4` SELECT value FROM `/Root/table-4`;");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "SELECT * FROM `/Root/table-4` ORDER BY key;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { int64_value: 1 } items { uint32_value: 303 } }, "
                "{ items { int64_value: 2 } items { uint32_value: 303 } }");
        }

        {
            TString result = KqpSimpleExec(runtime, "UPSERT INTO `/Root/table-4` SELECT value FROM `/Root/table-4` where key = 1;");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "SELECT * FROM `/Root/table-4` ORDER BY key;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { int64_value: 1 } items { uint32_value: 303 } }, "
                "{ items { int64_value: 2 } items { uint32_value: 303 } }, "
                "{ items { int64_value: 3 } items { uint32_value: 303 } }");
        }
    }

    Y_UNIT_TEST(CreateTableWithDefaultFromSequenceBadRequest) {
        TPortManager pm;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-3",
            TShardedTableOptions()
                .Sequences(true)
                .Columns({
                    {"key", "Int64", true, false, "default", "myseq"},
                    {"value", "Uint32", true, false},
                }));

        {
            TString result = KqpSimpleExec(
                runtime,
                "$to_update = AsList( "
                "   AsStruct(CAST(12 as Uint32) as value)); "
                "UPDATE `/Root/table-3` ON SELECT * FROM AS_TABLE($to_update)");
            UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: BAD_REQUEST");
        }

        {
            TString result = KqpSimpleExec(
                runtime,
                "$to_update = AsList( "
                "   AsStruct(CAST(12 as Uint32) as value)); "
                "DELETE FROM `/Root/table-3` ON SELECT * FROM AS_TABLE($to_update)");
            UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: BAD_REQUEST");
        }

    }

} // Y_UNIT_TEST_SUITE(TSequence)

}
