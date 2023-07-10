#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(TSequence) {
    Y_UNIT_TEST(CreateTableWithDefaultFromSequence) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions()
                .Sequences(true)
                .Columns({
                    {"key", "Uint32", true, false, "default", "myseq"},
                    {"value", "Uint32", true, false},
                }));

        {
            TString result = KqpSimpleExec(runtime, "UPSERT INTO `/Root/table-1` (value) VALUES (1), (2), (3);");
            UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        }

        {
            TString result = KqpSimpleExec(runtime, "SELECT * FROM `/Root/table-1`;");
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 3 } }");
        }
    }

} // Y_UNIT_TEST_SUITE(TSequence)

}