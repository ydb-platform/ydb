#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_ut_read_table.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NKikimr::NDataShardReadTableTest;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardFollowers) {

    Y_UNIT_TEST(FollowerKeepsWorkingAfterMvccReadTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(true)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions()
                .Followers(1));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");

        {
            auto ev = ExecRequest(runtime, sender, MakeSimpleStaleRoRequest("SELECT * FROM `/Root/table-1`", "/Root"));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            TString expected = "Struct { "
                               "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                               "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
                               "List { Struct { Optional { Uint32: 3 } } Struct { Optional { Uint32: 3 } } } "
                               "} Struct { Bool: false }";
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults()[0].GetValue().ShortDebugString(), expected);
        }

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1"));
        auto table1rows = table1state.All();
        UNIT_ASSERT_VALUES_EQUAL(table1rows,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // Wait for snapshot to disappear
        SimulateSleep(server, TDuration::Seconds(2));

        // Make a request to make sure snapshot metadata is updated on the follower
        {
            auto ev = ExecRequest(runtime, sender, MakeSimpleStaleRoRequest("SELECT * FROM `/Root/table-1`", "/Root"));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            TString expected = "Struct { "
                               "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                               "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
                               "List { Struct { Optional { Uint32: 3 } } Struct { Optional { Uint32: 3 } } } "
                               "} Struct { Bool: false }";
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults()[0].GetValue().ShortDebugString(), expected);
        }

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 4);");

        // The new row should be visible on the follower
        {
            auto ev = ExecRequest(runtime, sender, MakeSimpleStaleRoRequest("SELECT * FROM `/Root/table-1`", "/Root"));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            TString expected = "Struct { "
                               "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                               "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
                               "List { Struct { Optional { Uint32: 3 } } Struct { Optional { Uint32: 3 } } } "
                               "List { Struct { Optional { Uint32: 4 } } Struct { Optional { Uint32: 4 } } } "
                               "} Struct { Bool: false }";
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults()[0].GetValue().ShortDebugString(), expected);
        }

        // Wait a bit more and add one more row
        SimulateSleep(server, TDuration::Seconds(2));
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5);");

        // The new row should be visible on the follower
        {
            auto ev = ExecRequest(runtime, sender, MakeSimpleStaleRoRequest("SELECT * FROM `/Root/table-1`", "/Root"));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            TString expected = "Struct { "
                               "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                               "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
                               "List { Struct { Optional { Uint32: 3 } } Struct { Optional { Uint32: 3 } } } "
                               "List { Struct { Optional { Uint32: 4 } } Struct { Optional { Uint32: 4 } } } "
                               "List { Struct { Optional { Uint32: 5 } } Struct { Optional { Uint32: 5 } } } "
                               "} Struct { Bool: false }";
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults()[0].GetValue().ShortDebugString(), expected);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardFollowers)

} // namespace NKikimr
