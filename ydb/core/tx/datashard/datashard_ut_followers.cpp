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
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                               "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                               "{ items { uint32_value: 3 } items { uint32_value: 3 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
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
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                               "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                               "{ items { uint32_value: 3 } items { uint32_value: 3 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 4);");

        // The new row should be visible on the follower
        {
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                               "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                               "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
                               "{ items { uint32_value: 4 } items { uint32_value: 4 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }

        // Wait a bit more and add one more row
        SimulateSleep(server, TDuration::Seconds(2));
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5);");

        // The new row should be visible on the follower
        {
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                               "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                               "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
                               "{ items { uint32_value: 4 } items { uint32_value: 4 } }, "
                               "{ items { uint32_value: 5 } items { uint32_value: 5 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST(FollowerStaleRo) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions()
                .Shards(2)
                .Followers(1));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");

        bool dropFollowerUpdates = true;

        {
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                               "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                               "{ items { uint32_value: 3 } items { uint32_value: 3 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }

        std::vector<TAutoPtr<IEventHandle>> capturedUpdates;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) {
            if (ev->GetTypeRewrite() == NKikimr::TEvTablet::TEvFollowerUpdate::EventType ||
                ev->GetTypeRewrite() == NKikimr::TEvTablet::TEvFollowerAuxUpdate::EventType || 
                ev->GetTypeRewrite() == NKikimr::TEvTablet::TEvFUpdate::EventType ||
                ev->GetTypeRewrite() == NKikimr::TEvTablet::TEvFAuxUpdate::EventType)
            {

                if (dropFollowerUpdates) {
                    capturedUpdates.emplace_back(ev);
                    return true;
                }
                Cerr <<  "Followers update " << capturedUpdates.size() << Endl;
            }

            return false;
        };

        // blocking followers from new log updates.
        runtime.SetEventFilter(captureEvents);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 4), (2, 5), (3, 6);");

        {
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1` where key = 1", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 1 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }
    
        {
            // multiple shards, always read from main tablets.
            auto result = KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");
            TString expected = "{ items { uint32_value: 1 } items { uint32_value: 4 } }, "
                               "{ items { uint32_value: 2 } items { uint32_value: 5 } }, "
                               "{ items { uint32_value: 3 } items { uint32_value: 6 } }";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardFollowers)

} // namespace NKikimr
