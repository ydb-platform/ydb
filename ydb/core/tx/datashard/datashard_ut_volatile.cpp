#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardVolatile) {

    Y_UNIT_TEST(DistributedWrite) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        auto forceVolatile = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    auto* msg = ev->Get<TEvDataShard::TEvProposeTransaction>();
                    auto flags = msg->Record.GetFlags();
                    if (!(flags & TTxFlags::Immediate)) {
                        Cerr << "... forcing propose to use volatile prepare" << Endl;
                        flags |= TTxFlags::VolatilePrepare;
                        msg->Record.SetFlags(flags);
                    }
                    break;
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(forceVolatile);

        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        Cerr << "!!! distributed write start" << Endl;
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )");
        Cerr << "!!! distributed write end" << Endl;

        runtime.SetObserverFunc(prevObserverFunc);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } }");
    }

} // Y_UNIT_TEST_SUITE(DataShardVolatile)

} // namespace NKikimr
