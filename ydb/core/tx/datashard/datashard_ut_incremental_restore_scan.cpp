#include "incr_restore_scan.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/util/testactorsys.h>

namespace NKikimr::NDataShard {

Y_UNIT_TEST_SUITE(IncrementalRestoreScan) {
    Y_UNIT_TEST(Simple) {
        TPortManager pm;
        Tests::TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        auto sender2 = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        TUserTable::TCPtr table;
        TPathId targetPathId{};
        ui64 txId = 0;

        auto scan = CreateIncrementalRestoreScan(
            sender,
            [&](const TActorContext&) {
                return sender2;
            },
            TPathId{} /*sourcePathId*/,
            table,
            targetPathId,
            txId);
    }
}

} // namespace NKikimr::NDataShard
