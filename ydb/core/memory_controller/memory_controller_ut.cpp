#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/testlib/test_runtime.h>

namespace NKikimr::NMemory {

using namespace Tests;

namespace {
    void UpsertRows(TServer::TPtr server, TActorId sender, ui32 keyFrom = 0, ui32 keyTo = 2000) {
        TString query = "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (auto key : xrange(keyFrom, keyTo)) {
            if (key != keyFrom)
                query += ", ";
            query += "(" + ToString(key) + ", " + ToString(key) + ") ";
        }
        ExecSQL(server, sender, query);
    }
}

Y_UNIT_TEST_SUITE(TMemoryController) {

Y_UNIT_TEST(SharedCache) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    TIntrusivePtr<TServer> server = new TServer(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    auto sharedCacheCounters = MakeIntrusive<TSharedPageCacheCounters>(runtime.GetDynamicCounters());

    runtime.SetLogPriority(NKikimrServices::MEMORY_CONTROLLER, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);

    InitRoot(server, sender);

    auto [shards, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    ui64 shard1 = shards.at(0);

    UpsertRows(server, sender);

    CompactTable(runtime, shard1, tableId1, false);

    Cerr << "ActiveBytes = " << sharedCacheCounters->ActiveBytes->Val() << " PassiveBytes = " << sharedCacheCounters->PassiveBytes->Val() << Endl;
    Cerr << "ConfigLimitBytes = " << sharedCacheCounters->ConfigLimitBytes->Val() << " MemLimitBytes = " << sharedCacheCounters->MemLimitBytes->Val() << Endl;
}

}

}
