#include "incr_restore_scan.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/util/testactorsys.h>

namespace NKikimr::NDataShard {

class TDriverMock
    : public NTable::IDriver
{
public:
    void Touch(NTable::EScan) noexcept {

    }
};

class TCbExecutorActor : public TActorBootstrapped<TCbExecutorActor> {
public:
    std::function<void()> Cb;

    void Bootstrap() {
        Cb();
    }
};

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

        TUserTable::TPtr table = new TUserTable;

        table->Columns.emplace(0, TUserTable::TUserColumn{});

        NTable::TScheme::TTableSchema tableSchema;
        tableSchema.Columns[0] = NTable::TColumn("test", 0, {}, "");
        tableSchema.Columns[0].KeyOrder = 0;
        auto scheme = NTable::TRowScheme::Make(tableSchema.Columns, NUtil::TSecond());

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

        TDriverMock driver;
        auto* executor = new TCbExecutorActor;
        executor->Cb = [&]() {
            scan->Prepare(&driver, scheme);
        };
        auto executorActor = runtime.Register(executor);
        runtime.EnableScheduleForActor(executorActor);

        auto resp = runtime.GrabEdgeEventRethrow<TEvIncrementalRestoreScan::TEvFinished>(sender);
        Y_UNUSED(resp);
    }
}

} // namespace NKikimr::NDataShard
