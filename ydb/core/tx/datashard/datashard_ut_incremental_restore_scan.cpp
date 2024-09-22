#include "incr_restore_scan.h"
#include "change_exchange_impl.h"

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/util/testactorsys.h>
#include <ydb/core/tx/scheme_board/helpers.h>

namespace NKikimr::NDataShard {

using namespace NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace NSchemeBoard;
using namespace Tests;

class TDriverMock
    : public NTable::IDriver
{
public:
    std::optional<NTable::EScan> LastScan;

    void Touch(NTable::EScan scan) noexcept {
        LastScan = scan;
    }
};

class TCbExecutorActor : public TActorBootstrapped<TCbExecutorActor> {
public:
    enum EEv {
        EvExec = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvBoot,
        EvExecuted,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvExec : public TEventLocal<TEvExec, EvExec> {
        std::function<void()> OnHandle;
        bool Async;

        TEvExec(std::function<void()> onHandle, bool async = true)
            : OnHandle(onHandle)
            , Async(async)
        {}
    };

    struct TEvBoot : public TEventLocal<TEvBoot, EvBoot> {};
    struct TEvExecuted : public TEventLocal<TEvExecuted, EvExecuted> {};

    std::function<void()> OnBootstrap;
    TActorId ReplyTo;
    TActorId ForwardTo;

    void Bootstrap() {
        if (OnBootstrap) {
            OnBootstrap();
        }

        Become(&TThis::Serve);
        Send(ReplyTo, new TCbExecutorActor::TEvBoot());
    }

    void Handle(TEvExec::TPtr& ev) {
        ev->Get()->OnHandle();
        if (!ev->Get()->Async) {
            Send(ReplyTo, new TCbExecutorActor::TEvExecuted());
        }
    }

    STATEFN(Serve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExec, Handle);
            default: Y_ABORT("unexpected");
        }
    }
};

class TRuntimeCbExecutor {
public:
    TRuntimeCbExecutor(TTestActorRuntime& runtime, std::function<void()> onBootstrap = {}, TActorId forwardTo = {})
        : Runtime(runtime)
        , Sender(runtime.AllocateEdgeActor())
    {
        auto* executor = new TCbExecutorActor;
        executor->OnBootstrap = onBootstrap;
        executor->ForwardTo = forwardTo;
        executor->ReplyTo = Sender;
        Impl = runtime.Register(executor);
        Runtime.EnableScheduleForActor(Impl);
        Runtime.GrabEdgeEventRethrow<TCbExecutorActor::TEvBoot>(Sender);
    }

    void AsyncExecute(std::function<void()> cb) {
        Runtime.Send(new IEventHandle(Impl, Sender, new TCbExecutorActor::TEvExec(cb), 0, 0), 0);
    }

    void Execute(std::function<void()> cb) {
        Runtime.Send(new IEventHandle(Impl, Sender, new TCbExecutorActor::TEvExec(cb, false), 0, 0), 0);
        Runtime.GrabEdgeEventRethrow<TCbExecutorActor::TEvExecuted>(Sender);
    }

private:
    TTestActorRuntime& Runtime;
    TActorId Sender;
    TActorId Impl;
};

static void SetupLogging(TTestActorRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_METACACHE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::CONTINUOUS_BACKUP, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);
}

TShardedTableOptions SimpleTable() {
    return TShardedTableOptions();
}

TMaybe<TPathId> GetTablePathId(TTestActorRuntime& runtime, TActorId sender, TString path) {
    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));

    auto reply = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDescribeSchemeResult>(sender);
    if (reply->Get()->GetRecord().GetStatus() != NKikimrScheme::EStatus::StatusSuccess) {
        return {};
    }

    return GetPathId(reply->Get()->GetRecord());
}

Y_UNIT_TEST_SUITE(IncrementalRestoreScan) {
    Y_UNIT_TEST(Empty) {
        TPortManager pm;
        Tests::TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        auto sender2 = runtime.AllocateEdgeActor();

        SetupLogging(runtime);

        TUserTable::TPtr table = new TUserTable;
        NTable::TScheme::TTableSchema tableSchema;
        table->Columns.emplace(0, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "Key", true));
        tableSchema.Columns[0] = NTable::TColumn("key", 0, {}, "");
        tableSchema.Columns[0].KeyOrder = 0;

        table->Columns.emplace(1, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Bool), "", "__ydb_incrBackupImpl_deleted", false));
        tableSchema.Columns[1] = NTable::TColumn("__ydb_incrBackupImpl_deleted", 1, {}, "");
        tableSchema.Columns[1].KeyOrder = 1;

        auto scheme = NTable::TRowScheme::Make(tableSchema.Columns, NUtil::TSecond());

        TPathId sourcePathId{1, 2};
        TPathId targetPathId{3, 4};
        ui64 txId = 1337;

        auto* scan = CreateIncrementalRestoreScan(
            sender,
            [&](const TActorContext&) {
                return sender2;
            },
            sourcePathId,
            table,
            targetPathId,
            txId,
            {}).Release();

        TDriverMock driver;

        // later we can use driver, scan and scheme ONLY with additional sync, e.g. from actorExec to avoid races
        TRuntimeCbExecutor actorExec(runtime, [&]() {
            scan->Prepare(&driver, scheme);
        });

        actorExec.Execute([&]() {
            UNIT_ASSERT_EQUAL(scan->Exhausted(), NTable::EScan::Sleep);
        });

        auto resp = runtime.GrabEdgeEventRethrow<TEvIncrementalRestoreScan::TEvNoMoreData>(sender2);

        runtime.Send(new IEventHandle(resp->Sender, sender2, new TEvIncrementalRestoreScan::TEvFinished(), 0, 0), 0);

        actorExec.Execute([&]() {
            UNIT_ASSERT(driver.LastScan && *driver.LastScan == NTable::EScan::Final);
            scan->Finish(NTable::EAbort::None);
        });

        runtime.GrabEdgeEventRethrow<TEvIncrementalRestoreScan::TEvFinished>(sender);
    }

    Y_UNIT_TEST(ChangeSender) {
        TPortManager pm;
        Tests::TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        TPathId targetPathId = *GetTablePathId(runtime, edgeActor, "/Root/Table");

        CreateShardedTable(server, edgeActor, "/Root", "IncrBackupTable", SimpleTable());

        TPathId sourcePathId = *GetTablePathId(runtime, edgeActor, "/Root/IncrBackupTable");

        auto* changeSender = CreateIncrRestoreChangeSender(edgeActor, sourcePathId, targetPathId);

        auto changeSenderActor = runtime.Register(changeSender);
        runtime.EnableScheduleForActor(changeSenderActor);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        auto request = MakeHolder<TEvIncrementalRestoreScan::TEvNoMoreData>();
        runtime.Send(new IEventHandle(changeSenderActor, edgeActor, request.Release()));

        runtime.GrabEdgeEventRethrow<TEvIncrementalRestoreScan::TEvFinished>(edgeActor);
    }
}

} // namespace NKikimr::NDataShard
