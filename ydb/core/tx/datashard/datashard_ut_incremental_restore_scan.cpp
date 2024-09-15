#include "incr_restore_scan.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/util/testactorsys.h>

namespace NKikimr::NDataShard {

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

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

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
}

} // namespace NKikimr::NDataShard
