#include <ydb/core/base/appdata.h>
#include <ydb/core/engine/mkql_engine_flat_impl.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/locks/locks.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

static const bool EnableLogs = false;

namespace NKikimr {
namespace NLocksTests {

using namespace Tests;

///
class TFlatMsgBusClient : public TClient {
public:
    TFlatMsgBusClient(ui16 port)
        : TClient(TServerSettings(port))
    {}

    void InitRoot() {
        InitRootScheme();
    }

    using TClient::FlatQuery;

    void FlatQuery(const TString& mkql) {
        NKikimrMiniKQL::TResult res;
        TClient::TFlatQueryOptions opts;
        bool success = TClient::FlatQuery(mkql, opts, res);
        UNIT_ASSERT(success);
    }

    TAutoPtr<NMsgBusProxy::TBusResponse> LsPathId(ui64 schemeshardId, ui64 pathId) {
        TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
        request->Record.SetPathId(pathId);
        request->Record.SetSchemeshardId(schemeshardId);
        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus msgStatus = SendWhenReady(request, reply);
        UNIT_ASSERT_VALUES_EQUAL(msgStatus, NBus::MESSAGE_OK);
        Cout << PrintToString<NMsgBusProxy::TBusResponse>(reply.Get()) << Endl;
        return dynamic_cast<NMsgBusProxy::TBusResponse*>(reply.Release());
    }

    void ResetSchemeCache(TServer server, TTableId tableId) {
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId txProxy = MakeTxProxyID();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<TEvTxUserProxy::TEvInvalidateTable> ev(new TEvTxUserProxy::TEvInvalidateTable(tableId));
        runtime->Send(new IEventHandle(txProxy, sender, ev.Release()));
        TAutoPtr<IEventHandle> handle;
        auto readSchemeStringResult = runtime->GrabEdgeEventRethrow<TEvTxUserProxy::TEvInvalidateTableResult>(handle);
        Y_UNUSED(readSchemeStringResult);
    }
};

///
struct TClientServer {
    TPortManager PortManager;
    ui16 Port;
    TServer Server;
    TFlatMsgBusClient Client;

    TClientServer(bool outOfOrder = false, bool softUpdates = false, ui16 portNo = 2134)
        : Port(PortManager.GetPort(portNo))
        , Server(TServerSettings(Port))
        , Client(Port)
    {
        SetLogging(EnableLogs);
        Client.InitRoot();

        TString addition = "UniformPartitionsCount: 2 ";
        if (outOfOrder && softUpdates) {
            addition += "PartitionConfig { PipelineConfig { NumActiveTx: 8 EnableOutOfOrder: true EnableSoftUpdates: true }}";
        } else if (outOfOrder) {
            addition += "PartitionConfig { PipelineConfig { NumActiveTx: 8 EnableOutOfOrder: true }}";
        } else if (softUpdates) {
            addition += "PartitionConfig { PipelineConfig { EnableSoftUpdates: true }}";
        }

        const char * tableA = R"(Name: "A"
            Columns { Name: "key"    Type: "Uint32" }
            Columns { Name: "value"  Type: "Uint32" }
            KeyColumnNames: ["key"]
            %s)";

        const char * tableB = R"(Name: "B"
            Columns { Name: "key"    Type: "Uint32" }
            Columns { Name: "keyX"   Type: "Uint64" }
            Columns { Name: "value"  Type: "Uint32" }
            KeyColumnNames: ["key", "keyX"]
            %s)";

        const char * tableC = R"(Name: "C"
            Columns { Name: "key"    Type: "String" }
            Columns { Name: "value"  Type: "String" }
            KeyColumnNames: ["key"])";

        Client.MkDir("/dc-1", "Dir");
        Client.CreateTable("/dc-1/Dir", Sprintf(tableA, addition.data()).data());
        Client.CreateTable("/dc-1/Dir", Sprintf(tableB, addition.data()).data());
        Client.CreateTable("/dc-1/Dir", tableC);
    }

    void SetLogging(bool enableLogs = true) {
        if (enableLogs) {
            //Server.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            Server.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            Server.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
            Server.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        }
    }
};

using NKikimr::NClient::TValue;

///
struct TLocksV1 {
    static constexpr const char * TableName() { return "/sys/locks"; }
    static constexpr const char * Columns() { return "'LockId 'DataShard 'Generation 'Counter"; }
    static constexpr const char * ResultLabel() { return NMiniKQL::TxLocksResultLabel.data(); }

    static TString Key(ui64 lockId, ui64 datashard, ui64 schemeshard, ui64 pathId) {
        Y_UNUSED(schemeshard);
        Y_UNUSED(pathId);
        return Sprintf("'('LockId (Uint64 '%lu)) '('DataShard (Uint64 '%lu))", lockId, datashard);
    }

    static NMiniKQL::IEngineFlat::TTxLock ExtractLock(TValue l) {
        UNIT_ASSERT_VALUES_EQUAL(l.Size(), 4);
        ui64 lockId = l["LockId"];
        ui64 dataShard = l["DataShard"];
        ui32 generation = l["Generation"];
        ui64 counter = l["Counter"];
        return NMiniKQL::IEngineFlat::TTxLock(lockId, dataShard, generation, counter, -1, -1);
    }

    static void PrintLock(const NMiniKQL::IEngineFlat::TTxLock& lock) {
        Cout << "LockId: " << lock.LockId << ", "
            << "DataShard: " << lock.DataShard << ", "
            << "Generation: " << lock.Generation << ", "
            << "Counter: " << lock.Counter << Endl;
    }
};

///
struct TLocksV2 {
    static constexpr const char * TableName() { return "/sys/locks2"; }
    static constexpr const char * Columns() { return "'LockId 'DataShard 'Generation 'Counter 'SchemeShard 'PathId"; }
    static constexpr const char * ResultLabel() { return NMiniKQL::TxLocksResultLabel2.data(); }

    static TString Key(ui64 lockId, ui64 datashard, ui64 schemeshard, ui64 pathId) {
        return Sprintf(
            "'('LockId (Uint64 '%lu)) '('DataShard (Uint64 '%lu)) '('SchemeShard (Uint64 '%lu)) '('PathId (Uint64 '%lu))",
            lockId, datashard, schemeshard, pathId);
    }

    static NMiniKQL::IEngineFlat::TTxLock ExtractLock(TValue l) {
        UNIT_ASSERT_VALUES_EQUAL(l.Size(), 6);
        ui64 lockId = l["LockId"];
        ui64 dataShard = l["DataShard"];
        ui32 generation = l["Generation"];
        ui64 counter = l["Counter"];
        ui64 schemeShard = l["SchemeShard"];
        ui64 pathId = l["PathId"];
        return NMiniKQL::IEngineFlat::TTxLock(lockId, dataShard, generation, counter, schemeShard, pathId);
    }

    static void PrintLock(const NMiniKQL::IEngineFlat::TTxLock& lock) {
        Cout << "LockId: " << lock.LockId << ", "
            << "DataShard: " << lock.DataShard << ", "
            << "Generation: " << lock.Generation << ", "
            << "Counter: " << lock.Counter << ", "
            << "SchemeShard: "<< lock.SchemeShard << ", "
            << "PathId: " << lock.PathId << Endl;
    }
};

template<typename T>
static NMiniKQL::IEngineFlat::TTxLock ExtractRowLock(TValue l) {
    UNIT_ASSERT(l.HaveValue());
    return T::ExtractLock(l);
}

template<typename T>
static void ExtractResultLocks(const NKikimrMiniKQL::TResult& result, TVector<NMiniKQL::IEngineFlat::TTxLock>& txLocks) {
    TValue value = TValue::Create(result.GetValue(), result.GetType());
    TValue label = value[T::ResultLabel()];
    UNIT_ASSERT(label.HaveValue());
    for (ui32 i = 0; i < label.Size(); ++i) {
        txLocks.emplace_back(T::ExtractLock(label[i]));
    }
}

///
struct TLocksTestOptions {
    bool OutOfOrder = false;
    bool SoftUpdates = false;

    bool Break = false;
    bool BreakFail = false;
    bool TestErase = false;
    bool Dup = false;
    bool NoLocks = false;
    bool TestRange = false;
    bool SetOnly = false;
    ui32* LocksExpected = nullptr;

    const char * Table = nullptr;
    const char * BreakPoint = nullptr;
    const char * BreakKey = nullptr;
    const char * NoBreakKey0 = nullptr;
    const char * NoBreakKey1 = nullptr;
    const char * UpdateKey = nullptr;
    const char * UpdateValue = nullptr;

    const char * Range0Inc = nullptr;
    const char * Range0Begin = nullptr;
    const char * Range0End = nullptr;
    const char * Range0OptKey = nullptr;
    const char * Range1Inc = nullptr;
    const char * Range1Begin = nullptr;
    const char * Range1End = nullptr;
    const char * Range1OptKey = nullptr;

    TLocksTestOptions() {
        SetTableA();
    }

    void SetTableA() {
        Table = "/dc-1/Dir/A";
        BreakPoint = "(Uint32 '42)";
        BreakKey = "'('key (Uint32 '42))";
        NoBreakKey0 = "'('key (Uint32 '4200000000))";
        NoBreakKey1 = "'('key (Uint32 '4200000001))";
        UpdateKey = "'('key (Uint32 '0))";
        UpdateValue = "(Uint32 '100)";

        Range0Inc = "'ExcFrom 'IncTo";
        Range0Begin = "(Uint32 '0)";
        Range0End = BreakPoint;
        Range0OptKey = "";
        Range1Inc = "'ExcFrom 'IncTo";
        Range1Begin = BreakPoint;
        Range1End = "(Void)";
        Range1OptKey = "";
    }

    void SetTableB() {
        Table = "/dc-1/Dir/B";
        BreakPoint = "(Uint32 '42)";
        BreakKey = "'('key (Uint32 '42)) '('keyX (Uint64 '0))";
        NoBreakKey0 = "'('key (Uint32 '4200000000)) '('keyX (Uint64 '0))";
        NoBreakKey1 = "'('key (Uint32 '4200000001)) '('keyX (Uint64 '0))";
        UpdateKey = "'('key (Uint32 '0)) '('keyX (Uint64 '0))";
        UpdateValue = "(Uint32 '100)";

        Range0Inc = "'ExcFrom 'IncTo";
        Range0Begin = "(Uint32 '0)";
        Range0End = BreakPoint;
        Range0OptKey = "'('keyX (Uint64 '0) (Void))";
        Range1Inc = "'ExcFrom 'IncTo";
        Range1Begin = BreakPoint;
        Range1End = "(Uint32 '4294967295)";
        Range1OptKey = "'('keyX (Uint64 '0) (Void))";
    }

    void SetTableC() {
        Table = "/dc-1/Dir/C";
        BreakPoint = "(String '\"42\")";
        BreakKey = "'('key (String '\"42\"))";
        NoBreakKey0 = "'('key (String '\"\"))"; // breaks (no partitions for String key)
        NoBreakKey1 = "'('key (String '\"\"))"; // breaks (no partitions for String key)
        UpdateKey = "'('key (String '\"0\"))";
        UpdateValue = "(String '\"100\")";

        Range0Inc = "'IncFrom 'IncTo";
        Range0Begin = "(String '\"0\")";
        Range0End = BreakPoint;
        Range0OptKey = "";
        Range1Inc = "'ExcFrom 'IncTo";
        Range1Begin = BreakPoint;
        Range1End = "(Void)";
        Range1OptKey = "";
    }
};

template <typename TLocksVer>
void TestLock(const TLocksTestOptions& testOpts) {
    TClientServer cs(testOpts.OutOfOrder, testOpts.SoftUpdates);
    NKikimrMiniKQL::TResult res;

    ui64 txLockId = 0;
    auto getSetLocks = [&testOpts, &txLockId] () {
        if (testOpts.TestRange) {
            const char * setLocksT = R"___((
                (let table_ '%s)
                (let range0_ '(%s '('key %s %s) %s))
                (let range1_ '(%s '('key %s %s) %s))
                (let cols_ '('key 'value))
                (let ret_ (AsList
                    (SetResult 'res0 (SelectRange table_ range0_ cols_ '()))
                    (SetResult 'res1 (SelectRange table_ range1_ cols_ '()))
                    (AcquireLocks (Uint64 '%lu))
                ))
                (return ret_)
            ))___";
            return Sprintf(setLocksT, testOpts.Table,
                           testOpts.Range0Inc, testOpts.Range0Begin, testOpts.Range0End, testOpts.Range0OptKey,
                           testOpts.Range1Inc, testOpts.Range1Begin, testOpts.Range1End, testOpts.Range1OptKey, txLockId);
        } else {
            const char * setLocksT = R"___((
                (let table_ '%s)
                (let row0_ '(%s))
                (let row1_ '(%s))
                (let row2_ '(%s))
                (let cols_ '('value))
                (let select0_ (SelectRow table_ row0_ cols_))
                (let select1_ (SelectRow table_ row1_ cols_))
                (let select2_ (SelectRow table_ row2_ cols_))
                (let ret_ (AsList
                    (SetResult 'res0 select0_)
                    (SetResult 'res1 select1_)
                    (SetResult 'res2 select2_)
                    (SetResult 'txid (Nth (StepTxId) '1))
                    (AcquireLocks (Uint64 '%lu))
                ))
                (return ret_)
            ))___";
            return Sprintf(setLocksT, testOpts.Table,
                           testOpts.BreakKey, testOpts.NoBreakKey0, testOpts.NoBreakKey1, txLockId);
        }
    };

    TVector<NMiniKQL::IEngineFlat::TTxLock> txLocks;

    if (testOpts.NoLocks) {
        ui64 ssId = Max<ui64>();
        ui64 pathId = 3;
        txLocks.emplace_back(NMiniKQL::IEngineFlat::TTxLock{281474976710659, 72075186224037888, 2, 0, ssId, pathId});
        txLocks.emplace_back(NMiniKQL::IEngineFlat::TTxLock{281474976710659, 72075186224037889, 2, 0, ssId, pathId});
        txLockId = 281474976710659;
    } else {
        cs.Client.FlatQuery(getSetLocks(), res);
        ExtractResultLocks<TLocksVer>(res, txLocks);
        UNIT_ASSERT(txLocks.size() > 0);
        txLockId = txLocks[0].LockId;
    }

    for (auto& lock : txLocks)
        TLocksVer::PrintLock(lock);

    if (testOpts.SetOnly)
        return;

    UNIT_ASSERT_VALUES_EQUAL(txLocks.size(), 2);

    const char * breakLock = nullptr;
    if (!testOpts.TestErase) {
        // break lock at first shard
        breakLock = R"___((
            (let table_ '%s)
            (let row0_ '(%s))
            (let update_ '('('value (Uint32 '0))))
            (let breakTx_ (Bool '%s))
            (let ret_ (Extend
                (AsList (SetResult 'broken breakTx_))
                (ListIf breakTx_ (UpdateRow table_ row0_ update_))
            ))
            (return ret_)
        ))___";
    } else {
        breakLock = R"___((
            (let table_ '%s)
            (let row0_ '(%s))
            (let breakTx_ (Bool '%s))
            (let ret_ (Extend
                (AsList (SetResult 'broken breakTx_))
                (ListIf breakTx_ (EraseRow table_ row0_))
            ))
            (return ret_)
        ))___";
    }
    cs.Client.FlatQuery(Sprintf(breakLock, testOpts.Table, testOpts.BreakKey, (testOpts.Break ? "true" : "false") ));

    if (testOpts.Dup) {
        cs.Client.FlatQuery(getSetLocks(), res);
        ExtractResultLocks<TLocksVer>(res, txLocks);
    }

    const char * commit = R"___((
        (let table_ '%s)
        (let locksTable_ '%s)
        (let lockCols_ '(%s))
        (let lockKey0_ '(%s))
        (let lockKey1_ '(%s))
        (let gen0_ (Uint32 '%u))
        (let counter0_ (Uint64 '%lu))
        (let gen1_ (Uint32 '%u))
        (let counter1_ (Uint64 '%lu))
        (let lock1_ (SelectRow locksTable_ lockKey0_ lockCols_))
        (let lock2_ (SelectRow locksTable_ lockKey1_ lockCols_))
        (let curLocks_ (AsList lock1_ lock2_))
        (let ok1_ (IfPresent lock1_ (lambda '(x) (Coalesce
                (And (Equal (Member x 'Generation) gen0_)
                        (Equal (Member x 'Counter) counter0_))
                (Bool 'false)))
            (Bool 'false)
        ))
        (let ok2_ (IfPresent lock2_ (lambda '(x) (Coalesce
                (And (Equal (Member x 'Generation) gen1_)
                        (Equal (Member x 'Counter) counter1_))
                (Bool 'false)))
            (Bool 'false)
        ))
        (let checks_ (AsList ok1_ ok2_))
        (let goods_ (Filter checks_ (lambda '(x) (Equal x (Bool 'true)))))
        (let linksOk_ (Equal (Length goods_) (Uint64 '2)))
        (let row0_ '(%s))
        (let update_ '('('value %s)))
        (let return_ (Extend
            (AsList (SetResult 'done linksOk_)
                    (SetResult 'locks curLocks_)
                    (SetResult 'checks checks_))
            (ListIf linksOk_ (UpdateRow table_ row0_ update_))
        ))
        (return return_)
    ))___";

    cs.Client.FlatQuery(Sprintf(commit, testOpts.Table,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(txLocks[0].LockId, txLocks[0].DataShard, txLocks[0].SchemeShard, txLocks[0].PathId).data(),
        TLocksVer::Key(txLocks[1].LockId, txLocks[1].DataShard, txLocks[1].SchemeShard, txLocks[1].PathId).data(),
        txLocks[0].Generation, txLocks[0].Counter,
        txLocks[1].Generation, txLocks[1].Counter,
        testOpts.UpdateKey, testOpts.UpdateValue), res);

    {
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue locks = result["locks"];
        UNIT_ASSERT(locks.HaveValue());
        UNIT_ASSERT_VALUES_EQUAL(locks.Size(), 2);
        for (ui32 i = 0; i < locks.Size(); ++i) {
            TValue lock = locks[i];
            if (!lock.HaveValue()) {
                Cout << "Lock is broken" << Endl;
                continue;
            }
            TLocksVer::PrintLock(ExtractRowLock<TLocksVer>(lock));
        }

        TValue checks = result["checks"];
        UNIT_ASSERT(checks.HaveValue());
        UNIT_ASSERT_VALUES_EQUAL(checks.Size(), 2);

        ui32 numOK = 0;
        Cout << "Locks: ";
        for (ui32 i = 0; i < checks.Size(); ++i) {
            bool ok = checks[i];
            Cout << ok << ", ";
            if (ok)
                ++numOK;
        }
        Cout << Endl;

        if (testOpts.LocksExpected) {
            UNIT_ASSERT(numOK == *testOpts.LocksExpected);
        } else if (testOpts.Break) {
            UNIT_ASSERT(numOK == 1);
        } else {
            UNIT_ASSERT(numOK == 2);
        }
    }

    const char * checkUpdatedT = R"___((
        (let table_ '%s)
        (let row0_ '(%s))
        (let cols_ '('value))
        (let select0_ (SelectRow table_ row0_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
        ))
        (return ret_)
    ))___";
    TString checkUpdated = Sprintf(checkUpdatedT, testOpts.Table, testOpts.UpdateKey);
    cs.Client.FlatQuery(checkUpdated, res);

    {
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue val = result["res0"];
        UNIT_ASSERT_EQUAL(val.HaveValue(), (!testOpts.Break || testOpts.BreakFail));

        if (val.HaveValue()) {
            Cout << "Transaction completed" << Endl;
        } else {
            Cout << "Transaction aborted" << Endl;
        }
    }

    const char * eraseLocks = R"___((
        (let locksTable_ '%s)
        (let lockKey0_ '(%s))
        (let lockKey1_ '(%s))
        (return (AsList
            (EraseRow locksTable_ lockKey0_)
            (EraseRow locksTable_ lockKey1_)
        ))
    ))___";

    cs.Client.FlatQuery(Sprintf(eraseLocks,
        TLocksVer::TableName(),
        TLocksVer::Key(txLocks[0].LockId, txLocks[0].DataShard, txLocks[0].SchemeShard, txLocks[0].PathId).data(),
        TLocksVer::Key(txLocks[1].LockId, txLocks[1].DataShard, txLocks[1].SchemeShard, txLocks[1].PathId).data()), res);

    const char * checkErased = R"___((
        (let locksTable_ '%s)
        (let lockCols_ '(%s))
        (let lockKey0_ '(%s))
        (let lockKey1_ '(%s))
        (return (AsList
            (SetResult 'Result (AsList
                (SelectRow locksTable_ lockKey0_ lockCols_)
                (SelectRow locksTable_ lockKey1_ lockCols_)
            ))
        ))
    ))___";

    cs.Client.FlatQuery(Sprintf(checkErased,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(txLocks[0].LockId, txLocks[0].DataShard, txLocks[0].SchemeShard, txLocks[0].PathId).data(),
        TLocksVer::Key(txLocks[1].LockId, txLocks[1].DataShard, txLocks[1].SchemeShard, txLocks[1].PathId).data()), res);

    {
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue out = result["Result"];
        UNIT_ASSERT(out.HaveValue());
        UNIT_ASSERT_VALUES_EQUAL(out.Size(), 2);

        for (ui32 i = 0; i < out.Size(); ++i) {
            TValue lock = out[i];
            UNIT_ASSERT(!lock.HaveValue());
            Cout << "Erased lock" << Endl;
        }
    }
}

/// suite TLocksTest
Y_UNIT_TEST_SUITE(TLocksTest) {

// Point locks

Y_UNIT_TEST(GoodLock) {
    TLocksTestOptions opts;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(BrokenLockUpdate) {
    TLocksTestOptions opts;
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(BrokenLockErase) {
    TLocksTestOptions opts;
    opts.Break = true;
    opts.TestErase = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(UpdateLockedKey) {
    TLocksTestOptions opts;
    opts.Break = false;
    opts.UpdateKey = opts.BreakKey;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(NoLocksSet) {
    TLocksTestOptions opts;
    opts.Break = true;
    opts.NoLocks = true;
    ui32 numLocks = 0;
    opts.LocksExpected = &numLocks;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(GoodSameKeyLock) {
    TLocksTestOptions opts;
    opts.NoBreakKey1 = opts.NoBreakKey0;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(BrokenSameKeyLock) {
    TLocksTestOptions opts;
    opts.Break = true;
    opts.NoBreakKey0 = opts.BreakKey;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(GoodSameShardLock) {
    TLocksTestOptions opts;
    opts.NoBreakKey0 = "'('key (Uint32 '40))";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(BrokenSameShardLock) {
    TLocksTestOptions opts;
    opts.Break = true;
    opts.NoBreakKey0 = "'('key (Uint32 '40))";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(GoodDupLock) {
    TLocksTestOptions opts;
    opts.Dup = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(BrokenDupLock) {
    TLocksTestOptions opts;
    opts.Break = true;
    opts.Dup = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(GoodNullLock) {
    TLocksTestOptions opts;
    opts.BreakKey = "'('key (Null))";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(BrokenNullLock) {
    TLocksTestOptions opts;
    opts.Break = true;
    opts.BreakKey = "'('key (Null))";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// Range locks

// (0, 42] (42, inf)
Y_UNIT_TEST(Range_GoodLock0) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (Null, 42] (42*10^8, inf)
Y_UNIT_TEST(Range_GoodLock1) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Begin = "(Null)";
    opts.Range1Begin = "(Uint32 '4200000000)";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (0, x] (x, inf)
Y_UNIT_TEST(Range_BrokenLock0) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (0, x] (42*10^8, inf)
Y_UNIT_TEST(Range_BrokenLock1) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range1Begin = "(Uint32 '4200000000)";
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (0, x) [x, inf)
Y_UNIT_TEST(Range_BrokenLock2) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'ExcFrom 'ExcTo";
    opts.Range1Inc = "'IncFrom 'IncTo";
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (0, x] [x, inf)
Y_UNIT_TEST(Range_BrokenLock3) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'ExcFrom 'IncTo";
    opts.Range1Inc = "'IncFrom 'IncTo";
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (0, x] (x, inf)
Y_UNIT_TEST(Range_BrokenLockMax) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.BreakKey = "'('key (Uint32 '4294967295))";
    opts.Range1Inc = "'ExcFrom 'IncTo";
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (0, x) (x, inf)
Y_UNIT_TEST(Range_Pinhole) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'ExcFrom 'ExcTo";
    opts.Break = true;
    opts.BreakFail = true;
    ui32 numLocks = 2;
    opts.LocksExpected = &numLocks;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// [x, x] (x, inf)
Y_UNIT_TEST(Range_CorrectDot) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'IncFrom 'IncTo";
    opts.Range0Begin = opts.BreakPoint;
    opts.Range0End = opts.BreakPoint;
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (x, x] (x, inf)
Y_UNIT_TEST(Range_IncorrectDot1) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'ExcFrom 'IncTo";
    opts.Range0Begin = opts.BreakPoint;
    opts.Range0End = opts.BreakPoint;
    opts.Break = true;
    opts.BreakFail = true;
    ui32 numLocks = 2;
    opts.LocksExpected = &numLocks;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// [x, x) (x, inf)
Y_UNIT_TEST(Range_IncorrectDot2) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'IncFrom 'ExcTo";
    opts.Range0Begin = opts.BreakPoint;
    opts.Range0End = opts.BreakPoint;
    opts.Break = true;
    opts.BreakFail = true;
    ui32 numLocks = 2;
    opts.LocksExpected = &numLocks;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// [Null, Null] (x, inf)
Y_UNIT_TEST(Range_CorrectNullDot) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'IncFrom 'IncTo";
    opts.BreakKey = "'('key (Null))";
    opts.Range0Begin = "(Null)";
    opts.Range0End = "(Null)";
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// (Null, Null] (x, inf)
Y_UNIT_TEST(Range_IncorrectNullDot1) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'ExcFrom 'IncTo";
    opts.BreakKey = "'('key (Null))";
    opts.Range0Begin = "(Null)";
    opts.Range0End = "(Null)";
    opts.Break = true;
    opts.BreakFail = true;
    ui32 numLocks = 2;
    opts.LocksExpected = &numLocks;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// [Null, Null) (x, inf)
Y_UNIT_TEST(Range_IncorrectNullDot2) {
    TLocksTestOptions opts;
    opts.TestRange = true;
    opts.Range0Inc = "'IncFrom 'ExcTo";
    opts.BreakKey = "'('key (Null))";
    opts.Range0Begin = "(Null)";
    opts.Range0End = "(Null)";
    opts.Break = true;
    opts.BreakFail = true;
    ui32 numLocks = 2;
    opts.LocksExpected = &numLocks;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// test empty key
Y_UNIT_TEST(Range_EmptyKey) {
    TLocksTestOptions opts;
    opts.SetTableC();
    opts.SetOnly = true;
    //opts.TestErase = true;
    opts.TestRange = true;
    opts.Range0Inc = "'IncFrom 'IncTo";
    opts.Range0Begin = "(String '\"\")";
    opts.Range0End = "(Void)";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

// Composite key

Y_UNIT_TEST(CK_GoodLock) {
    TLocksTestOptions opts;
    opts.SetTableB();
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(CK_BrokenLock) {
    TLocksTestOptions opts;
    opts.SetTableB();
    opts.Break = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(CK_Range_GoodLock) {
    TLocksTestOptions opts;
    opts.SetTableB();
    opts.TestRange = true;
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(CK_Range_BrokenLock) {
    TLocksTestOptions opts;
    opts.SetTableB();
    opts.TestRange = true;
    opts.Break = true;
    opts.Range0OptKey = "'('keyX (Uint64 '0) (Uint64 '1))";
    opts.Range1OptKey = "'('keyX (Uint64 '0) (Uint64 '1))";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

Y_UNIT_TEST(CK_Range_BrokenLockInf) {
    TLocksTestOptions opts;
    opts.SetTableB();
    opts.TestRange = true;
    opts.Break = false;
    opts.Range0OptKey = "'('keyX (Null) (Void))";
    opts.Range1OptKey = "'('keyX (Null) (Void))";
    TestLock<TLocksV1>(opts);
    TestLock<TLocksV2>(opts);

    opts.OutOfOrder = true;
    TestLock<TLocksV2>(opts);
    opts.SoftUpdates = true;
    TestLock<TLocksV2>(opts);
    opts.OutOfOrder = false;
    TestLock<TLocksV2>(opts);
}

//

template <typename TLocksVer>
static void MultipleLocks() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    const char * q1 = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(q1, res);
    TVector<NMiniKQL::IEngineFlat::TTxLock> locks1;
    ExtractResultLocks<TLocksVer>(res, locks1);

    {
        UNIT_ASSERT_VALUES_EQUAL(locks1.size(), 1);
        for (const auto& l : locks1)
            TLocksVer::PrintLock(l);
    }

    const char * someUpdate = R"___((
        (let row0_ '('('key (Uint32 '100))))
        (let ret_ (AsList
            (EraseRow '/dc-1/Dir/A row0_)
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(someUpdate);

    const char * q2 = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let row1_ '('('key (Uint32 '4200000000))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (let select1_ (SelectRow '/dc-1/Dir/A row1_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
            (SetResult 'res1 select1_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(q2, res);
    TVector<NMiniKQL::IEngineFlat::TTxLock> locks2;
    ExtractResultLocks<TLocksVer>(res, locks2);

    {
        UNIT_ASSERT_VALUES_EQUAL(locks2.size(), 2);
        for (const auto& l : locks2)
            TLocksVer::PrintLock(l);
    }

    const char * selectLocksT = R"___((
        (let locksTable_ '%s)
        (let lockCols_ '(%s))
        (let lockKey0_ '(%s))
        (let lockKey1_ '(%s))
        (let lockKey2_ '(%s))
        (return (AsList
            (SetResult 'lock0 (SelectRow locksTable_ lockKey0_ lockCols_))
            (SetResult 'lock1 (SelectRow locksTable_ lockKey1_ lockCols_))
            (SetResult 'lock2 (SelectRow locksTable_ lockKey2_ lockCols_))
        ))
    ))___";
    TString selectLocks = Sprintf(selectLocksT,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(locks1[0].LockId, locks1[0].DataShard, locks1[0].SchemeShard, locks1[0].PathId).data(),
        TLocksVer::Key(locks2[0].LockId, locks2[0].DataShard, locks2[0].SchemeShard, locks2[0].PathId).data(),
        TLocksVer::Key(locks2[1].LockId, locks2[1].DataShard, locks2[1].SchemeShard, locks2[1].PathId).data());

    { // select locks
        cs.Client.FlatQuery(selectLocks, res);

        TValue result = TValue::Create(res.GetValue(), res.GetType());

        TVector<TString> names{"lock0", "lock1", "lock2"};
        TVector<NMiniKQL::IEngineFlat::TTxLock> outLocks;
        TSet<ui64> uniqueLockId;
        TSet<ui64> uniqueShards;
        TSet<ui64> uniqueCounters;
        for (const auto& name : names) {
            TValue lock = result[name];
            UNIT_ASSERT(lock.HaveValue());
            NMiniKQL::IEngineFlat::TTxLock out = ExtractRowLock<TLocksVer>(lock);
            outLocks.push_back(out);

            uniqueLockId.insert(out.LockId);
            uniqueShards.insert(out.DataShard);
            uniqueCounters.insert(out.Counter);
        }

        for (const auto& l : outLocks)
            TLocksVer::PrintLock(l);

        UNIT_ASSERT_VALUES_EQUAL(outLocks.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(uniqueLockId.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(uniqueShards.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(uniqueCounters.size(), 2);
    }

    const char * eraseLock = R"___((
        (let locksTable_ '%s)
        (let lock_ '(%s))
        (return (AsList
            (EraseRow locksTable_ lock_)
        ))
    ))___";
    cs.Client.FlatQuery(Sprintf(eraseLock,
        TLocksVer::TableName(),
        TLocksVer::Key(locks1[0].LockId, locks1[0].DataShard, locks1[0].SchemeShard, locks1[0].PathId).data()), res);

    { // select locks
        cs.Client.FlatQuery(selectLocks, res);

        TValue result = TValue::Create(res.GetValue(), res.GetType());

        TVector<TString> names{"lock0", "lock1", "lock2"};
        TVector<NMiniKQL::IEngineFlat::TTxLock> outLocks;
        for (const auto& name : names) {
            TValue lock = result[name];
            if (lock.HaveValue()) {
                outLocks.push_back(ExtractRowLock<TLocksVer>(lock));
            } else {
                UNIT_ASSERT_EQUAL(name, "lock0");
            }
        }

        for (const auto& l : outLocks)
            TLocksVer::PrintLock(l);
        UNIT_ASSERT_VALUES_EQUAL(outLocks.size(), 2);
    }
}

Y_UNIT_TEST(MultipleLocks) {
    MultipleLocks<TLocksV1>();
    MultipleLocks<TLocksV2>();
}

Y_UNIT_TEST(SetLockFail) {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    TClient::TFlatQueryOptions opts;

    // lock+update
    const char * lockUpdate = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let update_ '('('value (Uint32 '0))))
        (let ret_ (AsList
            (UpdateRow '/dc-1/Dir/A row0_ update_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(lockUpdate, opts, res, NMsgBusProxy::MSTATUS_REJECTED);

    // lock+erase
    const char * lockErase = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let ret_ (AsList
            (EraseRow '/dc-1/Dir/A row0_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(lockErase, opts, res, NMsgBusProxy::MSTATUS_REJECTED);
}

template <typename TLocksVer>
static void SetLockNothing() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;
    TClient::TFlatQueryOptions opts;

    // lock + no reads
    const char * lockNothing = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let row1_ '('('key (Uint32 '4200000000))))
        (let select1_ (SelectRow '/dc-1/Dir/A row1_ '('value)))
        (let cmp_ (IfPresent select1_ (lambda '(x) (Coalesce (Equal (Member x 'value) (Uint32 '4)) (Bool 'false))) (Bool 'false)))
        (let fakeRead_ (ListIf cmp_ (SelectRow '/dc-1/Dir/A row0_ '('value))))
        (let ret_ (AsList
            (SetResult 'FakeRead fakeRead_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(lockNothing, opts, res);

    TVector<NMiniKQL::IEngineFlat::TTxLock> locks;
    ExtractResultLocks<TLocksVer>(res, locks);
    for (const auto& l : locks) {
        TLocksVer::PrintLock(l);
    }
}

Y_UNIT_TEST(SetLockNothing) {
    SetLockNothing<TLocksV1>();
    SetLockNothing<TLocksV2>();
}

template <typename TLocksVer>
static void SetEraseSet() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    const char * queryT = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
            (AcquireLocks (Uint64 '%lu))
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(Sprintf(queryT, 0), res);

    TVector<NMiniKQL::IEngineFlat::TTxLock> locks1;
    ExtractResultLocks<TLocksVer>(res, locks1);

    {
        UNIT_ASSERT_VALUES_EQUAL(locks1.size(), 1);
        TLocksVer::PrintLock(locks1[0]);
    }

    const char * eraseLock = R"___((
        (let locksTable_ '%s)
        (let lock_ '(%s))
        (return (AsList
            (EraseRow locksTable_ lock_)
        ))
    ))___";
    cs.Client.FlatQuery(Sprintf(eraseLock,
        TLocksVer::TableName(),
        TLocksVer::Key(locks1[0].LockId, locks1[0].DataShard, locks1[0].SchemeShard, locks1[0].PathId).data()), res);

    cs.Client.FlatQuery(Sprintf(queryT, locks1[0].LockId), res);
    TVector<NMiniKQL::IEngineFlat::TTxLock> locks2;
    ExtractResultLocks<TLocksVer>(res, locks2);

    {
        UNIT_ASSERT_VALUES_EQUAL(locks2.size(), 1);
        TLocksVer::PrintLock(locks2[0]);
    }

    UNIT_ASSERT_EQUAL(locks1[0].LockId, locks2[0].LockId);
    UNIT_ASSERT_EQUAL(locks1[0].DataShard, locks2[0].DataShard);
    UNIT_ASSERT_EQUAL(locks1[0].Generation, locks2[0].Generation);
    UNIT_ASSERT(locks1[0].Counter != locks2[0].Counter);

    const char * checkLocks = R"((
        (let locksTable_ '%s)
        (let lockCols_ '(%s))
        (let lockKey_ '(%s))
        (let lock1_ (SelectRow locksTable_ lockKey_ lockCols_))
        (let ok1_ (IfPresent lock1_ (lambda '(x) (Coalesce
                (And (Equal (Member x 'Generation) (Uint32 '%u))
                        (Equal (Member x 'Counter) (Uint64 '%lu)))
                (Bool 'false)))
            (Bool 'false)
        ))
        (let checks_ (AsList ok1_))
        (let goods_ (Filter checks_ (lambda '(x) (Equal x (Bool 'true)))))
        (let apply_ (Equal (Length goods_) (Uint64 '1)))
        (return (AsList (SetResult 'apply apply_)))
    ))";
    cs.Client.FlatQuery(Sprintf(checkLocks,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(locks1[0].LockId, locks1[0].DataShard, locks1[0].SchemeShard, locks1[0].PathId).data(),
        locks1[0].Generation, locks1[0].Counter), res);

    {
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue val = result["apply"];
        UNIT_ASSERT(val.HaveValue());
        bool ok = val;
        UNIT_ASSERT(!ok);
    }
}

Y_UNIT_TEST(SetEraseSet) {
    SetEraseSet<TLocksV1>();
    SetEraseSet<TLocksV2>();
}

// PenPineappleApplePen
template <typename TLocksVer>
static void SetBreakSetEraseBreak() {
    TClientServer cs;

    const char * qSet = R"((
        (let key_ '('('key (Uint32 '42))))
        (let cols_ '('value))
        (let select_ (SelectRow '/dc-1/Dir/A key_ cols_))
        (return (AsList
            (SetResult 'res select_)
            (AcquireLocks (Uint64 '%lu))
        ))
    ))";

#if 0
    const char * qBreak = R"((
        (let row_ '('('key (Uint32 '42))))
        (let update_ '('('value (Uint32 '0))))
        (return (AsList
            (UpdateRow '/dc-1/Dir/A row_ update_)
        ))
    ))";
#else
    const char * qBreak = R"((
        (let key_ '('('key (Uint32 '42))))
        (return (AsList
            (EraseRow '/dc-1/Dir/A key_)
        ))
    ))";
#endif

#if 0
    NKikimrMiniKQL::TResult res0;
    UNIT_ASSERT(cs.Client.FlatQuery(qBreak, res0));
#endif

    // set first
    NKikimrMiniKQL::TResult res1;
    cs.Client.FlatQuery(Sprintf(qSet, 0), res1);

#if 0
    {
        TValue result = TValue::Create(res1.GetValue(), res1.GetType());
        TValue x = result["res"];
        UNIT_ASSERT(x.HaveValue());
        ui32 value = x;
        UNIT_ASSERT_VALUES_EQUAL(value, 0);
    }
#endif

    TVector<NMiniKQL::IEngineFlat::TTxLock> locks;
    ExtractResultLocks<TLocksVer>(res1, locks);

    {
        UNIT_ASSERT_VALUES_EQUAL(locks.size(), 1);
        TLocksVer::PrintLock(locks[0]);
    }

    NMiniKQL::IEngineFlat::TTxLock l = locks[0];
    locks.clear();

    const char * qCheckLocks = R"((
        (let locksTable_ '%s)
        (let lockCols_ '(%s))
        (let lockKey_ '(%s))
        (let gen_ (Uint32 '%u))
        (let counter_ (Uint64 '%lu))
        (let lock_ (SelectRow locksTable_ lockKey_ lockCols_ 'online))
        (let ok_ (IfPresent lock_ (lambda '(x) (Coalesce
                (And (Equal (Member x 'Generation) gen_) (Equal (Member x 'Counter) counter_))
                (Bool 'false)))
            (Bool 'false)
        ))
        (return (AsList (SetResult 'ok ok_)))
    ))";

    // check first
    NKikimrMiniKQL::TResult res2;
    cs.Client.FlatQuery(Sprintf(qCheckLocks,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(l.LockId, l.DataShard, l.SchemeShard, l.PathId).data(),
        l.Generation, l.Counter), res2);

    {
        TValue result = TValue::Create(res2.GetValue(), res2.GetType());
        TValue val = result["ok"];
        UNIT_ASSERT(val.HaveValue());
        bool ok = val;
        UNIT_ASSERT(ok);
    }

    // break first
    cs.Client.FlatQuery(qBreak);

    // check first broken
    NKikimrMiniKQL::TResult res3;
    cs.Client.FlatQuery(Sprintf(qCheckLocks,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(l.LockId, l.DataShard, l.SchemeShard, l.PathId).data(),
        l.Generation, l.Counter), res3);

    {
        TValue result = TValue::Create(res3.GetValue(), res3.GetType());
        TValue val = result["ok"];
        UNIT_ASSERT(val.HaveValue());
        bool ok = val;
        UNIT_ASSERT(!ok);
    }

    // set second
    NKikimrMiniKQL::TResult res4;
    cs.Client.FlatQuery(Sprintf(qSet, 0), res4);
    ExtractResultLocks<TLocksVer>(res4, locks);

    {
        UNIT_ASSERT_VALUES_EQUAL(locks.size(), 1);
        TLocksVer::PrintLock(locks[0]);
    }

    const char * qEraseLock = R"((
        (let locksTable_ '%s)
        (let lockKey_ '(%s))
        (return (AsList
            (EraseRow locksTable_ lockKey_)
        ))
    ))";

    // erase first
    NKikimrMiniKQL::TResult res5;
    cs.Client.FlatQuery(Sprintf(qEraseLock,
        TLocksVer::TableName(), TLocksVer::Key(l.LockId, l.DataShard, l.SchemeShard, l.PathId).data()), res5);

    // check second
    cs.Client.FlatQuery(Sprintf(qCheckLocks,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(locks[0].LockId, locks[0].DataShard, locks[0].SchemeShard, locks[0].PathId).data(),
        locks[0].Generation, locks[0].Counter), res5);

    {
        TValue result = TValue::Create(res5.GetValue(), res5.GetType());
        TValue val = result["ok"];
        UNIT_ASSERT(val.HaveValue());
        bool ok = val;
        UNIT_ASSERT(ok);
    }

    // break second
    cs.Client.FlatQuery(qBreak);

    // check second
    NKikimrMiniKQL::TResult res6;
    cs.Client.FlatQuery(Sprintf(qCheckLocks,
        TLocksVer::TableName(), TLocksVer::Columns(),
        TLocksVer::Key(locks[0].LockId, locks[0].DataShard, locks[0].SchemeShard, locks[0].PathId).data(),
        locks[0].Generation, locks[0].Counter), res6);

    {
        TValue result = TValue::Create(res6.GetValue(), res6.GetType());
        TValue val = result["ok"];
        UNIT_ASSERT(val.HaveValue());
        bool ok = val;
        UNIT_ASSERT(!ok);
    }
}

Y_UNIT_TEST(SetBreakSetEraseBreak) {
    SetBreakSetEraseBreak<TLocksV1>();
    SetBreakSetEraseBreak<TLocksV2>();
}

} // TLocksTest


/// suite TLocksFatTest
Y_UNIT_TEST_SUITE(TLocksFatTest) {

const ui32 NUM_LOTS = 250;

Y_UNIT_TEST(PointSetBreak) {
    TClientServer cs;
    //NKikimrMiniKQL::TResult res;

    const char * setLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";

    for (ui64 i = 0; i < NUM_LOTS; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, i*2)); // 0, 2, 4...
    }

    const char * breakLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (let ret_ (AsList
            (EraseRow '/dc-1/Dir/A row0_)
        ))
        (return ret_)
    ))___";

    ui32 retry = 2;
    for (ui32 r = 0; r < retry; ++r) {
        for (ui64 i = 0; i < NUM_LOTS; ++i) {
            cs.Client.FlatQuery(Sprintf(breakLock, i*2)); // 0, 2, 4...
        }
    }
}

Y_UNIT_TEST(PointSetNotBreak) {
    TClientServer cs;
    //NKikimrMiniKQL::TResult res;

    const char * setLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
            (AcquireLocks (Uint64 '0))
        ))
        (return ret_)
    ))___";

    for (ui64 i = 0; i < NUM_LOTS; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, i*2)); // 0, 2, 4...
    }

    const char * breakLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (let ret_ (AsList
            (EraseRow '/dc-1/Dir/A row0_)
        ))
        (return ret_)
    ))___";

    ui32 retry = 2;
    for (ui32 r = 0; r < retry; ++r) {
        for (ui64 i = 0; i < NUM_LOTS; ++i) {
            cs.Client.FlatQuery(Sprintf(breakLock, i*2+1)); // 1, 3, 5...
        }
    }
}

template <typename TLocksVer>
static void PointSetRemove() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    const char * setLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (return (AsList
            (SetResult 'res0 select0_)
            (AcquireLocks (Uint64 '0))
        ))
    ))___";

    TVector<NMiniKQL::IEngineFlat::TTxLock> locks;
    for (ui64 i = 0; i < NUM_LOTS; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, i*2), res); // 0, 2, 4...
        ExtractResultLocks<TLocksVer>(res, locks);
    }

    const char * removeLock = R"___((
        (let locksTable_ '%s)
        (let lockKey_ '(%s))
        (return (AsList
            (EraseRow locksTable_ lockKey_)
        ))
    ))___";
    for (auto& lock : locks) {
        cs.Client.FlatQuery(Sprintf(removeLock,
            TLocksVer::TableName(),
            TLocksVer::Key(lock.LockId, lock.DataShard, lock.SchemeShard, lock.PathId).data()));
    }
}

Y_UNIT_TEST(PointSetRemove) {
    PointSetRemove<TLocksV1>();
    PointSetRemove<TLocksV2>();
}

Y_UNIT_TEST(RangeSetBreak) {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    const char * setLock = R"___((
        (let range_ '('IncFrom 'IncTo '('key (Uint32 '%d) (Uint32 '%d))))
        (let cols_ '('key 'value))
        (return (AsList
            (SetResult 'res (SelectRange '/dc-1/Dir/A range_ cols_ '()))
            (AcquireLocks (Uint64 '0))
        ))
    ))___";

    for (ui64 i = 0; i < NUM_LOTS; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, i, i+1)); // [0,1], [1,2], [2,3]
    }

    const char * breakLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (return (AsList
            (EraseRow '/dc-1/Dir/A row0_)
        ))
    ))___";

    ui32 retry = 2;
    for (ui32 r = 0; r < retry; ++r) {
        for (ui64 i = 0; i < NUM_LOTS; ++i) {
            cs.Client.FlatQuery(Sprintf(breakLock, 2*i+1)); // 1, 3, 5...
        }
    }
}

Y_UNIT_TEST(RangeSetNotBreak) {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    const char * setLock = R"___((
        (let range_ '('IncFrom 'IncTo '('key (Uint32 '%d) (Uint32 '%d))))
        (let cols_ '('key 'value))
        (return (AsList
            (SetResult 'res (SelectRange '/dc-1/Dir/A range_ cols_ '()))
            (AcquireLocks (Uint64 '0))
        ))
    ))___";

    for (ui64 i = 0; i < NUM_LOTS; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, 3*i, 3*i+1)); // [0,1], [3,4], [6,7]
    }

    const char * breakLock = R"___((
        (let row0_ '('('key (Uint32 '%d))))
        (return (AsList
            (EraseRow '/dc-1/Dir/A row0_)
        ))
    ))___";

    ui32 retry = 2;
    for (ui32 r = 0; r < retry; ++r) {
        for (ui64 i = 0; i < NUM_LOTS; ++i) {
            cs.Client.FlatQuery(Sprintf(breakLock, 3*i+2)); // 2, 5, 8...
        }
    }
}

template <typename TLocksVer>
static void RangeSetRemove() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;

    const char * setLock = R"___((
        (let range_ '('IncFrom 'IncTo '('key (Uint32 '%d) (Uint32 '%d))))
        (let cols_ '('key 'value))
        (return (AsList
            (SetResult 'res (SelectRange '/dc-1/Dir/A range_ cols_ '()))
            (AcquireLocks (Uint64 '0))
        ))
    ))___";

    TVector<NMiniKQL::IEngineFlat::TTxLock> locks;
    for (ui64 i = 0; i < NUM_LOTS; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, 2*i, 2*i+1), res);
        ExtractResultLocks<TLocksVer>(res, locks);
    }

    const char * removeLock = R"___((
        (let locksTable_ '%s)
        (let lockKey_ '(%s))
        (return (AsList
            (EraseRow locksTable_ lockKey_)
        ))
    ))___";
    for (auto& lock : locks) {
        cs.Client.FlatQuery(Sprintf(removeLock,
            TLocksVer::TableName(),
            TLocksVer::Key(lock.LockId, lock.DataShard, lock.SchemeShard, lock.PathId).data()));
    }
}

Y_UNIT_TEST(RangeSetRemove) {
    RangeSetRemove<TLocksV1>();
    RangeSetRemove<TLocksV2>();
}

template <typename TLocksVer>
static void LocksLimit() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;
    TClient::TFlatQueryOptions opts;

    using TLock = TSysTables::TLocksTable::TLock;

    ui32 limit = NDataShard::TLockLocker::LockLimit();
    const ui32 factor = 100;

    const char * query = R"((
        (let row0_ '('('key (Uint32 '%u))))
        (let cols_ '('value))
        (let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))
        (let ret_ (AsList
            (SetResult 'res0 select0_)
            (AcquireLocks (Uint64 '%lu))
        ))
        (return ret_)
    ))";

    Cout << "setting locks... " << Endl;
    TVector<NMiniKQL::IEngineFlat::TTxLock> locks;
    locks.reserve(limit * 2);

    for (ui32 i = 0; i < limit; ++i) {
        cs.Client.FlatQuery(Sprintf(query, i%factor, 0), res);
        ExtractResultLocks<TLocksVer>(res, locks);
        TLocksVer::PrintLock(locks.back());
        UNIT_ASSERT_VALUES_EQUAL(locks.back().Counter, i);
    }

    for (ui32 i = 0; i < factor; ++i) {
        cs.Client.FlatQuery(Sprintf(query, i%factor, 0), res);
        ExtractResultLocks<TLocksVer>(res, locks);
        TLocksVer::PrintLock(locks.back());
        UNIT_ASSERT(TLock::IsTooMuch(locks.back().Counter));
    }

    Cout << "setting same locks... " << Endl;
    for (ui32 i = 0; i < factor; ++i) {
        cs.Client.FlatQuery(Sprintf(query, i%factor, locks[i].LockId), res);
        ExtractResultLocks<TLocksVer>(res, locks);
        TLocksVer::PrintLock(locks.back());
        UNIT_ASSERT_VALUES_EQUAL(locks.back().LockId, locks[i].LockId);
        UNIT_ASSERT_VALUES_EQUAL(locks.back().Counter, locks[i].Counter);
    }

    Cout << "erasing locks... " << Endl;
    const char * erase = R"((
        (let key_ '('('key (Uint32 '%u))))
        (return (AsList (EraseRow '/dc-1/Dir/A key_)))
    ))";
    for (ui32 i = 0; i < factor; ++i) {
        cs.Client.FlatQuery(Sprintf(erase, i%factor), res);
    }

    Cout << "evicting (first) locks... " << Endl;
    for (ui32 i = 0; i < factor; ++i) {
        cs.Client.FlatQuery(Sprintf(query, i%factor, 0), res);
        ExtractResultLocks<TLocksVer>(res, locks);
        TLocksVer::PrintLock(locks.back());
        UNIT_ASSERT_VALUES_EQUAL(locks.back().Counter, limit+i);
    }

    Cout << "resetting (first) locks... " << Endl;
    for (ui32 i = 0; i < factor; ++i) {
        cs.Client.FlatQuery(Sprintf(query, i%factor, locks[i].LockId), res);
        ExtractResultLocks<TLocksVer>(res, locks);
        TLocksVer::PrintLock(locks.back());
        UNIT_ASSERT_VALUES_EQUAL(locks.back().LockId, locks[i].LockId);
        UNIT_ASSERT(locks.back().Counter != locks[i].Counter);
    }

    const char * selectLocksT = R"___((
        (let locksTable_ '%s)
        (let lockCols_ '(%s))
        (let lockKey_ '(%s))
        (let return_ (AsList
            (SetResult 'Result (SelectRow locksTable_ lockKey_ lockCols_))
        ))
        (return return_)
    ))___";

    Cout << "reading locks... " << Endl;
    for (const auto& lock : locks) {
        cs.Client.FlatQuery(Sprintf(selectLocksT,
            TLocksVer::TableName(), TLocksVer::Columns(),
            TLocksVer::Key(lock.LockId, lock.DataShard, lock.SchemeShard, lock.PathId).data()), res);

        {
            TValue result = TValue::Create(res.GetValue(), res.GetType());
            TValue xres = result["Result"];
            if (xres.HaveValue()) {
                auto lock = ExtractRowLock<TLocksVer>(xres);
                TLocksVer::PrintLock(lock);
                UNIT_ASSERT(lock.Counter >= limit);
            }
        }
    }
}

Y_UNIT_TEST(LocksLimit) {
    LocksLimit<TLocksV1>();
    LocksLimit<TLocksV2>();
}

template <typename TLocksVer>
static void ShardLocks() {
    TClientServer cs;
    NKikimrMiniKQL::TResult res;
    TClient::TFlatQueryOptions opts;


    ui32 limit = NDataShard::TLockLocker::LockLimit();
    //const ui32 factor = 100;

    const char * setLock = R"___((
        (let range_ '('IncFrom 'IncTo '('key (Uint32 '%u) (Uint32 '%u))))
        (let cols_ '('key 'value))
        (return (AsList
            (SetResult 'res (SelectRange '/dc-1/Dir/A range_ cols_ '()))
            (AcquireLocks (Uint64 '%lu))
        ))
    ))___";

    // Attach lots of ranges to a single lock.
    TVector<NMiniKQL::IEngineFlat::TTxLock> locks;
    ui64 lockId = 0;
    for (ui32 i = 0; i < limit + 1; ++i) {
        cs.Client.FlatQuery(Sprintf(setLock, i * 10, i * 10 + 5, lockId), res);
        ExtractResultLocks<TLocksVer>(res, locks);
        lockId = locks.back().LockId;
    }

    // We now have too many rnages attached to locks and new lock
    // will be forced to be shard lock.
    cs.Client.FlatQuery(Sprintf(setLock, 0, 5, 0), res);
    ExtractResultLocks<TLocksVer>(res, locks);

    // Now check shard lock is ok.
    const char * checkLock = R"___((
        (let locksTable_ '%s)
        (let lockKey_ '(%s))
        (let lockCols_ '(%s))
        (return (AsList
            (SetResult 'Result (SelectRow locksTable_ lockKey_ lockCols_))
        ))
    ))___";
    {
        cs.Client.FlatQuery(Sprintf(checkLock,
                                    TLocksVer::TableName(),
                                    TLocksVer::Key(locks.back().LockId,
                                                    locks.back().DataShard,
                                                    locks.back().SchemeShard,
                                                    locks.back().PathId).data(),
                                    TLocksVer::Columns()), res);
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue xres = result["Result"];
        UNIT_ASSERT(xres.HaveValue());
        auto lock = ExtractRowLock<TLocksVer>(xres);
        UNIT_ASSERT_VALUES_EQUAL(lock.LockId, locks.back().LockId);
        UNIT_ASSERT_VALUES_EQUAL(lock.Generation, locks.back().Generation);
        UNIT_ASSERT_VALUES_EQUAL(lock.Counter, locks.back().Counter);
    }

    // Break locks by single row update.
    const char * lockUpdate = R"___((
        (let row0_ '('('key (Uint32 '42))))
        (let update_ '('('value (Uint32 '0))))
        (let ret_ (AsList
            (UpdateRow '/dc-1/Dir/A row0_ update_)
        ))
        (return ret_)
    ))___";
    cs.Client.FlatQuery(lockUpdate, opts, res);

    // Check locks are broken.
    {
        cs.Client.FlatQuery(Sprintf(checkLock,
                                    TLocksVer::TableName(),
                                    TLocksVer::Key(locks.back().LockId,
                                                    locks.back().DataShard,
                                                    locks.back().SchemeShard,
                                                    locks.back().PathId).data(),
                                    TLocksVer::Columns()), res);
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue xres = result["Result"];
        UNIT_ASSERT(!xres.HaveValue());
    }

    {
        cs.Client.FlatQuery(Sprintf(checkLock,
                                    TLocksVer::TableName(),
                                    TLocksVer::Key(locks[0].LockId,
                                                    locks[0].DataShard,
                                                    locks[0].SchemeShard,
                                                    locks[0].PathId).data(),
                                    TLocksVer::Columns()), res);
        TValue result = TValue::Create(res.GetValue(), res.GetType());
        TValue xres = result["Result"];
        UNIT_ASSERT(!xres.HaveValue());
    }
}

Y_UNIT_TEST(ShardLocks) {
    ShardLocks<TLocksV1>();
    ShardLocks<TLocksV2>();
}

} // TLocksFatTest

}}
