#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h> // Y_UNIT_TEST_(TWIN|QUAD)
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/library/actors/util/memory_tracker.h>
#include <util/system/valgrind.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

namespace NKikimr {

using NClient::TValue;
using IEngineFlat = NMiniKQL::IEngineFlat;
using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

using TActiveTxPtr = std::shared_ptr<TActiveTransaction>;

///
class TDatashardTester {
public:
    TDatashardTester()
        : FunctionRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry()))
        , RandomProvider(CreateDeterministicRandomProvider(1))
        , TimeProvider(CreateDeterministicTimeProvider(1))
    {}

    static TString MakeTxBody(const TString& miniKQL, bool immediate = false, ui64 lockId = 0) {
        NKikimrTxDataShard::TDataTransaction tx;
        tx.SetMiniKQL(miniKQL);
        tx.SetImmediate(immediate);
        if (lockId) {
            tx.SetLockTxId(lockId);
        }
        return tx.SerializeAsString();
    }

    static TActiveTxPtr MakeEmptyTx(ui64 step, ui64 txId) {
        TBasicOpInfo op(txId, EOperationKind::DataTx, 0, Max<ui64>(), TInstant(), 0);
        op.SetStep(step);
        return std::make_shared<TActiveTransaction>(op);
    }
#if 0
    TActiveTransaction MakeActiveTx(ui64 step, ui64 txId, const TString& txBody) {
        THolder<NMiniKQL::IEngineFlatHost> host = MakeHolder<NMiniKQL::TEngineHost>(DB);
        THolder<NMiniKQL::IEngineFlat> engine = CreateEngineFlat(
            NMiniKQL::TEngineFlatSettings(NMiniKQL::IEngineFlat::EProtocol::V1,
                                          FunctionRegistry.Get(), *RandomProvider, *TimeProvider, host.Get()));

        TEngineBay ebay(host.Release(), engine.Release());
        std::shared_ptr<TValidatedDataTx> dataTx(new TValidatedDataTx(std::move(ebay), txId, 0, txBody));

        TBasicOpInfo op(txId, NKikimrTxDataShard::ETransactionKind::TX_KIND_DATA, 0, Max<ui64>(), 0);
        op.SetStep(step);
        TActiveTransaction tx(op);
        tx.Activate(0, dataTx);
        return tx;
    }
#endif
private:
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    NTable::TDatabase DB;
};


///
Y_UNIT_TEST_SUITE(TxOrderInternals) {

Y_UNIT_TEST(OperationOrder) {
    using TTester = TDatashardTester;

    TActiveTxPtr tx0_100 = TTester::MakeEmptyTx(0, 100);
    TActiveTxPtr tx0_101 = TTester::MakeEmptyTx(0, 101);
    TActiveTxPtr tx1_40 = TTester::MakeEmptyTx(1, 40);
    TActiveTxPtr tx1_102 = TTester::MakeEmptyTx(1, 102);
    TActiveTxPtr tx1_103 = TTester::MakeEmptyTx(1, 103);
    TActiveTxPtr tx2_42 = TTester::MakeEmptyTx(2, 42);

    UNIT_ASSERT_EQUAL(tx0_100->GetStepOrder().CheckOrder(tx0_101->GetStepOrder()), ETxOrder::Any);
    UNIT_ASSERT_EQUAL(tx0_101->GetStepOrder().CheckOrder(tx0_100->GetStepOrder()), ETxOrder::Any);

    UNIT_ASSERT_EQUAL(tx0_100->GetStepOrder().CheckOrder(tx1_102->GetStepOrder()), ETxOrder::Unknown);
    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx0_100->GetStepOrder()), ETxOrder::Unknown);

    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx1_103->GetStepOrder()), ETxOrder::Before);
    UNIT_ASSERT_EQUAL(tx1_103->GetStepOrder().CheckOrder(tx1_102->GetStepOrder()), ETxOrder::After);

    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx1_40->GetStepOrder()), ETxOrder::After);
    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx2_42->GetStepOrder()), ETxOrder::Before);
}

}

static void InitCrossShard_ABC(TFakeMiniKQLProxy& proxy, TVector<ui32> uintVal) {
    UNIT_ASSERT_EQUAL(uintVal.size(), 3);

    auto programText = Sprintf(R"((
        (let row1_ '('('key (Uint32 '0))))
        (let row2_ '('('key (Uint32 '1000))))
        (let row3_ '('('key (Uint32 '2000))))
        (let upd1_ '('('value (Utf8 'A)) '('uint (Uint32 '%u))))
        (let upd2_ '('('value (Utf8 'B)) '('uint (Uint32 '%u))))
        (let upd3_ '('('value (Utf8 'C)) '('uint (Uint32 '%u))))
        (let ret_ (AsList
            (UpdateRow 'table1 row1_ upd1_)
            (UpdateRow 'table1 row2_ upd2_)
            (UpdateRow 'table1 row3_ upd3_)
        ))
        (return ret_)
    ))", uintVal[0], uintVal[1], uintVal[2]);

    UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
}

///
Y_UNIT_TEST_SUITE(DataShardTxOrder) {

static void ZigZag(TFakeMiniKQLProxy& proxy, bool symmetric, ui32 limit = 40) {
    InitCrossShard_ABC(proxy, {0, 0, 0});

    const char * zigzag = R"((
        (let src1_ '('('key (Uint32 '%u))))
        (let src2_ '('('key (Uint32 '%u))))
        (let dst1_ '('('key (Uint32 '%u))))
        (let dst2_ '('('key (Uint32 '%u))))
        (let val1_ (FlatMap (SelectRow 'table1 src1_ '('value)) (lambda '(x) (Member x 'value))))
        (let val2_ (FlatMap (SelectRow 'table1 src2_ '('value)) (lambda '(x) (Member x 'value))))
        (let upd1_ '('('value val1_)))
        (let upd2_ '('('value val2_)))
        (let ret_ (AsList
            (UpdateRow 'table1 dst1_ upd2_)
            (UpdateRow 'table1 dst2_ upd1_)
        ))
        (return ret_)
    ))";

    // strict ordered txs: {0->1001, 1000->1} {1->1002, 1001->2}
    for (ui32 i = 0; i < 10; ++i) {
        TString programText = Sprintf(zigzag, i, 1000+i, i+1, 1000+i+1);
        UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
    }

    if (symmetric) {
        // relaxed order: 20->1031, 1020->31
        for (ui32 shift = 0; shift < limit-10; shift+=10) {
            for (ui32 i = 0; i < 10; ++i) {
                TString programText = Sprintf(zigzag, shift+i, 1000+shift+i, shift+i+11, 1000+shift+i+11);
                //UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
                proxy.Enqueue(programText);
            }
        }
    } else {
        // relaxed order (asymmetric): 0->1031, 1020->1031
        for (ui32 shift = 0; shift < limit-10; shift+=10) {
            for (ui32 i = 0; i < 10; ++i) {
                TString programText = Sprintf(zigzag, i, 1000+shift+i, shift+i+11, 1000+shift+i+11);
                //UNIT_ASSERT_EQUAL(proxy.Execute(programText), IEngineFlat::EStatus::Complete);
                proxy.Enqueue(programText);
            }
        }
    }

    proxy.ExecQueue();

    {
        TString programText = Sprintf(R"((
            (let row1_ '('('key (Uint32 '%u))))
            (let row2_ '('('key (Uint32 '%u))))
            (let row3_ '('('key (Uint32 '%u))))
            (let row4_ '('('key (Uint32 '%u))))
            (let select_ '('value))
            (let ret_ (AsList
            (SetResult 'Result (AsList
                (SelectRow 'table1 row1_ select_)
                (SelectRow 'table1 row2_ select_)
                (SelectRow 'table1 row3_ select_)
                (SelectRow 'table1 row4_ select_)
            ))
            ))
            (return ret_)
        ))", limit-1, limit, 1000+limit-1, 1000+limit);

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(proxy.Execute(programText, res), IEngineFlat::EStatus::Complete);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rl = value["Result"];
        TValue row1 = rl[0];
        TValue row2 = rl[1];
        TValue row3 = rl[2];
        TValue row4 = rl[3];
        UNIT_ASSERT_EQUAL(TString(row1["value"]), "B");
        UNIT_ASSERT_EQUAL(TString(row2["value"]), "A");
        UNIT_ASSERT_EQUAL(TString(row3["value"]), "A");
        UNIT_ASSERT_EQUAL(TString(row4["value"]), "B");
    }
}

static void ZigZag(const TTester::TOptions& opts, bool symmetric, ui32 limit = 40) {
    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);
    ZigZag(proxy, symmetric, limit);
}

Y_UNIT_TEST(ZigZag) {
    TTester::TOptions opts;
    ZigZag(opts, true);
    ZigZag(opts, false);
}

Y_UNIT_TEST(ZigZag_oo) {
    TVector<ui32> variants = {4, 8, 16};
    for (ui32 var : variants) {
        TTester::TOptions opts;
        opts.EnableOutOfOrder(var);
        ZigZag(opts, true);
        ZigZag(opts, false);
    }
}

Y_UNIT_TEST(ZigZag_oo8_dirty) {
    TTester::TOptions opts;
    opts.EnableOutOfOrder(8);
    opts.EnableSoftUpdates();
    ZigZag(opts, true);
    ZigZag(opts, false);
}

//

static void ImmediateBetweenOnline(const TTester::TOptions& opts, bool forceOnline = false) {
    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);

    InitCrossShard_ABC(proxy, {0, 0, 0});

    const char * online = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let key2_ '('('key (Uint32 '%u))))
        (let key3_ '('('key (Uint32 '%u))))
        (let val1_ (FlatMap (SelectRow 'table1 key1_ '('value)) (lambda '(x) (Member x 'value))))
        (let val2_ (FlatMap (SelectRow 'table1 key2_ '('value)) (lambda '(x) (Member x 'value))))
        (let upd21_ '('('uint (Uint32 '%u)) '('value val2_)))
        (let upd13_ '('('value val1_)))
        (let ret_ (AsList
            (UpdateRow 'table1 key1_ upd21_)
            (UpdateRow 'table1 key3_ upd13_)
        ))
        (return ret_)
    ))";

    const char * immediate = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let key2_ '('('key (Uint32 '%u))))
        (let val1_ (Coalesce (FlatMap (SelectRow 'table1 key1_ '('uint)) (lambda '(x) (Member x 'uint))) (Uint32 '0)))
        (let val2_ (Coalesce (FlatMap (SelectRow 'table1 key2_ '('uint)) (lambda '(x) (Member x 'uint))) (Uint32 '0)))
        (let ret_ (AsList
            (SetResult 'val1 val1_)
            (SetResult 'val2 val2_)
        ))
        (return ret_)
    ))";

    auto immediateCheck = [&](TFakeProxyTx& tx) -> bool {
        NKikimrMiniKQL::TResult res = tx.GetResult();
        TValue value = TValue::Create(res.GetValue(), res.GetType());
        ui32 val1 = value["val1"];
        ui32 val2 = value["val2"];
        //Cerr << "val1 " << val1 << " val2 " << val2 << Endl;
        if (forceOnline) {
            UNIT_ASSERT(!tx.Immediate());
            UNIT_ASSERT(val1 == 1 && val2 == 0);
        } else {
            UNIT_ASSERT(tx.Immediate());
            UNIT_ASSERT((val1 == 0 && val2 == 0) || (val1 == 1 && val2 == 0) || (val1 == 1 && val2 == 2));
            if (val1 == 1 && val2 == 0) {
                Cerr << "Got it!" << Endl;
            }
        }
        return true;
    };

    ui32 flags = NDataShard::TTxFlags::Default;
    if (forceOnline) {
        flags |= NDataShard::TTxFlags::ForceOnline;
    }

    for (ui32 i = 0; i < 100; i+=2) {
        TString prog1 = Sprintf(online, i, 1000+i, 2000+i, 1);
        TString prog2 = Sprintf(online, i+1, 1000+i+1, 200+i, 2);
        TString progIm = Sprintf(immediate, i, i+1);
        proxy.Enqueue(prog1);
        proxy.Enqueue(progIm, immediateCheck, flags);
        proxy.Enqueue(prog2);
    }

    proxy.ExecQueue();
}

Y_UNIT_TEST(ImmediateBetweenOnline) {
    TTester::TOptions opts;
    ImmediateBetweenOnline(opts, false);
}

Y_UNIT_TEST(ImmediateBetweenOnline_Init) {
    TTester::TOptions opts;
    ImmediateBetweenOnline(opts, false);
}

Y_UNIT_TEST(ForceOnlineBetweenOnline) {
    TTester::TOptions opts;
    ImmediateBetweenOnline(opts, true);
}

Y_UNIT_TEST(ImmediateBetweenOnline_oo8) {
    TTester::TOptions opts;
    opts.EnableOutOfOrder(8);
    ImmediateBetweenOnline(opts, false);
}

Y_UNIT_TEST(ImmediateBetweenOnline_Init_oo8) {
    TTester::TOptions opts(1);
    opts.EnableOutOfOrder(8);
    ImmediateBetweenOnline(opts, false);
}

Y_UNIT_TEST(ForceOnlineBetweenOnline_oo8) {
    TTester::TOptions opts;
    opts.EnableOutOfOrder(8);
    ImmediateBetweenOnline(opts, true);
}

Y_UNIT_TEST(ImmediateBetweenOnline_oo8_dirty) {
    TTester::TOptions opts;
    opts.EnableOutOfOrder(8);
    opts.EnableSoftUpdates();
    ImmediateBetweenOnline(opts, false);
}

//

static void EvictShardCache(TFakeMiniKQLProxy& proxy, ui32 count = 500) {
    const char * programWrite = R"((
        (let key_ '('('key (Uint32 '%u))))
        (return (AsList (UpdateRow 'table1 key_ '('('value (Utf8 '"%s"))))))
    ))";

    const char * text = "You will always get that you always got if you always do that you've already done";
    for (ui32 i = 0; i < count; ++i) {
        UNIT_ASSERT_EQUAL(proxy.Execute(Sprintf(programWrite, (3*i)%1000, text)), IEngineFlat::EStatus::Complete);
    }

    // some more txs to enlarge step
    proxy.Enqueue(Sprintf(programWrite, 0, text));
    proxy.Enqueue(Sprintf(programWrite, 0, text));
    proxy.ExecQueue();
}

Y_UNIT_TEST(DelayData) {
    TTester::TOptions opts;
    opts.EnableOutOfOrder(2);
    opts.ExecutorCacheSize = 0;
    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);

    EvictShardCache(proxy, 500);

    ui64 indepTxId = proxy.LastTxId() + 2;
    proxy.DelayData({TTestTxConfig::TxTablet0, indepTxId});

    const char * programRead = R"((
        (let key_ '('('key (Uint32 '0))))
        (return (AsList (SetResult 'Result (SelectRow 'table1 key_ '('value)))))
    ))";

    const char * independentTx = R"((
        (let key_ '('('key (Uint32 '999))))
        (return (AsList (UpdateRow 'table1 key_ '('('value (Utf8 '"freedom"))))))
    ))";

    //
    proxy.Enqueue(programRead);
    proxy.Enqueue(independentTx);
    proxy.ExecQueue();
}

Y_UNIT_TEST(ReadWriteReorder) {
    TTester::TOptions opts;
    opts.EnableOutOfOrder(10);

    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);

    const char* programWriteKeys1 = R"((
        (let key_ '('('key (Uint32 '%u))))
        (let upd_ '('('value (Utf8 '"%s"))))
        (return (AsList
            (UpdateRow 'table1 key_ upd_)
        ))
    ))";

    proxy.CheckedExecute(Sprintf(programWriteKeys1, 0, "A"));
    proxy.CheckedExecute(Sprintf(programWriteKeys1, 1, "B"));
    proxy.CheckedExecute(Sprintf(programWriteKeys1, 1000, "C"));

    const char* programMoveKey = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let val1_ (FlatMap (SelectRow 'table1 key1_ '('value)) (lambda '(x) (Member x 'value))))
        (let key2_ '('('key (Uint32 '%u))))
        (let upd2_ '('('value val1_)))
        (return (AsList
            (UpdateRow 'table1 key2_ upd2_)
        ))
    ))";

    const char* programWriteKeys2 = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let upd1_ '('('value (Utf8 '"%s"))))
        (let key2_ '('('key (Uint32 '%u))))
        (let upd2_ '('('value (Utf8 '"%s"))))
        (return (AsList
            (UpdateRow 'table1 key1_ upd1_)
            (UpdateRow 'table1 key2_ upd2_)
        ))
    ))";

    const char* programWriteKeys3 = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let upd1_ '('('value (Utf8 '"%s"))))
        (let key2_ '('('key (Uint32 '%u))))
        (let upd2_ '('('value (Utf8 '"%s"))))
        (let key3_ '('('key (Uint32 '%u))))
        (let upd3_ '('('value (Utf8 '"%s"))))
        (return (AsList
            (UpdateRow 'table1 key1_ upd1_)
            (UpdateRow 'table1 key2_ upd2_)
            (UpdateRow 'table1 key3_ upd3_)
        ))
    ))";

    const char* programReadKeys3 = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let key2_ '('('key (Uint32 '%u))))
        (let key3_ '('('key (Uint32 '%u))))
        (let select_ '('value))
        (return (AsList
            (SetResult 'Result (AsList
                (SelectRow 'table1 key1_ select_)
                (SelectRow 'table1 key2_ select_)
                (SelectRow 'table1 key3_ select_)
            ))
        ))
    ))";

    const char* programReadKeys4 = R"((
        (let key1_ '('('key (Uint32 '%u))))
        (let key2_ '('('key (Uint32 '%u))))
        (let key3_ '('('key (Uint32 '%u))))
        (let key4_ '('('key (Uint32 '%u))))
        (let select_ '('value))
        (return (AsList
            (SetResult 'Result (AsList
                (SelectRow 'table1 key1_ select_)
                (SelectRow 'table1 key2_ select_)
                (SelectRow 'table1 key3_ select_)
                (SelectRow 'table1 key4_ select_)
            ))
        ))
    ))";

    auto noCheck = [&](TFakeProxyTx&) -> bool {
        return true;
    };
    auto txFlags = NDataShard::TTxFlags::Default;

    // tx 7: This moves key 1000 to key 2 (and will be blocked on readsets)
    proxy.Enqueue(Sprintf(programMoveKey, 1000, 2), noCheck, txFlags);
    auto txMoveKey = proxy.LastTxId();

    // tx 8: This writes to keys 0, 2 and 1000 (used as a progress blocker)
    proxy.Enqueue(Sprintf(programWriteKeys3, 0, "D", 2, "E", 1000, "F"), noCheck, txFlags);

    // tx 9: This reads keys 1, 3 and 1000
    // Does not conflict on the first shard and will be executed out of order
    NKikimrMiniKQL::TResult read_1_1000_3;
    proxy.Enqueue(Sprintf(programReadKeys3, 1, 1000, 3), [&](TFakeProxyTx& tx) -> bool {
        read_1_1000_3 = tx.GetResult();
        return true;
    }, txFlags);

    // tx 10: This is an immediate write to keys 0 and 1
    // It will be proposed after the above read completes, so it must
    // be ordered after the above read.
    proxy.Enqueue(Sprintf(programWriteKeys2, 0, "G", 1, "H"), noCheck, txFlags);

    // tx 11: This write to key 3 (force online), will block until the above read
    // This would unblock writes to keys 0 and 2 and used to add a delay
    proxy.Enqueue(Sprintf(programWriteKeys1, 3, "Z"));
    auto txWriteLast = proxy.LastTxId();

    // Delay first shard readsets until last write succeeds
    proxy.DelayReadSet(TExpectedReadSet(txMoveKey, { TTestTxConfig::TxTablet0, txWriteLast }));
    proxy.ExecQueue();

    // Sanity check: read must go first, otherwise the whole machinery would hang
    // Read result also proves that tx 8 is logically before tx 9
    {
        TValue value = TValue::Create(read_1_1000_3.GetValue(), read_1_1000_3.GetType());
        TValue rows = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL(TString(rows[0]["value"]), "B"); // key 1: initial value
        UNIT_ASSERT_VALUES_EQUAL(TString(rows[1]["value"]), "F"); // key 1000: tx 8 must be visible
    }

    // Read the final state of important keys
    NKikimrMiniKQL::TResult read_0_1_2_1000;
    proxy.Enqueue(Sprintf(programReadKeys4, 0, 1, 2, 1000), [&](TFakeProxyTx& tx) -> bool {
        read_0_1_2_1000 = tx.GetResult();
        return true;
    }, txFlags);
    proxy.ExecQueue();

    // Sanity check: must see correct state of these keys
    {
        TValue value = TValue::Create(read_0_1_2_1000.GetValue(), read_0_1_2_1000.GetType());
        TValue rows = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL(TString(rows[0]["value"]), "G"); // key 0: tx 10 must be visible
        UNIT_ASSERT_VALUES_EQUAL(TString(rows[1]["value"]), "H"); // key 1: tx 10 must be visible
        UNIT_ASSERT_VALUES_EQUAL(TString(rows[2]["value"]), "E"); // key 2: tx 8 must be visible
        UNIT_ASSERT_VALUES_EQUAL(TString(rows[3]["value"]), "F"); // key 1000: tx 8 must be visible
    }
}

//

static inline bool HasFlag(ui32 flags, ui32 pos) {
    return (flags & (1 << pos));
}

static TString MkRandomTx(ui64 txId, ui32 points, ui32 rw, ui32 keysCount, TVector<ui32>& expected, bool range = false)
{
    UNIT_ASSERT(keysCount <= 32);
    Cout << "tx " << txId << ' ' << Bin(points) << ' ' << Bin(points & rw) << " (" << points << '/' << rw << ')' << Endl;

    const char * rwPattern = R"(
        (let $%u '('('key (Uint32 '%u))))
        (let updates_ (Extend updates_ (AsList (UpdateRow 'table1 $%u '('('uint (Uint32 '%u)))))))
    )";

    const char * roPattern = R"(
        (let $%u '('('key (Uint32 '%u))))
        (let selects_ (Extend selects_ (ToList (SelectRow 'table1 $%u '('key 'uint)))))
    )";

    const char * rangePattern = R"(
        (let $%u '('IncFrom 'IncTo '('key (Uint32 '%u) (Uint32 '%u))))
        (let selects_ (Extend selects_ (Member (SelectRange 'table1 $%u '('key 'uint) '()) 'List)))
    )";

    TString body;
    for (ui32 i = 0; i < keysCount; ++i) {
        if (HasFlag(points, i)) {
            if (HasFlag(rw, i)) {
                body += Sprintf(rwPattern, i, i, i, txId);
                if (!expected.empty()) {
                    expected[i] = txId;
                }
            } else {
                if (range) {
                    body += Sprintf(rangePattern, i, i, i, i);
                } else {
                    body += Sprintf(roPattern, i, i, i);
                }
            }
        }
    }

    ui32 remoteKey = 1001;
    return Sprintf(R"((
        (let remoteKey_ '('('key (Uint32 '%u))))
        (let sel_ (SelectRow 'table1 remoteKey_ '('key 'uint)))
        (let rVal_ (Coalesce (FlatMap sel_ (lambda '(x) (Member x 'uint))) (Uint32 '0)))
        (let selects_ (ToList sel_))
        (let localKey_ '('('key (Uint32 '%u))))
        (let updates_ (AsList (UpdateRow 'table1 localKey_ '('('uint rVal_)))))
        %s
        (return (Extend (AsList (SetResult 'Result selects_)) updates_))
    ))", remoteKey, (ui32)txId+100, body.data());
}

static void PrintRandomResults(const TVector<ui32>& result, const TString& prefix) {
    Cerr << prefix;
    for (ui32 val : result) {
        if (val != Max<ui32>())
            Cerr << val << ' ';
        else
            Cerr << "- ";
    }
    Cerr << Endl;
}

static void CalcPoints(ui32 count, ui32& points, ui32& writes, bool lessWrites = false) {
    ui32 maxValue = (1ul << count) - 1;
    points = RandomNumber<ui32>(maxValue);
    writes = RandomNumber<ui32>(maxValue);
    if (lessWrites)
        writes &= RandomNumber<ui32>(maxValue);
}

static void CompareShots(const TVector<ui32>& finalShot, const TVector<ui32>& intermShot, std::pair<ui32, ui32> range) {
    UNIT_ASSERT(finalShot.size() == intermShot.size());
    UNIT_ASSERT(range.second < finalShot.size());

    ui32 lastKnown = Max<ui32>();
    ui32 leastUnknown = Max<ui32>();
    for (ui32 dot = range.first; dot <= range.second; ++dot) {
        ui32 intermVal = intermShot[dot];
        if (intermVal != Max<ui32>() && (intermVal > lastKnown || lastKnown == Max<ui32>())) {
            lastKnown = intermVal;
        }
        ui32 finalVal = finalShot[dot];
        if (intermVal != finalVal && finalVal < leastUnknown) {
            leastUnknown = finalVal;
        }
    }

    if (lastKnown != Max<ui32>() && leastUnknown != Max<ui32>()) {
        UNIT_ASSERT(lastKnown < leastUnknown);
    }
}

static void RandomTxDeps(const TTester::TOptions& opts, ui32 numTxs, ui32 maxKeys, bool lessWrites,
                         bool useRanges = false, TVector<ui32> counts = {}, TVector<ui32> pts = {},
                         TVector<ui32> wrs = {}) {
    const ui32 minKeys = 2;
    UNIT_ASSERT(maxKeys <= 32);
    UNIT_ASSERT(maxKeys > minKeys);

    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);

    ui32 indepPos = 0;
    ui64 indepTxId = 0;
    TString independentTx = R"((
            (let localKey_ '('('key (Uint32 '999))))
            (let remoteKey_ '('('key (Uint32 '1001))))
            (let update_ (UpdateRow 'table1 localKey_ '('('value (Utf8 '"freedom")))))
            (let select2_ (SelectRow 'table1 remoteKey_ '('uint)))
            (return (AsList update_ (SetResult 'R2 select2_)))
        ))";

    if (opts.DelayReadSet) {
        UNIT_ASSERT(numTxs >= 8);
        indepPos = 7;
        indepTxId = proxy.LastTxId() + 8;
        ui64 delayedRS = proxy.LastTxId() + 2; // 2 cause of o-o-o disabled till first complete (LastCompleteTx)
        proxy.DelayReadSet(TExpectedReadSet(delayedRS, {TTestTxConfig::TxTablet0, indepTxId}), opts.RebootOnDelay);
    } else if (opts.DelayData) {
        UNIT_ASSERT(numTxs >= 8);
        indepPos = 7;
        EvictShardCache(proxy, 500);
        indepTxId = proxy.LastTxId() + 8;
        proxy.DelayData({TTestTxConfig::TxTablet0, indepTxId});
    }

    TVector<ui32> expected(32, Max<ui32>());
    for (ui32 i = 0; i < numTxs; ++i) {
        ui32 count = minKeys + RandomNumber<ui32>(maxKeys-minKeys);
        if (counts.size() > i)
            count = counts[i];
        ui32 points = 0;
        ui32 writes = 0;
        CalcPoints(count, points, writes, lessWrites);
        if (pts.size() > i)
            points = pts[i];
        if (wrs.size() > i)
            writes = wrs[i];
        TString prog = MkRandomTx(i, points, writes, count, expected, useRanges);
        //Cout << prog << Endl;
        if (indepTxId && i == indepPos)
            proxy.Enqueue(independentTx);
        proxy.Enqueue(prog);
    }

    ui64 pictureTxId = proxy.LastTxId() + 1;
    TVector<ui32> actual(32, Max<ui32>());
    TVector<ui32> intermediate(32, Max<ui32>());

    auto extractActual = [&](TFakeProxyTx& tx) -> bool {
        TVector<ui32> * out = &actual;
        if (tx.TxId() != pictureTxId) {
            out = &intermediate;
        }

        NKikimrMiniKQL::TResult res = tx.GetResult();
        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rStruct = value["Result"];
        UNIT_ASSERT_EQUAL(bool(rStruct["Truncated"]), false);
        TValue rList = rStruct["List"];

        for (ui32 i = 0; i < rList.Size(); ++i) {
            TValue row = rList[i];
            ui32 key = row["key"];
            TValue opt = row["uint"];
            if (opt.HaveValue()) {
                (*out)[key] = (ui32)opt;
            }
        }
        return true;
    };

    // must be online
    const char * picture = R"((
        (let remoteKey_ '('('key (Uint32 '1001))))
        (let range_ '('IncFrom 'IncTo '('key (Uint32 '0) (Uint32 '32))))
        (let select_ (SelectRange 'table1 range_ '('key 'uint) '()))
        (let forPlan_ (SelectRow 'table1 remoteKey_ '('uint)))
        (return (AsList (SetResult 'Result select_) (SetResult 'Some forPlan_)))
    ))";

    proxy.Enqueue(picture, extractActual, NDataShard::TTxFlags::Default);

    const char * immPicture = R"((
        (let range_ '('IncFrom 'IncTo '('key (Uint32 '%u) (Uint32 '%d))))
        (let select_ (SelectRange 'table1 range_ '('key 'uint) '()))
        (return (AsList (SetResult 'Result select_)))
    ))";

    TVector<std::pair<ui32, ui32>> shots;
    shots.reserve(4);
    shots.push_back({0,7});
    shots.push_back({8,15});
    shots.push_back({16,23});
    shots.push_back({24,31});

    bool sendImmediates = !opts.RebootOnDelay; // can lose some immediate results on restart
    if (sendImmediates) {
        // inconsistent print screen
        for (const auto& s : shots) {
            proxy.Enqueue(Sprintf(immPicture, s.first, s.second), extractActual, NDataShard::TTxFlags::Default);
        }
    }

    proxy.ExecQueue();

    PrintRandomResults(expected, "expect ");
    PrintRandomResults(actual, "actual ");
    PrintRandomResults(intermediate, "interm ");
    for (ui32 i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_EQUAL(expected[i], actual[i]);
        UNIT_ASSERT(intermediate[i] <= expected[i] || intermediate[i] == Max<ui32>());
    }

    if (sendImmediates) {
        for (const auto& s : shots)
            CompareShots(expected, intermediate, s);
    }
}

//

static constexpr ui32 NumRun() { return 2; }

Y_UNIT_TEST(RandomPoints_ReproducerDelayData1) {
    TTester::TOptions opts;
    opts.DelayData = true;
    opts.ExecutorCacheSize = 0;
    opts.EnableOutOfOrder(8);

    RandomTxDeps(opts, 8, 8, false, false,
                 {5, 5, 6, 7, 3, 6, 5, 6},
                 {28, 26, 13, 27, 3, 3, 27, 36},
                 {11, 30, 10, 12, 5, 23, 30, 32});
}

Y_UNIT_TEST(RandomPoints_ReproducerDelayRS1) {
    TTester::TOptions opts;
    opts.DelayReadSet = true;
    opts.EnableOutOfOrder(8);

    RandomTxDeps(opts, 8, 8, true, false,
                 {2, 7, 7, 7, 6, 3, 2, 4},
                 {2, 66, 30, 104, 25, 4, 0, 1},
                 {0, 40, 2, 33, 8, 4, 1, 3});
}

Y_UNIT_TEST(RandomPoints_DelayRS) {
    TTester::TOptions opts;
    opts.DelayReadSet = true;
    opts.EnableOutOfOrder(8);

    TVector<std::pair<ui32, ui32>> variants;
    variants.push_back({8, 8});
    variants.push_back({8, 16});
    variants.push_back({8, 32});
    variants.push_back({16, 16});
    variants.push_back({16, 32});
    variants.push_back({32, 8});
    variants.push_back({32, 16});
    variants.push_back({32, 32});

    for (ui32 i = 0; i < NumRun(); ++i) {
        for (auto& v : variants) {
            RandomTxDeps(opts, v.first, v.second, true);
            RandomTxDeps(opts, v.first, v.second, false);
        }
    }
}

Y_UNIT_TEST(RandomDotRanges_DelayRS) {
    TTester::TOptions opts;
    opts.DelayReadSet = true;
    opts.EnableOutOfOrder(8);

    TVector<std::pair<ui32, ui32>> variants;
    variants.push_back({8, 8});
    variants.push_back({8, 16});
    variants.push_back({8, 32});
    variants.push_back({16, 16});
    variants.push_back({16, 32});
    variants.push_back({32, 8});
    variants.push_back({32, 16});
    variants.push_back({32, 32});

    for (ui32 i = 0; i < NumRun(); ++i) {
        for (auto& v : variants) {
            RandomTxDeps(opts, v.first, v.second, true, true);
            RandomTxDeps(opts, v.first, v.second, false, true);
        }
    }
}

Y_UNIT_TEST(RandomPoints_DelayRS_Reboot) {
    TTester::TOptions opts;
    opts.DelayReadSet = true;
    opts.RebootOnDelay = true;
    opts.EnableOutOfOrder(8);

    TVector<std::pair<ui32, ui32>> variants;
    variants.push_back({8, 8});
    variants.push_back({8, 16});
    variants.push_back({8, 32});
    variants.push_back({16, 16});
    variants.push_back({16, 32});
    variants.push_back({32, 8});
    variants.push_back({32, 16});
    variants.push_back({32, 32});

    for (ui32 i = 0; i < NumRun(); ++i) {
        for (auto& v : variants) {
            RandomTxDeps(opts, v.first, v.second, true);
            RandomTxDeps(opts, v.first, v.second, false);
        }
    }
}

Y_UNIT_TEST(RandomPoints_DelayRS_Reboot_Dirty) {
    TTester::TOptions opts;
    opts.DelayReadSet = true;
    opts.RebootOnDelay = true;
    opts.EnableSoftUpdates();
    opts.EnableOutOfOrder(8);

    TVector<std::pair<ui32, ui32>> variants;
    variants.push_back({8, 8});
    variants.push_back({8, 16});
    variants.push_back({8, 32});
    variants.push_back({16, 16});
    variants.push_back({16, 32});
    variants.push_back({32, 8});
    variants.push_back({32, 16});
    variants.push_back({32, 32});

    for (ui32 i = 0; i < NumRun(); ++i) {
        for (auto& v : variants) {
            RandomTxDeps(opts, v.first, v.second, true);
            RandomTxDeps(opts, v.first, v.second, false);
        }
    }
}

Y_UNIT_TEST(RandomPoints_DelayData) {
    TTester::TOptions opts;
    opts.DelayData = true;
    opts.ExecutorCacheSize = 0;
    opts.EnableOutOfOrder(8);

    TVector<std::pair<ui32, ui32>> variants;
    variants.push_back({8, 8});
    variants.push_back({8, 16});
    variants.push_back({8, 32});
    variants.push_back({16, 16});
    variants.push_back({16, 32});
    variants.push_back({32, 8});
    variants.push_back({32, 16});
    variants.push_back({32, 32});

    for (auto& v : variants) {
        RandomTxDeps(opts, v.first, v.second, true);
        RandomTxDeps(opts, v.first, v.second, false);
    }
}

///
class TSimpleTx {
public:
    struct TSimpleRange {
        ui32 From;
        ui32 To;

        TSimpleRange(ui32 from, ui32 to)
            : From(from)
            , To(to)
        {}
    };

    TSimpleTx(ui64 txId)
        : TxId(txId)
    {}

    void Generate(ui32 numWrites, ui32 numPoints, ui32 numRanges, ui32 max = 3000, ui32 maxRange = 100) {
        Writes.reserve(numWrites);
        for (ui32 i = 0; i < numWrites; ++i) {
            Writes.emplace_back(RandomNumber<ui32>(max));
        }

        Reads.reserve(numPoints);
        for (ui32 i = 0; i < numPoints; ++i) {
            Reads.emplace_back(RandomNumber<ui32>(max));
        }

        Ranges.reserve(numRanges);
        for (ui32 i = 0; i < numPoints; ++i) {
            ui32 from = RandomNumber<ui32>(max);
            Ranges.emplace_back(TSimpleRange(from, from + 1 + RandomNumber<ui32>(maxRange-1)));
        }
    }

    TString ToText() {
        const char * writePattern = R"(
            (let $%u '('('key (Uint32 '%u))))
            (let updates_ (Append updates_ (UpdateRow 'table1 $%u '('('uint (Uint32 '%u)))))))";

        const char * readPattern = R"(
            (let $%u '('('key (Uint32 '%u))))
            (let select$%u (SelectRow 'table1 $%u '('key 'uint)))
            (let points_ (Extend points_ (ToList select$%u))))";

        const char * rangePattern = R"(
            (let $%u '('IncFrom 'IncTo '('key (Uint32 '%u) (Uint32 '%u))))
            (let points_ (Extend points_ (Member (SelectRange 'table1 $%u '('key 'uint) '()) 'List))))";

        ui32 key = 0;
        TString body = ProgramTextSwap();

        for (ui32 point : Writes) {
            body += Sprintf(writePattern, key, point, key, TxId);
            ++key;
        }

        for (ui32 point : Reads) {
            body += Sprintf(readPattern, key, point, key, key, key, key);
            ++key;
        }

        for (const auto& r : Ranges) {
            body += Sprintf(rangePattern, key, r.From, r.To, key);
            ++key;
        }

        return Sprintf(R"((
            (let updates_ (List (ListType (TypeOf (UpdateRow 'table1 '('('key (Uint32 '0))) '('('uint (Uint32 '0))))))))
            (let points_ (List (ListType (TypeOf (Unwrap (SelectRow 'table1 '('('key (Uint32 '0))) '('key 'uint)))))))
            %s
            (return (Extend (AsList (SetResult 'Result points_)) updates_))
        ))", body.data());
    }

    ui64 GetTxId() const { return TxId; }
    const TVector<ui32>& GetWrites() const { return Writes; }
    const TVector<ui32>& GetReads() const { return Reads; }
    const TVector<TSimpleRange>& GetRanges() const { return Ranges; }
    const TMap<ui32, ui32>& GetResults() const { return Results; }

    void SetResults(const TVector<ui32>& kv) {
        THashSet<ui32> points;
        for (ui32 point : Reads) {
            points.insert(point);
        }

        for (const auto& r : Ranges) {
            for (ui32 i = r.From; i <= r.To; ++i)
                points.insert(i);
        }

        for (ui32 point : points) {
            ui32 value = kv[point];
            if (value != Max<ui32>())
                Results[point] = value;
        }
    }

    void ApplyWrites(TVector<ui32>& kv) const {
        for (ui32 point : Writes) {
            kv[point] = TxId;
        }
    }

private:
    ui64 TxId;
    TVector<ui32> Writes;
    TVector<ui32> Reads;
    TVector<TSimpleRange> Ranges;
    TMap<ui32, ui32> Results;

    static TString ProgramTextSwap(ui32 txId = 0) {
        ui32 point = txId % 1000;
        return Sprintf(R"(
            (let row1 '('('key (Uint32 '%u))))
            (let row2 '('('key (Uint32 '%u))))
            (let row3 '('('key (Uint32 '%u))))
            (let val1 (FlatMap (SelectRow 'table1 row1 '('value)) (lambda '(x) (Member x 'value))))
            (let val2 (FlatMap (SelectRow 'table1 row2 '('value)) (lambda '(x) (Member x 'value))))
            (let val3 (FlatMap (SelectRow 'table1 row3 '('value)) (lambda '(x) (Member x 'value))))
            (let updates_ (Extend updates_ (AsList
                (UpdateRow 'table1 row1 '('('value val3)))
                (UpdateRow 'table1 row2 '('('value val1)))
                (UpdateRow 'table1 row3 '('('value val2)))
            )))
        )", point, 1000+point, 2000+point);
    }
};

///
class TSimpleTable {
public:
    TSimpleTable(ui32 size = 4096) {
        Points.resize(size, Max<ui32>());
    }

    void Apply(TSimpleTx& tx) {
        tx.SetResults(Points);
        tx.ApplyWrites(Points);
    }

private:
    TVector<ui32> Points;
};

void Print(const TMap<ui32, ui32>& m) {
    for (auto& pair : m)
        Cerr << pair.first << ':' << pair.second << ' ';
    Cerr << Endl;
}

void RandomPointsAndRanges(TFakeMiniKQLProxy& proxy, ui32 numTxs, ui32 maxWrites, ui32 maxReads, ui32 maxRanges) {
    TVector<std::shared_ptr<TSimpleTx>> txs;
    txs.reserve(numTxs);
    ui32 startTxId = proxy.LastTxId()+1;
    for (ui32 i = 0; i < numTxs; ++i) {
        txs.push_back(std::make_shared<TSimpleTx>(startTxId + i));
    }

    auto extractActual = [&](TFakeProxyTx& tx) -> bool {
        UNIT_ASSERT(!tx.Immediate());

        auto& expected = txs[tx.TxId()-startTxId]->GetResults();
        TMap<ui32, ui32> actual;

        NKikimrMiniKQL::TResult res = tx.GetResult();
        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue rList = value["Result"];
        for (ui32 i = 0; i < rList.Size(); ++i) {
            TValue row = rList[i];
            ui32 key = row["key"];
            TValue opt = row["uint"];
            if (opt.HaveValue()) {
                actual[key] = (ui32)opt;
            }
        }

        if (expected != actual) {
            Print(expected);
            Print(actual);
            UNIT_ASSERT(false);
        }
        return true;
    };

    TSimpleTable table;
    for (auto tx : txs) {
        tx->Generate(RandomNumber<ui32>(maxWrites-1)+1,
                     RandomNumber<ui32>(maxReads-1)+1,
                     RandomNumber<ui32>(maxRanges-1)+1);
        table.Apply(*tx);
        TString progText = tx->ToText();
        //Cout << progText << Endl;
        proxy.Enqueue(progText, extractActual, NDataShard::TTxFlags::ForceOnline);
    }

    proxy.ExecQueue();
}

static void RandomPointsAndRanges(const TTester::TOptions& opts, ui32 numTxs, ui32 maxWrites, ui32 maxReads, ui32 maxRanges) {
    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);

    RandomPointsAndRanges(proxy, numTxs, maxWrites, maxReads, maxRanges);
}

Y_UNIT_TEST(RandomPointsAndRanges) {
    TTester::TOptions opts;
    opts.ExecutorCacheSize = 0;
    opts.EnableOutOfOrder(8);

    TVector<TVector<ui32>> variants;
    variants.push_back(TVector<ui32>() = {100, 20, 20, 20});
    variants.push_back(TVector<ui32>() = {100, 50, 50, 50});
    variants.push_back(TVector<ui32>() = {100, 40, 30, 20});
    variants.push_back(TVector<ui32>() = {400, 20, 20, 20});

    for (auto& v : variants) {
        RandomPointsAndRanges(opts, v[0], v[1], v[2], v[3]);
    }
}
}

///
Y_UNIT_TEST_SUITE(DataShardScan) {

Y_UNIT_TEST(ScanFollowedByUpdate) {
    TTester::TOptions opts;
    opts.ExecutorCacheSize = 0;
    opts.EnableOutOfOrder(8);

    TTester t(TTester::ESchema_MultiShardKV, opts);
    TFakeMiniKQLProxy proxy(t);

    auto checkScanResult = [](const TFakeProxyTx &tx, TSet<TString> ref) -> bool {
        const TFakeScanTx &scanTx = dynamic_cast<const TFakeScanTx &>(tx);
        YdbOld::ResultSet res = scanTx.GetScanResult();
        //Cerr << res.DebugString() << Endl;
        for (auto &row : res.rows()) {
            auto &val = row.items(0).text_value();
            UNIT_ASSERT(ref.contains(val));
            ref.erase(val);
        }

        UNIT_ASSERT(ref.empty());

        return true;
    };

    InitCrossShard_ABC(proxy, {{1, 2, 3}});

    NKikimrTxDataShard::TDataTransaction dataTransaction;
    dataTransaction.SetStreamResponse(true);
    dataTransaction.SetImmediate(false);
    dataTransaction.SetReadOnly(true);
    auto &tx = *dataTransaction.MutableReadTableTransaction();
    tx.MutableTableId()->SetOwnerId(FAKE_SCHEMESHARD_TABLET_ID);
    tx.MutableTableId()->SetTableId(13);
    auto &range = *tx.MutableRange();
    range.SetFromInclusive(true);
    range.SetToInclusive(true);
    auto &c = *tx.AddColumns();
    c.SetId(56);
    c.SetName("value");
    c.SetTypeId(NScheme::NTypeIds::Utf8);

    TSet<TString> ref1{"A", "B", "C"};
    proxy.EnqueueScan(dataTransaction.SerializeAsString(), [ref1, checkScanResult](TFakeProxyTx& tx) {
            return checkScanResult(tx, ref1);
        }, NDataShard::TTxFlags::Default);

    ui32 N = 30;

    auto programText = R"((
        (let row1_ '('('key (Uint32 '0))))
        (let row2_ '('('key (Uint32 '1000))))
        (let row3_ '('('key (Uint32 '2000))))
        (let upd1_ '('('value (Utf8 'A%u)) '('uint (Uint32 '1%u))))
        (let upd2_ '('('value (Utf8 'B%u)) '('uint (Uint32 '2%u))))
        (let upd3_ '('('value (Utf8 'C%u)) '('uint (Uint32 '3%u))))
        (let ret_ (AsList
            (UpdateRow 'table1 row1_ upd1_)
            (UpdateRow 'table1 row2_ upd2_)
            (UpdateRow 'table1 row3_ upd3_)
        ))
        (return ret_)
    ))";

    for (ui32 i = 1; i <= N; ++i) {
        proxy.Enqueue(Sprintf(programText, i, i, i, i, i, i));
    }

    proxy.ExecQueue();

    TSet<TString> ref2{"A" + ToString(N), "B" + ToString(N), "C" + ToString(N)};
    proxy.EnqueueScan(dataTransaction.SerializeAsString(), [ref2, checkScanResult](TFakeProxyTx& tx) {
            return checkScanResult(tx, ref2);
        }, NDataShard::TTxFlags::Default);
    proxy.ExecQueue();
}

}

Y_UNIT_TEST_SUITE(DataShardOutOfOrder) {

Y_UNIT_TEST_TWIN(TestOutOfOrderLockLost, StreamLookup) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    auto sender3Session = CreateSessionRPC(runtime);
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    }

    // Now send a simple request that would upsert a new value into table-1
    // It would have broken locks if executed before the above commit
    // However the above commit must succeed (readsets are already being exchanged)
    auto f3 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3)"), sender3Session, "", true));

    // Schedule a simple timer to simulate some time passing
    {
        auto sender4 = runtime.AllocateEdgeActor();
        runtime.Schedule(new IEventHandle(sender4, sender4, new TEvents::TEvWakeup()), TDuration::Seconds(1));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender4);
    }

    // Whatever happens we should resend blocked readsets
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Read the immediate reply first, it must always succeed
    {
        auto response = AwaitResponse(runtime, f3);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Read the commit reply next
    bool committed;
    {
        auto response = AwaitResponse(runtime, f2);
        if (response.operation().status() == Ydb::StatusIds::ABORTED) {
            // Let's suppose somehow locks still managed to become invalidated
            committed = false;
        } else {
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            committed = true;
        }
    }

    // Select keys 3 and 4 from both tables, either both or none should be inserted
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(
            $rows = (
                SELECT key, value FROM `/Root/table-1` WHERE key = 3
                UNION ALL
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
            );
            SELECT key, value FROM $rows ORDER BY key)"));
        TString expected;
        if (committed) {
            expected = "{ items { uint32_value: 3 } items { uint32_value: 2 } }, { items { uint32_value: 4 } items { uint32_value: 2 } }";
        } else {
            expected = "";
        }
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }
}

Y_UNIT_TEST_QUAD(TestOutOfOrderReadOnlyAllowed, StreamLookup, EvWrite) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", 1);

    {
        auto rows = EvWrite ? TEvWriteRows{{tableId1, {1, 1}}, {tableId2, {2, 1}}} : TEvWriteRows{};
        auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));
    }

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    auto rows = EvWrite ? TEvWriteRows{{tableId1, {3, 2}}, {tableId2, {4, 2}}} : TEvWriteRows{};
    auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

    // Send a commit request, it would block on readset exchange
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    evWriteObservers = TTestActorRuntimeBase::TEventObserverHolderPair{};

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    }

    // Now send a simple read request from table-1
    // Since it's readonly it cannot affect inflight transaction and should be allowed
    // Note: volatile transactions execute immediately and reads are blocked until resolved
    if (!usesVolatileTxs) {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` ORDER BY key"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
    }

    // Whatever happens we should resend blocked readsets
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Read the commit reply next, it must succeed
    {
        auto response = AwaitResponse(runtime, f2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Select keys 3 and 4 from both tables, both should have been be inserted
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(
            $rows = (
                SELECT key, value FROM `/Root/table-1` WHERE key = 3
                UNION ALL
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
            );
            SELECT key, value FROM $rows ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 3 } items { uint32_value: 2 } }, { items { uint32_value: 4 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST_QUAD(TestOutOfOrderNonConflictingWrites, StreamLookup, EvWrite) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetAppConfig(app)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", 1);

    {
        auto rows = EvWrite ? TEvWriteRows{{tableId1, {1, 1}}, {tableId2, {2, 1}}} : TEvWriteRows{};
        auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));
    }

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    bool blockReadSets = true;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (blockReadSets && event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    auto rows = EvWrite ? TEvWriteRows{{tableId1, {3, 2}}, {tableId2, {4, 2}}} : TEvWriteRows{};
    auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

    // Send a commit request, it would block on readset exchange
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    evWriteObservers = TTestActorRuntimeBase::TEventObserverHolderPair{};

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Now send non-conflicting upsert to both tables
    {
        auto rows1 = EvWrite ? TEvWriteRows{{tableId1, {5, 3}}, {tableId2, {6, 3}}} : TEvWriteRows{};
        auto evWriteObservers1 = ReplaceEvProposeTransactionWithEvWrite(runtime, rows1);

        blockReadSets = false;  // needed for volatile transactions
        auto result = KqpSimpleExec(runtime, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (6, 3))"));
        UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        blockReadSets = true; // restore to blocking
    }

    // Check that immediate non-conflicting upsert is working too
    {
        auto rows1 = EvWrite ? TEvWriteRows{{tableId1, {7, 4}}} : TEvWriteRows{};
        auto evWriteObservers1 = ReplaceEvProposeTransactionWithEvWrite(runtime, rows1);

        auto result = KqpSimpleExec(runtime, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (7, 4)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
    }

    // Resend previousy blocked readsets
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Read the commit reply next, it must succeed
    {
        auto response = AwaitResponse(runtime, f2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Select keys 3 and 4 from both tables, both should have been inserted
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(
            $rows = (
                SELECT key, value FROM `/Root/table-1` WHERE key = 3
                UNION ALL
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
            );
            SELECT key, value FROM $rows ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 3 } items { uint32_value: 2 } }, { items { uint32_value: 4 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST_TWIN(TestOutOfOrderRestartLocksReorderedWithoutBarrier, StreamLookup) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetAppConfig(app)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // This test requires barrier to be disabled
    runtime.GetAppData().FeatureFlags.SetDisableDataShardBarrier(true);

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    bool blockReadSets = true;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (blockReadSets && event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Execute some out-of-order upserts before rebooting
    blockReadSets = false; // needed for volatile transactions
    ExecSQL(server, sender, Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (6, 3))"));
    blockReadSets = true; // restore back to blocking

    // Select key 3, we expect a timeout, because logically writes
    // to 3 and 5 already happened, but physically write to 3 is still waiting.
    {
        TString tmpSessionId = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 3;"), tmpSessionId, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::TIMEOUT);
    }

    // Reboot table-1 tablet
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    readSets.clear();
    RebootTablet(runtime, table1shards[0], sender);

    // Wait until we captured both readsets again
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_C(readSets.size() >= expectedReadSets, "expected " << readSets.size() << " >= " << expectedReadSets);

    // Select key 3, we still expect a timeout
    {
        TString tmpSessionId = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 3;"), tmpSessionId, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::TIMEOUT);
    }

    // Select key 5, it shouldn't pose any problems
    {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 5;"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 5 } items { uint32_value: 3 } }");
    }

    // Release readsets allowing tx to progress
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* viaActorSystem */ true);
    }

    // Select key 3, we expect a success
    {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 3;"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 3 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST_TWIN(TestOutOfOrderNoBarrierRestartImmediateLongTail, StreamLookup) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetAppConfig(app)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // This test requires barrier to be disabled
    runtime.GetAppData().FeatureFlags.SetDisableDataShardBarrier(true);

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);
    TString sessionId3 = CreateSessionRPC(runtime);
    TString sessionId4 = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    THashMap<TActorId, ui64> actorToTablet;
    TVector<THolder<IEventHandle>> readSets;
    TVector<THolder<IEventHandle>> progressEvents;
    bool blockReadSets = true;
    bool blockProgressEvents = false;
    size_t bypassProgressEvents = 0;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        auto recipient = event->GetRecipientRewrite();
        switch (event->GetTypeRewrite()) {
            case TEvTablet::EvBoot: {
                auto* msg = event->Get<TEvTablet::TEvBoot>();
                auto tabletId = msg->TabletID;
                Cerr << "... found " << recipient << " to be tablet " << tabletId << Endl;
                actorToTablet[recipient] = tabletId;
                break;
            }
            case TEvTxProcessing::EvReadSet: {
                if (blockReadSets) {
                    readSets.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0 /* EvProgressTransaction */: {
                ui64 tabletId = actorToTablet.Value(recipient, 0);
                if (blockProgressEvents && tabletId == table1shards[0]) {
                    if (bypassProgressEvents == 0) {
                        Cerr << "... captured TEvProgressTransaction" << Endl;
                        progressEvents.push_back(std::move(event));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    Cerr << "... bypass for TEvProgressTransaction" << Endl;
                    --bypassProgressEvents;
                }
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto fCommit = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    blockReadSets = false; // needed when upsert is volatile

    // Send some more requests that form a staircase, they would all be blocked as well
    auto f3 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3), (5, 3);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 3), (6, 3))"), sessionId3, "", true));
    SimulateSleep(server, TDuration::Seconds(1));
    auto f4 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 4), (7, 4);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (6, 4), (8, 4))"), sessionId4, "", true));
    SimulateSleep(server, TDuration::Seconds(1));

    // One more request that would be executed out of order
    ExecSQL(server, sender, Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (11, 5);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (12, 5))"));

    blockReadSets = true; // restore after a volatile upsert

    // Select key 7, we expect a timeout, because logically a write to it already happened
    // Note: volatile upserts are not blocked however, so it will succeed
    {
        auto sender4 = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 7;"), sender4, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), usesVolatileTxs ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::TIMEOUT);
    }

    // Reboot table-1 tablet
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    readSets.clear();
    blockProgressEvents = true;
    bypassProgressEvents = 1;
    Cerr << "... rebooting tablet" << Endl;
    RebootTablet(runtime, table1shards[0], sender);
    Cerr << "... tablet rebooted" << Endl;

    // Wait until we captured both readsets again
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_C(readSets.size() >= expectedReadSets, "expected " << readSets.size() << " >= " << expectedReadSets);

    // Wait until we have a pending progress event
    if (usesVolatileTxs) {
        // Transaction does not restart when volatile (it's already executed)
        SimulateSleep(server, TDuration::Seconds(1));
    } else {
        if (progressEvents.size() < 1) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                [&](IEventHandle &) -> bool {
                    return progressEvents.size() >= 1;
                });
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(progressEvents.size(), 1u);
    }

    // Select key 7 again, we still expect a timeout, because logically a write to it already happened
    // Note: volatile upserts are not blocked however, so it will succeed
    {
        auto sender5 = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 7;"), sender5, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), usesVolatileTxs ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::TIMEOUT);
    }

    // Stop blocking readsets and unblock progress
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, true);
    }
    for (auto& ev : progressEvents) {
        runtime.Send(ev.Release(), 0, true);
    }

    // Select key 7 again, this time is should succeed
    {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 7;"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 7 } items { uint32_value: 4 } }");
    }
}

Y_UNIT_TEST(TestPlannedTimeoutSplit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    TString senderWrite1 = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    auto shards1 = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);
    auto shards2 = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 1u);

    // Capture and block some messages
    TVector<THolder<IEventHandle>> txProposes;
    auto captureMessages = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProxy::EvProposeTransaction: {
                Cerr << "---- observed EvProposeTransaction ----" << Endl;
                txProposes.push_back(std::move(event));
                return TTestActorRuntime::EEventAction::DROP;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureMessages);

    // Send a distributed write while capturing coordinator propose
    auto fWrite1 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (101, 101);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (202, 202);
    )"), senderWrite1, "", true));
    if (txProposes.size() < 1) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return txProposes.size() >= 1;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(txProposes.size(), 1u);
    runtime.SetObserverFunc(prevObserverFunc);

    size_t observedSplits = 0;
    auto observeSplits = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvSplit: {
                Cerr << "---- observed EvSplit ----" << Endl;
                ++observedSplits;
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observeSplits);

    // Split would fail otherwise :(
    SetSplitMergePartCountLimit(server->GetRuntime(), -1);

    // Start split for table-1 and table-2
    auto senderSplit = runtime.AllocateEdgeActor();
    ui64 txId1 = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1[0], 100);
    ui64 txId2 = AsyncSplitTable(server, senderSplit, "/Root/table-2", shards2[0], 100);

    // Wait until we observe both splits on both shards
    if (observedSplits < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return observedSplits >= 2;
            });
        runtime.DispatchEvents(options);
    }

    // Sleep a little so everything settles
    SimulateSleep(server, TDuration::Seconds(1));

    // We expect splits to finish successfully
    WaitTxNotification(server, senderSplit, txId1);
    WaitTxNotification(server, senderSplit, txId2);

    // We expect split to fully succeed on proposed transaction timeout
    auto shards1new = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1new.size(), 2u);
    auto shards2new = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2new.size(), 2u);

    // Unblock previously blocked coordinator propose
    for (auto& ev : txProposes) {
        runtime.Send(ev.Release(), 0, true);
    }

    // Wait for query to return an error
    {
        auto response = AwaitResponse(runtime, fWrite1);
        UNIT_ASSERT_C(
            response.operation().status() == Ydb::StatusIds::ABORTED ||
            response.operation().status() == Ydb::StatusIds::UNAVAILABLE,
            "Status: " << response.operation().status());
    }
}

Y_UNIT_TEST(TestPlannedHalfOverloadedSplit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    TString senderWrite1 = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    auto shards1 = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);
    auto shards2 = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 1u);
    TVector<ui64> tablets;
    tablets.push_back(shards1[0]);
    tablets.push_back(shards2[0]);

    // Capture and block some messages
    TVector<THolder<IEventHandle>> txProposes;
    TVector<THolder<IEventHandle>> txProposeResults;
    auto captureMessages = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvProposeTransaction: {
                Cerr << "---- observed EvProposeTransactionResult ----" << Endl;
                if (txProposes.size() == 0) {
                    // Capture the first propose
                    txProposes.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvDataShard::EvProposeTransactionResult: {
                Cerr << "---- observed EvProposeTransactionResult ----" << Endl;
                if (txProposes.size() > 0) {
                    // Capture all propose results
                    txProposeResults.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureMessages);

    // Send a distributed write while capturing coordinator propose
    auto fWrite1 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (101, 101);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (202, 202);
    )"), senderWrite1, "", true));
    if (txProposes.size() < 1 || txProposeResults.size() < 1) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return txProposes.size() >= 1 && txProposeResults.size() >= 1;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(txProposes.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(txProposeResults.size(), 1u);

    size_t observedSplits = 0;
    auto observeSplits = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvSplit: {
                Cerr << "---- observed EvSplit ----" << Endl;
                ++observedSplits;
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observeSplits);

    // Split would fail otherwise :(
    SetSplitMergePartCountLimit(server->GetRuntime(), -1);

    // Start split for table-1 and table-2
    auto senderSplit = runtime.AllocateEdgeActor();
    ui64 txId1 = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1[0], 100);
    ui64 txId2 = AsyncSplitTable(server, senderSplit, "/Root/table-2", shards2[0], 100);

    // Wait until we observe both splits on both shards
    if (observedSplits < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return observedSplits >= 2;
            });
        runtime.DispatchEvents(options);
    }

    // Sleep a little so everything settles
    SimulateSleep(server, TDuration::Seconds(1));

    // Unblock previously blocked proposes and results
    for (auto& ev : txProposeResults) {
        runtime.Send(ev.Release(), 0, true);
    }
    for (auto& ev : txProposes) {
        runtime.Send(ev.Release(), 0, true);
    }

    // We expect splits to finish successfully
    WaitTxNotification(server, senderSplit, txId1);
    WaitTxNotification(server, senderSplit, txId2);

    // We expect split to fully succeed on proposed transaction timeout
    auto shards1new = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1new.size(), 2u);
    auto shards2new = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2new.size(), 2u);

    // Wait for query to return an error
    {
        auto response = AwaitResponse(runtime, fWrite1);
        UNIT_ASSERT_C(
            response.operation().status() == Ydb::StatusIds::ABORTED ||
            response.operation().status() == Ydb::StatusIds::OVERLOADED ||
            response.operation().status() == Ydb::StatusIds::UNAVAILABLE,
            "Status: " << response.operation().status());
    }
}

namespace {

    void AsyncReadTable(
            Tests::TServer::TPtr server,
            TActorId sender,
            const TString& path)
    {
        auto &runtime = *server->GetRuntime();

        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetStreamResponse(true);
        auto &tx = *request->Record.MutableTransaction()->MutableReadTableTransaction();
        tx.SetPath(path);
        tx.SetApiVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
        tx.AddColumns("key");
        tx.AddColumns("value");

        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    }

}

/**
 * Regression test for KIKIMR-7751, designed to crash under asan
 */
Y_UNIT_TEST(TestReadTableWriteConflict) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    // NOTE: table-1 has 2 shards so ReadTable is not immediate
    CreateShardedTable(server, sender, "/Root", "table-1", 2);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    TString senderWriteImm = CreateSessionRPC(runtime);
    TString senderWriteDist = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(
            "SELECT * FROM `/Root/table-1` "
            "UNION ALL "
            "SELECT * FROM `/Root/table-2` "
            "ORDER BY key"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
             "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
             "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    TVector<THolder<IEventHandle>> txProposes;
    size_t seenPlanSteps = 0;
    bool captureReadSets = true;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProcessing::EvPlanStep:
                Cerr << "---- observed EvPlanStep ----" << Endl;
                ++seenPlanSteps;
                break;
            case TEvTxProcessing::EvReadSet:
                Cerr << "---- observed EvReadSet ----" << Endl;
                if (captureReadSets) {
                    readSets.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto qCommit = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2); "
        "UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2); "), sessionId, txId, true));

    // Wait until we captured all readsets
    const size_t expectedReadSets = usesVolatileTxs ? 8 : 4;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    captureReadSets = false;

    // Start reading table-1, wait for its plan step
    seenPlanSteps = 0;
    auto senderReadTable = runtime.AllocateEdgeActor();
    AsyncReadTable(server, senderReadTable, "/Root/table-1");
    if (seenPlanSteps < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return seenPlanSteps >= 2;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 2u);
    seenPlanSteps = 0;

    // Start an immediate write to table-1, it won't be able to start
    // because it arrived after the read table and they block each other
    auto fWriteImm = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3)"), senderWriteImm, "", true));

    // Start a planned write to both tables, wait for its plan step too
    auto fWriteDist = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (7, 4); "
        "UPSERT INTO `/Root/table-2` (key, value) VALUES (8, 4)"), senderWriteDist, "", true));
    if (seenPlanSteps < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return seenPlanSteps >= 2;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 2u);
    seenPlanSteps = 0;

    // Make sure everything is settled down
    SimulateSleep(server, TDuration::Seconds(1));

    // Unblock readsets, letting everything go
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Wait for commit to succeed
    {
        auto response = AwaitResponse(runtime, qCommit);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Wait for immediate write to succeed
    {
        auto response = AwaitResponse(runtime, fWriteImm);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Wait for distributed write to succeed
    {
        auto response = AwaitResponse(runtime, fWriteDist);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
}

/**
 * Regression test for KIKIMR-7903
 */
Y_UNIT_TEST(TestReadTableImmediateWriteBlock) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    // NOTE: table-1 has 2 shards so ReadTable is not immediate
    CreateShardedTable(server, sender, "/Root", "table-1", 2);

    TString senderWriteImm = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));

    // Capture and block all readset messages
    size_t seenPlanSteps = 0;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProcessing::EvPlanStep:
                Cerr << "---- observed EvPlanStep ----" << Endl;
                ++seenPlanSteps;
                break;
            case TEvTxProcessing::EvStreamClearanceResponse:
                Cerr << "---- dropped EvStreamClearanceResponse ----" << Endl;
                return TTestActorRuntime::EEventAction::DROP;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    // Start reading table-1, wait for its plan step
    // Since we drop EvStreamClearanceResponse it will block forever
    seenPlanSteps = 0;
    auto senderReadTable = runtime.AllocateEdgeActor();
    AsyncReadTable(server, senderReadTable, "/Root/table-1");
    if (seenPlanSteps < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return seenPlanSteps >= 2;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 2u);

    // Make sure everything is settled down
    SimulateSleep(server, TDuration::Seconds(1));

    // Start an immediate write to table-1, it should be able to complete
    auto fWriteImm = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3)"), senderWriteImm, "", true));

    // Wait for immediate write to succeed
    {
        auto response = AwaitResponse(runtime, fWriteImm);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(TestReadTableSingleShardImmediate) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));

    // Capture and block all readset messages
    size_t seenPlanSteps = 0;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProcessing::EvPlanStep:
                Cerr << "---- observed EvPlanStep ----" << Endl;
                ++seenPlanSteps;
                break;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    // Start reading table-1
    seenPlanSteps = 0;
    auto senderReadTable = runtime.AllocateEdgeActor();
    AsyncReadTable(server, senderReadTable, "/Root/table-1");

    // Wait for the first quota request
    runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvStreamQuotaRequest>(senderReadTable);

    // Since ReadTable was for a single-shard table we shouldn't see any plan steps
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 0u);
}

Y_UNIT_TEST(TestImmediateQueueThenSplit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    auto tablets = GetTableShards(server, sender, "/Root/table-1");

    // We need shard to have some data (otherwise it would die too quickly)
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);"));

    bool captureSplit = true;
    bool captureSplitChanged = true;
    bool capturePropose = true;
    THashSet<TActorId> captureDelayedProposeFrom;
    TVector<THolder<IEventHandle>> eventsSplit;
    TVector<THolder<IEventHandle>> eventsSplitChanged;
    TVector<THolder<IEventHandle>> eventsPropose;
    TVector<THolder<IEventHandle>> eventsDelayedPropose;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvSplit:
                if (captureSplit) {
                    Cerr << "---- captured EvSplit ----" << Endl;
                    eventsSplit.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case TEvDataShard::EvSplitPartitioningChanged:
                if (captureSplitChanged) {
                    Cerr << "---- captured EvSplitPartitioningChanged ----" << Endl;
                    eventsSplitChanged.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case TEvDataShard::EvProposeTransaction:
                if (capturePropose) {
                    Cerr << "---- capture EvProposeTransaction ----" << Endl;
                    eventsPropose.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 2 /* EvDelayedProposeTransaction */:
                if (captureDelayedProposeFrom.contains(event->GetRecipientRewrite())) {
                    Cerr << "---- capture EvDelayedProposeTransaction ----" << Endl;
                    Cerr << event->GetTypeName() << Endl;
                    eventsDelayedPropose.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    const int totalWrites = 10;
    TVector<TActorId> writeSenders;

    // Send a lot of write requests in parallel (so there's a large propose queue)
    for (int i = 0; i < totalWrites; ++i) {
        auto writeSender = runtime.AllocateEdgeActor();
        SendSQL(server, writeSender, Q_(Sprintf("UPSERT INTO `/Root/table-1` (key, value) VALUES (%d, %d);", i, i)));
        writeSenders.push_back(writeSender);
    }

    // Split would fail otherwise :(
    SetSplitMergePartCountLimit(server->GetRuntime(), -1);

    // Start split for table-1
    TInstant splitStart = TInstant::Now();
    auto senderSplit = runtime.AllocateEdgeActor();
    ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets[0], 100);

    // Wait until all propose requests and split reach our shard
    if (!(eventsSplit.size() >= 1 && eventsPropose.size() >= totalWrites)) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle&) -> bool {
                return eventsSplit.size() >= 1 && eventsPropose.size() >= totalWrites;
            });
        runtime.DispatchEvents(options);
    }

    // Unblock all propose requests (so they all successfully pass state test)
    capturePropose = false;
    for (auto& ev : eventsPropose) {
        Cerr << "---- Unblocking propose transaction ----" << Endl;
        captureDelayedProposeFrom.insert(ev->GetRecipientRewrite());
        runtime.Send(ev.Release(), 0, true);
    }
    eventsPropose.clear();

    // Unblock split request (shard will move to SplitSrcWaitForNoTxInFlight)
    captureSplit = false;
    captureSplitChanged = true;
    for (auto& ev : eventsSplit) {
        Cerr << "---- Unblocking split ----" << Endl;
        runtime.Send(ev.Release(), 0, true);
    }
    eventsSplit.clear();

    // Wait until split is finished and we have a delayed propose
    Cerr << "---- Waiting for split to finish ----" << Endl;
    if (!(eventsSplitChanged.size() >= 1 && eventsDelayedPropose.size() >= 1)) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle&) -> bool {
                return eventsSplitChanged.size() >= 1 && eventsDelayedPropose.size() >= 1;
            });
        runtime.DispatchEvents(options);
    }

    // Cause split to finish before the first delayed propose
    captureSplitChanged = false;
    for (auto& ev : eventsSplitChanged) {
        Cerr << "---- Unblocking split finish ----" << Endl;
        runtime.Send(ev.Release(), 0, true);
    }
    eventsSplitChanged.clear();

    // Unblock delayed propose transactions
    captureDelayedProposeFrom.clear();
    for (auto& ev : eventsDelayedPropose) {
        Cerr << "---- Unblocking delayed propose ----" << Endl;
        runtime.Send(ev.Release(), 0, true);
    }
    eventsDelayedPropose.clear();

    // Wait for split to finish at schemeshard
    WaitTxNotification(server, senderSplit, txId);

    // Split shouldn't take too much time to complete
    TDuration elapsed = TInstant::Now() - splitStart;
    UNIT_ASSERT_C(elapsed < TDuration::Seconds(NValgrind::PlainOrUnderValgrind(5, 25)),
        "Split needed " << elapsed.ToString() << " to complete, which is too long");

    // Count transaction results
    int successes = 0;
    int failures = 0;
    for (auto writeSender : writeSenders) {
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(writeSender);
        if (ev->Get()->Record.GetRef().GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            ++successes;
        } else {
            ++failures;
        }
    }

    // We expect all transactions to fail
    UNIT_ASSERT_C(successes + failures == totalWrites,
        "Unexpected "
        << successes << " successes and "
        << failures << " failures");
}

void TestLateKqpQueryAfterColumnDrop(bool dataQuery, const TString& query) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(false);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    auto streamSender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_WORKER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_TASKS_RUNNER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value1", "Uint32", false, false},
                {"value2", "Uint32", false, false}}));

    ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value1, value2) VALUES (1, 1, 10), (2, 2, 20);");

    bool capturePropose = true;
    TVector<THolder<IEventHandle>> eventsPropose;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) -> auto {
        // if (ev->GetRecipientRewrite() == streamSender) {
        //     Cerr << "Stream sender got " << ev->GetTypeRewrite() << " " << ev->GetBase()->ToStringHeader() << Endl;
        // }
        switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvProposeTransaction: {
                auto &rec = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (capturePropose && rec.GetTxKind() != NKikimrTxDataShard::TX_KIND_SNAPSHOT) {
                    Cerr << "---- capture EvProposeTransaction ---- type=" << rec.GetTxKind() << Endl;
                    eventsPropose.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }

            case TEvDataShard::EvKqpScan: {
                if (capturePropose) {
                    Cerr << "---- capture EvKqpScan ----" << Endl;
                    eventsPropose.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    std::function<void()> processEvents = [&]() {
        // Wait until there's exactly one propose message at our datashard
        if (eventsPropose.size() < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return eventsPropose.size() >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(eventsPropose.size(), 1u);
        Cerr << "--- captured scan tx proposal" << Endl;
        capturePropose = false;

        // Drop column value2 and wait for drop to finish
        auto dropTxId = AsyncAlterDropColumn(server, "/Root", "table-1", "value2");
        WaitTxNotification(server, dropTxId);

        // Resend delayed propose messages
        Cerr << "--- resending captured proposals" << Endl;
        for (auto& ev : eventsPropose) {
            runtime.Send(ev.Release(), 0, true);
        }
        eventsPropose.clear();
        return;
    };

    if (dataQuery) {
        Cerr << "--- sending data query request" << Endl;
        auto tmp = CreateSessionRPC(runtime);
        auto f = SendRequest(runtime, MakeSimpleRequestRPC(query, tmp, "", true));
        processEvents();
        auto response = AwaitResponse(runtime, f);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::ABORTED);
    } else {
        Cerr << "--- sending stream request" << Endl;
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, query, false));
        processEvents();

        Cerr << "--- waiting for result" << Endl;
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        auto& response = ev->Get()->Record.GetRef();
        Cerr << response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::ABORTED);
        auto& issue = response.GetResponse().GetQueryIssues(0);
        UNIT_ASSERT_VALUES_EQUAL(issue.issue_code(), (int) NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH);
        UNIT_ASSERT_STRINGS_EQUAL(issue.message(), "Table \'/Root/table-1\' scheme changed.");
    }
}

Y_UNIT_TEST(TestLateKqpScanAfterColumnDrop) {
    TestLateKqpQueryAfterColumnDrop(false, "SELECT SUM(value2) FROM `/Root/table-1`");
}

Y_UNIT_TEST(TestSecondaryClearanceAfterShardRestartRace) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 2);
    auto shards = GetTableShards(server, sender, "/Root/table-1");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);"));

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // We want to intercept delivery problem notifications
    TVector<THolder<IEventHandle>> capturedDeliveryProblem;
    size_t seenStreamClearanceRequests = 0;
    size_t seenStreamClearanceResponses = 0;
    auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvPipeCache::TEvDeliveryProblem::EventType: {
                Cerr << "... intercepted TEvDeliveryProblem" << Endl;
                capturedDeliveryProblem.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvTxProcessing::TEvStreamQuotaRelease::EventType: {
                Cerr << "... dropped TEvStreamQuotaRelease" << Endl;
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvTxProcessing::TEvStreamClearanceRequest::EventType: {
                Cerr << "... observed TEvStreamClearanceRequest" << Endl;
                ++seenStreamClearanceRequests;
                break;
            }
            case TEvTxProcessing::TEvStreamClearanceResponse::EventType: {
                Cerr << "... observed TEvStreamClearanceResponse" << Endl;
                ++seenStreamClearanceResponses;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(captureEvents);

    auto state = StartReadShardedTable(server, "/Root/table-1", TRowVersion::Max(), /* pause */ true, /* ordered */ false);

    waitFor([&]{ return seenStreamClearanceResponses >= 2; }, "observed TEvStreamClearanceResponse");

    seenStreamClearanceRequests = 0;
    seenStreamClearanceResponses = 0;
    RebootTablet(runtime, shards[0], sender);

    waitFor([&]{ return capturedDeliveryProblem.size() >= 1; }, "intercepted TEvDeliveryProblem");
    waitFor([&]{ return seenStreamClearanceRequests >= 1; }, "observed TEvStreamClearanceRequest");

    runtime.SetObserverFunc(prevObserver);
    for (auto& ev : capturedDeliveryProblem) {
        runtime.Send(ev.Release(), 0, true);
    }

    ResumeReadShardedTable(server, state);

    // We expect this upsert to complete successfully
    // When there's a bug it will get stuck due to readtable before barrier
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 4);"));
}

Y_UNIT_TEST_TWIN(TestShardRestartNoUndeterminedImmediate, StreamLookup) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
        .Indexes({
            // Note: async index forces read before write at the shard level,
            // which causes immediate upsert to block until volatile transaction
            // is resolved.
            TShardedTableOptions::TIndex{
                "by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync
            }
        }));
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);
    TString sender3Session = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    size_t delayedProposeCount = 0;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                readSets.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
            case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 2 /* EvDelayedProposeTransaction */: {
                ++delayedProposeCount;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto myCommit2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    waitFor([&]{ return readSets.size() >= expectedReadSets; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Now send an upsert to table-1, it should be blocked by our in-progress tx
    delayedProposeCount = 0;
    Cerr << "... sending immediate upsert" << Endl;
    auto f3 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 42), (3, 51))"), sender3Session, "", true));

    // Wait unti that propose starts to execute
    waitFor([&]{ return delayedProposeCount >= 1; }, "immediate propose");
    UNIT_ASSERT_VALUES_EQUAL(delayedProposeCount, 1u);
    Cerr << "... immediate upsert is blocked" << Endl;

    // Remove observer and gracefully restart the shard
    runtime.SetObserverFunc(prevObserverFunc);
    GracefulRestartTablet(runtime, table1shards[0], sender);

    // The result of immediate upsert must be neither SUCCESS nor UNDETERMINED
    {
        auto response = AwaitResponse(runtime, f3);
        UNIT_ASSERT_VALUES_UNEQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_UNEQUAL(response.operation().status(), Ydb::StatusIds::UNDETERMINED);
    }

    // Select key 1 and verify its value was not updated
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(SELECT key, value FROM `/Root/table-1` WHERE key = 1 ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
    }
}

Y_UNIT_TEST_QUAD(TestShardRestartPlannedCommitShouldSucceed, StreamLookup, EvWrite) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app)
        // Note: currently volatile transactions don't survive tablet reboots,
        // and reply with UNDETERMINED similar to immediate transactions.
        .SetEnableDataShardVolatileTransactions(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", 1);

    {
        auto rows = EvWrite ? TEvWriteRows{{tableId1, {1, 1}}, {tableId2, {2, 1}}} : TEvWriteRows{};
        auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

        Cerr << "===== UPSERT initial rows" << Endl;

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1)"));
    }

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        Cerr << "===== Begin SELECT" << Endl;

        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                Cerr << "... captured readset" << Endl;
                readSets.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    auto rows = EvWrite ? TEvWriteRows{{tableId1, {3, 2}}, {tableId2, {4, 2}}} : TEvWriteRows{};
    auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

    Cerr << "===== UPSERT and commit" << Endl;

    // Send a commit request, it would block on readset exchange
    auto myCommit2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    waitFor([&]{ return readSets.size() >= expectedReadSets; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Remove observer and gracefully restart the shard
    Cerr << "===== restarting tablet" << Endl;
    runtime.SetObserverFunc(prevObserverFunc);
    GracefulRestartTablet(runtime, shards1[0], sender);

    // The result of commit should be SUCCESS
    {
        Cerr << "===== Waiting for commit response" << Endl;

        auto response = AwaitResponse(runtime, myCommit2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    evWriteObservers = TTestActorRuntimeBase::TEventObserverHolderPair{};

    // Select key 3 and verify its value was updated
    {
        Cerr << "===== Last SELECT" << Endl;

        auto result = KqpSimpleExec(runtime, Q_(R"(SELECT key, value FROM `/Root/table-1` WHERE key = 3 ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result,  "{ items { uint32_value: 3 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestShardRestartDuringWaitingRead) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app)
        // We read from an unresolved volatile tx
        .SetEnableDataShardVolatileTransactions(true);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20)"));

    // Block readset exchange
    std::vector<std::unique_ptr<IEventHandle>> readSets;
    auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
        readSets.emplace_back(ev.Release());
    });

    // Start a distributed write to both tables
    TString sessionId = CreateSessionRPC(runtime, "/Root");
    auto upsertResult = SendRequest(
        runtime,
        MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 30);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 40);
            )", sessionId, /* txId */ "", /* commitTx */ true),
        "/Root");
    WaitFor(runtime, [&]{ return readSets.size() >= 4; }, "readsets");

    // Start reading the first table
    TString readSessionId = CreateSessionRPC(runtime, "/Root");
    auto readResult = SendRequest(
        runtime,
        MakeSimpleRequestRPC(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key;
            )", readSessionId, /* txId */ "", /* commitTx */ true),
        "/Root");

    // Sleep to ensure read is properly waiting
    runtime.SimulateSleep(TDuration::Seconds(1));

    // Gracefully restart the first table shard
    blockReadSets.Remove();
    GracefulRestartTablet(runtime, table1shards[0], sender);

    // Read succeeds because it is automatically retried
    // No assert should be triggered in debug builds
    UNIT_ASSERT_VALUES_EQUAL(
        FormatResult(AwaitResponse(runtime, std::move(readResult))),
        "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 30 } }");
}

Y_UNIT_TEST(TestShardSnapshotReadNoEarlyReply) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NLog::PRI_TRACE);
    // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");
    auto isTableShard = [&](ui64 tabletId) -> bool {
        if (std::find(table1shards.begin(), table1shards.end(), tabletId) != table1shards.end() ||
            std::find(table2shards.begin(), table2shards.end(), tabletId) != table2shards.end())
        {
            return true;
        }
        return false;
    };

    SimulateSleep(server, TDuration::Seconds(1));

    auto waitFor = [&](const auto& condition, const TString& description) {
        for (int i = 0; i < 5; ++i) {
            if (condition()) {
                return;
            }
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
    };

    TVector<THolder<IEventHandle>> blockedCommits;
    size_t seenProposeResults = 0;
    auto blockCommits = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTablet::TEvCommit::EventType: {
                auto* msg = ev->Get<TEvTablet::TEvCommit>();
                if (isTableShard(msg->TabletID)) {
                    Cerr << "... blocked commit for tablet " << msg->TabletID << Endl;
                    blockedCommits.push_back(std::move(ev));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvDataShard::TEvProposeTransactionResult::EventType: {
                Cerr << "... observed propose transaction result" << Endl;
                ++seenProposeResults;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(blockCommits);

    TString sessionId1 = CreateSessionRPC(runtime, "/Root");
    TString sessionId2 = CreateSessionRPC(runtime, "/Root");

    TString sq1 = CreateSessionRPC(runtime, "/Root");
    TString sq2 = CreateSessionRPC(runtime, "/Root");

    auto f1 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        SELECT * FROM `/Root/table-1`
        UNION ALL
        SELECT * FROM `/Root/table-2`
    )"), sessionId1, "", false), "/Root");
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        SELECT * FROM `/Root/table-1`
        UNION ALL
        SELECT * FROM `/Root/table-2`
    )"), sessionId2, "", false), "/Root");

    waitFor([&]{ return blockedCommits.size() >= 2; }, "at least 2 blocked commits");

    SimulateSleep(server, TDuration::Seconds(1));

    UNIT_ASSERT_C(seenProposeResults == 0, "Unexpected propose results observed");

    // Unblock commits and wait for results
    runtime.SetObserverFunc(prevObserver);
    for (auto& ev : blockedCommits) {
        runtime.Send(ev.Release(), 0, true);
    }
    blockedCommits.clear();

    TString txId1;
    {
        auto response = AwaitResponse(runtime, f1);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        txId1 = result.tx_meta().id();
    }

    TString txId2;
    {
        auto response = AwaitResponse(runtime, f2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        txId2 = result.tx_meta().id();
    }

    // Perform a couple of immediate reads to make sure shards are ready to respond to read-only requests
    // This may be needed when leases are enabled, otherwise paused leases would block the snapshot read reply
    ExecSQL(server, sender, Q_("SELECT * FROM `/Root/table-1` WHERE key = 1"));
    ExecSQL(server, sender, Q_("SELECT * FROM `/Root/table-2` WHERE key = 2"));
    Cerr << "... shards are ready for read-only immediate transactions" << Endl;

    // Start blocking commits again and try performing new writes
    prevObserver = runtime.SetObserverFunc(blockCommits);
    SendRequest(runtime, MakeSimpleRequestRPC(Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"), sq1, "", true), "/Root");
    SendRequest(runtime, MakeSimpleRequestRPC(Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 4)"), sq2, "", true), "/Root");
    waitFor([&]{ return blockedCommits.size() >= 2; }, "at least 2 blocked commits");

    // Send an additional read request, it must not be blocked
    auto f = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        SELECT * FROM `/Root/table-1`
        UNION ALL
        SELECT * FROM `/Root/table-2`
    )"), sessionId1, txId1, false), "/Root");

    {
        auto response = AwaitResponse(runtime, f);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST_TWIN(TestSnapshotReadAfterBrokenLock, EvWrite) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    auto rows = EvWrite ? TEvWriteRows{{{1, 1}}, {{2, 2}}, {{3, 3}}, {{5, 5}}} : TEvWriteRows{};
    auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::Seconds(1));

    TString sessionId = CreateSessionRPC(runtime);

    // Start transaction by reading from both tables, we will only set locks
    // to currently existing variables
    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    SimulateSleep(server, TDuration::Seconds(1));

    // Perform immediate write, which would not break the above lock
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

    // Perform an additional read, it would mark transaction as write-broken
    {
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 3
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    // Perform one more read, it would be in an already write-broken transaction
    {
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 5
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    {
        auto result = KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5)
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ABORTED");
    }
}

Y_UNIT_TEST(TestSnapshotReadAfterBrokenLockOutOfOrder) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::Seconds(1));

    // Start transaction by reading from both tables
    TString sessionId = CreateSessionRPC(runtime);
    TString txId;
    {
        Cerr << "... performing the first select" << Endl;
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    // Arrange for another distributed tx stuck at readset exchange
    TString sessionIdBlocker = CreateSessionRPC(runtime);
    TString txIdBlocker;
    {
        auto result = KqpSimpleBegin(runtime, sessionIdBlocker, txIdBlocker, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                Cerr << "... captured readset" << Endl;
                readSets.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto thisCommit2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (99, 99);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (99, 99))"), sessionIdBlocker, txIdBlocker, true));

    // Wait until we captured both readsets
    waitFor([&]{ return readSets.size() >= 2; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), 2u);

    // Restore the observer, we would no longer block new readsets
    runtime.SetObserverFunc(prevObserverFunc);

    SimulateSleep(server, TDuration::Seconds(1));

    // Perform immediate write, which would break the above lock
    Cerr << "... performing an upsert" << Endl;
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 3)"));

    // Perform an additional read, it would mark transaction as write-broken for the first time
    {
        Cerr << "... performing the second select" << Endl;
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 3
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    // Perform one more read, it would be in an already write-broken transaction
    {
        Cerr << "... performing the third select" << Endl;
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 5
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    {
        Cerr << "... performing the last upsert and commit" << Endl;
        auto result = KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5)
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ABORTED");
    }
}

Y_UNIT_TEST(TestSnapshotReadAfterStuckRW) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::Seconds(1));

    // Arrange for a distributed tx stuck at readset exchange
    TString sessionIdBlocker = CreateSessionRPC(runtime);
    TString txIdBlocker;
    {
        auto result = KqpSimpleBegin(runtime, sessionIdBlocker, txIdBlocker, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                Cerr << "... captured readset" << Endl;
                readSets.push_back(THolder(ev.Release()));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto sCommit = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (99, 99);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (99, 99))"), sessionIdBlocker, txIdBlocker, true));

    // Wait until we captured both readsets
    waitFor([&]{ return readSets.size() >= 2; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), 2u);

    // Restore the observer, we would no longer block new readsets
    runtime.SetObserverFunc(prevObserverFunc);

    SimulateSleep(server, TDuration::Seconds(1));

    // Start a transaction by reading from both tables
    TString sessionId = CreateSessionRPC(runtime);
    TString txId;
    {
        Cerr << "... performing the first select" << Endl;
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestSnapshotReadPriority) {
    TPortManager pm;
    TServerSettings::TControls controls;
    // This test needs to make sure mediator time does not advance while
    // certain operations are running. Unfortunately, volatile planning
    // may happen every 1ms, and it's too hard to guarantee time stays
    // still for such a short time. We disable volatile planning to make
    // coordinator ticks are 100ms apart.
    controls.MutableCoordinatorControls()->SetVolatilePlanLeaseMs(0);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetControls(controls)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);

    InitRoot(server, sender);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    TString senderImmediateWrite = CreateSessionRPC(runtime);

    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::MilliSeconds(850));

    // Perform an immediate write
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

    auto execSimpleRequest = [&](const TString& query) -> TString {
        return KqpSimpleExec(runtime, query);
    };

    auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
        return KqpSimpleBegin(runtime, sessionId, txId, query);
    };

    auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
        return KqpSimpleContinue(runtime, sessionId, txId, query);
    };

    auto execSnapshotRequest = [&](const TString& query) -> TString {
        TString sessionId, txId;
        TString result = beginSnapshotRequest(sessionId, txId, query);
        CloseSession(runtime, sessionId);
        return result;
    };

    // Perform an immediate read, we should observe the write above
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Same when using a fresh snapshot read
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Spam schedules in the runtime to prevent mediator time jumping prematurely
    {
        Cerr << "!!! Setting up wakeup spam" << Endl;
        auto senderWakeupSpam = runtime.AllocateEdgeActor();
        for (int i = 1; i <= 10; ++i) {
            runtime.Schedule(new IEventHandle(senderWakeupSpam, senderWakeupSpam, new TEvents::TEvWakeup()), TDuration::MicroSeconds(i * 250));
        }
    }

    // Send an immediate write transaction, but don't wait for result
    auto fImmWrite = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5)
        )"), senderImmediateWrite, "", true));

    // We sleep for very little so datashard commits changes, but doesn't advance
    SimulateSleep(runtime, TDuration::MicroSeconds(1));

    // Perform an immediate read again, it should NOT observe the write above
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Same when using a fresh snapshot read
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Wait for the write to finish
    {
        auto response = AwaitResponse(runtime, fImmWrite);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Perform an immediate read again, it should observe the write above
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Same when using a fresh snapshot read
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Start a new write and sleep again
    fImmWrite = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (7, 7)
        )"), senderImmediateWrite, "", true));

    SimulateSleep(runtime, TDuration::MicroSeconds(1));

    // Verify this write is not observed yet
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Spam schedules in the runtime to prevent mediator time jumping prematurely
    {
        Cerr << "!!! Setting up wakeup spam" << Endl;
        auto senderWakeupSpam = runtime.AllocateEdgeActor();
        for (int i = 1; i <= 10; ++i) {
            runtime.Schedule(new IEventHandle(senderWakeupSpam, senderWakeupSpam, new TEvents::TEvWakeup()), TDuration::MicroSeconds(i * 250));
        }
    }

    // Reboot the tablet
    RebootTablet(runtime, table1shards.at(0), sender);

    // Verify the write above cannot be observed after restart as well
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Send one more write and sleep again
    auto senderImmediateWrite2 = CreateSessionRPC(runtime);
    auto fImmWrite2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (9, 9)
        )"), senderImmediateWrite2, "", true));

    SimulateSleep(runtime, TDuration::MicroSeconds(1));

    // Verify it is also hidden at the moment
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Wait for result of the second write
    {
        auto response = AwaitResponse(runtime, fImmWrite2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // We should finally observe both writes
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }");

    TString snapshotSessionId, snapshotTxId;
    UNIT_ASSERT_VALUES_EQUAL(
        beginSnapshotRequest(snapshotSessionId, snapshotTxId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }");

    // Spam schedules in the runtime to prevent mediator time jumping prematurely
    {
        Cerr << "!!! Setting up wakeup spam" << Endl;
        auto senderWakeupSpam = runtime.AllocateEdgeActor();
        for (int i = 1; i <= 10; ++i) {
            runtime.Schedule(new IEventHandle(senderWakeupSpam, senderWakeupSpam, new TEvents::TEvWakeup()), TDuration::MicroSeconds(i * 250));
        }
    }

    // Reboot the tablet
    RebootTablet(runtime, table1shards.at(0), sender);

    // Upsert new data after reboot
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (11, 11)"));

    // Make sure datashard state is restored correctly and snapshot is not corrupted
    UNIT_ASSERT_VALUES_EQUAL(
        continueSnapshotRequest(snapshotSessionId, snapshotTxId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }");

    // Make sure new snapshot will actually observe new data
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }, "
        "{ items { uint32_value: 11 } items { uint32_value: 11 } }");
}

Y_UNIT_TEST(TestUnprotectedReadsThenWriteVisibility) {
    TPortManager pm;
    TServerSettings::TControls controls;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetNodeCount(2)
        .SetControls(controls)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);

    InitRoot(server, sender);

    const ui64 hiveTabletId = ChangeStateStorage(Hive, server->GetSettings().Domain);

    struct TNodeState {
        // mediator -> bucket -> [observed, passed] step
        THashMap<ui64, THashMap<ui32, std::pair<ui64, ui64>>> Steps;
        ui64 AllowedStep = 0;
    };
    THashMap<ui32, TNodeState> mediatorState;

    bool mustWaitForSteps[2] = { false, false };

    auto captureTimecast = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
        const ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
        switch (ev->GetTypeRewrite()) {
            case TEvMediatorTimecast::TEvUpdate::EventType: {
                auto* msg = ev->Get<TEvMediatorTimecast::TEvUpdate>();
                const ui64 mediatorId = msg->Record.GetMediator();
                const ui32 bucket = msg->Record.GetBucket();
                ui64 step = msg->Record.GetTimeBarrier();
                auto& state = mediatorState[nodeId];
                if (!mustWaitForSteps[nodeIndex]) {
                    // Automatically allow all new steps
                    state.AllowedStep = Max(state.AllowedStep, step);
                }
                Cerr << "... node " << nodeId << " observed update from " << mediatorId
                    << " for bucket " << bucket
                    << " to step " << step
                    << " (allowed " << state.AllowedStep << ")"
                    << Endl;
                auto& [observedStep, passedStep] = state.Steps[mediatorId][bucket];
                observedStep = Max(observedStep, step);
                if (step >= passedStep) {
                    if (step < state.AllowedStep) {
                        step = state.AllowedStep;
                        msg->Record.SetTimeBarrier(step);
                        Cerr << "...     shifted to allowed step " << step << Endl;
                    }
                    passedStep = step;
                    break;
                }
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvMediatorTimecast::TEvWaitPlanStep::EventType: {
                const auto* msg = ev->Get<TEvMediatorTimecast::TEvWaitPlanStep>();
                const ui64 tabletId = msg->TabletId;
                const ui64 step = msg->PlanStep;
                Cerr << "... node " << nodeId << " observed wait by " << tabletId
                    << " for step " << step
                    << Endl;
                auto& state = mediatorState[nodeId];
                if (state.AllowedStep < step) {
                    state.AllowedStep = step;
                    for (auto& kv1 : state.Steps) {
                        const ui64 mediatorId = kv1.first;
                        for (auto& kv2 : kv1.second) {
                            const ui32 bucket = kv2.first;
                            auto& [observedStep, passedStep] = kv2.second;
                            if (passedStep < step && passedStep < observedStep) {
                                passedStep = Min(step, observedStep);
                                auto* update = new TEvMediatorTimecast::TEvUpdate();
                                update->Record.SetMediator(mediatorId);
                                update->Record.SetBucket(bucket);
                                update->Record.SetTimeBarrier(passedStep);
                                runtime.Send(new IEventHandle(ev->GetRecipientRewrite(), ev->GetRecipientRewrite(), update), nodeIndex, /* viaActorSystem */ true);
                            }
                        }
                    }
                }
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureTimecast);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    auto table1shards = GetTableShards(server, sender, "/Root/table-1");

    // Make sure tablet is at node 1
    runtime.SendToPipe(hiveTabletId, sender, new TEvHive::TEvFillNode(runtime.GetNodeId(0)));
    {
        auto ev = runtime.GrabEdgeEventRethrow<TEvHive::TEvFillNodeResult>(sender);
        UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

    SimulateSleep(server, TDuration::Seconds(1));

    auto execSimpleRequest = [&](const TString& query) -> TString {
        return KqpSimpleExec(runtime, query);
    };

    auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
        return KqpSimpleBegin(runtime, sessionId, txId, query);
    };

    auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
        return KqpSimpleContinue(runtime, sessionId, txId, query);
    };

    auto execSnapshotRequest = [&](const TString& query) -> TString {
        TString sessionId, txId;
        TString result = beginSnapshotRequest(sessionId, txId, query);
        CloseSession(runtime, sessionId);
        return result;
    };

    // Perform an immediate read, we should observe the initial write
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // Same when using a fresh snapshot read
    TString sessionId, txId;
    UNIT_ASSERT_VALUES_EQUAL(
        beginSnapshotRequest(sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // Stop updating mediator timecast on the second node
    mustWaitForSteps[1] = true;

    // Insert a new row and wait for result
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)"));

    // Make sure tablet is at node 2
    runtime.SendToPipe(hiveTabletId, sender, new TEvHive::TEvFillNode(runtime.GetNodeId(1)));
    {
        auto ev = runtime.GrabEdgeEventRethrow<TEvHive::TEvFillNodeResult>(sender);
        UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    // Perform an immediate read, we should observe confirmed writes after restart
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

    // Previous snapshot must see original data
    UNIT_ASSERT_VALUES_EQUAL(
        continueSnapshotRequest(sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // However new snapshots must see updated data
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
}

Y_UNIT_TEST(UncommittedReadSetAck) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetNodeCount(2)
        .SetUseRealThreads(false);


    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);

    InitRoot(server, sender);

    const ui64 hiveTabletId = ChangeStateStorage(Hive, server->GetSettings().Domain);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    // Make sure these tablets are at node 1
    runtime.SendToPipe(hiveTabletId, sender, new TEvHive::TEvFillNode(runtime.GetNodeId(0)));
    {
        auto ev = runtime.GrabEdgeEventRethrow<TEvHive::TEvFillNodeResult>(sender);
        UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    // Create one more table, we expect it to run at node 2
    CreateShardedTable(server, sender, "/Root", "table-3", 1);
    auto table3shards = GetTableShards(server, sender, "/Root/table-3");
    auto table3actor = ResolveTablet(runtime, table3shards.at(0), /* nodeIndex */ 1);
    UNIT_ASSERT_VALUES_EQUAL(table3actor.NodeId(), runtime.GetNodeId(1));

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-3` (key, value) VALUES (3, 3)"));

    auto beginTx = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
        return KqpSimpleBegin(runtime, sessionId, txId, query);
    };

    TString sessionId1, txId1;
    UNIT_ASSERT_VALUES_EQUAL(
        beginTx(sessionId1, txId1, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    TString sessionId2, txId2;
    UNIT_ASSERT_VALUES_EQUAL(
        beginTx(sessionId2, txId2, Q_(R"(
            SELECT key, value FROM `/Root/table-2`
            ORDER BY key
            )")),
        "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

    bool capturePlanSteps = true;
    TVector<THolder<IEventHandle>> capturedPlanSteps;
    TVector<ui64> capturedPlanTxIds;
    THashSet<ui64> passReadSetTxIds;
    ui64 observedReadSets = 0;
    TVector<THolder<IEventHandle>> capturedReadSets;
    ui64 observedReadSetAcks = 0;
    bool captureCommits = false;
    TVector<THolder<IEventHandle>> capturedCommits;

    auto captureCommitAfterReadSet = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
        const ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
        if (nodeIndex == 1) {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvPlanStep::EventType: {
                if (nodeIndex == 1 && ev->GetRecipientRewrite() == table3actor && capturePlanSteps) {
                    Cerr << "... captured plan step for table-3" << Endl;
                    auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                    for (const auto& tx : msg->Record.GetTransactions()) {
                        ui64 txId = tx.GetTxId();
                        capturedPlanTxIds.push_back(txId);
                        Cerr << "... captured plan step tx " << txId << " for table-3" << Endl;
                    }
                    capturedPlanSteps.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvTxProcessing::TEvReadSet::EventType: {
                if (nodeIndex == 1 && ev->GetRecipientRewrite() == table3actor) {
                    auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    ui64 txId = msg->Record.GetTxId();
                    if ((msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET) &&
                        (msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA))
                    {
                        Cerr << "... passing expectation for txid# " << txId << Endl;
                        break;
                    }
                    ++observedReadSets;
                    if (!passReadSetTxIds.contains(txId)) {
                        Cerr << "... readset for txid# " << txId << " was blocked" << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    Cerr << "... passing readset for txid# " << txId << Endl;
                }
                break;
            }
            case TEvTxProcessing::TEvReadSetAck::EventType: {
                Cerr << "... read set ack" << Endl;
                ++observedReadSetAcks;
                break;
            }
            case TEvBlobStorage::TEvPut::EventType: {
                auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                if (nodeIndex == 1 && msg->Id.TabletID() == table3shards.at(0) && captureCommits) {
                    Cerr << "... capturing put " << msg->Id << " for table-3" << Endl;
                    capturedCommits.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
        }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureCommitAfterReadSet);

    // Make two commits in parallel, one of them will receive a readset and become complete
    SendRequest(runtime, MakeSimpleRequestRPC(
        Q_(R"(
            UPSERT INTO `/Root/table-3` (key, value) VALUES (4, 4)
        )"), sessionId1, txId1, true));
    SendRequest(runtime, MakeSimpleRequestRPC(
        Q_(R"(
            UPSERT INTO `/Root/table-3` (key, value) VALUES (5, 5)
        )"), sessionId2, txId2, true));

    auto waitFor = [&](const auto& condition, const TString& description) {
        while (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
        }
    };

    waitFor([&]{ return capturedPlanTxIds.size() >= 2; }, "captured transactions");
    UNIT_ASSERT_C(capturedPlanTxIds.size(), 2u);
    std::sort(capturedPlanTxIds.begin(), capturedPlanTxIds.end());
    ui64 realTxId1 = capturedPlanTxIds.at(0);
    ui64 realTxId2 = capturedPlanTxIds.at(1);

    // Unblock and resend the plan step message
    capturePlanSteps = false;
    for (auto& ev : capturedPlanSteps) {
        runtime.Send(ev.Release(), 1, true);
    }
    capturedPlanSteps.clear();

    // Wait until there are 2 readset messages (with data)
    waitFor([&]{ return capturedReadSets.size() >= 2; }, "initial readsets");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // Unblock readset messages for txId1, but block commits
    captureCommits = true;
    observedReadSetAcks = 0;
    passReadSetTxIds.insert(realTxId1);
    for (auto& ev : capturedReadSets) {
        runtime.Send(ev.Release(), 1, true);
    }
    capturedReadSets.clear();

    // Wait until transaction is complete and tries to commit
    waitFor([&]{ return capturedCommits.size() > 0 && capturedReadSets.size() > 0; }, "tx complete");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // Reboot tablets and wait for resent readsets
    // Since tx is already complete, it will be added to DelayedAcks
    observedReadSets = 0;
    capturedReadSets.clear();
    RebootTablet(runtime, table1shards.at(0), sender, 0, true);
    RebootTablet(runtime, table2shards.at(0), sender, 0, true);

    waitFor([&]{ return observedReadSets >= 2; }, "resent readsets");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // Now we unblock the second readset and resend it
    passReadSetTxIds.insert(realTxId2);
    for (auto& ev : capturedReadSets) {
        runtime.Send(ev.Release(), 1, true);
    }
    capturedReadSets.clear();
    observedReadSets = 0;

    // Wait until the second transaction commits
    ui64 prevCommitsBlocked = capturedCommits.size();
    waitFor([&]{ return capturedCommits.size() > prevCommitsBlocked && observedReadSets >= 1; }, "second tx complete");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // There must be no readset acks, since we're blocking all commits
    UNIT_ASSERT_VALUES_EQUAL(observedReadSetAcks, 0);

    // Now we stop blocking anything and "reply" to all blocked commits with an error
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : capturedCommits) {
        auto proxy = ev->Recipient;
        ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
        auto res = ev->Get<TEvBlobStorage::TEvPut>()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
        runtime.Send(new IEventHandle(ev->Sender, proxy, res.release()), 1, true);
    }
    capturedCommits.clear();

    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // This should succeed, unless the bug was triggered and readset acknowledged before commit
    ExecSQL(server, sender, Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 42);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 42);
        UPSERT INTO `/Root/table-3` (key, value) VALUES (4, 42), (5, 42);
    )"));
}

Y_UNIT_TEST(UncommittedReads) {
    TPortManager pm;
    TServerSettings::TControls controls;

    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetControls(controls)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);

    InitRoot(server, sender);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    auto shards1 = GetTableShards(server, sender, "/Root/table-1");

    auto isTableShard = [&](ui64 tabletId) -> bool {
        return std::find(shards1.begin(), shards1.end(), tabletId) != shards1.end();
    };

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

    SimulateSleep(server, TDuration::Seconds(1));

    // Read the upserted row and also prime shard for unprotected reads
    TString sessionId, txId;
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // Make sure we are at the max immediate write edge for current step and it's confirmed
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

    TString upsertSender = CreateSessionRPC(runtime);
    TString readSender = CreateSessionRPC(runtime);

    // Block commits and start counting propose responses
    TVector<THolder<IEventHandle>> blockedCommits;
    size_t seenProposeResults = 0;
    auto blockCommits = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTablet::TEvCommit::EventType: {
                auto* msg = ev->Get<TEvTablet::TEvCommit>();
                if (isTableShard(msg->TabletID)) {
                    Cerr << "... blocked commit for tablet " << msg->TabletID << Endl;
                    blockedCommits.push_back(std::move(ev));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvDataShard::TEvProposeTransactionResult::EventType: {
                Cerr << "... observed propose transaction result" << Endl;
                ++seenProposeResults;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(blockCommits);

    auto waitFor = [&](const auto& condition, const TString& description) {
        while (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
        }
    };

    // Start upserting a row with blocked commits, it will stick to the same version as the last upsert
    auto fUpsert = SendRequest(
        runtime,
        MakeSimpleRequestRPC(Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 4)"), upsertSender, "", true));

    waitFor([&]{ return blockedCommits.size() > 0; }, "blocked commit");

    // Start reading data, we know it must read confirmed data, but it will also include the blocked row above
    auto fRead = SendRequest(
        runtime,
        MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` ORDER BY key"), readSender, "", true));

    // Sleep for 1 second
    SimulateSleep(runtime, TDuration::Seconds(1));

    // We are blocking commits, so read must not see a 4th row until we unblock
    if (seenProposeResults > 0) {
        // We might make it possible in the future to run reads like that without blocking
        // However, it still means we must not return the 4th row that is not committed
        auto response = AwaitResponse(runtime, fRead);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(result),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");
        return;
    }

    // Unblock all commits
    runtime.SetObserverFunc(prevObserver);
    for (auto& ev : blockedCommits) {
        runtime.Send(ev.Release(), 0, true);
    }
    blockedCommits.clear();

    // We must successfully upsert the row
    {
        auto response = AwaitResponse(runtime, fUpsert);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // We must successfully read including the 4th row
    {
        auto response = AwaitResponse(runtime, fRead);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(result),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 4 } }");
    }
}

} // Y_UNIT_TEST_SUITE(DataShardOutOfOrder)

} // namespace NKikimr
