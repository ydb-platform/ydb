#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/test/libs/exec/dummy.h>
#include <ydb/core/tablet_flat/test/libs/exec/runner.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

struct Schema1 : NIceDb::Schema {
    struct RegularTable1 : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Uint32> {};
        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    struct TableToRename : Table<2> {
        struct KeyToRename : Column<1, NScheme::NTypeIds::Uint32> {};
        struct ValueToRename : Column<2, NScheme::NTypeIds::Uint32> {};
        using TKey = TableKey<KeyToRename>;
        using TColumns = TableColumns<KeyToRename, ValueToRename>;
    };

    struct RegularTable2 : Table<3> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Uint32> {};
        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    using TTables = SchemaTables<RegularTable1, TableToRename, RegularTable2>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                        ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

struct Schema2 : NIceDb::Schema {
    struct RegularTable1 : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Uint32> {};
        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    struct TableRenamed : Table<2> {
        struct KeyRenamed : Column<1, NScheme::NTypeIds::Uint32> {};
        struct ValueRenamed : Column<2, NScheme::NTypeIds::Uint32> {};
        using TKey = TableKey<KeyRenamed>;
        using TColumns = TableColumns<KeyRenamed, ValueRenamed>;
    };

    struct RegularTable2 : Table<3> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Uint32> {};
        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    using TTables = SchemaTables<RegularTable1, TableRenamed, RegularTable2>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                        ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

template<typename TSchema>
struct TTxInitSchema : public ITransaction {
    const TActorId Owner;

    TTxInitSchema(TActorId owner) : Owner(owner) { }

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb(txc.DB).Materialize<TSchema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

using TTxInitSchema1 = TTxInitSchema<Schema1>;
using TTxInitSchema2 = TTxInitSchema<Schema2>;

struct TTxStore : public ITransaction {
    const TActorId Owner;

    TTxStore(TActorId owner) : Owner(owner) { }

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        db.Table<Schema1::RegularTable1>().Key(1)
            .Update<Schema1::RegularTable1::Value>(10);
        db.Table<Schema1::RegularTable2>().Key(2)
            .Update<Schema1::RegularTable2::Value>(20);
        db.Table<Schema1::TableToRename>().Key(0xdead)
            .Update<Schema1::TableToRename::ValueToRename>(0xbeaf);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        ctx.Send(Owner, new NFake::TEvResult);
    }
};

struct TTxSelect1: public ITransaction {
    const TActorId Owner;

    TTxSelect1(TActorId owner) : Owner(owner) { }

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        auto row1 = db.Table<Schema1::RegularTable1>().Key(1).Select();
        auto row2 = db.Table<Schema1::RegularTable2>().Key(2).Select();

        auto row = db.Table<Schema1::TableToRename>().Key(0xdead).Select();

        if (!row1.IsReady() || !row2.IsReady() || !row.IsReady())
            return false;

        UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema1::RegularTable1::Value>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema1::RegularTable2::Value>(), 20);

        // the main purpose of this test
        UNIT_ASSERT_VALUES_EQUAL(row.GetValue<Schema1::TableToRename::ValueToRename>(), 0xbeaf);

        return true;
    };

    void Complete(const TActorContext &ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

struct TTxSelect2: public ITransaction {
    const TActorId Owner;

    TTxSelect2(TActorId owner) : Owner(owner) { }

    bool Execute(TTransactionContext &txc, const TActorContext &) override {
        NIceDb::TNiceDb db(txc.DB);

        auto row1 = db.Table<Schema2::RegularTable1>().Key(1).Select();
        auto row2 = db.Table<Schema2::RegularTable2>().Key(2).Select();

        auto row = db.Table<Schema2::TableRenamed>().Key(0xdead).Select();

        if (!row1.IsReady() || !row2.IsReady() || !row.IsReady())
            return false;

        UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema2::RegularTable1::Value>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema2::RegularTable2::Value>(), 20);

        // the main purpose of this test
        UNIT_ASSERT_VALUES_EQUAL(row.GetValue<Schema2::TableRenamed::ValueRenamed>(), 0xbeaf);

        return true;
    };

    void Complete(const TActorContext &ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

struct TEnv : public NFake::TRunner {
    TEnv()
        : Edge(Env.AllocateEdgeActor())
    {
        FireDummyTablet();

        SendSync(new NFake::TEvExecute{ new TTxInitSchema1(Edge) });

        SendAsync(new NFake::TEvExecute{ new TTxStore(Edge) });
        WaitFor<NFake::TEvResult>();

        SendSync(new NFake::TEvExecute{ new TTxSelect1(Edge) }, /* retry = */ true);
    }

    void FireDummyTablet()
    {
        FireTablet(Edge, Tablet, [this](const TActorId &tablet, TTabletStorageInfo *info) {
            return new NFake::TDummy(tablet, info, Edge);
        });

        WaitFor<NFake::TEvReady>();
    }

    void SendSync(IEventBase *event, bool retry = false, bool gone = false)
    {
        const auto wretry = PipeCfgRetries();
        const auto basic = NTabletPipe::TClientConfig();

        Env.SendToPipe(Tablet, Edge, event, 0, retry ? wretry : basic);

        gone ? WaitForGone() : WaitForWakeUp();
    }

    void SendAsync(IEventBase *event)
    {
        Env.SendToPipe(Tablet, Edge, event);
    }

    void RestartTablet()
    {
        SendSync(new TEvents::TEvPoison, false, true);
        FireDummyTablet();
    }

    void WaitForWakeUp() { WaitFor<TEvents::TEvWakeup>(); }
    void WaitForGone() { WaitFor<TEvents::TEvGone>(); }

    template<typename TEv>
    typename TEv::TPtr GrabEdgeEvent()
    {
        return Env.GrabEdgeEventRethrow<TEv>(Edge);
    }

    template<typename TEv>
    void WaitFor(size_t num = 1)
    {
        for (; num > 0; num--) {
            TAutoPtr<IEventHandle> handle;
            Env.GrabEdgeEventRethrow<TEv>(handle);
        }
    }

    static NTabletPipe::TClientConfig PipeCfgRetries()
    {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        return pipeConfig;
    }

    const ui64 Tablet = MakeTabletID(false, 1) & 0xFFFF'FFFF;
    const TActorId Edge;
};

Y_UNIT_TEST_SUITE(TFlatTableRenameTableAndColumn) {

    Y_UNIT_TEST(TestSchema1ToSchema2NoRestart) {
        TEnv env;

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema2(env.Edge) });

        env.SendSync(new NFake::TEvExecute{ new TTxSelect2(env.Edge) }, /* retry = */ true);
    };

    Y_UNIT_TEST(TestSchema1ToSchema2) {
        TEnv env;

        env.RestartTablet();

        // Check initial data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect1(env.Edge) }, /* retry = */ true);

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema2(env.Edge) });

        env.SendSync(new NFake::TEvExecute{ new TTxSelect2(env.Edge) }, /* retry = */ true);

        env.RestartTablet();

        // Check data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect2(env.Edge) }, /* retry = */ true);
    }

    Y_UNIT_TEST(TestSchema1ToSchema2ToSchema1) {
        TEnv env;

        env.RestartTablet();

        // Update to Schema2
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema2(env.Edge) }, /* retry = */ true);

        env.RestartTablet();

        // Update back to Schema1
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema1(env.Edge) }, /* retry = */ true);

        // Check data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect1(env.Edge) }, /* retry = */ true);

        env.RestartTablet();

        // Check data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect1(env.Edge) }, /* retry = */ true);
    };

    Y_UNIT_TEST(TestSchema1ToSchema2ToSchema1ToSchema2) {
        TEnv env;

        env.RestartTablet();

        // Update to Schema2
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema2(env.Edge) }, /* retry = */ true);

        env.RestartTablet();

        // Update back to Schema1
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema1(env.Edge) }, /* retry = */ true);

        // Check data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect1(env.Edge) }, /* retry = */ true);

        env.RestartTablet();

        // Check data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect1(env.Edge) }, /* retry = */ true);

        // Update to Schema2
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema2(env.Edge) }, /* retry = */ true);
        env.SendSync(new NFake::TEvExecute{ new TTxSelect2(env.Edge) }, /* retry = */ true);

        env.RestartTablet();

        // finally check data
        env.SendSync(new NFake::TEvExecute{ new TTxSelect2(env.Edge) }, /* retry = */ true);
    };
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr

