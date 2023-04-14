#include "tablet_flat_executed.h"
#include "flat_executor.h"

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_executor_compaction_logic.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/test/libs/exec/runner.h>
#include <ydb/core/tablet_flat/ut/flat_database_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TFlatTableDecimals) {
    class TDecimalTestFlatTablet : public TActor<TDecimalTestFlatTablet>, public TTabletExecutedFlat {
        struct Schema : NIceDb::Schema {
            struct DecimalValue : Table<1> {
                struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
                struct Value : Column<2, NScheme::NTypeIds::Decimal> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };

            struct DecimalKey : Table<2> {
                struct Key : Column<1, NScheme::NTypeIds::Decimal> {};
                struct Value : Column<2, NScheme::NTypeIds::Decimal> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };

            struct DecimalCompoundKey : Table<3> {
                struct Key1 : Column<1, NScheme::NTypeIds::Decimal> {};
                struct Key2 : Column<2, NScheme::NTypeIds::Int32> {};
                struct Key3 : Column<3, NScheme::NTypeIds::Decimal> {};
                struct Value : Column<4, NScheme::NTypeIds::Decimal> {};

                using TKey = TableKey<Key1, Key2, Key3>;
                using TColumns = TableColumns<Key1, Key2, Key3, Value>;
            };

            using TTables = SchemaTables<DecimalValue, DecimalKey, DecimalCompoundKey>;
            using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                             ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
        };

        template <typename T>
        static bool IsReady(T &t)
        {
            return t.IsReady();
        }

        template <typename T, typename ...Ts>
        static bool IsReady(T &t, Ts &...args)
        {
            return t.IsReady() && IsReady(args...);
        }

        struct TTxSchema : public ITransaction {
            TDecimalTestFlatTablet &Self;

            TTxSchema(TDecimalTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                Self.Execute(new TTxStore(Self), ctx);
            }
        };

        struct TTxStore : public ITransaction {
            TDecimalTestFlatTablet &Self;

            TTxStore(TDecimalTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb db(txc.DB);

                // DecimalValue
                db.Table<Schema::DecimalValue>().Key(1)
                    .Update<Schema::DecimalValue::Value>(std::pair<ui64, i64>(1, 0));
                db.Table<Schema::DecimalValue>().Key(2)
                    .Update<Schema::DecimalValue::Value>(std::pair<ui64, i64>(0, 2));
                db.Table<Schema::DecimalValue>().Key(3)
                    .Update<Schema::DecimalValue::Value>(std::pair<ui64, i64>(0xffffffffffffffff, 3));
                db.Table<Schema::DecimalValue>().Key(4)
                    .Update<Schema::DecimalValue::Value>(std::pair<ui64, i64>(4, 0xffffffffffffffff));
                db.Table<Schema::DecimalValue>().Key(5)
                    .Update<Schema::DecimalValue::Value>(std::pair<ui64, i64>(0xffffffffffffffff, 0xffffffffffffffff));

                // DecimalKey
                for (int i = 1; i <= 10; ++i) {
                    db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(i, 0))
                        .Update<Schema::DecimalKey::Value>(std::pair<ui64, i64>(i, 0));
                    db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(0, i))
                        .Update<Schema::DecimalKey::Value>(std::pair<ui64, i64>(i, i));
                    db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(i, 0xffffffffffffffff))
                        .Update<Schema::DecimalKey::Value>(std::pair<ui64, i64>(i, 0x1234567890123456));
                    db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(i, 0xffffffffffffff00))
                        .Update<Schema::DecimalKey::Value>(std::pair<ui64, i64>(i + i, 0x6543210987654321));
                }

                // DecimalCompoundKey
                for (int i = 1; i <= 10; ++i) {
                    db.Table<Schema::DecimalCompoundKey>().Key(std::pair<ui64, i64>(i, 0), i, std::pair<ui64, i64>(0, i))
                        .Update<Schema::DecimalCompoundKey::Value>(std::pair<ui64, i64>(i, 0));
                    db.Table<Schema::DecimalCompoundKey>().Key(std::pair<ui64, i64>(i, 0xffffffffffffffff), i, std::pair<ui64, i64>(0, i))
                        .Update<Schema::DecimalCompoundKey::Value>(std::pair<ui64, i64>(i, 0xffffffffffffffff));
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                Self.Execute(new TTxSelectSingle(Self), ctx);
            }
        };

        struct TTxSelectSingle : public ITransaction {
            TDecimalTestFlatTablet &Self;

            TTxSelectSingle(TDecimalTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb db(txc.DB);

                // DecimalValue
                {
                    auto row1 = db.Table<Schema::DecimalValue>().Key(1).Select<Schema::DecimalValue::Value>();
                    auto row2 = db.Table<Schema::DecimalValue>().Key(2).Select<Schema::DecimalValue::Value>();
                    auto row3 = db.Table<Schema::DecimalValue>().Key(3).Select<Schema::DecimalValue::Value>();
                    auto row4 = db.Table<Schema::DecimalValue>().Key(4).Select<Schema::DecimalValue::Value>();
                    auto row5 = db.Table<Schema::DecimalValue>().Key(5).Select<Schema::DecimalValue::Value>();

                    if (!IsReady(row1, row2, row3, row4, row5))
                        return false;

                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DecimalValue::Value>(), (std::pair<ui64, i64>(1, 0)));
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DecimalValue::Value>(), (std::pair<ui64, i64>(0, 2)));
                    UNIT_ASSERT_VALUES_EQUAL(row3.GetValue<Schema::DecimalValue::Value>(), (std::pair<ui64, i64>(0xffffffffffffffff, 3)));
                    UNIT_ASSERT_VALUES_EQUAL(row4.GetValue<Schema::DecimalValue::Value>(), (std::pair<ui64, i64>(4, 0xffffffffffffffff)));
                    UNIT_ASSERT_VALUES_EQUAL(row5.GetValue<Schema::DecimalValue::Value>(), (std::pair<ui64, i64>(0xffffffffffffffff, 0xffffffffffffffff)));
                }

                // DecimalKey
                for (int i = 1; i <= 10; ++i) {
                    auto row1 = db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(i, 0)).Select<Schema::DecimalKey::Value>();
                    auto row2 = db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(0, i)).Select<Schema::DecimalKey::Value>();
                    auto row3 = db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(i, 0xffffffffffffffff)).Select<Schema::DecimalKey::Value>();
                    auto row4 = db.Table<Schema::DecimalKey>().Key(std::pair<ui64, i64>(i, 0xffffffffffffff00)).Select<Schema::DecimalKey::Value>();

                    if (!IsReady(row1, row2, row3, row4))
                        return false;

                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, i)));
                    UNIT_ASSERT_VALUES_EQUAL(row3.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0x1234567890123456)));
                    UNIT_ASSERT_VALUES_EQUAL(row4.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i + i, 0x6543210987654321)));
                }

                // DecimalCompoundKey
                for (int i = 1; i <= 10; ++i) {
                    auto row1 = db.Table<Schema::DecimalCompoundKey>().Key(std::pair<ui64, i64>(i, 0), i, std::pair<ui64, i64>(0, i)).Select<Schema::DecimalCompoundKey::Value>();
                    auto row2 = db.Table<Schema::DecimalCompoundKey>().Key(std::pair<ui64, i64>(i, 0xffffffffffffffff), i, std::pair<ui64, i64>(0, i)).Select<Schema::DecimalCompoundKey::Value>();

                    if (!IsReady(row1, row2))
                        return false;

                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DecimalCompoundKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DecimalCompoundKey::Value>(), (std::pair<ui64, i64>(i, 0xffffffffffffffff)));
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                Self.Execute(new TTxSelectRange(Self), ctx);
            }
        };

        struct TTxSelectRange : public ITransaction {
            TDecimalTestFlatTablet &Self;

            TTxSelectRange(TDecimalTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb db(txc.DB);

                // DecimalKey
                {
                    auto rowset = db.Table<Schema::DecimalKey>().GreaterOrEqual(std::pair<ui64, i64>(1, 0)).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(0, i)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, i)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DecimalKey
                {
                    auto rowset = db.Table<Schema::DecimalKey>().GreaterOrEqual(std::pair<ui64, i64>(11, 0)).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(0, i)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, i)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DecimalKey
                {
                    auto rowset = db.Table<Schema::DecimalKey>().GreaterOrEqual(std::pair<ui64, i64>(6, 0xffffffffffffff00)).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (int i = 6; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0xffffffffffffff00)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i + i, 0x6543210987654321)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0xffffffffffffffff)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0x1234567890123456)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(0, i)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, i)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DecimalKey
                {
                    auto rowset = db.Table<Schema::DecimalKey>().LessOrEqual(std::pair<ui64, i64>(6, 0xffffffffffffff00)).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (int i = 1; i <= 6; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0xffffffffffffff00)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i + i, 0x6543210987654321)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DecimalKey
                {
                    auto rowset = db.Table<Schema::DecimalKey>().LessOrEqual(std::pair<ui64, i64>(3, 0)).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0xffffffffffffff00)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i + i, 0x6543210987654321)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0xffffffffffffffff)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0x1234567890123456)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 3; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), (std::pair<ui64, i64>(i, 0)));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DecimalCompoundKey
                {
                    auto rowset = db.Table<Schema::DecimalCompoundKey>().GreaterOrEqual(std::pair<ui64, i64>(3, 0), 3, std::pair<ui64, i64>(0, 3)).Select();
                    if (!rowset.IsReady())
                        return false;

                    for (int i = 3; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_EQUAL(rowset.GetKey(), (std::make_tuple(std::pair<ui64, i64>(i, 0), i, std::pair<ui64, i64>(0, i))));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalCompoundKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DecimalCompoundKey
                {
                    auto rowset = db.Table<Schema::DecimalCompoundKey>().LessOrEqual(std::pair<ui64, i64>(3, 0), 3, std::pair<ui64, i64>(0, 3)).Select();
                    if (!rowset.IsReady())
                        return false;

                    for (int i = 1; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_EQUAL(rowset.GetKey(), (std::make_tuple(std::pair<ui64, i64>(i, 0xffffffffffffffff), i, std::pair<ui64, i64>(0, i))));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalCompoundKey::Value>(), (std::pair<ui64, i64>(i, 0xffffffffffffffff)));
                        if (!rowset.Next())
                            return false;
                    }
                    for (int i = 1; i <= 3; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_EQUAL(rowset.GetKey(), (std::make_tuple(std::pair<ui64, i64>(i, 0), i, std::pair<ui64, i64>(0, i))));
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DecimalCompoundKey::Value>(), (std::pair<ui64, i64>(i, 0)));
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                ctx.Send(ctx.SelfID, new TEvents::TEvWakeup);
            }
        };

        void OnActivateExecutor(const TActorContext &ctx) override {
            Become(&TThis::StateWork);
            Execute(new TTxSchema(*this), ctx);
        }

        void OnDetach(const TActorContext &ctx) override {
            Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override {
            Die(ctx);
        }

        void Handle(TEvents::TEvWakeup::TPtr &, const TActorContext &ctx) {
            ctx.Send(Sender, new TEvents::TEvWakeup);
        }

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvWakeup, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
                break;
            }
        }

    public:
        TDecimalTestFlatTablet(const TActorId &sender, const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
            , Sender(sender)
        {
        }

    private:
        TActorId Sender;
    };

    struct TDecimalEnvProfiles : public NFake::TRunner {
        TDecimalEnvProfiles()
            : Edge(Env.AllocateEdgeActor())
        {
        }

        void Run()
        {
            FireTablet(Edge, Tablet, [this](const TActorId &tablet, TTabletStorageInfo *info) {
                return new TDecimalTestFlatTablet(Edge, tablet, info);
            });

            TAutoPtr<IEventHandle> handle;
            Env.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
        }

        const ui32 Tablet = MakeTabletID(0, 0, 1);
        const TActorId Edge;
    };

    Y_UNIT_TEST(TestDecimal) {
        TDecimalEnvProfiles env;
        env.Run();
    };
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr
