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

Y_UNIT_TEST_SUITE(TFlatTableDatetime) {
    class TDateTestFlatTablet : public TActor<TDateTestFlatTablet>, public TTabletExecutedFlat {
        struct Schema : NIceDb::Schema {
            struct DateValue : Table<1> {
                struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
                struct Date : Column<2, NScheme::NTypeIds::Date> {};
                struct Datetime : Column<3, NScheme::NTypeIds::Datetime> {};
                struct Timestamp : Column<4, NScheme::NTypeIds::Timestamp> {};
                struct Interval : Column<5, NScheme::NTypeIds::Interval> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Date, Datetime, Timestamp, Interval>;
            };

            struct DateKey : Table<2> {
                struct Key : Column<1, NScheme::NTypeIds::Date> {};
                struct Value : Column<2, NScheme::NTypeIds::Uint32> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };

            struct DatetimeKey : Table<3> {
                struct Key : Column<1, NScheme::NTypeIds::Datetime> {};
                struct Value : Column<2, NScheme::NTypeIds::Uint32> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };

            struct TimestampKey : Table<4> {
                struct Key : Column<1, NScheme::NTypeIds::Timestamp> {};
                struct Value : Column<2, NScheme::NTypeIds::Uint32> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };

            struct IntervalKey : Table<5> {
                struct Key : Column<1, NScheme::NTypeIds::Interval> {};
                struct Value : Column<2, NScheme::NTypeIds::Uint32> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };


            using TTables = SchemaTables<DateValue, DateKey, DatetimeKey, TimestampKey, IntervalKey>;
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
            TDateTestFlatTablet &Self;

            TTxSchema(TDateTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                Self.Execute(new TTxStore(Self), ctx);
            }
        };

        struct TTxStore : public ITransaction {
            TDateTestFlatTablet &Self;

            TTxStore(TDateTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb db(txc.DB);

                // DateValue
                db.Table<Schema::DateValue>().Key(1)
                    .Update<Schema::DateValue::Date>(Min<ui16>())
                    .Update<Schema::DateValue::Datetime>(Min<ui32>())
                    .Update<Schema::DateValue::Timestamp>(Min<ui64>())
                    .Update<Schema::DateValue::Interval>(Min<i64>());
                db.Table<Schema::DateValue>().Key(2)
                    .Update<Schema::DateValue::Date>((ui16)100)
                    .Update<Schema::DateValue::Datetime>((ui32)100)
                    .Update<Schema::DateValue::Timestamp>((ui64)100)
                    .Update<Schema::DateValue::Interval>((i64)100);
                db.Table<Schema::DateValue>().Key(3)
                    .Update<Schema::DateValue::Date>(Max<ui16>())
                    .Update<Schema::DateValue::Datetime>(Max<ui32>())
                    .Update<Schema::DateValue::Timestamp>(Max<ui64>())
                    .Update<Schema::DateValue::Interval>(Max<i64>());

                // DateKey
                for (ui16 i = 1; i <= 10; ++i) {
                    db.Table<Schema::DateKey>().Key(i)
                        .Update<Schema::DateKey::Value>(i);
                    db.Table<Schema::DatetimeKey>().Key((ui32)i)
                        .Update<Schema::DateKey::Value>(i);
                    db.Table<Schema::TimestampKey>().Key((ui64)i)
                        .Update<Schema::DateKey::Value>(i);
                    db.Table<Schema::IntervalKey>().Key((i64)i - 5)
                        .Update<Schema::DateKey::Value>(i);
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                Self.Execute(new TTxSelectSingle(Self), ctx);
            }
        };

        struct TTxSelectSingle : public ITransaction {
            TDateTestFlatTablet &Self;

            TTxSelectSingle(TDateTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb db(txc.DB);

                // DateValue
                {
                    auto row1 = db.Table<Schema::DateValue>().Key(1).Select();
                    auto row2 = db.Table<Schema::DateValue>().Key(2).Select();
                    auto row3 = db.Table<Schema::DateValue>().Key(3).Select();

                    if (!IsReady(row1, row2, row3))
                        return false;

                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DateValue::Date>(), Min<ui16>());
                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DateValue::Datetime>(), Min<ui32>());
                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DateValue::Timestamp>(), Min<ui64>());
                    UNIT_ASSERT_VALUES_EQUAL(row1.GetValue<Schema::DateValue::Interval>(), Min<i64>());
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DateValue::Date>(), (ui16)100);
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DateValue::Datetime>(), (ui32)100);
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DateValue::Timestamp>(), (ui64)100);
                    UNIT_ASSERT_VALUES_EQUAL(row2.GetValue<Schema::DateValue::Interval>(), (i64)100);
                    UNIT_ASSERT_VALUES_EQUAL(row3.GetValue<Schema::DateValue::Date>(), Max<ui16>());
                    UNIT_ASSERT_VALUES_EQUAL(row3.GetValue<Schema::DateValue::Datetime>(), Max<ui32>());
                    UNIT_ASSERT_VALUES_EQUAL(row3.GetValue<Schema::DateValue::Timestamp>(), Max<ui64>());
                    UNIT_ASSERT_VALUES_EQUAL(row3.GetValue<Schema::DateValue::Interval>(), Max<i64>());
                }

                // DateKey
                for (ui16 i = 1; i <= 10; ++i) {
                    auto row = db.Table<Schema::DateKey>().Key(i).Select();
                    if (!IsReady(row))
                        return false;
                    UNIT_ASSERT_VALUES_EQUAL(row.GetValue<Schema::DateKey::Value>(), (ui16)i);
                }

                // DatetimeKey
                for (ui32 i = 1; i <= 10; ++i) {
                    auto row = db.Table<Schema::DatetimeKey>().Key(i).Select();
                    if (!IsReady(row))
                        return false;
                    UNIT_ASSERT_VALUES_EQUAL(row.GetValue<Schema::DatetimeKey::Value>(), i);
                }

                // TimestampKey
                for (ui64 i = 1; i <= 10; ++i) {
                    auto row = db.Table<Schema::TimestampKey>().Key(i).Select();
                    if (!IsReady(row))
                        return false;
                    UNIT_ASSERT_VALUES_EQUAL(row.GetValue<Schema::TimestampKey::Value>(), i);
                }

                // IntervalKey
                for (i16 i = 1; i <= 10; ++i) {
                    auto row = db.Table<Schema::IntervalKey>().Key(i - 5).Select();
                    if (!IsReady(row))
                        return false;
                    UNIT_ASSERT_VALUES_EQUAL(row.GetValue<Schema::IntervalKey::Value>(), i);
                }

                return true;
            }

            void Complete(const TActorContext &ctx) override {
                Self.Execute(new TTxSelectRange(Self), ctx);
            }
        };

        struct TTxSelectRange : public ITransaction {
            TDateTestFlatTablet &Self;

            TTxSelectRange(TDateTestFlatTablet &self) : Self(self) {}

            bool Execute(TTransactionContext &txc, const TActorContext &) override {
                NIceDb::TNiceDb db(txc.DB);

                // DateKey
                {
                    auto rowset = db.Table<Schema::DateKey>().GreaterOrEqual(5).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (ui16 i = 5; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), i);
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DateKey::Value>(), i);
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // DatetimeKey
                {
                    auto rowset = db.Table<Schema::DatetimeKey>().GreaterOrEqual(5).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (ui32 i = 5; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), i);
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::DatetimeKey::Value>(), i);
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // TimestampKey
                {
                    auto rowset = db.Table<Schema::TimestampKey>().GreaterOrEqual(5).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (ui64 i = 5; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), i);
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::TimestampKey::Value>(), i);
                        if (!rowset.Next())
                            return false;
                    }
                    UNIT_ASSERT(rowset.EndOfSet());
                }

                // IntervalKey
                {
                    auto rowset = db.Table<Schema::IntervalKey>().GreaterOrEqual(-2).Select();
                    if (!rowset.IsReady())
                        return false;
                    for (i64 i = 3; i <= 10; ++i) {
                        UNIT_ASSERT(!rowset.EndOfSet());
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetKey(), i - 5);
                        UNIT_ASSERT_VALUES_EQUAL(rowset.GetValue<Schema::IntervalKey::Value>(), i);
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

        void DefaultSignalTabletActive(const TActorContext &) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext &ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
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
        TDateTestFlatTablet(const TActorId &sender, const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
            , Sender(sender)
        {
        }

    private:
        TActorId Sender;
    };

    struct TDateEnvProfiles : public NFake::TRunner {
        TDateEnvProfiles()
            : Edge(Env.AllocateEdgeActor())
        {
        }

        void Run()
        {
            FireTablet(Edge, Tablet, [this](const TActorId &tablet, TTabletStorageInfo *info) {
                return new TDateTestFlatTablet(Edge, tablet, info);
            });

            TAutoPtr<IEventHandle> handle;
            Env.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
        }

        const ui64 Tablet = MakeTabletID(false, 1) & 0xFFFF'FFFF;
        const TActorId Edge;
    };

    Y_UNIT_TEST(TestDate) {
        TDateEnvProfiles env;
        env.Run();
    };
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr
