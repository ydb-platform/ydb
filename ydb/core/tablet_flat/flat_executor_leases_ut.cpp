#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TFlatExecutorLeases) {

    struct TEvLeasesTablet {
        enum EEv {
            EvWrite = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvWriteAck,
            EvRead,
            EvReadResult,
        };

        struct TEvWrite : public TEventLocal<TEvWrite, EvWrite> {
            TEvWrite(ui64 key, ui64 value)
                : Key(key)
                , Value(value)
            { }

            const ui64 Key;
            const ui64 Value;
        };

        struct TEvWriteAck : public TEventLocal<TEvWriteAck, EvWriteAck> {
            TEvWriteAck(ui64 key)
                : Key(key)
            { }

            const ui64 Key;
        };

        struct TEvRead : public TEventLocal<TEvRead, EvRead> {
            TEvRead(ui64 key)
                : Key(key)
            { }

            const ui64 Key;
        };

        struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
            TEvReadResult(ui64 key, ui64 value)
                : Key(key)
                , Value(value)
            { }

            const ui64 Key;
            const ui64 Value;
        };
    };

    class TLeasesTablet
        : public TActor<TLeasesTablet>
        , public TTabletExecutedFlat
    {
    public:
        TLeasesTablet(const TActorId& tablet, TTabletStorageInfo* info, bool enableInitialLease)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
            , EnableInitialLease(enableInitialLease)
        { }

    private:
        struct Schema : NIceDb::Schema {
            struct Data : Table<1> {
                struct Key : Column<1, NScheme::NTypeIds::Uint64> {};
                struct Value : Column<2, NScheme::NTypeIds::Uint64> {};

                using TKey = TableKey<Key>;
                using TColumns = TableColumns<Key, Value>;
            };

            using TTables = SchemaTables<Data>;
        };

        using TTxBase = TTransactionBase<TLeasesTablet>;

        struct TTxInitSchema : public TTxBase {
            TTxInitSchema(TSelf* self)
                : TTxBase(self)
            { }

            bool Execute(TTransactionContext& txc, const TActorContext&) {
                NIceDb::TNiceDb db(txc.DB);
                db.Materialize<Schema>();
                Self->RunTxInit();
                return true;
            }

            void Complete(const TActorContext&) {
                // nothing
            }
        };

        void RunTxInitSchema() {
            Execute(new TTxInitSchema(this));
        }

        struct TTxInit : public TTxBase {
            TTxInit(TSelf* self)
                : TTxBase(self)
            { }

            bool Execute(TTransactionContext& txc, const TActorContext&) {
                NIceDb::TNiceDb db(txc.DB);

                auto rowset = db.Table<Schema::Data>().Select();
                if (!rowset.IsReady()) {
                    return false;
                }
                while (!rowset.EndOfSet()) {
                    ui64 key = rowset.GetValue<Schema::Data::Key>();
                    ui64 value = rowset.GetValue<Schema::Data::Value>();
                    Self->Data[key] = value;
                    if (!rowset.Next()) {
                        return false;
                    }
                }

                return true;
            }

            void Complete(const TActorContext& ctx) {
                Self->SwitchToWork(ctx);
            }
        };

        void RunTxInit() {
            Execute(new TTxInit(this));
        }

        struct TTxWrite : public TTxBase {
            TTxWrite(TSelf* self, TEvLeasesTablet::TEvWrite::TPtr&& ev)
                : TTxBase(self)
                , Ev(std::move(ev))
            { }

            bool Execute(TTransactionContext& txc, const TActorContext&) {
                auto* msg = Ev->Get();

                NIceDb::TNiceDb db(txc.DB);

                db.Table<Schema::Data>().Key(msg->Key).Update(
                    NIceDb::TUpdate<Schema::Data::Value>(msg->Value));
                return true;
            }

            void Complete(const TActorContext& ctx) {
                auto* msg = Ev->Get();
                Self->Data[msg->Key] = msg->Value;

                ctx.Send(Ev->Sender, new TEvLeasesTablet::TEvWriteAck(msg->Key), 0, Ev->Cookie);
            }

            const TEvLeasesTablet::TEvWrite::TPtr Ev;
        };

        void Handle(TEvLeasesTablet::TEvWrite::TPtr& ev) {
            Execute(new TTxWrite(this, std::move(ev)));
        }

        void Handle(TEvLeasesTablet::TEvRead::TPtr& ev) {
            auto sender = ev->Sender;
            auto cookie = ev->Cookie;
            auto key = ev->Get()->Key;
            auto value = Data[key];
            Executor()->ConfirmReadOnlyLease([this, sender, cookie, key, value]() {
                Send(sender, new TEvLeasesTablet::TEvReadResult(key, value), 0, cookie);
            });
        }

    private:
        void OnDetach(const TActorContext& ctx) override {
            Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) override {
            Die(ctx);
        }

        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext&) override {
            Become(&TThis::StateWork);
            RunTxInitSchema();
        }

        void SwitchToWork(const TActorContext& ctx) {
            SignalTabletActive(ctx);
        }

        bool ReadOnlyLeaseEnabled() override {
            return EnableInitialLease;
        }

    private:
        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvLeasesTablet::TEvWrite, Handle);
                hFunc(TEvLeasesTablet::TEvRead, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

    private:
        const bool EnableInitialLease;
        THashMap<ui64, ui64> Data;
    };

    void DoBasics(bool deliverDropLease, bool enableInitialLease) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        const ui64 tabletId = TTestTxConfig::TxTablet0;

        auto boot1 = CreateTestBootstrapper(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [enableInitialLease](const TActorId & tablet, TTabletStorageInfo* info) {
                return new TLeasesTablet(tablet, info, enableInitialLease);
            });
        runtime.EnableScheduleForActor(boot1);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 1));
            runtime.DispatchEvents(options);
        }

        auto sender = runtime.AllocateEdgeActor(0);
        auto pipe1 = runtime.ConnectToPipe(tabletId, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        runtime.SendToPipe(pipe1, sender, new TEvLeasesTablet::TEvWrite(1, 11));
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLeasesTablet::TEvWriteAck>(sender);
        }
        runtime.SendToPipe(pipe1, sender, new TEvLeasesTablet::TEvRead(1));
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLeasesTablet::TEvReadResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Value, 11u);
        }

        bool blockDropLease = true;
        TVector<THolder<IEventHandle>> dropLeaseMsgs;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTablet::TEvDropLease::EventType:
                    if (blockDropLease) {
                        dropLeaseMsgs.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                case TEvTablet::TEvTabletDead::EventType:
                    // Prevent tablets from restarting
                    // This is most important for the boot1 actor, since it
                    // quickly receives bad commit signal and tries to restart
                    // the original tablet. However we prevent executor from
                    // killing itself too, so we could make additional queries.
                    return TTestActorRuntime::EEventAction::DROP;
                case TEvTablet::TEvDemoted::EventType:
                    // Block guardian from telling tablet about a new generation
                    return TTestActorRuntime::EEventAction::DROP;
                case TEvStateStorage::TEvReplicaLeaderDemoted::EventType:
                    // Block replica from telling tablet about a new generation
                    return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserver = runtime.SetObserverFunc(observerFunc);

        auto boot2 = CreateTestBootstrapper(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [enableInitialLease](const TActorId & tablet, TTabletStorageInfo* info) {
                return new TLeasesTablet(tablet, info, enableInitialLease);
            },
            /* node index */ 1);
        runtime.EnableScheduleForActor(boot2);

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 1));
            runtime.DispatchEvents(options);
        }

        // After the new tablet is restored, we should still be able to read from the old tablet for a while
        runtime.SendToPipe(pipe1, sender, new TEvLeasesTablet::TEvRead(1));
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLeasesTablet::TEvReadResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Value, 11u);
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

        waitFor([&]{ return dropLeaseMsgs.size() == 1; }, "drop lease message");

        // We expect the new tablet to send a drop lease message, but the old tablet must still be readable, because it didn't receive it yet
        runtime.SendToPipe(pipe1, sender, new TEvLeasesTablet::TEvRead(1));
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLeasesTablet::TEvReadResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Value, 11u);
        }

        if (deliverDropLease) {
            blockDropLease = false;
            for (auto& ev : dropLeaseMsgs) {
                runtime.Send(ev.Release(), 0, true);
            }
            dropLeaseMsgs.clear();
        }

        auto sender2 = runtime.AllocateEdgeActor(1);
        auto pipe2 = runtime.ConnectToPipe(tabletId, sender2, 1, NTabletPipe::TClientRetryPolicy::WithRetries());
        runtime.SendToPipe(pipe2, sender2, new TEvLeasesTablet::TEvRead(1), 1);
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLeasesTablet::TEvReadResult>(sender2);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Value, 11u);
        }

        runtime.SendToPipe(pipe1, sender, new TEvLeasesTablet::TEvRead(1));
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLeasesTablet::TEvReadResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT(!ev);
        }
    }

    Y_UNIT_TEST(Basics) {
        DoBasics(true, false);
    }

    Y_UNIT_TEST(BasicsLeaseTimeout) {
        DoBasics(false, false);
    }

    Y_UNIT_TEST(BasicsInitialLease) {
        DoBasics(true, true);
    }

    Y_UNIT_TEST(BasicsInitialLeaseTimeout) {
        DoBasics(false, true);
    }
}

} // namespace NKikimr::NTabletFlatExecutor
