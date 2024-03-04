#pragma once

#include "world.h"
#include "events.h"
#include "helper.h"
#include <ydb/core/tablet_flat/util_fmt_logger.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/ut/flat_test_db.h>
#include <ydb/core/tablet_flat/ut/flat_test_db_helpers.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NFake {

    class TFuncTx final : public NTabletFlatExecutor::ITransaction {
    public:
        using TContext = NTabletFlatExecutor::TTransactionContext;
        using TCall = std::function<bool(NTable::ITestDb&, TContext&)>;

        TFuncTx(NTable::ITestDb &fake, const TActorId& owner, TCall func)
            : Owner(owner)
            , Func(std::move(func))
            , Fake(fake)
            , Pair(Local, Fake)
        {

        }

        ~TFuncTx()
        {
            Y_ABORT_UNLESS(Completed, "Destroying incomplted transaction");
        }

    private:
        bool Execute(TTransactionContext &txc, const TActorContext&) override
        {
            Y_ABORT_UNLESS(!Completed, "TFuncTx is already completed");

            Local.SetDb(&txc.DB);

            try {
                Completed = Func(Pair, txc);
            } catch (NTable::TIteratorNotReady&) {

            }

            Fake.FinishTransaction(Completed);
            return Completed;
        }

        void Complete(const TActorContext &ctx) override
        {
            Y_ABORT_UNLESS(Completed, "Finalizing incomplteted transaction");

            ctx.Send(Owner, new NFake::TEvResult);
            Local.SetDb(nullptr);
        }

    private:
        const TActorId Owner;
        TCall Func;
        bool Completed = false;
        NTable::ITestDb &Fake;
        NTable::TFlatDbWrapper Local;
        NTable::TDbPair Pair;
    };


    class TNanny : public ::NActors::IActorCallback {
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;

    public:

        enum class EDo {
            More    = 1,
            Born    = 2,
            Stop    = 3,
            Gone    = 8,
        };

        TNanny()
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TNanny::Inbox), NActors::EInternalActorType::OTHER)
            , Fake(NTable::CreateFakeDb())
        {
            Fake->Init(NTable::TScheme());
        }

        virtual ~TNanny() = default;

        static bool CompareDbs(NTable::ITestDb &test, TFuncTx::TContext&)
        {
            auto &pair = dynamic_cast<NTable::TDbPair&>(test);

            bool cmpRes  = pair.CompareDBs();
            UNIT_ASSERT_C(cmpRes, "Test DB differs from the reference DB!");
            return true;
        }

    protected:
        void QueueTx(TFuncTx::TCall func) noexcept
        {
            TxInFlight++;

            auto *tx = new TFuncTx(*Fake, SelfId(), func);

            Send(Tablet, new NFake::TEvExecute(tx));
        }

    private:
        virtual EDo Run() = 0;

        void Registered(TActorSystem *sys, const TActorId &owner) override
        {
            Logger = new NUtil::TLogger(sys, NKikimrServices::FAKE_ENV);

            Owner = owner, StartTablet();
        }

        void Inbox(TEventHandlePtr &eh)
        {
            if (auto *ev = eh->CastAsLocal<NFake::TEvResult>()) {
                Handle(*ev);
            } else if (auto *ev = eh->CastAsLocal<NFake::TEvReady>()) {
                Handle(*ev);
            } else if (eh->CastAsLocal<TEvents::TEvGone>()) {

                if (State == EDo::Born) {
                    StartTablet();
                } else if (State == EDo::Stop) {
                    DoSuicide();
                } else if (auto logl = Logger->Log(NUtil::ELnLev::Abort)) {
                    logl << "TNanny tablet " << MyId << " suddenly died";
                }
            } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
                DoSuicide();
            } else {
                Y_Fail("Unexpected event " << eh->GetTypeName());
            }
        }

        void Handle(NFake::TEvReady &ev) noexcept
        {
            if (std::exchange(State, EDo::More) != EDo::Born) {
                Y_ABORT("Got an unexpected TEvReady{ } event");
            } else if (std::exchange(Tablet, ev.ActorId)) {
                Y_ABORT("Child tablet actor is still alive");
            } else if (TxInFlight > 0) {
                Y_ABORT("Just bron(rebooted) tablet has pending tx");
            }

            QueueTx(CompareDbs);
        }

        void Handle(NFake::TEvResult&) noexcept
        {
            Y_ABORT_UNLESS(TxInFlight-- > 0, "Tx counter is underflowed");

            if (State == EDo::More) State = Run();

            if (TxInFlight > 0) {
                /* Should wait for pending tx completion before tablet kill */
            } else if (State == EDo::Born) {
                Y_ABORT_UNLESS(Tablet, "Tabled has been already restarted");

                Send(std::exchange(Tablet, { }), new TEvents::TEvPoison);
            } else if (State == EDo::Stop) {
                Send(std::exchange(Tablet, { }), new TEvents::TEvPoison);
            } else {
                Y_ABORT("TNanny actor cannot progress: no tx, no EDo");
            }
        }

        void StartTablet() noexcept
        {
            if (auto logl = Logger->Log(NUtil::ELnLev::Info)) {
                logl << "TNanny initiates TDummy tablet " << MyId << " birth";
            }

            auto make = [this](const TActorId &tablet, TStorageInfo *info) {
                return new NFake::TDummy(tablet, info, SelfId());
            };

            auto *actor = TStarter().Do(SelfId(), 1, MyId, std::move(make));
            auto *event = new TEvFire{ 7, { }, { actor, EMail::Simple, 0 } };

            Send(TWorld::Where(EPath::Root), event);
        }

        void DoSuicide() noexcept
        {
            Send(std::exchange(Owner, { }), new TEvents::TEvGone);
            State = EDo::Gone;
            PassAway();
        }

    private:
        const ui64 MyId = MakeTabletID(false, 1);
        TAutoPtr<NUtil::ILogger> Logger;
        TActorId Owner;
        TActorId Tablet;
        ui64 TxInFlight = 0;
        EDo State = EDo::Born;
        TAutoPtr<NTable::ITestDb> Fake;
    };

}
}
