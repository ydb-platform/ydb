#include "actor_coroutine.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "events.h"
#include "event_local.h"
#include "hfunc.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(ActorCoro) {
    enum {
        Begin = EventSpaceBegin(TEvents::ES_USERSPACE),
        Request,
        Response,
        Enough
    };

    struct TEvRequest: public TEventLocal<TEvRequest, Request> {
    };

    struct TEvResponse: public TEventLocal<TEvResponse, Response> {
    };

    struct TEvEnough: public TEventLocal<TEvEnough, Enough> {
    };

    class TBasicResponderActor: public TActorBootstrapped<TBasicResponderActor> {
        TDeque<TActorId> RespondTo;

    public:
        TBasicResponderActor() {
        }

        void Bootstrap(const TActorContext& /*ctx*/) {
            Become(&TBasicResponderActor::StateFunc);
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvRequest, Handle);
                HFunc(TEvents::TEvWakeup, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
            }
        }

        void Handle(TEvRequest::TPtr& ev, const TActorContext& ctx) {
            RespondTo.push_back(ev->Sender);
            ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
        }

        void Handle(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& ctx) {
            ctx.Send(RespondTo.front(), new TEvResponse());
            RespondTo.pop_front();
        }

        void Handle(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext& ctx) {
            Die(ctx);
        }
    };

    class TCoroActor: public TActorCoroImpl {
        TManualEvent& DoneEvent;
        TAtomic& ItemsProcessed;
        bool Finish;

        struct TPoisonPillException {};

    public:
        TCoroActor(TManualEvent& doneEvent, TAtomic& itemsProcessed)
            : TActorCoroImpl(1 << 20)
            , DoneEvent(doneEvent)
            , ItemsProcessed(itemsProcessed)
            , Finish(false)
        {
        }

        void Run() override {
            TActorId child = GetActorContext().Register(new TBasicResponderActor);
            ui32 itemsProcessed = 0;
            try {
                while (!Finish) {
                    GetActorContext().Send(child, new TEvRequest());
                    THolder<IEventHandle> resp = WaitForSpecificEvent<TEvResponse>(&TCoroActor::ProcessUnexpectedEvent);
                    UNIT_ASSERT_EQUAL(resp->GetTypeRewrite(), TEvResponse::EventType);
                    ++itemsProcessed;
                }
            } catch (const TPoisonPillException& /*ex*/) {
            }
            GetActorContext().Send(child, new TEvents::TEvPoisonPill);

            AtomicSet(ItemsProcessed, itemsProcessed);
            DoneEvent.Signal();
        }

        void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> event) {
            if (event->GetTypeRewrite() == Enough) {
                Finish = true;
            } else if (event->GetTypeRewrite() == TEvents::TSystem::Poison) {
                throw TPoisonPillException();
            }
        }
    };

    void Check(THolder<IEventBase> && message) {
        THolder<TActorSystemSetup> setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 0;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
            setup->Executors[i] = new TBasicExecutorPool(i, 5, 10, "basic");
        }
        setup->Scheduler = new TBasicSchedulerThread;

        TActorSystem actorSystem(setup);

        actorSystem.Start();

        TManualEvent doneEvent;
        TAtomic itemsProcessed = 0;
        TActorId actor = actorSystem.Register(new TActorCoro(MakeHolder<TCoroActor>(doneEvent, itemsProcessed)));
        NanoSleep(3UL * 1000 * 1000 * 1000);
        actorSystem.Send(actor, message.Release());
        doneEvent.WaitI();

        UNIT_ASSERT(AtomicGet(itemsProcessed) >= 2);

        actorSystem.Stop();
    }

    Y_UNIT_TEST(Basic) {
        if (NSan::TSanIsOn()) {
            // TODO https://st.yandex-team.ru/DEVTOOLS-3154
            return;
        }
        Check(MakeHolder<TEvEnough>());
    }

    Y_UNIT_TEST(PoisonPill) {
        UNIT_ASSERT(42 == 420);
    }
}
