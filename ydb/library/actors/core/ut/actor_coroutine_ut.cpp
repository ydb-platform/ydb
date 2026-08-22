#include "actor_coroutine.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "events.h"
#include "event_local.h"
#include "hfunc.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>
#include <util/system/hp_timer.h>
#include <util/string/cast.h>

#include <cstdlib>

using namespace NActors;

Y_UNIT_TEST_SUITE(ActorCoro) {
    enum {
        Begin = EventSpaceBegin(TEvents::ES_USERSPACE),
        Request,
        Response,
        Enough,
        ChainMessage,
        ChainMessageReceived
    };

    struct TEvRequest: public TEventLocal<TEvRequest, Request> {
    };

    struct TEvResponse: public TEventLocal<TEvResponse, Response> {
    };

    struct TEvEnough: public TEventLocal<TEvEnough, Enough> {
    };

    struct TEvChainMessage: public TEventLocal<TEvChainMessage, ChainMessage> {
        ui32 Step = 0;

        explicit TEvChainMessage(ui32 step)
            : Step(step)
        {}
    };

    struct TEvChainMessageReceived: public TEventLocal<TEvChainMessageReceived, ChainMessageReceived> {
        ui32 Step = 0;

        explicit TEvChainMessageReceived(ui32 step)
            : Step(step)
        {}
    };

    ui32 GetCoroutineActorChainLength() {
        if (const char* value = std::getenv("ACTOR_COROUTINE_CHAIN_LENGTH")) {
            return FromString<ui32>(value);
        }
        return 10000;
    }

    ui32 GetCoroutineActorChainExecutorThreads() {
        if (const char* value = std::getenv("ACTOR_COROUTINE_CHAIN_EXECUTOR_THREADS")) {
            return FromString<ui32>(value);
        }
        return 4;
    }

    enum class EChainCoroStackKind {
        Legacy,
#if !CORO_THROUGH_THREADS
        StackPool,
#endif
    };

    template <EChainCoroStackKind StackKind>
    class TChainCoroActor;

    struct TChainCoroCompletion;

    template <EChainCoroStackKind StackKind>
    IActor* CreateChainCoroActor(TChainCoroCompletion& completion, ui32 step, ui32 chainLength);

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

    static constexpr ui32 ChainCoroStackSize = 64 * 1024;

    struct TChainCoroCompletion {
        explicit TChainCoroCompletion(ui32 chainCount) {
            AtomicSet(RemainingChains, chainCount);
        }

        void CompleteChain() {
            if (AtomicDecrement(RemainingChains) == 0) {
                DoneEvent.Signal();
            }
        }

        void Wait() {
            DoneEvent.WaitI();
        }

    private:
        TManualEvent DoneEvent;
        TAtomic RemainingChains = 0;
    };

    template <EChainCoroStackKind StackKind>
    struct TChainCoroStackArg;

    template <>
    struct TChainCoroStackArg<EChainCoroStackKind::Legacy> {
        static size_t Create() {
            return ChainCoroStackSize;
        }
    };

#if !CORO_THROUGH_THREADS
    template <>
    struct TChainCoroStackArg<EChainCoroStackKind::StackPool> {
        static TUsePooledStack Create() {
            return UsePooledStack<ChainCoroStackSize>();
        }
    };
#endif

    template <EChainCoroStackKind StackKind>
    class TChainCoroActor: public TActorCoroImpl {
        TChainCoroCompletion& Completion;
        const ui32 Step;
        const ui32 ChainLength;

    public:
        TChainCoroActor(TChainCoroCompletion& completion, ui32 step, ui32 chainLength)
            : TActorCoroImpl(TChainCoroStackArg<StackKind>::Create())
            , Completion(completion)
            , Step(step)
            , ChainLength(chainLength)
        {}

        void Run() override {
            if (Step) {
                THolder<IEventHandle> ev = WaitForEvent();
                UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvChainMessage::EventType);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvChainMessage>()->Step, Step);
                Send(ev->Sender, new TEvChainMessageReceived(Step));
            }

            if (Step == ChainLength) {
                return;
            }

            const ui32 nextStep = Step + 1;
            const TActorId nextActor = Register(CreateChainCoroActor<StackKind>(Completion, nextStep, ChainLength));
            Send(nextActor, new TEvChainMessage(nextStep));

            THolder<IEventHandle> ev = WaitForEvent();
            UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvChainMessageReceived::EventType);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvChainMessageReceived>()->Step, nextStep);
            UNIT_ASSERT_C(ev->Sender == nextActor, "TEvChainMessageReceived sender mismatch");

            if (nextStep == ChainLength) {
                Completion.CompleteChain();
            }
        }
    };

    template <EChainCoroStackKind StackKind>
    IActor* CreateChainCoroActor(TChainCoroCompletion& completion, ui32 step, ui32 chainLength) {
        return new TActorCoro(MakeHolder<TChainCoroActor<StackKind>>(completion, step, chainLength));
    }

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
        Check(MakeHolder<TEvents::TEvPoisonPill>());
    }

    template <EChainCoroStackKind StackKind>
    void RunActorCreationChains(const char* name, ui32 chainCount) {
        if (NSan::TSanIsOn()) {
            return;
        }

        const ui32 chainLength = GetCoroutineActorChainLength();
        const ui32 executorThreads = GetCoroutineActorChainExecutorThreads();
        UNIT_ASSERT_C(chainLength > 0, "ACTOR_COROUTINE_CHAIN_LENGTH must be greater than zero");
        UNIT_ASSERT_C(executorThreads > 0, "ACTOR_COROUTINE_CHAIN_EXECUTOR_THREADS must be greater than zero");
        UNIT_ASSERT_C(chainCount > 0, "chainCount must be greater than zero");

        THolder<TActorSystemSetup> setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 0;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new TBasicExecutorPool(0, executorThreads, 10, "basic");
        setup->Scheduler = new TBasicSchedulerThread;

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TChainCoroCompletion completion(chainCount);

        THPTimer timer;
        timer.Reset();
        for (ui32 chain = 0; chain < chainCount; ++chain) {
            actorSystem.Register(CreateChainCoroActor<StackKind>(completion, 0, chainLength));
        }
        completion.Wait();
        const double elapsedUs = timer.Passed() * 1000000.0;
        const double perChainStepUs = elapsedUs / chainLength;

        Cerr << name
             << " length# " << chainLength
             << " chains# " << chainCount
             << " executor_threads# " << executorThreads
             << " elapsed_us# " << elapsedUs
             << " per_chain_step_us# " << perChainStepUs
             << Endl;

        actorSystem.Stop();
    }

    Y_UNIT_TEST(ActorCreationChainLegacy) {
        RunActorCreationChains<EChainCoroStackKind::Legacy>("ActorCreationChainLegacy", 1);
    }

    Y_UNIT_TEST(ActorCreationChainLegacyParallel4) {
        RunActorCreationChains<EChainCoroStackKind::Legacy>("ActorCreationChainLegacyParallel4", 4);
    }

    Y_UNIT_TEST(ActorCreationChainLegacyParallel16) {
        RunActorCreationChains<EChainCoroStackKind::Legacy>("ActorCreationChainLegacyParallel16", 16);
    }

    Y_UNIT_TEST(ActorCreationChainLegacyParallel64) {
        RunActorCreationChains<EChainCoroStackKind::Legacy>("ActorCreationChainLegacyParallel64", 64);
    }

#if !CORO_THROUGH_THREADS
    Y_UNIT_TEST(ActorCreationChainStackPool) {
        RunActorCreationChains<EChainCoroStackKind::StackPool>("ActorCreationChainStackPool", 1);
    }

    Y_UNIT_TEST(ActorCreationChainStackPoolParallel4) {
        RunActorCreationChains<EChainCoroStackKind::StackPool>("ActorCreationChainStackPoolParallel4", 4);
    }

    Y_UNIT_TEST(ActorCreationChainStackPoolParallel16) {
        RunActorCreationChains<EChainCoroStackKind::StackPool>("ActorCreationChainStackPoolParallel16", 16);
    }

    Y_UNIT_TEST(ActorCreationChainStackPoolParallel64) {
        RunActorCreationChains<EChainCoroStackKind::StackPool>("ActorCreationChainStackPoolParallel64", 64);
    }
#endif
}
