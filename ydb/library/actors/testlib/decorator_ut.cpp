#include "test_runtime.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>


using namespace NActors;


Y_UNIT_TEST_SUITE(TesTTestDecorator) {

    bool IsVerbose = false;
    void Write(TString msg) {
        if (IsVerbose) {
            Cerr << (TStringBuilder() << msg << Endl);
        }
    }

    struct TDyingChecker : TTestDecorator {
        TActorId MasterId;

        TDyingChecker(THolder<IActor> &&actor, TActorId masterId)
            : TTestDecorator(std::move(actor))
            , MasterId(masterId)
        {
            Write("TDyingChecker::Construct\n");
        }

        virtual ~TDyingChecker() {
            Write("TDyingChecker::~TDyingChecker");
            TActivationContext::Send(new IEventHandle(MasterId, SelfId(), new TEvents::TEvPing()));
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle> &/*ev*/, const TActorContext &/*ctx*/) override {
            Write("TDyingChecker::DoBeforeReceiving");
            return true;
        }

        void DoAfterReceiving(const TActorContext &/*ctx*/) override {
            Write("TDyingChecker::DoAfterReceiving");
        }
    };

    struct TTestMasterActor : TActorBootstrapped<TTestMasterActor> {
        friend TActorBootstrapped<TTestMasterActor>;

        TSet<TActorId> ActorIds;
        TVector<THolder<IActor>> Actors;
        TActorId EdgeActor;

        TTestMasterActor(TVector<THolder<IActor>> &&actors, TActorId edgeActor)
            : TActorBootstrapped()
            , Actors(std::move(actors))
            , EdgeActor(edgeActor)
        {
        }

        void Bootstrap()
        {
            Write("Start master actor");
            for (auto &actor : Actors) {
                THolder<IActor> decaratedActor = MakeHolder<TDyingChecker>(std::move(actor), SelfId());
                TActorId id = Register(decaratedActor.Release());
                Write("Register test actor");
                UNIT_ASSERT(ActorIds.insert(id).second);
            }
            Become(&TTestMasterActor::State);
        }

        STATEFN(State) {
            auto it = ActorIds.find(ev->Sender);
            UNIT_ASSERT(it != ActorIds.end());
            Write("End test actor");
            ActorIds.erase(it);
            if (!ActorIds) {
                Send(EdgeActor, new TEvents::TEvPing());
                PassAway();
            }
        }
    };

    enum {
        Begin = EventSpaceBegin(TEvents::ES_USERSPACE),
        EvWords
    };

    struct TEvWords : TEventLocal<TEvWords, EvWords> {
        TVector<TString> Words;

        TEvWords()
            : TEventLocal()
        {
        }
    };

    struct TFizzBuzzToFooBar : TTestDecorator {
        TFizzBuzzToFooBar(THolder<IActor> &&actor)
            : TTestDecorator(std::move(actor))
        {
        }

        bool DoBeforeSending(TAutoPtr<IEventHandle> &ev) override {
            if (ev->Type == TEvents::TSystem::Bootstrap) {
                return true;
            }
            Write("TFizzBuzzToFooBar::DoBeforeSending");
            TEventHandle<TEvWords> *handle = reinterpret_cast<TEventHandle<TEvWords>*>(ev.Get());
            UNIT_ASSERT(handle);
            TEvWords *event = handle->Get();
            TVector<TString> &words = event->Words;
            TStringBuilder wordsMsg;
            for (auto &word : words) {
                wordsMsg << word << ';';
            }
            Write(TStringBuilder() << "Send# " << wordsMsg);
            if (words.size() == 2 && words[0] == "Fizz" && words[1] == "Buzz") {
                words[0] = "Foo";
                words[1] = "Bar";
            }
            return true;
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle> &/*ev*/, const TActorContext &/*ctx*/) override {
            Write("TFizzBuzzToFooBar::DoBeforeReceiving");
            return true;
        }

        void DoAfterReceiving(const TActorContext &/*ctx*/) override {
            Write("TFizzBuzzToFooBar::DoAfterReceiving");
        }
    };

    struct TWordEraser : TTestDecorator {
        TString ErasingWord;

        TWordEraser(THolder<IActor> &&actor, TString word)
            : TTestDecorator(std::move(actor))
            , ErasingWord(word)
        {
        }

        bool DoBeforeSending(TAutoPtr<IEventHandle> &ev) override {
            if (ev->Type == TEvents::TSystem::Bootstrap) {
                return true;
            }
            Write("TWordEraser::DoBeforeSending");
            TEventHandle<TEvWords> *handle = reinterpret_cast<TEventHandle<TEvWords>*>(ev.Get());
            UNIT_ASSERT(handle);
            TEvWords *event = handle->Get();
            TVector<TString> &words = event->Words;
            auto it = Find(words.begin(), words.end(), ErasingWord);
            if (it != words.end()) {
                words.erase(it);
            }
            return true;
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle> &/*ev*/, const TActorContext &/*ctx*/) override {
            Write("TWordEraser::DoBeforeReceiving");
            return true;
        }

        void DoAfterReceiving(const TActorContext &/*ctx*/) override {
            Write("TWordEraser::DoAfterReceiving");
        }
    };

    struct TWithoutWordsDroper : TTestDecorator {
        TWithoutWordsDroper(THolder<IActor> &&actor)
            : TTestDecorator(std::move(actor))
        {
        }

        bool DoBeforeSending(TAutoPtr<IEventHandle> &ev) override {
            if (ev->Type == TEvents::TSystem::Bootstrap) {
                return true;
            }
            Write("TWithoutWordsDroper::DoBeforeSending");
            TEventHandle<TEvWords> *handle = reinterpret_cast<TEventHandle<TEvWords>*>(ev.Get());
            UNIT_ASSERT(handle);
            TEvWords *event = handle->Get();
            return bool(event->Words);
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle> &/*ev*/, const TActorContext &/*ctx*/) override {
            Write("TWithoutWordsDroper::DoBeforeReceiving");
            return true;
        }

        void DoAfterReceiving(const TActorContext &/*ctx*/) override {
            Write("TWithoutWordsDroper::DoAfterReceiving");
        }
    };

    struct TFooBarReceiver : TActorBootstrapped<TFooBarReceiver> {
        TActorId MasterId;
        ui64 Counter = 0;

        TFooBarReceiver(TActorId masterId)
            : TActorBootstrapped()
            , MasterId(masterId)
        {
        }

        void Bootstrap()
        {
            Become(&TFooBarReceiver::State);
        }

        STATEFN(State) {
            TEventHandle<TEvWords> *handle = reinterpret_cast<TEventHandle<TEvWords>*>(ev.Get());
            UNIT_ASSERT(handle);
            UNIT_ASSERT(handle->Sender == MasterId);
            TEvWords *event = handle->Get();
            TVector<TString> &words = event->Words;
            UNIT_ASSERT(words.size() == 2 && words[0] == "Foo" && words[1] == "Bar");
            Write(TStringBuilder() << "Receive# " << Counter + 1 << '/' << 2);
            if (++Counter == 2) {
                PassAway();
            }
        }
    };

    struct TFizzBuzzSender : TActorBootstrapped<TFizzBuzzSender> {
        TActorId SlaveId;

        TFizzBuzzSender()
            : TActorBootstrapped()
        {
            Write("TFizzBuzzSender::Construct");
        }

        void Bootstrap() {
            Write("TFizzBuzzSender::Bootstrap");
            THolder<IActor> actor = MakeHolder<TFooBarReceiver>(SelfId());
            THolder<IActor> decoratedActor = MakeHolder<TDyingChecker>(std::move(actor), SelfId());
            SlaveId = Register(decoratedActor.Release());
            for (ui64 idx = 1; idx <= 30; ++idx) {
                THolder<TEvWords> ev = MakeHolder<TEvWords>();
                if (idx % 3 == 0) {
                    ev->Words.push_back("Fizz");
                }
                if (idx % 5 == 0) {
                    ev->Words.push_back("Buzz");
                }
                Send(SlaveId, ev.Release());
                Write("TFizzBuzzSender::Send words");
            }
            Become(&TFizzBuzzSender::State);
        }

        STATEFN(State) {
            UNIT_ASSERT(ev->Sender == SlaveId);
            PassAway();
        }
    };

    struct TCounters {
        ui64 SendedCount = 0;
        ui64 RecievedCount = 0;
    };

    struct TCountingDecorator : TTestDecorator {
        TCounters *Counters;

        TCountingDecorator(THolder<IActor> &&actor, TCounters *counters)
            : TTestDecorator(std::move(actor))
            , Counters(counters)
        {
        }

        bool DoBeforeSending(TAutoPtr<IEventHandle> &ev) override {
            if (ev->Type == TEvents::TSystem::Bootstrap) {
                return true;
            }
            Write("TCountingDecorator::DoBeforeSending");
            Counters->SendedCount++;
            return true;
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle> &/*ev*/, const TActorContext &/*ctx*/) override {
            Write("TCountingDecorator::DoBeforeReceiving");
            Counters->RecievedCount++;
            return true;
        }
    };

    bool ScheduledFilterFunc(NActors::TTestActorRuntimeBase& runtime, TAutoPtr<NActors::IEventHandle>& event,
            TDuration delay, TInstant& deadline) {
        if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
            deadline = runtime.GetTimeProvider()->Now() + delay;
            return false;
        }
        return true;
    }

    THolder<IActor> CreateFizzBuzzSender() {
        THolder<IActor> actor = MakeHolder<TFizzBuzzSender>();
        THolder<IActor> foobar = MakeHolder<TFizzBuzzToFooBar>(std::move(actor));
        THolder<IActor> fizzEraser = MakeHolder<TWordEraser>(std::move(foobar), "Fizz");
        THolder<IActor> buzzEraser = MakeHolder<TWordEraser>(std::move(fizzEraser), "Buzz");
        return MakeHolder<TWithoutWordsDroper>(std::move(buzzEraser));
    }

    Y_UNIT_TEST(Basic) {
        TTestActorRuntimeBase runtime(1, false);

        runtime.SetScheduledEventFilter(&ScheduledFilterFunc);
        runtime.SetEventFilter([](NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>&) {
            return false;
        });
        runtime.Initialize();

        TActorId edgeActor = runtime.AllocateEdgeActor();
        TVector<THolder<IActor>> actors(1);
        actors[0] = CreateFizzBuzzSender();
        //actors[1] = CreateFizzBuzzSender();
        THolder<IActor> testActor = MakeHolder<TTestMasterActor>(std::move(actors), edgeActor);
        Write("Start test");
        runtime.Register(testActor.Release());

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEventRethrow<TEvents::TEvPing>(handle);
        UNIT_ASSERT(ev);
        Write("Stop test");
    }
}
