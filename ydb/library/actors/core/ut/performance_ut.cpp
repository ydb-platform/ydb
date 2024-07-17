#include "actorsystem.h"
#include "av_bootstrapped.h"
#include "actor_virtual.h"
#include "actor_bootstrapped.h"
#include "config.h"
#include "event_local.h"
#include "executor_pool_basic.h"
#include "executor_pool_io.h"
#include "executor_pool_base.h"
#include "scheduler_basic.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(ActorSystemPerformance) {

    class TQueueTestRuntime {
        std::unique_ptr<TActorSystem> ActorSystem;
    public:
        TQueueTestRuntime() {
            auto setup = MakeHolder<TActorSystemSetup>();
            setup->NodeId = 1;
            setup->ExecutorsCount = 2;
            setup->Executors.Reset(new TAutoPtr<IExecutorPool>[2]);
            setup->Executors[0].Reset(new TBasicExecutorPool(0, 4, 20));
            setup->Executors[1].Reset(new TIOExecutorPool(1, 10));

            setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

            ActorSystem.reset(new TActorSystem{ setup });
        }

        void Start() {
            ActorSystem->Start();
        }

        void Stop() {
            ActorSystem->Stop();
            ActorSystem.reset();
        }

        void Send(std::unique_ptr<IEventHandle>&& ev) {
            ActorSystem->Send(ev.release());
        }

        void Register(IActor* actor) {
            ActorSystem->Register(actor);
        }
    };

    enum EEvSubscribe {
        EvDolbExecute,
        EvEnd
    };

    class TEventLocalDolbilkaOld: public NActors::TEventLocal<TEventLocalDolbilkaOld, EvDolbExecute> {
    public:

    };

    class TDolbilkaCommon {
    protected:
        ui32 Counter = 0;
        ui32 CounterLimit = 10000000;
        const TInstant Start = Now();
        std::atomic<TDuration> Duration;
        std::atomic<bool> Ready = false;
    public:
        bool MakeStep() {
            if (++Counter >= CounterLimit) {
                TDolbilkaCommon::Finish();
                return false;
            }
            return true;
        }
        TDuration GetDurationInProgress() const {
            return Now() - Start;
        }
        double GetOperationDuration() const {
            return 1.0 * Duration.load().MicroSeconds() / Counter * 1000;
        }
        void Finish() {
            if (!Ready.exchange(true)) {
                Duration = GetDurationInProgress();
            }
        }
        bool IsFinished() const {
            return Ready;
        }
    };

    class TDolbilkaOld: public TDolbilkaCommon, public NActors::TActorBootstrapped<TDolbilkaOld> {
    private:
        using TBase = NActors::TActorBootstrapped<TDolbilkaOld>;
    public:

        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEventLocalDolbilkaOld, Handle);
                default:
                    Y_ABORT_UNLESS(false);
            }
        }

        void Handle(TEventLocalDolbilkaOld::TPtr& /*ev*/, const TActorContext&) {
            if (MakeStep()) {
                Sender<TEventLocalDolbilkaOld>().SendTo(SelfId());
            }
        }

        void Bootstrap() {
            Become(&TThis::StateInit);
            Sender<TEventLocalDolbilkaOld>().SendTo(SelfId());
        }
    };

    class TDolbilkaNew;
    class TEventLocalDolbilkaNew: public NActors::TEventLocalForActor<TEventLocalDolbilkaNew, TDolbilkaNew> {
    public:

    };

    class TDolbilkaNew: public TDolbilkaCommon, public NActors::TActorAutoStart {
    private:
        using TBase = NActors::TActorAutoStart;
    protected:
        virtual void DoOnStart(const TActorId& /*senderActorId*/) override {
            Sender<TEventLocalDolbilkaNew>().SendTo(SelfId());
        }

    public:
        void ProcessEvent(NActors::TEventContext<TEventLocalDolbilkaNew>& /*ev*/) {
            if (MakeStep()) {
                Sender<TEventLocalDolbilkaNew>().SendTo(SelfId());
            }
        }
    };

    class IDolbilkaSimple {
    public:
        virtual ~IDolbilkaSimple() = default;
        virtual bool ProcessEvent() = 0;
    };

    class TDolbilkaSimple: public TDolbilkaCommon, public IDolbilkaSimple {
    private:
//        TMutex Mutex;
    public:
        virtual bool ProcessEvent() override {
//            TGuard<TMutex> g(Mutex);
            return MakeStep();
        }
    };

    Y_UNIT_TEST(PerfTest) {
        THolder<TQueueTestRuntime> runtime(new TQueueTestRuntime);
        runtime->Start();
        TDolbilkaNew* dNew = new TDolbilkaNew;
        runtime->Register(dNew);
        while (dNew->GetDurationInProgress() < TDuration::Seconds(1000) && !dNew->IsFinished()) {
            Sleep(TDuration::Seconds(1));
        }
        Y_ABORT_UNLESS(dNew->IsFinished());
        TDolbilkaOld* dOld = new TDolbilkaOld;
        runtime->Register(dOld);
        while (dOld->GetDurationInProgress() < TDuration::Seconds(1000) && !dOld->IsFinished()) {
            Sleep(TDuration::Seconds(1));
        }
        Y_ABORT_UNLESS(dOld->IsFinished());
        std::unique_ptr<TDolbilkaSimple> dSimple(new TDolbilkaSimple);
        IDolbilkaSimple* dSimpleIface = dSimple.get();
        while (dSimpleIface->ProcessEvent()) {

        }
        Cerr << "DURATION_OLD: " << 1.0 * dOld->GetOperationDuration() << "ns" << Endl;
        Cerr << "DURATION_NEW: " << 1.0 * dNew->GetOperationDuration() << "ns" << Endl;
        Cerr << "DURATION_SIMPLE: " << 1.0 * dSimple->GetOperationDuration() << "ns" << Endl;

    }
}
