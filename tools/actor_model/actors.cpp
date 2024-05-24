#include "actors.h"
#include "events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

static auto ShouldContinue = std::make_shared<TProgramShouldContinue>();

class TReadActor : public NActors::TActorBootstrapped<TReadActor> {
public:
    TReadActor(const NActors::TActorId writeActor)
        : WriteActor(writeActor) {
        flag = false;
        count = 0;
    
    }

    void Bootstrap() {
        Become(&TReadActor::StateFunc);
        Send(SelfId(), new NActors::TEvents::TEvWakeup());
    }

    void WakeUpHandler() {
        int64_t value;
        if (std::cin >> value) {
            Register(CreateMaximumPrimeDevisorActor(value, SelfId(), WriteActor).Release());
            count++;
            Send(SelfId(), new NActors::TEvents::TEvWakeup());
        }
        else {
            flag = true;
            if (count == 0) {
                Send(wr, new NActors::TEvents::TEvPoisonPill());
            }
        }
    }

    void HandleDone() {
        count--;
        if (flag && count == 0) {
            Send(wr, new NActors::TEvents::TEvPoisonPill());
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, WakeUpHandler);
            hFunc(TEvents::TEvDone, HandleDone);
        default:
            break;
        }
    }

private:
    bool flag = false;
    const NActors::TActorId WriteActor;
    int count = 0;
};
THolder<NActors::IActor> CreateReadActor(NActors::TActorId wr) {
    return MakeHolder<TReadActor>(wr);
}



class TMaximumPrimeDevisorActor : public NActors::TActorBootstrapped<TMaximumPrimeDevisorActor> {
public:
    TMaximumPrimeDevisorActor(int64_t value, const NActors::TActorId readActor, const NActors::TActorId writeActor)
        : num(value), ReadActor(readActor), WriteActor(writeActor) {}

    void Bootstrap() {
        Become(&TMaximumPrimeDevisorActor::StateFunc);
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
    }

    void HandleWakeUp() {
        if (num < 1) {
            Send(WriteActor, new TEvents::TEvWriteValueRequest(0));
            Send(ReadActor, new TEvents::TEvDone());
            PassAway();
            return;
        }
        auto startTime = std::chrono::steady_clock::now();

        while (CurrentDevisor * CurrentDevisor <= num) {
            if (num % CurrentDevisor == 0) {
                LargestPrimeDevisor = CurrentDevisor;
                num /= CurrentDevisor;
            }
            else {
                CurrentDevisor++;
            }
            auto runTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startTime).count();
            if (runTime > 10) {
                Send(SelfId(), new NActors::TEvents::TEvWakeup());
                return;
            }
        }

        if (num > 1) {
            LargestPrimeDevisor = num;
        }

        Send(WriteActor, std::make_unique<TEvents::TEvWriteValueRequest>(LargestPrimeDevisor));
        Send(ReadActor, std::make_unique<TEvents::TEvDone>());
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            sFunc(NActors::TEvents::TEvWakeup, HandleWakeUp);
        default:
            break;
        }
    }
private:
    int64_t num;
    const NActors::TActorId ReadActor;
    const NActors::TActorId WriteActor;
    int64_t LargestPrimeDevisor = 1;
    int64_t CurrentDevisor = 2;
};
NActors::IActor* CreateDivideActor(int64_t val, NActors::TActorId rd, NActors::TActorId wr) {
    return new TMaximumPrimeDevisorActor(val, rd, wr);
}
class TWriteActor : public NActors::TActor<TWriteActor> {
public:
    TWriteActor() : TActor(&TWriteActor::StateFunc) {}

    void WriteValueRequestHandler(TEvents::TEvWriteValueRequest::TPtr& ev) {
        list += ev->Get()->Value;
    }

    void PoisonPillHandler() {
        std::cout << list << std::endl;
        ShouldContinue->ShouldStop();
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWriteValueRequest, WriteValueRequestHandler);
            sFunc(NActors::TEvents::TEvPoisonPill, PoisonPillHandler);
        default:
            break;
        }
    }
private:
    int64_t list = 0;
};
THolder<NActors::IActor> CreateWriteActor() {
    return MakeHolder<TWriteActor>();
}




//




class TSelfPingActor : public NActors::TActorBootstrapped<TSelfPingActor> {
    TDuration Latency;
    TInstant LastTime;

public:
    TSelfPingActor(const TDuration& latency)
        : Latency(latency)
    {}

    void Bootstrap() {
        LastTime = TInstant::Now();
        Become(&TSelfPingActor::StateFunc);
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
    }

    STRICT_STFUNC(StateFunc, {
        cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeup);
    });

    void HandleWakeup() {
        auto now = TInstant::Now();
        TDuration delta = now - LastTime;
        Y_VERIFY(delta <= Latency, "Latency too big");
        LastTime = now;
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
    }
};

THolder<NActors::IActor> CreateSelfPingActor(const TDuration& latency) {
    return MakeHolder<TSelfPingActor>(latency);
}

std::shared_ptr<TProgramShouldContinue> GetProgramShouldContinue() {
    return ShouldContinue;
}
