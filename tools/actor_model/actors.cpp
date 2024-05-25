#include "actors.h"
#include "events.h"
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <iostream>
#include <thread>

static auto ShouldContinue = std::make_shared<TProgramShouldContinue>();

class TReadActor : public NActors::TActorBootstrapped<TReadActor> {
private:
    std::shared_ptr<TProgramShouldContinue> ShouldContinue;
    NActors::TActorId WriteActor;
    size_t ActiveCalculations = 0;

public:
    TReadActor(std::shared_ptr<TProgramShouldContinue> shouldContinue, const NActors::TActorId& writeActor)
        : ShouldContinue(shouldContinue), WriteActor(writeActor) {}

    void Bootstrap() {
        Become(&TReadActor::StateFunc);
        Send(SelfId(), new NActors::TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
        hFunc(TEvents::TEvDone, HandleDone)
    )

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr) {
        int64_t value;
        if (std::cin >> value) {
            Register(CreateMaximumPrimeDevisorActor(value, SelfId(), WriteActor).Release());
            ++ActiveCalculations;
            Send(SelfId(), new NActors::TEvents::TEvWakeup());
        } else {
            if (ActiveCalculations == 0) {
                Send(WriteActor, new NActors::TEvents::TEvPoisonPill());
                PassAway();
            }
        }
    }

    void HandleDone(TEvents::TEvDone::TPtr) {
        --ActiveCalculations;
        if (ActiveCalculations == 0) {
            Send(WriteActor, new NActors::TEvents::TEvPoisonPill());
            PassAway();
        }
    }
};

class TMaximumPrimeDevisorActor : public NActors::TActorBootstrapped<TMaximumPrimeDevisorActor> {
private:
    int64_t Value;
    NActors::TActorId ReadActor;
    NActors::TActorId WriteActor;

public:
    TMaximumPrimeDevisorActor(int64_t value, const NActors::TActorId& readActor, const NActors::TActorId& writeActor)
        : Value(value), ReadActor(readActor), WriteActor(writeActor) {}

    void Bootstrap() {
        Become(&TMaximumPrimeDevisorActor::StateFunc);
        Send(SelfId(), new NActors::TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
    )

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr) {
        auto startTime = std::chrono::high_resolution_clock::now();

        int64_t maxPrimeDivisor = 1;
        for (int64_t i = 2; i <= Value / i; i++) {
            while (Value % i == 0) {
                maxPrimeDivisor = i;
                Value /= i;
            }
        }
        if (Value > 1) {
            maxPrimeDivisor = Value;
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count() > 10) {
            Send(SelfId(), new NActors::TEvents::TEvWakeup());
        } else {
            Send(WriteActor, new TEvents::TEvWriteValueRequest(maxPrimeDivisor));
            Send(ReadActor, new TEvents::TEvDone());
            PassAway();
        }
    }
};

class TWriteActor : public NActors::TActorBootstrapped<TWriteActor> {
private:
    std::shared_ptr<TProgramShouldContinue> ShouldContinue;
    int64_t Sum = 0;

public:
    TWriteActor(std::shared_ptr<TProgramShouldContinue> shouldContinue)
        : ShouldContinue(shouldContinue) {}

    void Bootstrap() {
        Become(&TWriteActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill)
        hFunc(TEvents::TEvWriteValueRequest, HandleWriteValueRequest)
    )

    void HandlePoisonPill(NActors::TEvents::TEvPoisonPill::TPtr) {
        std::cout << Sum << std::endl;
        ShouldContinue->ShouldStop();
        PassAway();
    }

    void HandleWriteValueRequest(TEvents::TEvWriteValueRequest::TPtr ev) {
        Sum += ev->Get()->Value;
    }
};

THolder<NActors::IActor> CreateReadActor(std::shared_ptr<TProgramShouldContinue> shouldContinue, const NActors::TActorId& writeActor) {
    return MakeHolder<TReadActor>(shouldContinue, writeActor);
}

THolder<NActors::IActor> CreateMaximumPrimeDevisorActor(int64_t value, const NActors::TActorId& readActor, const NActors::TActorId& writeActor) {
    return MakeHolder<TMaximumPrimeDevisorActor>(value, readActor, writeActor);
}

THolder<NActors::IActor> CreateWriteActor(std::shared_ptr<TProgramShouldContinue> shouldContinue) {
    return MakeHolder<TWriteActor>(shouldContinue);
}

std::shared_ptr<TProgramShouldContinue> GetProgramShouldContinue() {
    return ShouldContinue;
}
