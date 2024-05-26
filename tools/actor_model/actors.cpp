#include "actors.h"
#include "events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

static auto ShouldContinue = std::make_shared<TProgramShouldContinue>();

bool IsPrime(int64_t number) {
    if (number <= 1) return false;
    if (number <= 3) return true;
    if (number % 2 == 0 || number % 3 == 0) return false;
    for (int64_t i = 5; i * i <= number; i += 6) {
        if (number % i == 0 || number % (i + 2) == 0) return false;
    }
    return true;
}

int64_t FindMaxPrimeDivisor(int64_t n) {
    int64_t maxPrime = 0;
    int64_t curVal = 1;
    
    if (n == 1){
    	return 1;
    }
    
    while (curVal <= n){
    	if (n%curVal==0 && IsPrime(curVal)){
    	    maxPrime = curVal;
    	}
    	curVal++;
    }
    
    return maxPrime;
}

class TReadActor : public NActors::TActorBootstrapped<TReadActor> {
    std::istream& Strm;
    NActors::TActorId Recipient;
    int Counter = 0;
    bool InputEnded;

public:
    TReadActor(std::istream& strm, NActors::TActorId recipient) : Strm(strm), Recipient(recipient), InputEnded() {}

    void Bootstrap() {
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
        Become(&TReadActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc, {
        cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeup);
        cFunc(TEvents::TEvDone::EventType, HandleDone);
    });

    void HandleWakeup() {
        int64_t value;

        if (Strm >> value) {
            Register(CreateMaximumPrimeDevisorActor(value, SelfId(), Recipient).Release());
            Counter++;
            Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
        } else {
            InputEnded = true;
            if (Counter == 0) {
                Send(Recipient, std::make_unique<NActors::TEvents::TEvPoisonPill>());
                PassAway();
            }
        }
    }

    void HandleDone() {
        Counter--;

        if (Counter == 0 && InputEnded) {
            Send(Recipient, std::make_unique<NActors::TEvents::TEvPoisonPill>());
            PassAway();
        }
    }
};

class TMaximumPrimeDevisorActor : public NActors::TActorBootstrapped<TMaximumPrimeDevisorActor> {
    const int64_t Value;
    const NActors::TActorId ReadActor;
    const NActors::TActorId WriteActor;

public:
    TMaximumPrimeDevisorActor(int64_t value, const NActors::TActorId& readActor, const NActors::TActorId& writeActor)
        : Value(value), ReadActor(readActor), WriteActor(writeActor) {}

    void Bootstrap() {
        Become(&TMaximumPrimeDevisorActor::StateFunc);
        Send(SelfId(), new NActors::TEvents::TEvWakeup);
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
    });

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        using namespace std::chrono;
        auto start = steady_clock::now();
        int64_t maxPrime = FindMaxPrimeDivisor(Value);
        auto end = steady_clock::now();
        duration<double> diff = end - start;

        if (diff.count() > 0.01) {
            Send(SelfId(), new NActors::TEvents::TEvWakeup);
        } else {
            Send(WriteActor, new TEvents::TEvWriteValueRequest(maxPrime));
            Send(ReadActor, new TEvents::TEvDone);
            PassAway();
        }
    }
};

class TWriteActor : public NActors::TActor<TWriteActor> {
    int64_t Sum = 0;

public:
    TWriteActor() : TActor(&TWriteActor::StateFunc) {}

    STRICT_STFUNC(StateFunc, {
        hFunc(TEvents::TEvWriteValueRequest, HandleRequest);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, HandlePoisoning);
    });

    void HandleRequest(TEvents::TEvWriteValueRequest::TPtr ev) {
        Sum += ev -> Get() -> Value;
    }

    void HandlePoisoning() {
        Cout << Sum << Endl;
        ShouldContinue->ShouldStop();
        PassAway();
    }
};

THolder<NActors::IActor> CreateReadActor(std::istream& strm, NActors::TActorId recipient) {
    return MakeHolder<TReadActor>(strm, recipient);
}

THolder<NActors::IActor> CreateMaximumPrimeDevisorActor(int64_t value, NActors::TActorId readActorId, NActors::TActorId writeActorId) {
    return MakeHolder<TMaximumPrimeDevisorActor>(value, readActorId, writeActorId);
}

THolder<NActors::IActor> CreateWriteActor() {
    return MakeHolder<TWriteActor>();
}

std::shared_ptr<TProgramShouldContinue> GetProgramShouldContinue() {
    return ShouldContinue;
}
