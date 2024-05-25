#include "actors.h"
#include "events.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

static auto IsContinueProgram = std::make_shared<TProgramShouldContinue>();

class TInputReadActor : public NActors::TActorBootstrapped<TInputReadActor> {
    std::istream& InputStream;
    NActors::TActorId TargetActor;
    int ActiveActors = 0;
    bool InputStreamEnd;

public:
    TInputReadActor(std::istream& input, NActors::TActorId target)
        : InputStream(input), TargetActor(target), InputStreamEnd(false) {}

    void Bootstrap() {
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
        Become(&TInputReadActor::StateHandler);
    }

    STRICT_STFUNC(StateHandler, {
        cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeUpCall);
        cFunc(TEvents::TEvDone::EventType, HandleCompletion);
    });

    void HandleWakeUpCall() {
        int64_t value;

        if (InputStream >> value) {
            Register(CreateMaximumPrimeDevisorActor(value, SelfId(), TargetActor).Release());
            ActiveActors++;
            Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
        } else {
            InputStreamEnd = true;
            if (ActiveActors == 0) {
                IsContinueProgram->ShouldContinue = false;
            }
        }
    }

    void HandleCompletion() {
        ActiveActors--;
        if (ActiveActors == 0 && InputStreamEnd) {
            IsContinueProgram->ShouldContinue = false;
        }
    }
};

class TOutputWriteActor : public NActors::TActorBootstrapped<TOutputWriteActor> {
    std::ostream& OutputStream;

public:
    explicit TOutputWriteActor(std::ostream& output)
        : OutputStream(output) {}

    void Bootstrap() {
        Become(&TOutputWriteActor::StateHandler);
    }

    STRICT_STFUNC(StateHandler, {
        hFunc(TEvents::TEvDone, ProcessDoneEvent);
    });

    void ProcessDoneEvent(TEvents::TEvDone::TPtr& event) {
        OutputStream << event->Get()->MaxPrimeDivisor << std::endl;
    }
};

class TMaximumPrimeDevisorActor : public NActors::TActorBootstrapped<TMaximumPrimeDevisorActor> {
    const int64_t Number;
    const NActors::TActorId ReaderActor;
    const NActors::TActorId OutputActor;

public:
    TMaximumPrimeDevisorActor(int64_t number, NActors::TActorId reader, NActors::TActorId output)
        : Number(number), ReaderActor(reader), OutputActor(output) {}

    void Bootstrap() {
        int64_t maxPrimeDivisor = FindMaxPrimeDivisor();
        auto doneEvent = std::make_unique<TEvents::TEvDone>(maxPrimeDivisor);
        Send(OutputActor, doneEvent.release());
        Send(ReaderActor, std::make_unique<TEvents::TEvDone>());
        passAway();
    }

private:
    int64_t FindMaxPrimeDivisor() const {
        if (Number <= 1) {
            return Number;
        }

        int64_t maxPrime = 1;
        int64_t n = Number;

        for (int64_t i = 2; i * i <= n; i++) {
            while (n % i == 0) {
                n /= i;
                maxPrime = i;
            }
        }

        if (n > 1) {
            maxPrime = n;
        }

        return maxPrime;
    }
};

THolder<NActors::IActor> CreateInputReaderActor(std::istream& input, NActors::TActorId target) {
    return MakeHolder<TInputReadActor>(input, target);
}

THolder<NActors::IActor> CreateOutputWriterActor(std::ostream& output) {
    return MakeHolder<TOutputWriteActor>(output);
}

THolder<NActors::IActor> CreateMaximumPrimeDevisorActor(int64_t number, NActors::TActorId readerId, NActors::TActorId writerId) {
    return MakeHolder<TMaximumPrimeDevisorActor>(number, readerId, writerId);
}
