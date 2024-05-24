#include "actors.h"
#include "events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

static auto ShouldContinue = std::make_shared<TProgramShouldContinue>();

namespace {
#define SEND(T, destination, ...)                               \
    do {                                                        \
        Send(destination, std::make_unique<T>(__VA_ARGS__));    \
    } while (0)

#define SEND_WAKEUP(destination)                                \
    SEND(NActors::TEvents::TEvWakeup, destination)

#define TIMEOUT_HANDLE(start)                                           \
    do {                                                                \
        if (TInstant::Now() - start >= TDuration::MilliSeconds(10)) {   \
            SEND_WAKEUP(SelfId());                                      \
            return;                                                     \
        }                                                               \
    } while (0)

class ActorFactory {
public:
    template <typename T, typename... Args>
    static THolder<T> GetInstance(Args&&... args) {
        return THolder(new T(std::forward<Args>(args)...));
    }
};

class TMaximumPrimeDevisorActor final : public NActors::TActorBootstrapped<TMaximumPrimeDevisorActor> {
public:
    void Bootstrap() {
        Become(&TMaximumPrimeDevisorActor::StateFunc);
        SEND_WAKEUP(SelfId());
    }

private:
    friend class ActorFactory;

    TMaximumPrimeDevisorActor(int64_t value, const NActors::TActorId& readActorId, const NActors::TActorId& writeActorId)
        : Value(value), ReadActorId(readActorId), WriteActorId(writeActorId)
    {}

    void TEvWakeupHandler() {
        auto startAt = TInstant::Now();
        for (; Divider * Divider <= Value; Divider += 2 - ((Divider + 1) & 1)) {
            while (Value % Divider == 0) {
                Result = std::max(Result, Divider);
                Value /= Divider;
                TIMEOUT_HANDLE(startAt);
            }
            TIMEOUT_HANDLE(startAt);
        }
        if (Value > 1) {
            Result = std::max(Result, Value);
        }
        SEND(TEvWriteValueRequest, WriteActorId, Result);
        SEND(TEvDone, ReadActorId);
        PassAway();
    }

    STRICT_STFUNC(StateFunc, {
        cFunc(NActors::TEvents::TEvWakeup::EventType, TEvWakeupHandler);
    });
    
    int64_t Result = 1;
    int64_t Divider = 2;
    int64_t Value;
    const NActors::TActorId ReadActorId;
    const NActors::TActorId WriteActorId;
};

class TReadActor final : public NActors::TActorBootstrapped<TReadActor> {
public:
    void Bootstrap() {
        Become(&TReadActor::StateFunc);
        SEND_WAKEUP(SelfId());
    }

private:
    friend class ActorFactory;

    explicit TReadActor(const NActors::TActorId& writeActorId)
        : WriteActorId(writeActorId)
    {}

    void DiscardsHandler() {
        if (EndOfStream && Remaining == 0) {
            SEND(NActors::TEvents::TEvPoisonPill, WriteActorId);
            PassAway();
        }
    }

    void TEvWakeupHandler() {
        int64_t data;
        if (std::cin >> data) {
            Register(ActorFactory::GetInstance<TMaximumPrimeDevisorActor>(data, SelfId(), WriteActorId).Release());
            ++Remaining;
            SEND_WAKEUP(SelfId());
            return;
        }
        EndOfStream = true;
        DiscardsHandler();
    }

    void TEvDoneHandler() {
        --Remaining;
        DiscardsHandler();
    }

    STRICT_STFUNC(StateFunc, {
        cFunc(NActors::TEvents::TEvWakeup::EventType, TEvWakeupHandler);
        cFunc(TEvDone::EventType, TEvDoneHandler);
    });

    const NActors::TActorId WriteActorId;
    int64_t Remaining = 0;
    bool EndOfStream = false;
};

class TWriteActor final : public NActors::TActorBootstrapped<TWriteActor> {
public:
    void Bootstrap() {
        Become(&TWriteActor::StateFunc);
    }

private:
    friend class ActorFactory;

    TWriteActor() = default;

    void TEvWriteValueRequestHandler(const TEvWriteValueRequest::TPtr& ev) {
        Summary += ev->Get()->Value;
    }

    void TEvPoisonPillHandler() {
        std::cout << Summary << std::endl;
        ShouldContinue->ShouldStop();
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, TEvPoisonPillHandler);
            hFunc(TEvWriteValueRequest, TEvWriteValueRequestHandler);
        }
    }

    int64_t Summary = 0;
};
}

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

THolder<NActors::IActor> CreateReadActor(const NActors::TActorId& writeActorId) {
    return ActorFactory::GetInstance<TReadActor>(writeActorId);
}

THolder<NActors::IActor> CreateWriteActor() {
    return ActorFactory::GetInstance<TWriteActor>();
}

std::shared_ptr<TProgramShouldContinue> GetProgramShouldContinue() {
    return ShouldContinue;
}
