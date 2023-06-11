#include "actors.h"
#include "events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <limits>
#include <math.h>
#include "theSieveOfEratosthenes.h"

static auto ShouldContinue = std::make_shared<TProgramShouldContinue>();

/*
Вам нужно написать реализацию TReadActor, TMaximumPrimeDevisorActor, TWriteActor
*/
/*
Требования к TReadActor:
1. Рекомендуется отнаследовать этот актор от NActors::TActorBootstrapped
2. В Boostrap этот актор отправляет себе NActors::TEvents::TEvWakeup
3. После получения этого сообщения считывается новое int значение из strm
4. После этого порождается новый TMaximumPrimeDevisorActor который занимается вычислениями
5. Далее актор посылает себе сообщение NActors::TEvents::TEvWakeup чтобы не блокировать поток этим актором
6. Актор дожидается завершения всех TMaximumPrimeDevisorActor через TEvents::TEvDone
7. Когда чтение из файла завершено и получены подтверждения от всех TMaximumPrimeDevisorActor,
этот актор отправляет сообщение NActors::TEvents::TEvPoisonPill в TWriteActor

TReadActor
    Bootstrap:
        send(self, NActors::TEvents::TEvWakeup)

    NActors::TEvents::TEvWakeup:
        if read(strm) -> value:
            register(TMaximumPrimeDevisorActor(value, self, receipment))
            send(self, NActors::TEvents::TEvWakeup)
        else:
            ...

    TEvents::TEvDone:
        if Finish:
            send(receipment, NActors::TEvents::TEvPoisonPill)
        else:
            ...
*/

// TODO: напишите реализацию TReadActor

class TReadActor : public NActors::TActorBootstrapped<TReadActor> {
private:
    const NActors::TActorId WriteActor;
    int aliveActors;
    TheSieveOfEratosthenes* theSieveOfEratosthenes;
public:
    TReadActor(NActors::TActorId writeActor, TheSieveOfEratosthenes* theSieveOfEratosthenes1)
            : WriteActor(writeActor), aliveActors(0), theSieveOfEratosthenes(theSieveOfEratosthenes1) {}

    void Bootstrap() {
        Become(&TReadActor::StateFunc);
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
    }

    STRICT_STFUNC(StateFunc,
    {
        cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeUp);
        cFunc(TEvents::TEvDone::EventType, HandleDone);
    });

    void HandleWakeUp() {
        int value;
        if (std::cin >> value) {
            Register(CreateSelfTMaximumPrimeDevisorActor(value, SelfId(), WriteActor, theSieveOfEratosthenes).Release());
            aliveActors++;
            Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
        }
    }

    void HandleDone() {
        aliveActors--;
        if (aliveActors == 0) {
            Send(WriteActor, std::make_unique<NActors::TEvents::TEvPoisonPill>());
        }
    }
};
/*
Требования к TMaximumPrimeDevisorActor:
1. Рекомендуется отнаследовать этот актор от NActors::TActorBootstrapped
2. В конструкторе этот актор принимает:
 - значение для которого нужно вычислить простое число
 - ActorId отправителя (ReadActor)
 - ActorId получателя (WriteActor)
2. В Boostrap этот актор отправляет себе NActors::TEvents::TEvWakeup по вызову которого происходит вызов Handler для вычислений
3. Вычисления нельзя проводить больше 10 миллисекунд
4. По истечении этого времени нужно сохранить текущее состояние вычислений в акторе и отправить себе NActors::TEvents::TEvWakeup
5. Когда результат вычислен он посылается в TWriteActor c использованием сообщения TEvWriteValueRequest
6. Далее отправляет ReadActor сообщение TEvents::TEvDone
7. Завершает свою работу

TMaximumPrimeDevisorActor
    Bootstrap:
        send(self, NActors::TEvents::TEvWakeup)

    NActors::TEvents::TEvWakeup:
        calculate
        if > 10 ms:
            Send(SelfId(), NActors::TEvents::TEvWakeup)
        else:
            Send(WriteActor, TEvents::TEvWriteValueRequest)
            Send(ReadActor, TEvents::TEvDone)
            PassAway()
*/

// TODO: напишите реализацию TMaximumPrimeDevisorActor

class TMaximumPrimeDevisorActor : public NActors::TActorBootstrapped<TMaximumPrimeDevisorActor> {
private:
    int Value;
    int answer;
    int ObtainedInThePreviousIteration;
    NActors::TActorIdentity ReadActor;
    NActors::TActorId WriteActor;
    TheSieveOfEratosthenes* theSieveOfEratosthenes;

public:
    TMaximumPrimeDevisorActor(int value, NActors::TActorIdentity readActor, NActors::TActorId writeActor, TheSieveOfEratosthenes* theSieveOfEratosthenes1)
            : Value(value), ReadActor(readActor),
              WriteActor(writeActor), ObtainedInThePreviousIteration(1),
              answer(1), theSieveOfEratosthenes(theSieveOfEratosthenes1) {}

    void Bootstrap() {
        Become(&TMaximumPrimeDevisorActor::StateFunc);
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
    }

    STRICT_STFUNC(StateFunc,
    {
        cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeUp);
    });

    void HandleWakeUp() {
        auto startTime = std::chrono::steady_clock::now();
        if(Value >= theSieveOfEratosthenes->getN()){
            theSieveOfEratosthenes->selfResize(Value);
        }
        if(theSieveOfEratosthenes->checkPrime(Value)){
            answer = Value;
        }
        else{
            for(int i = ObtainedInThePreviousIteration; i <= Value; i++){
                if(theSieveOfEratosthenes->checkPrime(i)){
                    if(Value % i == 0 && i > answer){
                        answer = i;
                    }
                }
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - startTime).count() > 10) {
                    ObtainedInThePreviousIteration = i;
                    Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
                    return;
                }
            }
        }
        Send(WriteActor, std::make_unique<TEvents::TEvWriteValueRequest>(answer));
        Send(ReadActor, std::make_unique<TEvents::TEvDone>());
        PassAway();
    }
};

/*
Требования к TWriteActor:
1. Рекомендуется отнаследовать этот актор от NActors::TActor
2. Этот актор получает два типа сообщений NActors::TEvents::TEvPoisonPill::EventType и TEvents::TEvWriteValueRequest
2. В случае TEvents::TEvWriteValueRequest он принимает результат посчитанный в TMaximumPrimeDevisorActor и прибавляет его к локальной сумме
4. В случае NActors::TEvents::TEvPoisonPill::EventType актор выводит в Cout посчитанную локальнкую сумму, проставляет ShouldStop и завершает свое выполнение через PassAway

TWriteActor
    TEvents::TEvWriteValueRequest ev:
        Sum += ev->Value

    NActors::TEvents::TEvPoisonPill::EventType:
        Cout << Sum << Endl;
        ShouldStop()
        PassAway()
*/

// TODO: напишите реализацию TWriteActor

class TWriteActor : public NActors::TActor<TWriteActor> {
private:
    int sum;
public:
    using TBase = NActors::TActor<TWriteActor>;

    TWriteActor() : TBase(&TWriteActor::Handler), sum(0) {}

    STRICT_STFUNC(Handler,
    {
        hFunc(TEvents::TEvWriteValueRequest, HandleWakeUp);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, HandleDone);
    });

    void HandleWakeUp(TEvents::TEvWriteValueRequest::TPtr &ev) {
        auto &event = *ev->Get();
        sum = sum + event.Value;
    }

    void HandleDone() {
        std::cout << sum << std::endl;
        ShouldContinue->ShouldStop();
        PassAway();
    }
};

class TSelfPingActor : public NActors::TActorBootstrapped<TSelfPingActor> {
    TDuration Latency;
    TInstant LastTime;

public:
    TSelfPingActor(const TDuration &latency)
            : Latency(latency) {}

    void Bootstrap() {
        LastTime = TInstant::Now();
        Become(&TSelfPingActor::StateFunc);
        Send(SelfId(), std::make_unique<NActors::TEvents::TEvWakeup>());
    }

    STRICT_STFUNC(StateFunc,
    {
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

THolder <NActors::IActor> CreateSelfPingActor(const TDuration &latency) {
    return MakeHolder<TSelfPingActor>(latency);
}

THolder <NActors::IActor> CreateSelfTReadActor(const NActors::TActorId writeActor, TheSieveOfEratosthenes* theSieveOfEratosthenes) {
    return MakeHolder<TReadActor>(writeActor, theSieveOfEratosthenes);
}

THolder <NActors::IActor> CreateSelfTMaximumPrimeDevisorActor(int value, const NActors::TActorIdentity readActor,
                                                              const NActors::TActorId writeActor, TheSieveOfEratosthenes* theSieveOfEratosthenes) {
    return MakeHolder<TMaximumPrimeDevisorActor>(value, readActor, writeActor, theSieveOfEratosthenes);
}

THolder <NActors::IActor> CreateSelfTWriteActor() {
    return MakeHolder<TWriteActor>();
}

std::shared_ptr <TProgramShouldContinue> GetProgramShouldContinue() {
    return ShouldContinue;
}