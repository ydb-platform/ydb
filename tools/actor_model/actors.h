#include <iostream>
#include <library/cpp/actors/core/actor.h>
#include <util/generic/ptr.h>
#include <library/cpp/actors/util/should_continue.h>
#include "theSieveOfEratosthenes.h"
THolder<NActors::IActor> CreateSelfPingActor(const TDuration& latency);
THolder<NActors::IActor> CreateSelfTReadActor(const NActors::TActorId writeActor, TheSieveOfEratosthenes* theSieveOfEratosthenes);
THolder <NActors::IActor> CreateSelfTMaximumPrimeDevisorActor(int value, const NActors::TActorIdentity readActor,
                                                              NActors::TActorId writeActor, TheSieveOfEratosthenes* theSieveOfEratosthenes);
THolder<NActors::IActor> CreateSelfTWriteActor();

std::shared_ptr<TProgramShouldContinue> GetProgramShouldContinue();
