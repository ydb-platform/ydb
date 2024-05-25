#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <util/generic/ptr.h>
#include <library/cpp/actors/util/should_continue.h>
#include <memory>

THolder<NActors::IActor> CreateSelfPingActor(const TDuration& latency);
THolder<NActors::IActor> CreateReadActor(std::shared_ptr<TProgramShouldContinue> shouldContinue, const NActors::TActorId& writeActor);
THolder<NActors::IActor> CreateMaximumPrimeDevisorActor(int64_t value, const NActors::TActorId& readActor, const NActors::TActorId& writeActor);
THolder<NActors::IActor> CreateWriteActor(std::shared_ptr<TProgramShouldContinue> shouldContinue);
std::shared_ptr<TProgramShouldContinue> GetProgramShouldContinue();
