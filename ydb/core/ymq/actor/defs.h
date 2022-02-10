#pragma once
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSQS {

using NActors::TActorId;
using NActors::IActor;

template <typename TDerived>
using TActor = NActors::TActor<TDerived>;

template <typename TDerived>
using TActorBootstrapped = NActors::TActorBootstrapped<TDerived>;

using TEvWakeup = NActors::TEvents::TEvWakeup;
using TEvPoisonPill = NActors::TEvents::TEvPoisonPill;

} // namespace NKikimr::NSQS
