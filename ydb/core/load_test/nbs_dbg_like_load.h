#pragma once

#include <ydb/core/protos/load_test.pb.h>

#include <ydb/library/actors/core/actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NNbsDbgLike {

// TNbsDbgLikeLoadActor — standard load actor that drives a
// TNbsDbgLikeLoadTablet via a tablet pipe using TEvLoad::TEvNbsWrite /
// TEvNbsRead. It owns the address sampler (Sequential / random, `M_eff`
// slice), the MaxInFlight budget, the ReadRatio
// pacing, and the per-Run latency histograms. On TEvPoisonPill
// (scheduled at DurationSeconds) the actor drains, sends
// TEvLoadTestFinished to its parent (the service actor), and PassAways.
// Started by TLoadActor in service_actor.cpp like any other load actor.
NActors::IActor* CreateNbsDbgLikeLoadActor(
    const NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
    const NActors::TActorId& parent,            // service actor (TLoadActor)
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag);

} // namespace NKikimr::NNbsDbgLike
