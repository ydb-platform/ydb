#pragma once

#include "counters.h"

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/signer/signer.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;

/// Connection manipulation actors
NActors::IActor* MakeCreateConnectionActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TString& objectStorageEndpoint,
    TSigner::TPtr signer);

NActors::IActor* MakeModifyConnectionActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TString& objectStorageEndpoint,
    TSigner::TPtr signer);

NActors::IActor* MakeDeleteConnectionActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TSigner::TPtr signer);

/// Binding manipulation actors
NActors::IActor* MakeCreateBindingActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

NActors::IActor* MakeModifyBindingActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

NActors::IActor* MakeDeleteBindingActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

} // namespace NPrivate
} // namespace NFq
