#pragma once

#include "counters.h"

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/signer/signer.h>

namespace NFq {
namespace NPrivate {

/// Connection manipulation actors
NActors::IActor* MakeCreateConnectionActor(
    const NActors::TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer);

NActors::IActor* MakeModifyConnectionActor(
    const NActors::TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer);

NActors::IActor* MakeDeleteConnectionActor(
    const NActors::TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer);

/// Binding manipulation actors
NActors::IActor* MakeCreateBindingActor(
    const NActors::TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

NActors::IActor* MakeModifyBindingActor(
    const NActors::TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

NActors::IActor* MakeDeleteBindingActor(
    const NActors::TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

} // namespace NPrivate
} // namespace NFq
