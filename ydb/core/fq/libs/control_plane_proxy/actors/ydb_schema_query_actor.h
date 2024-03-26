#pragma once

#include "counters.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/signer/signer.h>

namespace NFq::NPrivate {

enum class ETaskCompletionStatus {
    NONE,
    SUCCESS,
    SKIPPED,
    ROLL_BACKED,
    ERROR
};

/// Connection manipulation actors
NActors::IActor* MakeCreateConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TPermissions permissions,
    const NConfig::TCommonConfig& commonConfig,
    const TComputeConfig& computeConfig,
    TSigner::TPtr signer,
    bool withoutRollback = false,
    TMaybe<TString> connectionId = Nothing());

NActors::IActor* MakeModifyConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    const TComputeConfig& computeConfig,
    TSigner::TPtr signer);

NActors::IActor* MakeDeleteConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer);

/// Binding manipulation actors
NActors::IActor* MakeCreateBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TPermissions permissions,
    const TComputeConfig& computeConfig,
    bool withoutRollback = false,
    TMaybe<TString> bindingId = Nothing());

NActors::IActor* MakeModifyBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TComputeConfig& computeConfig);

NActors::IActor* MakeDeleteBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters);

} // namespace NFq::NPrivate
