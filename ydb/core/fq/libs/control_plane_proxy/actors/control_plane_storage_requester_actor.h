#pragma once

#include "counters.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;

/// Discover connection_name
NActors::IActor* MakeDiscoverYDBConnectionContentActor(
    const TActorId& proxyActorId,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBConnectionContentActor(
    const TActorId& proxyActorId,
    const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBConnectionContentActor(
    const TActorId& proxyActorId,
    const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBConnectionContentActor(
    const TActorId& proxyActorId,
    const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

/// Discover binding_name

NActors::IActor* MakeDiscoverYDBBindingContentActor(
    const TActorId& proxyActorId,
    const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBBindingContentActor(
    const TActorId& proxyActorId,
    const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

/// ModifyConnection

NActors::IActor* MakeListBindingIdsActor(
    const TActorId sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDescribeListedBindingActor(
    const TActorId sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

/// Uniqueness constraint

NActors::IActor* MakeListBindingIdsActor(
    const TActorId proxyActorId,
    const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeListConnectionIdsActor(
    const TActorId proxyActorId,
    const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeListBindingIdsActor(
    const TActorId proxyActorId,
    const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeListConnectionIdsActor(
    const TActorId proxyActorId,
    const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

} // namespace NPrivate
} // namespace NFq
