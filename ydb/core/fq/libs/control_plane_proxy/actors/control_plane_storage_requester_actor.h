#pragma once

#include "counters.h"

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;

/// Discover connection_name
NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

/// Discover binding_name

NActors::IActor* MakeDiscoverYDBBindingName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDiscoverYDBBindingName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

/// ModifyConnection

NActors::IActor* MakeListBindingIds(
    const TActorId sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

NActors::IActor* MakeDescribeListedBinding(
    const TActorId sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions);

} // namespace NPrivate
} // namespace NFq
