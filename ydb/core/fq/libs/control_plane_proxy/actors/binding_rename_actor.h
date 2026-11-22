#pragma once

#include "counters.h"

#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

namespace NFq::NPrivate {

NActors::IActor* MakeProbeBindingRenameActor(const NActors::TActorId& proxy, TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TCounters& counters, TDuration timeout, TPermissions permissions);

NActors::IActor* MakeRenameBindingActor(const NActors::TActorId& proxy, TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TCounters& counters, TDuration timeout, TPermissions permissions, const TComputeConfig& compute, const TYqSharedResources::TPtr& resources,
    const NKikimr::TYdbCredentialsProviderFactory& credentials);

}   // namespace NFq::NPrivate
