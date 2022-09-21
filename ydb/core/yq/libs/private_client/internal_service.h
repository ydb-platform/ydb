#pragma once

#include "events.h"
#include "private_client.h"

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/yq/libs/events/event_subspace.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <ydb/core/yq/libs/control_plane_storage/proto/yq_internal.pb.h>

namespace NYq {

NActors::IActor* CreateInternalServiceActor(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} /* NYq */
