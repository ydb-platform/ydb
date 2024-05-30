#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>


namespace NKikimr::NKqp {

IActor* CreateScriptFinalizerActor(TEvScriptFinalizeRequest::TPtr request,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup);

}  // namespace NKikimr::NKqp
