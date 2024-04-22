#pragma once

#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>


namespace NKikimr::NKqp {

IActor* CreateKqpFinalizeScriptService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory);

}  // namespace NKikimr::NKqp
