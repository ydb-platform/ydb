#pragma once

#include <ydb/core/fq/libs/config/protos/common.pb.h>

#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>


namespace NKikimr::NKqp {

IActor* CreateKqpFinalizeScriptService(const NKikimrConfig::TFinalizeScriptServiceConfig& finalizeScriptServiceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig, const NFq::NConfig::TCommonConfig& federatedQueryConfig,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory);

}  // namespace NKikimr::NKqp
