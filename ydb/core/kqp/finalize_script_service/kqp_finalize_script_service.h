#pragma once

#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
    class TMetadataProviderConfig;
}

namespace NKikimr::NKqp {

IActor* CreateKqpFinalizeScriptService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory);

}  // namespace NKikimr::NKqp
