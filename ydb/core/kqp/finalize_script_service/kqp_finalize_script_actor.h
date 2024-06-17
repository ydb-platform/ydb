#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
    class TMetadataProviderConfig;
}

namespace NKikimr::NKqp {

IActor* CreateScriptFinalizerActor(TEvScriptFinalizeRequest::TPtr request,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory);

}  // namespace NKikimr::NKqp
