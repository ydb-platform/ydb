#pragma once
#include <ydb/core/yq/libs/config/protos/rate_limiter.pb.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <library/cpp/actors/core/actor.h>

namespace NYq {

NActors::IActor* CreateQuoterService(
    const NYq::NConfig::TRateLimiterConfig& rateLimiterConfig,
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory);

} // namespace NYq
