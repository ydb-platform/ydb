#pragma once
#include <ydb/core/yq/libs/config/protos/rate_limiter.pb.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/actor.h>

namespace NYq {

NActors::TActorId RateLimiterControlPlaneServiceId();

NActors::IActor* CreateRateLimiterControlPlaneService(
    const NConfig::TRateLimiterConfig& rateLimiterConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory);

} // namespace NYq
