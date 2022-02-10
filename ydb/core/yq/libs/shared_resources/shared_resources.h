#pragma once
#include "db_pool.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/core/yq/libs/config/protos/yq_config.pb.h>
#include <ydb/core/yq/libs/shared_resources/interface/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/actors/core/actorsystem.h>

namespace NYq {

struct TYqSharedResources : public IYqSharedResources {
    using TPtr = TIntrusivePtr<TYqSharedResources>;
    static TPtr Cast(const IYqSharedResources::TPtr& ptr);

    // Resources
    NYdb::TDriver YdbDriver;
    TDbPoolHolder::TPtr DbPoolHolder; 

protected:
    explicit TYqSharedResources(NYdb::TDriver driver);
};

TYqSharedResources::TPtr CreateYqSharedResourcesImpl(
    const NYq::NConfig::TConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory, 
    const NMonitoring::TDynamicCounterPtr& counters); 

} // namespace NYq
