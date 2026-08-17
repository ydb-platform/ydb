#include "config.h"

namespace NYT::NChaosClient {

using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

void TReplicationCardCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_watching", &TThis::EnableWatching)
        .Default(true)
        .DontSerializeDefault();

    registrar.Preprocessor([] (auto* config) {
        config->ExpireAfterAccessTime = TDuration::Minutes(1);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(1);
        config->ExpireAfterFailedUpdateTime = TDuration::Minutes(1);
        config->RefreshTime = TDuration::Seconds(10);
    });
}

void TReplicationCardCacheConfig::ApplyDynamicInplace(const TReplicationCardCacheDynamicConfigPtr& dynamicConfig)
{
    TAsyncExpiringCacheConfig::ApplyDynamicInplace(dynamicConfig);
    UpdateYsonStructField(EnableWatching, dynamicConfig->EnableWatching);
}

TReplicationCardCacheConfigPtr TReplicationCardCacheConfig::ApplyDynamic(
    const TReplicationCardCacheDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(dynamicConfig);
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationCardCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_watching", &TThis::EnableWatching)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
