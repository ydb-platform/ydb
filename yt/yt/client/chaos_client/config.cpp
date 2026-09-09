#include "config.h"

namespace NYT::NChaosClient {

using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

void TWatchedReplicationCardCacheConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (auto* config) {
        config->ExpireAfterAccessTime = TDuration::Days(1);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Days(1);
        config->ExpireAfterFailedUpdateTime = TDuration::Minutes(1);
        config->RefreshTime = std::nullopt;
        config->ExpirationPeriod = TDuration::Seconds(10);
    });
}

TWatchedReplicationCardCacheConfigPtr TWatchedReplicationCardCacheConfig::ApplyDynamic(
    const TAsyncExpiringCacheDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(dynamicConfig);
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationCardCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_watching", &TThis::EnableWatching)
        .Default(true)
        .DontSerializeDefault();
    registrar.Parameter("watched_cache", &TThis::WatchedCacheConfig)
        .DefaultNew();

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
    WatchedCacheConfig = WatchedCacheConfig->ApplyDynamic(dynamicConfig->WatchedCacheConfig);
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
    registrar.Parameter("watched_cache", &TThis::WatchedCacheConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
